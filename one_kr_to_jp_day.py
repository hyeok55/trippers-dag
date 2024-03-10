from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from amadeus import Client, ResponseError
import pandas as pd
import boto3
from io import StringIO
from airflow.hooks.base_hook import BaseHook
import time
import json

aws_conn = BaseHook.get_connection("aws_conn_id")
aws_access_key_id = aws_conn.login
aws_secret_access_key = aws_conn.password

# Amadeus 클라이언트 생성 - 형후님
amadeus = Client(
    client_id='AsNLnF0kGas2Zh46sabDkZzFvcDt7Xcd',
    client_secret='BpphXl3KIB1Zi0qb'
)


# 일본 공항 리스트
airport = ['HND','NRT','KIX','FUK','NGO','CTS','OKA']

# 현재 연월일 받아오기
date_list = []
api_date = date.today() + timedelta(days=31)
dag_date = str(api_date)
for i in range(1,11):
    date_list.append(str(api_date + timedelta(days=i)))

# S3 연결 정보
s3_bucket = 'de-6-2-bucket'
s3_key = 'one_trip/'+'kr_'+dag_date+'.csv' # 현재 날짜에서 5일 뒤로 수정





# DAG 기본 인수
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 21, 14, 0),  # 시작 시간을 오전 1시로 설정
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Amadeus API를 호출하여 데이터를 가져오는 함수
def fetch_flight_offers(airport, dag_date, date_list):
    response_list = []
    for port in airport:
        for dt in date_list:
            try:
                response = amadeus.shopping.flight_offers_search.get(
                    originLocationCode='ICN',
                    destinationLocationCode=port,
                    departureDate=dt,
                    adults=1,
                    nonStop='true')
                response_list += response.data
                time.sleep(5)
            except ResponseError as error:
                print(error)
                return []
    return response_list


# 데이터 전처리 함수
def preprocess_data(data):
    round_df = pd.json_normalize(data)
    itineraries_df = round_df['itineraries']
    travelerPricings_df = round_df['travelerPricings']
    fees_df = round_df['price.fees']

    # fees 전처리
    data_list = []
    for i in range(len(fees_df)):
        data = fees_df[i]
        new_data = {f"{d['type']}_amount": d['amount'] for d in data}
        data_list.append(new_data)
    fees_df = pd.DataFrame(data_list)

    # itineraries 전처리
    def process_go_itinerary(df):
        data_list = []
        for i in range(len(df)):
            data = df[i][0]['segments'][0]
            new_data = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        new_key = f"{key}_{sub_key}"
                        new_data[new_key] = sub_value
                else:
                    new_data[key] = value
            data_list.append(new_data)
        return pd.DataFrame(data_list)
    
    def process_come_itinerary(df):
        data_list = []
        for i in range(len(df)):
            data = df[i][0]['segments'][0]
            new_data = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        new_key = f"{key}_{sub_key}"
                        new_data[new_key] = sub_value
                else:
                    if key == 'id':
                        new_key = f"iti_{key}"
                        new_data[new_key] = value
                    else:    
                        new_data[key] = value 
            data_list.append(new_data)
        return pd.DataFrame(data_list)

    itineraries_df = process_go_itinerary(itineraries_df)

    # travelerPricings 전처리
    def process_traveler_pricing(df):
        data_list = []
        for i in range(len(df)):
            data = df[i][0]
            if 'price' in data:
                del data['price']
            new_data = {}
            for key, value in data.items():
                if isinstance(value, list):  # 값이 리스트인 경우
                    for i, sub_data in enumerate(value):
                        for sub_key, sub_value in sub_data.items():
                            
                            if isinstance(sub_value, dict):  # 값이 딕셔너리인 경우
                                for sub_sub_key, sub_sub_value in sub_value.items():
                                    
                                    new_key = f"{sub_key}_{sub_sub_key}"  # 새로운 키에 딕셔너리의 키를 추가
                                    new_data[new_key] = sub_sub_value # 새로운 키와 값을 새로운 딕셔너리에 추가
                                    
                            
                            else:
                                new_key = f"{sub_key}"   
                                new_data[new_key] = sub_value  # 새로운 키와 값을 새로운 딕셔너리에 추가
                else:
                    new_data[key] = value
            data_list.append(new_data)
        return pd.DataFrame(data_list)

    travelerPricings_df = process_traveler_pricing(travelerPricings_df)
    #travelerPricings_df.drop(columns=['amenities'], inplace=True)

    # 기존 데이터프레임에서 필요없는 열 삭제
    round_df.drop(columns=['itineraries', 'travelerPricings', 'price.fees','validatingAirlineCodes','pricingOptions.fareType'], inplace=True)
    
    # 데이터 병합
    round_df = pd.concat([round_df, fees_df, itineraries_df, travelerPricings_df], axis=1)
    round_df = round_df.rename(columns=lambda x: x.replace('.', '_')) # 변경
    round_df.drop(columns = ['type', 'id', 'source', 'instantTicketingRequired','nonHomogeneous','oneWay','lastTicketingDateTime',
                            'price_grandTotal','pricingOptions_includedCheckedBagsOnly','SUPPLIER_amount','TICKETING_amount',
                            'aircraft_code','operating_carrierCode','numberOfStops','blacklistedInEU',
                            'travelerId','fareOption','travelerType','segmentId','fareBasis','class','includedCheckedBags_quantity',
                            'lastTicketingDate','price_base','brandedFare','brandedFareLabel','amenities','includedCheckedBags_weight','includedCheckedBags_weightUnit'], inplace = True)
    # carreir code 합치는 과정 -> 편도
    round_df['carrier_code'] = round_df['carrierCode'] +' ' + round_df['number']
    round_df.drop(columns=['carrierCode','number'],inplace = True)
    round_df = round_df.rename(columns={'price_total':'price','departure_at':"departure_datetime",'arrival_at':'arrival_datetime','cabin': 'cabin_class','numberOfBookableSeats':'number_Of_Bookable_Seats','departure_iataCode':'departure_city_id','arrival_iataCode':'arrival_city_id'}) 

    #city id 매핑     ->   'ICN': 1,'GMP': 2,'PUS': 3,'CJU': 4,'HND': 5,'KIX': 6,'NGO': 7,'FUK': 8,'FSZ': 9,'KCZ': 10,'OKA': 11,'CTS': 12,'NRT': 13
    mapping = {'ICN': 1,'GMP': 2,'PUS': 3,'CJU': 4,'HND': 5,'KIX': 6,'NGO': 7,'FUK': 8,'FSZ': 9,'KCZ': 10,'OKA': 11,'CTS': 12,'NRT': 13}
    round_df['departure_city_id'] = round_df['departure_city_id'].map(mapping)
    round_df['arrival_city_id'] = round_df['arrival_city_id'].map(mapping)
    round_df = round_df.to_dict()
    round_df = json.dumps(round_df)
    return round_df


# S3에 저장하는 함수
def save_to_s3(data, s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key):
    result_all = json.loads(data)
    result_all = pd.DataFrame(result_all)
    s3 = boto3.client('s3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)
    s3.put_object(Body=result_all.to_csv(encoding='utf-8'), Bucket=s3_bucket, Key=s3_key)#변경
    


# Airflow DAG에 태스크 추가
with DAG('api_kr_to_jp_day', default_args=default_args, description='Fetch flight offers from Amadeus API and save to S3', schedule_interval=timedelta(days=1),catchup = False) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_flight_offers',
        python_callable=fetch_flight_offers,
        op_kwargs={'airport': airport, 'dag_date': dag_date, 'date_list': date_list},
    )

    preprocess_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
        op_args=[fetch_task.output],
    )

    save_task = PythonOperator(
        task_id='save_to_s3',
        python_callable=save_to_s3,
        op_args=[preprocess_task.output , s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key],
    )
fetch_task >> preprocess_task >> save_task
