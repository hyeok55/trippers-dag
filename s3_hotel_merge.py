import requests
import json
import logging, time
import pendulum
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.operators.dagrun_operator import TriggerDagRunOperator

citys = [
    "fukuoka",
    "nagoya",
    "narita",
    "okinawa",
    "osaka",
    "sapporo",
    "tokyo"
]
@task
def merge_hotel_lists():
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    bucket_name = 'de-6-2-bucket'
    all_citys_hotel_data = []
    for city in citys:
        prefix = 'raw_data_hotel/{}/'.format(city)
        json_files = s3_hook.list_keys(bucket_name, prefix=prefix)
        latest_file = None
        latest_modified_time = None
    
        for file in json_files:
            if file.endswith('.json'):
                file_name = s3_hook.get_key(file, bucket_name=bucket_name)
                if latest_modified_time is None or file_name.last_modified > latest_modified_time:
                    latest_file = file
                    latest_modified_time = file_name.last_modified
        
        json_data = s3_hook.read_key(latest_file, bucket_name=bucket_name)
        parsed_data = json.loads(json_data)
        
        for data in parsed_data:
            all_citys_hotel_data.append({
                "name": data["name"],
                "link": data["link"],
                "city" : city,
                "location": data["location"],
                "pricing": data["pricing"],
                "rating": data["rating"],
                "review_count": data["review_count"],
                "thumbnail": data["thumbnail"],
                "room_unit" : data["room_unit"],
                "recommended_units" : data["recommended_units"],
                "checkin" : data["checkin"],
                "checkout" : data["checkout"]
            })
    
    now = datetime.now().strftime("%Y%m%d%H%M")
    file_name = f'raw_data_hotel/hotel_list/hotellist_{now}.json'
    data_str = json.dumps(all_citys_hotel_data, indent=4, ensure_ascii=False)
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    s3_hook.load_string(
        string_data=data_str,
        key=file_name,
        bucket_name='de-6-2-bucket'
    )

default_args = {
    'owner': 'ohyoung',
    'start_date': pendulum.yesterday(),
    'provide_context': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='merge_hotel_list',
    default_args=default_args,
    # Schedule to run specific timeline
    schedule_interval="25 15 * * *"
    #schedule_interval="@once"
) as dag:
    # 직접 작성
    merge_hotel_lists_of_citys = merge_hotel_lists()
    
    trigger_dag1 = TriggerDagRunOperator(
        task_id='trigger_dag1',
        trigger_dag_id="hotellists_s3_to_red",
        dag=dag
    )

    merge_hotel_lists_of_citys >> trigger_dag1
    
    