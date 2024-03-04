import json
import logging
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta



def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='aws_red')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def extract_transform(**context):
    logging.info("추출 및 가공 시작")   
    # S3 연결 설정
    s3_hook = S3Hook(aws_conn_id='aws_s3')

    # S3 버킷에서 JSON 파일 목록 가져오기
    bucket_name = 'de-6-2-bucket'
    prefix = 'exchange_rate/'
    json_files = s3_hook.list_keys(bucket_name, prefix=prefix)
    
    # 가져온 객체들 중에서 JSON 파일만 필터링하여 가장 최근 파일 선택
    latest_file = None
    latest_modified_time = None
    
    for file in json_files:
        if file.endswith('.json'):
            file_info = s3_hook.get_key(file, bucket_name=bucket_name)
            if latest_modified_time is None or file_info.last_modified > latest_modified_time:
                latest_file = file
                latest_modified_time = file_info.last_modified
                
    # 최근 파일의 내용 가져오기
    if latest_file is not None:
        json_data = s3_hook.read_key(latest_file, bucket_name=bucket_name)
        # json_data 변수에 최근 파일의 JSON 데이터가 들어오면 가공.
        # JSON 데이터 가공
        parsed_data = json.loads(json_data)
        # 필요한 정보를 추출하여 가공하는 작업.
        processed_data = []
        
        for item in parsed_data:
            # 유로화, 일본 옌, 달러 만 선택
            if item["cur_unit"] == 'EUR' or item["cur_unit"] == 'JPY(100)' or item["cur_unit"] == 'USD':
                result = item["result"]
                cur_unit = item["cur_unit"]
                ttb = float(item["ttb"].replace(',', ''))
                tts = float(item["tts"].replace(',', ''))
                cur_nm = item["cur_nm"]

            # 가공된 데이터를 딕셔너리 형태로 저장
                processed_data.append({
                    "검색_시간": result,
                    "cur_unit": cur_unit,
                    "파실_때": ttb,
                    "사실_때": tts,
                    "cur_nm": cur_nm
                    })
    return processed_data

def load(**context):
    logging.info("Redshift에 가공 시작")    
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    records = context["task_instance"].xcom_pull(key="return_value", task_ids="extract_transform")
    
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        검색_시간 TIMESTAMP,
                        cur_unit VARCHAR(10),
                        파실_때 FLOAT,
                        사실_때 FLOAT,
                        cur_nm VARCHAR(100)
                        );
                        """)
        if records is not None:
            for r in records:
                cur.execute(f"""
                            INSERT INTO {schema}.{table} (검색_시간, cur_unit, 파실_때, 사실_때, cur_nm)
                            VALUES (%s, %s, %s, %s, %s);""",
                            (r["검색_시간"], r["cur_unit"], r["파실_때"], r["사실_때"], r["cur_nm"]))
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")

default_args = {
    'owner': 'jaewoo',
    'provide_context': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    dag_id='exchange_s3_to_red',
    default_args=default_args,
    catchup = False,
    start_date=datetime(2024, 1, 1)
)

extract_transform = PythonOperator(
    task_id = 'extract_transform',
    python_callable = extract_transform,
    params = {
    },
    dag = dag)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'public',
        'table': 'exchange_rate'
    },
    dag = dag)

extract_transform >> load