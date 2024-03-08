import requests
import json
import logging, time
import pendulum
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
import boto3
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

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='aws_red')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def extract_transform():
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    bucket_name = 'de-6-2-bucket'
    prefix = 'raw_data_hotel/hotel_list/'
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
    return parsed_data

@task
def etl_hotellists_to_red(data_list):
    logging.info("Redshift load start")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM DEV.PUBLIC.HOTEL_LISTS;")
        cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS PUBLIC.HOTEL_LISTS (
                        호텔_이름 VARCHAR(MAX),
                        호텔_링크 VARCHAR(MAX),
                        도시 VARCHAR(MAX),
                        지역 VARCHAR(MAX),
                        가격 VARCHAR(MAX),
                        별점 VARCHAR(MAX),
                        리뷰_갯수 VARCHAR(MAX),
                        사진 VARCHAR(MAX),
                        방_종류 VARCHAR(MAX),
                        방_정보 VARCHAR(MAX),
                        체크인 DATE,
                        체크아웃 DATE
                        );
                        """)
        if data_list is not None:
            for val in data_list:
                cur.execute(f"""
                            INSERT INTO PUBLIC.HOTEL_LISTS (호텔_이름, 호텔_링크, 도시, 지역, 가격, 별점, 리뷰_갯수, 사진, 방_종류, 방_정보, 체크인, 체크아웃)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                            (val["name"], val["link"], val["city"], val["location"], val["pricing"], val["rating"], val["review_count"], val["thumbnail"], val["room_unit"], val["recommended_units"], val["checkin"], val["checkout"]))
        cur.execute("COMMIT;")
    except Exception as e:
        logging.error(f"error occured while loading data", e)
        cur.execute("ROLLBACK;")
        raise
    logging.info("Redshift load finish")

default_args = {
    'owner': 'ohyoung',
    'start_date': pendulum.yesterday(),
    'provide_context': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id = 'hotellists_s3_to_red',
    default_args=default_args,
    #schedule_interval="30 15 * * *"
    schedule_interval="@once"
) as dag:
    data_hotel = extract_transform()
    load_to_redshift = etl_hotellists_to_red(data_hotel)

    red_to_mysql = TriggerDagRunOperator(
        task_id='red_to_mysql',
        trigger_dag_id="redshift_to_mysql_hotel",
        dag=dag
    )

    data_hotel >> load_to_redshift >> red_to_mysql
