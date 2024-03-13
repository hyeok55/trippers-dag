from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import io
import json
import psycopg2
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 21, 14, 30),  # 시작 시간을 오전 1시로 설정
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_json_to_redshift():
    # S3 연결 설정
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    s3_bucket_name = 'de-6-2-bucket'

    # 레드시프트 연결 설정
    redshift_hook = PostgresHook(postgres_conn_id='redshift_conn_id')

    # S3 버킷에서 모든 CSV 파일 목록 가져오기
    csv_files = s3_hook.list_keys(bucket_name=s3_bucket_name, prefix='one_trip/')
    csv_files = [file for file in csv_files if file.endswith('.csv')]

    # 각 CSV 파일을 읽어서 레드시프트로 복사
    for csv_file in csv_files:
        # S3에서 CSV 파일 읽기
        logging.info(f"Processing file: {csv_file}")
        csv_obj = s3_hook.get_key(bucket_name=s3_bucket_name, key=csv_file)
        logging.info(f"Processing file: {csv_obj}")
        csv_content = csv_obj.get()['Body'].read().decode('utf-8')
        
        # CSV 데이터를 Pandas DataFrame으로 변환
        result_all = pd.read_csv(io.StringIO(csv_content), encoding='utf-8',index_col=0)
        result_all.fillna(psycopg2.extensions.AsIs('NULL'),inplace=True) 
        
        # 레드시프트로 데이터 복사
        target_fields = result_all.columns.tolist()
        redshift_hook.insert_rows(table='raw_data.flight_offers', rows=result_all.values.tolist(), target_fields=target_fields)


# DAG 정의
with DAG('one_s3_to_redshift', default_args=default_args, description='Transfer CSV files from S3 to Redshift', schedule_interval=timedelta(days=1),catchup = False) as dag:
    
    # CSV 파일을 레드시프트로 복사하는 태스크
    load_json_to_redshift_task = PythonOperator(
        task_id='load_json_to_redshift',
        python_callable=load_json_to_redshift,
        dag=dag
    )

    # Task 의 실행 순서 설정
    load_json_to_redshift_task
