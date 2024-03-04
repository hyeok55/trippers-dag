from datetime import datetime, timedelta
import json
import requests
import logging
import codecs
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner': 'jaewoo',
    'start_date': days_ago(1),
    'provide_context': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

@task
def fetch_api_and_save_to_s3(api_key):
    # API 요청을 보낼 URL 설정
    response = requests.get(f'https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={api_key}&data=AP01')

    # 응답 데이터를 JSON 형식으로 변환
    data = response.json()
    if not data:
        logging.warning("API로부터 데이터를 가져오지 못했습니다. DAG 실행을 중단합니다.")
        return

    # 현재 연월시분을 포함한 파일 이름 설정
    now = datetime.now().strftime("%Y%m%d%H%M")
    file_name = f'exchange_rate/exchange_rate_{now}.json'

    # 현재 연월일시분을 포함한 값을 각 데이터의 result로 설정
    time_stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    for item in data:
        item['result'] = time_stamp
        
    # JSON 데이터를 S3에 저장
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    # indent는 보기 Json 파일상에서 보기 편의성
    # ensure_ascii=False는 한글 보기 편의성 ex) "일본 옌"
    data_str = json.dumps(data, indent=4, ensure_ascii=False)
    s3_hook.load_string(
        string_data=data_str,
        key=file_name,
        bucket_name='de-6-2-bucket'
        )

with DAG(
    'exchange_rate_api_to_s3',
    default_args=default_args,
    catchup = False,
    schedule_interval='1 13 * * *'  # 매일 오후 1시 1분에 실행하도록 설정 환율 정보가 오전 11 시 갱신
) as dag:
    fetch_and_save_task = fetch_api_and_save_to_s3(Variable.get("exchange_rate_api_key"))
    trigger_exchange_s3_to_red = TriggerDagRunOperator(
        task_id='trigger_exchange_s3_to_red',
        trigger_dag_id='exchange_s3_to_red',
        execution_date="{{ execution_date }}"
    )
