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

default_args = {
    'owner': 'ohyoung',
    'start_date': pendulum.yesterday(),
    'provide_context': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='aws_red')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()
    
def get_Mysql_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='aws_mysql')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def move_redshift_to_rds():
    redshift_conn = get_Redshift_connection()
    rds_conn = get_Mysql_connection()

    # Redshift에서 데이터를 가져옵니다.
    redshift_query = "SELECT * FROM DEV.PUBLIC.HOTEL_LISTS"
    redshift_data = redshift_conn.get_records(redshift_query)

    
    rds_table = "rds_mysql_table"
    for row in redshift_data:
        #rds_conn.run("INSERT INTO rds_table (column1, column2) VALUES (%s, %s)", (row[0], row[1]))


with DAG(
    dag_id = 'redshift_to_mysql_hotel',
    default_args=default_args,
    schedule_interval="30 15 * * *"
) as dag:
    