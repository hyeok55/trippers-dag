import requests
import json
import logging, time
import pendulum
import psycopg2
import decimal
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from datetime import timedelta
import boto3

default_args = {
    'owner': 'ohyoung',
    'start_date': pendulum.yesterday(),
    'provide_context': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}
citys_id = {
    'fukuoka' : 8,
    'nagoya' : 7,
    'osaka' : 6,
    'tokyo' : 5,
    'okinawa' : 11,
    'sapporo' : 12,
    'narita' : 13
}
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='aws_red')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()
    
def get_Mysql_connection(autocommit=True):
    mysql_hook = MySqlHook(mysql_conn_id='aws_mysql')
    mysql_conn = mysql_hook.get_conn()
    mysql_conn.autocommit = autocommit
    return mysql_conn.cursor()
'''
@task
def extract_redshift_data():
    redshift_conn = get_Redshift_connection()
    data_list = []
    try:
        redshift_conn.execute(f"""SELECT * FROM PUBLIC.HOTEL_LISTS;""")
        data_list = redshift_conn.fetchall()
    except Exception as e:
        logging.error(f"error occured while loading data", e)
        redshift_conn.execute("ROLLBACK;")
        raise
    else:
        logging.info("Redshift load finish")
        print(len(data_list))
        return data_list
'''
@task
def move_redshift_to_rds():
    redshift_conn = get_Redshift_connection()
    redshift_conn.execute(f"""SELECT * FROM PUBLIC.HOTEL_LISTS;""")
    data_list = redshift_conn.fetchall()
    rds_conn = get_Mysql_connection()
    try:
        logging.info("mysql start")
        rds_conn.execute(f"""TRUNCATE TABLE trippers.accomodation;""")
        for val in data_list:
            name = val[0]
            link = val[1]
            city = citys_id[val[2]]
            location = val[3]
            pricing = decimal.Decimal(val[4].replace('₩', '').replace(',', ''))
            if '강력 추천 ' in val[5]:
                rating = decimal.Decimal(val[5].replace('강력 추천 ', ''))
            elif '최고 ' in val[5]:
                rating = decimal.Decimal(val[5].replace('최고 ', ''))
            elif '매우 좋음 ' in val[5]:
                rating = decimal.Decimal(val[5].replace('매우 좋음 ', ''))
            elif '좋음 ' in val[5]:
                rating = decimal.Decimal(val[5].replace('좋음 ', ''))
            elif '우수함 ' in val[5]:
                rating = decimal.Decimal(val[5].replace('우수함 ', ''))
            else:
                rating = decimal.Decimal(val[5])
            #rating = decimal.Decimal(val[5][-3:])
            thumbnail = val[7]
            checkin = val[10]
            checkout = val[11]
            rds_conn.execute(f"""
                        INSERT INTO trippers.accomodation (checkin_date, checkout_date, link, location, name, price, rating, thumbnail, city_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                        (checkin, checkout, link, location, name, pricing, rating, thumbnail, city))
            rds_conn.execute("COMMIT;")  
    except Exception as e:
        logging.error(f"error occured while loading data", e)
        rds_conn.execute("ROLLBACK;")
        raise
    finally:
        rds_conn.close()
  
    

with DAG(
    dag_id = 'redshift_to_mysql_hotel',
    default_args=default_args,
    schedule_interval="@once"
) as dag:
    #redshift_data = extract_redshift_data()
    redshift_to_rds_hotellist = move_redshift_to_rds()

    #redshift_data >> redshift_to_rds_hotellist
    