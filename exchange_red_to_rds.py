import logging
import psycopg2
import pymysql
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='aws_red')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def get_RDS_connection(autocommit=True):
    rds_hook = MySqlHook(mysql_conn_id='aws_rds')
    conn = rds_hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# 이전 dag의 실행이 완료된 후에 실행되는 함수
def exchange_red_to_rds(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    rds_schema = context["params"]["rds_schema"]
    rds_table = context["params"]["rds_table"]
    # redshift 에서 데이터 가져오기
    redshift_cursor = get_Redshift_connection()
    try:
        redshift_cursor.execute(f"""SELECT * FROM {schema}.{table} ORDER BY 검색_시간 DESC LIMIT 3;""")
        data = redshift_cursor.fetchall()  # 쿼리 결과 가져오기
        logging.info("extract done")
        # RDS에 연결 설정하기
        rds_cursor = get_RDS_connection()
        try:
            # 가져온 데이터를 RDS에 적재하는 로직 추가
            rds_cursor.execute(f"TRUNCATE TABLE {rds_schema}.{rds_table};")
            for row in data:
                buy_rate = row[3]
                cur_unit = row[1]
                sell_rate = row[2]
                if cur_unit == 'EUR':
                    country_id = 4
                elif cur_unit == 'JPY(100)':
                    country_id = 2
                elif cur_unit == 'USD':
                    country_id = 3
                else:
                    country_id = 1
                created_at = row[0]
                rds_cursor.execute(f"""
                                   INSERT INTO {rds_schema}.{rds_table} 
                                   (buy_rate, cur_unit, sell_rate, country_id, created_at)
                                   VALUES (%s, %s, %s, %s, %s);""",
                                   (buy_rate, cur_unit, sell_rate, country_id, created_at))
            rds_cursor.execute('commit;')
        except (Exception, pymysql.DatabaseError) as error:
            print('error')
            rds_cursor.execute("ROLLBACK;")
            raise
        
        finally:
            rds_cursor.close()  # RDS 커서 닫기
    except (Exception, psycopg2.DatabaseError) as error:
        print('error')
        redshift_cursor.execute("ROLLBACK;")
        raise
    
    finally:
        redshift_cursor.close()  # Redshift 커서 닫기
    
    logging.info("load done")
    

default_args = {
    'owner': 'jaewoo',
    'provide_context': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}
# 이전 dag와 exchange_red_to_rds dag를 정의하는 부분
with DAG(
    dag_id='exchange_red_to_rds',
    default_args=default_args,
    start_date=datetime(2024, 3, 1),
    catchup = False,
    schedule_interval=None
) as dag:
    
    # exchange_red_to_rds dag의 실행 함수를 호출하는 task
    exchange_red_to_rds_task = PythonOperator(
        task_id='exchange_red_to_rds',
        python_callable=exchange_red_to_rds,
        params = {
        'schema': 'public',
        'table': 'exchange_rate',
        'rds_schema': 'trippers',
        'rds_table': 'exchange_rate'
        },
        dag = dag
    )

    # task 간의 의존성 설정
exchange_red_to_rds_task
