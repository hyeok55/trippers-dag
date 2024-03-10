from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 20, 15, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_data_to_redshift():
    try:
        # Redshift 연결 설정
        redshift_hook = PostgresHook(postgres_conn_id='redshift_conn_id')
        
        # 쿼리 작성
        sql_query = """
INSERT INTO analytics.flight_offers_round (
    number_Of_Bookable_Seats,
    price_currency,
    departure_city_id,
    departure_terminal,
    departure_datetime,
    arrival_city_id,
    arrival_terminal,
    arrival_datetime,
    carrier_code,
    duration,
    return_departure_city_id,
    return_departure_terminal,
    return_departure_datetime,
    return_arrival_city_id,
    return_arrival_terminal,
    return_arrival_datetime,
    return_duration,
    cabin_class,
    return_cabin_class,
    return_carrier_code,
    price
)
SELECT
    number_Of_Bookable_Seats,
    price_currency,
    departure_city_id,
    departure_terminal,
    departure_datetime,
    arrival_city_id,
    arrival_terminal,
    arrival_datetime,
    carrier_code,
    duration,
    return_departure_city_id,
    return_departure_terminal,
    return_departure_datetime,
    return_arrival_city_id,
    return_arrival_terminal,
    return_arrival_datetime,
    return_duration,
    cabin_class,
    return_cabin_class,
    return_carrier_code,
    ROUND(price * rr.사실_때, 0) AS price
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY
                            departure_city_id, departure_terminal, departure_datetime,
                            arrival_city_id, arrival_terminal, arrival_datetime,return_departure_city_id, return_departure_terminal, return_departure_datetime,
                            return_arrival_city_id, return_arrival_terminal, return_arrival_datetime ORDER BY price) AS row_num,
        (SELECT 사실_때 FROM public.exchange_rate WHERE cur_unit = 'EUR' ORDER BY 검색_시간 DESC LIMIT 1) AS 사실_때
    FROM raw_data.flight_offers_round
    WHERE arrival_datetime >= CURRENT_DATE
) AS rr
WHERE rr.row_num = 1;
        """

        # 쿼리 실행
        redshift_hook.run(sql_query)
        print("Data loaded successfully to Redshift.")
    except Exception as e:
        print(f"Error loading data to Redshift: {str(e)}")
        raise

with DAG('round_rds_to_rds', default_args=default_args, description='Load filtered data to Redshift', schedule_interval=timedelta(days=1), catchup=False) as dag:
    
    load_data_task = PythonOperator(
        task_id='load_data_to_redshift',
        python_callable=load_data_to_redshift,
        dag=dag
    )
    
    load_data_task
