import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "../scripts"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from extract import extract_weather
from transform import transform_weather
from load import load_weather

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def run_etl():
    raw = extract_weather()
    transformed = transform_weather(raw)
    load_weather(transformed)

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline to fetch and store weather data',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl
    )

    etl_task
