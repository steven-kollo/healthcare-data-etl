import pandas as pd
from io import BytesIO
from google.cloud import storage
from google.cloud import bigquery
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param


def read_data_from_raw_level():
    pass


def transform_to_columns():
    pass


def load_columns_to_bq():
    pass


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'get_columns_from_raw_level',
    default_args=default_args,
    description='Read json file the BigQuery raw level and push columns to the column level',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['Clean'],
    catchup=False,
    params={
        "filename": Param("", type="string")
    }

) as dag:
    read_data_from_raw_level_task = PythonOperator(
        task_id='read_data_from_raw_level_task', python_callable=read_data_from_raw_level, dag=dag)
    transform_to_columns_task = PythonOperator(
        task_id='transform_to_columns_task', python_callable=transform_to_columns, dag=dag)
    load_columns_to_bq_task = PythonOperator(
        task_id='load_columns_to_bq_task', python_callable=load_columns_to_bq, dag=dag)

read_data_from_raw_level_task >> transform_to_columns_task >> load_columns_to_bq_task
