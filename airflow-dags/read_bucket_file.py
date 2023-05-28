import pandas as pd
from google.cloud import storage
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def write_read():
    bucket_name = "healthcare-data-bucket"
    blob_name = "why-us_q1-w1-2023.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("r") as f:
        print(f.read())


with DAG(
    'read_bucket_file',
    default_args=default_args,
    description='Read CSV file inside the bucket',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['Read'],
) as dag:
    read_csv_task = PythonOperator(
        task_id='read_csv_task', python_callable=write_read, dag=dag)

read_csv_task
