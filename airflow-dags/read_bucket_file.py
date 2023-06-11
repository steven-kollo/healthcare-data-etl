import pandas as pd
from io import BytesIO
from google.cloud import storage
from google.cloud import bigquery
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def parse_file_name(**kwargs):
    dag_run = kwargs.get('dag_run')
    file_name = f"{dag_run.conf['filename']}.csv"
    file_name.split('.')
    report_type = file_name.split('.')[0].split('_')[0]
    source_format = file_name.split('.')[1]
    period = file_name.split('.')[0].split('_')[1].split('-')
    metadata = {
        'file_name': file_name,
        'report_type': report_type,
        'source_format': source_format,
        'week': period[1].replace('w', ''),
        'qtr': period[0].replace('q', ''),
        'year': period[2]
    }
    kwargs['ti'].xcom_push(key='metadata', value=metadata)


def read_csv_file(**kwargs):
    metadata = kwargs['ti'].xcom_pull(
        task_ids='parse_file_name_task', key='metadata')
    bucket_name = "healthcare-data-bucket"
    blob_name = metadata['file_name']
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    column_names = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L",
                    "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"]
    binary_stream = blob.download_as_string()
    df = pd.read_csv(BytesIO(binary_stream), sep=',',
                     header=None, names=column_names).dropna(axis=1, how='all')
    json_to_load = {
        "db": "raw_json_data",
        "source_format": "csv",
        "report_type": "why_us",
        "week": 1,
        "qtr": 1,
        "year": 2023,
        "data": df.to_json(orient="records").replace("/", "").replace("\\", "")
    }
    json_to_load['db']
    kwargs['ti'].xcom_push(key='json', value=json_to_load)


def load_raw_to_bq(**kwargs):
    client = bigquery.Client()
    json = kwargs['ti'].xcom_pull(
        task_ids='read_csv_task', key='json_to_load')

    INSERT_ROWS_QUERY = (
        f"INSERT {json['db']}.{json['report_type']} VALUES "
        f"('{json['week']}-{json['qtr']}-{json['year']}', '{json['data']}');"
    )
    query_job = client.query(INSERT_ROWS_QUERY)  # API request
    result = query_job.result()
    return result


with DAG(
    'read_bucket_file',
    default_args=default_args,
    description='Read CSV file inside the bucket',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['Read'],
    catchup=False,
    params={
        "filename": Param("", type="string")
    }

) as dag:
    parse_file_name_task = PythonOperator(
        task_id='parse_file_name_task', python_callable=parse_file_name, dag=dag)
    read_csv_task = PythonOperator(
        task_id='read_csv_task', python_callable=read_csv_file, dag=dag)
    load_raw_to_bq_task = PythonOperator(
        task_id='load_raw_to_bq_task', python_callable=load_raw_to_bq, dag=dag)

parse_file_name_task >> read_csv_task >> load_raw_to_bq_task
