from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def return_names_from_xCom(ti):
    file_names = ti.xcom_pull(task_ids=['get_file_names'])
    if not file_names:
        raise ValueError('No file names currently stored in XComs.')

    return file_names


with DAG(
    'read_bucket_file_names',
    default_args=default_args,
    description='Get names of CSV files inside the bucket',
    tags=['read_names'],
) as dag:
    get_file_names = GCSListObjectsOperator(
        task_id='get_file_names',
        bucket='healthcare-data-bucket',
        prefix=None,
        delimiter='.csv',
        dag=dag
    )

    return_file_names = PythonOperator(
        task_id='print_file_names', python_callable=return_names_from_xCom, dag=dag)


get_file_names >> return_file_names
