from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def increase_period_by_one_week(filename, period):
    period = f"{period}"
    year = int(period[:4])
    qtr = int(period[4])
    week = int(period[5:])
    if week < 13:
        week += 1
    elif qtr < 4:
        qtr += 1
        week = 1
    else:
        year += 1
        qtr = 1
        week = 1
    return (f"{filename}_q{qtr}-w{week}-{year}.csv")


def check_new_files(**kwargs):
    file_names = kwargs['ti'].xcom_pull(
        task_ids='get_bucket_file_names_task', key="return_value")
    last_loaded_jsons_metadata = kwargs['ti'].xcom_pull(
        task_ids='get_last_loaded_jsons_task', key="return_value")

    for json_metadata in last_loaded_jsons_metadata:
        search_filename = increase_period_by_one_week(
            json_metadata[0], json_metadata[1])
        if (search_filename in file_names):
            return search_filename


with DAG(
    'check_bucket',
    default_args=default_args,
    description='Check if new file was uploaded into GCP Bucket',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['Check'],
    catchup=False,

) as dag:
    get_last_loaded_jsons_task = PostgresOperator(
        task_id='get_last_loaded_jsons_task',
        postgres_conn_id='postgres',
        sql="""
            SELECT filename, MAX(period) FROM raw_json_metadata GROUP BY filename
        """
    )
    get_bucket_file_names_task = GoogleCloudStorageListOperator(
        task_id='get_bucket_file_names_task',
        bucket='healthcare-raw-json-files',
        prefix='',
        delimiter='.csv',
        gcp_conn_id="gcp"
    )
    check_new_files_task = PythonOperator(
        task_id='check_new_files_task', python_callable=check_new_files, dag=dag
    )
    trigger_task = TriggerDagRunOperator(
        task_id="trigger_task",
        trigger_dag_id="read_bucket_file",
        conf={
            "filename": "{{ ti.xcom_pull(task_ids='check_new_files_task', key='return_value') }}"},
        dag=dag
    )

get_last_loaded_jsons_task >> get_bucket_file_names_task >> check_new_files_task >> trigger_task
