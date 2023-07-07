from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pandas as pd
from io import BytesIO
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def parse_period(file_name):
    period = file_name.split('.')[0].split('_')[1].split('-')
    year = period[2]
    qtr = period[0].replace('q', '')
    week = period[1].replace('w', '')
    if (len(week) == 1):
        week = "0" + week

    return {
        "year": int(year),
        "qtr": int(qtr),
        "week": int(week),
        "period": int(f"{year}{qtr}{week}")
    }


def parse_file_name(**context):
    file_name = context['params']['filename']
    file_name.split('.')
    report_type = file_name.split('.')[0].split('_')[0]
    source_format = file_name.split('.')[1]
    period = parse_period(file_name)

    metadata = {
        'file_name': file_name,
        'report_type': report_type,
        'source_format': source_format,
        'week': period["week"],
        'qtr': period["qtr"],
        'year': period["year"],
        'period': period["period"]
    }

    return metadata


def read_csv_file(**kwargs):
    metadata = kwargs['ti'].xcom_pull(
        task_ids='parse_file_name_task', key='return_value')
    column_names = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L",
                    "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
                    "AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ", "AK", "AL",
                    "AM", "AN", "AO", "AP", "AQ", "AR", "AS", "AT", "AU", "AV", "AW", "AX", "AY", "AZ"]
    df = pd.read_csv("/opt/airflow/dags/"+metadata["file_name"], sep=',',
                     header=None, names=column_names).dropna(axis=1, how='all')
    print(df)
    metadata_json_to_load = {
        "db": "raw_json_data",
        "source_format": metadata["source_format"],
        "report_type": metadata["report_type"],
        "week": metadata["week"],
        "qtr": metadata["qtr"],
        "year": metadata["year"]
    }
    kwargs['ti'].xcom_push(key='metadata', value=metadata_json_to_load)
    json_string = df.to_json(orient="records").replace(
        "/", "").replace("\\", "").replace("'", "")
    kwargs['ti'].xcom_push(key='data', value=json_string)


with DAG(
    'read_bucket_file',
    default_args=default_args,
    description='Read CSV file inside the bucket and push data as json to the DB raw level tables',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['Read'],
    catchup=False,
    params={
        "filename": Param("", type="string")
    }
) as dag:
    parse_file_name_task = PythonOperator(
        task_id='parse_file_name_task', python_callable=parse_file_name, provide_context=True, dag=dag
    )
    read_bucket_file_task = GoogleCloudStorageDownloadOperator(
        task_id='read_bucket_file_task',
        bucket='healthcare-raw-json-files',
        object_name="{{ ti.xcom_pull(task_ids='parse_file_name_task', key='return_value')['file_name'] }}",
        filename="/opt/airflow/dags/" +
        "{{ ti.xcom_pull(task_ids='parse_file_name_task', key='return_value')['file_name'] }}",
        gcp_conn_id="gcp"
    )
    read_csv_task = PythonOperator(
        task_id='read_csv_task', python_callable=read_csv_file, provide_context=True, dag=dag
    )
    insert_data_task = PostgresOperator(
        task_id='get_last_loaded_jsons_task',
        postgres_conn_id='postgres',
        sql="""
            INSERT INTO raw_json_{{ ti.xcom_pull(task_ids='parse_file_name_task', key='return_value')['report_type'] }} (period, data)
            VALUES ( {{ ti.xcom_pull(task_ids='parse_file_name_task', key='return_value')['period'] }}, '{{ ti.xcom_pull(task_ids='read_csv_task', key='data') }}');
        """
    )
    remove_temp_file_task = BashOperator(
        task_id="remove_temp_file_task",
        bash_command="cd /opt/airflow/dags/; rm {{ ti.xcom_pull(task_ids='parse_file_name_task', key='return_value')['file_name'] }}"
    )
    update_raw_metadata_task = PostgresOperator(
        task_id='update_raw_metadata_task',
        postgres_conn_id='postgres',
        sql="""
        INSERT INTO raw_json_metadata (period, filename)
        VALUES ( {{ ti.xcom_pull(task_ids='parse_file_name_task', key='return_value')['period'] }}, '{{ ti.xcom_pull(task_ids='parse_file_name_task', key='return_value')['report_type'] }}');
    """
    )
    trigger_clean_dag_task = TriggerDagRunOperator(
        task_id="trigger_clean_dag_task",
        trigger_dag_id="clean_raw_data",
        conf={
            "table": "raw_json_{{ ti.xcom_pull(task_ids='parse_file_name_task', key='return_value')['report_type'] }}",
            "period": "{{ ti.xcom_pull(task_ids='parse_file_name_task', key='return_value')['period'] }}"
        },
        dag=dag
    )

parse_file_name_task >> read_bucket_file_task >> read_csv_task >> insert_data_task >> remove_temp_file_task >> update_raw_metadata_task >> trigger_clean_dag_task
