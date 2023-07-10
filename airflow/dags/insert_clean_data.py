from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from airflow.operators.postgres_operator import PostgresOperator
from scripts.clean_raw_data.create_insert_data_query import create_insert_data_query
import pandas as pd
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def read_params(**context):
    insert_dims_query = context['params']['insert_dims_query']
    cleaned_data = context['params']['cleaned_data']
    period = context['params']['period']
    facts_table = context['params']['facts_table']
    dims_tables = context['params']['dims_tables']
    return [insert_dims_query, cleaned_data, period, facts_table, dims_tables]


def push_params_to_xcom(**kwargs):
    params = kwargs['ti'].xcom_pull(
        task_ids='read_params_task', key='return_value')
    kwargs['ti'].xcom_push(key="insert_dims_query", value=params[0])
    kwargs['ti'].xcom_push(key="cleaned_data", value=params[1])
    kwargs['ti'].xcom_push(key="period", value=params[2])
    kwargs['ti'].xcom_push(key="facts_table", value=params[3])
    kwargs['ti'].xcom_push(key="dims_tables", value=params[4])


def read_xcom_str_as_list(str):
    return json.loads('{ "data": ' + f"{str}".replace("'", '"') + "}")['data']


def create_insert_data_query_main(**kwargs):
    data = kwargs['ti'].xcom_pull(
        task_ids='push_params_to_xcom_task', key='cleaned_data')
    dims = kwargs['ti'].xcom_pull(
        task_ids='push_params_to_xcom_task', key='dims_tables')
    dims = read_xcom_str_as_list(dims)
    period = kwargs['ti'].xcom_pull(
        task_ids='push_params_to_xcom_task', key='period')
    facts_table = kwargs['ti'].xcom_pull(
        task_ids='push_params_to_xcom_task', key='facts_table')

    return create_insert_data_query(
        data, dims, period, facts_table)


with DAG(
    'insert_cleaned_data',
    default_args=default_args,
    description='Insert cleaned data dims and facts',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['Insert'],
    catchup=False,
    params={
        "period": Param("", type="string"),
        "facts_table": Param("", type="string"),
        "dims_tables": Param("", type="string"),
        "insert_dims_query": Param("", type="string"),
        "cleaned_data": Param("", type="string")
    }

) as dag:
    read_params_task = PythonOperator(
        task_id='read_params_task', python_callable=read_params, provide_context=True, dag=dag)
    push_params_to_xcom_task = PythonOperator(
        task_id='push_params_to_xcom_task', python_callable=push_params_to_xcom, provide_context=True, dag=dag)
    create_insert_data_query_task = PythonOperator(
        task_id='create_insert_data_query_task', python_callable=create_insert_data_query_main, provide_context=True, dag=dag)

    insert_dims_task = PostgresOperator(
        task_id='insert_dims_task',
        postgres_conn_id='postgres',
        sql="""{{ ti.xcom_pull(task_ids='push_params_to_xcom_task', key='insert_dims_query') }}"""
    )
    insert_data_task = PostgresOperator(
        task_id='insert_data_task',
        postgres_conn_id='postgres',
        sql="""{{ ti.xcom_pull(task_ids='create_insert_data_query_task', key='return_value') }}"""
    )
read_params_task >> push_params_to_xcom_task >> create_insert_data_query_task >> insert_dims_task >> insert_data_task
