from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from airflow.operators.postgres_operator import PostgresOperator
import pandas as pd
import json
from scripts.clean_raw_data.clean_raw_data import clean_raw_data
from scripts.clean_raw_data.create_dims_query import create_dims_query
from scripts.clean_raw_data.clean_raw_data import RAW_REPORTS_CONFIG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def json_to_pd_df(raw_data):
    raw_data = '{ "data": ' + raw_data[0][0] + "}"
    print(raw_data)
    dict_raw = json.loads(raw_data)
    df = pd.DataFrame.from_records(dict_raw["data"])
    return df


def clean_data(**kwargs):
    ti = kwargs['ti']
    dim_tables_names = ti.xcom_pull(
        task_ids='read_params_task', key='return_value')[2]
    report_type = ti.xcom_pull(
        task_ids='read_params_task', key='return_value')[0].split("_")[2]
    raw_data = ti.xcom_pull(
        task_ids='query_json_data_task', key="return_value")
    df = json_to_pd_df(raw_data)
    cleaned_df = clean_raw_data(df, report_type, ti)

    cleaned_data = cleaned_df.to_json(orient="records").replace(
        "/", "").replace("\\", "").replace("'", "")
    dims_query = create_dims_query(cleaned_df, dim_tables_names)

    ti.xcom_push(key='insert_dims_query', value=dims_query)
    ti.xcom_push(key='cleaned_data', value=cleaned_data)


def read_params(**context):
    table = context['params']['table']
    period = context['params']['period']
    report_config = RAW_REPORTS_CONFIG[table.split("_")[2]]
    return [table, int(period), report_config["dim_tables_names"], report_config["facts_table_name"]]


with DAG(
    'clean_raw_data',
    default_args=default_args,
    description='Read json from the raw level, clean and push data to the cleaned level',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['Clean'],
    catchup=False,
    params={
        "table": Param("", type="string"),
        "period": Param("", type="string")
    }

) as dag:
    read_params_task = PythonOperator(
        task_id='read_params_task', python_callable=read_params, provide_context=True, dag=dag)
    query_json_data_task = PostgresOperator(
        task_id='query_json_data_task',
        postgres_conn_id='postgres',
        sql="""
            SELECT data FROM {{ ti.xcom_pull(task_ids='read_params_task', key='return_value')[0] }} WHERE period = {{ ti.xcom_pull(task_ids='read_params_task', key='return_value')[1]}}
        """
    )
    clean_data_task = PythonOperator(
        task_id='clean_data_task', python_callable=clean_data, provide_context=True, dag=dag)
    trigger_clean_dag_task = TriggerDagRunOperator(
        task_id="trigger_insert_dag_task",
        trigger_dag_id="insert_cleaned_data",
        conf={
            "period": "{{ ti.xcom_pull(task_ids='read_params_task', key='return_value')[1] }}",
            "facts_table": "{{ ti.xcom_pull(task_ids='read_params_task', key='return_value')[3] }}",
            "dims_tables": "{{ ti.xcom_pull(task_ids='read_params_task', key='return_value')[2] }}",
            "insert_dims_query": "{{ ti.xcom_pull(task_ids='clean_data_task', key='insert_dims_query') }}",
            "cleaned_data": "{{ ti.xcom_pull(task_ids='clean_data_task', key='cleaned_data') }}"
        },
        dag=dag
    )

read_params_task >> query_json_data_task >> clean_data_task >> trigger_clean_dag_task
