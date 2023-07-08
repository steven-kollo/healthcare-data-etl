from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from airflow.operators.postgres_operator import PostgresOperator
import pandas as pd
import json
from scripts.clean_raw_data.df_to_required_columns import to_required_columns


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


def clean_df(report_type, df, period):
    df_required_columns = to_required_columns(df, report_type)
    # cleaned_df = CLEAN_DATA_SCRIPTS[report_type]["script"](df)
    # print(df_required_columns)


def clean_data(**kwargs):
    period = kwargs['ti'].xcom_pull(
        task_ids='read_params_task', key='return_value')[1]
    report_type = kwargs['ti'].xcom_pull(
        task_ids='read_params_task', key='return_value')[0].split("_")[2]
    raw_data = kwargs['ti'].xcom_pull(
        task_ids='query_json_data_task', key="return_value")
    df = json_to_pd_df(raw_data)

    clean_df(report_type, df, period)


def read_params(**context):
    table = "raw_json_salesbybrand"
    period = "2023101"
    # table = context['params']['table']
    # period = context['params']['period']
    return [table, int(period)]


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
    # query_dims_task = PostgresOperator(
    #     task_id='query_dims_task',
    #     postgres_conn_id='postgres',
    #     sql="""
    #         SELECT data FROM {{ ti.xcom_pull(task_ids='read_params_task', key='return_value')[0] }}
    #     """
    # )
    clean_data_task = PythonOperator(
        task_id='clean_data_task', python_callable=clean_data, provide_context=True, dag=dag)
    # continue_if_complete_data_task = ShortCircuitOperator(
    #     task_id="continue_if_complete_data_task",
    #     provide_context=True,
    #     python_callable=continue_if_complete_data,
    #     op_kwargs={},
    # )
#     read_data_from_raw_level_task = PythonOperator(
#         task_id='read_data_from_raw_level_task', python_callable=read_data_from_raw_level, dag=dag)

#     whyus_clean_task = PythonOperator(
#         task_id='whyus_clean_task', python_callable=whyus_clean, dag=dag)
#     transactions_clean_task = PythonOperator(
#         task_id='transactions_clean_task', python_callable=transactions_clean, dag=dag)
#     newpatients_clean_task = PythonOperator(
#         task_id='newpatients_clean_task', python_callable=newpatients_clean, dag=dag)
#     salesbybrand_clean_task = PythonOperator(
#         task_id='salesbybrand_clean_task', python_callable=salesbybrand_clean, dag=dag)
read_params_task >> query_json_data_task >> clean_data_task
# check_raw_data_completeness_task >> continue_if_complete_data_task >> read_data_from_raw_level_task >> [
#     whyus_clean_task, transactions_clean_task, newpatients_clean_task, salesbybrand_clean_task]
