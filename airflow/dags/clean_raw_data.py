# from airflow_includes.modules.clean_raw.clean_raw import clean_df
# from airflow_includes.modules.clean_raw.insert_cleaned import insert_clean_data
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from airflow.operators.postgres_operator import PostgresOperator
import pandas as pd
import json

# client = bigquery.Client()
# DATASET_NAME_WHY_US = "whyus"  # TODO REPLACE WITH ENV VAR
# REQUIRED_FIELDS_WHY_US = ["branch", "whyus",
#                           "name"]

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
    print(df)
    print("========")
    return df


def clean_data(**kwargs):
    period = kwargs['ti'].xcom_pull(
        task_ids='read_params_task', key='return_value')[1]
    report_type = kwargs['ti'].xcom_pull(
        task_ids='read_params_task', key='return_value')[0].split("_")[2]
    raw_data = kwargs['ti'].xcom_pull(
        task_ids='query_json_data_task', key="return_value")

    df = json_to_pd_df(raw_data)
    df = df[df['G'].notnull()]
    headers = df.iloc[0].tolist()
    print(df)
    print(headers)
    print(report_type)
    print(period)


def read_params(**context):
    table = "raw_json_whyus"
    period = "2023102"
    # table = context['params']['table']
    # period = context['params']['period']
    print(table)
    print(period)
    return [table, int(period)]
    # period = get_unfinished_period()
    # print(f"Least unfinished period is: {period}")
    # CHECK_DATA_QUERY = (
    #     f"SELECT * FROM `raw_json_data.metadata` WHERE period={period}"
    # )
    # query_job = client.query(CHECK_DATA_QUERY)
    # rows = query_job.result()
    # rows_count = 0
    # for row in rows:
    #     rows_count += 1
    # if rows_count == 4:
    #     print(f"Period {period} raw json data is ready for processing")
    #     return period
    # print(f"Period {period} raw json data is not ready for processing yet")
    return False


# def get_unfinished_period():
#     GET_LAST_PERIOD_QUERY = (
#         f"SELECT filename, MAX(period) AS period FROM `column_metadata.metadata` GROUP BY filename"
#     )
#     query_job = client.query(GET_LAST_PERIOD_QUERY)
#     rows = query_job.result()
#     periods = []
#     for row in rows:
#         periods.append(row.period)
#     if_equal = all(period == periods[0] for period in periods)
#     if (if_equal):
#         return increase_period_by_one_week(periods[0])
#     return min(periods)


# def increase_period_by_one_week(period):
#     period = f"{period}"
#     year = int(period[:4])
#     qtr = int(period[4])
#     week = int(period[5:])
#     if week < 13:
#         week += 1
#         if week < 10:
#             week = f"0{week}"
#     elif qtr < 4:
#         qtr += 1
#         week = "01"
#     else:
#         year += 1
#         qtr = 1
#         week = "01"
#     return int(f"{year}{qtr}{week}")


# def continue_if_complete_data(**kwargs):
#     return kwargs['ti'].xcom_pull(
#         task_ids='check_raw_data_completeness_task')


# def query_raw_data_by_filename(filename, period):
#     READ_RAW_JSON_DATA_QUERY = (
#         f"SELECT * FROM `raw_json_data.{filename}` WHERE period={period}"
#     )
#     query_job = client.query(READ_RAW_JSON_DATA_QUERY)
#     rows = query_job.result()
#     for row in rows:
#         return row.data


# def read_data_from_raw_level(**kwargs):
#     period = kwargs['ti'].xcom_pull(
#         task_ids='check_raw_data_completeness_task')
#     FILENAMES = ["salesbybrand", "whyus",
#                  "transactions", "newpatients"]
#     for filename in FILENAMES:
#         data = query_raw_data_by_filename(filename, period)
#         kwargs['ti'].xcom_push(key=filename, value=data)


# def whyus_clean(**kwargs):
#     period = kwargs['ti'].xcom_pull(
#         task_ids='check_raw_data_completeness_task')
#     data = kwargs['ti'].xcom_pull(
#         task_ids='read_data_from_raw_level_task', key='whyus')

#     df = pd.read_json(data)
#     df = df[df['G'].notnull()]
#     headers = df.iloc[0].tolist()

#     cleaned_df = clean_df(df, headers, REQUIRED_FIELDS_WHY_US,
#                           DATASET_NAME_WHY_US)
#     print(cleaned_df)
#     insert_clean_data(cleaned_df, DATASET_NAME_WHY_US, period)


# def transactions_clean(**kwargs):
#     # TODO read from xComs and push to xComs by cleaned columns
#     data = kwargs['ti'].xcom_pull(
#         task_ids='read_data_from_raw_level_task', key='transactions')


# def newpatients_clean(**kwargs):
#     # TODO read from xComs and push to xComs by cleaned columns
#     data = kwargs['ti'].xcom_pull(
#         task_ids='read_data_from_raw_level_task', key='newpatients')


# def salesbybrand_clean(**kwargs):
#     # TODO read from xComs and push to xComs by cleaned columns
#     data = kwargs['ti'].xcom_pull(
#         task_ids='read_data_from_raw_level_task', key='salesbybrand')


# def load_clean_data_to_bq():
#     # TODO read from xComs and push to BQ cleaned columns
#     # insert_clean_data(cleaned_df, DATASET_NAME_WHY_US, period)
#     pass


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
