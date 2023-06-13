import pandas as pd
from google.cloud import bigquery
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param


def check_raw_data_completeness():
    client = bigquery.Client()
    period = get_unfinished_period()
    print(f"Least unfinished period is: {period}")
    CHECK_DATA_QUERY = (
        f"SELECT * FROM `uber-etl-386321.raw_json_data.metadata` WHERE period={period}"
    )
    query_job = client.query(CHECK_DATA_QUERY)
    rows = query_job.result()
    rows_count = 0
    for row in rows:
        rows_count += 1
    if rows_count == 4:
        print(f"Period {period} raw json data is ready for processing")
        return period
    print(f"Period {period} raw json data is not ready for processing yet")
    return False


def get_unfinished_period():
    client = bigquery.Client()
    GET_LAST_PERIOD_QUERY = (
        f"SELECT filename, MAX(period) AS period FROM `uber-etl-386321.column_metadata.metadata` GROUP BY filename"
    )
    query_job = client.query(GET_LAST_PERIOD_QUERY)
    rows = query_job.result()
    periods = []
    for row in rows:
        periods.append(row.period)
    if_equal = all(period == periods[0] for period in periods)
    if (if_equal):
        return increase_period_by_one_week(periods[0])
    return min(periods)


def increase_period_by_one_week(period):
    period = f"{period}"
    year = int(period[:4])
    qtr = int(period[4])
    week = int(period[5:])
    if week < 13:
        week += 1
        if week < 10:
            week = f"0{week}"
    elif qtr < 4:
        qtr += 1
        week = "01"
    else:
        year += 1
        qtr = 1
        week = "01"
    return int(f"{year}{qtr}{week}")


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
    check_raw_data_completeness_task = PythonOperator(
        task_id='check_raw_data_completeness_task', python_callable=check_raw_data_completeness, dag=dag)
    read_data_from_raw_level_task = PythonOperator(
        task_id='read_data_from_raw_level_task', python_callable=read_data_from_raw_level, dag=dag)
    transform_to_columns_task = PythonOperator(
        task_id='transform_to_columns_task', python_callable=transform_to_columns, dag=dag)
    load_columns_to_bq_task = PythonOperator(
        task_id='load_columns_to_bq_task', python_callable=load_columns_to_bq, dag=dag)

check_raw_data_completeness_task >> read_data_from_raw_level_task >> transform_to_columns_task >> load_columns_to_bq_task
