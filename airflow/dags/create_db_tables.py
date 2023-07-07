from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'create_db_tables',
    default_args=default_args,
    description='Create DB tables if not exist',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['CreateTables'],
    catchup=False,
) as dag:
    create_raw_json_metadata_table_task = PostgresOperator(
        task_id='create_raw_json_metadata_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS raw_json_metadata (
                period INT NOT NULL, 
                filename CHARACTER VARYING
            )
        """
    )

#     2022413	transactions
# 2022413	whyUs
# 2022413	newPatients
# 2022413	salesByBrand
    create_raw_json_transactions_table_task = PostgresOperator(
        task_id='create_raw_json_transactions_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS raw_json_transactions (
                period INT NOT NULL, 
                data CHARACTER VARYING,
                PRIMARY KEY (period)
            )
        """
    )
    create_raw_json_whyus_table_task = PostgresOperator(
        task_id='create_raw_json_whyus_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS raw_json_whyus (
                period INT NOT NULL,
                data CHARACTER VARYING,
                PRIMARY KEY (period)
            )
        """
    )
    create_raw_json_salesbybrand_table_task = PostgresOperator(
        task_id='create_raw_json_salesbybrand_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS raw_json_salesbybrand (
                period INT NOT NULL,
                data CHARACTER VARYING,
                PRIMARY KEY (period)
            )
        """
    )
    create_raw_json_newpatients_table_task = PostgresOperator(
        task_id='create_raw_json_newpatients_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS raw_json_newpatients (
                period INT NOT NULL,
                data CHARACTER VARYING,
                PRIMARY KEY (period)
            )
        """
    )

create_raw_json_metadata_table_task >> create_raw_json_transactions_table_task >> create_raw_json_whyus_table_task >> create_raw_json_salesbybrand_table_task >> create_raw_json_newpatients_table_task
