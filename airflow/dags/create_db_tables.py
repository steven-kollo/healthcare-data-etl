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
    create_dim_branch_table_task = PostgresOperator(
        task_id='create_dim_branch_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS branch (
                branchId SERIAL PRIMARY KEY,
                branch CHARACTER VARYING,
                UNIQUE(branch)
            )
        """
    )
    create_dim_product_table_task = PostgresOperator(
        task_id='create_dim_product_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS product (
                productId SERIAL PRIMARY KEY,
                product CHARACTER VARYING,
                UNIQUE(product)
            )
        """
    )
    create_dim_whyUsClientName_table_task = PostgresOperator(
        task_id='create_dim_whyUsClientName_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS whyUsClientName (
                whyUsClientNameId SERIAL PRIMARY KEY,
                whyUsClientName CHARACTER VARYING,
                UNIQUE(whyUsClientName)
            )
        """
    )
    create_dim_whyUs_table_task = PostgresOperator(
        task_id='create_dim_whyUs_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS whyUs (
                whyUsId SERIAL PRIMARY KEY,
                whyUs CHARACTER VARYING,
                UNIQUE(whyUs)
            )
        """
    )
    create_fact_survey_table_task = PostgresOperator(
        task_id='create_fact_survey_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS survey (
                surveyId SERIAL PRIMARY KEY,
                period INT NOT NULL,
                branchId INT NOT NULL,
                whyUsClientNameId INT NOT NULL,
                whyUsId INT NOT NULL
            )
        """
    )

create_raw_json_metadata_table_task >> create_raw_json_transactions_table_task >> create_raw_json_whyus_table_task >> create_raw_json_salesbybrand_table_task >> create_raw_json_newpatients_table_task
