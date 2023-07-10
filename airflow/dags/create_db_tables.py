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
    # RAW
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
    # GENERAL
    create_dim_client_table_task = PostgresOperator(
        task_id='create_dim_client_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS client (
                clientId SERIAL PRIMARY KEY,
                client CHARACTER VARYING,
                pxPublicId INT,
                UNIQUE(client)
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
    # SURVEY
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
                period INT,
                branchId INT,
                whyUsClientNameId INT,
                whyUsId INT
            )
        """
    )
    # NEW PATIENTS
    create_dim_staffTitle_table_task = PostgresOperator(
        task_id='create_dim_staffTitle_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS staffTitle (
                staffTitleId SERIAL PRIMARY KEY,
                staffTitle CHARACTER VARYING,
                UNIQUE(staffTitle)
            )
        """
    )
    create_dim_pxTitle_table_task = PostgresOperator(
        task_id='create_dim_pxTitle_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS pxTitle (
                pxTitleId SERIAL PRIMARY KEY,
                pxTitle CHARACTER VARYING,
                UNIQUE(pxTitle)
            )
        """
    )
    create_dim_appType_table_task = PostgresOperator(
        task_id='create_dim_appType_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS appType (
                appTypeId SERIAL PRIMARY KEY,
                appType CHARACTER VARYING,
                UNIQUE(appType)
            )
        """
    )
    create_dim_appStatus_table_task = PostgresOperator(
        task_id='create_dim_appStatus_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS appStatus (
                appStatusId SERIAL PRIMARY KEY,
                appStatus CHARACTER VARYING,
                UNIQUE(appStatus)
            )
        """
    )
    create_fact_newPatients_table_task = PostgresOperator(
        task_id='create_fact_newPatients_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS newPatients (
                newpatientId SERIAL PRIMARY KEY,
                period INT NOT NULL,
                branchId INT,
                staffTitleId INT,
                pxPublicId INT,
                pxTitleId INT,
                bookingDate DATE,
                appStartDate DATE,
                appTypeId INT,
                appStatusId INT
            )
        """
    )
    # TRANSACTIONS
    create_dim_pxLastName_table_task = PostgresOperator(
        task_id='create_dim_pxLastName_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS pxLastName (
                pxLastNameId SERIAL PRIMARY KEY,
                pxLastName CHARACTER VARYING,
                UNIQUE(pxLastName)
            )
        """
    )
    create_dim_paymentCategory_table_task = PostgresOperator(
        task_id='create_dim_paymentCategory_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS paymentCategory (
                paymentCategoryId SERIAL PRIMARY KEY,
                paymentCategory CHARACTER VARYING,
                UNIQUE(paymentCategory)
            )
        """
    )
    create_dim_itemDesc_table_task = PostgresOperator(
        task_id='create_dim_itemDesc_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS itemDesc (
                itemDescId SERIAL PRIMARY KEY,
                itemDesc CHARACTER VARYING,
                UNIQUE(itemDesc)
            )
        """
    )
    create_fact_transactions_table_task = PostgresOperator(
        task_id='create_fact_transactions_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS transactions (
                transactionId SERIAL PRIMARY KEY,
                period INT NOT NULL,
                transDate DATE,
                pxPublicId INT,
                pxLastNameId INT,
                transPublicId INT,
                paymentCategoryId INT,
                itemDescId INT,
                salesValue FLOAT,
                discount FLOAT,
                netSales FLOAT,
                sales FLOAT,
                takings FLOAT,
                refunds FLOAT
            )
        """
    )
    # PRODUCT SALES
    create_dim_prodType_table_task = PostgresOperator(
        task_id='create_dim_prodType_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS prodType (
                prodTypeId SERIAL PRIMARY KEY,
                prodType CHARACTER VARYING,
                UNIQUE(prodType)
            )
        """
    )
    create_dim_brand_table_task = PostgresOperator(
        task_id='create_dim_brand_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS brand (
                brandId SERIAL PRIMARY KEY,
                brand CHARACTER VARYING,
                UNIQUE(brand)
            )
        """
    )
    create_dim_manufacturer_table_task = PostgresOperator(
        task_id='create_dim_manufacturer_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS manufacturer (
                manufacturerId SERIAL PRIMARY KEY,
                manufacturer CHARACTER VARYING,
                UNIQUE(manufacturer)
            )
        """
    )
    create_dim_prodCode_table_task = PostgresOperator(
        task_id='create_dim_prodCode_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS prodCode (
                prodCodeId SERIAL PRIMARY KEY,
                prodCode CHARACTER VARYING,
                UNIQUE(prodCode)
            )
        """
    )
    create_dim_prodName_table_task = PostgresOperator(
        task_id='create_dim_prodName_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS prodName (
                prodNameId SERIAL PRIMARY KEY,
                prodName CHARACTER VARYING,
                UNIQUE(prodName)
            )
        """
    )
    create_fact_productSales_table_task = PostgresOperator(
        task_id='create_fact_productSales_table_task',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS productSales (
                productSaleId SERIAL PRIMARY KEY,
                period INT NOT NULL,
                branchId INT,
                prodTypeId INT,
                brandId INT,
                manufacturerId INT,
                prodCodeId INT,
                prodNameId INT,
                unitPrice FLOAT,
                quantity INT,
                totalPrice FLOAT,
                discount FLOAT,
                voidRefund FLOAT,
                endPrice FLOAT
            )
        """
    )
create_raw_json_metadata_table_task >> create_raw_json_transactions_table_task >> create_raw_json_whyus_table_task >> \
    create_raw_json_salesbybrand_table_task >> create_raw_json_newpatients_table_task >> create_dim_client_table_task >> \
    create_dim_branch_table_task >> create_dim_product_table_task >> create_dim_whyUsClientName_table_task >> \
    create_dim_whyUs_table_task >> create_fact_survey_table_task >> create_dim_staffTitle_table_task >> \
    create_dim_pxTitle_table_task >> create_dim_appType_table_task >> create_dim_appStatus_table_task >> \
    create_fact_newPatients_table_task >> create_dim_pxLastName_table_task >> create_dim_paymentCategory_table_task >> \
    create_dim_itemDesc_table_task >> create_fact_transactions_table_task >> create_dim_prodType_table_task >> \
    create_dim_brand_table_task >> create_dim_manufacturer_table_task >> create_dim_prodCode_table_task >> \
    create_dim_prodName_table_task >> create_fact_productSales_table_task
