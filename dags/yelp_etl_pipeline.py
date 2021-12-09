from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators import (LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries, TestCase

from subdags import load_subdag

DAG_NAME = 'yelp_etl_pipeline'

DIMESIONS_LOAD_MODE = "delete-load"
REDSHIFT_CONN_ID = Variable.get("redshift_conn_id", "redshift")
AWS_CREDENTIALS_CONN_ID = Variable.get("aws_credentials_conn_id", "aws_credentials")
TABLES_SCHEMA = Variable.get("redshift_schema", "public")

enable_staging = True

default_args = {
    'owner': 'Roman Lukash',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'schedule_interval': None,
    'catchup': False
}

with DAG(DAG_NAME,
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         catchup=False,
         ) as dag:
    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    create_tables_if_not_exist = PostgresOperator(
        task_id="create_tables_if_not_exist",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql="sql/create_schema.sql",
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dim_table',
        table="user_dim",
        redshift_conn_id=REDSHIFT_CONN_ID,
        load_mode=DIMESIONS_LOAD_MODE,
        table_insert_query=SqlQueries.user_dim_table_insert,
        dag=dag
    )

    load_business_dimension_table = LoadDimensionOperator(
        task_id='load_business_dim_table',
        table="business_dim",
        redshift_conn_id=REDSHIFT_CONN_ID,
        load_mode=DIMESIONS_LOAD_MODE,
        table_insert_query=SqlQueries.business_dim_table_insert,
        dag=dag
    )

    load_review_table = LoadFactOperator(
        task_id='load_review_fact_table',
        table_name="review_fact",
        redshift_conn_id=REDSHIFT_CONN_ID,
        dag=dag
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id=REDSHIFT_CONN_ID,
        test_cases=[
            TestCase("SELECT  COUNT(*)>0 FROM review_fact", True),
            TestCase("SELECT  COUNT(*)>0 FROM business_dim", True),
            TestCase("SELECT  COUNT(*)>0 FROM user_dim", True),
            TestCase("SELECT  COUNT(*)>0 FROM tip_dim", False),
        ],
        dag=dag
    )

    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

    if enable_staging:
        staging_processes = SubDagOperator(
            task_id="staging_processes",
            subdag=load_subdag(DAG_NAME, "staging_processes", dag.default_args,
                               tables_schema=TABLES_SCHEMA,
                               redshift_conn_id=REDSHIFT_CONN_ID,
                               aws_credentials_conn_id=AWS_CREDENTIALS_CONN_ID)
        )
        start_operator >> create_tables_if_not_exist >> staging_processes >> \
        [load_user_dimension_table, load_business_dimension_table, load_review_table] >> \
        run_quality_checks >> end_operator
    else:
        start_operator >> create_tables_if_not_exist >> \
        [load_user_dimension_table, load_business_dimension_table, load_review_table] >> \
        run_quality_checks >> end_operator

    #     [stage_businesses_to_redshift, stage_users_to_redshift, stage_reviews_to_redshift]
    #
    #
    # stage_businesses_to_redshift >> load_user_dimension_table >> end_operator
    # stage_users_to_redshift >> load_user_dimension_table
    # stage_reviews_to_redshift >> load_user_dimension_table
