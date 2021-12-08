from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.models import Variable
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator,
                               DataQualityOperator, PostgresOperator)
from airflow.operators.dummy_operator import DummyOperator

from helpers import TestCase

DIMESIONS_LOAD_MODE = "delete-load"
REDSHIFT_CONN_ID = Variable.get("redshift_conn_id", "redshift")
AWS_CREDENTIALS_CONN_ID = Variable.get("aws_credentials_conn_id", "aws_credentials")
TABLES_SCHEMA = Variable.get("redshift_schema", "public")

S3_BUCKET = Variable.get("s3_bucket", "yelp-eu-north-1")
BUSINESS_DATA_S3_KEY = Variable.get("business_data_s3_key", "yelp_academic_dataset_business.json")
USERS_DATA_S3_KEY = Variable.get("users_data_s3_key", "yelp_academic_dataset_user.json")
REVIEWS_DATA_S3_KEY = Variable.get("reviews_data_s3_key", "yelp_academic_dataset_review.json")

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

with DAG('yelp_etl_pipeline',
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

    # stage_businesses_to_redshift = StageToRedshiftOperator(
    #     task_id='stage_businesses',
    #     s3_bucket=S3_BUCKET,
    #     s3_key=BUSINESS_DATA_S3_KEY,
    #     schema=TABLES_SCHEMA,
    #     table="staging_businesses",
    #     redshift_conn_id=REDSHIFT_CONN_ID,
    #     aws_conn_id=AWS_CREDENTIALS_CONN_ID,
    #     copy_options=dedent("""
    #     COMPUPDATE OFF STATUPDATE OFF
    #     FORMAT AS JSON 'auto ignorecase'
    #     TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
    #     TRUNCATECOLUMNS
    #     BLANKSASNULL;
    #     """),
    #     dag=dag
    # )
    #
    # stage_users_to_redshift = StageToRedshiftOperator(
    #     task_id='stage_users',
    #     s3_bucket=S3_BUCKET,
    #     s3_key=USERS_DATA_S3_KEY,
    #     schema=TABLES_SCHEMA,
    #     table="staging_users",
    #     redshift_conn_id=REDSHIFT_CONN_ID,
    #     aws_conn_id=AWS_CREDENTIALS_CONN_ID,
    #     copy_options=dedent("""
    #     COMPUPDATE OFF STATUPDATE OFF
    #     FORMAT AS JSON 'auto ignorecase'
    #     TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
    #     TRUNCATECOLUMNS
    #     BLANKSASNULL;
    #     """),
    #     dag=dag
    # )
    #
    # stage_reviews_to_redshift = StageToRedshiftOperator(
    #     task_id='stage_reviews',
    #     s3_bucket=S3_BUCKET,
    #     s3_key=REVIEWS_DATA_S3_KEY,
    #     schema=TABLES_SCHEMA,
    #     table="staging_reviews",
    #     redshift_conn_id=REDSHIFT_CONN_ID,
    #     aws_conn_id=AWS_CREDENTIALS_CONN_ID,
    #     copy_options=dedent("""
    #     COMPUPDATE OFF STATUPDATE OFF
    #     FORMAT AS JSON 'auto ignorecase'
    #     TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
    #     TRUNCATECOLUMNS
    #     BLANKSASNULL;
    #     """),
    #     dag=dag
    # )
    #
    load_review_table = LoadFactOperator(
        task_id='load_review_fact_table',
        table_name="review_fact",
        redshift_conn_id=REDSHIFT_CONN_ID,
        dag=dag
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dim_table',
        table="user_dim",
        redshift_conn_id=REDSHIFT_CONN_ID,
        load_mode=DIMESIONS_LOAD_MODE,
        dag=dag
    )

    load_business_dimension_table = LoadDimensionOperator(
        task_id='load_user_dim_table',
        table="user_dim",
        redshift_conn_id=REDSHIFT_CONN_ID,
        load_mode=DIMESIONS_LOAD_MODE,
        dag=dag
    )

    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

    start_operator >> create_tables_if_not_exist >> \
    [load_user_dimension_table, load_review_table] >> end_operator

    # start_operator >> create_tables_if_not_exist >> \
    #     [stage_businesses_to_redshift, stage_users_to_redshift, stage_reviews_to_redshift]
    #
    #
    # stage_businesses_to_redshift >> load_user_dimension_table >> end_operator
    # stage_users_to_redshift >> load_user_dimension_table
    # stage_reviews_to_redshift >> load_user_dimension_table
