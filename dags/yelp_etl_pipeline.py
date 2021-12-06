from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.models import Variable
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator,
                               DataQualityOperator)
from airflow.operators.dummy_operator import DummyOperator

from helpers import TestCase

DIMESIONS_LOAD_MODE = "delete-load"
REDSHIFT_CONN_ID = Variable.get("redshift_conn_id", "redshift")
AWS_CREDENTIALS_CONN_ID = Variable.get("aws_credentials_conn_id", "aws_credentials")

S3_BUCKET = Variable.get("s3_bucket", "yelp-data-sources")
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
}

with DAG('yelp_etl_pipeline',
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='0 * * * *',
         catchup=False,
         ) as dag:
    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    stage_businesses_to_redshift = StageToRedshiftOperator(
        task_id='stage_businesses',
        s3_bucket=S3_BUCKET,
        s3_key=BUSINESS_DATA_S3_KEY,
        schema="public",
        table="staging_businesses",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
        COMPUPDATE OFF STATUPDATE OFF
        FORMAT AS JSON 'auto ignorecase'
        TIMEFORMAT AS 'epochmillisecs'
        TRUNCATECOLUMNS
        BLANKSASNULL;
        """),
        dag=dag
    )

    stage_users_to_redshift = StageToRedshiftOperator(
        task_id='stage_users',
        s3_bucket=S3_BUCKET,
        s3_key=USERS_DATA_S3_KEY,
        schema="public",
        table="staging_users",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
        COMPUPDATE OFF STATUPDATE OFF
        FORMAT AS JSON 'auto ignorecase'
        TIMEFORMAT AS 'epochmillisecs'
        TRUNCATECOLUMNS
        BLANKSASNULL;
        """),
        dag=dag
    )

    stage_reviews_to_redshift = StageToRedshiftOperator(
        task_id='stage_reviews',
        s3_bucket=S3_BUCKET,
        s3_key=REVIEWS_DATA_S3_KEY,
        schema="public",
        table="staging_reviews",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
        COMPUPDATE OFF STATUPDATE OFF
        FORMAT AS JSON 'auto ignorecase'
        TIMEFORMAT AS 'epochmillisecs'
        TRUNCATECOLUMNS
        BLANKSASNULL;
        """),
        dag=dag
    )

    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

    start_operator >> [stage_businesses_to_redshift, stage_users_to_redshift, stage_reviews_to_redshift] >> end_operator
