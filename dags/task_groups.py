from textwrap import dedent

from airflow import DAG
from airflow.models import Variable
from airflow.operators import (LoadFactOperator, LoadDimensionOperator)
from airflow.operators import StageToRedshiftOperator, PopulateTableOperator

DIMESIONS_LOAD_MODE = Variable.get("dimenions_load_mode", "delete-load")
REDSHIFT_CONN_ID = Variable.get("redshift_conn_id", "redshift")
AWS_CREDENTIALS_CONN_ID = Variable.get("aws_credentials_conn_id", "aws_credentials")
TABLES_SCHEMA = Variable.get("redshift_schema", "public")

S3_BUCKET = Variable.get("s3_bucket", "yelp-eu-north-1")

BUSINESS_DATA_S3_KEY = Variable.get("business_data_s3_key", "yelp_academic_dataset_business.json")
USERS_DATA_S3_KEY = Variable.get("users_data_s3_key", "yelp_academic_dataset_user.json")
REVIEWS_DATA_S3_KEY = Variable.get("reviews_data_s3_key", "yelp_academic_dataset_review.json")
# TODO: add step to compute next resource
CHECK_IN_DATA_S3_KEY = Variable.get("check_in_data_s3_key", "cleaned-check-ins.json")
TIP_DATA_S3_KEY = Variable.get("tip_data_s3_key", "yelp_academic_dataset_tip.json")


def create_staging_tasks(dag: DAG):

    stage_businesses = StageToRedshiftOperator(
        task_id='stage_businesses',
        s3_bucket=S3_BUCKET,
        s3_key=BUSINESS_DATA_S3_KEY,
        schema=TABLES_SCHEMA,
        table="staging_businesses",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS JSON 'auto ignorecase'
            TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
            TRUNCATECOLUMNS
            BLANKSASNULL;
            """),
        dag=dag
    )
    stage_users = StageToRedshiftOperator(
        task_id='stage_users',
        s3_bucket=S3_BUCKET,
        s3_key=USERS_DATA_S3_KEY,
        schema=TABLES_SCHEMA,
        table="staging_users",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS JSON 'auto ignorecase'
            TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
            TRUNCATECOLUMNS
            BLANKSASNULL;
            """),
        dag=dag
    )
    stage_reviews = StageToRedshiftOperator(
        task_id='stage_reviews',
        s3_bucket=S3_BUCKET,
        s3_key=REVIEWS_DATA_S3_KEY,
        schema=TABLES_SCHEMA,
        table="staging_reviews",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS JSON 'auto ignorecase'
            TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
            TRUNCATECOLUMNS
            BLANKSASNULL;
            """),
        dag=dag
    )
    stage_tips = StageToRedshiftOperator(
        task_id='stage_tips',
        s3_bucket=S3_BUCKET,
        s3_key=TIP_DATA_S3_KEY,
        schema=TABLES_SCHEMA,
        table="staging_tips",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS JSON 'auto ignorecase'
            TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
            TRUNCATECOLUMNS
            BLANKSASNULL;
            """),
        dag=dag
    )
    stage_checkins = StageToRedshiftOperator(
        task_id='stage_checkins',
        s3_bucket=S3_BUCKET,
        s3_key=CHECK_IN_DATA_S3_KEY,
        schema=TABLES_SCHEMA,
        table="staging_checkins",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS JSON 'auto ignorecase'
            TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
            TRUNCATECOLUMNS
            BLANKSASNULL;
            """),
        dag=dag
    )
    return [stage_businesses, stage_tips, stage_checkins, stage_users, stage_reviews]


def create_load_dimension_tasks(dag: DAG):
    load_user_dimension = LoadDimensionOperator(
        task_id='load_user_dim_table',
        table="dim_user",
        redshift_conn_id=REDSHIFT_CONN_ID,
        load_mode=DIMESIONS_LOAD_MODE,
        dag=dag
    )
    load_business_dimension = LoadDimensionOperator(
        task_id='load_business_dim_table',
        table="dim_business",
        redshift_conn_id=REDSHIFT_CONN_ID,
        load_mode=DIMESIONS_LOAD_MODE,
        dag=dag
    )
    return [load_user_dimension, load_business_dimension]


def create_load_facts_tasks(dag):
    load_review_facts = LoadFactOperator(
        task_id='load_review_fact_table',
        table="fact_review",
        redshift_conn_id=REDSHIFT_CONN_ID,
        dag=dag
    )
    load_business_category_facts = LoadFactOperator(
        task_id='load_business_category_fact_table',
        table="fact_business_category",
        redshift_conn_id=REDSHIFT_CONN_ID,
        dag=dag
    )
    load_checkin_facts = LoadFactOperator(
        task_id='load_check_in_fact_table',
        table="fact_checkin",
        redshift_conn_id=REDSHIFT_CONN_ID,
        dag=dag
    )

    load_tip_facts = LoadFactOperator(
        task_id='load_tip_fact_table',
        table="fact_tip",
        redshift_conn_id=REDSHIFT_CONN_ID,
        dag=dag
    )

    return [load_review_facts, load_business_category_facts, load_checkin_facts, load_tip_facts]
