"""
    Includes functions to create group of tasks
"""

from textwrap import dedent

from airflow import DAG
from operators.load_fact_operator import LoadFactOperator
from operators.load_dimension_operator import LoadDimensionOperator
from operators.stage_redshift_operator import StageToRedshiftOperator

from common import *


def create_staging_tasks(dag: DAG):
    stage_businesses = StageToRedshiftOperator(
        task_id='stage_businesses',
        s3_bucket=S3_BUCKET,
        s3_key=RAW_BUSINESS_DATA_S3_KEY,
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
        s3_key=PROCESSED_USERS_DATA_S3_KEY,
        schema=TABLES_SCHEMA,
        table="staging_users",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS CSV 
            TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
            TRUNCATECOLUMNS
            BLANKSASNULL;
            """),
        dag=dag
    )
    stage_reviews = StageToRedshiftOperator(
        task_id='stage_reviews',
        s3_bucket=S3_BUCKET,
        s3_key=PROCESSED_REVIEWS_DATA_S3_KEY,
        schema=TABLES_SCHEMA,
        table="staging_reviews",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS CSV 
            TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
            TRUNCATECOLUMNS
            BLANKSASNULL;
            """),
        dag=dag
    )
    stage_tips = StageToRedshiftOperator(
        task_id='stage_tips',
        s3_bucket=S3_BUCKET,
        s3_key=RAW_TIP_DATA_S3_KEY,
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
        s3_key=PROCESSED_CHECK_IN_DATA_S3_KEY,
        schema=TABLES_SCHEMA,
        table="staging_checkins",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            CSV
            TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
            TRUNCATECOLUMNS
            BLANKSASNULL;
            """),
        dag=dag
    )
    stage_friends = StageToRedshiftOperator(
        task_id='stage_friends',
        s3_bucket=S3_BUCKET,
        s3_key=PROCESSED_FRIEND_DATA_S3_KEY,
        schema=TABLES_SCHEMA,
        table="staging_friends",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            CSV
            TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
            TRUNCATECOLUMNS
            BLANKSASNULL;
            """),
        dag=dag
    )
    return [stage_businesses, stage_tips, stage_checkins, stage_users, stage_reviews, stage_friends]


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

    load_friend_facts = LoadFactOperator(
        task_id='load_friend_fact_table',
        table="fact_friend",
        redshift_conn_id=REDSHIFT_CONN_ID,
        dag=dag
    )

    return [load_review_facts, load_business_category_facts, load_checkin_facts, load_tip_facts, load_friend_facts]
