from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers import TestCase
from operators import DataQualityOperator, PopulateTableOperator
from task_groups import create_staging_tasks, create_load_dimension_tasks, create_load_facts_tasks

DAG_NAME = 'yelp_etl_pipeline'

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

enable_staging = False

default_args = {
    'owner': 'Roman Lukash',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
    # 'email_on_retry': False,
}

with DAG(DAG_NAME,
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         catchup=False,
         schedule_interval=None,
         ) as dag:
    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    create_tables_if_not_exist = PostgresOperator(
        task_id="create_tables_if_not_exist",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql="sql/create_schema.sql",
    )
    populate_dim_date_if_empty = PopulateTableOperator(
        task_id='populate_date_dimension_if_empty',
        s3_bucket=S3_BUCKET,
        s3_key="dim_date.csv",
        schema=TABLES_SCHEMA,
        table="dim_date",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS CSV;
            """),
        dag=dag
    )

    load_dimensions = create_load_dimension_tasks(dag)
    load_facts = create_load_facts_tasks(dag)

    test_cases = [
        # table is empty checks
        TestCase("SELECT  COUNT(*)>0 FROM fact_review", True),
        TestCase("SELECT  COUNT(*)>0 FROM fact_checkin", True),
        TestCase("SELECT  COUNT(*)>0 FROM fact_tip", True),
        TestCase("SELECT  COUNT(*)>0 FROM fact_business_category", True),
        TestCase("SELECT  COUNT(*)>0 FROM dim_business", True),
        TestCase("SELECT  COUNT(*)>0 FROM dim_user", True),
        # NOT NULL checks
        TestCase("SELECT  COUNT(*)>0 FROM dim_user WHERE name is null or trim(name) = ''", False),
        TestCase("SELECT  COUNT(*)>0 FROM dim_user WHERE user_id is null", False),
        TestCase("SELECT  COUNT(*)>0 FROM dim_business WHERE business_id is null", False),
        TestCase("SELECT  COUNT(*)>0 FROM dim_business WHERE name is null or trim(name) = ''", False),

        TestCase("SELECT  COUNT(*)>0 FROM fact_review WHERE review_id is null or trim(review_id) = ''", False),
        TestCase("SELECT  COUNT(*)>0 FROM fact_review WHERE user_id is null or trim(user_id) = ''", False),
        TestCase("SELECT  COUNT(*)>0 FROM fact_review WHERE business_id is null or trim(business_id) = ''", False),
        TestCase("SELECT  COUNT(*)>0 FROM fact_review WHERE text is null or trim(text) = ''", False),
        TestCase("SELECT  COUNT(*)>0 FROM fact_review WHERE stars<1 or stars>5", False),

        TestCase("SELECT  COUNT(*)>0 FROM fact_business_category WHERE category is null or trim(category) = ''", False),
        TestCase("SELECT  COUNT(*)>0 FROM fact_business_category WHERE business_id is null or trim(business_id) = ''", False),

        TestCase("SELECT  COUNT(*)>0 FROM fact_tip WHERE user_id is null or trim(user_id) = ''", False),
        TestCase("SELECT  COUNT(*)>0 FROM fact_tip WHERE business_id is null or trim(business_id) = ''", False),
        TestCase("SELECT  COUNT(*)>0 FROM fact_tip WHERE text is null or trim(text) = ''", False),

        TestCase("SELECT  COUNT(*)>0 FROM fact_checkin WHERE business_id is null or trim(business_id) = ''", False),
        TestCase("SELECT  COUNT(*)>0 FROM fact_checkin WHERE timestamp is null or trim(timestamp) = ''", False),



        # TODO: what to do if this is continous pipeline? Should I calculate count beforehand?
        # TestCase("""SELECT SUM(REGEXP_COUNT(s.categories, ',') + 1)=(SELECT COUNT(*) FROM fact_business_category)
        #              FROM staging_businesses s""", True)
    ]
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id=REDSHIFT_CONN_ID,
        test_cases=test_cases,
        dag=dag
    )

    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

    if enable_staging:
        staging_processes = create_staging_tasks(dag)
        start_operator >> create_tables_if_not_exist >> populate_dim_date_if_empty >> staging_processes

        for p in staging_processes:
            p >> load_dimensions

        for d in load_dimensions:
            d >> load_facts

        load_facts >> run_quality_checks >> end_operator

    else:
        start_operator >> create_tables_if_not_exist >> populate_dim_date_if_empty >> load_dimensions

        for d in load_dimensions:
            d >> load_facts

        load_facts >> run_quality_checks >> end_operator
