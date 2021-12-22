import os
from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from common import *
from operators import DataQualityOperator, PopulateTableOperator
from task_groups import create_staging_tasks, create_load_dimension_tasks, create_load_facts_tasks
from test_cases import test_cases
from yelp_spark_pipeline import create_subdag

DAG_NAME = os.path.basename(__file__).replace('.py', '')

DIM_DATE_DATA_FILE_NAME = "dim_date.csv"

enable_staging = True
run_spark = True

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
         # max_active_runs=1,
         ) as dag:
    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    create_tables_if_not_exist = PostgresOperator(
        task_id="create_tables_if_not_exist",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql="sql/create_schema.sql",
    )

    dim_date_data_s3_key = f"{RAW_DATA_PATH}/{DIM_DATE_DATA_FILE_NAME}"
    copy_date_dim_data_to_s3 = PythonOperator(
        dag=dag,
        task_id="copy_date_dim_data_to_s3",
        python_callable=copy_local_to_s3,
        op_kwargs={"filename": f"./dags/{DIM_DATE_DATA_FILE_NAME}", "key": dim_date_data_s3_key, },
    )

    populate_date_dimension_if_empty = PopulateTableOperator(
        task_id='populate_date_dimension_if_empty',
        s3_bucket=S3_BUCKET,
        s3_key=dim_date_data_s3_key,
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

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id=REDSHIFT_CONN_ID,
        test_cases=test_cases,
        dag=dag
    )

    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

    if run_spark:
        spark_etl = SubDagOperator(
            task_id="yelp_spark_pipeline",
            subdag=create_subdag(
                parent_dag_name=DAG_NAME,
                child_dag_name="yelp_spark_pipeline",
                args=default_args
            ),
            default_args=default_args,
            dag=dag,
        )
        start_operator >> spark_etl >> [create_tables_if_not_exist, copy_date_dim_data_to_s3]
    else:
        start_operator >> [create_tables_if_not_exist, copy_date_dim_data_to_s3]

    create_tables_if_not_exist >> populate_date_dimension_if_empty
    copy_date_dim_data_to_s3 >> populate_date_dimension_if_empty

    if enable_staging:
        staging_processes = create_staging_tasks(dag)
        populate_date_dimension_if_empty >> staging_processes

        for p in staging_processes:
            p >> load_dimensions

        for d in load_dimensions:
            d >> load_facts

        load_facts >> run_quality_checks >> end_operator

    else:
        populate_date_dimension_if_empty >> load_dimensions

        for d in load_dimensions:
            d >> load_facts

        load_facts >> run_quality_checks >> end_operator
