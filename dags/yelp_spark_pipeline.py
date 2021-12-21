import json
import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from operators.emr_get_or_create_job_flow_operator import EmrGetOrCreateJobFlowOperator

# CLUSTER_NAME = "Yelp ETL"

terminate_cluster = False

EMR_CREDENTIALS_CONN_ID = "emr_credentials"

AWS_CREDENTIALS_CONN_ID = Variable.get("aws_credentials_conn_id", "aws_credentials")
AWS_REGION = Variable.get("aws_region", "us-west-2")
DAG_NAME = os.path.basename(__file__).replace('.py', '')

# Configurations
BUCKET_NAME = "udacity-data-modelling"
s3_data = "data/yelp_academic_dataset_checkin.json"
local_script = "./dags/scripts/etl.py"
s3_script = "scripts/etl.py"
s3_clean = "clean_data/"

SPARK_STEPS = [
    {
        "Name": "Yelp ETL",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
                "--input", "s3://{{ params.BUCKET_NAME }}/data",
                "--output", "s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean}}",
            ],
        },
    },
]

with open('./dags/config/emr_cluster.json') as f:
    JOB_FLOW_OVERRIDES = json.load(f)


# helper function
def _copy_local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook(AWS_CREDENTIALS_CONN_ID)
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)


default_args = {
    'owner': 'Roman Lukash',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
    # 'email_on_retry': False,
}

dag = DAG(
    dag_id=DAG_NAME,
    description='Load and transform yelp\'s data using spark',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    # max_active_runs=1
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

script_to_s3 = PythonOperator(
    dag=dag,
    task_id="copy_script_to_s3",
    python_callable=_copy_local_to_s3,
    op_kwargs={"filename": local_script, "key": s3_script, },
)

# Create an EMR cluster
get_or_create_emr_cluster_task_id = "get_or_create_emr_cluster"
get_or_create_emr_cluster = EmrGetOrCreateJobFlowOperator(
    task_id=get_or_create_emr_cluster_task_id,
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id=AWS_CREDENTIALS_CONN_ID,
    emr_conn_id=EMR_CREDENTIALS_CONN_ID,
    dag=dag,
    region_name=AWS_REGION,
)


class CustomEmrJobFlowSensor(EmrJobFlowSensor):
    """
    Asks for the state of the JobFlow until it reaches WAITING/RUNNING state.
    If it fails the sensor errors, failing the task.
    :param job_flow_id: job_flow_id to check the state of
    :type job_flow_id: str
    """
    NON_TERMINAL_STATES = ['STARTING', 'BOOTSTRAPPING', 'TERMINATING']


job_flow_id = f"{{{{ task_instance.xcom_pull(task_ids='{get_or_create_emr_cluster_task_id}', key='return_value') }}}}"

wait_for_cluster_to_start = CustomEmrJobFlowSensor(
    task_id="wait_for_cluster_to_start",
    job_flow_id=job_flow_id,
    dag=dag,
    aws_conn_id=AWS_CREDENTIALS_CONN_ID,
)

step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id=job_flow_id,
    aws_conn_id=AWS_CREDENTIALS_CONN_ID,
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_data": s3_data,
        "s3_script": s3_script,
        "s3_clean": s3_clean,
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id=job_flow_id,
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
            + str(last_step)
            + "] }}",
    aws_conn_id=AWS_CREDENTIALS_CONN_ID,
    dag=dag,
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

start_data_pipeline >> script_to_s3 >> get_or_create_emr_cluster >> wait_for_cluster_to_start
wait_for_cluster_to_start >> step_adder >> step_checker
if terminate_cluster:
    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id=job_flow_id,
        aws_conn_id=AWS_CREDENTIALS_CONN_ID,
        dag=dag,
        trigger_rule="all_done"
    )
    step_checker >> terminate_emr_cluster
else:
    step_checker >> end_data_pipeline
