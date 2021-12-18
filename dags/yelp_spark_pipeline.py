import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

AWS_CREDENTIALS_CONN_ID = Variable.get("aws_credentials_conn_id", "aws_credentials")
AWS_REGION = Variable.get("aws_region", "eu-north-1")
DAG_NAME = os.path.basename(__file__).replace('.py', '')

# Configurations
BUCKET_NAME = "yelp-eu-north-1"  # replace this with your bucket name
s3_data = "data/yelp_academic_dataset_checkin.json"
local_script = "./dags/scripts/etl.py"
s3_script = "scripts/etl.py"
s3_clean = "clean_data/"

SPARK_STEPS = [
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/data",
                # TODO: add filter
                # "--srcPattern=yelp_academic_dataset_checkin*",
                "--dest=/data",
            ],
        },
    },
    {
        "Name": "Transform check ins",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean }}",
            ],
        },
    },
]

JOB_FLOW_OVERRIDES = {
    "Name": "Transform check ins",
    "ReleaseLabel": "emr-5.33.1",
    'LogUri': 's3n://aws-logs-508278446598-eu-north-1/',
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


# helper function
def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
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
    DAG_NAME,
    description='Load and transform data in Redshift with Airflow',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    # max_active_runs=1
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

script_to_s3 = PythonOperator(
    dag=dag,
    task_id="script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": local_script, "key": s3_script, },
)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id=AWS_CREDENTIALS_CONN_ID,
    emr_conn_id="emr_credentials",
    dag=dag,
    region_name=AWS_REGION,
)

# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
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
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
            + str(last_step)
            + "] }}",
    aws_conn_id=AWS_CREDENTIALS_CONN_ID,
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id=AWS_CREDENTIALS_CONN_ID,
    dag=dag,
    trigger_rule="all_done"
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

start_data_pipeline >> script_to_s3 >> create_emr_cluster
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> end_data_pipeline
