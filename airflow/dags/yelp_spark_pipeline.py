from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.operators import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from common import *
from operators.emr_get_or_create_job_flow_operator import EmrGetOrCreateJobFlowOperator

terminate_cluster = True

EMR_CREDENTIALS_CONN_ID = Variable.get("emr_credentials_conn_id", "emr_credentials")

AWS_REGION = Variable.get("aws_region", "us-west-2")
DAG_NAME = os.path.basename(__file__).replace('.py', '')

# Configurations
LOCAL_SCRIPT_PATH = f"{CURRENT_PATH}/scripts/etl.py"
S3_SCRIPT_KEY = "scripts/etl.py"

SPARK_STEPS = [
    {
        "Name": "Yelp ETL",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "s3://{{ params.s3_bucket }}/{{ params.s3_script }}",
                "--check-ins-input-path", "s3://{{ params.s3_bucket }}/{{params.check_in_data_key}}",
                "--users-input-path", "s3://{{ params.s3_bucket }}/{{ params.user_data_key }}",
                "--output", "s3a://{{ params.s3_bucket }}/{{ params.s3_output}}",
            ],
        },
    },
]

JOB_FLOW_OVERRIDES = {
    "Name": "Yelp ETL",
    "ReleaseLabel": "emr-5.33.1",
    "LogUri": "s3n://aws-emr-resources-508278446598-us-west-2/",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [{
                "Classification": "export",
                "Properties": {
                    "PYSPARK_PYTHON": "/usr/bin/python3"
                }
            }],
            "Properties": {}
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1
            }, {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole"
}

default_args = {
    'owner': 'Roman Lukash',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'email_on_retry': False,
}


def create_subdag(parent_dag_name: str, child_dag_name, args):
    with DAG(
            dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
            description='Load and transform data using spark',
            default_args=args,
            catchup=False,
            schedule_interval=None,
            # max_active_runs=1
    ) as dag:
        start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

        script_to_s3 = PythonOperator(
            dag=dag,
            task_id="copy_script_to_s3",
            python_callable=copy_local_to_s3,
            op_kwargs={"filename": LOCAL_SCRIPT_PATH, "key": S3_SCRIPT_KEY, },
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

        # Copied from https://github.com/aws-samples/emr-studio-samples/blob/main/airflow_dag_for_execution/simple_dag.py         
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
                "s3_bucket": S3_BUCKET,
                "check_in_data_key": RAW_CHECK_IN_DATA_KEY,
                "user_data_key": USERS_DATA_S3_KEY,
                "s3_script": S3_SCRIPT_KEY,
                "s3_output": PROCESSED_DATA_PATH,
            },
            dag=dag,
        )

        last_step = len(SPARK_STEPS) - 1
        
        wait_for_step_to_complete = EmrStepSensor(
            task_id="wait_for_step_to_complete",
            job_flow_id=job_flow_id,
            step_id=f"{{{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[{last_step}] }}}}",
            aws_conn_id=AWS_CREDENTIALS_CONN_ID,
            dag=dag,
        )

        end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

        start_data_pipeline >> script_to_s3 >> get_or_create_emr_cluster >> wait_for_cluster_to_start
        wait_for_cluster_to_start >> step_adder >> wait_for_step_to_complete
        if terminate_cluster:
            
            terminate_emr_cluster = EmrTerminateJobFlowOperator(
                task_id="terminate_emr_cluster",
                job_flow_id=job_flow_id,
                aws_conn_id=AWS_CREDENTIALS_CONN_ID,
                dag=dag,
                trigger_rule="all_done"
            )
            wait_for_step_to_complete >> terminate_emr_cluster >> end_data_pipeline
        else:
            wait_for_step_to_complete >> end_data_pipeline

    return dag
