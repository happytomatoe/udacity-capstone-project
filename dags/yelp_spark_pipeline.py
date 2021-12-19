import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from operators.emr_get_or_create_job_flow_operator import EmrGetOrCreateJobFlowOperator

CLUSTER_NAME = "Yelp ETL"

terminate_cluster = False

EMR_CREDENTIALS_CONN_ID = "emr_credentials"

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

JOB_FLOW_OVERRIDES = {
    "Name": CLUSTER_NAME,
    "ReleaseLabel": "emr-6.5.0",
    'LogUri': 's3n://aws-logs-508278446598-eu-north-1/',
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "yarn-site",
            "Properties": {
                "yarn.nodemanager.vmem-check-enabled": "false",
                "yarn.nodemanager.pmem-check-enabled": "false",
                "yarn.nodemanager.resource.memory-mb": "15G"
            }
        },
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "false"
            }
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.dynamicAllocation.enabled": "false",
                "spark.sql.adaptive.enabled": "true",
                "spark.driver.memory": "12288M",
                "spark.executor.memory": "12288M",
                "spark.executor.cores": "3",
                "spark.executor.instances": "2",
                "spark.executor.memoryOverhead": "1331M",
                "spark.driver.memoryOverhead": "1331M",
                "spark.memory.fraction": "0.80",
                "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                "spark.yarn.scheduler.reporterThread.maxFailures": "5",
                "spark.storage.level": "MEMORY_AND_DISK_SER",
                "spark.rdd.compress": "true",
                "spark.shuffle.compress": "true",
                "spark.shuffle.spill.compress": "true",
                "spark.default.parallelism": "12"
            }
        },
        {
            "Classification": "mapred-site",
            "Properties": {
                "mapreduce.map.output.compress": "true"
            }
        },
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
                "InstanceType": "m5d.xlarge",
                "InstanceCount": 1,
            }, {
                "Name": "Core - 2",
                "Market": "SPOT",  # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5d.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


# helper function
def _copy_local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook(AWS_CREDENTIALS_CONN_ID)
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)


def create_subdag(parent_dag_name: str, child_dag_name, args):
    dag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        description='Load and transform yelp\'s data using spark',
        default_args=args,
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

    job_flow_id = f"{{{{ task_instance.xcom_pull(task_ids='{get_or_create_emr_cluster_task_id}', key='return_value') }}}}"

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

    start_data_pipeline >> script_to_s3 >> get_or_create_emr_cluster
    get_or_create_emr_cluster >> step_adder >> step_checker
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

    return dag
