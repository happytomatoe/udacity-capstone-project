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
S3_BUCKET = Variable.get("s3_bucket", "udacity-dend")
LOG_DATA_S3_KEY = Variable.get("log_data_s3_key", "log_data/")
SONG_DATA_S3_KEY = Variable.get("song_data_s3_key", "song_data/A/A/C")


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



    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

    start_operator >>  end_operator
