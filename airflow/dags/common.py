"""
    Common functionality. Includes airflow variables and functions
"""
import os

from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

DIMESIONS_LOAD_MODE = Variable.get("dimenions_load_mode", "delete-load")
REDSHIFT_CONN_ID = Variable.get("redshift_conn_id", "redshift")
AWS_CREDENTIALS_CONN_ID = Variable.get("aws_credentials_conn_id", "aws_credentials")
TABLES_SCHEMA = Variable.get("redshift_schema", "public")

S3_BUCKET = Variable.get("s3_bucket", "udacity-data-modelling")

RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"

BUSINESS_DATA_S3_KEY = Variable.get("business_data_s3_key", f"{RAW_DATA_PATH}/yelp_academic_dataset_business.json")
USERS_DATA_S3_KEY = Variable.get("users_data_s3_key", f"{RAW_DATA_PATH}/yelp_academic_dataset_user.json")
REVIEWS_DATA_S3_KEY = Variable.get("reviews_data_s3_key", f"{RAW_DATA_PATH}/yelp_academic_dataset_review.json")
TIP_DATA_S3_KEY = Variable.get("tip_data_s3_key", f"{RAW_DATA_PATH}/yelp_academic_dataset_tip.json")
RAW_CHECK_IN_DATA_KEY = Variable.get("raw_check_in_data_s3_key", f"{RAW_DATA_PATH}/yelp_academic_dataset_checkin.json")

PROCESSED_CHECK_IN_DATA_S3_KEY = Variable.get("processed_check_in_data_s3_key", f"{PROCESSED_DATA_PATH}/check-ins/")
FRIEND_DATA_S3_KEY = Variable.get("friend_data_s3_key", f"{PROCESSED_DATA_PATH}/friends/")

CURRENT_PATH = os.path.dirname(__file__)


# helper function
def copy_local_to_s3(filename, key, bucket_name=S3_BUCKET):
    s3 = S3Hook(AWS_CREDENTIALS_CONN_ID)
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)
