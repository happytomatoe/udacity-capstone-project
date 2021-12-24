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

S3_BUCKET = Variable.get("s3_bucket")

RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"

RAW_BUSINESS_DATA_S3_KEY = Variable.get("raw_business_data_s3_key", f"{RAW_DATA_PATH}/yelp_academic_dataset_business.json")
RAW_REVIEWS_DATA_S3_KEY = Variable.get("raw_reviews_data_s3_key", f"{RAW_DATA_PATH}/yelp_academic_dataset_review.json")
RAW_TIP_DATA_S3_KEY = Variable.get("raw_tip_data_s3_key", f"{RAW_DATA_PATH}/yelp_academic_dataset_tip.json")
RAW_CHECK_IN_DATA_KEY = Variable.get("raw_check_in_data_s3_key", f"{RAW_DATA_PATH}/yelp_academic_dataset_checkin.json")
RAW_USERS_DATA_KEY = Variable.get("raw_user_data_s3_key", f"{RAW_DATA_PATH}/yelp_academic_dataset_user.json")


PROCESSED_USERS_DATA_S3_KEY = Variable.get("processed_users_data_s3_key", f"{PROCESSED_DATA_PATH}/users/")
PROCESSED_REVIEWS_DATA_S3_KEY = Variable.get("processed_reviews_data_s3_key", f"{PROCESSED_DATA_PATH}/reviews/")
PROCESSED_CHECK_IN_DATA_S3_KEY = Variable.get("processed_check_in_data_s3_key", f"{PROCESSED_DATA_PATH}/check-ins/")
PROCESSED_FRIEND_DATA_S3_KEY = Variable.get("processed_friend_data_s3_key", f"{PROCESSED_DATA_PATH}/friends/")

CURRENT_PATH = os.path.dirname(__file__)


# helper function
def copy_local_to_s3(filename, key, bucket_name=S3_BUCKET):
    s3 = S3Hook(AWS_CREDENTIALS_CONN_ID)
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)
