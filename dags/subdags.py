from textwrap import dedent

from airflow import DAG
from airflow.models import Variable
from airflow.operators import StageToRedshiftOperator

BUSINESS_DATA_S3_KEY = Variable.get("business_data_s3_key", "yelp_academic_dataset_business.json")
USERS_DATA_S3_KEY = Variable.get("users_data_s3_key", "yelp_academic_dataset_user.json")
REVIEWS_DATA_S3_KEY = Variable.get("reviews_data_s3_key", "yelp_academic_dataset_review.json")
S3_BUCKET = Variable.get("s3_bucket", "yelp-eu-north-1")


def load_subdag(parent_dag_name, child_dag_name, args,
                tables_schema,
                redshift_conn_id,
                aws_credentials_conn_id):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@daily",
    )
    with dag_subdag:
        stage_businesses_to_redshift = StageToRedshiftOperator(
            task_id='stage_businesses',
            s3_bucket=S3_BUCKET,
            s3_key=BUSINESS_DATA_S3_KEY,
            schema=tables_schema,
            table="staging_businesses",
            redshift_conn_id=redshift_conn_id,
            aws_conn_id=aws_credentials_conn_id,
            copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS JSON 'auto ignorecase'
            TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
            TRUNCATECOLUMNS
            BLANKSASNULL;
            """),
            dag=dag_subdag
        )

        stage_users_to_redshift = StageToRedshiftOperator(
            task_id='stage_users',
            s3_bucket=S3_BUCKET,
            s3_key=USERS_DATA_S3_KEY,
            schema=tables_schema,
            table="staging_users",
            redshift_conn_id=redshift_conn_id,
            aws_conn_id=aws_credentials_conn_id,
            copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS JSON 'auto ignorecase'
            TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
            TRUNCATECOLUMNS
            BLANKSASNULL;
            """),
            dag=dag_subdag
        )

        stage_reviews_to_redshift = StageToRedshiftOperator(
            task_id='stage_reviews',
            s3_bucket=S3_BUCKET,
            s3_key=REVIEWS_DATA_S3_KEY,
            schema=tables_schema,
            table="staging_reviews",
            redshift_conn_id=redshift_conn_id,
            aws_conn_id=aws_credentials_conn_id,
            copy_options=dedent("""
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS JSON 'auto ignorecase'
            TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
            TRUNCATECOLUMNS
            BLANKSASNULL;
            """),
            dag=dag_subdag
        )

    return dag_subdag
