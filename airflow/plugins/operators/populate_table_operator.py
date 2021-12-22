import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

from operators import StageToRedshiftOperator


class PopulateTableOperator(StageToRedshiftOperator):
    """
        Copies data from s3 to redshift using `Copy Command <https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-source-s3.html/>`_.
        if the table is empty
        :param redshift_conn_id - redshift connection id
        :type redshift_conn_id: str
        :param aws_conn_id - aws connection used to access objects in s3
        :type aws_conn_id: str
        :param s3_bucket: s3 bucket
        :type s3_bucket: str
        :param s3_key: s3 key/prefix. This field is templated. It works with jinja templates
        :type s3_key: str
        :param schema: redshift schema name
        :type schema: str
        :param table: redshift table name
        :type table: str
        :param copy_options: copy command options
    """

    ui_color = '#89DA59'

    @apply_defaults
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(PopulateTableOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = f"SELECT COUNT(*)=0 FROM {self.table}"
        records = redshift_hook.get_first(sql)
        if len(records) < 1:
            raise ValueError(
                f"Can not get table {self.table} count. Query '{sql} returned {records}'")

        logging.info(f"{self.table} is empty {records[0]}")
        if records[0]:
            super().execute(context)