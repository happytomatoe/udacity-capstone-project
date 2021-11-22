from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_queries import SqlQueries


class LoadFactOperator(BaseOperator):
    """
    Loads/inserts records into fact table
    :param redshift_conn_id: redshift connection id
    :type redshift_conn_id: str
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, redshift_conn_id: str, *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

#         redshift_hook.run(SqlQueries.songplay_table_insert, True)
#         for output in redshift_hook.conn.notices:
#             self.log.info(output)
