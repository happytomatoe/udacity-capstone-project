from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import TableInsertQueries


class LoadFactOperator(BaseOperator):
    """
    Loads/inserts records into fact table
    :param redshift_conn_id: redshift connection id
    :type redshift_conn_id: str
    :param table - fact table name
    :type table: str
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, table: str, redshift_conn_id: str, *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.insert_query = TableInsertQueries.get(table)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift_hook.run(self.insert_query, True)
        for output in redshift_hook.conn.notices:
            self.log.info(output)
