from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import TableInsertQueries


class LoadDimensionOperator(BaseOperator):
    """
        Loads/inserts records into dimension table

        :param table - dimension table name
        :type table: str
        :param redshift_conn_id: redshift connection id
        :type redshift_conn_id: str
        :param load_mode: loading mode. Available value - 'append', 'delete-load'
        :type load_mode: str
    """

    load_modes = ['append', 'delete-load']
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, table: str, redshift_conn_id: str, load_mode: str, *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.insert_query = TableInsertQueries.get(table)
        self.redshift_conn_id = redshift_conn_id
        self.load_mode = load_mode

    def execute(self, context):
        if self.load_mode not in self.load_modes:
            raise ValueError(
                f"Cannot find mode '{self.load_modes}'. Available values - {', '.join(self.load_modes)}")

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.load_mode == 'delete-load':
            redshift_hook.run(f"TRUNCATE TABLE {self.table}", False)
            for output in redshift_hook.conn.notices:
                self.log.info(output)

        redshift_hook.run(self.insert_query, autocommit=True)
        for output in redshift_hook.conn.notices:
            self.log.info(output)
