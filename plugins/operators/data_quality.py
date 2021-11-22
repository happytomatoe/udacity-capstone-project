from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import TestCase


class DataQualityOperator(BaseOperator):
    """
        Runs test cases (see :class: `~helpers.TestCase` ) and raises error if  expected and
        actual value doesn't match
        :param redshift_conn_id: redshift connection id
        :type redshift_conn_id: str
        :param test_cases: test cases
        :type test_cases: [TestCase]
        :raises valueError: if expected value doesn't match actual value
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, redshift_conn_id: str, test_cases: [TestCase], *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        for t in self.test_cases:
            self.test(t.sql, t.expected_value)

    def test(self, sql: str, expected_value):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift_hook.get_records(sql)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(
                f"Data quality check failed. Query returned no results. Query '{sql}'")

        actual_value = records[0][0]

        if actual_value != expected_value:
            raise ValueError(
                f"""Data quality check failed. Expected value {expected_value}
                ({type(expected_value)}) doesn't match actual value {actual_value}
                ({type(actual_value)}). Query '{sql}'
                """)
        self.log.info(f"Data quality check passed. Query '{sql}' \n returned {actual_value}")
