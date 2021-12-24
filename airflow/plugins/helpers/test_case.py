import typing
from dataclasses import dataclass


@dataclass
class TestCase:
    """
        Class that represents test case
        :param sql: sql query
        :type: sql: str
        :param: expected_value: expected value that query should return
        :type expected_value: `typing.Any`
    """
    sql: str
    expected_value: 'typing.Any'
