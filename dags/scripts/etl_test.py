# make sure env variables are set correctly
import findspark  # this needs to be the first import
from chispa import assert_df_equality

findspark.init()

import logging
import pytest

from pyspark.sql import SparkSession
import etl


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark(request):
    """Fixture for creating a spark context."""

    spark = (SparkSession
             .builder
             .master('local[2]')
             .appName('pytest-pyspark-local-testing')
             .enableHiveSupport()
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark


def test_checkins_transform(spark):
    users = [
        {"business_id": "id1", "date": "2000-01-01 17:00:00"},
        {"business_id": "id2", "date": "2000-01-01 17:00:01, 2000-01-01 17:00:02, 2000-01-01 17:00:03"},
        {"business_id": "id3", "date": "2000-01-01 17:00:04"},
    ]
    expected = [
        {"business_id": "id1", "date": "2000-01-01 17:00:00"},
        {"business_id": "id2", "date": "2000-01-01 17:00:01"},
        {"business_id": "id2", "date": "2000-01-01 17:00:02"},
        {"business_id": "id2", "date": "2000-01-01 17:00:03"},
        {"business_id": "id3", "date": "2000-01-01 17:00:04"},
    ]
    df = spark.read.json(spark.sparkContext.parallelize([users]))
    actual = etl._transform_checkins_inner(df)
    print("Actual")
    actual.show()
    expected = spark.read.json(spark.sparkContext.parallelize([expected]))
    assert_df_equality(expected, actual)


def test_friends_transformation(spark):
    users = [
        {"user_id": "id1", "name": "Jack", "friends": "id2, id3"},
        {"user_id": "id2", "name": "Jane", "friends": "id1, id3"},
        {"user_id": "id3", "name": "Jane", "friends": "id1, id2"},
    ]
    expected = [
        {"user_id": "id1", "friend_id": "id2"},
        {"user_id": "id1", "friend_id": "id3"},
        {"user_id": "id2", "friend_id": "id1"},
        {"user_id": "id2", "friend_id": "id3"},
        {"user_id": "id3", "friend_id": "id1"},
        {"user_id": "id3", "friend_id": "id2"},
        {"user_id": "id3", "friend_id": "id5"},
    ]

    df = spark.read.json(spark.sparkContext.parallelize([users]))
    actual = etl._create_friends_inner(df)

    expected = spark.read.json(spark.sparkContext.parallelize([expected]))
    assert_df_equality(expected, actual, ignore_column_order=True, ignore_row_order=True)

