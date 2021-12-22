import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def _transform_checkins(input_loc, output_loc):
    """
        Transforms check-ins by unnesting date field
    """
    df = spark.read.json(f"{input_loc}")
    _transform_checkins_inner(df) \
        .write.format('csv').mode("overwrite").save(f"{output_loc}/check-ins")


def _transform_checkins_inner(df):
    return df.selectExpr("business_id", "explode(split(date,',')) as date").selectExpr("business_id",
                                                                                       "ltrim(date) as date")


def _create_friends(input_loc, output_loc):
    """
        Creates user-friend relation by unnesting friend field
    """
    df = spark.read.json(input_loc)
    df.cache()
    df_friends = _create_friends_inner(df)
    df_friends.write.format('csv').mode("overwrite").save(f"{output_loc}/friends")
    return df


def _create_friends_inner(df):
    df_users = df.selectExpr("user_id").alias("u")
    df_friends = df.filter(df.friends != "None").selectExpr("user_id",
                                                            "explode(split(friends,',')) as friend_id") \
        .selectExpr("user_id", "ltrim(friend_id) as friend_id").alias("f")

    c = col("u.user_id")
    # TODO: should we do this in the datawarehouse?
    # Filter out friends that are not registered
    # df_friends = df_friends.join(df_users, col("f.friend_id") == c).drop(c)

    return df_friends


if __name__ == "__main__":
    logging.info("Starting spark")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="/data")
    parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Yelp ETL pipeline").getOrCreate()

    _transform_checkins(input_loc=args.input + "/yelp_academic_dataset_checkin.json", output_loc=args.output)
    _create_friends(input_loc=args.input + "/yelp_academic_dataset_user.json", output_loc=args.output)
