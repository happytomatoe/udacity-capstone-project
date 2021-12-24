import argparse
import logging

from pyspark.sql import SparkSession


def _transform_checkins(input_loc, output_loc):
    """
        Transforms check-ins by unnesting date field
    """
    df = spark.read.json(f"{input_loc}")
    _transform_checkins_inner(df) \
        .write.format('csv').option("header", True).mode("overwrite").save(output_loc)


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
    df_friends.write.format('csv').option("header", True).mode("overwrite").save(output_loc)
    return df


def _create_friends_inner(df):
    df_friends = df.filter(df.friends != "None").selectExpr("user_id",
                                                            "explode(split(friends,',')) as friend_id") \
        .selectExpr("user_id", "ltrim(friend_id) as friend_id").alias("f")
    return df_friends


if __name__ == "__main__":
    logging.info("Starting spark")
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-ins-input-path", type=str, help="Check-in data input path")
    parser.add_argument("--check-ins-output-path", type=str, help="Check-in data output path")

    parser.add_argument("--users-input-path", type=str, help="User data input path")
    parser.add_argument("--friends-output-path", type=str, help="Friends data output path")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Yelp ETL pipeline").getOrCreate()

    _transform_checkins(args.check_ins_input_path, args.check_ins_output_path)
    _create_friends(args.users_input_path, args.friends_output_path)
