import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def transform_checkins(input_loc, output_loc):
    df = spark.read.json(f"{input_loc}/yelp_academic_dataset_checkin.json")
    df.selectExpr("business_id", "explode(split(date,',')) as date").selectExpr("business_id",
                                                                                "ltrim(date) as date") \
        .write.format('csv').option("codec", "com.hadoop.compression.lzo.LzopCodec").mode("overwrite").save(
        f"{output_loc}/check-ins")


def create_friends(input_loc, output_loc):
    #     # TODO: add check if friend is user(I mean we can find user with userId=friendId)
    df = spark.read.json(f"{input_loc}/yelp_academic_dataset_user.json")
    df.cache()

    df_users = df.selectExpr("user_id").alias("u")
    df_friends = df.filter(df.friends != "None").selectExpr("user_id",
                                                            "explode(split(friends,',')) as friend_id").selectExpr(
        "user_id", "ltrim(friend_id) as friend_id").alias("f")
    df_friends = df_friends.join(df_users, col("f.friend_id") == col("u.user_id")).drop("u.user_id")

    df_friends.write.format('csv').option("codec", "com.hadoop.compression.lzo.LzopCodec").mode("overwrite").save(
        f"{output_loc}/friends")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="/data")
    parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Yelp ETL pipeline").getOrCreate()

    transform_checkins(input_loc=args.input, output_loc=args.output)
    create_friends(input_loc=args.input, output_loc=args.output)
