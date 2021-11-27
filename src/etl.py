import glob
import os
import sys
from io import StringIO
from uuid import uuid4

from pyspark.sql import SparkSession

TEMP_TABLE_NAME = 'source'


def process_reviews(filepath):
    """
    Processes song file
    :param filepath - filepath to json file
    """
    spark = SparkSession.builder.master("local[8]").getOrCreate()

    mode = "overwrite"
    url = "jdbc:postgresql://127.0.0.1:5432/studentdb"
    properties = {"user": "student","password": "student","driver": "org.postgresql.Driver"}

    # for path in glob.glob(f"{data_path}/*.json"):
    #     tbl=path[path.rindex("/")+1:path.rindex(".")]
    #     spark.read.json(path).write.jdbc(url=url, table=tbl, mode=mode, properties=properties)



    # spark.read.json(f"{data_path}/yelp_academic_dataset_user.json").write.jdbc(url=url, table="users", mode=mode, properties=properties)

    # spark.read.json(f"{data_path}/yelp_academic_dataset_review.json").write.jdbc(url=url, table="reviews", mode=mode, properties=properties)

    df= spark.read.json(filepath).drop("attributes")
    df.printSchema()
    df.write.jdbc(url=url, table="reviews", mode=mode, properties=properties)

def main():
    arr = os.listdir("./data/yelp")
    print(arr)
    process_reviews(filepath='./data/yelp/yelp_academic_dataset_review.json')

    conn.close()


if __name__ == "__main__":
    main()
