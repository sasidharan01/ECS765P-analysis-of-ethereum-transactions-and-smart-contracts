import os
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Ethereum").getOrCreate()

    def check_blocks(line):
        try:
            fields = line.split(",")
            if len(fields) != 19:
                return False
            return True
        except:
            return False

    def calculate_size(col):
        try:
            if len(col) > 0:
                size = (len(col) - 2) * 4
                return size
            else:
                return 0
        except:
            return 0

    s3_data_repository_bucket = os.environ["DATA_REPOSITORY_BUCKET"]
    s3_endpoint_url = os.environ["S3_ENDPOINT_URL"] + ":" + os.environ["BUCKET_PORT"]
    s3_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    s3_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    s3_bucket = os.environ["BUCKET_NAME"]

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    blocks = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv"
    )

    blocks_filtered = blocks.filter(check_blocks)

    blocks_transformed = blocks_filtered.map(
        lambda x: (
            "size",
            (
                calculate_size(x.split(",")[4]),
                calculate_size(x.split(",")[5]),
                calculate_size(x.split(",")[6]),
                calculate_size(x.split(",")[7]),
                calculate_size(x.split(",")[8]),
            ),
        )
    )

    blocks_reduced = blocks_transformed.reduceByKey(
        lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4])
    )

    blocks_mapped = blocks_reduced.map(
        lambda x: (x[0], (x[1][0] + x[1][1] + x[1][2] + x[1][3] + x[1][4]))
    )

    bucket = boto3.resource(
        "s3",
        endpoint_url="http://" + s3_endpoint_url,
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )

    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    obj = bucket.Object(
        s3_bucket, "ethereum_miscellaneous_analysis_" + date_time + "/data_ovehead.txt"
    )
    obj.put(Body=json.dumps(blocks_mapped.collect()))

    spark.stop()
