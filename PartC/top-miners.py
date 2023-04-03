import os
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    # Initialize spark session
    spark = SparkSession.builder.appName("Ethereum").getOrCreate()

    # Check the format of blocks data
    def verify_blocks(line):
        try:
            fields = line.split(",")
            if len(fields) == 19 and fields[1] != "hash":
                return True
            else:
                return False
        except:
            return False

    # Extract features (miner address and block size) from blocks data
    def get_block_features(line):
        try:
            fields = line.split(",")
            miner = str(fields[9])
            size = int(fields[12])
            return (miner, size)
        except:
            return (0, 1)

    # Fetch S3 environment variables
    s3_data_repository_bucket = os.environ["DATA_REPOSITORY_BUCKET"]
    s3_endpoint_url = os.environ["S3_ENDPOINT_URL"] + ":" + os.environ["BUCKET_PORT"]
    s3_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    s3_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    s3_bucket = os.environ["BUCKET_NAME"]

    # Configure Hadoop settings for the Spark session
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Fetch blocks file from S3 bucket
    blocks = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv"
    )

    # Filter blocks data
    blocks_filtered = blocks.filter(verify_blocks)

    # Transform blocks data
    blocks_transformed = blocks_filtered.map(get_block_features)

    # Reduce blocks data by miner address
    blocks_reduced = blocks_transformed.reduceByKey(lambda x, y: x + y)

    # Top 10 miners
    top_miners = blocks_reduced.takeOrdered(10, key=lambda x: -x[1])

    # Create resource object for S3 bucket
    bucket = boto3.resource(
        "s3",
        endpoint_url="http://" + s3_endpoint_url,
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )

    # Store results in S3 bucket
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    obj = bucket.Object(
        s3_bucket, "ethereum_top_miners" + date_time + "/top_miners.txt"
    )

    obj.put(Body=json.dumps(top_miners))

    spark.stop()
