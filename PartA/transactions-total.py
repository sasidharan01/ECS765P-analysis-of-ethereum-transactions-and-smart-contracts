import os
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    # Initialize spark session
    spark = SparkSession.builder.appName("Ethereum").getOrCreate()

    # Check the format of transactions data
    def verify_transaction(line):
        try:
            fields = line.split(",")
            if len(fields) != 15:
                return False
            int(fields[11])
            return True
        except:
            return False

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

    # Fetch transactions.csv file from S3 bucket
    transactions = spark.sparkContext.textFile(
        "s3a://"
        + s3_data_repository_bucket
        + "/ECS765/ethereum-parvulus/transactions.csv"
    )

    # Filter transactions data
    transactions_filtered = transactions.filter(verify_transaction)

    """
    Extract the month and year from the block_timestamp field 
    and map it to 1 for each transaction
    """
    transactions_mapped = transactions_filtered.map(
        lambda x: (time.strftime("%m/%Y", time.gmtime(int(x.split(",")[11]))), 1)
    )

    # Reduce by key (month and year) to get the total number of transactions per month
    total_transactions = transactions_mapped.reduceByKey(operator.add)

    # Create resource object for S3 bucket
    bucket = boto3.resource(
        "s3",
        endpoint_url="http://" + s3_endpoint_url,
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )

    # Store results in S3 bucket
    now = datetime.now()  # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    obj = bucket.Object(s3_bucket, "ethereum_total_" + date_time + "/transactions_total.txt")
    obj.put(Body=json.dumps(total_transactions.take(100)))

    spark.stop()
