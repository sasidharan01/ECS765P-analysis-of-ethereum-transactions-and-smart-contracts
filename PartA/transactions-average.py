import os
import time
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
            float(fields[7])
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

    def mapping(line):
        [_, _, _, _, _, _, _, value, _, _, _, block_timestamp, _, _, _] = line.split(
            ","
        )
        date = time.strftime("%m/%Y", time.gmtime(int(block_timestamp)))
        v = float(value)
        return (date, (v, 1))

    # Fetch transactions.csv file from S3 bucket
    transactions = spark.sparkContext.textFile(
        "s3a://"
        + s3_data_repository_bucket
        + "/ECS765/ethereum-parvulus/transactions.csv"
    )

    # Filter transactions data
    transactions_filtered = transactions.filter(verify_transaction)
    
    # Map transactions data to extract features and aggregate values for average transaction value calculation
    transactions_mapped = transactions_filtered.map(mapping)
    
    # Reduce transactions data by date to calculate average transaction value per month
    transactions_reduced = transactions_mapped.reduceByKey(
        lambda x, y: (x[0] + y[0], x[1] + y[1])
    )
    
    transactions_avg = transactions_reduced.map(
        lambda x: (x[0], str(x[1][0] / x[1][1]))
    )
    transactions_avg = transactions_avg.map(lambda x: ",".join(str(transaction) for transaction in x))

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
        s3_bucket, "ethereum_avg_" + date_time + "/transactions_avg.txt"
    )

    obj.put(Body=json.dumps(transactions_avg.take(100)))

    spark.stop()
