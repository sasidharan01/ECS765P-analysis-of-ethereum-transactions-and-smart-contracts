import os
import time
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    # Initialize spark session
    spark = SparkSession.builder.appName("Ethereum").getOrCreate()

    # Check the format of transactions dataset
    def verify_transactions(line):
        try:
            fields = line.split(",")
            if len(fields) != 15:
                return False
            int(fields[11])
            str(fields[6])
            float(fields[7])
            return True
        except:
            return False

    # Check the format of scams dataset
    def verify_scams(line):
        try:
            fields = line.split(",")
            if len(fields) != 8:
                return False
            int(fields[0])
            str(fields[4])
            str(fields[6])
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

    # Fetch transactions file from S3 bucket
    transactions = spark.sparkContext.textFile(
        "s3a://"
        + s3_data_repository_bucket
        + "/ECS765/ethereum-parvulus/transactions.csv"
    )

    # Fetch scams file from S3 bucket
    scams = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.csv"
    )

    # Filter transactions and scams datasets to remove malformed records
    transactions_filetered = transactions.filter(verify_transactions)
    scams_filtered = scams.filter(verify_scams)

    # Transform scams dataset into key-value pairs
    scams_transformed = scams_filtered.map(
        lambda x: (x.split(",")[6], (x.split(",")[0], x.split(",")[4]))
    )

    # Transform transactions dataset into key-value pairs
    transactions_transformed = transactions_filetered.map(
        lambda x: (x.split(",")[6], float(x.split(",")[7]))
    )

    # Join transactions and scams datasets and perform necessary transformations
    transactions_scams_joined = transactions_transformed.join(scams_transformed)

    # Transform the joined dataset to map scam types and addresses to transaction values
    transactions_scams_transformed = transactions_scams_joined.map(
        lambda x: ((x[1][1][0], x[1][1][1]), x[1][0])
    )

    # Calculate the total value of transactions for each scam type and address
    popular_scams = transactions_scams_transformed.reduceByKey(lambda x, y: x + y)

    """
    Transform popular_scams dataset to map scam types and addresses 
    to total transaction values
    """
    popular_scams = popular_scams.map(lambda x: ((x[0][0], x[0][1]), float(x[1])))
    top_popular_scams = popular_scams.takeOrdered(15, key=lambda x: -1 * x[1])

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

    obj = bucket.Object(
        s3_bucket, "ethereum_scam_analysis_" + date_time + "/popular_scams.txt"
    )
    obj.put(Body=json.dumps(top_popular_scams))

    spark.stop()
