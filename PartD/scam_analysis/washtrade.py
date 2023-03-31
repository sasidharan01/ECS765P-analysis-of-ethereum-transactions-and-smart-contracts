import os
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as pyspark_fnc

if __name__ == "__main__":

    # Initialize spark session
    spark = SparkSession.builder.appName("Ethereum").getOrCreate()

    # Check the format of blocks data
    def verify_transactions(line):
        try:
            fields = line.split(",")
            if len(fields) != 15:
                return False

            float(fields[7])
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

    # Filter transactions data
    transactions_filtered = transactions.filter(verify_transactions)

    # Transform transactions data
    transactions_transformed = transactions_filtered.map(
        lambda x: (x.split(",")[5], x.split(",")[6], x.split(",")[7], x.split(",")[11])
    )

    # Convert transactions RDD to DataFrame
    col = ["from_addr", "to_addr", "value", " timestamp"]
    df = transactions_transformed.toDF(col)
    df = df.filter(pyspark_fnc.col("from_addr") == pyspark_fnc.col("to_addr"))

    # Transform filtered DataFrame back to RDD and map to key-value pairs
    transactions_rdd = df.rdd.map(lambda x: ((x[0], x[1]), float(x[2])))

    # Reduce transactions by address pair and sum transaction values
    output = transactions_rdd.reduceByKey(lambda x, y: x + y)

    # Identify top 10 wash trades (self-transactions) by transaction value
    top_washtrade = output.takeOrdered(10, key=lambda x: -x[1])

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
        s3_bucket, "ethereum_washtrade_" + date_time + "/top_washtrade.txt"
    )
    obj.put(Body=json.dumps(top_washtrade))

    spark.stop()
