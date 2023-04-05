import os
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    # Initialize Spark session
    spark = SparkSession.builder.appName("Ethereum").getOrCreate()

    # Check the format of transactions dataset
    def verify_transactions(line):
        try:
            fields = line.split(",")
            if len(fields) != 15:
                return False
            int(fields[11])
            float(fields[9])
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
            return True
        except:
            return False

    # Fetch S3 environment variables
    s3_data_repository_bucket = os.environ["DATA_REPOSITORY_BUCKET"]
    s3_endpoint_url = os.environ["S3_ENDPOINT_URL"] + \
        ":" + os.environ["BUCKET_PORT"]
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

    # Fetch scams.csv file from S3 bucket
    scams = spark.sparkContext.textFile(
        "s3a://"
        + s3_data_repository_bucket
        + "/ECS765/ethereum-parvulus/scams.csv"
    )

    # Fetch transactions.csv file from S3 bucket
    transactions = spark.sparkContext.textFile(
        "s3a://"
        + s3_data_repository_bucket
        + "/ECS765/ethereum-parvulus/transactions.csv"
    )

    # Filter scams dataset to remove malformed records
    scams_filtered = scams.filter(verify_scams)

    # Filter scams dataset to remove malformed records
    scams_transformed = scams_filtered.map(
        lambda x: (x.split(",")[6], (x.split(",")[0],
                   x.split(",")[4], x.split(",")[7]))
    )

    # Filter transactions to remove malformed records
    transactions_filtered = transactions.filter(verify_transactions)
    
    # Transform scams dataset into key-value pairs
    transactions_transformed = transactions_filtered.map(
        lambda x: (
            x.split(",")[6],
            (
                time.strftime("%m/%Y", time.gmtime(int(x.split(",")[11]))),
                float(x.split(",")[7]),
            ),
        )
    )

    # Join transactions and scams datasets required to perform necessary transformations
    transactions_scams_joined = transactions_transformed.join(
        scams_transformed)
    
    category_wise_mapped = transactions_scams_joined.map(lambda l: (l[1][1][1], l[1][0][1]))
    
    category_wise_reduced = category_wise_mapped.reduceByKey(operator.add).sortBy(lambda a: -a[1])
    
    """
    Transform the joined dataset to map scam category, status,
    and month to transaction values
    """
    category_status_mapping = transactions_scams_joined.map(
        lambda x: ((x[1][1][1], x[1][1][2], x[1][0][0]), x[1][0][1])
    )
    """
    Calculate the total transaction value for each scam category, 
    status, and month
    """
    category_status_reduced = category_status_mapping.reduceByKey(operator.add)

    """
    Create a new RDD that maps the category and month to the sum of all 
    transaction values that occurred during that month for that category.
    """
    category_mapping = transactions_scams_joined.map(
        lambda x: ((x[1][1][1], x[1][0][0]), x[1][0][1]))
    category_reduced = category_mapping.reduceByKey(operator.add)

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
        s3_bucket, "ethereum_scam_analysis_" + 
        date_time +
        "/category_ether.txt"
    )
    obj.put(Body=json.dumps(category_wise_reduced.collect()))
    
    obj = bucket.Object(
        s3_bucket, "ethereum_scam_analysis_" +
        date_time + "/ether_category_status_time.txt"
    )
    obj.put(Body=json.dumps(category_status_reduced.collect()))

    obj = bucket.Object(
        s3_bucket, "ethereum_scam_analysis_" + 
        date_time +
        "/ether_category_time.txt"
    )
    obj.put(Body=json.dumps(category_reduced.collect()))

    spark.stop()
