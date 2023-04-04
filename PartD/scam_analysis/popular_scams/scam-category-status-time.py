import os
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Ethereum").getOrCreate()

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

    def verify_scams(line):
        try:
            fields = line.split(",")
            if len(fields) != 8:
                return False
            int(fields[0])
            return True
        except:
            return False

    s3_data_repository_bucket = os.environ["DATA_REPOSITORY_BUCKET"]
    s3_endpoint_url = os.environ["S3_ENDPOINT_URL"] + \
        ":" + os.environ["BUCKET_PORT"]
    s3_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    s3_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    s3_bucket = os.environ["BUCKET_NAME"]

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    scams = spark.sparkContext.textFile(
        "s3a://"
        + s3_data_repository_bucket
        + "/ECS765/ethereum-parvulus/scams.csv"
    )
    transactions = spark.sparkContext.textFile(
        "s3a://"
        + s3_data_repository_bucket
        + "/ECS765/ethereum-parvulus/transactions.csv"
    )

    scams_filtered = scams.filter(verify_scams)
    scams_transformed = scams_filtered.map(
        lambda x: (x.split(",")[6], (x.split(",")[0],
                   x.split(",")[4], x.split(",")[7]))
    )

    transactions_filtered = transactions.filter(verify_transactions)
    transactions_transformed = transactions_filtered.map(
        lambda x: (
            x.split(",")[6],
            (
                time.strftime("%m/%Y", time.gmtime(int(x.split(",")[11]))),
                float(x.split(",")[7]),
            ),
        )
    )
    transactions_scams_transformed = transactions_transformed.join(
        scams_transformed)

    category_status_mapping = transactions_scams_transformed.map(
        lambda x: ((x[1][1][1], x[1][1][2], x[1][0][0]), x[1][0][1])
    )
    category_status_reduced = category_status_mapping.reduceByKey(operator.add)

    category_mapping = transactions_scams_transformed.map(
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
