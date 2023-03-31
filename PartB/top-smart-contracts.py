import os
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    # Initialize spark session
    spark = SparkSession.builder.appName("Ethereum").getOrCreate()

    # Check the format of transactions and contracts data
    def check_transactions(line):
        try:
            fields = line.split(",")
            if len(fields) != 15:
                return False
            int(fields[3])
            return True
        except:
            return False

    def check_contracts(line):
        try:
            fields = line.split(",")
            if len(fields) != 6:
                return False
            else:
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

    # Fetch transactions and contracts files from S3 bucket
    transactions = spark.sparkContext.textFile(
        "s3a://"
        + s3_data_repository_bucket
        + "/ECS765/ethereum-parvulus/transactions.csv"
    )
    contracts = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv"
    )

    transactions_filtered = transactions.filter(check_transactions)
    contracts_filtered = contracts.filter(check_contracts)

    transactions_transformed = transactions_filtered.map(
        lambda x: (x.split(",")[6], int(x.split(",")[7]))
    )
    contracts_transformed = contracts_filtered.map(lambda x: (x.split(",")[0], 1))
    transaction_reduced = transactions_transformed.reduceByKey(lambda x, y: x + y)
    transcation_joined = transaction_reduced.join(contracts_transformed)
    address_value = transcation_joined.map(lambda x: (x[0], x[1][0]))

    # Top 10 Smart Contract
    top_smart_contract = address_value.takeOrdered(10, key=lambda l: -1 * l[1])

    # Create resource object for S3 bucket
    bucket = boto3.resource(
        "s3",
        endpoint_url="http://" + s3_endpoint_url,
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )

    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    obj = bucket.Object(
        s3_bucket, "ethereum_smart_contracts" + date_time + "/top_smart_contracts.json"
    )
    obj.put(Body=json.dumps(top_smart_contract))

    spark.stop()
