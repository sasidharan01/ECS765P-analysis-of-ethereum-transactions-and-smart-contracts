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
            float(fields[9])
            float(fields[11])
            return True
        except:
            return False

    # Check the format of contracts dataset
    def verify_contracts(line):
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

    # Fetch transactions.csv file from S3 bucket
    transactions = spark.sparkContext.textFile(
        "s3a://"
        + s3_data_repository_bucket
        + "/ECS765/ethereum-parvulus/transactions.csv"
    )

    # Fetch contracts.csv file from S3 bucket
    contracts = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv"
    )

    transactions_filtered = transactions.filter(verify_transactions)
    contracts_filtered = contracts.filter(verify_contracts)

    # Extract and aggregate features for average gas price calculation
    def map_gas_price(line):
        [
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            gas_price,
            _,
            block_timestamp,
            _,
            _,
            _,
        ] = line.split(",")
        date = time.strftime("%m/%Y", time.gmtime(int(block_timestamp)))
        gp = float(gas_price)
        return (date, (gp, 1))

    # Reduce transactions data to calculate average gas price per month
    transactions_gas_price = transactions_filtered.map(map_gas_price)
    transactions_gas_price_reduced = transactions_gas_price.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    average_gas_price = transactions_gas_price_reduced.sortByKey(ascending=True)
    average_gas_price = average_gas_price.map(lambda x: (x[0], str(x[1][0] / x[1][1])))

    # Map transactions and contracts data to calculate average gas used per smart contract per month
    def map_gas_used(line):
        [
            _,
            _,
            _,
            _,
            _,
            _,
            to_address,
            _,
            gas,
            _,
            _,
            block_timestamp,
            _,
            _,
            _,
        ] = line.split(",")
        date = time.strftime("%m/%Y", time.gmtime(int(block_timestamp)))
        g = float(gas)
        s = str(to_address)
        return (s, (date, g))

    # Reduce transactions data by date and calculate average gas used
    transactions_gas_used = transactions_filtered.map(map_gas_used)
    
    # filter the contracts dataset to only contain smart contract addresses
    contracts_filtered = contracts_filtered.map(lambda x: (x.split(",")[0], 1))
    transactions_contracts_joined = transactions_gas_used.join(contracts_filtered)
    transactions_contracts_mapped = transactions_contracts_joined.map(lambda x: (x[1][0][0], (x[1][0][1], x[1][1])))
    transactions_reduced = transactions_contracts_mapped.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    average_gas_used = transactions_reduced.map(lambda x: (x[0], str(x[1][0] / x[1][1])))
    average_gas_used = average_gas_used.sortByKey(ascending=True)

    # Create resource objects for S3 bucket
    bucket = boto3.resource(
        "s3",
        endpoint_url="http://" + s3_endpoint_url,
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )

    # Store results in S3 bucket
    now = datetime.now()  # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    # store the average gas price results
    obj_avg_price = bucket.Object(
        s3_bucket, "ethereum_gas_guzzlers_" + date_time + "/gas_price_avg.txt"
    )
    obj_avg_price.put(Body=json.dumps(average_gas_price.take(100)))
    
    # store the average gas used results
    obj_avg_gas = bucket.Object(
        s3_bucket, "ethereum_gas_guzzlers_" + date_time + "/gas_used_avg.txt"
    )
    obj_avg_gas.put(Body=json.dumps(average_gas_used.take(100)))

    spark.stop()
