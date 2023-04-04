import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession, Row
from datetime import datetime
from pyspark.sql.functions import from_unixtime, month, year

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Olympic").getOrCreate()

    def good_line_tra(line):
        try:
            fields = line.split(",")
            if len(fields) != 15:
                return False
            int(fields[11])
            float(fields[9])
            return True
        except:
            return False

    def good_line_con(line):
        try:
            fields = line.split(",")
            if len(fields) != 6:
                return False
            bool(fields[3])
            return True
        except:
            return False

    # shared read-only object bucket containing datasets
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

    my_bucket_resource = boto3.resource(
        "s3",
        endpoint_url="http://" + s3_endpoint_url,
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )

    lines_con = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv"
    )
    # For testing small data set uncomment below lines and comment above line
    # lines_con = spark.sparkContext.textFile("s3a://" + s3_bucket + "/ethereum" + "/contracts_sample.csv")
    clean_lines_con = lines_con.filter(good_line_con)
    address_con = clean_lines_con.map(lambda l: (l.split(",")[0], 1))
    # (address, 1)

    lines_tra = spark.sparkContext.textFile(
        "s3a://"
        + s3_data_repository_bucket
        + "/ECS765/ethereum-parvulus/transactions.csv"
    )
    # For testing small data set uncomment below lines and comment above line
    # lines_tra = spark.sparkContext.textFile("s3a://" + s3_bucket + "/ethereum" + "/transactions_sample.csv")
    clean_lines_tra = lines_tra.filter(good_line_tra)
    address_tra = clean_lines_tra.map(
        lambda l: (
            l.split(",")[6],
            (
                time.strftime("%m/%Y", time.gmtime(int(l.split(",")[11]))),
                float(l.split(",")[7]),
                float(l.split(",")[8]),
                float(l.split(",")[9]),
            ),
        )
    )
    # (address, (time, value, gas, gas_price))

    con_tra_joined = address_con.join(address_tra)

    print("****************************" * 50, con_tra_joined.take(10))

    # [('0x6baf48e1c966d16559ce2dddb616ffa72004851e', (1, ('08/2015', 5000000000000000.0, 21000.0, 500000000000.0))), ('0x6baf48e1c966d16559ce2dddb616ffa72004851e', (1, ('08/2015', 5000000000000000.0, 21000.0, 500000000000.0))), ('0x6baf48e1c966d16559ce2dddb616ffa72004851e', (1, ('08/2015', 5000000000000000.0, 21000.0, 500000000000.0))), ('0x6baf48e1c966d16559ce2dddb616ffa72004851e', (1, ('08/2015', 5000000000000000.0, 21000.0, 500000000000.0))), ('0x6baf48e1c966d16559ce2dddb616ffa72004851e', (1, ('08/2015', 5000000000000000.0, 21000.0, 500000000000.0))), ('0x6baf48e1c966d16559ce2dddb616ffa72004851e', (1, ('08/2015', 5000000000000000.0, 21000.0, 500000000000.0))), ('0x6baf48e1c966d16559ce2dddb616ffa72004851f', (1, ('08/2015', 5000000000000000.0, 21000.0, 500000000000.0))), ('0x6baf48e1c966d16559ce2dddb616ffa72004851f', (1, ('08/2015', 5000000000000000.0, 21000.0, 500000000000.0)))]

    time_gas_price_map = con_tra_joined.map(lambda l: (l[1][1][0], l[1][1][3]))

    # print('time_gas_price_map----------------------------'*20, time_gas_price_map.collect())
    # (time, gas_price)
    # [('08/2015', 500000000000.0), ('08/2015', 500000000000.0), ('08/2015', 500000000000.0), ('08/2015', 500000000000.0), ('08/2015', 500000000000.0), ('08/2015', 500000000000.0), ('08/2015', 500000000000.0), ('08/2015', 500000000000.0)]
    time_gas_price_reduce = time_gas_price_map.reduceByKey(operator.add)

    print(
        "time_gas_price_reduce----------------------------" * 20,
        time_gas_price_reduce.collect(),
    )
    # [('08/2015', 4000000000000.0)]

    my_result_object = my_bucket_resource.Object(
        s3_bucket, "ethereum" + "/partdgasguzzlers_time_gas_price.txt"
    )
    my_result_object.put(Body=json.dumps(time_gas_price_reduce.collect()))

    time_gas_map = con_tra_joined.map(lambda l: (l[1][1][0], l[1][1][2]))

    # print('time_gas_map?????????????????????????????????'*20, time_gas_map.collect())
    # (time, gas)
    # [('08/2015', 21000.0), ('08/2015', 21000.0), ('08/2015', 21000.0), ('08/2015', 21000.0), ('08/2015', 21000.0), ('08/2015', 21000.0), ('08/2015', 21000.0), ('08/2015', 21000.0)]
    time_gas_reduce = time_gas_map.reduceByKey(operator.add)

    print(
        "time_gas_reduce???????????????????????????????" * 20, time_gas_reduce.collect()
    )
    # [('08/2015', 168000.0)]

    my_result_object = my_bucket_resource.Object(
        s3_bucket, "ethereum" + "/partdgasguzzlers_time_gas.txt"
    )
    my_result_object.put(Body=json.dumps(time_gas_reduce.collect()))

    time_value_map = con_tra_joined.map(lambda l: (l[1][1][0], l[1][1][1]))

    # print('time_value_map+++++++++++++++++++++++++++++++++++++'*20, time_value_map.collect())
    # (time, value)
    # [('08/2015', 5000000000000000.0), ('08/2015', 5000000000000000.0), ('08/2015', 5000000000000000.0), ('08/2015', 5000000000000000.('08/2015', 5000000000000000.0), ('08/2015', 5000000000000000.0), ('08/2015', 5000000000000000.0), ('08/2015', 5000000000000000.0)]
    time_value_reduce = time_value_map.reduceByKey(operator.add)

    print(
        "time_value_reduce++++++++++++++++++++++++++++++++++" * 20,
        time_value_reduce.collect(),
    )
    # [('08/2015', 4e+16)]

    my_result_object = my_bucket_resource.Object(
        s3_bucket, "ethereum" + "/partdgasguzzlers_time_value.txt"
    )
    my_result_object.put(Body=json.dumps(time_value_reduce.collect()))

    time_gas_price_count_map = con_tra_joined.map(
        lambda l: ("Average", (l[1][1][3], 1))
    )

    # print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'*40, time_gas_price_count_map.collect())

    # [('Average', (500000000000.0, 1)), ('Average', (500000000000.0, 1)), ('Average', (500000000000.0, 1)), ('Average', (500000000000.0, 1)), ('Average', (500000000000.0, 1)), ('Average', (500000000000.0, 1)), ('Average', (500000000000.0, 1)), ('Average', (500000000000.0, 1))]

    average_gas_price = time_gas_price_count_map.reduceByKey(
        lambda x, y: (x[0] + y[0], x[1] + y[1])
    ).map(lambda x: (x[0], x[1][0] / x[1][1]))

    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" * 30, average_gas_price.collect())

    my_result_object = my_bucket_resource.Object(
        s3_bucket, "ethereum" + "/partdgasguzzlers_average_gas_price.txt"
    )
    my_result_object.put(Body=json.dumps(average_gas_price.collect()))

    spark.stop()
