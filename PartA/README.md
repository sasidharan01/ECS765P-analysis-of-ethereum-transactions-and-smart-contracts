## Part A. Time Analysis (25%)

### 1. Total Transactions

#### Objective:
To create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.

#### Data Source:
The data used in this analysis was fetched from a CSV file stored in an S3 bucket:

_**transactions.csv:**_  The transactions file contains information about Ethereum transactions.

#### Execution:

1. Execute the spark application.

    ```sh
    ccc create spark transactions-total.py -s
    ```
2. Stream the logs of the drive container.

    ```sh
    oc logs -f spark transactions-total-spark-app-driver
    ```
  
### Methodology:

- The Spark script begins by initializing a Spark session.

- The Spark script then fetches environment variables required for S3 storage [S3 endpoint URL, access key ID, secret access key, and the bucket name]. The Hadoop configuration is then set to allow Spark to access the S3 bucket.

- The transactions and contracts files are then fetched from the S3 bucket using the ``textFile()`` method. The ``verify_transactions`` and ``verify_contracts ``methods are used to filter out any lines in the datasets that do not conform to their respective formats.

- The transactions dataset is then transformed using a map operation that extracts the Ethereum address and transaction value from each line. 
- The contracts dataset is transformed using a map operation that associates a value of 1 with each contract address. 
- The ``reduceByKey()`` operation is then used to group the transaction values by address, and the join() operation is used to join the grouped transaction values with the contracts dataset. 
- Finally, a map operation is used to extract the address and total transaction value from the joined dataset.
- The ``takeOrdered()`` method is used to get the top 10 smart contracts based on their total transaction value.
- The results are then written to S3 bucket as a TXT file using the boto3 library, and the Spark session is stopped.