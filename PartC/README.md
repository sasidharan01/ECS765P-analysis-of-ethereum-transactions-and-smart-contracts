## Part C. Top Ten Most Active Miners (10%)

#### Objective:
To evaluate the top 10 miners by the size of the blocks mined, using an approach that does not require a join. This involved aggregating the blocks to determine the involvement of each miner in terms of the size of the blocks mined and aggregating the size for addresses in the miner field and adding each value from the reducer to a list. The list will then be sorted to obtain the most active (top 10) miners.

#### Data Source:

The data used in this code is fetched from CSV files stored in an S3 bucket. The highligted fields in the data schema is used in the source code to obtain the results.

- _**blocks.csv:**_ [number, hash, parent_hash, nonce, sha3_uncles, logs_bloom, transactions_root, state_root, receipts_root, **miner**, difficulty, total_difficulty, **size**, extra_data, gas_limit, gas_used, timestamp, transaction_count, base_fee_per_gas,]

#### Execution:

1. Execute the spark application.

    ```sh
    ccc create spark top-miners.py -s
    ```
2. Stream the logs of the drive container.

    ```sh
    oc logs -f spark top-miners-spark-app-driver
    ```
    
#### Methodology:

1.  **_Initialize Spark session and S3 environment variables:_** Initialized a Spark session using the SparkSession object. Fetched environment variables related to the S3 bucket, such as the data repository bucket, endpoint URL, access key ID, secret access key, and bucket name. Configured Hadoop settings for the Spark session using the hadoopConf object.

2. ***Fetch blocks.csv file and verify blocks data:***  The `blocks.csv` file is fetched from the S3 bucket and the data is filtered using `verify_blocks` method to remove malformed data.

3. ***Extract block features and reduce by miner address:*** The filtered data is transformed using `get_block_features` function to extract the miner address and block size. The block data is then reduced by the miner address to calculate the total block size mined by each miner. The top 10 miners are identified using `takeOrdered()` method using a lambda function that sorts the miners by the total block size mined.

4.  _**Store results in S3 bucket:**_  The results are then written to S3 bucket as a TXT file  `top_miners.txt`  using the boto3 library and the Spark session is stopped using  `stop()`  method.

#### Output:

The following are the top 10 by block size. The code used to generate this table can be found in [`PartC/top-miners.ipynb`](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartC/top_miners.ipynb)

![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartC/output/top_miners.png?raw=true)

