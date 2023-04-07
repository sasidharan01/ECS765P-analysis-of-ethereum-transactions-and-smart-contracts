## Part B. Top Ten Most Popular Services (25%)

#### Objectives:

To evaluate the top 10 smart contracts by total Ether received. This includes joining the **address** field in the contracts dataset to the **to_address** in the transactions dataset to determine how much ether a contract has received.

#### Data Source:

The data used in this analysis was fetched from two CSV files stored in an S3 bucket. The highligted fields in the data schema is used in the source code to obtain the results.

- **_transactions.csv:_**
  [hash, nonce, block_hash, block_number, transaction_index, from_address, **to_address**, **value**, gas, gas_price, input, block_timestamp, max_fee_per_gas, max_priority_fee_per_gas, transaction_type]

- **_contracts.csv:_**
  [**address**, bytecode, function_sighashes, is_erc20, is_erc721, block_number]

#### Source Code:

```sh
PartB
├── README.md
├── output
│   ├── top10_smart_contracts.md
│   └── top_smart_contracts.txt
├── top-smart-contracts.py # source code
└── top_smart_contracts.ipynb # source code for generating table
```

#### Execution:

1. Execute the spark application.

   ```sh
   ccc create spark top-smart-contracts.py -s
   ```

2. Stream the logs of the drive container.

   ```sh
   oc logs -f spark top-smart-contracts-spark-app-driver
   ```

#### Methodology:

1.  **_Initialize Spark session and S3 environment variables:_** Initialized a Spark session using the SparkSession object. Fetched environment variables related to the S3 bucket, such as the data repository bucket, endpoint URL, access key ID, secret access key, and bucket name. Configured Hadoop settings for the Spark session using the hadoopConf object.

2.  **_Fetch transactions.csv and contracts.csv file and verify the same:_** The `transactions.csv` and `contracts.csv` is fetched from the S3 bucket using the `textFile()` method of the Spark context. The methods `verify_transactions()` and `verify_contracts()` respectively reads every line of transactions and contracts as input and return `True` if the data type is correct and `False` if its invalid.

3.  **_Map transactions and contracts dataset:_** The transactions dataset is then transformed using a map operation that extracts the Ethereum address and transaction value from each line. Similarly, the contracts dataset is transformed using a `map()` operation that associates a value of 1 with each contract address.

4.  **_Reduce transactions and join with contracts:_** The `reduceByKey()` operation is then used to group the transaction values by address, and the `join()` operation is used to join the grouped transaction values with the contracts dataset. Finally, a map operation is used to extract the address and total transaction value from the joined dataset. The `takeOrdered()` method is used to get the top 10 smart contracts based on their total transaction value.

5.  **_Storing the results in an S3 bucket:_** The results are then written to S3 bucket as a TXT file `top_smart_contracts.txt` using the boto3 library and the Spark session is stopped using `stop()` method.

#### Output:

The following are the top 10 smart contracts by total Ether received. The code used to generate this table can be found in [`PartB/top_smart_contracts.ipynb`](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartB/top_smart_contracts.ipynb)

![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartB/output/top_contracts.png?raw=true)
