
# Analysis of Ethereum Transactions and Smart Contracts

**Module:** ECS765P - Big Data Processing - 2022/23

## Part A. Time Analysis (25%)

### 1.1 Total Transactions

#### Objective:
To create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.

#### Data Source:

The data used in this analysis was fetched from the following CSV files stored in S3 bucket. The highligted fields in the data schema is used in the source code to obtain the results.

- ***transactions.csv:*** [hash, nonce, block_hash, block_number, transaction_index, from_address, to_address, **value**, gas, gas_price, input, **block_timestamp**, max_fee_per_gas, max_priority_fee_per_gas, transaction_type]

#### Source Code:
```sh
PartA
├── README.md
├── output
│   ├── transactions_avg.png 
│   ├── transactions_avg.txt
│   ├── transactions_total.png
│   └── transactions_total.txt
├── transactions-average.py
├── transactions-total.py # source code
├── transactions_average.ipynb
└── transactions_total.ipynb # source code for plots
```
#### Execution:

1. Execute the spark application.

    ```sh
    ccc create spark transactions-total.py -s
    ```
2. Stream the logs of the drive container.

    ```sh
    oc logs -f spark transactions-total-spark-app-driver
    ```
  
#### Methodology:

1.  ***Initialize Spark session and S3 environment variables:*** Initialized a  Spark session using the SparkSession object. Fetched environment variables related to the S3 bucket, such as the data repository bucket, endpoint URL, access key ID, secret access key, and bucket name. Configured Hadoop settings for the Spark session using the hadoopConf object.

2.  ***Fetch transactions.csv file and verify transactions***  The transactions.csv is fetched from the S3 bucket using the textFile() method of the Spark context. The methods `verify_transactions()` reads every line of transactions as input and return `True` if the data is in the correct format and `False` otherwise.

3. ***Map and Reduce transactions data:*** Each transaction is mapped  `map()`to a tuple containing key as Month/Year of the block timestamp and value of 1. The resultant data is reduced by key `reduceByKey()` (Month/Year) to get the total number of transactions every month.
4. ***Store results in S3 bucket:*** The results are then written to S3 bucket as a TXT file `transactions_total.txt` using the boto3 library and the Spark session is stopped using `stop()` method.

#### Output:
The bar plot showing the total number of transactions occurring each month between the start and end of the dataset is obtained. The code used to obtain this graph can be found in [`PartA/transactions_total.ipynb`](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartA/transactions_total.ipynb)

![alt txt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartA/output/transactions_total.png)

### 1.2 Average Transactions

#### Objective:
To create a bar plot showing the average value of transaction in each month between the start and end of the dataset.

#### Data Source:

The data used in this analysis was fetched from the following CSV files stored in an S3 bucket. The highligted field in the data schema is used in source code to obtain the results.

***transactions.csv:*** [hash, nonce, block_hash, block_number, transaction_index, from_address, to_address, value, gas, gas_price, input, **block_timestamp**, max_fee_per_gas, max_priority_fee_per_gas, transaction_type]

#### Source Code:
```sh
PartA
├── README.md
├── output
│   ├── transactions_avg.png 
│   ├── transactions_avg.txt
│   ├── transactions_total.png
│   └── transactions_total.txt
├── transactions-average.py # source code
├── transactions-total.py
├── transactions_average.ipynb # source code for plots
└── transactions_total.ipynb
```
#### Execution:

1. Execute the spark application.

    ```sh
    ccc create spark transactions-average.py -s
    ```
2. Stream the logs of the drive container.

    ```sh
    oc logs -f spark transactions-average-spark-app-driver
    ```

#### Methodology:

1.  ***Initialize Spark session and S3 environment variables:*** Initialized a  Spark session using the SparkSession object. Fetched environment variables related to the S3 bucket, such as the data repository bucket, endpoint URL, access key ID, secret access key, and bucket name. Configured Hadoop settings for the Spark session using the hadoopConf object.
    
2.  ***Fetch transactions.csv file and verify transactions***  The transactions.csv is fetched from the S3 bucket using the textFile() method of the Spark context. The methods `verify_transactions()` reads every line of transactions as input and return `True` if the data is in the correct format and `False` otherwise.
    
5.  ***Defined a method to extract features from the transactions data and aggregate values for average transaction value calculation:*** This method `mapping()` takes every single single line of transactions data as input and returns a tuple of date and a tuple of transaction value and count. The date is extracted from the timestamp.
    
8.  ***Mapping  and Reducing the transactions data to calculate the average transactions each month:*** Using the above defined `mapping()` method required features are extracted and the aggregate values for calculating the average transaction value per month. This resulting RDD contains tuples of date and a tuple of transaction value and count. The `reduceByKey()` method is used to reduce the transactions data by date of the RDD object.  The RDD obtained after reduce contains tuples of date and a tuple of total transaction value and count.
    
10.  ***Storing the results in an S3 bucket:*** The results are then written to S3 bucket as a TXT file `transactions_avg.txt` using the boto3 library and the Spark session is stopped using `stop()` method.

#### Output:
The bar plot showing the average value of transaction in each month between the start and end of the dataset is obtained. The code used to obtain this graph can be found in [`PartA/transactions_avg.ipynb`](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartA/transactions_avg.ipynb)

![alt txt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartA/output/transactions_avg.png)

## Part B. Top Ten Most Popular Services (25%)

#### Objectives:

To evaluate the top 10 smart contracts by total Ether received. This includes joining the **address** field in the contracts dataset to the **to_address** in the transactions dataset to determine how much ether a contract has received.

#### Data Source:

The data used in this analysis was fetched from two CSV files stored in an S3 bucket. The highligted fields in the data schema is used in the source code to obtain the results.

- **_transactions.csv:_**
  [hash, nonce, block_hash, **block_number**, transaction_index, from_address, **to_address**, **value**, gas, gas_price, input, block_timestamp, max_fee_per_gas, max_priority_fee_per_gas, transaction_type]

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

2.  **_Fetch transactions.csv and contracts.csv file and verify the same:_** The `transactions.csv` and `contracts.csv` is fetched from the S3 bucket using the `textFile()` method of the Spark context. The methods `verify_transactions()` and `verify_contracts()` respectively reads every line of transactions and contracts as input and return `True` if the data is in the correct format and `False` otherwise.

3.  **_Map transactions and contracts dataset:_** The transactions dataset is then transformed using a map operation that extracts the Ethereum address and transaction value from each line. Similarly, the contracts dataset is transformed using a map operation that associates a value of 1 with each contract address.

4.  **_Reduce transactions and join with contracts:_** The `reduceByKey()` operation is then used to group the transaction values by address, and the join() operation is used to join the grouped transaction values with the contracts dataset. Finally, a map operation is used to extract the address and total transaction value from the joined dataset. The `takeOrdered()` method is used to get the top 10 smart contracts based on their total transaction value.

5.  **_Storing the results in an S3 bucket:_** The results are then written to S3 bucket as a TXT file `top_smart_contracts.txt` using the boto3 library and the Spark session is stopped using `stop()` method.

#### Output:

The following are the top 10 smart contracts by total Ether received.

| Address                                    |                      Value |   Rank |
|--------------------------------------------|----------------------------|--------|
| 0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444 | 84155363699941767867374641 |      1 |
| 0x7727e5113d1d161373623e5f49fd568b4f543a9e | 45627128512915344587749920 |      2 |
| 0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef | 42552989136413198919298969 |      3 |
| 0xbfc39b6f805a9e40e77291aff27aee3c96915bdd | 21104195138093660050000000 |      4 |
| 0xe94b04a0fed112f3664e45adb2b8915693dd5ff3 | 15543077635263742254719409 |      5 |
| 0xabbb6bebfa05aa13e908eaa492bd7a8343760477 | 10719485945628946136524680 |      6 |
| 0x341e790174e3a4d35b65fdc067b6b5634a61caea |  8379000751917755624057500 |      7 |
| 0x58ae42a38d6b33a1e31492b60465fa80da595755 |  2902709187105736532863818 |      8 |
| 0xc7c7f6660102e9a1fee1390df5c76ea5a5572ed3 |  1238086114520042000000000 |      9 |
| 0xe28e72fcf78647adce1f1252f240bbfaebd63bcc |  1172426432515823142714582 |     10 |




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

3. ***Extract block features and reduce by miner address:*** The filtered data is transformed using `get_block_features` function to extract the miner address and block size. The block data is then reduced by the miner address to calculate the total block size mined by each miner. The top 10 miners are identified using `takeOrdered` method using a lambda function that sorts the miners by the total block size mined.

4.  _**Store results in S3 bucket:**_  The results are then written to S3 bucket as a TXT file  `top_miners.txt`  using the boto3 library and the Spark session is stopped using  `stop()`  method.

#### Output:

The following are the top 10 by block size.

| Address                                    |       Value |   Rank |
|--------------------------------------------|-------------|--------|
| 0xea674fdde714fd979de3edf0f56aa9716b898ec8 | 17453393724 |      1 |
| 0x829bd824b016326a401d083b33d092293333a830 | 12310472526 |      2 |
| 0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c |  8825710065 |      3 |
| 0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5 |  8451574409 |      4 |
| 0xb2930b35844a230f00e51431acae96fe543a0347 |  6614130661 |      5 |
| 0x2a65aca4d5fc5b5c859090a6c34d164135398226 |  3173096011 |      6 |
| 0xf3b9d2c81f2b24b0fa0acaaa865b7d9ced5fc2fb |  1152847020 |      7 |
| 0x4bb96091ee9d802ed039c4d1a5f6216f90f81b01 |  1134151226 |      8 |
| 0x1e9939daaad6924ad004c2560e90804164900341 |  1080436358 |      9 |
| 0x61c808d82a3ac53231750dadc13c777b59310bd9 |   692942577 |     10 |

