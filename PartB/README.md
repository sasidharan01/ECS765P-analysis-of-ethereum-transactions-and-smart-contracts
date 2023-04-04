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

|     | Address                                    | Value                      | Rank |
| --- | ------------------------------------------ | -------------------------- | ---- |
| 0   | 0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444 | 84155363699941767867374641 | 1    |
| 1   | 0x7727e5113d1d161373623e5f49fd568b4f543a9e | 45627128512915344587749920 | 2    |
| 2   | 0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef | 42552989136413198919298969 | 3    |
| 3   | 0xbfc39b6f805a9e40e77291aff27aee3c96915bdd | 21104195138093660050000000 | 4    |
| 4   | 0xe94b04a0fed112f3664e45adb2b8915693dd5ff3 | 15543077635263742254719409 | 5    |
| 5   | 0xabbb6bebfa05aa13e908eaa492bd7a8343760477 | 10719485945628946136524680 | 6    |
| 6   | 0x341e790174e3a4d35b65fdc067b6b5634a61caea | 8379000751917755624057500  | 7    |
| 7   | 0x58ae42a38d6b33a1e31492b60465fa80da595755 | 2902709187105736532863818  | 8    |
| 8   | 0xc7c7f6660102e9a1fee1390df5c76ea5a5572ed3 | 1238086114520042000000000  | 9    |
| 9   | 0xe28e72fcf78647adce1f1252f240bbfaebd63bcc | 1172426432515823142714582  | 10   |
