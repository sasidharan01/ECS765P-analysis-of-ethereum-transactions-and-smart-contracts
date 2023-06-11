
# Analysis of Ethereum Transactions and Smart Contracts

## ECS765P - Big Data Processing - 2022/23


## Table of Contents



1. [Part A. Time Analysis](#part-a-time-analysis-25)

    1.1. [Total Transactions](#11-total-transactions)
    
    1.2. [Average Transactions](#12-average-transactions)
    
2. [Part B. Top Ten Most Popular Services](#part-b-top-ten-most-popular-services-25)

3. [Part C. Top Ten Most Active Miners](#part-c-top-ten-most-active-miners-10)

4. [Part D. Data exploration](#part-d-data-exploration-40)

    4.1. [Scam Analysis](#41-scam-analysis)
    
    &nbsp;&nbsp;&nbsp;&nbsp;4.1.1. [Popular Scams](#411-popular-scams-20)
	
    4.2. [Miscellaneous Analysis](#42-miscellaneous-analysis)
    
    &nbsp;&nbsp;&nbsp;&nbsp;4.2.1. [Data Overhead](#421-data-overhead-20)
	
    &nbsp;&nbsp;&nbsp;&nbsp;4.2.2. [Gas Guzzlers](#422-gas-guzzlers-20)

## Part A. Time Analysis (25%)

### 1.1. Total Transactions

#### Objective:
To create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.

#### Data Source:

The data used in this analysis was fetched from the following CSV files stored in S3 bucket. The highligted fields in the data schema is used in the source code to obtain the results.

- ***transactions.csv:*** [hash, nonce, block_hash, block_number, transaction_index, from_address, to_address, value, gas, gas_price, input, **block_timestamp**, max_fee_per_gas, max_priority_fee_per_gas, transaction_type]

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

2.  ***Fetch transactions.csv file and verify transactions***  The transactions.csv is fetched from the S3 bucket using the textFile() method of the Spark context. The methods `verify_transactions()` reads every line of transactions as input and return `True` if the data  type is in the correct format and `False` if it is invalid.

3. ***Map and Reduce transactions data:*** Each transaction is mapped  `map()`to a tuple containing key as Month/Year of the block timestamp and value of 1. The resultant data is reduced by key `reduceByKey()` (Month/Year) to get the total number of transactions every month.
4. ***Store results in S3 bucket:*** The results are then written to S3 bucket as a TXT file `transactions_total.txt` using the boto3 library and the Spark session is stopped using `stop()` method.

#### Output:
The bar plot showing the total number of transactions occurring each month (sorted first on month and then year) between the start and end of the dataset is obtained. The code used to obtain this graph can be found in [`PartA/transactions_total.ipynb`](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartA/transactions_total.ipynb)

![alt txt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartA/output/transactions_total.png?raw=true)

### 1.2. Average Transactions

#### Objective:
To create a bar plot showing the average value of transaction in each month between the start and end of the dataset.

#### Data Source:

The data used in this analysis was fetched from the following CSV files stored in an S3 bucket. The highligted field in the data schema is used in source code to obtain the results.

***transactions.csv:*** [hash, nonce, block_hash, block_number, transaction_index, from_address, to_address, **value**, gas, gas_price, input, **block_timestamp**, max_fee_per_gas, max_priority_fee_per_gas, transaction_type]

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
    
2.  ***Fetch transactions.csv file and verify transactions***  The transactions.csv is fetched from the S3 bucket using the textFile() method of the Spark context. The methods `verify_transactions()` reads every line of transactions as input and return `True` if the data type is in the correct format and `False` if its invalid.
    
5.  ***Defined a method to extract features from the transactions data and aggregate values for average transaction value calculation:*** This method `mapping()` takes every single single line of transactions data as input and returns a tuple of date and a tuple of transaction value and count. The date is extracted from the timestamp.
    
8.  ***Mapping  and Reducing the transactions data to calculate the average transactions each month:*** Using the above defined `mapping()` method required features are extracted and the aggregate values for calculating the average transaction value per month. This resulting RDD contains tuples of date and a tuple of transaction value and count. The `reduceByKey()` method is used to reduce the transactions data by date of the RDD object.  The RDD obtained after reduce contains tuples of date and a tuple of total transaction value and count.
    
10.  ***Storing the results in an S3 bucket:*** The results are then written to S3 bucket as a TXT file `transactions_avg.txt` using the boto3 library and the Spark session is stopped using `stop()` method.

#### Output:
The bar plot showing the average value of transaction in each month (sorted first on month and then year) between the start and end of the dataset is obtained. The code used to obtain this graph can be found in [`PartA/transactions_average.ipynb`](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartA/transactions_average.ipynb)

![alt txt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartA/output/transactions_avg.png?raw=true)

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

## Part D. Data exploration (40%)

## 4.1. Scam Analysis

### 4.1.1. Popular Scams (20%)

#### Objective:
To analyze the provided scam dataset and determine the most lucrative form of scam. Investigate how this changes over time (generate graph) and examine any correlation with known scams going offline or becoming inactive. To provide the ID of the most lucrative scam and a graph illustrating the changes in ether received over time for the dataset.

#### Data Source:
The data used in this analysis was fetched from the following CSV files stored in an S3 bucket. The highligted fields in the data schema is used in the source code to obtain the results.

- ***transactions.csv:*** [hash, nonce, block_hash, block_number, transaction_index, from_address, **to_address**, **value**, gas,  gas_price, input, block_timestamp, max_fee_per_gas, max_priority_fee_per_gas, transaction_type]
- ***scams.csv:*** [**id**, name, url, coin, **category**, subcategory, description, **addresses**, reporter, ip, **status**]

#### Source Code:

```sh
PartD/scam_analysis/popular_scams
├── output
│   ├── active_scam.png
│   ├── category_ether.md
│   ├── category_ether.txt
│   ├── ether_category_status_time.txt
│   ├── ether_category_time.txt
│   ├── ether_time.png
│   ├── inactive_scam.png
│   ├── offline_scam.png
│   ├── popular_active_scams.md
│   ├── popular_inactive_scams.md
│   ├── popular_offline_scams.md
│   ├── popular_scams.md
│   ├── popular_scams.txt
│   ├── popular_suspended_scams.md
│   └── suspended_scam.png
├── popular-scams.py # Source code to get popular scams
├── scam-category-status-time.py # Source code to get category-status wise scams
├── scam.py
├── scams_category.ipynb
└── scams_category_status.ipynb
```
#### Execution:
1. Execute the spark application.

    ```sh
    ccc create spark popular-scams.py -d -s
    ccc create spark scam-category-status-time.py -d -s
    ```
2. Stream the logs of the drive container.

    ```sh
    oc logs -f spark popular-scams-spark-app-driver
    oc logs -f scam-category-status-time-spark-app-driver
    ```
#### Methodology:


1.  ***Initialize Spark session and S3 environment variables:*** Initialize a Spark session using the SparkSession object. Fetch environment variables related to the S3 bucket, such as the data repository bucket, endpoint URL, access key ID, secret access key, and bucket name. Configure Hadoop settings for the Spark session using the hadoopConf object.

2. ***Verify transactions and scams data:*** The methods `verify_transactions()` and `verify_contracts()` reads every line of transactions and contracts data respectively as input and return `True` if the data type is in the correct format and `False` if its invalid.

3.  ***Fetch transactions.csv and scams.csv files from S3 bucket:***  The `transactions.csv` and `scams.csv` files are fetched from the S3 bucket using the `textFile()` method of the Spark context.

4.  ***Transform Scam and Transactions Dataset:*** The scams dataset is transformed into key-value pairs where the key is the scam address and the value is a tuple containing the scam type and scam address. Similarly, the transactions dataset into key-value pairs where the key is the transaction address and the value is the transaction value.

5.  ***Join and Map the transactions and scams datasets:*** The transactions and scam dataset obtained from the above step is joined to map scam types and addresses to transaction value. The total value of transactions for each scam type and address is calculated to get the top popular scams.

6.  ***Create a new RDD and Map :*** The month and scam address to the sum of all transaction values occurred during that month for that scam address is mapped and the ethertime RDD is obtained to contain a tuple with a tuple containing the month and scam address as the key, and the total transaction value as the value.

7. ***Store results in S3 bucket:*** The results are then written to S3 bucket as a TXT file ( `ether_category_time.txt`, `ether_category_status_time.txt` and `popular_scams.txt` ) using the `boto3` library and the Spart session is stopped using `stop()` method.

#### Output:

The below list shows the most lucrative form of scams wrt to total value. The most lucrative form of scam is found to be `Phishing` with total value of  `4.32186e+22` The code to generate this table can be found in [`PartD/scam_analysis/popular_Scams/scams_category.ipynb`](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/scam_analysis/popular_scams/scams_category.ipynb)
| Type     |       Value |   Rank |
|----------|-------------|--------|
| Phishing | 4.32186e+22 |      1 |
| Scamming | 4.1627e+22  |      2 |
| Fake ICO | 1.35646e+21 |      3 |


The below is the list of popular scams. The most popular scam is found to be Scamming with scam id of `5622` with value of `1.67091e+22`

![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/scam_analysis/popular_scams/output/popular_scams.png?raw=true)




The below plot shows how ether received has changed over time. The code for this graph can be found here [`PartD/scam_analysis/popular_scams/scam_category.ipynb`](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/scam_analysis/popular_scams/scams_category.ipynb)

![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/scam_analysis/popular_scams/output/ether_time.png?raw=true)

The code for the below plots can be found here [`PartD/scam_analysis/popular_scams/scam_category_status.ipynb`](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/scam_analysis/popular_scams/scams_category_status.ipynb)

**Active Scams:**

The below plot show how ether receive has changed over time for Active scams.

![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/scam_analysis/popular_scams/output/active_scam.png?raw=true)

**Inactive Scams**

The below plot show how ether receive has changed over time for Inactive scams.

![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/scam_analysis/popular_scams/output/inactive_scam.png?raw=true)


**Offline Scams**

The below plot show how ether receive has changed over time for Offline scams.

![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/scam_analysis/popular_scams/output/offline_scam.png?raw=true)

**Suspended Scams**

The below plot show how ether receive has changed over time for Suspended scams.

![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/scam_analysis/popular_scams/output/suspended_scam.png?raw=true)

**Inference:**

For Active scams, the highest value for scamming was recorded in September 2018, while the highest value for phishing was observed in July 2017. Both scamming and phishing are fluctuating over the months. The lowest value for scamming was recorded in November 2017, and the lowest value for phishing was observed in July 2018.

For Inactive scams, only the Phishing scam type exist and it steadily decreases from Feb 2018 to April 2018.

For Offline scams, phishing, scamming, and fake ICOs occurred between May 2017 and January 2019 but were taken offline. Phishing and scamming were more prevalent than fake ICOs during this period.

For Suspended scams, scamming is more prevalent than phishing activities.

Overall,  plot suggests that phishing and scamming scams are more prevalent. The value of phishing and scamming scams has been increasing over time, with phishing showing a steep increase in August 2017 and scamming in Sep 2018. The value of fake ICO scams is volatile, with large fluctuations between July 2017 and June 2018.


## 4.2. Miscellaneous Analysis

### 4.2.1. Data Overhead (20%)

#### Objective:

To analyze the amount of space that can be saved by removing unnecessary columns from the blocks table in a cryptocurrency database. The columns that can be considered for removal are logs_bloom, sha3_uncles, transactions_root, state_root, and receipts_root. Since these columns contain hex strings, it is assumed that each character after the first two requires four bits. The objective is to determine how much space can be saved by removing these columns.

#### Data Source:
The data used in this analysis was fetched from a CSV file stored in an S3 bucket. The highlighted fields in the data schema is used in the source code to obtain the results.

- ***blocks.csv:*** [number, hash, parent_hash, nonce, **sha3_uncles**, **logs_bloom**, **transactions_root**, **state_root**, **receipts_root**, miner, difficulty, total_difficulty, size, extra_data, gas_limit, gas_used, timestamp, transaction_count, base_fee_per_gas,]

#### Source Code:
```sh
PartD/miscellaneous_analysis/data_overhead/
├── data-overhead.py # Source code 
└── output
    └── data_ovehead.txt
```
#### Execution:
1. Execute the spark application.

	```sh
	ccc create spark data-overhead.py -d -s
	```

2. Stream the logs of the drive container.

	```sh
	oc logs -f data-overhead-spark-app-driver
	```

#### Methodology:

1.  ***Initialize Spark session and S3 environment variables:*** Initialized a Spark session using the SparkSession object. Fetched environment variables related to the S3 bucket, such as the data repository bucket, endpoint URL, access key ID, secret access key, and bucket name. Configured Hadoop settings for the Spark session using the hadoopConf object.

2.  ***Fetch blocks.csv file and verify block data*** The `blocks.csv` is fetched from the S3 bucket using the `textFile()` method of the Spark context. The methods `verify_blocks()` reads every line of transactions as input and return `True` if the data type is in the correct format and `False` if its invalid.

3. ***Transform and reduce blocks dataset:*** The blocks dataset is mapped to extract the five columns that need to be analysed and the `calculate_size()` method is used to calculate the size of each column using the `map()`. The resultant data is reduced `reduceByKey()` by summing the sized of all the columns.

4. ***Create new RDD to calculate total size:***  The above reduced dataset is mapped to calculate the total size of all columns and stored in a new RDD.

5. ***Store results in S3 bucket:*** The results are then written to S3 bucket as a TXT file `data-overhead.txt` using the boto3 library and the Spark session is stopped using `stop()` method.

#### Output:

The size of the unwanted columns (logs_bloom, sha3_uncles, transactions_root, state_root, and receipts_root) is calculated and found to be `21504003276`. Therefore, we would be above to save `21504003276` bits of data when the above mentioned columns are removed.

### 4.2.2. Gas Guzzlers (20%)

#### Objective:
To analyze the changes in gas price and gas used for contract transactions on Ethereum over time and determine if there is a correlation with the results seen in Part B. Create a graph showing the change in gas price over time, a graph showing the change in gas used for contract transactions over time, and identify if the most popular contracts use more or less than the average gas used.

#### Data Source:
The data used in this analysis was fetched from a CSV file stored in an S3 bucket. The highlighted fields in the data schema is used in the source code to obtain the results.

- ***transactions.csv:*** [hash, nonce, block_hash, block_number, transaction_index, from_address, **to_address**, value,  **gas**, **gas_price**, input, **block_timestamp**, max_fee_per_gas, max_priority_fee_per_gas, transaction_type]
- **_contracts.csv:_** [**address**, bytecode, function_sighashes, is_erc20, is_erc721, block_number]

#### Source Code:
```sh
PartD/miscellaneous_analysis/gas_guzzlers
├── gas-guzzlers.py # Source code
├── gas_price_average.ipynb # Code for generating average gas price plot
├── gas_price_used.ipynb # Code for generating average gas used plot
└── output
    ├── gas_price_avg.md
    ├── gas_price_avg.png
    ├── gas_price_avg.txt
    ├── gas_used_avg.md
    ├── gas_used_avg.png
    └── gas_used_avg.txt
```
#### Execution:
1. Execute the spark application.

    ```sh
    ccc create spark gas-guzzlers.py -d -s
    ```
2. Stream the logs of the drive container.

    ```sh
    oc logs -f spark gas-guzzlers-spark-app-driver
    ```
#### Methodology:

1.  ***Initialize Spark session and S3 environment variables:*** Initialized a Spark session using the SparkSession object. Fetched environment variables related to the S3 bucket, such as the data repository bucket, endpoint URL, access key ID, secret access key, and bucket name. Configured Hadoop settings for the Spark session using the hadoopConf object.

2.  **_Fetch transactions.csv and contracts.csv file and verify for malformed lines:_**  The  `transactions.csv`  and  `contracts.csv`  is fetched from the S3 bucket using the  `textFile()`  method of the Spark context. The methods  `verify_transactions()`  and  `verify_contracts()`  respectively reads every line of transactions and contracts as input and return  `True`  if the data type is in the correct format and  `False`  if its invalid.

3. ***Extract and aggregate features for average gas price calculation:*** The method `map_gas_price()` takes every single line of transactions data as input and returns a tuple of date and a tuple of gas price and count. This method is used to extract features from the data and aggregate values for calculating the average gas price per month. The transactions data is mapped using this function to calculate the average gas price per month.

4. ***Reduce transactions data by date and calculate average gas used:*** Similar to the above step, the method `map_gas_used()` takes a every line of transactions data as input and returns a tuple of smart contract address and a tuple of date and gas used. The method is used to extract features from the data which is used for calculating the average gas used per smart contract per month. The transactions and contracts data is then mapped using this function. Then, both the data are joined based on the smart contract address. The result of the join RDD is reduced by date, and the average gas used per smart contract per month is calculated.

5. ***Store results in S3 bucket:*** The results are then written to S3 bucket as a TXT file `gas_used_avg.txt` and `gas_price_avg.txt` using the `boto3` library and the Spark session is stopped using `stop()` method.

#### Output:

The below plot shows the Average gas price with respect to Month/Year (sorted first on month and then year). The code for the below graph can be found here [`PartD/miscellaneous_analysis/gas_guzzlers/gas_price_average.ipynb`](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/miscellaneous_analysis/gas_guzzlers/gas_price_average.ipynb)

![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/miscellaneous_analysis/gas_guzzlers/output/gas_price_avg.png?raw=true)

The below plot shows the Average gas used with respect to Month/Year (sorted first on month and then year). The code for the below graphs can be found here [`PartD/miscellaneous_analysis/gas_guzzlers/gas_price_used.ipynb`](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/miscellaneous_analysis/gas_guzzlers/gas_price_used.ipynb)

![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/miscellaneous_analysis/gas_guzzlers/output/gas_used_avg.png?raw=true)
