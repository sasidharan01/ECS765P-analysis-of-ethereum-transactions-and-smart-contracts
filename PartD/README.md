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
