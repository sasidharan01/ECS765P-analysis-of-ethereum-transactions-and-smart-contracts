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

