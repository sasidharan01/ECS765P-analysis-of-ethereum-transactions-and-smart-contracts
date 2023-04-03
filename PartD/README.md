
## Part D. Data exploration (40%)
## 4.1. Scam Analysis

### 4.1.1 Popular Scams (20%)
#### Objective:
To analyze the provided scam dataset and determine the most lucrative form of scam. Investigate how this changes over time (generate graph) and examine any correlation with known scams going offline or becoming inactive. To provide the ID of the most lucrative scam and a graph illustrating the changes in ether received over time for the dataset.

#### Data Source:
The data used in this analysis was fetched from the following CSV files stored in an S3 bucket:

***transactions.csv:*** [hash, nonce, block_hash, block_number, transaction_index, from_address, to_address, value, gas]
***scams.csv:*** [id, name, url, coin, category, subcategory, description, addresses, reporter, ip, status]

#### Source Code:

```sh
PartD/scam_analysis/popular_scams
├── convert.py
├── output
│   ├── ether_time.csv
│   ├── ether_time.png
│   ├── ether_time.txt
│   ├── popular_scams.txt
│   ├── scams.csv
│   ├── scams.json
│   └── untitled.txt
├── scam.py # Source code
└── scams.ipynb # Scource code to ether time plot
```
#### Methodology:


1.  ***Initialize Spark session and S3 environment variables:*** Initialize a Spark session using the SparkSession object. Fetch environment variables related to the S3 bucket, such as the data repository bucket, endpoint URL, access key ID, secret access key, and bucket name. Configure Hadoop settings for the Spark session using the hadoopConf object.

2. ***Verify transactions and contracts data:*** The methods verify_transactions() and verify_contracts() reads every line of transactions and contracts data respectively as input and return True if the data is in the correct format and False otherwise.
3.  ***Fetch transactions.csv and contracts.csv files from S3 bucket:***  The transactions.csv and contracts.csv files are fetched from the S3 bucket using the textFile() method of the Spark context.

4.  ***Transform Scam and Transactions Dataset:*** The scams dataset is transformed into key-value pairs where the key is the scam address and the value is a tuple containing the scam type and scam address. Similarly, the transactions dataset into key-value pairs where the key is the transaction address and the value is the transaction value.
5.  ***Join and Map the transactions and scams datasets:*** The transactions and scam dataset obtained from the above step is joined to map scam types and addresses to transaction value. The total value of transactions for each scam type and address is calculated to get the top popular scams.

6.  ***Create a new RDD and Map :*** The month and scam address to the sum of all transaction values occurred during that month for that scam address is mapped and the ethertime RDD is obtained to contain a tuple with a tuple containing the month and scam address as the key, and the total transaction value as the value.
7. ***Store results in S3 bucket:*** The results are then written to S3 bucket as a TXT file ( `ether_time.txt` and `popular_scams.txt` ) using the boto3 library and the Spart session is stopped using stop() method.

#### Output:

The below are the list of popular scams
|   Scam ID | Type     |       Value |   Rank |
|-----------|----------|-------------|--------|
|      5622 | Scamming | 1.67091e+22 |      1 |
|      2135 | Phishing | 6.58397e+21 |      2 |
|        90 | Phishing | 5.97259e+21 |      3 |
|      2258 | Phishing | 3.46281e+21 |      4 |
|      2137 | Phishing | 3.38991e+21 |      5 |
|      2132 | Scamming | 2.42807e+21 |      6 |
|        88 | Phishing | 2.06775e+21 |      7 |
|      2358 | Scamming | 1.83518e+21 |      8 |
|      2556 | Phishing | 1.80305e+21 |      9 |
|      1200 | Phishing | 1.63058e+21 |     10 |
|      2181 | Phishing | 1.1639e+21  |     11 |
|        41 | Fake ICO | 1.1513e+21  |     12 |
|      5820 | Scamming | 1.13397e+21 |     13 |
|        86 | Phishing | 8.94456e+20 |     14 |
|      2193 | Phishing | 8.8271e+20  |     15 |

The below plot shows how ether received has changed over time.
![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/scam_analysis/popular_scams/output/ether_time.png)
**Inference:**
Overall, the value of phishing and scamming scams has been increasing over time, with phishing showing a steep increase in mid-2017 and again in mid-2018. 

The value of fake ICO scams has been more volatile, with large fluctuations in value between mid-2017 and early 2018, followed by a decline and relatively stable values since then. 

The plot suggests that phishing and scamming scams are more prevalent.

## 4.2 Miscellaneous Analysis

### 4.2.1 Gas Guzzlers (20%)

#### Objective:
To analyze the changes in gas price and gas used for contract transactions on Ethereum over time and determine if there is a correlation with the results seen in Part B. Create a graph showing the change in gas price over time, a graph showing the change in gas used for contract transactions over time, and identify if the most popular contracts use more or less than the average gas used.

#### Data Source:
The data used in this analysis was fetched from a CSV file stored in an S3 bucket:

***transactions.csv:*** The transactions file contains information about Ethereum transactions.

#### Source Code:
```sh
PartD/miscellaneous_analysis/gas_guzzlers
├── README.md
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

#### Methodology:

1. *Initialize spark session:* Initialize a Spark session using the SparkSession object.

2. *Verify transactions and contracts data:* The methods verify_transactions() and verify_contracts() that every line of transactions and contracts data respectively as input and return True if the data is in the correct format and False otherwise.

3. *Fetch S3 environment variables:* Fetch environment variables related to the S3 bucket, such as the data repository bucket, endpoint URL, access key ID, secret access key, and bucket name.

4. *Configure Hadoop settings for the Spark session:* Configure Hadoop settings for the Spark session using the hadoopConf object.

5. *Fetch transactions.csv and contracts.csv files from S3 bucket:* The transactions.csv and contracts.csv files are fetched from the S3 bucket using the textFile() method of the Spark context.

6. *Filter transactions and contracts data based on the format:* The transactions and contracts data are filtered based on the format using the filter() method of the RDD object.

7. *Extract and aggregate features for average gas price calculation:* The method map_gas_price() takes every single line of transactions data as input and returns a tuple of date and a tuple of gas price and count. This method is used to extract features from the data and aggregate values for calculating the average gas price per month. The transactions data is mapped using this function to calculate the average gas price per month.

8. *Reduce transactions data by date and calculate average gas used:* Similar to the above step, the method map_gas_used() takes a every line of transactions data as input and returns a tuple of smart contract address and a tuple of date and gas used. The method is used to extract features from the data which is used for calculating the average gas used per smart contract per month. The transactions and contracts data is then mapped using this function. Then, both the data are joined based on the smart contract address. The result of the join RDD is reduced by date, and the average gas used per smart contract per month is calculated.

9. *Store results in S3 bucket:* The results are then written to S3 bucket as a TXT file using the boto3 library and the Spart session is stopped using stop() method.

#### Output:

The below plot shows the Average gas price with respect to Month/Year (sorted first on month and then year).

![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/miscellaneous_analysis/gas_guzzlers/output/gas_price_avg.png)

The below plot shows the Average gas used with respect to Month/Year (sorted first on month and then year).

![alt](https://github.com/sasidharan01/ECS765P-analysis-of-ethereum-transactions-and-smart-contracts/blob/master/PartD/miscellaneous_analysis/gas_guzzlers/output/gas_used_avg.png)