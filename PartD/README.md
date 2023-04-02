
## Part D. Data exploration (40%)

## Miscellaneous Analysis

### 4.1 Gas Guzzlers (20%)

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