

## Part B. Top Ten Most Popular Services (25%)

### Objectives:

To evaluate the top 10 smart contracts by total Ether received. This includes joining the **address** field in the contracts dataset to the **to_address** in the transactions dataset to determine how much ether a contract has received.

### Data Source:

The data used in this analysis was fetched from two CSV files stored in an S3 bucket: 

***transactions.csv:*** The transactions file contains information about Ethereum transactions.

***contracts.csv:*** The contracts file contains information about Ethereum smart contracts.

### Methodology:

- The Spark script begins by initializing a Spark session.

- The Spark script then fetches environment variables required for S3 storage [S3 endpoint URL, access key ID, secret access key, and the bucket name]. The Hadoop configuration is then set to allow Spark to access the S3 bucket.

- The transactions and contracts files are then fetched from the S3 bucket using the ``textFile()`` method. The ``check_transactions`` and ``check_contracts ``methods are used to filter out any lines in the datasets that do not conform to their respective formats.

- The transactions dataset is then transformed using a map operation that extracts the Ethereum address and transaction value from each line. 
- The contracts dataset is transformed using a map operation that associates a value of 1 with each contract address. 
- The ``reduceByKey()`` operation is then used to group the transaction values by address, and the join() operation is used to join the grouped transaction values with the contracts dataset. 
- Finally, a map operation is used to extract the address and total transaction value from the joined dataset.
- The ``takeOrdered()`` method is used to get the top 10 smart contracts based on their total transaction value.
- The results are then written to S3 bucket as a TXT file using the boto3 library, and the Spark session is stopped.

### Output:

The following are the top 10 smart contracts by total Ether received.

|    | Address                                    |                      Value |   Rank |
|----|--------------------------------------------|----------------------------|--------|
|  0 | 0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444 | 84155363699941767867374641 |      1 |
|  1 | 0x7727e5113d1d161373623e5f49fd568b4f543a9e | 45627128512915344587749920 |      2 |
|  2 | 0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef | 42552989136413198919298969 |      3 |
|  3 | 0xbfc39b6f805a9e40e77291aff27aee3c96915bdd | 21104195138093660050000000 |      4 |
|  4 | 0xe94b04a0fed112f3664e45adb2b8915693dd5ff3 | 15543077635263742254719409 |      5 |
|  5 | 0xabbb6bebfa05aa13e908eaa492bd7a8343760477 | 10719485945628946136524680 |      6 |
|  6 | 0x341e790174e3a4d35b65fdc067b6b5634a61caea |  8379000751917755624057500 |      7 |
|  7 | 0x58ae42a38d6b33a1e31492b60465fa80da595755 |  2902709187105736532863818 |      8 |
|  8 | 0xc7c7f6660102e9a1fee1390df5c76ea5a5572ed3 |  1238086114520042000000000 |      9 |
|  9 | 0xe28e72fcf78647adce1f1252f240bbfaebd63bcc |  1172426432515823142714582 |     10 |
