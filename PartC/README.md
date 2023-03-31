
### Part C. Top Ten Most Active Miners (10%)
#### Objective:
To evaluate the top 10 miners by the size of the blocks mined, using an approach that does not require a join. This involved aggregating the blocks to determine the involvement of each miner in terms of the size of the blocks mined and aggregating the size for addresses in the miner field and adding each value from the reducer to a list. The list will then be sorted to obtain the most active (top 10) miners.

#### Data Source:

The data used in this code is fetched from a CSV files stored in an S3 bucket:

_**blocks.csv:**_  This file contains information about Ethereum blocks, including the miner address, timestamp, and size

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
- Firstly, a Spark session is initialised and the Hadoop configuration is setup with the S3 environment variables.
- The `blocks.csv` file is fetched from the S3 bucket and the data is filtered using `verify_blocks` function.
- The filtered data is transformed using `get_block_features` function to extract the miner address and block size.
- The block data is reduced by the miner address to calculate the total block size mined by each miner.
- The top 10 miners are identified using `takeOrdered` method with a lambda function to sort the miners by the total block size mined.
- The results are then written to S3 bucket as a TXT file using the boto3 library, and the Spark session is stopped.

#### Output:

The following are the top 10 by block size.

|    | Address                                    |       Value |   Rank |
|----|--------------------------------------------|-------------|--------|
|  0 | 0xea674fdde714fd979de3edf0f56aa9716b898ec8 | 17453393724 |      1 |
|  1 | 0x829bd824b016326a401d083b33d092293333a830 | 12310472526 |      2 |
|  2 | 0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c |  8825710065 |      3 |
|  3 | 0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5 |  8451574409 |      4 |
|  4 | 0xb2930b35844a230f00e51431acae96fe543a0347 |  6614130661 |      5 |
|  5 | 0x2a65aca4d5fc5b5c859090a6c34d164135398226 |  3173096011 |      6 |
|  6 | 0xf3b9d2c81f2b24b0fa0acaaa865b7d9ced5fc2fb |  1152847020 |      7 |
|  7 | 0x4bb96091ee9d802ed039c4d1a5f6216f90f81b01 |  1134151226 |      8 |
|  8 | 0x1e9939daaad6924ad004c2560e90804164900341 |  1080436358 |      9 |
|  9 | 0x61c808d82a3ac53231750dadc13c777b59310bd9 |   692942577 |     10 |
