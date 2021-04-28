# Advancing the USA Tourism Sector
## Applying Big Data Technology to Better Equip the USTA's Partners

<img src="https://github.com/Morgan-Sell/usa-tourism-etl/blob/main/img/main_tourism.jpeg" width="850" height="275">

## Project Overview

In 2019, the United States hosted more than 79 million international tourists. These visitors add to the country's diverse social fabric and contribute to its thriving economy. On average, **more than 9,000 people arrive in the U.S. every hour**. Consequently, the United States Travel Association (USTA) decided that it's in everyone's best interest - e.g., government, private sector, U.S citizens, and foreigners - to have an organized, centralized data repository that is relevant to tourism.

The USTA contracted our team to design and implemented a solution. We proposed and implemented a data lake constructing on AWS technology stack. 

A few reasons that we selected a data lake:
1. Multiple data sources using different data formats
2. Different end-users with dissimilar requirements
3. Enablement of "big data" technology like Hadoop, EMR, and Spark
4. Need to address high data volume


### Data Sources:

The data lake coalesces different types of data from various sources:
- US National Tourism and Trade Office 
- Lawrence Berkeley National Laboratory (LBNL)
- International Air Transport Association (IATA
- United States Census Bureau


## ETL Architecture

The process starts with the extraction of data from the sources mentioned. The file types are either parquet or CSV. The data is transformed, organized, and formatted into tables that can be better queried and are more intuitive. These tables are then partitioned, when appropriate, and loaded as parquet files into an S3 bucket.

The transformation work is allocated to a number of worker nodes/virtual computers (vCPUs). The vCPUs work in parallel and are overseen by a master node. Both master and worker nodes are housed in an EMR cluster. 

A wonderful benefit of EMR is its ability to adjust the number of worker nodes to meet demand. When the data volume is high, the USTA can request additional help, i.e., turn on more vCPUs. And, when the data volume is low and the USTA would like to save costs, the agency can decrease the quantity of vCPUs in use.


<img src="https://github.com/Morgan-Sell/usa-tourism-etl/blob/main/img/aws_flow.jpg" width="600" height="400" class="center">



## Addressing Other Scenario

As discussed, EMR can address the change in needs in accordance with data volume, even if the increase was 100x. 

The data lake does have its limitations. If one of the USTA's needs was to generate reports for daily morning briefings, requiring the pipelines run on a daily basis at 7 am, then schematized data warehouse would be more appropriate.

However, this is not the case. Also, the USTA's partners, e.g., hotel chains, can develop their own data warehouse on top of the USTA's data lake. Hilton's desired schema and needs are most likely different than those of Southwest Airlines or local/state tourism agencies


## Notes
To run the program on a local CPU, execute `main_local.py` in a command-line prompt. The file will perform the ETL by running the code in the `etl.py` module.

To run the program on an AWS EMR cluster, one must run the `emr_notebook.ipynb`. EMR requires a user of an EMR notebook that has a PySpark kernel to enable the use of the PySpark package.

## Packages
The following packages are used throughout the project:
- PySpark
