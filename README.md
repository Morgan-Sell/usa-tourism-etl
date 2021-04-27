# USA Tourism Trends
### Analyzing the Drivers of Foreigner Travel Trends

<img src="https://github.com/Morgan-Sell/usa-tourism-etl/blob/main/img/main_tourism.jpeg" width="850" height="275">

## Project Overview

In 2019, the United States hosted more than 79 million international tourists. These visitors add to the country's diverse social fabric and contribute to its thriving economy. On average, **more than 9,000 people arrive to the U.S. every hour**. Consequently, the United States Travel Association (USTA) decided that it's in everyone best interest - e.g., government, private sectors, U.S Cctizens, and foreigners - to have a centralized source that provides a organized data on factors that are relevant to tourism.

The USTA contracted our team to design and implemented a solution. We proposed and implemented a data lake constructing on AWS technology stack. The process that extracts all of the information from various data sources, transforms them into a "semi-schematized" structure using Elacstic MapReduce (EMR) and Spark then loads the data into S3.

A few reasons that we selected a data lake instead of a data warehouse:
1. Multiple data sources using different data formats
2. Different end-users with dissimilar requirements
3. Allowment of big data technology like Hadoop, EMR, and Spark
4. High data volume


### Data Sources:

The data lake coaslesces a variety of types of data from various sources:
- US National Tourism and Trade Office 
- Lawrence Berkeley National Laboratory (LBNL)
- International Air Transport Association (IATA)
- United States Census Bureau


## ETL Architecture

By using AWS, we could implement distributed computing to improve the ETL's performance. AWS provides EMR which allows us to manage a cluster comprised of one master node and a large quantiy of worker nodes, which can be adjusted depending on the volumise. The diagram below summarizes the architecture.

<img src="https://github.com/Morgan-Sell/usa-tourism-etl/blob/main/img/aws_flow.jpg" width="600" height="400" class="center">
