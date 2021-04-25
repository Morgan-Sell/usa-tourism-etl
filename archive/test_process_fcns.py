import pandas as pd
import numpy as np

import configparser
from pyspark.sql import SparkSession, Window
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf, col, monotonically_increasing_id, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType, StringType
from pyspark.sql import functions as F

from etl import *


# Connect to spark
config_app_name = "udac_config"
session_app_name = "udac_cap"

spark = create_spark_session(config_app_name, session_app_name)


output_data = "test_output/"

# Test aiport data function
input_data = "data/us_cities_demographics.csv"
#process_cities_demographics_data(spark, input_data, output_data)

tourism_data = "data/immigration_data_sample.csv"
airport_codes = "data/airport_dict.csv"
country_codes = "data/country_codes.csv"
process_usa_tourism_data(spark, tourism_data, airport_codes,
                                country_codes, output_data)