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
input_data = "data/GlobalLandTemperaturesByCity.csv"
process_usa_temperature_data(spark, input_data, output_data)