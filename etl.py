import pandas as pd
import numpy as np

import configparser
from pyspark.sql import SparkSession, Window
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf, col, monotonically_increasing_id, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType, StringType
from pyspark.sql import functions as F

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def create_spark_session(config_app_name, session_app_name):
    """ 
    Configures and initiates a Spark connection/session
    

    
    """
    
    configure = SparkConf() \
            .setAppName(config_app_name) \
            .setMaster('local')

    sc = SparkContext(conf=configure)
    
    spark = SparkSession.builder \
            .appName(session_app_name) \
            .config('config option', 'config value') \
            .getOrCreate()

    return spark


def process_airports_data(spark, input_data, output_data):
    """
    Loads and processes the raw data using Spark. 
    Returns the data as a semi-schematized parquet file.

    """

    df = spark.read.option("header", True).csv(input_data)

    # Extract values from within columns
    lat_long = F.split(df.coordinates, ",")
    df = df.withColumn("longitude", lat_long.getItem(0))
    df = df.withColumn("latitude", lat_long.getItem(1))
    region_split = F.split(df.iso_region, "-")
    df = df.withColumn("state", region_split.getItem(1))

    # Select subset of original dataframe.
    df2 = df.select(["ident",
            "iata_code",
            "name",
            "type",
            "municipality",
            "state",
            "local_code",
            "latitude",
            "longitude",
            "elevation_ft"]).where(df.iso_country=="US")