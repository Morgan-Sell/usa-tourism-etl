import pandas as pd
import numpy as np

import configparser
from pyspark.sql import SparkSession, Window
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf, col, monotonically_increasing_id, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType, StringType
from pyspark.sql import functions as F

from datetime import datetime, timedelta
import os

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
    Loads and processes the raw airport data using Spark. 
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
    
    # Revise numeric values data types
    df2 = df2.withColumn("latitude", df2.latitude.cast('float')) \
            .withColumn("longitude", df2.longitude.cast('float')) \
            .withColumn("elevation_fit", df2.elevation_ft.cast('integer'))
    
    # Sort 
    df2 = df2.sort('iata_code', ascending=True) \
            .na.drop(subset='iata_code')
    
    # Export data to a parquet file
    df2.write.mode('overwrite').parquet(os.path.join(output_data, "airports"))


def process_cities_demographics_data(spark, input_data, output_data):
    """
    Loads and processes the raw U.S. cities demographics data using Spark. 
    Returns the data as a semi-schematized parquet file.

    """
    df = spark.read.option('header', True) \
                .option('delimiter', ";") \
                .csv(input_data)
    
    df2 = df

    # Rename columns
    cities_cols_rename = {"City": "city",
                        "State": "state",
                        "Median Age": "median_age",
                        "Male Population": "male_pop",
                        "Female Population": "female_pop",
                        "Total Population": "total_pop",
                        "Number of Veterans": "num_veterans",
                        "Foreign-born": "num_foreigners",
                        "Average Household Size": "avg_household_size",
                        "State Code": "state_code",
                        "Race": "race",
                        "Count": "race_pop"}

    for original, revised in cities_cols_rename.items():
        df2 = df2.withColumnRenamed(original, revised)

    df2 = df2.withColumn("state_city", F.concat_ws("_", df2.state_code, df2.city))

    # Cast values to numeric
    integer_vars = ["male_pop", "female_pop", "total_pop", "num_veterans", "num_foreigners", "race_pop"]
    float_vars = ["median_age", "avg_household_size"]

    for i_var in integer_vars:
        df2 = df2.withColumn(i_var, df2[i_var].cast('integer'))
    
    for f_var in float_vars:
        df2 = df2.withColumn(f_var, df2[f_var].cast('float'))

    df2 = df2.dropDuplicates(["state_city"])

    # Create race population group-by table.
    race  = df2.select("state_city", "race", "race_pop")
    race = race.groupBy("state_city").pivot("race").agg(F.first("race_pop"))

    # join dataframes
    df3 = df2.join(race, df2.state_city == race.state_city)
    df3 = df3.drop("race", "race_pop", "state_city", "state_city")

    # rename race-related columns
    race_cols_rename = {"American Indian and Alaska Native": "native_american_pop",
                        "Asian": "asian_pop",
                        "Black or African-American": "black_american_pop",
                        "Hispanic or Latino": "hispanic_pop",
                        "White": "white_pop"}
             
    for original, revised in race_cols_rename.items():
        df3 = df3.withColumnRenamed(original, revised)

    # Export data to a parquet file
    df3.write.mode('overwrite').parquet(os.path.join(output_data, "cities_demographics"))