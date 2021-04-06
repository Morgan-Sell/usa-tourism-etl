from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import configparser

import config
from etl import *


config = configparser.ConfigParser()
config.read_file(open("dl.cfg"))

os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

DL_AIRPORT_DATA = config.get("S3", "AIRPORT_DATA")
DL_USA_CITIES_DATA = config.get("S3", "USA_CITIES_DATA")
DL_WEATHER_DATA = config.get("S3", "WEATHER_DATA")
DL_TOURISM_DATE = config.get("S3", "TOURISM_DATA")
DL_AIRPORT_CODES = config.get("S3", "AIRPORT_CODES")
DL_COUNTRY_CODES = config.get("S3", "COUNTRY_CODES")
DL_OUTPUT_PATH = config.get("S3", "OUTPUT_PATH")


def create_spark_session():
    """ 
    Create spark entry point
    """
    spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
    return spark


def main():

    spark = create_spark_session()

    process_airports_data(spark, DL_AIRPORT_DATA, DL_OUTPUT_PATH)
    process_cities_demographics_data(spark, DL_USA_CITIES_DATA, DL_OUTPUT_PATH)
    process_usa_temperature_data(spark, DL_WEATHER_DATA, DL_OUTPUT_PATH)
    process_usa_tourism_data(spark, DL_TOURISM_DATA, DL_AIRPORT_CODES, DL_COUNTRY_CODES, DL_OUTPUT_PATH)


if __name__ == "__main__":
    main()