from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import configparser

import config
from etl import *


config = configparser.Configparser()
config.read_file(open("dl.cfg"))

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]

DL_AIRPORT_DATA = config.get("S3", "AIRPORT_DATA")
DL_USA_CITIES_DATA = config.get("S3", "USA_CITIES_DATA")
DL_WEATHER_DATA = config.get("S3", "WEATHER_DATA")
DL_TOURISM_DATE = config.get("S3", "TOURISM_DATA")
DL_AIRPORT_CODES = config.get("S3", "AIRPORT_CODES")
DL_COUNTRY_CODES = config.get("S3", "COUNTRY_CODES")


def start_spark_session(session_app_name):
    """ 
    Create sparkentry point
    """
    spark = SparkSession.builder \
            .appName(session_app_name) \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
    return spark


def main():

    sc = start_spark_context(config.CONFIG_APP_NAME, config.CLUSTER_LOCATION)
    spark = start_spark_session(config.SESSION_APP_NAME)

    process_airports_data(spark, DL_AIRPORT_DATA, config.OUTPUT_PATH)
    process_cities_demographics_data(spark, DL_USA_CITIES_DATA, config.OUTPUT_PATH)
    process_usa_temperature_data(spark, DL_WEATHER_DATA, config.OUTPUT_PATH)
    process_usa_tourism_data(spark, DL_TOURISM_DATA, DL_AIRPORT_CODES, DL_COUNTRY_CODES, config.OUTPUT_PATH)


    sc.stop()

if __name__ == "__main__":
    main()