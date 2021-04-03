import configparser
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

import config
from etl import *



def start_spark_context(config_app_name, cluster_location):
    """
    Create spark entry point.
    """
    configure = SparkConf() \
            .setAppName(config_app_name) \
            .setMaster(cluster_location)

    sc = SparkContext(conf=configure)

    return sc

def start_spark_session(session_app_name):
    """ 
    Create spark v2 entry point

    """
    spark = SparkSession.builder \
            .appName(session_app_name) \
            .config('config option', 'config value') \
            .getOrCreate()

    return spark


def main():

    sc = start_spark_context(config.CONFIG_APP_NAME, config.CLUSTER_LOCATION)
    spark = start_spark_session(config.SESSION_APP_NAME)

    process_airports_data(spark, config.AIRPORTS_URL_PATH, config.OUTPUT_PATH)
    process_cities_demographics_data(spark, config.USA_CITIES_URL_PATH, config.OUTPUT_PATH)
    process_usa_temperature_data(spark, config.TEMPERATURE_URL_PATH, config.OUTPUT_PATH)
    process_usa_tourism_data(spark, config.TOURISM_URL_PATH, config.AIRPORT_CODES, config.COUNTRY_CODES, config.OUTPUT_PATH)


    sc.stop()

if __name__ == "__main__":
    main()