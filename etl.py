from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf, col, monotonically_increasing_id, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType, StringType
from pyspark.sql import functions as F

import config


def create_spark_session():
    """ 
    Create spark entry point
    """
    spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
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
    df2 = df2.orderBy(["state", "iata_code"]) \
            .na.drop(subset='iata_code')
    
    # Export data to a parquet file
    df2.write.partitionBy("state").mode('overwrite').parquet(os.path.join(output_data, "airports"))


def process_cities_demographics_data(spark, input_data, output_data):
    """
    Loads and processes the raw U.S. cities demographics data using Spark. 
    Returns the data as a semi-schematized parquet file.

    """
    df = spark.read.option('header', True) \
                .option('delimiter', ";") \
                .csv(input_data)
    
    df2 = df

    for original, revised in config.USA_CITIES_RENAME_COLS.items():
        df2 = df2.withColumnRenamed(original, revised)

    df2 = df2.withColumn("state_city", F.concat_ws("_", df2.state_code, df2.city))

    # Change data types to integers
    for i_var in config.USA_CITIES_INTEGER_VARS:
        df2 = df2.withColumn(i_var, df2[i_var].cast('integer'))
    
    # Change data types to floats
    for f_var in config.USA_CITIES_FLOAT_VARS:
        df2 = df2.withColumn(f_var, df2[f_var].cast('float'))

    df2 = df2.dropDuplicates(["state_city"])

    # Create race population group-by table.
    race  = df2.select("state_city", "race", "race_pop")
    race = race.groupBy("state_city").pivot("race").agg(F.first("race_pop"))

    # join dataframes
    df3 = df2.join(race, df2.state_city == race.state_city)
    df3 = df3.drop("race", "race_pop", "state_city", "state_city")
             
    for original, revised in config.RACE_RENAME_COLS.items():
        df3 = df3.withColumnRenamed(original, revised)

    df3 = df3.orderBy(["state", "city"])

    # Export data to a parquet file
    df3.write.partitionBy("state").mode('overwrite').parquet(os.path.join(output_data, "cities_demographics"))


def process_usa_temperature_data(spark, input_data, output_data):
    """
    Loads global temperature files. 
    Returns a parquet file for the climate of U.S. cities.
    """

    df = spark.read.option('header', True).csv(input_data)
    df2 = df.select("*").where((df.Country == "United States") & (df.dt > "1969-12-31"))
    
    for original, revised in config.TEMPERATURE_RENAME_COLS.items():
        df2 = df2.withColumnRenamed(original, revised)
    
    df2 = df2.withColumn("lat_length", F.length("latitude")) \
            .withColumn("long_length", F.length("longitude")) \
            .withColumn("latitude_2", F.expr("""substr(latitude, 1, lat_length-1)""")) \
            .withColumn("longitude_2", F.expr("""substr(longitude, 1, long_length-1)"""))
    
    df2 = df2.withColumn("latitude", df2.latitude_2.cast('float')) \
            .withColumn("longitude", df2.longitude_2.cast('float'))

    df2 = df2.withColumn("longitude", -1 * col("longitude")) \
            .withColumn("year", year(df2.date)) \
            .withColumn("month", month(df2.date)) \
            .drop("Country", "lat_length", "long_length", "latitude_2", "longitude_2")
    
    df2 = df2.orderBy(["date", "city"])

    df2.write.partitionBy("year").mode('overwrite').parquet(os.path.join(output_data, "usa_temperatures"))


def convert_datetime(num_days):
    """
    Converts a uni-codic numeric string value to a date object.
    """
    try:
        start = datetime(1960, 1, 1)
        res = start + timedelta(days=int(float((num_days))))
        return res.date()
    except:
        return None
    

def process_usa_tourism_data(spark, tourism_data, airport_codes, country_codes, output_data):
    """
    Loads and process the U.S. tourism SAS files.
    Joins tourism data with airport_codes and countries.
    Returns PySpark dataframe as partitioned parquet files.
    """

    tourism = spark.read.option('header', True) \
                    .option('inferSchema', True) \
                    .csv(tourism_data)
    
    airports = spark.read.option('header', True).csv(airport_codes)
    countries = spark.read.option('header', True).csv(country_codes)

    # Create airport-cities dictionary
    airports2 = airports.withColumn("city", F.split(col("airport"), ",").getItem(0))
    airports2 = airports2.withColumn("city", F.initcap("city")) \
                        .drop("airport")
    
    # Create country-I94 code dictionary
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), DateType())
    countries2 = countries.withColumn("country", F.initcap("country")) \
                        .withColumn("country_code", countries.country_code.cast('integer'))

    # Process tourism data
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), DateType())

    #tourism2 = tourism.withColumn("arrival_date", udf_datetime_from_sas(col("arrdate"))) \
    tourism2 = tourism.withColumn("arrival_date", udf_datetime_from_sas(tourism.arrdate)) \
                .withColumn("departure_date", udf_datetime_from_sas(tourism.depdate)) \
                .drop(*config.DROP_TOURISM_COLS)
    
    for original, renamed in config.TOURISM_RENAME_COLS.items():
        tourism2 = tourism2.withColumnRenamed(original, renamed)

    for feature in config.TOURISM_INTEGER_VARS:
        tourism2 = tourism2.withColumn(feature, tourism2[feature].cast('integer'))
    
    # Create master dataframe by joining tourism2 and countries2 dataframes.
    master = tourism2.join(countries2,
                    tourism2.citizen_cntry_code == countries2.country_code,
                    how ='left')
    
    master = master.withColumnRenamed("country", "citizen_country") \
                    .drop("country_code")
    
    master = master.join(countries2,
                    master.residency_cntry_code == countries2.country_code,
                    how='left')
    
    master = master.withColumnRenamed("country", "residency_country") \
                    .drop("country_code")
    
    # Join master and airports2 dataframes.
    master = master.join(airports2, master.airport == airports2.airport_code, how='left')
    master = master.withColumnRenamed("city", "airport_city") \
                    .drop("airport_code")

    # Change the categorical values from integers/characters to descriptive strings.
    travel_mode_func = udf(lambda x: config.MODE_OF_TRAVEL.get(x), StringType())
    travel_reason_func = udf(lambda x: config.REASON_FOR_TRAVEL.get(x), StringType())
    maritime_signals_func = udf(lambda x: config.MARITIME_SIGNAL_FLAGS.get(x), StringType())

    master = master.withColumn("travel_mode", travel_mode_func(master.travel_mode)) \
                        .withColumn("reason_for_travel", travel_reason_func(master.reason_for_travel)) \
                        .withColumn("maritime_status_arrival", maritime_signals_func(master.entdepa)) \
                        .withColumn("maritime_status_departure", maritime_signals_func(master.entdepd)) \
                        .drop("entedepa", "entdepd")
    
    # Add sequential ID
    master = master.orderBy("arrival_date") \
                .withColumn("tourism_id", monotonically_increasing_id())
    master = master.orderBy("tourism_id")
    #window = Window.orderBy(col("mono_increasing_id"))

    # Write master dataframe to parque files partitioned by year and month
    master.write.partitionBy("arrival_yr", "arrival_month").mode('overwrite').parquet(os.path.join(output_data, "tourist_visits"))
    

def main():

    spark = create_spark_session()
    
    AIRPORT_DATA="s3a://udac-capstone/airports.csv"
    TOURISM_DATA="s3a://udac-capstone/tourism_data/*"
    USA_CITIES_DATA="s3a://udac-capstone/us_cities_demographics.csv"
    WEATHER_DATA="s3a://udac-capstone/GlobalLandTemperaturesByCity.csv"
    AIRPORT_CODES="s3a://udac-capstone/airport_codes.csv"
    COUNTRY_CODES="s3a://udac-capstone/country_codes.csv"
    OUTPUT_PATH = "s3a://udac-capstone-output/"

    process_airports_data(spark, AIRPORT_DATA, OUTPUT_PATH)
    process_cities_demographics_data(spark, USA_CITIES_DATA, OUTPUT_PATH)
    process_usa_temperature_data(spark, WEATHER_DATA, OUTPUT_PATH)
    process_usa_tourism_data(spark, TOURISM_DATA, AIRPORT_CODES, COUNTRY_CODES, OUTPUT_PATH)


if __name__ == '__main__':
    main()