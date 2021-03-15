import os
import glob
import psycopg2
import pandas as pd

# STAGING TABLES
staging_visits_table_drop = "DROP TABLE IF EXISTS staging_visits"
staging_weather_table_drop = "DROP TABLE IF EXISTS staging_weather"
staging_cities_table_drop = "DROP TABLE IF EXISTS staging_cities"
staging_airports_table_drop = "DROP TABLE IF EXISTS staging_airports"




# CREATE TABLES
staging_visits_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_visits (
        visit_id              VARCHAR,
        tourist_id            VARCHAR,
        arr_date              DATE,
        depart_date           DATE,
        citizenship_id        VARCHAR,
        airport_id            VARCHAR,
        airport_name          VARCHAR,
        city_id               VARCHAR,
        city_name             VARCHAR,
        mode_of_travel        VARCHAR,
        airline               VARCHAR,
        visa_post             VARCHAR,
        visa_type             VARCHAR,
        birth_year            INT,
        gender                VARCHAR  
    )
""")

staging_weather_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_weather (
        date                  DATE,
        city_name             VARCHAR,
        avg_daily_temp        FLOAT8,
        avg_daily_temp_var    FLOAT8,
        lattitude             VARCHAR,
        longitude             VARCHAR
    )
""")

staging_cities_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_cities (
        city                  VARCHAR,
        state                 VARCHAR,
        median_age            FLOAT8,
        male_pop              INT,
        female_pop            INT,
        total_pop             INT,
        num_veterans          INT,
        avg_house_size        FLOAT8
    )
""")




# QUERY LISTS
drop_table_queries = [staging_visits_table_drop, staging_weather_table_drop, staging_cities_table_drop]
create_table_queries = [staging_visits_table_create, staging_weather_table_create, staging_cities_table_create]

