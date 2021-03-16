import os
import glob
import psycopg2
import pandas as pd

# DROP TABLES
staging_visits_table_drop = "DROP TABLE IF EXISTS staging_visits"
staging_weather_table_drop = "DROP TABLE IF EXISTS staging_weather"
staging_cities_table_drop = "DROP TABLE IF EXISTS staging_cities"
staging_airports_table_drop = "DROP TABLE IF EXISTS staging_airports"
visits_table_drop = "DROP TABLE IF EXISTS visits"
tourists_table_drop = "DROP TABLE IF EXISTS tourists"
airports_table_drop = "DROP TABLE IF EXISTS airports"
tourists_table_drop = "DROP TABLE IF EXISTS cities"

# CREATE TABLES
staging_visits_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_visits (
        visit_id              VARCHAR,
        tourist_id            VARCHAR,
        arrival_date          DATE,
        depart_date           DATE,
        citizenship_id        VARCHAR,
        airport_id            VARCHAR,
        airport_name          VARCHAR,
        city_id               VARCHAR,
        city_name             VARCHAR,
        mode_of_travel        VARCHAR,
        airline               VARCHAR,
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

visits_table_create = ("""
    CREATE TABLE IF NOT EXISTS visits (
        id                    SERIAL PRIMARY KEY,
        airport_id            VARCHAR,
        city_id               VARCHAR,
        arrival_date          DATE,
        depart_date           DATE,
        mode_of_travel        VARCHAR,
        airline               VARCHAR,
        citizen_id            VARCHAR,
        visa_type             VARCHAR
    
    )

""")

tourists_table_create = ("""
    CREATE TABLE IF NOT EXISTS tourists (
        id                    SERIAL PRIMARY KEY,
        citizen_id            VARCHAR FOREIGN KEY NOT NULL,
        birth_year            INT,
        cntry_citizenship.    VARCHAR,
        gender                VARCHAR  
    )
""")

airports_table_create = ("""
    CREATE TABLE IF NOT EXISTS airports (
        id                    SERIAL PRIMARY KEY,
        iata_id               VARCHAR,
        name                  VARCHAR,
        municipality          VARCHAR,
        state                 VARCHAR,
        elevation_ft          DECIMAL,
        longitude             DECIMAL,
        latitude              DECIMAL
    )
""")

cities_table_create = ("""
    CREATE TABLE IF NOT EXISTS cities (
        id                    SERIAL PRIMARY KEY,
        name                  VARCHAR,
        state                 VARCHAR,
        median_age            FLOAT8,
        male_pop              INT,
        female_pop            INT,
        total_pop             INT,
        num_veterans          INT,
        avg_house_size        FLOAT8
    )
""")

# INSERT RECORDS
staging_visits_insert = ("""
    INSERT INTO staging_vists (
        visit_id              VARCHAR,
        tourist_id            VARCHAR,
        arrival_date          DATE,
        depart_date           DATE,
        citizenship_id        VARCHAR,
        airport_id            VARCHAR,
        airport_name          VARCHAR,
        city_id               VARCHAR,
        city_name             VARCHAR,
        mode_of_travel        VARCHAR,
        airline               VARCHAR,
        visa_type             VARCHAR,
        birth_year            INT,
        gender                VARCHAR  
    )
    VALUES (%s, %s, %s, %s, %s)
""")



user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(user_id) DO UPDATE SET level=EXCLUDED.level;
""")

# QUERY LISTS
drop_table_queries = [staging_visits_table_drop,
                      staging_weather_table_drop, staging_cities_table_drop]

create_table_queries = [staging_visits_table_create,
                        staging_weather_table_create,
                        staging_cities_table_create,
                        visits_table_create,
                        tourists_table_create,
                        airports_table_create,
                        cities_table_create]

