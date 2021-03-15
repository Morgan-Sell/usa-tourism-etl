import os
import glob
import psycopg2
import pandas as pd

# STAGING TABLES
staging_visits_drop = "DROP TABLE IF EXISTS staging_visits"
staging_weather_drop = "DROP TABLE IF EXISTS staging_weather"
staging_usa_cities_drop = "DROP TABLE IF EXISTS staging_usa_cities"




# CREATE TABLES
staging_visits_table = ("""
    CREATE TABLE IF NOT EXISTS staging_visits (
        visit_id SERIAL PRIMARY KEY,
        tourist_id INTEGER NOT NULL FOREIGN KEY,
        arr_date NOT NULL DATE FOREIGN KEY,
        depart_date NOT NULL DATE,
        citizenship_id,
        airport_id VARCHAR(5) NOT NULL,
        airport_name VARCHAR(60),
        city_id VARCHAR(5) NOT NULL,
        city_name VARCHAR(60),
        mode_of_travel VARCHAR(15),
        airline VARCHAR(30),
        visa_post VARCHAR(40),
        visa_type VARCHAR(4),
        birth_year INTEGER,
        gender VARCHAR(2)   
    )
""")

staging_weather_table = ("""
    CREATE TABLE IF NOT EXISTS staging_weather (
        weather_id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        city_id NOT NULL,
        avg_daily_temp DECIMAL NOT NULL,
        avg_daily_temp_uncrtnty DECIMAL,
        lattitude VARCHAR(10),
        longitude VARCHAR(10)
    
    )



""")