from collections import defaultdict

# MAIN INPUTS
CONFIG_APP_NAME = "udac_config"
SESSION_APP_NAME = "udac_cap"
CLUSTER_LOCATION = "local"
OUTPUT_PATH = "test_output/"

# AIRPORT DATASET
AIRPORTS_URL_PATH = "data/airports.csv"

# USA CITIES DATASET
USA_CITIES_URL_PATH = "data/us_cities_demographics.csv"

USA_CITIES_RENAME_COLS = {"City": "city",
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


RACE_RENAME_COLS = {"American Indian and Alaska Native": "native_american_pop",
                    "Asian": "asian_pop",
                    "Black or African-American": "black_american_pop",
                    "Hispanic or Latino": "hispanic_pop",
                    "White": "white_pop"}

USA_CITIES_INTEGER_VARS = ["male_pop", "female_pop", "total_pop",
                            "num_veterans", "num_foreigners", "race_pop"]

USA_CITIES_FLOAT_VARS = ["median_age", "avg_household_size"]


# TEMPERATURE DATASET
TEMPERATURE_URL_PATH = "data/GlobalLandTemperaturesByCity.csv"

TEMPERATURE_RENAME_COLS = {"dt": "date",
                        "AverageTemperature": "avg_daily_temp",
                        "AverageTemperatureUncertainty": "avg_temp_uncertainty",
                        "City": "city",
                        "Latitude": "latitude",
                        "Longitude": "longitude"}


# USA Visits Dataset
TOURISM_URL_PATH = "data/immigration_data_sample.csv"

AIRPORT_CODES = "data/airport_codes.csv"

COUNTRY_CODES = "data/country_codes.csv"

TOURISM_RENAME_COLS = {"_c0": "visit_id",
                    "cicid": "citizen_id",
                    "i94yr": "arrival_yr",
                    "i94mon": "arrival_month",
                    "i94cit": "citizen_cntry_code",
                    "i94res": "residency_cntry_code",
                    "i94port": "airport",
                    "i94mode": "travel_mode",
                    "i94addr": "airport_state",
                    "i94visa": "reason_for_travel",
                    "count": "num_people",
                    "biryear": "birth_year",
                    "visatype": "visa_type"}

TOURISM_INTEGER_VARS = ["citizen_id", "arrival_yr", "arrival_month",
                        "citizen_cntry_code", "residency_cntry_code", "travel_mode",
                        "reason_for_travel", "num_people", "birth_year"]


MARITIME_SIGNAL_FLAGS = defaultdict(lambda: "nan",
                        {"A": "Diver below. Undergoing a speed trial",
                        "D": "Keep clear of me. Manoevering with difficulty",
                        "G": "Require a pilot",
                        "H": "Pilot on board",
                        "I": "Altering my course to port",
                        "J": "Sending a message by semaphore",
                        "K": "Stop vessel instantly",
                        "L": "Stop. I have something important to communicate",
                        "N": "No (negative)",
                        "O": "Man overboard",
                        "P": "Your lights are out",
                        "Q": "My vessel is health. Requesting permission to land",
                        "R": "My vessel is off course.",
                        "T": "Do not pass ahead of me",
                        "U": "You're heading into danger.",
                        "W": "Require medical assitance",
                        "Z": "Call to shore stations"})
                               
REASON_FOR_TRAVEL = {1: "Business",
                     2: "Pleasure",
                     3: "Student"}

MODE_OF_TRAVEL = {1: "Air",
                  2: "Sea",
                  3: "Land",
                  9: "Not reported"}

DROP_TOURISM_COLS = ["insnum", "dtadfile", "fltno", 'i94bir', "occup", "matflag",
                    "admnum", "entdepu", "visapost", "arrdate", "depdate"]