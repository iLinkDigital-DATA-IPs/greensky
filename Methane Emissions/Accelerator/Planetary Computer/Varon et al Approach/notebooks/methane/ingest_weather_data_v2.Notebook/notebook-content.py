# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "95f7f55e-9d06-4699-9483-754709a1d397",
# META       "default_lakehouse_name": "Planetary_computer_LH",
# META       "default_lakehouse_workspace_id": "060ba34b-f1a3-4509-a6e2-36d1e736a8eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "95f7f55e-9d06-4699-9483-754709a1d397"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "cf70e84c-e5f3-9589-4218-88cc1ae7b47d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Weather Data Analysis
# 
# #### Extracts weather information and atmospheric data from WeatherFeeds stream and meteo historic API. 
# 
# ##### Swath passing over Permian Basin, Texas, US 

# CELL ********************

# IMPORTS
import time
import logging
from pyspark.sql.functions import col, lit, to_timestamp, to_date
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import requests
import pandas as pd
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("weather_bronze")
 
TABLE_NAME = "bronze.weather"
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create table if it doesn't already exist.
# Schema is identical to the previous version so downstream consumers are unaffected.
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.weather (
    dateTime          TIMESTAMP,
    date              DATE,
    temperature       DOUBLE,
    relativeHumidity  DOUBLE,
    pressure          DOUBLE,
    wind_speed        DOUBLE,
    wind_direction    DOUBLE,
    latitude          DOUBLE,
    longitude         DOUBLE,
    locationName      STRING,
    source            STRING
)
USING DELTA
""")
 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Meteo-Historic Data

# CELL ********************

# Define locations of interest
locations = [
    {"locationName": "Permian Basin", "latitude": 31.5,  "longitude": -102.0},
    {"locationName": "Texas Site A",  "latitude": 30.2,  "longitude": -101.5},
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reuse TCP connection across all requests for performance
session = requests.Session()
 
def fetch_weather_for_location(lat, lon, start_date, end_date, retries=3):
    """
    Fetch daily weather from the Open-Meteo archive API for a single location.
    Returns the 'daily' dict on success, None on failure.
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":  round(lat, 3),
        "longitude": round(lon, 3),
        "start_date": start_date,
        "end_date":   end_date,
        "daily": ",".join([
            "temperature_2m_mean",
            "relative_humidity_2m_mean",
            "surface_pressure_mean",
            "wind_speed_10m_max",
            "wind_direction_10m_dominant",
        ]),
        "timezone": "UTC",
    }
 
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=(5, 30))
 
            if response.status_code == 429:
                wait = 2 ** attempt
                logger.warning("Rate-limited — waiting %ds before retry.", wait)
                time.sleep(wait)
                continue
 
            if response.status_code != 200:
                raise ValueError(f"Unexpected status: {response.status_code}")
 
            json_data = response.json()
 
            if "daily" not in json_data:
                logger.warning("No 'daily' key in response for (%s, %s).", lat, lon)
                return None
 
            return json_data["daily"]
 
        except Exception as exc:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                logger.error("API failed for (%s, %s) after %d attempts: %s", lat, lon, retries, exc)
                return None
 
 
def safe_get(data, key, i):
    """Return data[key][i] safely, or None on any error."""
    try:
        return data.get(key, [None])[i]
    except Exception:
        return None
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Date range: last 5 days up to today (UTC)
end_date   = datetime.utcnow()
start_date = end_date - timedelta(days=5)
 
start_str = start_date.strftime("%Y-%m-%d")
end_str   = end_date.strftime("%Y-%m-%d")
 
logger.info("Fetching weather data from %s to %s.", start_str, end_str)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch from API and build row list
rows = []
 
for loc in locations:
    logger.info("Fetching for %s ...", loc["locationName"])
 
    data = fetch_weather_for_location(
        loc["latitude"],
        loc["longitude"],
        start_str,
        end_str,
    )
 
    if data is None or "time" not in data:
        logger.warning("No data returned for %s — skipping.", loc["locationName"])
        continue
 
    for i, d in enumerate(data["time"]):
        temperature      = safe_get(data, "temperature_2m_mean",          i)
        relativeHumidity = safe_get(data, "relative_humidity_2m_mean",     i)
        pressure         = safe_get(data, "surface_pressure_mean",         i)
        wind_speed       = safe_get(data, "wind_speed_10m_max",            i)
        wind_direction   = safe_get(data, "wind_direction_10m_dominant",   i)
 
        # Skip rows where every weather field is None
        if all(v is None for v in [temperature, relativeHumidity, pressure, wind_speed, wind_direction]):
            continue
 
        rows.append({
            "dateTime":         pd.to_datetime(d),
            "latitude":         loc["latitude"],
            "longitude":        loc["longitude"],
            "locationName":     loc["locationName"],
            "temperature":      temperature,
            "relativeHumidity": relativeHumidity,
            "pressure":         pressure,
            "wind_speed":       wind_speed,
            "wind_direction":   wind_direction,
        })
 
weather_pd = pd.DataFrame(rows)
logger.info("Total rows fetched: %d", len(weather_pd))
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Clean, merge and write table to bronze layer

# CELL ********************

spark_weather_df = spark.createDataFrame(weather_pd)
 
spark_weather_df = (
    spark_weather_df
    .withColumn("date",   to_date("dateTime"))
    .withColumn("source", lit("historical"))
    .dropDuplicates(["dateTime", "latitude", "longitude"])
    .replace(float("nan"), None)   # pandas NaN → NULL in Delta
)
 
try:
    DeltaTable.forName(spark, TABLE_NAME)
    table_exists = True
except AnalysisException:
    table_exists = False
 
if table_exists:
    logger.info("Table exists — performing MERGE into %s.", TABLE_NAME)
 
    (
        DeltaTable.forName(spark, TABLE_NAME)
        .alias("t")
        .merge(
            spark_weather_df.alias("s"),
            """
            t.dateTime  = s.dateTime
            AND t.latitude  = s.latitude
            AND t.longitude = s.longitude
            """,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
 
    logger.info("Merge complete.")
 
else:
    logger.info("Table does not exist — creating %s.", TABLE_NAME)
    (
        spark_weather_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(TABLE_NAME)
    )
    logger.info("Table created.")
 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
