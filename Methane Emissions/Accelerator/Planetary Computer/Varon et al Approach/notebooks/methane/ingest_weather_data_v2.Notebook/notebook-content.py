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
from pyspark.sql.functions import (
    col, lit, to_timestamp, to_date, current_date, from_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import requests
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("weather_bronze")

# Single source of truth for table name — was "dbo.weather" in the
# streaming block and "bronze.weather" in the historical block.
# Unified here so both paths write to the same table.
TABLE_NAME = "bronze.weather"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Live Data

# CELL ********************

# Set the event stream item and datasource IDs : WeatherFeeds
__in_eventstream_item_id = "69738a9d-905c-4ada-84a7-7eb3996b271f"
__in_eventstream_datasource_id = "cdd48ad7-0e01-4159-a602-9d1a8addfdc9"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.weather (
    dateTime TIMESTAMP,
    date DATE,
    temperature DOUBLE,
    relativeHumidity DOUBLE,
    pressure DOUBLE,
    wind_speed DOUBLE,
    wind_direction DOUBLE,
    latitude DOUBLE,
    longitude DOUBLE,
    locationName STRING,
    source STRING
)
USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Eventstream via Fabric's native Kafka-compatible connector.
# eventstream.itemid + eventstream.datasourceid are Fabric-specific options
# that resolve auth automatically — no connection string needed.
# Resilience options added: failOnDataLoss, maxOffsetsPerTrigger, retries.
df_raw = (
    spark.readStream
    .format("kafka")
    .option("eventstream.itemid",          __in_eventstream_item_id)
    .option("eventstream.datasourceid",    __in_eventstream_datasource_id)
    # Survive gaps if Eventstream retention window is shorter than stream downtime
    .option("failOnDataLoss",              "false")
    # Cap batch size — prevents one giant batch after a restart
    .option("maxOffsetsPerTrigger",        "10000")
    # Retry transient broker hiccups before failing the batch
    .option("fetchOffset.numRetries",      "5")
    .option("fetchOffset.retryIntervalMs", "1000")
    .load()
)

decoded_df = df_raw.select(
    col("value").cast(StringType()).alias("value")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define Schema 
schema = StructType([
    StructField("dateTime", StringType()),
    StructField("relativeHumidity", DoubleType()),
    
    StructField("temperature", StructType([
        StructField("value", DoubleType())
    ])),
    
    StructField("pressure", StructType([
        StructField("value", DoubleType())
    ])),
    
    StructField("wind", StructType([
        StructField("speed", StructType([
            StructField("value", DoubleType())
        ])),
        StructField("direction", StructType([
            StructField("degrees", DoubleType())
        ]))
    ])),
    
    StructField("location", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType())
    ])),
    
    StructField("locationName", StringType())
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Parse + Flatten JSON

parsed_df = decoded_df.withColumn(
    "data",
    from_json(col("value"), schema)
).select("data.*")

parsed_df = parsed_df.select(
    col("dateTime"),
    col("temperature.value").alias("temperature"),
    col("relativeHumidity"),
    col("pressure.value").alias("pressure"),
    col("wind.speed.value").alias("wind_speed"),
    col("wind.direction.degrees").alias("wind_direction"),
    col("location.latitude").alias("latitude"),
    col("location.longitude").alias("longitude"),
    col("locationName")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Time conversion + Filtering 
parsed_df = parsed_df.withColumn(
    "dateTime",
    to_timestamp("dateTime")
)

parsed_df = parsed_df.withColumn(
    "date",
    to_date("dateTime")
)

today_df = parsed_df.filter(
    col("date") == current_date()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#De-duplicate + Add source

live_df = today_df \
    .dropDuplicates(["dateTime", "latitude", "longitude"]) \
    .withColumn("source", lit("live"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CHECKPOINT_PATH    = "Files/checkpoints/bronze_weather/v2"
QUERY_NAME         = "weather_bronze_stream"
TRIGGER_INTERVAL   = "30 seconds"
STREAM_RUN_SECONDS = 300  # 5 minutes — tune to how fresh you need the data


def ensure_table_exists(sample_df) -> None:
    try:
        DeltaTable.forName(spark, TABLE_NAME)
    except AnalysisException:
        logger.info("Table '%s' not found — creating from first batch.", TABLE_NAME)
        sample_df.write.mode("append").format("delta").saveAsTable(TABLE_NAME)
        logger.info("Table '%s' created.", TABLE_NAME)


def upsert_weather_live(batch_df, batch_id: int) -> None:
    if batch_df.limit(1).count() == 0:
        logger.info("Batch %s: empty — skipping.", batch_id)
        return

    logger.info("Batch %s: processing started.", batch_id)

    batch_df.cache()

    try:
        ensure_table_exists(batch_df)

        (
            DeltaTable.forName(spark, TABLE_NAME)
            .alias("t")
            .merge(
                batch_df.alias("s"),
                """
                t.dateTime  = s.dateTime
                AND t.latitude  = s.latitude
                AND t.longitude = s.longitude
                """,
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info("Batch %s: merge complete.", batch_id)

    except Exception as exc:
        logger.error("Batch %s: FAILED — will retry. Error: %s", batch_id, exc, exc_info=True)
        raise

    finally:
        batch_df.unpersist()


# Stop any conflicting stream from a previous notebook run
for q in spark.streams.active:
    if q.name == QUERY_NAME:
        logger.info("Stopping existing stream '%s'", QUERY_NAME)
        q.stop()
        q.awaitTermination(timeout=60)

query = (
    live_df
    .writeStream
    .queryName(QUERY_NAME)
    .foreachBatch(upsert_weather_live)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

logger.info("Stream '%s' started — will run for %ds then stop.", QUERY_NAME, STREAM_RUN_SECONDS)

try:
    query.awaitTermination(timeout=STREAM_RUN_SECONDS)
except Exception as exc:
    logger.error("Stream terminated with error: %s", exc, exc_info=True)
    raise
finally:
    if query.isActive:
        logger.info("Stopping stream after %ds window.", STREAM_RUN_SECONDS)
        query.stop()
        query.awaitTermination(timeout=60)
        logger.info("Stream stopped cleanly — notebook continuing.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Meteo-Historic Data

# CELL ********************

#Define Location
locations = [
    {"locationName": "Permian Basin", "latitude": 31.5, "longitude": -102.0},
    {"locationName": "Texas Site A", "latitude": 30.2, "longitude": -101.5},
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Parallel Fetch Per Date

# Reuse TCP connection (BIG performance boost)
session = requests.Session()

def fetch_weather_for_location(lat, lon, start_date, end_date, retries=3):
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": round(lat, 3),
        "longitude": round(lon, 3),
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join([
            "temperature_2m_mean",
            "relative_humidity_2m_mean",
            "surface_pressure_mean",
            "wind_speed_10m_max",
            "wind_direction_10m_dominant"
        ]),
        "timezone": "UTC"
    }

    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=(5, 30))

            if response.status_code == 429:
                wait = 2 ** attempt
                time.sleep(wait)
                continue

            if response.status_code != 200:
                raise Exception(f"Bad status: {response.status_code}")

            json_data = response.json()

            if "daily" not in json_data:
                return None

            return json_data["daily"] 

        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"API failed for {lat}, {lon}: {e}")
                return None

# -------------------------------
# Helper (prevents index errors)
# -------------------------------
def safe_get(data, key, i):
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

#Fetch All Dates (Auto till today)
end_date = datetime.utcnow()
start_date = end_date - timedelta(days=5)

start_str = start_date.strftime("%Y-%m-%d")
end_str = end_date.strftime("%Y-%m-%d")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Fetch Loop
rows = []

for loc in locations:
    print(f"Fetching for {loc['locationName']}...")

    data = fetch_weather_for_location(
        loc["latitude"],
        loc["longitude"],
        start_str,
        end_str
    )

    if data is None or "time" not in data:
        continue

    # Loop through returned days (NOT API calls)
    for i, d in enumerate(data["time"]):

        row = {
            "dateTime": pd.to_datetime(d),
            "latitude": loc["latitude"],
            "longitude": loc["longitude"],
            "locationName": loc["locationName"],
            "temperature": safe_get(data, "temperature_2m_mean", i),
            "relativeHumidity": safe_get(data, "relative_humidity_2m_mean", i),
            "pressure": safe_get(data, "surface_pressure_mean", i),
            "wind_speed": safe_get(data, "wind_speed_10m_max", i),
            "wind_direction": safe_get(data, "wind_direction_10m_dominant", i),
        }

        # Skip bad rows
        if all(v is None for k, v in row.items() if k not in ["dateTime", "latitude", "longitude", "locationName"]):
            continue

        rows.append(row)

weather_pd = pd.DataFrame(rows)

print(f"Total rows: {len(weather_pd)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Clean, merge and write table to bronze layer

# CELL ********************

# TABLE_NAME already set to "bronze.weather" in Block 1 — consistent with live stream

spark_weather_df = spark.createDataFrame(weather_pd)

spark_weather_df = spark_weather_df \
    .withColumn("date", to_date("dateTime")) \
    .withColumn("source", lit("historical"))

spark_weather_df = spark_weather_df.dropDuplicates(
    ["dateTime", "latitude", "longitude"]
)

# Replace float NaN (from pandas) with None so Delta doesn't store NaN
spark_weather_df = spark_weather_df.replace(float("nan"), None)

# Use tableExists via AnalysisException pattern — consistent with live path
# and avoids the internal _jsparkSession.catalog() private API call
try:
    DeltaTable.forName(spark, TABLE_NAME)
    table_exists = True
except AnalysisException:
    table_exists = False

if table_exists:
    logger.info("Table exists — performing MERGE")

    (
        DeltaTable.forName(spark, TABLE_NAME)
        .alias("t")
        .merge(
            spark_weather_df.alias("s"),
            """
            t.dateTime  = s.dateTime
            AND t.latitude  = s.latitude
            AND t.longitude = s.longitude
            """
        )
        # whenNotMatchedInsertAll() is safer than manually mapping cols —
        # won't break if source/target schemas drift slightly
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info("Historical merge complete.")

else:
    logger.info("Table does not exist — creating.")
    spark_weather_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TABLE_NAME)
    logger.info("Table created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
