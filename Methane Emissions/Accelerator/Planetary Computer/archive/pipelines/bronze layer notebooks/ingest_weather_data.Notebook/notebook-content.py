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

#IMPORTS
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.functions import to_timestamp, to_date, current_date
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import numpy as np
import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql.functions import to_date, lit
from datetime import datetime, timedelta
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

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

#Read Event Stream (Kafka)

eventstream_options = {
    "eventstream.itemid": __in_eventstream_item_id,
    "eventstream.datasourceid": __in_eventstream_datasource_id
}

df_raw = spark.readStream \
    .format("kafka") \
    .options(**eventstream_options) \
    .load()

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

TABLE_NAME = "dbo.weather"
CHECKPOINT_PATH = "Files/checkpoints/bronze_weather"
QUERY_NAME = "weather_bronze_stream"

# -------------------------------
# Helper: Stop existing stream with same name
# -------------------------------
def stop_existing_stream(query_name):
    for q in spark.streams.active:
        if q.name == query_name:
            print(f"Stopping existing stream: {q.name}")
            q.stop()

# -------------------------------
# Upsert Function
# -------------------------------
def upsert_weather(batch_df, batch_id):

    # -------------------------------
    # 0. Efficient empty check
    # -------------------------------
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: empty")
        return

    print(f"Processing batch {batch_id}")

    # -------------------------------
    # 1. Add source
    # -------------------------------
    batch_df = batch_df.withColumn("source", lit("live"))

    # -------------------------------
    # 2. Deduplicate batch
    # -------------------------------
    batch_df = batch_df.dropDuplicates(
        ["dateTime", "latitude", "longitude"]
    )

    # -------------------------------
    # 3. Merge into Delta
    # -------------------------------
    try:
        delta_table = DeltaTable.forName(spark, TABLE_NAME)

        delta_table.alias("t").merge(
            batch_df.alias("s"),
            """
            t.dateTime = s.dateTime
            AND t.latitude = s.latitude
            AND t.longitude = s.longitude
            """
        ).whenNotMatchedInsertAll().execute()

        print(f"Batch {batch_id}: merged")

    except Exception as e:
        print(f"Table not found or error: {e}")
        print("Creating new table...")

        batch_df.write \
            .mode("overwrite") \
            .format("delta") \
            .saveAsTable(TABLE_NAME)

        print(f"Batch {batch_id}: table created")

# -------------------------------
# Start Stream Safely
# -------------------------------

# 1. Stop existing stream (prevents checkpoint conflict)
stop_existing_stream(QUERY_NAME)

# 2. Start stream
query = live_df.writeStream \
    .queryName(QUERY_NAME) \
    .foreachBatch(upsert_weather) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

# 3. Safe termination
try:
    query.awaitTermination(120)
finally:
    query.stop()

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
start_date = datetime(2024, 1, 1) #date change
end_date = datetime.utcnow()

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

TABLE_NAME = "dbo.weather"

# -------------------------------
# 1. Convert to Spark + align schema
# -------------------------------
spark_weather_df = spark.createDataFrame(weather_pd)

spark_weather_df = spark_weather_df \
    .withColumn("date", to_date("dateTime")) \
    .withColumn("source", lit("historical"))

# -------------------------------
# 2. De-duplicate
# -------------------------------
spark_weather_df = spark_weather_df.dropDuplicates(
    ["dateTime", "latitude", "longitude"]
)

# -------------------------------
# 3. Clean NaNs
# -------------------------------
spark_weather_df = spark_weather_df.replace(float("nan"), None)

# -------------------------------
# 4. Check if table exists
# -------------------------------
if spark._jsparkSession.catalog().tableExists(TABLE_NAME):

    print("Table exists : performing MERGE")

    delta_table = DeltaTable.forName(spark, TABLE_NAME)

    cols = spark.table(TABLE_NAME).columns

    delta_table.alias("t").merge(
        spark_weather_df.alias("s"),
        """
        t.dateTime = s.dateTime 
        AND t.latitude = s.latitude 
        AND t.longitude = s.longitude
        """
    ).whenNotMatchedInsert(
        values={c: f"s.{c}" for c in cols}
    ).execute()

else:
    print("Table does not exist : creating new table")

    spark_weather_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(TABLE_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
