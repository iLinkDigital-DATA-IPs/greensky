# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5d5c8002-789e-4319-81d1-a60f08a77996",
# META       "default_lakehouse_name": "greensky_lakehouse",
# META       "default_lakehouse_workspace_id": "640876ea-6158-4ffd-8598-5eb210e088a0",
# META       "known_lakehouses": [
# META         {
# META           "id": "5d5c8002-789e-4319-81d1-a60f08a77996"
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

# ### Load config:

# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build weather station grid:

# CELL ********************

import numpy as np
import itertools

# Create a grid of query points across the Permian Basin
# Open-Meteo is a point-based API, so we query a grid and interpolate later
spacing = CONFIG["weather_grid_spacing"]  # 0.5 degrees

lats = np.arange(BBOX["min_lat"], BBOX["max_lat"] + spacing, spacing)
lons = np.arange(BBOX["min_lon"], BBOX["max_lon"] + spacing, spacing)
grid_points = list(itertools.product(lats, lons))

print(f"Weather grid: {len(lats)} lats x {len(lons)} lons = {len(grid_points)} points")
print(f"Lat range: {lats[0]} to {lats[-1]}")
print(f"Lon range: {lons[0]} to {lons[-1]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Query Open-Meteo for all grid points:

# CELL ********************

import requests
import pandas as pd
import time

start_date = CONFIG["start_date"]
end_date = CONFIG["end_date"]

all_weather = []
failed_points = []

for i, (lat, lon) in enumerate(grid_points):
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly=wind_speed_10m,wind_direction_10m,"
        f"temperature_2m,relative_humidity_2m,surface_pressure"
        f"&wind_speed_unit=ms"
    )

    try:
        r = requests.get(url, timeout=60)
        if r.status_code == 200:
            data = r.json()
            hourly = data.get("hourly", {})

            if hourly and hourly.get("time"):
                df_point = pd.DataFrame({
                    "weather_lat": lat,
                    "weather_lon": lon,
                    "time": hourly["time"],
                    "wind_speed_10m": hourly.get("wind_speed_10m"),
                    "wind_direction_10m": hourly.get("wind_direction_10m"),
                    "temperature_2m": hourly.get("temperature_2m"),
                    "relative_humidity_2m": hourly.get("relative_humidity_2m"),
                    "surface_pressure": hourly.get("surface_pressure"),
                })
                all_weather.append(df_point)
        else:
            failed_points.append((lat, lon, r.status_code))
    except Exception as e:
        failed_points.append((lat, lon, str(e)))

    # Progress update every 10 points
    if (i + 1) % 10 == 0:
        print(f"  Queried {i + 1}/{len(grid_points)} points...")

    # Rate limiting -- Open-Meteo allows ~600 requests/minute for free tier
    time.sleep(0.15)

print(f"\nCompleted: {len(all_weather)} successful, {len(failed_points)} failed")
if failed_points:
    print(f"Failed points: {failed_points[:5]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Convert to Spark DataFrame and write to Bronze:

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType, TimestampType
)

# Combine all weather data
weather_pdf = pd.concat(all_weather, ignore_index=True)

# Parse timestamps
weather_pdf["time"] = pd.to_datetime(weather_pdf["time"])

print(f"Total weather records: {len(weather_pdf):,}")
print(f"Date range: {weather_pdf['time'].min()} to {weather_pdf['time'].max()}")
print(f"Grid points with data: {weather_pdf[['weather_lat', 'weather_lon']].drop_duplicates().shape[0]}")

# Convert to Spark
weather_spark = spark.createDataFrame(weather_pdf)
weather_spark.printSchema()
weather_spark.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write to Bronze table:

# CELL ********************

TABLE_NAME = "bronze_weather"

weather_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(TABLE_NAME)

count = spark.table(TABLE_NAME).count()
print(f"Written {count:,} rows to {TABLE_NAME}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validate:

# CELL ********************

from pyspark.sql.functions import min as spark_min, max as spark_max, avg, count as spark_count

df = spark.table("bronze_weather")

print("=== Weather Data Summary ===")
df.select(
    spark_min("time").alias("earliest"),
    spark_max("time").alias("latest"),
    spark_count("*").alias("total_rows"),
    avg("wind_speed_10m").alias("avg_wind_speed"),
    avg("temperature_2m").alias("avg_temp"),
).show(truncate=False)

# Check for nulls
print("=== Null Counts ===")
for col_name in ["wind_speed_10m", "wind_direction_10m", "temperature_2m",
                  "relative_humidity_2m", "surface_pressure"]:
    null_count = df.filter(df[col_name].isNull()).count()
    print(f"  {col_name}: {null_count} nulls")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd
import numpy as np
import itertools
import time

start_date = CONFIG["start_date"]
end_date = CONFIG["end_date"]

# Find missing points
spacing = CONFIG["weather_grid_spacing"]
all_lats = np.arange(BBOX["min_lat"], BBOX["max_lat"] + spacing, spacing)
all_lons = np.arange(BBOX["min_lon"], BBOX["max_lon"] + spacing, spacing)
all_points = set(itertools.product(
    [round(x, 1) for x in all_lats],
    [round(x, 1) for x in all_lons]
))

existing = spark.table("bronze_weather").select("weather_lat", "weather_lon").distinct().collect()
existing_points = set((row.weather_lat, row.weather_lon) for row in existing)
missing_points = all_points - existing_points
print(f"Missing points to retry: {len(missing_points)}")

backfill = []
still_failed = []

for i, (lat, lon) in enumerate(missing_points):
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly=wind_speed_10m,wind_direction_10m,"
        f"temperature_2m,relative_humidity_2m,surface_pressure"
        f"&wind_speed_unit=ms"
    )

    for attempt in range(3):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 200:
                data = r.json()
                hourly = data.get("hourly", {})
                if hourly and hourly.get("time"):
                    df_point = pd.DataFrame({
                        "weather_lat": lat,
                        "weather_lon": lon,
                        "time": hourly["time"],
                        "wind_speed_10m": hourly.get("wind_speed_10m"),
                        "wind_direction_10m": hourly.get("wind_direction_10m"),
                        "temperature_2m": hourly.get("temperature_2m"),
                        "relative_humidity_2m": hourly.get("relative_humidity_2m"),
                        "surface_pressure": hourly.get("surface_pressure"),
                    })
                    backfill.append(df_point)
                    print(f"  ({lat}, {lon}): OK on attempt {attempt + 1}")
                    break
        except Exception as e:
            if attempt == 2:
                still_failed.append((lat, lon, str(e)))
                print(f"  ({lat}, {lon}): FAILED after 3 attempts")
        time.sleep(2)

print(f"\nBackfilled: {len(backfill)}, Still failed: {len(still_failed)}")

if backfill:
    backfill_pdf = pd.concat(backfill, ignore_index=True)
    backfill_pdf["time"] = pd.to_datetime(backfill_pdf["time"])
    backfill_spark = spark.createDataFrame(backfill_pdf)

    backfill_spark.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("bronze_weather")

    new_count = spark.table("bronze_weather").count()
    print(f"bronze_weather now has {new_count:,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
