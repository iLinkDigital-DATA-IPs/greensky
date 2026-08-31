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

# ### Load source tables:

# CELL ********************

from pyspark.sql.functions import (
    col, radians, sin, cos, sqrt, lit, exp,
    unix_timestamp, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast

# Load methane pixels (Permian Basin only)
methane = spark.table("bronze_methane_pixels").filter(
    (col("latitude") >= BBOX["min_lat"]) &
    (col("latitude") <= BBOX["max_lat"]) &
    (col("longitude") >= BBOX["min_lon"]) &
    (col("longitude") <= BBOX["max_lon"])
)
methane_count = methane.count()
print(f"Methane pixels (Permian): {methane_count:,}")

# Load weather
weather = spark.table("bronze_weather")
weather_count = weather.count()
print(f"Weather records: {weather_count:,}")

# Check for ERA5
era5_available = False
try:
    era5 = spark.table("bronze_era5_wind")
    era5_available = True
    print(f"ERA5 records: {era5.count():,}")
except Exception:
    print("ERA5 table not found -- will use Open-Meteo wind only")
    print("ERA5 columns will be added as null placeholders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Decompose wind to u/v components:

# CELL ********************

# Wind decomposition: speed + direction -> u/v components
# Meteorological convention: direction is where wind comes FROM
# u = east-west component (positive = eastward)
# v = north-south component (positive = northward)

weather = weather.withColumn(
    "wind_u",
    -col("wind_speed_10m") * sin(radians(col("wind_direction_10m")))
).withColumn(
    "wind_v",
    -col("wind_speed_10m") * cos(radians(col("wind_direction_10m")))
)

print("Wind u/v components added")
weather.select(
    "weather_lat", "weather_lon", "time",
    "wind_speed_10m", "wind_direction_10m", "wind_u", "wind_v"
).show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Round timestamps to nearest hour for joining:

# CELL ********************

# Round methane timestamps to nearest hour
methane = methane.withColumn(
    "datetime_hour",
    (
        (unix_timestamp("datetime") / 3600).cast("long") * 3600
    ).cast("timestamp")
)

# Round weather timestamps to nearest hour
weather = weather.withColumn(
    "time_hour",
    (
        (unix_timestamp("time") / 3600).cast("long") * 3600
    ).cast("timestamp")
)

print("Timestamps rounded to nearest hour")
methane.select("datetime", "datetime_hour").show(3, truncate=False)
weather.select("time", "time_hour").show(3, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Temporal join:

# CELL ********************

# Join methane pixels to weather on matching hour
# Weather table is small (45K rows), so broadcast it
joined = methane.join(
    broadcast(weather),
    methane["datetime_hour"] == weather["time_hour"],
    "inner"
)

joined_count = joined.count()
print(f"After temporal join: {joined_count:,} rows")
print(f"Expansion factor: {joined_count / methane_count:.1f}x (each pixel matched to {weather.select('weather_lat', 'weather_lon').distinct().count()} weather stations)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Compute distance, weight, and keep nearest station:

# CELL ********************

# Distance approximation (degrees to km)
# At ~32N latitude: 1 deg lat ~ 111 km, 1 deg lon ~ 94 km
joined = joined.withColumn(
    "dist_km",
    sqrt(
        ((col("latitude") - col("weather_lat")) * 111.0) ** 2 +
        ((col("longitude") - col("weather_lon")) * 94.0) ** 2
    )
)

# Gaussian decay weight (sigma = 50 km)
joined = joined.withColumn(
    "weather_weight",
    exp(-col("dist_km") ** 2 / (2.0 * 50.0 ** 2))
)

# Keep only the nearest weather station per methane pixel
window = Window.partitionBy(
    "latitude", "longitude", "datetime"
).orderBy("dist_km")

nearest = joined.withColumn(
    "rank", row_number().over(window)
).filter(
    col("rank") == 1
).drop("rank")

nearest_count = nearest.count()
print(f"After nearest-station selection: {nearest_count:,} rows")
print(f"Should match original methane count: {methane_count:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Add ERA5 data (or null placeholders):

# CELL ********************

if era5_available:
    # Round ERA5 time to nearest hour
    era5 = era5.withColumn(
        "era5_time_hour",
        (
            (unix_timestamp("era5_time") / 3600).cast("long") * 3600
        ).cast("timestamp")
    )

    # Temporal join
    nearest = nearest.join(
        broadcast(era5),
        nearest["datetime_hour"] == era5["era5_time_hour"],
        "left"
    )

    # Compute ERA5 grid point distance
    nearest = nearest.withColumn(
        "era5_dist_km",
        sqrt(
            ((col("latitude") - col("era5_lat")) * 111.0) ** 2 +
            ((col("longitude") - col("era5_lon")) * 94.0) ** 2
        )
    )

    # Keep nearest ERA5 grid point per pixel
    window_era5 = Window.partitionBy(
        "latitude", "longitude", "datetime"
    ).orderBy("era5_dist_km")

    nearest = nearest.withColumn(
        "era5_rank", row_number().over(window_era5)
    ).filter(
        col("era5_rank") == 1
    ).drop("era5_rank", "era5_dist_km")

    era5_filled = nearest.filter(col("u10").isNotNull()).count()
    print(f"ERA5 data joined: {era5_filled:,} rows with ERA5 wind")
else:
    nearest = (
        nearest
        .withColumn("u10", lit(None).cast("double"))
        .withColumn("v10", lit(None).cast("double"))
        .withColumn("boundary_layer_height", lit(None).cast("double"))
    )
    print("ERA5 not available -- null placeholder columns added")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Select final columns and write to Silver:

# CELL ********************

silver_df = nearest.select(
    # Methane pixel data
    col("latitude"),
    col("longitude"),
    col("ch4"),
    col("qa_value"),
    col("datetime"),
    col("stac_id"),
    col("gas"),
    col("processing_level"),

    # Open-Meteo weather
    col("wind_speed_10m"),
    col("wind_direction_10m"),
    col("wind_u"),
    col("wind_v"),
    col("temperature_2m"),
    col("relative_humidity_2m"),
    col("surface_pressure"),
    col("weather_weight"),
    col("dist_km").alias("weather_dist_km"),

    # ERA5 wind (filled or null)
    col("u10").alias("era5_u10"),
    col("v10").alias("era5_v10"),
    col("boundary_layer_height").alias("era5_blh"),
)

TABLE_NAME = "silver_plume_ready_pixels"

silver_df.write \
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

# ### Validate Silver table:

# CELL ********************

from pyspark.sql.functions import avg, min as spark_min, max as spark_max

df = spark.table("silver_plume_ready_pixels")

print("=== Silver Table Summary ===")
df.select(
    spark_min("datetime").alias("earliest"),
    spark_max("datetime").alias("latest"),
    avg("ch4").alias("avg_ch4_ppb"),
    avg("wind_speed_10m").alias("avg_wind_speed_ms"),
    avg("wind_u").alias("avg_wind_u"),
    avg("wind_v").alias("avg_wind_v"),
    avg("weather_dist_km").alias("avg_weather_dist_km"),
).show(truncate=False)

# ERA5 fill rate
total = df.count()
era5_filled = df.filter(col("era5_u10").isNotNull()).count()
print(f"ERA5 fill rate: {era5_filled}/{total} ({100.0 * era5_filled / total:.1f}%)")

# Null check across all columns
print("\n=== Null Counts ===")
for col_name in df.columns:
    null_count = df.filter(df[col_name].isNull()).count()
    if null_count > 0:
        print(f"  {col_name}: {null_count:,} nulls")

# Weather distance sanity check
print("\n=== Weather Distance Stats ===")
df.select("weather_dist_km").summary("min", "25%", "50%", "75%", "max").show()

# Sample rows
print("\n=== Sample Rows ===")
df.select(
    "latitude", "longitude", "ch4", "wind_speed_10m",
    "wind_u", "wind_v", "era5_u10", "era5_v10", "era5_blh"
).show(5)

print("\n=== Schema ===")
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
