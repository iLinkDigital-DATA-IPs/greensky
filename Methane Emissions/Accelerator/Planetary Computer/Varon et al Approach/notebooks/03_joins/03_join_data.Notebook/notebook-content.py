# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5d5c8002-789e-4319-81d1-a60f08a77996",
# META       "default_lakehouse_name": "greensky_v2_lakehouse",
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
    unix_timestamp, row_number, when
)
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast

# Load methane pixels (Permian Basin only)
methane = spark.table("bronze_ch4_pixels").filter(
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

# ### Diagnostic: duplication check on bronze_ch4_pixels (pre-join):
#
# Checks whether duplicate pixel rows already exist in bronze before any join. Includes a
# distinct count on (orbit, latitude, longitude) alongside the (stac_id, ...) counts,
# because NRTI and OFFL items for the same orbit carry different stac_ids for the same
# physical pixel -- a dedup key that includes stac_id would silently miss that overlap.
# (orbit, scanline, ground_pixel) is not used here: scanline is granule-relative, not
# orbit-relative, so it never collides across granules and is uninformative for this check.
# If total rows is roughly 2x distinct_orbit_lat_lon and the processing_mode split below
# is roughly even, that confirms NRTI/OFFL overlap as the source of duplication.

# CELL ********************

from pyspark.sql.functions import countDistinct

print("=== Duplication check: bronze_ch4_pixels (post BBOX filter, pre-join) ===")
print(f"Total rows: {methane_count:,}")
methane.select(
    countDistinct("stac_id", "scanline", "ground_pixel").alias("distinct_stac_scanline_gp"),
    countDistinct("stac_id", "latitude", "longitude").alias("distinct_stac_lat_lon"),
    countDistinct("orbit", "latitude", "longitude").alias("distinct_orbit_lat_lon"),
).show(truncate=False)

print("Row count by processing_mode:")
methane.groupBy("processing_mode").count().show(truncate=False)

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

# Casting a double to long truncates toward zero, so the previous
# (unix_timestamp / 3600).cast("long") * 3600 was actually a floor, not a round --
# 19:58:59 became 19:00:00, matching a pixel to weather up to 59 minutes stale. Since
# U_eff drives T_mix = L/U_eff in 04, that staleness propagates directly into the
# emission-rate quantification. Round to the nearest hour instead.
from pyspark.sql.functions import from_unixtime, round as spark_round, minute

# Round methane timestamps to nearest hour
methane = methane.withColumn(
    "datetime_hour",
    from_unixtime(spark_round(unix_timestamp("datetime") / 3600) * 3600).cast("timestamp")
)

# Round weather timestamps to nearest hour
weather = weather.withColumn(
    "time_hour",
    from_unixtime(spark_round(unix_timestamp("time") / 3600) * 3600).cast("timestamp")
)

print("Timestamps rounded to nearest hour (not floored)")
methane.select("datetime", "datetime_hour").show(3, truncate=False)
print("Sample from the second half of an hour (verifies rounding, not flooring):")
methane.filter(minute("datetime") >= 30).select("datetime", "datetime_hour").show(3, truncate=False)
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

# ### Diagnostic: duplication check after the weather join (pre nearest-station selection):
#
# Same checks as above, run on the joined (fanned-out) table. distinct_orbit_lat_lon
# should be unchanged from the pre-join check -- same set of physical pixels, just
# multiplied across weather stations -- if it grew, the join itself is introducing
# duplication rather than the expected one-pixel-to-many-weather-stations fan-out.

# CELL ********************

print("=== Duplication check: after weather temporal join (pre nearest-station selection) ===")
print(f"Total rows: {joined_count:,}")
joined.select(
    countDistinct("stac_id", "scanline", "ground_pixel").alias("distinct_stac_scanline_gp"),
    countDistinct("stac_id", "latitude", "longitude").alias("distinct_stac_lat_lon"),
    countDistinct("orbit", "latitude", "longitude").alias("distinct_orbit_lat_lon"),
).show(truncate=False)

print("Row count by processing_mode:")
joined.groupBy("processing_mode").count().show(truncate=False)

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
    # Round ERA5 time to nearest hour -- must match the rounding used for datetime_hour
    # above (nearest, not floor), since this join key is compared against datetime_hour
    # directly below. A mismatch here silently nulls out era5_u10/era5_v10 for any pixel
    # whose hour rounds differently under the two schemes, and 04 falls back to the fixed
    # 48h mixing time without raising -- see the null-rate assertion after this join.
    era5 = era5.withColumn(
        "era5_time_hour",
        from_unixtime(spark_round(unix_timestamp("era5_time") / 3600) * 3600).cast("timestamp")
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
    era5_null = nearest_count - era5_filled
    era5_null_pct = (100.0 * era5_null / nearest_count) if nearest_count else 0.0
    print(f"ERA5 data joined: {era5_filled:,} rows with ERA5 wind")
    print(f"ERA5 null rate: {era5_null:,}/{nearest_count:,} ({era5_null_pct:.2f}%)")

    # Guards against join-key drift between datetime_hour and era5_time_hour (e.g. one
    # side floored and the other rounded, as happened once already). A silent null here
    # makes 04 fall back to the fixed 48h mixing time for the affected pixels, which
    # changes emission rates by roughly two orders of magnitude, without raising anything.
    assert era5_null_pct <= 10.0, (
        f"ERA5 join produced {era5_null_pct:.2f}% null era5_u10 (> 10% threshold) -- "
        "likely a mismatch between the datetime_hour and era5_time_hour rounding/join keys."
    )
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
    col("scanline"),
    col("ground_pixel"),
    col("n_ground_pixels"),
    col("processing_mode"),
    col("orbit"),

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

# ── Deduplicate on (orbit, latitude, longitude) ──
# 07c found the same physical detector cell appearing twice with near-identical CH4
# (e.g. 1931.955 vs 1931.908 ppb at the same lat/lon). Root cause: this pipeline ingests
# both NRTI and OFFL processing modes (02_ingest_tropomi_ch4), and the same orbit is
# delivered as two separate STAC items -- with different stac_ids -- covering the same
# physical pixels. Keying dedup on stac_id would silently miss this, since the stac_ids
# differ.
#
# scanline/ground_pixel are NOT a valid substitute key here: scanline is granule-relative,
# not orbit-relative -- two granules from the same orbit both number their scanlines from
# zero, so the same physical pixel gets different scanline values across granules and
# (orbit, scanline, ground_pixel) never collides. Verified on a full pull: 35,522 rows,
# 35,522 distinct on (orbit, scanline, ground_pixel), but only 35,428 distinct on
# (orbit, latitude, longitude) -- the 94 genuine duplicates are all consecutive NRTI
# granules overlapping at their 5-minute boundaries. Do not reinstate the swath-index key;
# it is not a stable cross-granule pixel identifier. (scanline/ground_pixel are kept as
# columns below -- ground_pixel is still the correct detector index within a single
# granule and is needed by 04 for destriping.)
#
# OFFL is the reprocessed, higher-quality product and supersedes NRTI; ties within
# the same processing_mode are broken deterministically (qa_value descending, then
# weather_dist_km ascending, then latitude ascending) rather than an arbitrary
# dropDuplicates, so the pipeline stays reproducible.
dedup_window = Window.partitionBy("orbit", "latitude", "longitude").orderBy(
    when(col("processing_mode") == "OFFL", 0).otherwise(1).asc(),
    col("qa_value").desc(),
    col("weather_dist_km").asc(),
    col("latitude").asc(),
)

pre_dedup_count = silver_df.count()

silver_df = silver_df.withColumn(
    "_dedup_rank", row_number().over(dedup_window)
).filter(col("_dedup_rank") == 1).drop("_dedup_rank")

post_dedup_count = silver_df.count()
removed = pre_dedup_count - post_dedup_count
pct_removed = (100.0 * removed / pre_dedup_count) if pre_dedup_count else 0.0
print(f"Dedup on (orbit, latitude, longitude): {pre_dedup_count:,} -> {post_dedup_count:,} rows "
      f"({removed:,} removed, {pct_removed:.2f}%)")

TABLE_NAME = "silver_plume_ready_pixels"

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
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
