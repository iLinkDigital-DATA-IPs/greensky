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

# # 04b — Multi-Gas Cross-Correlation
# 
# For each CH₄ plume in `gold_plumes`, checks for co-located NO₂ anomalies  
# and classifies the emission signature.  
# 
# **Inputs:**  
# - `gold_plumes` — CH₄ plume detections with source location, emission rate, IME  
# - `bronze_no2_pixels` — NO₂ tropospheric column density observations  
# 
# **Output:**  
# - `gold_multi_gas_signatures` — plume-level records enriched with NO₂ context and emission signature  
# 
# **Signature classifications:**  
# - **Fugitive Leak** — CH₄ elevated, NO₂ normal (venting, open valve, tank leak)  
# - **Incomplete Combustion** — CH₄ elevated, NO₂ elevated (malfunctioning flare or engine)  
# - **Undetermined** — insufficient NO₂ coverage to classify  
# 
# **Future extensions:** CO co-detection, FIRMS flare cross-reference

# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Configuration and imports:

# CELL ********************

from pyspark.sql.functions import (
    col, lit, abs as spark_abs, avg, min as spark_min, max as spark_max,
    count as spark_count, stddev, expr, when, coalesce, round as spark_round,
    percentile_approx, unix_timestamp
)
from pyspark.sql.types import StringType, DoubleType

# ── Cross-correlation parameters ──
SPATIAL_RADIUS_DEG  = 0.15   # ~15 km at Permian Basin latitude
TEMPORAL_WINDOW_S   = 3600   # ±1 hour (TROPOMI overpasses are near-simultaneous for CH4/NO2)
NO2_BACKGROUND_RADIUS_DEG = 0.5  # broader area for NO2 background estimation
NO2_ANOMALY_SIGMA   = 2.0    # NO2 is anomalous if > background_mean + 2*std

OUTPUT_TABLE = "gold_multi_gas_signatures"

print(f"Spatial match radius: {SPATIAL_RADIUS_DEG}° (~{SPATIAL_RADIUS_DEG * 111:.0f} km)")
print(f"Temporal window: ±{TEMPORAL_WINDOW_S}s ({TEMPORAL_WINDOW_S/3600:.0f} hr)")
print(f"NO2 background radius: {NO2_BACKGROUND_RADIUS_DEG}°")
print(f"NO2 anomaly threshold: mean + {NO2_ANOMALY_SIGMA}σ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load inputs and inspect:

# CELL ********************

# ── Load CH4 plumes ──
plumes = spark.table("gold_plume_catalog")
plume_count = plumes.count()
print(f"CH4 plumes: {plume_count}")
print(f"Plume columns: {plumes.columns}")
plumes.show(3, truncate=False)

# ── Load NO2 pixels ──
no2_available = False
try:
    no2_pixels = spark.table("bronze_no2_pixels")
    no2_count = no2_pixels.count()
    no2_available = no2_count > 0
    print(f"\nNO2 pixels: {no2_count:,}")
    no2_pixels.select(
        spark_min("datetime").alias("earliest"),
        spark_max("datetime").alias("latest"),
        avg("no2").alias("mean_no2"),
    ).show(truncate=False)
except Exception as e:
    print(f"\nNO2 table not available: {e}")
    print("Will produce signatures with NO2 = undetermined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Identify plume source columns:
# 
# Adapts to the actual `gold_plumes` schema — handles different column naming conventions.

# CELL ********************

# ── Resolve column names (gold_plumes may use different naming) ──
plume_cols = [c.lower() for c in plumes.columns]

# Source location
if "source_lat" in plume_cols:
    LAT_COL, LON_COL = "source_lat", "source_lon"
elif "src_lat" in plume_cols:
    LAT_COL, LON_COL = "src_lat", "src_lon"
elif "latitude" in plume_cols:
    LAT_COL, LON_COL = "latitude", "longitude"
else:
    raise ValueError(f"Cannot find lat/lon columns in gold_plumes: {plumes.columns}")

# Timestamp
if "detect_ts" in plume_cols:
    TS_COL = "detect_ts"
elif "datetime" in plume_cols:
    TS_COL = "datetime"
elif "scene_datetime" in plume_cols:
    TS_COL = "scene_datetime"
else:
    # Fall back to scene_id parsing or first timestamp column
    ts_candidates = [c for c in plumes.columns if "time" in c.lower() or "date" in c.lower() or "ts" in c.lower()]
    if ts_candidates:
        TS_COL = ts_candidates[0]
    else:
        raise ValueError(f"Cannot find timestamp column in gold_plumes: {plumes.columns}")

# Emission rate
if "emission_rate_kg_s" in plume_cols:
    RATE_COL = "emission_rate_kg_s"
elif "emission_rate" in plume_cols:
    RATE_COL = "emission_rate"
else:
    RATE_COL = None

# Plume ID
if "plume_id" in plume_cols:
    ID_COL = "plume_id"
elif "cluster_id" in plume_cols:
    ID_COL = "cluster_id"
else:
    # Generate one from row number
    from pyspark.sql.functions import monotonically_increasing_id
    plumes = plumes.withColumn("plume_id", monotonically_increasing_id())
    ID_COL = "plume_id"

print(f"Resolved columns → ID: {ID_COL}, Lat: {LAT_COL}, Lon: {LON_COL}, Time: {TS_COL}, Rate: {RATE_COL}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Compute NO₂ background statistics:
# 
# For each plume location, estimate the local NO₂ background from a broader spatial window.  
# A plume-location NO₂ reading is only "elevated" if it exceeds this local background + 2σ.

# CELL ********************

if no2_available:
    # ── Join plumes with nearby NO2 pixels for background estimation ──
    # Broader window: all NO2 within 0.5° of each plume location (any time)
    # This gives us the typical NO2 level for that area

    no2_bg = (
        plumes.alias("p")
        .crossJoin(no2_pixels.alias("n"))
        .filter(
            (spark_abs(col("n.latitude") - col(f"p.{LAT_COL}")) <= NO2_BACKGROUND_RADIUS_DEG) &
            (spark_abs(col("n.longitude") - col(f"p.{LON_COL}")) <= NO2_BACKGROUND_RADIUS_DEG)
        )
        .groupBy(col(f"p.{ID_COL}").alias("plume_id"))
        .agg(
            avg(col("n.no2")).alias("no2_bg_mean"),
            stddev(col("n.no2")).alias("no2_bg_std"),
            spark_count(col("n.no2")).alias("no2_bg_pixel_count"),
        )
    )

    print("NO2 background statistics per plume location:")
    no2_bg.show(5, truncate=False)
    print(f"Plumes with NO2 background data: {no2_bg.count()} / {plume_count}")
else:
    no2_bg = None
    print("Skipping NO2 background — no data available")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Co-locate NO₂ with each CH₄ plume:
# 
# For each plume, find NO₂ pixels within ~15 km and ±1 hour.  
# Compare the co-located NO₂ value against the local background to determine if NO₂ is elevated.

# CELL ********************

if no2_available:
    # ── Spatial + temporal co-location ──
    plumes_ts = plumes.withColumn("plume_epoch", unix_timestamp(col(TS_COL)))
    no2_ts = no2_pixels.withColumn("no2_epoch", unix_timestamp(col("datetime")))

    co_located = (
        plumes_ts.alias("p")
        .crossJoin(no2_ts.alias("n"))
        .filter(
            # Spatial: within ~15 km
            (spark_abs(col("n.latitude") - col(f"p.{LAT_COL}")) <= SPATIAL_RADIUS_DEG) &
            (spark_abs(col("n.longitude") - col(f"p.{LON_COL}")) <= SPATIAL_RADIUS_DEG) &
            # Temporal: within ±1 hour
            (spark_abs(col("n.no2_epoch") - col("p.plume_epoch")) <= TEMPORAL_WINDOW_S)
        )
        .groupBy(col(f"p.{ID_COL}").alias("plume_id"))
        .agg(
            avg(col("n.no2")).alias("no2_co_located_mean"),
            spark_max(col("n.no2")).alias("no2_co_located_max"),
            spark_count(col("n.no2")).alias("no2_co_located_count"),
        )
    )

    print("NO2 co-located values per plume:")
    co_located.show(5, truncate=False)
    print(f"Plumes with co-located NO2: {co_located.count()} / {plume_count}")
else:
    co_located = None
    print("Skipping NO2 co-location — no data available")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Classify emission signatures:
# 
# | Signature | CH₄ | NO₂ | Interpretation |
# |---|---|---|---|
# | Fugitive Leak | ↑ | Normal | Venting, open valve, tank leak |
# | Incomplete Combustion | ↑ | ↑ | Malfunctioning flare or engine |
# | Undetermined | ↑ | No data | Insufficient NO₂ coverage |

# CELL ********************

# ── Start with all plumes ──
result = plumes.select(
    col(ID_COL).alias("plume_id"),
    col(LAT_COL).alias("source_lat"),
    col(LON_COL).alias("source_lon"),
    col(TS_COL).alias("detect_ts"),
    *[col(c) for c in plumes.columns if c in [
        "scene_id", "pixel_count", "plume_area_km2",
        "peak_enhancement_ppb", "mean_enhancement_ppb",
        "ime_kg", "emission_rate_kg_s",
        "wind_alignment_deg", "wind_confidence",
    ]]
)

if no2_available and co_located is not None and no2_bg is not None:
    # ── Join NO2 background + co-located values ──
    result = (
        result
        .join(no2_bg, on="plume_id", how="left")
        .join(co_located, on="plume_id", how="left")
    )

    # ── Compute anomaly threshold ──
    result = result.withColumn(
        "no2_anomaly_threshold",
        col("no2_bg_mean") + (lit(NO2_ANOMALY_SIGMA) * coalesce(col("no2_bg_std"), lit(0)))
    )

    # ── Flag NO2 as elevated or normal ──
    result = result.withColumn(
        "no2_elevated",
        when(
            col("no2_co_located_count").isNull() | (col("no2_co_located_count") == 0),
            lit(None).cast("boolean")
        ).when(
            col("no2_co_located_mean") > col("no2_anomaly_threshold"),
            lit(True)
        ).otherwise(
            lit(False)
        )
    )

    # ── Classify emission signature ──
    result = result.withColumn(
        "emission_signature",
        when(
            col("no2_elevated").isNull(),
            lit("Undetermined")
        ).when(
            col("no2_elevated") == True,
            lit("Incomplete Combustion")
        ).otherwise(
            lit("Fugitive Leak")
        )
    )
else:
    # ── No NO2 data: all undetermined ──
    result = (
        result
        .withColumn("no2_bg_mean", lit(None).cast(DoubleType()))
        .withColumn("no2_bg_std", lit(None).cast(DoubleType()))
        .withColumn("no2_bg_pixel_count", lit(None).cast("long"))
        .withColumn("no2_co_located_mean", lit(None).cast(DoubleType()))
        .withColumn("no2_co_located_max", lit(None).cast(DoubleType()))
        .withColumn("no2_co_located_count", lit(None).cast("long"))
        .withColumn("no2_anomaly_threshold", lit(None).cast(DoubleType()))
        .withColumn("no2_elevated", lit(None).cast("boolean"))
        .withColumn("emission_signature", lit("Undetermined"))
    )

print(f"Signature distribution:")
result.groupBy("emission_signature").agg(
    spark_count("*").alias("plume_count")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write to Gold:

# CELL ********************

result.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(OUTPUT_TABLE)

count = spark.table(OUTPUT_TABLE).count()
print(f"Written {count:,} rows to {OUTPUT_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validate and inspect:

# CELL ********************

final = spark.table(OUTPUT_TABLE)

print(f"=== {OUTPUT_TABLE} Summary ===")
print(f"Total plumes: {final.count()}")
print(f"Columns: {final.columns}\n")

# ── Signature breakdown ──
print("=== Emission Signatures ===")
final.groupBy("emission_signature").agg(
    spark_count("*").alias("count"),
    spark_round(avg("emission_rate_kg_s"), 4).alias("avg_emission_rate"),
).show(truncate=False)

# ── NO2 context (if available) ──
if no2_available:
    print("=== NO2 Context ===")
    final.select(
        spark_count(when(col("no2_elevated") == True, 1)).alias("no2_elevated"),
        spark_count(when(col("no2_elevated") == False, 1)).alias("no2_normal"),
        spark_count(when(col("no2_elevated").isNull(), 1)).alias("no2_no_data"),
        spark_round(avg("no2_co_located_mean"), 6).alias("avg_co_located_no2"),
        spark_round(avg("no2_bg_mean"), 6).alias("avg_background_no2"),
    ).show(truncate=False)

# ── Sample rows ──
print("=== Sample Enriched Plumes ===")
final.select(
    "plume_id", "source_lat", "source_lon", "detect_ts",
    "emission_signature", "no2_elevated",
    spark_round("no2_co_located_mean", 6).alias("no2_value"),
    spark_round("no2_anomaly_threshold", 6).alias("no2_threshold"),
).show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Debug

# CELL ********************

import planetary_computer
import pystac_client
from datetime import datetime, timedelta

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

search_geometry = {
    "type": "Polygon",
    "coordinates": [[
        [-105.0, 30.5], [-105.0, 33.5],
        [-101.0, 33.5], [-101.0, 30.5],
        [-105.0, 30.5],
    ]]
}

recent_search = catalog.search(
    collections="sentinel-5p-l2-netcdf",
    intersects=search_geometry,
    datetime=f"2026-08-01/{datetime.utcnow().strftime('%Y-%m-%d')}",
    query={
        "s5p:processing_mode": {"in": ["OFFL"]},
        "s5p:product_name": {"eq": "ch4"},
    },
)

items = list(recent_search.items())
print(f"CH4 items available for Aug-Sep 2026: {len(items)}")
if items:
    dates = sorted(set(i.datetime.date() for i in items))
    print(f"Date range: {dates[0]} to {dates[-1]}")
    print(f"Most recent: {dates[-1]} ({(datetime.utcnow().date() - dates[-1]).days} days ago)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
