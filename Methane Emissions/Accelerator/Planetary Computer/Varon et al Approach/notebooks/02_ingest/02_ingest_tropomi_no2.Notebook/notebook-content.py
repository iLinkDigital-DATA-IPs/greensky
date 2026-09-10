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

# # 02 — Ingest TROPOMI NO₂ from Planetary Computer
# 
# Ingests Sentinel-5P Level-2 nitrogen dioxide data via Microsoft Planetary Computer STAC API.  
# Filters by QA value and BBOX, writes incrementally to `bronze_no2_pixels`.  
# 
# **Source:** Planetary Computer → `sentinel-5p-l2-netcdf` (NRTI + OFFL)  
# **Output:** `bronze_no2_pixels` (Delta table, append mode)  
# **Config:** All parameters from `00_config`  
# 
# **Note:** NO₂ has a stricter recommended QA threshold (0.75) than CH₄ (0.5).  
# The measurement unit is tropospheric column density (mol/m²), not mixing ratio (ppb).

# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Imports and STAC client setup:

# CELL ********************

import planetary_computer
import pystac_client
import requests
import io
import time
import xarray as xr
import pandas as pd
import numpy as np
from planetary_computer import sign
from collections import Counter
from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType, TimestampType
)
from pyspark.sql.utils import AnalysisException

# ── STAC client ──
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)
print("STAC client connected")

# ── Ingestion parameters (NO2-specific) ──
GAS_SPECIES       = "no2"
TABLE_NAME         = "bronze_no2_pixels"
NETCDF_VAR         = "nitrogendioxide_tropospheric_column"
MEASUREMENT_COL    = "no2"
STAC_COLLECTION    = "sentinel-5p-l2-netcdf"
PROCESSING_MODES   = ["OFFL"]
QA_THRESHOLD       = 0.75

# ── Download settings ──
CONNECT_TIMEOUT    = 15   # seconds to establish connection
READ_TIMEOUT       = 120  # seconds max for full file download
BATCH_SIZE         = 5    # save to Delta every N successful items

# ── Output schema ──
SCHEMA = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField(MEASUREMENT_COL, DoubleType(), True),
    StructField("qa_value", DoubleType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("stac_id", StringType(), True),
    StructField("gas", StringType(), True),
])

print(f"Gas: {GAS_SPECIES} | Table: {TABLE_NAME} | QA >= {QA_THRESHOLD}")
print(f"Timeouts: connect={CONNECT_TIMEOUT}s, read={READ_TIMEOUT}s")
print(f"Batch save every {BATCH_SIZE} items")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check already-ingested STAC IDs (for incremental load):

# CELL ********************

existing_stac_ids = set()
table_exists = False

try:
    existing_df = spark.table(TABLE_NAME)
    existing_stac_ids = set(
        row.stac_id for row in
        existing_df.select("stac_id").distinct().collect()
    )
    table_exists = True
    print(f"Table '{TABLE_NAME}' exists with {existing_df.count():,} rows, "
          f"{len(existing_stac_ids)} distinct STAC IDs")
except AnalysisException:
    print(f"Table '{TABLE_NAME}' not found — will create on first write")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Search STAC catalog for NO₂ items:

# CELL ********************

search_geometry = {
    "type": "Polygon",
    "coordinates": [[
        [BBOX["min_lon"], BBOX["min_lat"]],
        [BBOX["min_lon"], BBOX["max_lat"]],
        [BBOX["max_lon"], BBOX["max_lat"]],
        [BBOX["max_lon"], BBOX["min_lat"]],
        [BBOX["min_lon"], BBOX["min_lat"]],
    ]]
}

search = catalog.search(
    collections=STAC_COLLECTION,
    intersects=search_geometry,
    datetime=f"{CONFIG['start_date']}/{CONFIG['end_date']}",
    query={
        "s5p:processing_mode": {"in": PROCESSING_MODES},
        "s5p:product_name": {"eq": GAS_SPECIES},
    },
)

all_items = list(search.items())
print(f"STAC search returned {len(all_items)} items")

# Filter out already-ingested
new_items = [item for item in all_items if item.id not in existing_stac_ids]
skipped = len(all_items) - len(new_items)
print(f"New items to process: {len(new_items)} (skipping {skipped} already ingested)")

if new_items:
    dates = [item.datetime.date() for item in new_items]
    print(f"Date range: {min(dates)} to {max(dates)}")
    modes = Counter(item.properties.get('s5p:processing_mode', '?') for item in new_items)
    print(f"Processing modes: {modes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Download, extract, and save incrementally:
# 
# Uses `requests` with hard socket-level timeouts instead of `fsspec`.  
# Saves to Delta every 5 successful items — safe to cancel at any point.

# CELL ********************

batch_dfs = []
total_saved = 0
total_pixels = 0
failed_items = []
t0 = time.time()

print(f"Processing {len(new_items)} items...\n")

for i, item in enumerate(new_items):
    item_id = item.id
    item_t = time.time()

    # ── Sign URL ──
    try:
        href = sign(item.assets[GAS_SPECIES].href)
    except KeyError:
        print(f"[{i+1}/{len(new_items)}] {item_id} → SKIP: no asset")
        failed_items.append((item_id, "no_asset"))
        continue

    # ── Download with hard timeout ──
    try:
        resp = requests.get(href, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        dt = time.time() - item_t
        print(f"[{i+1}/{len(new_items)}] {item_id} → TIMEOUT after {dt:.0f}s")
        failed_items.append((item_id, "timeout"))
        continue
    except requests.exceptions.RequestException as e:
        dt = time.time() - item_t
        print(f"[{i+1}/{len(new_items)}] {item_id} → FAIL ({dt:.0f}s): {str(e)[:80]}")
        failed_items.append((item_id, str(e)[:80]))
        continue

    # ── Parse NetCDF from memory ──
    try:
        ds = xr.open_dataset(
            io.BytesIO(resp.content), group="PRODUCT", engine="h5netcdf"
        )
        lat = ds["latitude"].values.ravel()
        lon = ds["longitude"].values.ravel()
        qa  = ds["qa_value"].values.ravel()
        val = ds[NETCDF_VAR].values.ravel()
        ds.close()
        del resp  # free memory

        mask = (
            (lat >= BBOX["min_lat"]) & (lat <= BBOX["max_lat"]) &
            (lon >= BBOX["min_lon"]) & (lon <= BBOX["max_lon"]) &
            (qa >= QA_THRESHOLD) &
            np.isfinite(val) & np.isfinite(lat) & np.isfinite(lon)
        )

        n_pixels = int(mask.sum())
        if n_pixels == 0:
            dt = time.time() - item_t
            print(f"[{i+1}/{len(new_items)}] {item_id} → 0 pixels ({dt:.0f}s)")
            continue

        df = pd.DataFrame({
            "latitude": lat[mask], "longitude": lon[mask],
            MEASUREMENT_COL: val[mask], "qa_value": qa[mask],
        })
        df["datetime"] = pd.to_datetime(item.datetime, utc=True)
        df["stac_id"] = item_id
        df["gas"] = GAS_SPECIES.upper()
        df = df[["latitude", "longitude", MEASUREMENT_COL, "qa_value",
                  "datetime", "stac_id", "gas"]]

        batch_dfs.append(df)
        total_pixels += n_pixels
        dt = time.time() - item_t
        print(f"[{i+1}/{len(new_items)}] {item_id} → {n_pixels:,} pixels ({dt:.0f}s)")

    except Exception as e:
        dt = time.time() - item_t
        print(f"[{i+1}/{len(new_items)}] {item_id} → PARSE FAIL ({dt:.0f}s): {str(e)[:80]}")
        failed_items.append((item_id, str(e)[:80]))
        continue

    # ── Batch save to Delta ──
    if len(batch_dfs) >= BATCH_SIZE:
        combined = pd.concat(batch_dfs, ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["latitude", "longitude", "datetime", "stac_id"]
        )
        spark_df = spark.createDataFrame(combined, schema=SCHEMA).coalesce(1)
        mode = "append" if table_exists else "overwrite"
        spark_df.write.format("delta").mode(mode).saveAsTable(TABLE_NAME)
        table_exists = True
        total_saved += len(combined)
        batch_dfs = []
        elapsed = time.time() - t0
        print(f"   >> Saved batch — {total_saved:,} total rows ({elapsed:.0f}s elapsed)\n")

# ── Final batch ──
if batch_dfs:
    combined = pd.concat(batch_dfs, ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["latitude", "longitude", "datetime", "stac_id"]
    )
    spark_df = spark.createDataFrame(combined, schema=SCHEMA).coalesce(1)
    mode = "append" if table_exists else "overwrite"
    spark_df.write.format("delta").mode(mode).saveAsTable(TABLE_NAME)
    table_exists = True
    total_saved += len(combined)

elapsed = time.time() - t0
print(f"\n{'='*50}")
print(f"Done in {elapsed:.0f}s ({elapsed/60:.1f} min)")
print(f"Saved: {total_saved:,} rows | Failed: {len(failed_items)} items")
if failed_items:
    print(f"\nFailed items:")
    for fid, reason in failed_items:
        print(f"  {fid}: {reason}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validate Bronze output:

# CELL ********************

from pyspark.sql.functions import min as spark_min, max as spark_max, avg, count as spark_count

try:
    df = spark.table(TABLE_NAME)

    print(f"=== {TABLE_NAME} Summary ===")
    df.select(
        spark_count("*").alias("total_rows"),
        spark_min("datetime").alias("earliest"),
        spark_max("datetime").alias("latest"),
        spark_min(MEASUREMENT_COL).alias("min_no2"),
        spark_max(MEASUREMENT_COL).alias("max_no2"),
        avg(MEASUREMENT_COL).alias("mean_no2"),
        avg("qa_value").alias("mean_qa"),
    ).show(truncate=False)

    print(f"Distinct STAC IDs: {df.select('stac_id').distinct().count()}")
    print(f"Distinct dates: {df.select(df.datetime.cast('date')).distinct().count()}")

    print(f"\n=== Spatial Bounds ===")
    df.select(
        spark_min("latitude").alias("min_lat"),
        spark_max("latitude").alias("max_lat"),
        spark_min("longitude").alias("min_lon"),
        spark_max("longitude").alias("max_lon"),
    ).show(truncate=False)

    print(f"Config BBOX: lat [{BBOX['min_lat']}, {BBOX['max_lat']}], "
          f"lon [{BBOX['min_lon']}, {BBOX['max_lon']}]")

    print(f"\n=== Pixels per STAC ID (top 10) ===")
    df.groupBy("stac_id").agg(
        spark_count("*").alias("pixel_count")
    ).orderBy("pixel_count", ascending=False).show(10, truncate=50)

except AnalysisException:
    print(f"Table '{TABLE_NAME}' does not exist — no data was saved.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
