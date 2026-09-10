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

# # 02 — Ingest TROPOMI CH₄ from Planetary Computer
# 
# Ingests Sentinel-5P Level-2 methane data via Microsoft Planetary Computer STAC API.  
# Filters by QA value and BBOX, writes incrementally to `bronze_ch4_pixels`.  
# 
# **Source:** Planetary Computer → `sentinel-5p-l2-netcdf` (NRTI + OFFL)  
# **Output:** `bronze_ch4_pixels` (Delta table, append mode)  
# **Config:** All parameters from `00_config`

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
import fsspec
import xarray as xr
import pandas as pd
import numpy as np
from planetary_computer import sign
from collections import Counter
from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType, TimestampType, IntegerType
)
from pyspark.sql.utils import AnalysisException

# ── STAC client ──
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)
print("STAC client connected")

# ── Ingestion parameters ──
GAS_SPECIES      = "ch4"
TABLE_NAME        = "bronze_ch4_pixels"
NETCDF_VAR        = "methane_mixing_ratio_bias_corrected"  # bias-corrected XCH4
STAC_COLLECTION   = "sentinel-5p-l2-netcdf"
PROCESSING_MODES  = ["OFFL", "NRTI"]
QA_THRESHOLD      = CONFIG["qa_threshold"]  # 0.5

print(f"Gas: {GAS_SPECIES} | Table: {TABLE_NAME} | QA >= {QA_THRESHOLD}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check already-ingested STAC IDs (for incremental load):

# CELL ********************

# Skip items we've already ingested — much faster than downloading and deduping
existing_stac_ids = set()

try:
    existing_df = spark.table(TABLE_NAME)
    existing_stac_ids = set(
        row.stac_id for row in
        existing_df.select("stac_id").distinct().collect()
    )
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

# ### Search STAC catalog for CH₄ items:

# CELL ********************

# Build search geometry from config BBOX
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

start_date = CONFIG["start_date"]
end_date = CONFIG["end_date"]

search = catalog.search(
    collections=STAC_COLLECTION,
    intersects=search_geometry,
    datetime=f"{start_date}/{end_date}",
    query={
        "s5p:processing_mode": {"in": PROCESSING_MODES},
        "s5p:product_name": {"eq": GAS_SPECIES},
    },
)

all_items = list(search.items())
print(f"STAC search returned {len(all_items)} items")

# Filter out already-ingested items
new_items = [item for item in all_items if item.id not in existing_stac_ids]
skipped = len(all_items) - len(new_items)
print(f"New items to process: {len(new_items)} (skipping {skipped} already ingested)")

# Show date distribution
if new_items:
    dates = [item.datetime.date() for item in new_items]
    print(f"Date range: {min(dates)} to {max(dates)}")
    print(f"Processing modes: {Counter(item.properties.get('s5p:processing_mode', '?') for item in new_items)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Extract CH₄ pixels from NetCDF assets:

# CELL ********************

all_dfs = []
failed_items = []

for i, item in enumerate(new_items):
    item_id = item.id
    print(f"[{i+1}/{len(new_items)}] {item_id}", end=" ")

    # ── Processing mode + orbit, needed to key NRTI/OFFL dedup in 03_join_data ──
    # s5p:processing_mode is already used as a STAC query filter above, so it is
    # guaranteed present on every returned item. Orbit is read from sat:absolute_orbit
    # (confirmed present on all returned CH4 items), NOT parsed from item.id: Planetary
    # Computer item IDs are truncated forms of the full ESA product name (e.g.
    # "S5P_L2_CH4____20260612T223751_20260613T001921_44897" -- no collection /
    # processor-version / production-time suffix), so filename parsing is not a reliable
    # source of the orbit number here. sat:absolute_orbit is authoritative; its value is
    # not assumed to have any fixed digit width, since S5P absolute orbit numbers will
    # exceed 99999 in the future. If either property is unavailable, the item is skipped
    # rather than guessed at.
    processing_mode = item.properties.get("s5p:processing_mode")
    if not processing_mode:
        print("→ SKIP: no s5p:processing_mode on item properties")
        failed_items.append((item_id, "missing_processing_mode"))
        continue

    orbit = item.properties.get("sat:absolute_orbit")
    if orbit is None:
        print("→ SKIP: no sat:absolute_orbit on item properties")
        failed_items.append((item_id, "missing_orbit"))
        continue
    orbit = int(orbit)

    # ── Get signed asset URL ──
    try:
        href = sign(item.assets[GAS_SPECIES].href)
    except KeyError:
        print(f"→ SKIP: no '{GAS_SPECIES}' asset")
        failed_items.append((item_id, "no_asset"))
        continue

    # ── Load NetCDF and extract variables ──
    try:
        with fsspec.open(href).open() as f:
            ds = xr.open_dataset(f, group="PRODUCT", engine="h5netcdf")
            ds = ds[[NETCDF_VAR, "qa_value", "latitude", "longitude"]]

            # ── Swath index arrays, needed for destriping ──
            # scanline = along-track index, ground_pixel = across-track detector index.
            # Build 2D index grids over (scanline, ground_pixel) with np.indices, attach
            # as dataset variables, and let xarray's own to_dataframe() broadcast/flatten
            # them so each row carries the indices of the cell it came from.
            n_ground_pixels_item = ds.sizes["ground_pixel"]
            scanline_grid, ground_pixel_grid = np.indices(
                (ds.sizes["scanline"], n_ground_pixels_item)
            )
            ds = ds.assign(
                scanline_idx=(("scanline", "ground_pixel"), scanline_grid),
                ground_pixel_idx=(("scanline", "ground_pixel"), ground_pixel_grid),
            )

            df = ds.to_dataframe().reset_index()
            df = df.rename(columns={
                "scanline_idx": "scanline",
                "ground_pixel_idx": "ground_pixel",
            })
            df["n_ground_pixels"] = n_ground_pixels_item
    except Exception as e:
        print(f"→ SKIP: {e}")
        failed_items.append((item_id, str(e)))
        continue

    # ── Rename measurement column ──
    df = df.rename(columns={NETCDF_VAR: "ch4"})

    # ── Drop rows missing core fields ──
    df = df.dropna(subset=["latitude", "longitude", "ch4", "qa_value"])

    # ── QA filter ──
    df = df[df["qa_value"] >= QA_THRESHOLD]

    # ── BBOX filter (satellite swaths extend beyond search area) ──
    df = df[
        (df["latitude"] >= BBOX["min_lat"]) &
        (df["latitude"] <= BBOX["max_lat"]) &
        (df["longitude"] >= BBOX["min_lon"]) &
        (df["longitude"] <= BBOX["max_lon"])
    ]

    if df.empty:
        print("→ 0 pixels after filtering")
        continue

    # ── Attach metadata ──
    df["datetime"] = pd.to_datetime(item.datetime, utc=True)
    df["stac_id"] = item_id
    df["gas"] = GAS_SPECIES.upper()
    df["processing_mode"] = processing_mode
    df["orbit"] = orbit

    # ── Keep only the columns we need ──
    df = df[[
        "latitude", "longitude", "ch4", "qa_value", "datetime", "stac_id", "gas",
        "scanline", "ground_pixel", "n_ground_pixels", "processing_mode", "orbit",
    ]]

    all_dfs.append(df)
    print(f"→ {len(df):,} pixels")

print(f"\n{'='*50}")
print(f"Processed: {len(all_dfs)} items with data")
print(f"Failed: {len(failed_items)} items")
if failed_items:
    for fid, reason in failed_items[:5]:
        print(f"  {fid}: {reason}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Combine and write to Bronze:

# CELL ********************

if not all_dfs:
    print("No new data to write — all items were already ingested or had no valid pixels.")
else:
    # ── Combine ──
    combined_pdf = pd.concat(all_dfs, ignore_index=True)
    # This only removes duplicate rows sharing the same stac_id (e.g. re-processing the
    # same item within one run). It does NOT catch NRTI/OFFL overlap of the same physical
    # pixel, since those land under two different stac_ids -- that is handled by the
    # (orbit, scanline, ground_pixel) dedup in 03_join_data.Notebook.
    combined_pdf = combined_pdf.drop_duplicates(
        subset=["latitude", "longitude", "datetime", "stac_id"]
    )
    print(f"Combined: {len(combined_pdf):,} rows after dedup")
    print(f"CH4 range: {combined_pdf['ch4'].min():.2f} – {combined_pdf['ch4'].max():.2f} ppb")
    print(f"STAC IDs: {combined_pdf['stac_id'].nunique()}")

    # ── Clean types ──
    combined_pdf = combined_pdf.replace([np.inf, -np.inf], None)
    combined_pdf["latitude"] = pd.to_numeric(combined_pdf["latitude"], errors="coerce")
    combined_pdf["longitude"] = pd.to_numeric(combined_pdf["longitude"], errors="coerce")
    combined_pdf["ch4"] = pd.to_numeric(combined_pdf["ch4"], errors="coerce")
    combined_pdf["qa_value"] = pd.to_numeric(combined_pdf["qa_value"], errors="coerce")
    combined_pdf["datetime"] = pd.to_datetime(combined_pdf["datetime"], errors="coerce")
    combined_pdf["scanline"] = pd.to_numeric(combined_pdf["scanline"], errors="coerce")
    combined_pdf["ground_pixel"] = pd.to_numeric(combined_pdf["ground_pixel"], errors="coerce")
    combined_pdf["n_ground_pixels"] = pd.to_numeric(combined_pdf["n_ground_pixels"], errors="coerce")
    combined_pdf["orbit"] = pd.to_numeric(combined_pdf["orbit"], errors="coerce")
    combined_pdf = combined_pdf.dropna(subset=[
        "latitude", "longitude", "ch4", "datetime", "scanline", "ground_pixel",
        "n_ground_pixels", "processing_mode", "orbit",
    ])
    combined_pdf["scanline"] = combined_pdf["scanline"].astype(int)
    combined_pdf["ground_pixel"] = combined_pdf["ground_pixel"].astype(int)
    combined_pdf["n_ground_pixels"] = combined_pdf["n_ground_pixels"].astype(int)
    combined_pdf["orbit"] = combined_pdf["orbit"].astype(int)

    # ── Schema ──
    schema = StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("ch4", DoubleType(), True),
        StructField("qa_value", DoubleType(), True),
        StructField("datetime", TimestampType(), True),
        StructField("stac_id", StringType(), True),
        StructField("gas", StringType(), True),
        StructField("scanline", IntegerType(), True),
        StructField("ground_pixel", IntegerType(), True),
        StructField("n_ground_pixels", IntegerType(), True),
        StructField("processing_mode", StringType(), True),
        StructField("orbit", IntegerType(), True),
    ])

    # ── Convert to Spark ──
    spark_df = spark.createDataFrame(combined_pdf, schema=schema)
    spark_df = spark_df.coalesce(2)

    # ── Write (create or append) ──
    table_exists = len(existing_stac_ids) > 0
    if table_exists:
        spark_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(TABLE_NAME)
    else:
        spark_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(TABLE_NAME)

    new_total = spark.table(TABLE_NAME).count()
    print(f"\nWritten to '{TABLE_NAME}' — total rows now: {new_total:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validate Bronze output:

# CELL ********************

from pyspark.sql.functions import min as spark_min, max as spark_max, avg, count as spark_count

df = spark.table(TABLE_NAME)

print(f"=== {TABLE_NAME} Summary ===")
df.select(
    spark_count("*").alias("total_rows"),
    spark_min("datetime").alias("earliest"),
    spark_max("datetime").alias("latest"),
    spark_min("ch4").alias("min_ch4"),
    spark_max("ch4").alias("max_ch4"),
    avg("ch4").alias("mean_ch4"),
    avg("qa_value").alias("mean_qa"),
).show(truncate=False)

print(f"Distinct STAC IDs: {df.select('stac_id').distinct().count()}")
print(f"Distinct dates: {df.select(df.datetime.cast('date')).distinct().count()}")

# ── Spatial bounds check ──
print(f"\n=== Spatial Bounds ===")
df.select(
    spark_min("latitude").alias("min_lat"),
    spark_max("latitude").alias("max_lat"),
    spark_min("longitude").alias("min_lon"),
    spark_max("longitude").alias("max_lon"),
).show(truncate=False)

print(f"Config BBOX: lat [{BBOX['min_lat']}, {BBOX['max_lat']}], "
      f"lon [{BBOX['min_lon']}, {BBOX['max_lon']}]")

# ── Pixel count per STAC ID (spot-check for outliers) ──
print(f"\n=== Pixels per STAC ID (top 10) ===")
df.groupBy("stac_id").agg(
    spark_count("*").alias("pixel_count")
).orderBy("pixel_count", ascending=False).show(10, truncate=50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
