# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f2c3a17e-d2bb-4686-a19c-42d729908353",
# META       "default_lakehouse_name": "Planetary_computer_LH",
# META       "default_lakehouse_workspace_id": "640876ea-6158-4ffd-8598-5eb210e088a0",
# META       "known_lakehouses": [
# META         {
# META           "id": "f2c3a17e-d2bb-4686-a19c-42d729908353"
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

# # Planetary Computer Ingestion Notebook
# 
# #### Extracts geospatial and atmospheric datasets from Microsoft Planetary Computer STAC APIs, processes relevant variables, and ingests structured data into the lakehouse for analytics and visualization
# 
# ##### Swath passing over Permian Basin, Texas, US 

# CELL ********************

# IMPORTS

## Access Catalog
import planetary_computer
import pystac_client

## Load Dataset
import fsspec
import xarray as xr
from datetime import datetime, timedelta
from planetary_computer import sign

## Data Manipulation
import pandas as pd
import numpy as np
from collections import Counter  # ← single import, moved here

## Data Visualization
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point

## Data Storage
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import broadcast, col
from pyspark.sql.types import *
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType
import io
import requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ACCESSING CATALOG
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
from collections import Counter

# SEARCHING CATALOG
longitude = -99.36    # Permian Basin, Texas, US
latitude  = 31.5

WINDOW_DAYS = 20  # rolling lookback

_default_end   = datetime.utcnow().strftime("%Y-%m-%d")
_default_start = (datetime.utcnow() - timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")

try:
    start_date = getArgument("start_date", _default_start)
    end_date   = getArgument("end_date",   _default_end)
except Exception:
    start_date = _default_start
    end_date   = _default_end

# Use the pystac_client catalog (opened in cell 2), NOT a raw POST.
# Two critical differences from the old request:
#   1. query filters to CH4 server-side, so the result set isn't crowded out
#      by NO2/CO/O3/SO2/HCHO/aerosol/cloud products that also live in
#      sentinel-5p-l2-netcdf.
#   2. item_collection() auto-paginates through ALL matches. The old
#      "limit": 50 was a hard cap with no paging, so once the most recent
#      day(s) filled the 50 slots, every older day silently fell off.
search = catalog.search(
    collections=["sentinel-5p-l2-netcdf"],
    intersects={"type": "Point", "coordinates": [longitude, latitude]},
    datetime=f"{start_date}/{end_date}",
    query={"s5p:product_name": {"eq": "ch4"}},
)

items = list(search.item_collection())

# Client-side guard in case the backend ignores the product query filter
items = [it for it in items if it.properties.get("s5p:product_name") == "ch4"]

dates = [it.properties["datetime"][:10] for it in items]
print("Window:", start_date, "→", end_date)
print("Available CH4 dates:", Counter(dates))
print(f"Total CH4 items found: {len(items)}")
if not items:
    print("No CH4 items found for the given search parameters.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
import requests

items_by_mode = {"OFFL": [], "NRTI": []}

for mode in ["OFFL", "NRTI"]:
    payload = {
        "collections": ["sentinel-5p-l2-netcdf"],
        "intersects": {"type": "Point", "coordinates": [longitude, latitude]},
        "datetime": f"{start_date}/{end_date}",
        "limit": 50,
    }

    for attempt in range(5):
        try:
            resp = requests.post(
                "https://planetarycomputer.microsoft.com/api/stac/v1/search",
                json=payload,
                timeout=60,
            )
            resp.raise_for_status()
            all_items = resp.json().get("features", [])

            # filter client-side instead of via query param
            mode_items = [
                i for i in all_items
                if i["properties"].get("s5p:processing_mode") == mode
                and i["properties"].get("s5p:product_name") == "ch4"
            ]
            items_by_mode[mode] = mode_items
            break
        except Exception as e:
            wait = 15 * (attempt + 1)
            print(f"{mode} attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    if items_by_mode[mode]:
        print(f"{mode} max date:", max(i["properties"]["datetime"] for i in items_by_mode[mode]))
        print(f"{mode} count:", len(items_by_mode[mode]))
    else:
        print(f"{mode}: no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Arrow off: avoids type-mapping issues between pandas Timestamps and Spark TimestampType
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled",          "false")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")

schema = StructType([
    StructField("latitude",         DoubleType(),    True),
    StructField("longitude",        DoubleType(),    True),
    StructField("ch4",              DoubleType(),    True),
    StructField("qa_value",         DoubleType(),    True),
    StructField("datetime",         TimestampType(), True),
    StructField("gas",              StringType(),    True),
    StructField("instrument",       StringType(),    True),
    StructField("platform",         StringType(),    True),
    StructField("collection",       StringType(),    True),
    StructField("stac_id",          StringType(),    True),
    StructField("provider",         StringType(),    True),
    StructField("provider_all",     StringType(),    True),
    StructField("provider_roles",   StringType(),    True),
    StructField("processing_level", StringType(),    True),
    StructField("mission_phase",    StringType(),    True),
])

S5P_PROVIDERS = [
    {"name": "European Space Agency", "roles": ["producer"]},
    {"name": "Microsoft",             "roles": ["host"]},
]
PROVIDER_ALL   = ", ".join(p["name"] for p in S5P_PROVIDERS)
PROVIDER_ROLES = ", ".join(",".join(p.get("roles", [])) for p in S5P_PROVIDERS)
PROVIDER_NAME  = S5P_PROVIDERS[0]["name"]

def get_mission_phase(dt):
    if dt is None:
        return "operational"
    try:
        dt = pd.to_datetime(dt, utc=True)
    except Exception:
        return "operational"
    if dt < pd.Timestamp("2018-04-30", tz="UTC"):
        return "commissioning"
    elif dt < pd.Timestamp("2019-01-01", tz="UTC"):
        return "early_operations"
    return "operational"

def fetch_and_parse(href, timeout=120):
    """
    Stream-download NetCDF via requests into a BytesIO buffer,
    parse with xarray. Streaming avoids loading the full file
    into memory at once (S5P files can be 300–600 MB).
    xarray's h5netcdf engine auto-masks _FillValue sentinels.
    """
    buf = io.BytesIO()
    with requests.get(href, timeout=timeout, stream=True) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):  # 8 MB chunks
            buf.write(chunk)
    buf.seek(0)

    with xr.open_dataset(buf, group="PRODUCT", engine="h5netcdf") as ds:
        ds = ds[["methane_mixing_ratio_bias_corrected", "qa_value",
                 "latitude", "longitude"]]
        df = ds.to_dataframe().reset_index()

    df = df.drop(columns=[c for c in ["scanline", "ground_pixel", "time"] if c in df.columns])
    return df

def flush_batch(pandas_dfs):
    """Combine a list of per-item DataFrames into a single Spark DataFrame."""
    combined = pd.concat(pandas_dfs, ignore_index=True)
    combined = combined.drop_duplicates(["latitude", "longitude", "datetime", "stac_id"])
    return spark.createDataFrame(combined, schema=schema)

BATCH_SIZE   = 50
HTTP_TIMEOUT = 120

OUTPUT_COLS = [
    "latitude", "longitude", "ch4", "qa_value", "datetime",
    "gas", "instrument", "platform", "collection", "stac_id",
    "provider", "provider_all", "provider_roles",
    "processing_level", "mission_phase",
]

skipped    = []
spark_dfs  = []
batch_pdfs = []

for i, item in enumerate(items):
    # items is a list of pystac.Item — access via attributes, not dict keys
    item_id    = item.id
    item_dt    = item.datetime
    props      = item.properties                          # plain dict
    collection = (item.collection_id                      # may be None without root catalog
                  or props.get("collection")
                  or "sentinel-5p-l2-netcdf")

    print(f"[{i+1}/{len(items)}] {item_id}", end="  ")

    try:
        href = sign(item.assets["ch4"].href)
    except KeyError:
        skipped.append((item_id, "no ch4 asset"))
        print("SKIP: no ch4 asset")
        continue

    try:
        df = fetch_and_parse(href, timeout=HTTP_TIMEOUT)
    except requests.Timeout:
        skipped.append((item_id, f"timed out after {HTTP_TIMEOUT}s"))
        print("SKIP: timeout")
        continue
    except Exception as e:
        skipped.append((item_id, str(e)))
        print(f"SKIP: {e}")
        continue

    df = df.rename(columns={"methane_mixing_ratio_bias_corrected": "ch4"})
    df = df.replace([np.inf, -np.inf], np.nan)
    df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["ch4"]       = pd.to_numeric(df["ch4"],       errors="coerce")
    df["qa_value"]  = pd.to_numeric(df["qa_value"],  errors="coerce")
    df = df.dropna(subset=["latitude", "longitude", "ch4", "qa_value"])
    df = df[df["qa_value"] > 0.5]

    if df.empty:
        skipped.append((item_id, "all rows failed QA"))
        print("SKIP: no rows passed QA")
        continue

    df["datetime"]         = pd.to_datetime(item_dt, utc=True)
    df["gas"]              = props.get("s5p:product_name", "ch4").upper()
    df["instrument"]       = (props.get("instruments") or [None])[0]
    df["platform"]         = props.get("platform")
    df["collection"]       = collection
    df["stac_id"]          = item_id
    df["provider"]         = PROVIDER_NAME
    df["provider_all"]     = PROVIDER_ALL
    df["provider_roles"]   = PROVIDER_ROLES
    df["processing_level"] = props.get("s5p:processing_mode")
    df["mission_phase"]    = get_mission_phase(item_dt)

    batch_pdfs.append(df[OUTPUT_COLS])
    print(f"{len(df):,} rows")

    if len(batch_pdfs) >= BATCH_SIZE:
        spark_dfs.append(flush_batch(batch_pdfs))
        print(f"  >>> Flushed batch → {len(spark_dfs)} Spark DFs so far")
        batch_pdfs = []

if batch_pdfs:
    spark_dfs.append(flush_batch(batch_pdfs))
    print(f"  >>> Flushed final batch → {len(spark_dfs)} Spark DFs total")

if not spark_dfs:
    raise RuntimeError("spark_dfs is empty — every item was skipped.")

if skipped:
    print(f"\nSkipped {len(skipped)} items:")
    for sid, reason in skipped:
        print(f"  {sid}: {reason}")

spark_df = reduce(DataFrame.union, spark_dfs)
spark_df = spark_df.dropDuplicates(["latitude", "longitude", "datetime", "stac_id"])

print(f"\nPre-write count: {spark_df.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# VISUALIZATION OF DETECTIONS

df_plot = (
    spark_df
    .select("longitude", "latitude", "ch4")
    .sample(fraction=0.05, seed=42)
    .toPandas()
)

if df_plot.empty:
    print("No data to plot.")
else:
    gdf_geometry = [Point(xy) for xy in zip(df_plot["longitude"], df_plot["latitude"])]

    gdf = gpd.GeoDataFrame(
        df_plot,
        geometry=gdf_geometry,
        crs="EPSG:4326"
    )

    gdf["ch4_clipped"] = gdf["ch4"].clip(1800, 2000)

    world = gpd.read_file(
        "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
    )

    fig, ax = plt.subplots(figsize=(14, 7))
    world.boundary.plot(ax=ax, linewidth=0.5, color="black")
    gdf.plot(
        ax=ax,
        column="ch4_clipped",
        markersize=2,
        cmap="viridis",
        legend=True,
        alpha=0.7,
    )
    ax.set_title("Methane Concentration (TROPOMI Observations)")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    plt.tight_layout()
    plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.utils import AnalysisException

TABLE_NAME = "bronze.planetary_comp_raw_data"

# Tune file size at write time instead of manual repartition
spark.conf.set("spark.sql.files.maxRecordsPerFile", "500000")

table_exists        = True
existing_keys_cached = None

try:
    existing_df = spark.table(TABLE_NAME)

    existing_key_count = existing_df.select(
        "latitude", "longitude", "datetime", "stac_id"
    ).distinct().count()
    print(f"Existing table has ~{existing_key_count:,} distinct keys")

    existing_keys_cached = (
        existing_df
        .select("latitude", "longitude", "datetime", "stac_id")
        .distinct()
        .cache()
    )
    existing_keys_cached.count()  # materialize cache

    # Only broadcast if the existing key set is small (< 200k rows ≈ safe threshold)
    if existing_key_count < 200_000:
        join_right = broadcast(existing_keys_cached)
    else:
        join_right = existing_keys_cached

    spark_df = spark_df.alias("new").join(
        join_right.alias("old"),
        on=["latitude", "longitude", "datetime", "stac_id"],
        how="left_anti"
    )

except AnalysisException:
    table_exists = False
    print("Table not found — will create new.")

new_row_count = spark_df.count()
print(f"Rows to write: {new_row_count:,}")

if new_row_count > 0:
    if table_exists:
        spark_df.write \
            .mode("append") \
            .format("delta") \
            .option("mergeSchema", "true") \
            .saveAsTable(TABLE_NAME)
    else:
        spark_df.write \
            .mode("overwrite") \
            .format("delta") \
            .saveAsTable(TABLE_NAME)
    print("Data written to bronze layer.")
else:
    print("No new rows to write — table unchanged.")

if existing_keys_cached is not None:
    existing_keys_cached.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read back a sample to verify
df_check = spark.sql(f"SELECT * FROM {TABLE_NAME} LIMIT 1000")
display(df_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
