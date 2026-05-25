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

# SEARCHING CATALOG
longitude = -99.36    # Permian Basin, Texas, US
latitude  = 31.5

# AREA WISE SEARCH — renamed to avoid collision with viz cell's `geometry`
search_geometry = {
    "type": "Polygon",
    "coordinates": [[
        [-105, 25],
        [-105, 35],
        [-90,  35],
        [-90,  25],
        [-105, 25],
    ]]
}

end_date   = datetime.utcnow()
start_date = (end_date - timedelta(days=5)).strftime("%Y-%m-%d")
end_date   = end_date.strftime("%Y-%m-%d")

search = catalog.search(
    collections=["sentinel-5p-l2-netcdf"],
    intersects=search_geometry,
    datetime=f"{start_date}/{end_date}",
    query={
        "s5p:processing_mode": {"in": ["OFFL", "NRTI"]},
        "s5p:product_name":    {"eq": "ch4"}
    },
    max_items=500,
)

items = list(search.items())

dates = [item.datetime.date() for item in items]
print("Available dates:", Counter(dates))
if items:
    print("Latest item:", max(item.datetime for item in items))
else:
    print("No items found for the given search parameters.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for mode in ["OFFL", "NRTI"]:
    search = catalog.search(
        collections=["sentinel-5p-l2-netcdf"],
        intersects=search_geometry,   # ← use renamed variable
        datetime=f"{start_date}/{end_date}",
        query={
            "s5p:processing_mode": {"eq": mode},
            "s5p:product_name":    {"eq": "ch4"}
        },
        max_items=500,
    )
    mode_items = list(search.items())
    if mode_items:
        print(f"{mode} max date:", max(i.datetime for i in mode_items))
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

    # Drop xarray index columns that aren't needed
    df = df.drop(columns=[c for c in ["scanline", "ground_pixel", "time"] if c in df.columns])
    return df

def flush_batch(pandas_dfs):
    """Combine a list of per-item DataFrames into a single Spark DataFrame."""
    combined = pd.concat(pandas_dfs, ignore_index=True)
    # Dedup now works because datetime + stac_id are set before append (see loop)
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
    print(f"[{i+1}/{len(items)}] {item.id}", end="  ")

    try:
        href = sign(item.assets["ch4"].href)
    except KeyError:
        skipped.append((item.id, "no ch4 asset"))
        print("SKIP: no ch4 asset")
        continue

    props = item.properties

    try:
        df = fetch_and_parse(href, timeout=HTTP_TIMEOUT)
    except requests.Timeout:
        skipped.append((item.id, f"timed out after {HTTP_TIMEOUT}s"))
        print("SKIP: timeout")
        continue
    except Exception as e:
        skipped.append((item.id, str(e)))
        print(f"SKIP: {e}")
        continue

    df = df.rename(columns={"methane_mixing_ratio_bias_corrected": "ch4"})
    df = df.replace([np.inf, -np.inf], np.nan)   # ← np.nan, not None; pandas handles it better
    df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["ch4"]       = pd.to_numeric(df["ch4"],       errors="coerce")
    df["qa_value"]  = pd.to_numeric(df["qa_value"],  errors="coerce")
    df = df.dropna(subset=["latitude", "longitude", "ch4", "qa_value"])
    df = df[df["qa_value"] > 0.5]

    if df.empty:
        skipped.append((item.id, "all rows failed QA"))
        print("SKIP: no rows passed QA")
        continue

    # ← Metadata assigned BEFORE append so flush_batch dedup is effective
    df["datetime"]         = pd.to_datetime(item.datetime, utc=True)
    df["gas"]              = props.get("s5p:product_name", "ch4").upper()
    df["instrument"]       = (props.get("instruments") or [None])[0]
    df["platform"]         = props.get("platform")
    df["collection"]       = item.collection_id or "sentinel-5p-l2-netcdf"
    df["stac_id"]          = item.id
    df["provider"]         = PROVIDER_NAME
    df["provider_all"]     = PROVIDER_ALL
    df["provider_roles"]   = PROVIDER_ROLES
    df["processing_level"] = props.get("s5p:processing_mode")
    df["mission_phase"]    = get_mission_phase(item.datetime)

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
# Repartition deferred to the write cell where we know the final row count

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
