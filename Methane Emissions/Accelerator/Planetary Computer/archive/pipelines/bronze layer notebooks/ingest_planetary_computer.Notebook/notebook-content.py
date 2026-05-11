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

#Requirements 
#%pip install pystac-client planetary-computer xarray h5netcdf h5py fsspec geopandas


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#IMPORTS 

## Access Catalog
import planetary_computer
import pystac_client

## Load Dataset
import fsspec
import xarray as xr
from collections import Counter
from datetime import datetime
from planetary_computer import sign

## Data Manipulation
import pandas as pd 
import numpy as np 

## Data Visualization 
import matplotlib.pyplot as plt 
import geopandas as gpd 
from shapely.geometry import Point 

## Data storage 
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col
from pyspark.sql.types import *

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

#SEARCHING CATALOG 
longitude = -99.36    # Permian Basin, Texas, US
latitude = 31.5

#AREA WISE SEARCH
geometry = {
    "type": "Polygon",
    "coordinates": [[
        [-105, 25],   # min lon, min lat
        [-105, 35],
        [-90, 35],
        [-90, 25],
        [-105, 25]
    ]]
}

#loop to current date
start_date = "2024-01-01"
end_date = datetime.utcnow().strftime("%Y-%m-%d")


search = catalog.search(
    collections="sentinel-5p-l2-netcdf",
    intersects=geometry,
    datetime=f"{start_date}/{end_date}",
    query={
        "s5p:processing_mode": {"in": ["OFFL","NRTI"]}, #OFFL : sparse, high quality recent data, #NRTI: near real time, lower quality
        "s5p:product_name": {"eq": "ch4"}   
    },
)

items = list(search.items())

dates = [item.datetime.date() for item in items]
print("Available dates:", Counter(dates))
print(max(item.datetime for item in items))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from collections import Counter

for mode in ["OFFL", "NRTI"]:
    search = catalog.search(
        collections="sentinel-5p-l2-netcdf",
        intersects=geometry,
        datetime=f"{start_date}/{end_date}",
        query={
            "s5p:processing_mode": {"eq": mode},
            "s5p:product_name": {"eq": "ch4"}
        },
    )

    items = list(search.items())
    if items:
        print(f"{mode} max date:", max(item.datetime for item in items))
    else:
        print(f"{mode}: no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ADDING METADATA

all_dfs = []

# Sentinel-5P provider info is collection-level only — hardcoded per Planetary Computer catalog
S5P_PROVIDERS = [
    {"name": "European Space Agency", "roles": ["producer"]},
    {"name": "Microsoft", "roles": ["host"]},
]

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
    else:
        return "operational"


for i, item in enumerate(items):
    print(f"Processing {i+1}/{len(items)}  id={item.id}")

    try:
        href = sign(item.assets["ch4"].href) #access asset  #if no sign, you can't access data. 
    except KeyError:
        print(f"  SKIP: no 'ch4' asset on item {item.id}")
        continue

    props = item.properties #get metadata, if absent below 

    # ── Providers (hardcoded — absent from Planetary Computer item-level STAC) ──
    providers      = S5P_PROVIDERS
    provider_all   = ", ".join(p["name"] for p in providers)
    provider_roles = ", ".join(",".join(p.get("roles", [])) for p in providers)
    provider_name  = provider_all.split(",")[0].strip()   # "European Space Agency"

    # ── Mission phase ─────────────────────────────────────────────────────────
    mission_phase_value = get_mission_phase(item.datetime)

    # ── Load xarray dataset ───────────────────────────────────────────────────
    try:
        with fsspec.open(href).open() as f:
            ds = xr.open_dataset(f, group="PRODUCT", engine="h5netcdf")
            ds = ds[[
                "methane_mixing_ratio_bias_corrected", #bias corrected xch4
                "qa_value",
                "latitude",
                "longitude"
            ]] #select relevant variables
            df = ds.to_dataframe().reset_index()
    except Exception as e:
        print(f"  SKIP: failed to open dataset — {e}")
        continue

    df = df.rename(columns={"methane_mixing_ratio_bias_corrected": "ch4"})
    df = df.dropna(subset=["latitude", "longitude", "ch4", "qa_value"])
    df = df[df["qa_value"] > 0.5]

    if df.empty:
        print(f"  SKIP: no rows passed QA filter")
        continue

    # ── Attach metadata ───────────────────────────────────────────────────────
    df["datetime"]         = pd.to_datetime(item.datetime, utc=True)
    df["gas"]              = props.get("s5p:product_name", "ch4").upper()
    df["instrument"]       = (props.get("instruments") or [None])[0]
    df["platform"]         = props.get("platform")
    df["collection"]       = item.collection_id or "sentinel-5p-l2-netcdf"
    df["stac_id"]          = item.id
    df["provider"]         = provider_name
    df["provider_all"]     = provider_all
    df["provider_roles"]   = provider_roles
    df["processing_level"] = props.get("s5p:processing_mode")
    df["mission_phase"]    = mission_phase_value

    all_dfs.append(df)
    print(f"  {len(df):,} rows added")

# ── Combine & deduplicate ─────────────────────────────────────────────────────
if not all_dfs:
    raise RuntimeError("all_dfs is empty — every item was skipped.")

df_valid = pd.concat(all_dfs, ignore_index=True)
df_valid = df_valid.drop_duplicates(subset=["latitude", "longitude", "datetime", "stac_id"])

print("\n=== Final null counts ===")
print(df_valid.isnull().sum())
print("\nDone:", df_valid.shape)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# VISUALIZATION OF DETECTIONS
# Convert to GeoDataFrame
geometry = [Point(xy) for xy in zip(df_valid["longitude"], df_valid["latitude"])]

gdf = gpd.GeoDataFrame(
    df_valid,
    geometry=geometry,
    crs="EPSG:4326"
)

# Clip extreme values (better color scaling)
gdf["ch4_clipped"] = gdf["ch4"].clip(1800, 2000)

# Sample for performance
gdf_sample = gdf.sample(frac=0.1, random_state=42)

# Load world map
world = gpd.read_file(
    "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
)

# Plot
fig, ax = plt.subplots(figsize=(14,7))

world.boundary.plot(ax=ax, linewidth=0.5, color="black")

gdf_sample.plot(
    ax=ax,
    column="ch4_clipped",
    markersize=2,
    cmap="viridis",
    legend=True,
    alpha=0.7
)

ax.set_title("Methane Concentration (TROPOMI Observations)")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#SAVING DATA TO TABLE 
TABLE_NAME = "dbo.planetary_comp_raw_data" #for comparison 

# -------------------------------
# 1. Clean Pandas data
# -------------------------------
df_valid = df_valid.replace([np.inf, -np.inf], None)

df_valid["latitude"] = pd.to_numeric(df_valid["latitude"], errors="coerce")
df_valid["longitude"] = pd.to_numeric(df_valid["longitude"], errors="coerce")
df_valid["ch4"] = pd.to_numeric(df_valid["ch4"], errors="coerce")
df_valid["qa_value"] = pd.to_numeric(df_valid["qa_value"], errors="coerce")
df_valid["datetime"] = pd.to_datetime(df_valid["datetime"], errors="coerce")

df_valid = df_valid.dropna(subset=["latitude", "longitude", "datetime"])

# reorder columns like schema
df_valid = df_valid[[
    "latitude", "longitude", "ch4", "qa_value",
    "datetime", "gas", "instrument", "platform",
    "collection", "stac_id",
    "provider", "provider_all", "provider_roles",
    "processing_level", "mission_phase"
]]

# -------------------------------
# 2. Define schema
# -------------------------------
schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("ch4", DoubleType(), True),
    StructField("qa_value", DoubleType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("gas", StringType(), True),
    StructField("instrument", StringType(), True),
    StructField("platform", StringType(), True),

    # NEW FIELDS
    StructField("collection", StringType(), True),
    StructField("stac_id", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("provider_all", StringType(), True),
    StructField("provider_roles", StringType(), True),
    StructField("processing_level", StringType(), True),
    StructField("mission_phase", StringType(), True),
])

# -------------------------------
# 3. Convert to Spark
# -------------------------------
spark_df = spark.createDataFrame(df_valid, schema=schema)

# -------------------------------
# 4. Deduplicate
# -------------------------------
spark_df = spark_df.dropDuplicates(["latitude", "longitude", "datetime", "stac_id"])

# -------------------------------
# 5. Reduce partitions
# -------------------------------
spark_df = spark_df.coalesce(2)

# -------------------------------
# 6. Remove existing duplicates
# -------------------------------
table_exists = True

try:
    existing_df = spark.table(TABLE_NAME)

    spark_df = spark_df.alias("new").join(
        existing_df.alias("old"),
        on=["latitude", "longitude", "datetime","stac_id"],
        how="left_anti"
    )

except AnalysisException:
    table_exists = False
    print("Table not found, will create new")

# -------------------------------
# 7. Write
# -------------------------------
if spark_df.limit(1).count() == 0:
    print("No new data to write")

else:
    if table_exists:
        spark_df.write \
    .mode("append") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(TABLE_NAME)
    else:
        spark_df.write.mode("overwrite").format("delta").saveAsTable(TABLE_NAME)

    print("Data written to bronze layer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Planetary_computer_LH.dbo.planetary_comp_raw_data LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
