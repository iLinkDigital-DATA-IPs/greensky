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
# META     "event_stream": {
# META       "known_event_streams": [
# META         {
# META           "artifact_id": "69738a9d-905c-4ada-84a7-7eb3996b271f",
# META           "stream_id": "69738a9d-905c-4ada-84a7-7eb3996b271f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#requirements 
%pip install pystac-client planetary-computer xarray h5netcdf h5py fsspec geopandas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Methane plume clusters in the orbital swath passing over Permian Basin, Texas, US ( 30th March 2026)
# Sentinel - 5P has global coverage but collected in oribtal strips. Below code is for Permian Basin swath
# 
# Docs Code: STAC
# 
# Following the docs found [here](https://planetarycomputer.microsoft.com/dataset/sentinel-5p-l2-netcdf#Example-Notebook)


# CELL ********************

#Access catalog
import planetary_computer
import pystac_client

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

longitude = -99.36    # Permian Basin, Texas, US
latitude = 31.5

geometry = {
    "type": "Point",
    "coordinates": [longitude, latitude],
}

search = catalog.search(
    collections="sentinel-5p-l2-netcdf",
    intersects=geometry,
    datetime="2026-03-25/2026-04-03",
    query={
        "s5p:processing_mode": {"eq": "OFFL"},
        "s5p:product_name": {"eq": "ch4"}   
    },
)

items = list(search.items())

print(f"Found {len(items)} items")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Loading Dataset
import fsspec
import xarray as xr

f = fsspec.open(items[0].assets["ch4"].href).open()

ds = xr.open_dataset(
    f,
    group="PRODUCT",
    engine="h5netcdf"
)

ds

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Working with existing data. 
# 
# Other data will have to be derived (?)
# 


# CELL ********************

#For existing data: methane_mixing_ratio, time,latitude, longitude,qa_value
#Extracting Variables

import numpy as np

lat = ds["latitude"].values[0]
lon = ds["longitude"].values[0]
ch4 = ds["methane_mixing_ratio"].values[0]
qa = ds["qa_value"].values[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Flatten into table
import pandas as pd

df = pd.DataFrame({
    "latitude": lat.flatten(),
    "longitude": lon.flatten(),
    "ch4": ch4.flatten(),
    "qa_value": qa.flatten()
})

#Ensuring all arrays have same shape
print(lat.shape, lon.shape, ch4.shape, qa.shape)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adding metadata
df["datetime"] = str(ds["time"].values[0])
df["gas"] = "CH4"
df["instrument"] = "TROPOMI"
df["platform"] = "Sentinel-5P"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspecting data
df.describe()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.head(100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Filtering data for qa_value > 0.5. qa_value is the quality filter
df_valid=df[df["qa_value"] > 0.5]

df_valid.describe()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(len(df_valid))

df_valid.head(100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================
# Geospatial Methane Hotspot Map
# ================================
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point

# Convert to GeoDataFrame
geometry = [Point(xy) for xy in zip(df_valid["longitude"], df_valid["latitude"])]

gdf = gpd.GeoDataFrame(
    df_valid,
    geometry=geometry,
    crs="EPSG:4326"
)

# Load world map
world = gpd.read_file(
    "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
)

# -------------------------------
# Plot
# -------------------------------
fig, ax = plt.subplots(figsize=(14,7))

# Plot world boundaries
world.boundary.plot(ax=ax, linewidth=0.5, color="black")

# Plot methane points
sc = ax.scatter(
    gdf["longitude"],
    gdf["latitude"],
    c=gdf["ch4"],
    s=2,
    cmap="viridis",
    alpha=0.7
)

# Colorbar
plt.colorbar(sc, ax=ax, label="CH4 (ppb)")

# Labels
ax.set_title("Global Methane Concentration (TROPOMI)")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Deriving Features from existing data

# CELL ********************

#Finding methane plumes. (By finding spatial anomalies (hotspots))

#Core idea: use DBSCAN 
## Groups nearby high values -> actual plume shapes 
## Removes isolated high values that are likely noise or measurement errors.

# 1. QA filter 
df_plumes = df_valid.copy()

# 2. High percentile filter
high = df_plumes[df_valid["ch4"] > df_plumes["ch4"].quantile(0.95)]

# 3. Cluster
from sklearn.cluster import DBSCAN

coords = high[["latitude", "longitude"]].values
clusters = DBSCAN(eps=0.25, min_samples=8).fit(coords)

high["cluster"] = clusters.labels_

# 4. Keep real plumes
plumes = high[high["cluster"] != -1]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================
# 1. Plotting Plumes
# ================================
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point

# ================================
# 2. Prepare GeoDataFrame
# ================================
# Convert df_valid → GeoDataFrame
geometry = [Point(xy) for xy in zip(df_valid["longitude"], df_valid["latitude"])]

gdf = gpd.GeoDataFrame(
    df_valid,
    geometry=geometry,
    crs="EPSG:4326"
)

# ================================
# 3. Load world boundaries (FIXED)
# ================================
world = gpd.read_file(
    "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
)
# ================================
# 4. Spatial join (assign country)
# ================================
gdf_with_country = gpd.sjoin(
    gdf,
    world[["NAME", "geometry"]],  # cleaner join
    how="left",
    predicate="within"
)

# Clean AFTER join
gdf_with_country = gdf_with_country.dropna(subset=["NAME", "ch4"])
# ================================
# 5. Country-level methane stats
# ================================
country_ch4 = (
    gdf_with_country
    .groupby("NAME")["ch4"]
    .mean()
    .sort_values(ascending=False)
)

print("\nTop 10 Countries by Average CH4:\n")
print(country_ch4.head(10))

# ================================
# 6. Map plumes → countries
# ================================


# Use same indices from plumes
plumes_geo = gdf_with_country.merge(
    plumes[["cluster"]],
    left_index=True,
    right_index=True,
    how="inner"
)

# Count plume points per country
plume_counts = (
    plumes_geo
    .groupby("NAME")
    .size()
    .sort_values(ascending=False)
)

print("\nTop Countries with Most Plume Points:\n")
print(plume_counts.head(10))

# ================================
# 7. Plume summary (cluster + country)
# ================================
plumes_with_country = plumes_geo.copy()

plume_summary = (
    plumes_with_country
    .groupby(["cluster", "NAME"])
    .agg({
        "ch4": ["mean", "max", "count"]
    })
    .reset_index()
)

print("\nPlume Summary (first few rows):\n")
print(plume_summary.head())

# ================================
# 8. Plot plumes on map
# ================================
fig, ax = plt.subplots(figsize=(14,7))

# Country borders
world.boundary.plot(ax=ax, linewidth=0.5, color="black")

# Background points (faint)
ax.scatter(
    gdf_with_country["longitude"],
    gdf_with_country["latitude"],
    s=1,
    alpha=0.1
)

# Plumes highlighted
sc = ax.scatter(
    plumes_geo["longitude"],
    plumes_geo["latitude"],
    c=plumes_geo["cluster"],
    cmap="tab10",
    s=15
)

plt.colorbar(sc, ax=ax, label="Cluster ID")

ax.set_title("Methane Plumes by Cluster (with Countries)")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================
# Build plume-level dataset (STRICT)
# ================================

# 1. Assign plume_id
plumes_geo["plume_id"] = plumes_geo["cluster"]

# 2. Ensure required columns exist
plumes_geo["gas"] = "CH4"
plumes_geo["instrument"] = "TROPOMI"
plumes_geo["platform"] = "Sentinel-5P"
plumes_geo["provider"] = "ESA"

# 3. Aggregate (ONLY required fields)
plume_df = (
    plumes_geo
    .groupby("plume_id")
    .agg({
        "latitude": "mean",
        "longitude": "mean",
        "datetime": "min",
        "gas": "first",
        "instrument": "first",
        "platform": "first",
        "provider": "first"
    })
)

# 4. Rename columns
plume_df = plume_df.rename(columns={
    "latitude": "plume_latitude",
    "longitude": "plume_longitude"
})

# 5. Reset index
plume_df = plume_df.reset_index()

# 6. Ensure correct types
plume_df["plume_id"] = plume_df["plume_id"].astype(str)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plume_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Calculating the plume bound
## Logic:  [min_longitude, min_latitude, max_longitude, max_latitude]
## Using Convex Hull for real geometric footprint 

# ================================
# Convex hull 
# ================================

def safe_convex_hull(geom):
    hull = geom.union_all().convex_hull
    
    if hull.geom_type == "Point":
        return hull.buffer(0.01)
    elif hull.geom_type == "LineString":
        return hull.buffer(0.01)
    
    return hull

plume_hulls = (
    plumes_geo
    .groupby("plume_id")
    .geometry
    .apply(safe_convex_hull)
)

#match types
plume_hulls.index = plume_hulls.index.astype(str)

# Map correctly
plume_df["plume_bounds"] = plume_df["plume_id"].map(
    plume_hulls.apply(lambda x: x.wkt)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plume_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Visualizing plume bounds 
from shapely import wkt
import geopandas as gpd

# Convert WKT to geometry
plume_df["geometry"] = plume_df["plume_bounds"].apply(wkt.loads)

# Create GeoDataFrame
plume_gdf = gpd.GeoDataFrame(
    plume_df,
    geometry="geometry",
    crs="EPSG:4326"
)

#Load world map
world = gpd.read_file(
    "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
)

#plot plumes
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(10,8))

# World boundaries
world.boundary.plot(ax=ax, linewidth=0.5, color="black")

# Raw points
plumes_geo.plot(
    ax=ax,
    markersize=2,
    alpha=0.3,
    color="gray"
)

# Hulls
plume_gdf.plot(
    ax=ax,
    edgecolor="red",
    facecolor="none",
    linewidth=2
)

# Centroids
ax.scatter(
    plume_gdf["plume_longitude"],
    plume_gdf["plume_latitude"],
    color="blue",
    s=30,
    label="Centroid"
)

ax.set_title("Zoomed Methane Plumes (Convex Hull)")
ax.legend()

plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Weather Data Analysis
# ## Getting from WeatherFeeds

# PARAMETERS CELL ********************

# Set the event stream item and datasource IDs
__in_eventstream_item_id = "69738a9d-905c-4ada-84a7-7eb3996b271f"
__in_eventstream_datasource_id = "cdd48ad7-0e01-4159-a602-9d1a8addfdc9"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, from_json, to_timestamp, to_date, current_date
from pyspark.sql.types import *
import time

# -------------------------------
# 1. Eventstream config
# -------------------------------
eventstream_options = {
    "eventstream.itemid": __in_eventstream_item_id,
    "eventstream.datasourceid": __in_eventstream_datasource_id
}

# -------------------------------
# 2. Read stream
# -------------------------------
df_raw = spark.readStream.format("kafka").options(**eventstream_options).load()

decoded_df = df_raw.select(
    col("value").cast(StringType()).alias("value")
)

# -------------------------------
# 3. CORRECT schema (based on your JSON)
# -------------------------------
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

# -------------------------------
# 4. Parse JSON
# -------------------------------
parsed_df = decoded_df.withColumn(
    "data",
    from_json(col("value"), schema)
).select("data.*")

# -------------------------------
# 5. Flatten (CRITICAL)
# -------------------------------
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

# -------------------------------
# 6. Convert time
# -------------------------------
parsed_df = parsed_df.withColumn(
    "dateTime",
    to_timestamp("dateTime")
)

parsed_df = parsed_df.withColumn(
    "date",
    to_date("dateTime")
)

# -------------------------------
# 7. Filter today
# -------------------------------
today_df = parsed_df.filter(
    col("date") == current_date()
)

# -------------------------------
# 8. Collect batches
# -------------------------------
collected_batches = []

def collect_batch(df, epoch_id):
    if df.count() > 0:
        print(f"\nBatch {epoch_id}")
        df.show(truncate=False)
        collected_batches.append(df)

# -------------------------------
# 9. Run stream (120 sec)
# -------------------------------
query = today_df.writeStream \
    .foreachBatch(collect_batch) \
    .outputMode("append") \
    .start()

time.sleep(120)
query.stop()

# -------------------------------
# 10. Combine batches
# -------------------------------
if collected_batches:
    final_weather_df = collected_batches[0]
    for df in collected_batches[1:]:
        final_weather_df = final_weather_df.union(df)
    
    final_weather_df = final_weather_df.dropDuplicates()
else:
    final_weather_df = spark.createDataFrame([], today_df.schema)

# -------------------------------
# 11. Final output
# -------------------------------
final_weather_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Computing plume movement vector
from pyspark.sql.functions import cos, sin, radians

final_weather_df = final_weather_df.withColumn(
    "wind_dx",
    col("wind_speed") * cos(radians(col("wind_direction")))
).withColumn(
    "wind_dy",
    col("wind_speed") * sin(radians(col("wind_direction")))
)
final_weather_pd = final_weather_df.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Saving Data

# CELL ********************

plume_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_valid.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_weather_pd.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import geopandas as gpd

# Local temp directory
temp_path = "/tmp/bronze_data/"
os.makedirs(temp_path, exist_ok=True)

# Geometry is already Shapely Polygons, just convert directly to GeoDataFrame
plume_gdf = gpd.GeoDataFrame(plume_df, geometry="geometry", crs="EPSG:4326")
plume_gdf.to_file(temp_path + "plume_data.geojson", driver="GeoJSON")

# Save others as CSV
df_valid.to_csv(temp_path + "methane_data.csv", index=False)
final_weather_pd.to_csv(temp_path + "weather_data.csv", index=False)

print("Saved locally, now copying to OneLake...")

workspace_id = "060ba34b-f1a3-4509-a6e2-36d1e736a8eb"
lakehouse_id = "95f7f55e-9d06-4699-9483-754709a1d397"
base_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/data-bronze/"

notebookutils.fs.mkdirs(base_path)

notebookutils.fs.cp("file://" + temp_path + "plume_data.geojson", base_path + "plume_data.geojson")
notebookutils.fs.cp("file://" + temp_path + "methane_data.csv", base_path + "methane_data.csv")
notebookutils.fs.cp("file://" + temp_path + "weather_data.csv", base_path + "weather_data.csv")

print("Done! Verifying...")

files = notebookutils.fs.ls(base_path)
for f in files:
    print(f.name, f.size)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# Disable Arrow to avoid buffer error
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# Fix datetime types
df_valid['datetime'] = pd.to_datetime(df_valid['datetime'])
final_weather_pd['dateTime'] = pd.to_datetime(final_weather_pd['dateTime'])

# Rename weather dateTime to datetime to match other tables
final_weather_pd = final_weather_pd.rename(columns={'dateTime': 'datetime'})

# Fix plume datetime
plume_df_copy = plume_df.copy()
plume_df_copy["geometry"] = plume_df_copy["geometry"].astype(str)
plume_df_copy["plume_bounds"] = plume_df_copy["plume_bounds"].astype(str)
plume_df_copy['datetime'] = pd.to_datetime(plume_df_copy['datetime'])

plume_df_copy = plume_df_copy.drop_duplicates(
    subset=["plume_id", "datetime"]
)

# Re-save with overwriteSchema=True to force schema update
spark.createDataFrame(df_valid).write.mode("append").saveAsTable("methane_data")
spark.createDataFrame(final_weather_pd).write.mode("append").saveAsTable("weather_data")
spark.createDataFrame(plume_df_copy).write.mode("append").saveAsTable("plume_data")

print("Tables re-saved with correct datetime types!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Deriving Emission Rate
# Using IME : it's the most widely used for satellite-derived plume data 
# 
# Emission Rate (kg/hr) = IME × U_eff / L_plume
# 
# Where:
# IME        = sum of CH4 enhancement × pixel area (kg)
# U_eff      = effective wind speed at plume height (m/s)
# L_plume    = plume length (m) derived from geometry
# 
# ## Note
# The IME method has ~30-50% uncertainty which is typical for satellite-based estimates. The key assumptions are that TROPOMI pixel area is ~3.5 km² and wind speed is uniform across the plume.

# CELL ********************

# This was missing — define df_filtered from df_valid
df_filtered = df_valid[df_valid['qa_value'] >= 0.5].copy()

# Compute background and enhancement here too
background_ch4 = df_filtered['ch4'].median()
df_filtered['ch4_enhancement'] = (df_filtered['ch4'] - background_ch4).clip(lower=0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#SPATIAL JOIN PIXELS TO PLUMES
import geopandas as gpd
from shapely.geometry import Point

# Convert df_valid pixels to GeoDataFrame
gdf_pixels = gpd.GeoDataFrame(
    df_filtered,
    geometry=gpd.points_from_xy(df_filtered['longitude'], df_filtered['latitude']),
    crs="EPSG:4326"
)

# Ensure plume_df has Shapely geometries and correct CRS
gdf_plumes = plume_df[['plume_id', 'plume_latitude', 'plume_longitude', 'geometry']].copy()
gdf_plumes = gpd.GeoDataFrame(gdf_plumes, geometry='geometry', crs="EPSG:4326")

# Spatial join — tags each pixel with the plume it falls inside
pixels_in_plumes = gpd.sjoin(
    gdf_pixels,
    gdf_plumes[['plume_id', 'geometry']],
    how='inner',
    predicate='within'
)

print(f"Pixels matched to plumes: {len(pixels_in_plumes)}")
print(pixels_in_plumes.groupby('plume_id').size())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CH4_MOLAR_MASS = 16.04e-3
AIR_MOLAR_MASS = 28.97e-3
SURFACE_PRESSURE = 101325
GRAVITY = 9.81
TROPOMI_PIXEL_AREA = 3.5e6
PPB_TO_MOL_FRACTION = 1e-9

def ppb_to_column_density(ch4_ppb, pressure_pa=SURFACE_PRESSURE):
    mol_fraction = ch4_ppb * PPB_TO_MOL_FRACTION
    dry_air_column = pressure_pa / (AIR_MOLAR_MASS * GRAVITY)
    return mol_fraction * dry_air_column

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#COMPUTE IME PER PLUME FROM ITS MATCHED PIXELS
def calculate_ime(ch4_enhancement_ppb_series):
    col_density = ppb_to_column_density(ch4_enhancement_ppb_series)
    mass_per_pixel = col_density * CH4_MOLAR_MASS * TROPOMI_PIXEL_AREA
    return mass_per_pixel.sum()

# Compute background from ALL pixels (global median)
background_ch4 = gdf_pixels['ch4'].median()
pixels_in_plumes['ch4_enhancement'] = (
    pixels_in_plumes['ch4'] - background_ch4
).clip(lower=0)

# IME per plume
ime_per_plume = (
    pixels_in_plumes
    .groupby('plume_id')['ch4_enhancement']
    .apply(calculate_ime)
    .reset_index()
    .rename(columns={'ch4_enhancement': 'ime_kg'})
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#PLUME LENGTH VIA MINIMUM ROTATED RECTANGLE
from shapely.geometry import Point

def plume_length_degrees_to_m(geom):
    """Major axis of minimum rotated rectangle, converted from degrees to meters."""
    rect = geom.minimum_rotated_rectangle
    coords = list(rect.exterior.coords)
    sides = [
        Point(coords[i]).distance(Point(coords[i+1]))
        for i in range(len(coords) - 1)
    ]
    length_deg = max(sides)
    return length_deg * 111320  # degrees → meters (valid near equator)

gdf_plumes['plume_length_m'] = gdf_plumes['geometry'].apply(plume_length_degrees_to_m)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import numpy as np
import pandas as pd

def fetch_historical_wind(lat, lon, date_str):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": round(lat, 4),
        "longitude": round(lon, 4),
        "start_date": date_str,
        "end_date": date_str,
        "daily": "wind_speed_10m_max",
        "wind_speed_unit": "ms",
        "timezone": "UTC"
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        return data['daily']['wind_speed_10m_max'][0]
    except Exception:
        return np.nan

def fetch_wind_for_date(date_str):
    base = gdf_plumes[['plume_id', 'plume_latitude', 'plume_longitude', 'plume_length_m']].copy()
    base['datetime'] = pd.to_datetime(date_str)

    rows = base[['plume_id', 'plume_latitude', 'plume_longitude']].to_dict('records')
    wind_results = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(fetch_historical_wind, row['plume_latitude'], row['plume_longitude'], date_str): row['plume_id']
            for row in rows
        }
        for future in as_completed(futures):
            wind_results[futures[future]] = future.result()

    base['wind_speed'] = base['plume_id'].map(wind_results)
    print(f"{date_str} — NaN wind: {base['wind_speed'].isna().sum()} / {len(base)}")
    return base

# Fetch both dates
wind_mar28 = fetch_wind_for_date("2026-03-28")
wind_mar29 = fetch_wind_for_date("2026-03-29")
wind_mar30 = fetch_wind_for_date("2026-03-30")

# Combine
plume_with_wind = pd.concat([wind_mar28, wind_mar29, wind_mar30], ignore_index=True)
print(f"\nTotal rows: {len(plume_with_wind)}")
print(plume_with_wind[['plume_id', 'datetime', 'wind_speed']])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#MERGE EVERYTHING AND COMPUTE EMISSION RATE
# Merge IME + plume metadata + wind
emission_input = (
    gdf_plumes[['plume_id', 'plume_latitude', 'plume_longitude', 'plume_length_m']]
    .merge(ime_per_plume, on='plume_id')
    .merge(plume_with_wind[['plume_id', 'datetime', 'wind_speed']], on='plume_id')
)

mean_wind = final_weather_pd['wind_speed'].mean()

def compute_emission_rate(row):
    u = row['wind_speed'] if pd.notna(row['wind_speed']) else mean_wind
    l = row['plume_length_m']
    if l > 0 and u > 0:
        return row['ime_kg'] * u / l * 3600  # kg/s → kg/hr
    return np.nan

emission_input['emission_rate_kg_hr'] = emission_input.apply(compute_emission_rate, axis=1)
print(emission_input[['plume_id', 'ime_kg', 'wind_speed', 'plume_length_m', 'emission_rate_kg_hr']])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#save new table
spark.createDataFrame(emission_input).write.mode("append").saveAsTable("emission_rates")
#spark.createDataFrame(emission_input).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("emission_rates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Sector data fetched from Carbon Mapper API 
# 
# Selectively get only sector information

# CELL ********************

import requests

# === API Configuration ===
base_url = "https://api.carbonmapper.org/api/v1/"
endpoint = "catalog/plumes/annotated"

token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzc1NjMxMTM4LCJpYXQiOjE3NzUwMjYzMzgsImp0aSI6IjZjOTA5YTY1MzI2YzQyN2Q4NTE2NDM4OGVhZDg1MWI1Iiwic2NvcGUiOiJzdGFjIGNhdGFsb2c6cmVhZCIsImdyb3VwcyI6IlB1YmxpYyIsImFsbF9ncm91cF9uYW1lcyI6eyJjb21tb24iOlsiUHVibGljIl19LCJvcmdhbml6YXRpb25zIjoiIiwic2V0dGluZ3MiOnt9LCJpc19zdGFmZiI6ZmFsc2UsImlzX3N1cGVydXNlciI6ZmFsc2UsInVzZXJfaWQiOjMyMjQyfQ.KrtcQz82StB0LTykPNmO0Ajh1cd6qKOF8gv88ihiWuQ" 

params = {
    "limit": 1000,
    "offset": 0,
    "bbox": [-110, 15, -95, 30], #Permian Basin (Texas)
    "start_date": "2026-01-01T00:00:00Z",
    "end_date": "2026-03-30T23:59:59Z",
    "sector": "1B2"
}

headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json"
}

# Pagination
all_records = []
page = 0
page_size = params["limit"]

while True:
    params["offset"] = page * page_size
    
    response = requests.get(base_url + endpoint, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        break

    data = response.json()
    items = data.get("items", [])

    if not items:
        print("All pages fetched.")
        break

    all_records.extend(items)
    page += 1

print(f"Total records fetched: {len(all_records)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

json_rdd = spark.sparkContext.parallelize(
    [json.dumps(r) for r in all_records]
)

df = spark.read.json(json_rdd)

print(f"Records loaded: {df.count()}")
display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

cm_pd = df.select(
    "plume_id",
    "sector",
    "scene_timestamp",
    "geometry_json"   
).toPandas()

# Rename columns
cm_pd = cm_pd.rename(columns={
    "scene_timestamp": "datetime"
})

# Fix datetime
cm_pd["datetime"] = pd.to_datetime(
    cm_pd["datetime"],
    format="ISO8601",
    errors="coerce"
).dt.tz_localize(None)

cm_pd["plume_id"] = cm_pd["plume_id"].astype(str)

# Extract lat/long from geometry_json
cm_pd["longitude"] = cm_pd["geometry_json"].apply(
    lambda x: x["coordinates"][0] if x else None
)
cm_pd["latitude"] = cm_pd["geometry_json"].apply(
    lambda x: x["coordinates"][1] if x else None
)

# Drop geometry_json (no longer needed)
cm_pd = cm_pd.drop(columns=["geometry_json"])

# Deduplicate
cm_pd = cm_pd.drop_duplicates(subset=["plume_id", "datetime"])

print("Carbon Mapper data ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plume_df_copy = plume_df.copy()

plume_df_copy["geometry"] = plume_df_copy["geometry"].astype(str)
plume_df_copy["plume_bounds"] = plume_df_copy["plume_bounds"].astype(str)

plume_df_copy["datetime"] = pd.to_datetime(
    plume_df_copy["datetime"]
).dt.tz_localize(None)

plume_df_copy["plume_id"] = plume_df_copy["plume_id"].astype(str)

plume_df_copy["latitude"] = plume_df_copy["plume_latitude"]
plume_df_copy["longitude"] = plume_df_copy["plume_longitude"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  Align timestamps
plume_df_copy["datetime"] = plume_df_copy["datetime"].dt.floor("min")
cm_pd["datetime"] = cm_pd["datetime"].dt.floor("min")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ensure datetime is clean and aligned
plume_df_copy["datetime"] = pd.to_datetime(
    plume_df_copy["datetime"]
).dt.tz_localize(None)

cm_pd["datetime"] = pd.to_datetime(
    cm_pd["datetime"],
    format="ISO8601",
    errors="coerce"
).dt.tz_localize(None)

# Align timestamps to minute
plume_df_copy["datetime"] = plume_df_copy["datetime"].dt.floor("min")
cm_pd["datetime"] = cm_pd["datetime"].dt.floor("min")

# Ensure lat/long exist
plume_df_copy["latitude"] = plume_df_copy["plume_latitude"]
plume_df_copy["longitude"] = plume_df_copy["plume_longitude"]

# -------------------------------
#  GLOBAL DEBUG CHECKS
# -------------------------------
print("Plume datetime range:", plume_df_copy["datetime"].min(), "→", plume_df_copy["datetime"].max())
print("CM datetime range:", cm_pd["datetime"].min(), "→", cm_pd["datetime"].max())

print("\nPlume lat/lon range:")
print(plume_df_copy[["latitude", "longitude"]].describe())

print("\nCM lat/lon range:")
print(cm_pd[["latitude", "longitude"]].describe())

print("\nCM sector distribution:")
print(cm_pd["sector"].value_counts(dropna=False).head())

# -------------------------------
# Geo + nearest-time matching
# -------------------------------
def match_sector(row, cm_df, max_dist=2.0, max_time_diff_days=360):
    candidates = cm_df[
        (abs(cm_df["latitude"] - row["latitude"]) <= max_dist) &
        (abs(cm_df["longitude"] - row["longitude"]) <= max_dist)
    ]
    
    #  DEBUG: spatial candidates
    if row.name < 3:
        print(f"\nRow {row.name} → spatial candidates:", len(candidates))
    
    if candidates.empty:
        return None
    
    candidates = candidates.copy()
    candidates["time_diff"] = abs(
        (candidates["datetime"] - row["datetime"]).dt.total_seconds()
    )  # in seconds
    
    #  DEBUG: show closest BEFORE filtering
    if row.name < 3:
        print(f"Row {row.name} → closest time diffs (mins):",
              (candidates["time_diff"] / 60).sort_values().head(3).tolist())
    
    # Optional sanity filter (e.g., ignore matches > X days)
    max_time_diff_sec = max_time_diff_days * 24 * 60 * 60
    candidates = candidates[candidates["time_diff"] <= max_time_diff_sec]
    
    #  DEBUG: after time sanity filter
    if row.name < 3:
        print(f"Row {row.name} → after time sanity filter:", len(candidates))
        if not candidates.empty:
            print(candidates[["latitude", "longitude", "datetime", "sector"]].head(2))
    
    if candidates.empty:
        return None
    
    #  pick closest in time
    best_match = candidates.sort_values("time_diff").iloc[0]
    
    return best_match["sector"]

# Apply matching
plume_df_copy["sector"] = plume_df_copy.apply(
    lambda row: match_sector(row, cm_pd),
    axis=1
)

# Fill missing sectors
plume_df_copy["sector"] = plume_df_copy["sector"].fillna("unknown")

print("\nSector mapping complete")
print("\nFinal sector distribution:")
print(plume_df_copy["sector"].value_counts())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------
# IPCC SECTOR MAP
# -------------------------------
sector_map = {
    "1A1": "Energy Industries (Power Plants, Refineries)",
    "1A2": "Manufacturing & Industry",
    "1A3": "Transport",
    "1A4": "Buildings",
    "1B1A": "Coal Mining",
    "1B2": "Oil & Gas (Fugitive Emissions)",
    "2": "Industrial Processes",
    "3": "Agriculture",
    "4": "Land Use & Forestry",
    "5": "Other",
    "6A": "Solid Waste (Landfills)",
    "6B": "Wastewater",
    "6C": "Waste Incineration",
    "OTHER": "Other / Unknown"
}

# -------------------------------
# CLEAN SECTOR VALUES
# -------------------------------
plume_df_copy["sector"] = (
    plume_df_copy["sector"]
    .astype(str)
    .str.strip()     # remove spaces
    .str.upper()     # normalize case
)

# -------------------------------
# FALLBACK MAPPING FUNCTION
# -------------------------------
def map_sector_fallback(code):
    # direct match
    if code in sector_map:
        return sector_map[code]
    
    # fallback to parent category (prefix match)
    for key in sector_map:
        if code.startswith(key):
            return sector_map[key]
    
    return "Unknown"

# -------------------------------
# APPLY MAPPING
# -------------------------------
plume_df_copy["sector_name"] = plume_df_copy["sector"].apply(map_sector_fallback)

# -------------------------------
# DEBUG: CHECK UNMAPPED VALUES
# -------------------------------
unmapped = plume_df_copy[
    plume_df_copy["sector_name"] == "Unknown"
]["sector"].unique()

print("Unmapped sector values:", unmapped)

print("\nSector Mapping Preview:")
print(plume_df_copy[["sector", "sector_name"]].drop_duplicates())

# -------------------------------
# FINAL CHECK
# -------------------------------
print("\nSector Distribution:")
print(plume_df_copy["sector_name"].value_counts())

plume_df_copy.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plume_df_copy = plume_df_copy.drop_duplicates(
    subset=["plume_id", "datetime"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(len(cm_pd))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#New composite primary key
plume_df_copy["plume_key"] = (
    plume_df_copy["plume_id"].astype(str) + "_" +
    plume_df_copy["datetime"].astype(str)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plume_df_copy.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.createDataFrame(plume_df_copy) \
    .write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("plume_data")

print("plume_data updated with sector info!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
