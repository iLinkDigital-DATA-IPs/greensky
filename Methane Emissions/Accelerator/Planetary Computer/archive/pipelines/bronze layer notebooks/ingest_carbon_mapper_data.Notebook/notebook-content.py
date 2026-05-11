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

# # Fetching Sector data from Carbon Mapper API 
# 
# ### Free Tier API. 
# 
# Note: Carbon Mapper APIs have outdated information, sometimes over 2 years old. 

# CELL ********************

#IMPORTS 
import requests
import json
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from pyspark.sql.functions import col 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === API Configuration ===
base_url = "https://api.carbonmapper.org/api/v1/"
endpoint = "catalog/plumes/annotated"

token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzc3OTczNTAwLCJpYXQiOjE3NzczNjg3MDAsImp0aSI6IjM0ZDI0NTc0NmQ1NjQzYzFiMmVhOGU5NTgzYTI3NzZhIiwic2NvcGUiOiJzdGFjIGNhdGFsb2c6cmVhZCIsImdyb3VwcyI6IlB1YmxpYyIsImFsbF9ncm91cF9uYW1lcyI6eyJjb21tb24iOlsiUHVibGljIl19LCJvcmdhbml6YXRpb25zIjoiIiwic2V0dGluZ3MiOnt9LCJpc19zdGFmZiI6ZmFsc2UsImlzX3N1cGVydXNlciI6ZmFsc2UsInVzZXJfaWQiOjMyMjQyfQ.9zjFbBl0c2nB3s7JXq1UQN8YR7b5rbC3lpDC5wX1kqk" 
# Fetch without sector filter, split bbox into sub-regions for denser coverage
bbox_regions = [
    [-110, 15, -102, 22],   # Western Mexico
    [-102, 15, -95, 22],    # Eastern Mexico  
    [-110, 22, -102, 30],   # Northwest — Permian Basin
    [-102, 22, -95, 30],    # Northeast — Texas/Gulf
]

all_records = []

params = {
    "limit": 1000,
    "offset": 0,
    "bbox": [-110, 15, -95, 30], #Permian Basin (Texas)
    "start_date": "2026-03-01T00:00:00Z",
    "end_date": "2026-04-08T23:59:59Z",
}

headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json"
}

#Fetch all records
for bbox in bbox_regions:
    page = 0
    while True:
        params = {
            "limit": 1000,
            "offset": page * 1000,
            "bbox": bbox,
            "start_date": "2024-01-01T00:00:00Z",   # wider date range
            "end_date": "2026-04-08T23:59:59Z",
            # removed "sector" filter — fetch all
        }
        response = requests.get(base_url + endpoint, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error on bbox {bbox}:", response.text)
            break
        data = response.json().get("items", [])
        if not data:
            break
        all_records.extend(data)
        print(f"bbox {bbox}, page {page}: {len(data)} records")
        if len(data) < 1000:
            break
        page += 1

# Deduplicate by plume_id in case bboxes overlap
seen = set()
all_records = [r for r in all_records if not (r["plume_id"] in seen or seen.add(r["plume_id"]))]
print(f"Total unique records: {len(all_records)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.getOrCreate()

json_rdd = spark.sparkContext.parallelize(
    [json.dumps(r) for r in all_records]
)

df = spark.read.json(json_rdd)

print(f"Records loaded: {df.count()}")
display(df.limit(5))
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Convert API data to pandas
cm_pd = pd.json_normalize(all_records)

print("Columns:", cm_pd.columns.tolist())
cm_pd.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Extract coordinates
# Extract from flattened column
def extract_coords(coords):
    try:
        if isinstance(coords, list) and len(coords) >= 2:
            return coords[0], coords[1]
    except:
        pass
    return None, None

cm_pd[["longitude", "latitude"]] = cm_pd["geometry_json.coordinates"].apply(
    lambda x: pd.Series(extract_coords(x))
)

# Convert to numeric
cm_pd["latitude"] = pd.to_numeric(cm_pd["latitude"], errors="coerce")
cm_pd["longitude"] = pd.to_numeric(cm_pd["longitude"], errors="coerce")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Load plume data
plume_df_copy = spark.sql("""
    SELECT * 
    FROM Planetary_computer_LH.dbo.methane_plumes_dbscan 
""")

plume_df_copy = plume_df_copy.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Clean plume data
# Datetime cleanup
plume_df_copy["datetime"] = pd.to_datetime(
    plume_df_copy["datetime"],
    errors="coerce"
).dt.tz_localize(None)

# Ensure plume_id
if "plume_id" not in plume_df_copy.columns:
    plume_df_copy["plume_id"] = plume_df_copy.index.astype(str)
else:
    plume_df_copy["plume_id"] = plume_df_copy["plume_id"].astype(str)

# Ensure numeric coords
plume_df_copy["latitude"] = pd.to_numeric(
    plume_df_copy["latitude"], errors="coerce"
)
plume_df_copy["longitude"] = pd.to_numeric(
    plume_df_copy["longitude"], errors="coerce"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use correct column from Carbon Mapper
cm_pd["datetime"] = pd.to_datetime(
    cm_pd["scene_timestamp"],
    errors="coerce"
).dt.tz_localize(None)

# Align to minute
plume_df_copy["datetime"] = plume_df_copy["datetime"].dt.floor("min")
cm_pd["datetime"] = cm_pd["datetime"].dt.floor("min")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Debug checks
print("Plume datetime range:", plume_df_copy["datetime"].min(), "→", plume_df_copy["datetime"].max())
print("CM datetime range:", cm_pd["datetime"].min(), "→", cm_pd["datetime"].max())

print("\nPlume lat/lon range:")
print(plume_df_copy[["latitude", "longitude"]].describe())

print("\nCM lat/lon range:")
print(cm_pd[["latitude", "longitude"]].describe())

print("\nCM sector distribution:")
print(cm_pd["sector"].value_counts(dropna=False).head())

print(cm_pd["sector"].value_counts(dropna=False))
print(cm_pd["sector"].unique())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Pre-clean CM data
cm_pd = cm_pd.copy()
cm_pd["sector"] = cm_pd["sector"].str.upper().str.strip()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Matching logic
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # km

    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    return 2 * R * np.arcsin(np.sqrt(a))


def match_sector_smart(row, cm_df):

    distances = haversine(
        row["latitude"], row["longitude"],
        cm_df["latitude"].values, cm_df["longitude"].values
    )

    cm_df_local = cm_df.copy()
    cm_df_local["distance_km"] = distances

    nearby = cm_df_local[cm_df_local["distance_km"] <= 500]

    if nearby.empty:
        return "UNKNOWN", None

    # Weight closer points more
    nearby["weight"] = 1 / (nearby["distance_km"] + 1)

    sector_scores = (
        nearby.groupby("sector")["weight"]
        .sum()
        .sort_values(ascending=False)
    )

    best_sector = sector_scores.index[0]
    best_distance = nearby["distance_km"].min()

    if best_distance <= 50:
        confidence = "high"
    elif best_distance <= 150:
        confidence = "medium"
    else:
        confidence = "low"

    return best_sector, confidence

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Apply matching — only for rows within CM spatial coverage
    
results = plume_df_copy.apply(
    lambda row: match_sector_smart(row, cm_pd),
    axis=1
)

plume_df_copy["sector"] = results.apply(lambda x: x[0])
plume_df_copy["match_confidence"] = results.apply(lambda x: x[1])

plume_df_copy["sector"] = plume_df_copy["sector"].fillna("UNKNOWN")
plume_df_copy["sector"] = plume_df_copy["sector"].str.upper().str.strip()


#DEBUG
print("\nFinal sector distribution:")
print(plume_df_copy["sector"].value_counts())

print("\nMatch confidence distribution:")
print(plume_df_copy["match_confidence"].value_counts(dropna=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plume_df_copy["sector"] = plume_df_copy["sector"].fillna("UNKNOWN")
plume_df_copy["sector"] = plume_df_copy["sector"].str.upper().str.strip()

# Add descriptions
sector_map = {
    "1A1": "Energy industries (power generation)",
    "1B1A": "Fugitive emissions from solid fuels",
    "1B2": "Fugitive emissions from oil and gas",
    "6A": "Solid waste disposal",
    "4B": "Manure management",
    "OTHER": "Other / unspecified",
    "UNKNOWN": "No matching sector found"
}

plume_df_copy["sector_description"] = (
    plume_df_copy["sector"]
    .map(sector_map)
    .fillna("Unmapped sector")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create composite key
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

#Convert back to spark 
spark_df = spark.createDataFrame(plume_df_copy)

spark_df = spark_df.dropDuplicates(["plume_key"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#cast cluster dtype
spark_df = spark_df.withColumn(
    "cluster",
    col("cluster").cast("int")
)

#Write to silver table
spark_df.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("dbo.plume_sector_mapped")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Planetary_computer_LH.dbo.plume_sector_mapped LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
