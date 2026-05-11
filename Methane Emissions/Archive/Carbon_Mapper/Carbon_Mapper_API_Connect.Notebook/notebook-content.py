# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e0efee7f-4d05-4685-b178-768ed7635e44",
# META       "default_lakehouse_name": "GreenSky_LH",
# META       "default_lakehouse_workspace_id": "060ba34b-f1a3-4509-a6e2-36d1e736a8eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "e0efee7f-4d05-4685-b178-768ed7635e44"
# META         },
# META         {
# META           "id": "427f0431-b084-4858-82dd-1bfa55380658"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Carbon Mapper: Bronze Table Loading from URL
# 
# This notebook import data from  the `CarbonMapper URL`  to a Bronze layer
# - **Import raw tables from the URL**

# CELL ********************

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit
from pyspark.sql import Row


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === API Configuration ===
base_url = "https://api.carbonmapper.org/api/v1/"
endpoint = "catalog/plumes/annotated"
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzY4NTQwNzA5LCJpYXQiOjE3Njc5MzU5MDksImp0aSI6IjExNmViYjZhODBiNTQxYmU4ZTdkMzAxODM0NGZkZGExIiwic2NvcGUiOiJzdGFjIGNhdGFsb2c6cmVhZCIsImdyb3VwcyI6IlB1YmxpYyIsImFsbF9ncm91cF9uYW1lcyI6eyJjb21tb24iOlsiUHVibGljIl19LCJvcmdhbml6YXRpb25zIjoiIiwic2V0dGluZ3MiOnt9LCJpc19zdGFmZiI6ZmFsc2UsImlzX3N1cGVydXNlciI6ZmFsc2UsInVzZXJfaWQiOjI0MDI2fQ.8uMVIAcxoOO1OAtHVuMDP_4PVSri1SzjHNmXv2x6y0w"  # masked

# Query parameters (you can tweak limits, bbox, etc.)
params = {
    "limit": 1000,
    "offset": 0,
    "bbox": [-125, 24, -66, 49],  # pass as list, not string
    "start_date": "2025-12-09T00:00:00Z",
    "end_date": "2025-12-12T23:59:59Z",
    "sector": "1B2"
}

# Headers (Bearer Token)
headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json"
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# Convert all records to JSON strings
json_rdd = spark.sparkContext.parallelize([json.dumps(r) for r in all_records])

# Let Spark infer schema properly
df = spark.read.json(json_rdd)

print(f"Records loaded: {df.count()}")
print(f"Columns: {len(df.columns)}")
display(df.limit(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(df.write
    .format("delta")
    .mode("append")       # or "append" for incremental loads
    .option("overwriteSchema", "true")  # useful if schema evolved
    .saveAsTable("bronze.bronze_CarbonMapper"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM GreenSky_Lakehouse.methane_intelligence.carbon_mapper_plumes LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
