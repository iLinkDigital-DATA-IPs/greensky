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

# # Main Debug Notebook

# CELL ********************

import requests

url = "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-5p-l2-ch4"
try:
    r = requests.get(url, timeout=30)
    print(f"Planetary Computer STAC: {r.status_code}")
    if r.status_code == 200:
        print(" STAC API accessible")
        print(f"Collection title: {r.json().get('title', 'N/A')}")
    else:
        print(" STAC API returned non-200")
except Exception as e:
    print(f" STAC API unreachable: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import planetary_computer
import pystac_client

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace
)

# Search for 1 item in the Permian Basin
search = catalog.search(
    collections=["sentinel-5p-l2-ch4"],
    bbox=[-105.0, 30.5, -101.0, 33.5],
    datetime="2024-01-01/2024-01-07",
    max_items=1
)

items = list(search.items())
if items:
    print(f" Found {len(items)} item(s)")
    print(f"First item: {items[0].id}")
    print(f"Assets: {list(items[0].assets.keys())}")
else:
    print(" No items found — try a wider date range")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

url = (
    "https://archive-api.open-meteo.com/v1/archive"
    "?latitude=31.9&longitude=-103.7"
    "&start_date=2024-01-01&end_date=2024-01-02"
    "&hourly=wind_speed_10m,wind_direction_10m,temperature_2m,"
    "relative_humidity_2m,surface_pressure"
)
try:
    r = requests.get(url, timeout=30)
    print(f"Open-Meteo: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        hourly = data.get("hourly", {})
        print(f" Open-Meteo accessible")
        print(f"Hourly variables returned: {list(hourly.keys())}")
        print(f"First wind speed: {hourly.get('wind_speed_10m', [None])[0]} m/s")
    else:
        print(f" Open-Meteo returned {r.status_code}")
except Exception as e:
    print(f" Open-Meteo unreachable: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

# Test if CDS API is reachable
try:
    r = requests.get("https://cds.climate.copernicus.eu/api", timeout=30)
    print(f"CDS API: {r.status_code}")
    if r.status_code in [200, 401, 403]:
        print(" CDS API is reachable from Fabric")
        print("   (401/403 just means you need API credentials — that's fine)")
        print("   Next: register at https://cds.climate.copernicus.eu and get your API key")
    else:
        print(f" CDS returned {r.status_code}")
except requests.exceptions.ConnectionError:
    print(" CDS API unreachable — network egress blocked")
    print("   FALLBACK: You'll need to download ERA5 locally and upload to OneLake")
    print("   See Step 6 below for instructions")
except Exception as e:
    print(f" CDS API error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType
from datetime import datetime

# Create a tiny test DataFrame
schema = StructType([
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("ch4", DoubleType()),
    StructField("qa_value", DoubleType()),
    StructField("datetime", TimestampType()),
    StructField("stac_id", StringType()),
])

test_data = [(31.9, -103.7, 1890.5, 0.7, datetime(2024, 1, 1, 12, 0, 0), "test_001")]
df = spark.createDataFrame(test_data, schema)

# Write to lakehouse
df.write.format("delta").mode("overwrite").saveAsTable("test_write_check")

# Read back
result = spark.sql("SELECT * FROM test_write_check")
result.show()
print(f" OneLake Delta write/read works — {result.count()} row(s)")

# Clean up
spark.sql("DROP TABLE IF EXISTS test_write_check")
print(" Cleanup done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    import xarray as xr
    print(f" xarray version: {xr.__version__}")
except ImportError:
    print(" xarray not available — installing")
    %pip install xarray netcdf4 h5netcdf
    import xarray as xr
    print(f" xarray installed: {xr.__version__}")

try:
    import netCDF4
    print(f" netCDF4 version: {netCDF4.__version__}")
except ImportError:
    print(" netCDF4 not available — may need h5netcdf as fallback")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sc = spark.sparkContext
print(f"Spark version: {sc.version}")
print(f"App name: {sc.appName}")
print(f"Master: {sc.master}")
print(f"Default parallelism: {sc.defaultParallelism}")

# Check memory
java_import = sc._jvm.java.lang.Runtime.getRuntime()
max_mem = java_import.maxMemory() / (1024**3)
print(f"Max JVM memory: {max_mem:.1f} GB")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
