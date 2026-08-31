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
# META         },
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

# # Main Debug Notebook

# MARKDOWN ********************

# ## Pre-Flight Results -- August 2026
# - Fabric capacity SKU: F64
# - Lakehouse created: yes (greensky_lakehouse)
# - Spark version: 3.5.5 | 8 cores | 49.8 GB JVM memory
# 
# ### Network Egress
# - Planetary Computer STAC: PASS
#   - Collection: sentinel-5p-l2-netcdf
#   - CH4 data confirmed (asset key: "ch4", hosted on Azure Blob)
#   - Permian Basin BBOX returns CH4 items (5 items in June 2026 test)
#   - Signed URLs via planetary_computer.sign_inplace (no separate auth)
#   - Filter: s5p:product_name == "ch4" or "CH4" in item.id
#   - Earlier 404 was due to wrong collection name (sentinel-5p-l2-ch4 does not exist)
#   - Earlier empty results were due to wrong date range and wrong query filter value
# - Open-Meteo API: PASS (200, hourly resolution confirmed)
# - ERA5 CDS API: PASS (202, reachable -- credentials needed)
# - CDSE STAC: PASS (available as fallback, needs auth for download)
# 
# ### Dependencies
# - OneLake Delta write/read: PASS
# - xarray: PASS (2026.2.0)
# - netCDF4: PASS (1.7.4)
# - planetary-computer + pystac-client: PASS
# 
# ### Existing Assets
# - Existing table: bronze.planetary_comp_raw_data (445,446 distinct keys)
# - Existing notebook: ingest_planetary_computer_v2 (working, uses correct collection)
# 
# ### Environment File Changes
# - Added: netcdf4==1.7.4, cdsapi==0.7.5
# - Removed: delta-spark==4.1.0 (Fabric provides its own Delta runtime)
# 
# ### Action Items Before Day 1
# - [x] RESOLVED: Planetary Computer CH4 access confirmed
# - [ ] Set up CDS API credentials for ERA5 (register at cds.climate.copernicus.eu)
# - [ ] Update environment file (add netcdf4, cdsapi; remove delta-spark)
# - [ ] Pre-download CAMS and EPA validation CSVs to Files/validation/


# MARKDOWN ********************

# #### Resolved (BLOCKER)
# - Planetary Computer CH4 data confirmed available in collection sentinel-5p-l2-netcdf
# - Root cause: initial preflight used wrong collection name (sentinel-5p-l2-ch4) and wrong
#   query filter (s5p:product_name: "L2__CH4___" instead of "ch4")
# - CDSE STAC verified as working fallback (catalogue.dataspace.copernicus.eu) but not needed
# - No change to notebook 01 data source -- Planetary Computer remains the primary source

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

# CELL ********************

import requests

# Check what collections Planetary Computer actually has for Sentinel-5P
url = "https://planetarycomputer.microsoft.com/api/stac/v1/collections"
r = requests.get(url, timeout=30)

if r.status_code == 200:
    collections = r.json().get("collections", [])
    s5p_collections = [
        c for c in collections 
        if "sentinel-5p" in c.get("id", "").lower() 
        or "sentinel-5p" in c.get("title", "").lower()
        or "s5p" in c.get("id", "").lower()
        or "tropomi" in c.get("title", "").lower()
        or "ch4" in c.get("id", "").lower()
    ]
    
    if s5p_collections:
        print(f"Found {len(s5p_collections)} Sentinel-5P / CH4 collection(s):")
        for c in s5p_collections:
            print(f"  ID: {c['id']}")
            print(f"  Title: {c.get('title', 'N/A')}")
            print(f"  Description: {c.get('description', 'N/A')[:200]}")
            print()
    else:
        print("No Sentinel-5P collections found on Planetary Computer")
        print("Checking total collection count...")
        print(f"Total collections available: {len(collections)}")
        print()
        # Print all collection IDs for manual inspection
        all_ids = sorted([c["id"] for c in collections])
        print("All collection IDs:")
        for cid in all_ids:
            print(f"  {cid}")
else:
    print(f"Collections endpoint returned: {r.status_code}")

# Also test CDSE STAC directly (the actual Copernicus source)
print("\n--- Testing CDSE STAC API directly ---")
cdse_urls = [
    "https://catalogue.dataspace.copernicus.eu/stac/collections/SENTINEL-5P",
    "https://catalogue.dataspace.copernicus.eu/stac/collections",
]

for cdse_url in cdse_urls:
    try:
        r2 = requests.get(cdse_url, timeout=30)
        print(f"CDSE {cdse_url.split('/')[-1]}: {r2.status_code}")
        if r2.status_code == 200:
            data = r2.json()
            if "collections" in data:
                s5p = [c for c in data["collections"] if "5p" in c.get("id", "").lower() or "5p" in c.get("title", "").lower()]
                print(f"  S5P collections on CDSE: {len(s5p)}")
                for c in s5p[:10]:
                    print(f"    {c['id']}: {c.get('title', 'N/A')[:80]}")
            elif "id" in data:
                print(f"  Collection found: {data['id']}")
    except Exception as e:
        print(f"  CDSE error: {e}")

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

# Search for CH4 items in the unified collection
search = catalog.search(
    collections=["sentinel-5p-l2-netcdf"],
    bbox=[-105.0, 30.5, -101.0, 33.5],
    datetime="2024-01-01/2024-01-07",
    max_items=5,
    query={"s5p:product_name": {"eq": "L2__CH4___"}}
)

items = list(search.items())
if items:
    print(f"Found {len(items)} CH4 item(s)")
    for item in items:
        print(f"\n  ID: {item.id}")
        print(f"  Datetime: {item.datetime}")
        print(f"  Properties: {list(item.properties.keys())[:15]}")
        print(f"  Assets: {list(item.assets.keys())}")
        
        # Check the main asset for CH4 data
        for asset_key, asset in item.assets.items():
            if "ch4" in asset_key.lower() or asset_key == "data":
                print(f"  Asset '{asset_key}': {asset.href[:120]}...")
                print(f"  Media type: {asset.media_type}")
else:
    print("No CH4 items found with query filter")
    print("Trying without product filter...")
    
    search2 = catalog.search(
        collections=["sentinel-5p-l2-netcdf"],
        bbox=[-105.0, 30.5, -101.0, 33.5],
        datetime="2024-01-01/2024-01-07",
        max_items=10
    )
    items2 = list(search2.items())
    print(f"Found {len(items2)} total L2 item(s)")
    for item in items2[:5]:
        props = item.properties
        product = props.get("s5p:product_name", props.get("product_type", "unknown"))
        print(f"  {item.id} | product: {product} | {item.datetime}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

# The blog references these exact CDSE collection URLs:
# NRTI: https://browser.stac.dataspace.copernicus.eu/collections/sentinel-5p-l2-ch4-nrti
# OFFL: https://browser.stac.dataspace.copernicus.eu/collections/sentinel-5p-l2-ch4-offl

# Test the CDSE STAC browser endpoint
base_urls = [
    "https://browser.stac.dataspace.copernicus.eu/stac",
    "https://catalogue.dataspace.copernicus.eu/stac",
    "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0",
    "https://zipper.dataspace.copernicus.eu/odata/v1",
]

for base in base_urls:
    try:
        r = requests.get(base, timeout=30)
        print(f"{base.split('//')[1][:50]}: {r.status_code}")
    except Exception as e:
        print(f"{base.split('//')[1][:50]}: ERROR - {str(e)[:80]}")

print("\n--- Testing CDSE STAC for CH4 collections ---")

# Test the exact URLs from the blog
ch4_urls = [
    "https://browser.stac.dataspace.copernicus.eu/stac/collections/sentinel-5p-l2-ch4-nrti",
    "https://browser.stac.dataspace.copernicus.eu/stac/collections/sentinel-5p-l2-ch4-offl",
    "https://catalogue.dataspace.copernicus.eu/stac/collections/sentinel-5p-l2-ch4-nrti",
    "https://catalogue.dataspace.copernicus.eu/stac/collections/sentinel-5p-l2-ch4-offl",
]

for url in ch4_urls:
    try:
        r = requests.get(url, timeout=30)
        short = url.replace("https://", "").replace("stac/collections/", "")
        print(f"{short}: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            print(f"  Title: {data.get('title', 'N/A')}")
            print(f"  ID: {data.get('id', 'N/A')}")
            extent = data.get("extent", {}).get("temporal", {}).get("interval", [])
            if extent:
                print(f"  Temporal: {extent[0]}")
    except Exception as e:
        print(f"  ERROR: {str(e)[:80]}")

print("\n--- Testing CDSE STAC search for CH4 items ---")

# Try searching for actual CH4 items
search_endpoints = [
    "https://browser.stac.dataspace.copernicus.eu/stac/search",
    "https://catalogue.dataspace.copernicus.eu/stac/search",
]

search_body = {
    "collections": ["sentinel-5p-l2-ch4-offl"],
    "bbox": [-105.0, 30.5, -101.0, 33.5],
    "datetime": "2024-01-01T00:00:00Z/2024-01-07T23:59:59Z",
    "limit": 3
}

for endpoint in search_endpoints:
    try:
        r = requests.post(endpoint, json=search_body, timeout=30)
        short = endpoint.replace("https://", "").split("/")[0]
        print(f"{short} POST search: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            features = data.get("features", [])
            print(f"  Items found: {len(features)}")
            for f in features[:3]:
                print(f"  {f.get('id', 'N/A')} | {f.get('properties', {}).get('datetime', 'N/A')}")
                print(f"  Assets: {list(f.get('assets', {}).keys())[:5]}")
        elif r.status_code != 404:
            print(f"  Response: {r.text[:200]}")
    except Exception as e:
        print(f"  ERROR: {str(e)[:80]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

# First, get the actual asset URL from one of the items we found
search_body = {
    "collections": ["sentinel-5p-l2-ch4-offl"],
    "bbox": [-105.0, 30.5, -101.0, 33.5],
    "datetime": "2024-01-01T00:00:00Z/2024-01-07T23:59:59Z",
    "limit": 1
}

r = requests.post(
    "https://catalogue.dataspace.copernicus.eu/stac/search",
    json=search_body,
    timeout=30
)

data = r.json()
item = data["features"][0]
print(f"Item: {item['id']}")
print(f"Datetime: {item['properties']['datetime']}")

# Get the NetCDF asset URL
netcdf_asset = item["assets"].get("netcdf", {})
asset_url = netcdf_asset.get("href", "No href found")
print(f"Asset URL: {asset_url}")
print(f"Asset type: {netcdf_asset.get('type', 'N/A')}")

# Test if we can download without auth (just the first 1KB)
print("\n--- Testing download without authentication ---")
try:
    r2 = requests.get(asset_url, timeout=30, stream=True, headers={"Range": "bytes=0-1023"})
    print(f"Download status: {r2.status_code}")
    if r2.status_code in [200, 206]:
        print("PASS: Data accessible without authentication")
        print(f"Content-Type: {r2.headers.get('Content-Type', 'N/A')}")
        print(f"Content-Length: {r2.headers.get('Content-Length', 'N/A')}")
    elif r2.status_code in [401, 403]:
        print("AUTH REQUIRED: Need CDSE access token to download")
        print("Register at: https://dataspace.copernicus.eu")
        print("Then generate token via identity.dataspace.copernicus.eu")
    else:
        print(f"Unexpected status: {r2.status_code}")
        print(f"Response: {r2.text[:300]}")
except Exception as e:
    print(f"Download error: {e}")

# Also check what other properties/metadata the item has
print("\n--- Item properties ---")
props = item.get("properties", {})
useful_keys = [
    "datetime", "start_datetime", "end_datetime",
    "instruments", "platform", "constellation",
    "processing:level", "s5p:product_name", "s5p:product_type"
]
for k in useful_keys:
    if k in props:
        print(f"  {k}: {props[k]}")

# Print all property keys for reference
print(f"\n  All property keys: {sorted(props.keys())}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

# The item we found
item_id = "S5P_OFFL_L2__CH4____20240103T070658_20240103T084829_32247_03_020600_20240104T231949"
s3_path = "s3://eodata/Sentinel-5P/TROPOMI/L2__CH4___/2024/01/03/S5P_OFFL_L2__CH4____20240103T070658_20240103T084829_32247_03_020600_20240104T231949.nc"

# Approach 1: CDSE OData download (zipper endpoint)
# Construct the OData product URL from the STAC item
print("--- Approach 1: OData / Zipper download ---")
odata_search = (
    "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    f"?$filter=Name eq '{item_id}'"
    "&$top=1"
)
try:
    r = requests.get(odata_search, timeout=30)
    print(f"OData search: {r.status_code}")
    if r.status_code == 200:
        products = r.json().get("value", [])
        if products:
            product_id = products[0]["Id"]
            product_name = products[0]["Name"]
            print(f"  Product ID: {product_id}")
            print(f"  Product Name: {product_name}")
            
            # This is the download URL (needs auth token)
            download_url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
            print(f"  Download URL: {download_url}")
            
            # Test without auth
            r2 = requests.get(download_url, timeout=30, stream=True, allow_redirects=False)
            print(f"  Download status (no auth): {r2.status_code}")
            if r2.status_code == 401:
                print("  AUTH REQUIRED (expected)")
            elif r2.status_code in [301, 302]:
                print(f"  Redirect to: {r2.headers.get('Location', 'N/A')[:120]}")
        else:
            print("  No products found via OData")
except Exception as e:
    print(f"  OData error: {e}")

# Approach 2: CDSE S3 access with boto3
print("\n--- Approach 2: S3 access ---")
try:
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    
    # Test anonymous S3 access to eodata
    s3 = boto3.client(
        "s3",
        endpoint_url="https://eodata.dataspace.copernicus.eu",
        config=Config(signature_version=UNSIGNED)
    )
    
    bucket = "eodata"
    key = s3_path.replace("s3://eodata/", "")
    
    # Try to get just the file metadata (HEAD request)
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        print(f"  S3 HEAD: success")
        print(f"  File size: {head['ContentLength'] / (1024*1024):.1f} MB")
        print(f"  Content type: {head.get('ContentType', 'N/A')}")
        print("  PASS: Anonymous S3 access works")
    except s3.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        print(f"  S3 HEAD error: {code}")
        if code in ["403", "AccessDenied"]:
            print("  AUTH REQUIRED for S3 access")
        elif code == "404":
            print("  File not found at this path")
            
except ImportError:
    print("  boto3 not available")
    print("  Install with: %pip install boto3")
except Exception as e:
    print(f"  S3 error: {e}")

# Approach 3: Check CDSE token endpoint accessibility
print("\n--- CDSE Token Endpoint ---")
token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
try:
    r3 = requests.post(token_url, data={
        "grant_type": "client_credentials",
        "client_id": "cdse-public",
        "client_secret": "not-a-real-secret"
    }, timeout=30)
    print(f"Token endpoint: {r3.status_code}")
    if r3.status_code in [400, 401]:
        print("  PASS: Token endpoint reachable (auth failed as expected with dummy creds)")
        print("  Register at: https://dataspace.copernicus.eu")
        print("  Then use your username/password to get tokens")
    elif r3.status_code == 200:
        print("  Unexpected success with dummy creds")
except Exception as e:
    print(f"  Token endpoint error: {e}")

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

# Use a date range we KNOW works from your existing table (June 2026)
search = catalog.search(
    collections=["sentinel-5p-l2-netcdf"],
    datetime="2026-06-10/2026-06-16",
    max_items=20
)

items = list(search.items())
print(f"Total items found: {len(items)}")

# Separate by product type
ch4_items = [i for i in items if "CH4" in i.id]
other_items = [i for i in items if "CH4" not in i.id]

print(f"CH4 items: {len(ch4_items)}")
print(f"Other items: {len(other_items)}")

if ch4_items:
    print("\nCH4 items found:")
    for item in ch4_items[:5]:
        print(f"  {item.id}")
        print(f"  Datetime: {item.datetime}")
        print(f"  Assets: {list(item.assets.keys())}")
        # Check how to access the data
        for ak, av in item.assets.items():
            print(f"    {ak}: {av.href[:100]}...")
            print(f"    type: {av.media_type}")
    
    # Also check — what properties does the item have?
    print(f"\n  All properties of first CH4 item:")
    for k, v in sorted(ch4_items[0].properties.items()):
        print(f"    {k}: {v}")
else:
    print("\nNo CH4 items in this date range either.")
    print("Checking what products ARE available:")
    products = set()
    for item in items:
        # Try to identify product type from item ID
        parts = item.id.split("_")
        for p in parts:
            if p in ["CH4", "CO", "NO2", "O3", "SO2", "HCHO", "AER"]:
                products.add(p)
    print(f"  Products found: {products}")

# Now also try Permian Basin BBOX with broader date range
print("\n--- Permian Basin search (2026-06-01 to 2026-06-30) ---")
search2 = catalog.search(
    collections=["sentinel-5p-l2-netcdf"],
    bbox=[-105.0, 30.5, -101.0, 33.5],
    datetime="2026-06-01/2026-06-30",
    max_items=50
)

items2 = list(search2.items())
ch4_items2 = [i for i in items2 if "CH4" in i.id]
print(f"Total items in Permian BBOX: {len(items2)}")
print(f"CH4 items in Permian BBOX: {len(ch4_items2)}")

if ch4_items2:
    for item in ch4_items2[:3]:
        print(f"  {item.id} | {item.datetime}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
   
cds_key = "cdfeae22-35a3-473c-b462-cb9cdf5bf690"  # replace with your actual key

os.makedirs(os.path.expanduser("~/.cdsapi"), exist_ok=True)
with open(os.path.expanduser("~/.cdsapirc"), "w") as f:
    f.write(f"url: https://cds.climate.copernicus.eu/api\n")
    f.write(f"key: {cds_key}\n")
   
# Verify
with open(os.path.expanduser("~/.cdsapirc"), "r") as f:
    print(f.read())
print("CDS credentials saved")
   
# Test connection
import cdsapi
client = cdsapi.Client()
print("CDS client initialized successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, min as spark_min, max as spark_max, count, countDistinct

TABLE_NAME = "Planetary_computer_LH.bronze.planetary_comp_raw_data"
df = spark.table(TABLE_NAME)

print("=== Schema ===")
df.printSchema()

print("\n=== Row Count ===")
print(f"Total rows: {df.count():,}")

print("\n=== Date Range ===")
df.select(
    spark_min("datetime").alias("earliest"),
    spark_max("datetime").alias("latest")
).show(truncate=False)

print("\n=== Geographic Extent ===")
df.select(
    spark_min("latitude").alias("min_lat"),
    spark_max("latitude").alias("max_lat"),
    spark_min("longitude").alias("min_lon"),
    spark_max("longitude").alias("max_lon")
).show()

print("\n=== Permian Basin Coverage ===")
permian = df.filter(
    (col("latitude") >= 30.5) & (col("latitude") <= 33.5) &
    (col("longitude") >= -105.0) & (col("longitude") <= -101.0)
)
print(f"Rows in Permian BBOX: {permian.count():,}")

print("\n=== QA Value Distribution ===")
df.select("qa_value").summary("min", "25%", "50%", "75%", "max").show()

print("\n=== Distinct STAC IDs ===")
df.select(countDistinct("stac_id").alias("distinct_stac_ids")).show()

print("\n=== Sample Rows (Permian Basin) ===")
permian.select(
    "latitude", "longitude", "ch4", "qa_value", "datetime", "stac_id"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
