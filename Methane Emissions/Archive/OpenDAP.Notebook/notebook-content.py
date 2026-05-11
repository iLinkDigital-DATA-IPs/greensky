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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================
# CELL 1: Install Required Libraries
# ============================================
%pip install earthaccess xarray netCDF4 pydap requests


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ============================================
# CELL 2: Import Libraries
# ============================================
import earthaccess
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
import os
import json

print("✅ All libraries imported successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# CELL 3: Set Credentials (Use Key Vault in Production)
# ============================================
# Store these in Azure Key Vault for production
EARTHDATA_USERNAME = "dharun_karthick_2223"
EARTHDATA_PASSWORD = "sdharunk@2003A"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# For testing connectivity
print("⚙️ Credentials configured")

# ============================================
# CELL 4: Test Internet Connectivity
# ============================================
def test_connectivity():
    try:
        response = requests.get("https://www.google.com", timeout=5)
        print("✅ Internet connectivity: OK")
        return True
    except Exception as e:
        print(f"❌ Internet connectivity failed: {e}")
        return False

test_connectivity()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# ============================================
# CELL 5: Test NASA Earthdata Login
# ============================================
def test_earthdata_auth():
    try:
        # Test authentication
        auth = earthaccess.login(strategy="interactive")
        # Alternative: Use environment variables
        # os.environ['EARTHDATA_USERNAME'] = EARTHDATA_USERNAME
        # os.environ['EARTHDATA_PASSWORD'] = EARTHDATA_PASSWORD
        # auth = earthaccess.login()
        
        if auth.authenticated:
            print("✅ NASA Earthdata authentication: SUCCESS")
            return True
        else:
            print("❌ NASA Earthdata authentication: FAILED")
            return False
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return False

# Authenticate
test_earthdata_auth()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# CELL 6: Test GES DISC API Connectivity
# ============================================
def test_gesdisc_api():
    try:
        # Test GES DISC OPeNDAP endpoint
        test_url = "https://tropomi.gesdisc.eosdis.nasa.gov/opendap/S5P_TROPOMI_Level2/S5P_L2__CH4____HiR.2/2025/contents.html"
        
        response = requests.get(test_url, timeout=10)
        
        if response.status_code == 200:
            print("✅ GES DISC OPeNDAP service: ACCESSIBLE")
            return True
        else:
            print(f"⚠️ GES DISC returned status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ GES DISC connectivity error: {e}")
        return False

test_gesdisc_api()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ============================================
# CELL 7: Search for Available Data Files
# ============================================
# Define date range
target_date = datetime.now() - timedelta(days=1)  # Yesterday
date_start = target_date.strftime("%Y-%m-%d")
date_end = target_date.strftime("%Y-%m-%d")

print(f"🔍 Searching for data from: {date_start} to {date_end}")

try:
    # Search for TROPOMI CH4 data
    results = earthaccess.search_data(
        short_name="S5P_L2__CH4____HiR",
        temporal=(date_start, date_end),
        count=10  # Limit for testing
    )
    
    print(f"✅ Found {len(results)} granules")
    
    # Display first 3 results
    for i, granule in enumerate(results[:3]):
        print(f"\n📄 Granule {i+1}:")
        print(f"   Name: {granule['umm']['GranuleUR']}")
        print(f"   Size: {granule['umm']['DataGranule']['ArchiveAndDistributionInformation'][0]['Size']} MB")
        
except Exception as e:
    print(f"❌ Search failed: {e}")
    results = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/S5P_OFFL_L2__CH4____20251129T011651_20251129T025820_42118_03_020901_20251130T213125.nc.csv")
# df now is a Spark DataFrame containing CSV data from "Files/S5P_OFFL_L2__CH4____20251129T011651_20251129T025820_42118_03_020901_20251130T213125.nc.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 1 - Spark session (Databricks / Spark already provides `spark`)
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, ArrayType, StringType

# Adjust as needed
csv_path = "Files/S5P_OFFL_L2__CH4____20251129T011651_20251129T025820_42118_03_020901_20251130T213125.nc.csv"
output_path = "/mnt/data/tropomi_reconstructed"   # change to your lakehouse destination (parquet/delta)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2 - read file as raw lines
raw_df = spark.read.text(csv_path).withColumnRenamed("value", "line")

# Filter likely metadata/noise lines — keep lines that start with a slash '/'
# (most variable lines begin like '/PRODUCT_latitude[...], ...')
lines_df = raw_df.filter(F.col("line").rlike(r'^\s*/')).cache()

print("Total raw lines:", raw_df.count())
print("Kept lines (likely variables):", lines_df.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3 - split into var_with_indices and the remaining values string
# var_with_indices example: /PRODUCT_latitude[340][40]
# values_str example: " 12.998, 12.999, ..."

df = lines_df.withColumn(
    "var_with_indices",
    F.expr("trim(split(line,',')[0])")
).withColumn(
    "values_str",
    # remove the leading token+comma and any leading spaces
    F.expr("trim(regexp_replace(line, '^[^,]+,\\s*', ''))")
).select("line", "var_with_indices", "values_str")

df.show(5, truncate=120)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4 - extract base variable and indices using regex
# base_var e.g. /PRODUCT_latitude  -> we'll also remove leading slash for nicer names
df2 = df.withColumn(
    "base_var_raw",
    F.regexp_replace(F.col("var_with_indices"), r"\[.*", "")  # remove brackets and everything after
).withColumn(
    "base_var",
    F.regexp_replace(F.col("base_var_raw"), r"^/+", "")  # remove leading slash(es)
).withColumn(
    "idx1",
    F.regexp_extract(F.col("var_with_indices"), r"\[(\d+)\]", 1)  # first index if present
).withColumn(
    "idx2",
    F.regexp_extract(F.col("var_with_indices"), r"\[(\d+)\].*\[(\d+)\]", 2)  # second index if present
)

# normalize empty strings to nulls for indices
df2 = df2.withColumn("idx1", F.when(F.length(F.col("idx1")) == 0, None).otherwise(F.col("idx1").cast(IntegerType())))
df2 = df2.withColumn("idx2", F.when(F.length(F.col("idx2")) == 0, None).otherwise(F.col("idx2").cast(IntegerType())))

df2.select("var_with_indices", "base_var", "idx1", "idx2", "values_str").show(10, truncate=120)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5 - split values string into array; handle possible trailing commas or spaces
# split on comma, and trim spaces around values
split_expr = "transform(split(values_str,','), x -> trim(x))"

df3 = df2.withColumn("values_array", F.expr(split_expr))

# explode with position (pos starts at 0); we will use pos as the "col_index" for each row
df_exploded = df3.select(
    "base_var", "idx1", "idx2",
    F.posexplode("values_array").alias("col_pos", "raw_value")
).withColumn("raw_value_trim", F.trim(F.col("raw_value")))

# Cast numeric values where possible (safe cast)
df_exploded = df_exploded.withColumn("value_double", F.when(F.col("raw_value_trim") == "", None).otherwise(F.col("raw_value_trim").cast(DoubleType())))

df_exploded.show(10, truncate=120)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6 - keep the variables of interest - adjust names if your CSV uses slightly different names
# Common base names we expect: PRODUCT_latitude, PRODUCT_longitude, PRODUCT_methane_mixing_ratio, PRODUCT_methane_mixing_ratio_bias_corrected, PRODUCT_qa_value

# Long list of possible variable names - adjust if your file uses different paths
interesting_vars = [
    "PRODUCT_latitude",
    "PRODUCT_longitude",
    "PRODUCT_methane_mixing_ratio",
    "PRODUCT_methane_mixing_ratio_bias_corrected",
    "PRODUCT_methane_mixing_ratio_precision",
    "PRODUCT_qa_value",
    "PRODUCT_time_utc"
]

df_filtered = df_exploded.filter(F.col("base_var").isin(interesting_vars))

# We need a join key that uniquely identifies a pixel. The CSV layout appears to flatten arrays line-by-line:
# - idx1 likely represents the row index (swath line)
# - col_pos represents the across-track column index for that row
# For some 1D arrays idx2 may be null; this approach handles both.

# Create a consistent key
df_filtered = df_filtered.withColumn(
    "row_index", F.coalesce(F.col("idx1"), F.lit(-1)).cast(IntegerType())
).withColumn(
    "col_index", F.col("col_pos").cast(IntegerType())
).withColumn(
    "var_name", F.col("base_var")
)

df_filtered.select("var_name", "row_index", "col_index", "value_double").show(20, truncate=120)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 7 - pivot so each pixel key has lat, lon, xch4, qa in columns
# First reduce to the variables' relevant columns
vars_df = df_filtered.select("var_name", "row_index", "col_index", "value_double")

# Create a composite string key for easier grouping
vars_df = vars_df.withColumn("pixel_key", F.concat_ws("_", F.col("row_index"), F.col("col_index")))

# Pivot: group by pixel_key and pivot on var_name
pivot_df = vars_df.groupBy("pixel_key", "row_index", "col_index").pivot("var_name").agg(F.first("value_double"))

# Rename columns to readable names (if they exist)
cols_rename = {
    "PRODUCT_latitude": "latitude",
    "PRODUCT_longitude": "longitude",
    "PRODUCT_methane_mixing_ratio_bias_corrected": "xch4_bias_corrected",
    "PRODUCT_methane_mixing_ratio": "xch4_raw",
    "PRODUCT_methane_mixing_ratio_precision": "xch4_precision",
    "PRODUCT_qa_value": "qa_value"
}

for old, new in cols_rename.items():
    if old in pivot_df.columns:
        pivot_df = pivot_df.withColumnRenamed(old, new)

# Show sample
pivot_df.select("pixel_key", "row_index", "col_index", "latitude", "longitude","PRODUCT_time_utc", "qa_value").show(20, truncate=120)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# -------------------------------
# CONFIG - update paths as needed
# -------------------------------
csv_path = "Files/S5P_OFFL_L2__CH4____20251129T011651_20251129T025820_42118_03_020901_20251130T213125.nc.csv"
output_path = "/lakehouse/default/Files/tropomi_clean"  # <-- output Delta/Parquet path
# -------------------------------

# 1) Read raw file as lines
raw = spark.read.text(csv_path).withColumnRenamed("value", "line")

# 2) Keep only lines that look like variables (start with '/')
vars_only = raw.filter(F.col("line").rlike(r'^\s*/'))

# 3) Extract var token and the rest of the line (values string)
#    We use split to grab the first token (var_with_indices) and substring to get values.
df = vars_only.withColumn("var_token", F.expr("trim(split(line,',')[0])")) \
              .withColumn("values_str", F.expr("trim(substring(line, length(var_token)+2, 1000000))"))

# 4) Normalize variable name: remove bracket indices and leading '/PRODUCT_' prefix (if present)
df = df.withColumn("base_var", F.regexp_replace(F.col("var_token"), r"\[.*", "")) \
       .withColumn("base_var", F.regexp_replace(F.col("base_var"), r"^/+", "")) \
       .withColumn("base_var", F.regexp_replace(F.col("base_var"), r"^PRODUCT_", ""))

# 5) Split values into an array and trim each element
#    transform(...) requires Spark 2.4+ ; alternative would be explode(split(...)) with trimming.
df = df.withColumn("values_array", F.expr("transform(split(values_str, ','), x -> trim(x))"))

# 6) Keep only the variables we need
needed = ["latitude", "longitude", "qa_value", "time_utc"]
df = df.filter(F.col("base_var").isin(needed))

# 7) Explode arrays into rows with position using posexplode in select (correct aliasing)
#    Use select + posexplode to produce two columns: pos and val
exploded = df.select("base_var", F.expr("posexplode(values_array) as (pos, val)"))

# 8) Clean values: convert empty strings to null, cast numeric where appropriate
exploded = exploded.withColumn("val_trim", F.when(F.col("val") == "", None).otherwise(F.col("val")))

# Cast latitude/longitude/qa_value to double; keep time_utc as string for parsing later
exploded = exploded.withColumn(
    "value_cast",
    F.when(F.col("base_var") == "time_utc", F.col("val_trim"))
     .otherwise(F.col("val_trim").cast(DoubleType()))
)

# 9) Pivot to wide format: each pos becomes one pixel, columns are base_var names
pivoted = exploded.groupBy("pos").pivot("base_var").agg(F.first("value_cast"))

# 10) Convert time_utc string to timestamp (TROPOMI format: 2025-11-26T01:22:31Z)
#     If time_utc is not present or has different exact format, adjust the pattern.
if "time_utc" in pivoted.columns:
    pivoted = pivoted.withColumn("time_utc_ts", F.to_timestamp(F.col("time_utc"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    # fallback if to_timestamp produced nulls: try alternative parse (optional)
    pivoted = pivoted.withColumn("time_utc_final", F.coalesce(F.col("time_utc_ts"), F.col("time_utc")))
else:
    pivoted = pivoted.withColumn("time_utc_final", F.lit(None))

# 11) Select final columns, rename where required
final = pivoted.select(
    F.col("time_utc_final").alias("time_utc"),
    F.col("latitude").cast(DoubleType()),
    F.col("longitude").cast(DoubleType()),
    F.col("qa_value").cast(DoubleType())
)

# 12) Optional: filter out rows where lat/lon are null (bad pixels)
final = final.filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())

# 13) Save result as Delta (or parquet). Use mode as needed (overwrite/append)
#final.write.mode("overwrite").format("delta").save(output_path)

print("Saved reconstructed table to:", output_path)
final.show(20, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final = final.filter(F.col("qa_value") != 0)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# -00--------------------------------------------------------------------------------------------------------------------------------------------------------


# CELL ********************

# ============================================
# CELL 1: Install Required Libraries
# ============================================
%pip install requests pandas openpyxl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# CELL 2: Import Libraries
# ============================================
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import io
from datetime import datetime, timedelta
import re
import json

print("✅ Libraries imported successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# CELL 3: NASA Earthdata Credentials
# ============================================
# Store in Azure Key Vault for production
EARTHDATA_USERNAME = "dharun_karthick_2223"
EARTHDATA_PASSWORD = "sdharunk@2003A"

USE_NETRC = False  # Set to True if using .netrc file

import os
from pathlib import Path

def create_netrc_file(username, password):
    """
    Create .netrc file for NASA Earthdata authentication
    This is the NASA-recommended method
    """
    netrc_path = Path.home() / '.netrc'
    
    netrc_content = f"""machine urs.earthdata.nasa.gov
    login {username}
    password {password}
"""
    
    try:
        with open(netrc_path, 'w') as f:
            f.write(netrc_content)
        
        # Set proper permissions (important for security)
        os.chmod(netrc_path, 0o600)
        
        print(f"✅ .netrc file created at {netrc_path}")
        return True
    except Exception as e:
        print(f"❌ Could not create .netrc file: {e}")
        return False

# Optionally create .netrc file
# Uncomment the line below to create .netrc file
# create_netrc_file(EARTHDATA_USERNAME, EARTHDATA_PASSWORD)

print("⚙️ Credentials configured")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# CELL 4: Setup NASA Earthdata Session with URS Authentication
# ============================================
def setup_earthdata_session(username, password):
    """
    Create authenticated session for NASA Earthdata
    Handles URS (Unified Resource Services) authentication
    """
    from requests import Session
    from requests.auth import HTTPBasicAuth
    
    session = Session()
    session.auth = (username, password)
    
    # Set up session to handle redirects and cookies
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    return session

def test_opendap_connection(username, password):
    """Test connection to TROPOMI OPeNDAP service"""
    try:
        # Create authenticated session
        session = setup_earthdata_session(username, password)
        
        # Test URL - small data request
        test_url = "https://tropomi.gesdisc.eosdis.nasa.gov/opendap/S5P_TROPOMI_Level2/S5P_L2__CH4____HiR.2/2025/330/S5P_OFFL_L2__CH4____20251126T003230_20251126T021400_42075_03_020901_20251128T232631.nc.dap.csv?dap4.ce=/PRODUCT_qa_value[0:1:0][0:1:10][0:1:10]"
        
        # Make request with session
        response = session.get(test_url, timeout=30, allow_redirects=True)
        
        if response.status_code == 200:
            print("✅ OPeNDAP authentication: SUCCESS")
            print(f"   Response size: {len(response.content)} bytes")
            print(f"   First 200 chars: {response.text[:200]}")
            return True
        elif response.status_code == 401:
            print("❌ Authentication failed: Invalid credentials")
            print("   Please verify your NASA Earthdata username and password")
            return False
        elif response.status_code == 403:
            print("❌ Access forbidden: You may need to approve the GES DISC application")
            print("   Visit: https://urs.earthdata.nasa.gov/users/YOUR_USERNAME/authorized_apps")
            return False
        else:
            print(f"⚠️ Unexpected status code: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

# Run test
test_opendap_connection(EARTHDATA_USERNAME, EARTHDATA_PASSWORD)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 1 — params / URL
url = "https://tropess.gesdisc.eosdis.nasa.gov/opendap/TROPESS_Summary/TRPSYL2CH4AIRSFS.1/2025/TROPESS_AIRS-Aqua_L2_Summary_CH4_20251129_MUSES_R1p24_FS_F0p9_J1.nc.dap.csv"
target_table = "your_catalog.your_schema.trps_ch4_20251129"   # <-- change to your catalog.schema.table

# CELL 2 — read CSV into Spark dataframe (handles large files)
df = spark.read.option("header", "true").option("inferSchema", "true").csv(url)
print("Rows:", df.count())
display(df.limit(10))

# CELL 3 — basic column tidy (optional): lower-case and snake_case column names
from pyspark.sql.functions import col
new_cols = [c.lower().replace(" ", "_").replace("-", "_") for c in df.columns]
df = df.toDF(*new_cols)

# CELL 4 — write to managed table (delta) in Fabric (append or overwrite)
# If the catalog/schema/table doesn't exist this will create it (permissions required).
#df.write.format("delta").mode("append").saveAsTable(target_table)

# CELL 5 — verify
#spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# CELL ********************

%pip install requests pandas pyarrow --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================================

# NASA Earthdata Login Credentials
NASA_USERNAME = "dharun_karthick_2223"  # Replace with your username
NASA_PASSWORD = "sdharunk@2003A"  # Replace with your password
NASA_TOKEN = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImRoYXJ1bl9rYXJ0aGlja18yMjIzIiwiZXhwIjoxNzcwMDExOTg1LCJpYXQiOjE3NjQ4Mjc5ODUsImlzcyI6Imh0dHBzOi8vdXJzLmVhcnRoZGF0YS5uYXNhLmdvdiIsImlkZW50aXR5X3Byb3ZpZGVyIjoiZWRsX29wcyIsImFjciI6ImVkbCIsImFzc3VyYW5jZV9sZXZlbCI6M30.pa5xlirCnjs7j5tV0AiB6lUu8dBeUOtPQgqSna-rtOLfKQFz2M4R7EXJUFMAYYXYlfMABsPgoDb4YGVbdUl_eGo1VydRahfLpTFVcEweH9h1l0LhzGgd3FYzcwMNZTSXZK937yxEB7fyLqz90GF8ozWNG67RHZFRbzDWjx4OVP-zGgDd5pt6zfDWT-A55hpdkCugsMcgC7uRBquRF8JZM8CPW8v7bWckcfyjaVjNFNIQQQ3CS9kBBjF8clhaUkAKcrKISTTKOBfJUUJxeRbN3_BZ3kVZacNclRLcNdAJ4HkLjrpwX9WJNGzIkhN7hcOo0oN4QACZlQson4aBpceYzg"  # Paste your token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# STEP 1: Install required packages (run once)
# %pip install requests pandas pyarrow --quiet

import requests
import pandas as pd
from datetime import datetime
import io

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OPeNDAP CSV URL
OPENDAP_URL = "https://tropess.gesdisc.eosdis.nasa.gov/opendap/TROPESS_Summary/TRPSYL2CH4AIRSFS.1/2025/TROPESS_AIRS-Aqua_L2_Summary_CH4_20251129_MUSES_R1p24_FS_F0p9_J1.nc.dap.csv"

# Fabric Lakehouse Configuration
LAKEHOUSE_PATH = "Files/nasa_tropess/"  # Relative path in your default lakehouse
OUTPUT_FILENAME = "tropess_ch4_20251129.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# AUTHENTICATION SETUP
# ============================================================================

def create_authenticated_session(username=None, password=None, token=None):
    """
    Creates an authenticated session for NASA Earthdata
    Handles redirects and cookies properly
    """
    session = requests.Session()
    
    if token:
        # Token-based authentication (preferred)
        session.headers.update({'Authorization': f'Bearer {token}'})
        print("✓ Using Bearer Token authentication")
    elif username and password:
        # Username/Password authentication
        session.auth = (username, password)
        print("✓ Using Username/Password authentication")
    else:
        raise ValueError("Must provide either token or username/password")
    
    # Allow redirects and handle cookies
    session.max_redirects = 10
    
    return session

# ============================================================================
# DATA DOWNLOAD FUNCTION
# ============================================================================

def download_nasa_opendap_data(url, session, timeout=300):
    """
    Downloads data from NASA OPeNDAP server with proper authentication
    
    Parameters:
    - url: OPeNDAP CSV URL
    - session: Authenticated requests session
    - timeout: Request timeout in seconds (default 5 minutes)
    
    Returns:
    - pandas DataFrame
    """
    print(f"\n{'='*70}")
    print(f"Downloading data from NASA TROPESS...")
    print(f"{'='*70}")
    print(f"URL: {url}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Make the request with authentication
        response = session.get(url, timeout=timeout)
        
        # Check for successful response
        response.raise_for_status()
        
        print(f"✓ Download successful! Status code: {response.status_code}")
        print(f"✓ Content length: {len(response.content)} bytes")
        
        # Parse CSV data into DataFrame
        csv_data = io.StringIO(response.text)
        df = pd.read_csv(csv_data)
        
        print(f"✓ Data loaded into DataFrame")
        print(f"  - Shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"  - Columns: {list(df.columns)}")
        
        return df
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print("✗ ERROR: Authentication failed!")
            print("  Check your credentials or token")
        elif e.response.status_code == 404:
            print("✗ ERROR: Data file not found!")
            print("  Verify the URL is correct")
        else:
            print(f"✗ HTTP ERROR: {e}")
        raise
    
    except requests.exceptions.Timeout:
        print("✗ ERROR: Request timed out!")
        print("  The server took too long to respond. Try again.")
        raise
    
    except Exception as e:
        print(f"✗ ERROR: {type(e).__name__}: {str(e)}")
        raise

# ============================================================================
# FABRIC LAKEHOUSE SAVE FUNCTION
# ============================================================================

def save_to_lakehouse(df, lakehouse_path, filename):
    """
    Saves DataFrame to Fabric Lakehouse in multiple formats
    
    Parameters:
    - df: pandas DataFrame
    - lakehouse_path: Path in lakehouse (e.g., "Files/nasa_tropess/")
    - filename: Base filename without extension
    """
    print(f"\n{'='*70}")
    print(f"Saving data to Fabric Lakehouse...")
    print(f"{'='*70}")
    
    try:
        # Create directory if it doesn't exist
        import os
        full_path = lakehouse_path
        os.makedirs(full_path, exist_ok=True)
        
        base_name = filename.replace('.csv', '')
        
        # Save as CSV
        csv_path = f"{full_path}{base_name}.csv"
        df.to_csv(csv_path, index=False)
        print(f"✓ Saved CSV: {csv_path}")
        
        # Save as Parquet (optimized for analytics)
        parquet_path = f"{full_path}{base_name}.parquet"
        df.to_parquet(parquet_path, index=False)
        print(f"✓ Saved Parquet: {parquet_path}")
        
        # Save metadata
        metadata = {
            'download_timestamp': datetime.now().isoformat(),
            'source_url': OPENDAP_URL,
            'rows': df.shape[0],
            'columns': df.shape[1],
            'column_names': list(df.columns)
        }
        
        import json
        metadata_path = f"{full_path}{base_name}_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        print(f"✓ Saved Metadata: {metadata_path}")
        
        print(f"\n✓ All files saved successfully!")
        
    except Exception as e:
        print(f"✗ ERROR saving to lakehouse: {type(e).__name__}: {str(e)}")
        raise

# ============================================================================
# DATA QUALITY CHECKS
# ============================================================================

def perform_data_quality_checks(df):
    """
    Performs basic data quality checks
    """
    print(f"\n{'='*70}")
    print(f"Data Quality Report")
    print(f"{'='*70}")
    
    # Basic statistics
    print(f"\n1. BASIC STATISTICS:")
    print(f"   Total rows: {len(df):,}")
    print(f"   Total columns: {len(df.columns)}")
    print(f"   Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Missing values
    print(f"\n2. MISSING VALUES:")
    missing = df.isnull().sum()
    if missing.sum() > 0:
        print(missing[missing > 0])
    else:
        print("   ✓ No missing values found")
    
    # Data types
    print(f"\n3. DATA TYPES:")
    print(df.dtypes)
    
    # Sample data
    print(f"\n4. SAMPLE DATA (first 5 rows):")
    print(df.head())
    
    return True

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function
    """
    print(f"\n{'#'*70}")
    print(f"# NASA TROPESS DATA IMPORT TO FABRIC LAKEHOUSE")
    print(f"# Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*70}\n")
    
    try:
        # Step 1: Create authenticated session
        print("STEP 1: Creating authenticated session...")
        session = create_authenticated_session(
            username=NASA_USERNAME if NASA_USERNAME != "your_earthdata_username" else None,
            password=NASA_PASSWORD if NASA_PASSWORD != "your_earthdata_password" else None,
            token=NASA_TOKEN if NASA_TOKEN != "your_bearer_token" else None
        )
        
        # Step 2: Download data
        print("\nSTEP 2: Downloading data from NASA OPeNDAP...")
        df = download_nasa_opendap_data(OPENDAP_URL, session)
        
        # Step 3: Data quality checks
        print("\nSTEP 3: Performing data quality checks...")
        perform_data_quality_checks(df)
        
        # Step 4: Save to Fabric Lakehouse
        print("\nSTEP 4: Saving to Fabric Lakehouse...")
        save_to_lakehouse(df, LAKEHOUSE_PATH, OUTPUT_FILENAME)
        
        # Success message
        print(f"\n{'#'*70}")
        print(f"# ✓ SUCCESS! Data imported to Fabric Lakehouse")
        print(f"# Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#'*70}\n")
        
        return df
        
    except Exception as e:
        print(f"\n{'#'*70}")
        print(f"# ✗ FAILED! Error during execution")
        print(f"# Error: {type(e).__name__}: {str(e)}")
        print(f"{'#'*70}\n")
        raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# RUN THE SCRIPT
# ============================================================================

if __name__ == "__main__":
    # Execute main function
    data_df = main()
    
    # Optional: Display final DataFrame
    print("\nFinal DataFrame loaded in variable: data_df")
    print("Use 'data_df' to perform further analysis")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import os

# ============================================================================
# CONFIGURATION - UPDATE YOUR TOKEN
# ============================================================================
NASA_TOKEN = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImRoYXJ1bl9rYXJ0aGlja18yMjIzIiwiZXhwIjoxNzcwMDExOTg1LCJpYXQiOjE3NjQ4Mjc5ODUsImlzcyI6Imh0dHBzOi8vdXJzLmVhcnRoZGF0YS5uYXNhLmdvdiIsImlkZW50aXR5X3Byb3ZpZGVyIjoiZWRsX29wcyIsImFjciI6ImVkbCIsImFzc3VyYW5jZV9sZXZlbCI6M30.pa5xlirCnjs7j5tV0AiB6lUu8dBeUOtPQgqSna-rtOLfKQFz2M4R7EXJUFMAYYXYlfMABsPgoDb4YGVbdUl_eGo1VydRahfLpTFVcEweH9h1l0LhzGgd3FYzcwMNZTSXZK937yxEB7fyLqz90GF8ozWNG67RHZFRbzDWjx4OVP-zGgDd5pt6zfDWT-A55hpdkCugsMcgC7uRBquRF8JZM8CPW8v7bWckcfyjaVjNFNIQQQ3CS9kBBjF8clhaUkAKcrKISTTKOBfJUUJxeRbN3_BZ3kVZacNclRLcNdAJ4HkLjrpwX9WJNGzIkhN7hcOo0oN4QACZlQson4aBpceYzg"  # Paste your token
OPENDAP_URL = "https://tropess.gesdisc.eosdis.nasa.gov/opendap/TROPESS_Summary/TRPSYL2CH4AIRSFS.1/2025/TROPESS_AIRS-Aqua_L2_Summary_CH4_20251129_MUSES_R1p24_FS_F0p9_J1.nc.dap.csv"
OUTPUT_PATH = "abfss://060ba34b-f1a3-4509-a6e2-36d1e736a8eb@onelake.dfs.fabric.microsoft.com/e0efee7f-4d05-4685-b178-768ed7635e44/Files/nasa/"
OUTPUT_FILENAME = "tropess_ch4_20251129.csv"

# ============================================================================
# DOWNLOAD AND SAVE
# ============================================================================

# Create session with authentication
session = requests.Session()
session.headers.update({'Authorization': f'Bearer {NASA_TOKEN}'})

# Download file
print(f"Downloading from: {OPENDAP_URL}")
response = session.get(OPENDAP_URL, timeout=300)
response.raise_for_status()

print(f"✓ Downloaded {len(response.content):,} bytes")

# Save to ABFS path (Fabric Lakehouse)
full_path = OUTPUT_PATH + OUTPUT_FILENAME
# Use mssparkutils for Fabric
from notebookutils import mssparkutils
mssparkutils.fs.put(full_path, response.text, overwrite=True)

print(f"✓ File saved to: {full_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/nasa/tropess_ch4_20251129.csv")
# df now is a Spark DataFrame containing CSV data from "Files/nasa/tropess_ch4_20251129.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import os

# ============================================================================
# CONFIGURATION - UPDATE YOUR TOKEN
# ============================================================================
NASA_TOKEN = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImRoYXJ1bl9rYXJ0aGlja18yMjIzIiwiZXhwIjoxNzcwMDExOTg1LCJpYXQiOjE3NjQ4Mjc5ODUsImlzcyI6Imh0dHBzOi8vdXJzLmVhcnRoZGF0YS5uYXNhLmdvdiIsImlkZW50aXR5X3Byb3ZpZGVyIjoiZWRsX29wcyIsImFjciI6ImVkbCIsImFzc3VyYW5jZV9sZXZlbCI6M30.pa5xlirCnjs7j5tV0AiB6lUu8dBeUOtPQgqSna-rtOLfKQFz2M4R7EXJUFMAYYXYlfMABsPgoDb4YGVbdUl_eGo1VydRahfLpTFVcEweH9h1l0LhzGgd3FYzcwMNZTSXZK937yxEB7fyLqz90GF8ozWNG67RHZFRbzDWjx4OVP-zGgDd5pt6zfDWT-A55hpdkCugsMcgC7uRBquRF8JZM8CPW8v7bWckcfyjaVjNFNIQQQ3CS9kBBjF8clhaUkAKcrKISTTKOBfJUUJxeRbN3_BZ3kVZacNclRLcNdAJ4HkLjrpwX9WJNGzIkhN7hcOo0oN4QACZlQson4aBpceYzg"  # Paste your token
#OPENDAP_URL = "https://tropess.gesdisc.eosdis.nasa.gov/opendap/TROPESS_Summary/TRPSYL2CH4AIRSFS.1/2025/TROPESS_AIRS-Aqua_L2_Summary_CH4_20251128_MUSES_R1p24_FS_F0p9_J1.nc.dap.csv?dap4.ce=/time[0:1:7519];/longitude[0:1:7519];/latitude[0:1:7519];/x_col_p[0:1:7519]"
#OPENDAP_URL = "https://tropess.gesdisc.eosdis.nasa.gov/opendap/TROPESS_Summary/TRPSYL2CH4AIRSFS.1/2025/TROPESS_AIRS-Aqua_L2_Summary_CH4_20250107_MUSES_R1p23_FS_F0p9_J1.nc?SouthBoundingCoordinate=-67.000&NorthBoundingCoordinate=65.000"
OPENDAP_URL = "https://tropomi.gesdisc.eosdis.nasa.gov/opendap/S5P_TROPOMI_Level2/S5P_L2__CH4____HiR.2/2025/337/S5P_OFFL_L2__CH4____20251203T000040_20251203T014210_42174_03_020901_20251204T161845.nc.dap.csv?dap4.ce=/PRODUCT_SUPPORT_DATA_GEOLOCATIONS_satellite_altitude[0:1:0][0:1:4171];/PRODUCT_qa_value[0:1:0][0:1:4171][0:1:214];/PRODUCT_longitude[0:1:0][0:1:4171][0:1:214];/PRODUCT_latitude[0:1:0][0:1:4171][0:1:214];/PRODUCT_time_utc[0:1:0][0:1:4171];/PRODUCT_methane_mixing_ratio[0:1:0][0:1:4171][0:1:214]"
OUTPUT_PATH = "abfss://060ba34b-f1a3-4509-a6e2-36d1e736a8eb@onelake.dfs.fabric.microsoft.com/e0efee7f-4d05-4685-b178-768ed7635e44/Files/nasa/"
OUTPUT_FILENAME = "tropomi_ch4_20251203.csv"

# ============================================================================
# DOWNLOAD AND SAVE
# ============================================================================

# Create session with authentication
session = requests.Session()
session.headers.update({'Authorization': f'Bearer {NASA_TOKEN}'})

# Download file
print(f"Downloading from: {OPENDAP_URL}")
response = session.get(OPENDAP_URL, timeout=300)
response.raise_for_status()

print(f"✓ Downloaded {len(response.content):,} bytes")

# Save to ABFS path (Fabric Lakehouse)
full_path = OUTPUT_PATH + OUTPUT_FILENAME
# Use mssparkutils for Fabric
from notebookutils import mssparkutils
mssparkutils.fs.put(full_path, response.text, overwrite=True)

print(f"✓ File saved to: {full_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/nasa/tropess_ch4_20251130.csv")
# df now is a Spark DataFrame containing CSV data from "Files/nasa/tropess_ch4_20251130.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(raw_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------
# 1. Read raw file from OneLake ABFS path
# -----------------------------------------------

#path = "abfss://060ba34b-f1a3-4509-a6e2-36d1e736a8eb@onelake.dfs.fabric.microsoft.com/e0efee7f-4d05-4685-b178-768ed7635e44/Files/nasa/tropess_ch4_20251130.csv"
path = "abfss://060ba34b-f1a3-4509-a6e2-36d1e736a8eb@onelake.dfs.fabric.microsoft.com/e0efee7f-4d05-4685-b178-768ed7635e44/Files/nasa/tropomi_ch4_20251203.csv"
raw_df = spark.read.text(path)

# Convert rows → list of strings
lines = [row.value.strip() for row in raw_df.collect() if row.value.strip()]

# -----------------------------------------------
# 2. Remove first metadata row (starts with 'Dataset')
# -----------------------------------------------
lines = [line for line in lines if not line.lower().startswith("dataset")]

# -----------------------------------------------
# 3. Parse OPeNDAP ASCII variable arrays
# -----------------------------------------------

data = {}

for line in lines:
    parts = [p.strip() for p in line.split(",")]

    var = parts[0].lstrip("/")           # remove leading slash
    values = [float(x) for x in parts[1:]]

    data[var] = values

# -----------------------------------------------
# 4. Convert dict → pandas → Spark DataFrame
# -----------------------------------------------

import pandas as pd
pdf = pd.DataFrame(data)

spark_df = spark.createDataFrame(pdf)

# -----------------------------------------------
# 5. Convert UNIX time → proper timestamp column
# -----------------------------------------------

from pyspark.sql.functions import from_unixtime, col

spark_df = spark_df.withColumn(
    "time",
    from_unixtime(col("time"))
)

# -----------------------------------------------
# 6. Display clean final dataframe
# -----------------------------------------------

display(spark_df)
spark_df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
