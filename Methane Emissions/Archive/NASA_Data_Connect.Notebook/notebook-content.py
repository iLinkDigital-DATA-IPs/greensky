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

%pip install xarray netCDF4

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import xarray as xr
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
import warnings
warnings.filterwarnings('ignore')

print("✓ Libraries imported successfully!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get Spark session
spark = SparkSession.builder.getOrCreate()

# Lakehouse configuration
LAKEHOUSE_NAME = "GreenSky_LH"
LAKEHOUSE_PATH = f"abfss://{LAKEHOUSE_NAME}@onelake.dfs.fabric.microsoft.com"
TABLE_PATH = f"{LAKEHOUSE_PATH}/Tables/bronze/tropomi_methane_data"

# Data URL - Update with your specific date
DATA_URL = "https://tropomi.gesdisc.eosdis.nasa.gov/opendap/S5P_TROPOMI_Level2/S5P_L2__CH4____HiR.2/2025/333/S5P_OFFL_L2__CH4____20251129T011651_20251129T025820_42118_03_020901_20251130T213125.nc"

PROCESSING_DATE = "2025-11-29"

print(f"✓ Lakehouse: {LAKEHOUSE_NAME}")
print(f"✓ Processing date: {PROCESSING_DATE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Loading TROPOMI data from NASA GES DISC...")
print("This may take 2-5 minutes...\n")

# Open the PRODUCT group
ds = xr.open_dataset(DATA_URL, group='PRODUCT')
print("✓ Successfully loaded PRODUCT group")

# Extract variables
try:
    methane = ds['methane_mixing_ratio_bias_corrected'].values
    print("✓ Using bias-corrected methane data")
except:
    methane = ds['methane_mixing_ratio'].values
    print("✓ Using standard methane data")

lat = ds['latitude'].values
lon = ds['longitude'].values
qa = ds['qa_value'].values

# Optional: precision
try:
    precision = ds['methane_mixing_ratio_precision'].values
    has_precision = True
    print("✓ Precision data extracted")
except:
    has_precision = False
    print("✓ Precision data not available")

print(f"\nData shape: {methane.shape}")
print(f"Total pixels: {lat.size:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\nCreating clean DataFrame...")

# Flatten arrays
data_dict = {
    'latitude': lat.flatten(),
    'longitude': lon.flatten(),
    'methane_ppb': methane.flatten(),
    'qa_value': qa.flatten(),
    'observation_date': PROCESSING_DATE,
    'data_source': 'TROPOMI_S5P',
    'processing_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
}

if has_precision:
    data_dict['precision'] = precision.flatten()

# Create pandas DataFrame
df_clean = pd.DataFrame(data_dict)

# Remove NaN values
df_clean = df_clean.dropna(subset=['methane_ppb', 'latitude', 'longitude'])

print(f"✓ Clean DataFrame created")
print(f"✓ Total valid records: {len(df_clean):,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n" + "="*60)
print("CLEAN DATA PREVIEW")
print("="*60)

print(f"\nDataFrame Shape: {df_clean.shape}")
print(f"Columns: {list(df_clean.columns)}")

print("\nFirst 20 rows:")
display(df_clean.head(20))

print("\nData Statistics:")
display(df_clean.describe())

print("\nData Info:")
print(df_clean.info())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
