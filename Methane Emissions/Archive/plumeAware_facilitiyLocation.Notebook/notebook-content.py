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

# ============================================================================
# Configuration and Imports
# ============================================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import hashlib

# Set random seed for reproducibility
np.random.seed(42)

# Lakehouse configuration
LAKEHOUSE_NAME = "GreenSky_LH"
BRONZE_SCHEMA = "bronze"  # Schema for all bronze tables

print("=" * 80)
print("METHANE EMISSIONS INTELLIGENCE - SYNTHETIC DATA GENERATOR")
print("=" * 80)
print(f"Current Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Data Generation Period: 90 days (up to today)")
print(f"Target Schema: {BRONZE_SCHEMA}")
print("=" * 80)
print("    This script generates synthetic data for internal systems only:")
print("    • Facilities (ERP)")
print("    • Equipment (Asset Management)")
print("    • SCADA Operations")
print("    • Maintenance History (CMMS)")
print("    • Regulatory Compliance")
print("    • LDAR History")
print("    • Work Orders")
print("=" * 80)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CREATE BRONZE SCHEMA
# ============================================================================
# Purpose: Ensure the bronze schema exists before saving tables

print("\n" + "=" * 80)
print("INITIALIZING BRONZE SCHEMA")
print("=" * 80)

try:
    # Create bronze schema/database if it doesn't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
    print(f" Bronze schema '{BRONZE_SCHEMA}' is ready")
    
    # Verify schema exists
    schemas = spark.sql("SHOW SCHEMAS").collect()
    schema_names = [row.namespace for row in schemas]
    
    if BRONZE_SCHEMA in schema_names:
        print(f"Confirmed: '{BRONZE_SCHEMA}' schema exists")
    else:
        print(f"  Warning: '{BRONZE_SCHEMA}' schema not found in list")
    
except Exception as e:
    print(f"  Error creating schema: {e}")
    print("   Tables will be created with schema prefix")

print("=" * 80)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 2: USA Oil & Gas Region Definitions
# ============================================================================
# Purpose: Define realistic geographic boundaries for USA O&G operations
# These coordinates align with Carbon Mapper's coverage area

USA_OG_REGIONS = {
    "Permian Basin": {
        "state": "TX", 
        "lat_range": (31.5, 32.5), 
        "lon_range": (-103.5, -101.5),
        "description": "West Texas, largest oil producing basin in USA"
    },
    "Eagle Ford": {
        "state": "TX", 
        "lat_range": (28.0, 29.5), 
        "lon_range": (-99.5, -97.5),
        "description": "South Texas shale play"
    },
    "Bakken": {
        "state": "ND", 
        "lat_range": (47.5, 48.5), 
        "lon_range": (-103.5, -102.0),
        "description": "North Dakota oil fields"
    },
    "Marcellus": {
        "state": "PA", 
        "lat_range": (39.5, 41.5), 
        "lon_range": (-80.5, -77.5),
        "description": "Pennsylvania natural gas"
    },
    "Anadarko": {
        "state": "OK", 
        "lat_range": (35.0, 36.5), 
        "lon_range": (-99.0, -97.0),
        "description": "Oklahoma oil & gas"
    },
    "DJ Basin": {
        "state": "CO", 
        "lat_range": (39.5, 40.5), 
        "lon_range": (-105.0, -103.5),
        "description": "Colorado oil production"
    },
    "Powder River": {
        "state": "WY", 
        "lat_range": (43.0, 45.0), 
        "lon_range": (-106.5, -105.0),
        "description": "Wyoming coal & gas"
    },
    "Haynesville": {
        "state": "LA", 
        "lat_range": (32.0, 33.0), 
        "lon_range": (-94.0, -93.0),
        "description": "Louisiana natural gas"
    }
}

print("\nConfigured USA Oil & Gas Regions:")
for region, info in USA_OG_REGIONS.items():
    print(f"  • {region} ({info['state']}): {info['description']}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 3: Utility Functions
# ============================================================================
# Purpose: Helper functions for data generation

def add_cdc_columns(df, record_type="INSERT", source_system=""):
    """Add CDC (Change Data Capture) columns following best practices"""
    current_timestamp = datetime.now()
    df['_created_at'] = current_timestamp
    df['_modified_at'] = current_timestamp
    df['_record_version'] = 1
    df['_is_current'] = True
    df['_record_hash'] = df.apply(
        lambda row: hashlib.md5(str(row.to_dict()).encode()).hexdigest(), 
        axis=1
    )
    df['_operation'] = record_type
    df['_source_system'] = source_system
    return df

def generate_random_coords(region_name):
    """Generate random coordinates within a specific USA O&G region"""
    region = USA_OG_REGIONS[region_name]
    lat = np.random.uniform(*region["lat_range"])
    lon = np.random.uniform(*region["lon_range"])
    return round(lat, 7), round(lon, 7)

def save_to_lakehouse(df, table_name, schema=BRONZE_SCHEMA):
    """Save pandas DataFrame to Fabric Lakehouse as Delta table in bronze schema"""
    try:
        # Convert pandas to Spark DataFrame
        spark_df = spark.createDataFrame(df)
        
        # Construct full table name with schema
        full_table_name = f"{schema}.{table_name}"
        
        # Save as Delta table with schema
        spark_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table_name)
        
        print(f"Saved {len(df)} records to {full_table_name}")
        return True
    except Exception as e:
        print(f" Error saving {table_name}: {str(e)}")
        return False



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 4: Generate BRONZE - Facilities Master Data (Plume-Aware)
# ============================================================================

print("\n" + "=" * 80)
print("GENERATING BRONZE LAYER - TABLE 1: FACILITIES (PLUME-AWARE)")
print("=" * 80)

# Load plume geometry from Bronze CarbonMapper
df = spark.table("bronze.bronze_CarbonMapper")


import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

facilities_data = []
facility_ids = [f"FAC-{i:04d}" for i in range(1, 101)]  # generate 100 facilities

operators = [
    "XYZ Energy", "Apex Oil & Gas", "Summit Petroleum", 
    "Highland Resources", "Vista Energy Corp", "Frontier Oil", 
    "Pinnacle Resources", "Crestwood Energy",
    "Silver Peak Oil", "Mountain View Resources"
]

# Filter plumes (only rows with valid array)
plumes_pdf = (
    df
    .where("plume_bounds IS NOT NULL AND size(plume_bounds) >= 4")
    .select("plume_id", "plume_bounds")
    .toPandas()
)

for idx, fac_id in enumerate(facility_ids):

    plume = plumes_pdf.sample(1).iloc[0]
    plume_bounds = plume["plume_bounds"]

    if plume_bounds is None or len(plume_bounds) < 4:
        continue

    min_lon, min_lat, max_lon, max_lat = plume_bounds[:4]


    # Compute plume bounding box center
    center_lat = (min_lat + max_lat) / 2
    center_lon = (min_lon + max_lon) / 2

    # Generate small offset (up to ~0.5 km) to place facility inside or near the plume
    lat_offset = random.uniform(-0.005, 0.005)
    lon_offset = random.uniform(-0.005, 0.005)

    lat = round(center_lat + lat_offset, 6)
    lon = round(center_lon + lon_offset, 6)

    # Region logic (unchanged)
    region_name = list(USA_OG_REGIONS.keys())[idx % len(USA_OG_REGIONS)]
    region = USA_OG_REGIONS[region_name]
    operator = np.random.choice(operators)

    facility = {
        "facility_id": fac_id,
        "facility_name": f"Well Pad {np.random.randint(100, 999)}",
        "operator": operator,
        "operator_id": f"OP-{operators.index(operator) + 1:03d}",
        "permit_number": f"EPA-{region['state']}-{np.random.randint(2018, 2025)}-{np.random.randint(1000, 9999)}",
        "facility_type": np.random.choice(
            ["Production", "Processing", "Compression", "Storage"], 
            p=[0.6, 0.2, 0.15, 0.05]
        ),

        #  CORRECTED GEOSPATIAL LOGIC 
        "latitude": lat,
        "longitude": lon,

        "state": region['state'],
        "county": f"{region['state']}-County-{np.random.randint(1, 20):02d}",
        "basin": region_name,
        "sector_code": "1B2",
        "epa_threshold_kg_hr": np.random.choice([100, 500, 1000], p=[0.2, 0.5, 0.3]),
        "operational_since": (datetime.now() - timedelta(days=np.random.randint(365, 2920))).strftime("%Y-%m-%d"),
        "operational_status": np.random.choice(
            ["Active", "Active", "Active", "Temporarily Inactive"], 
            p=[0.85, 0.10, 0.03, 0.02]
        ),
        "primary_contact": f"{np.random.choice(['John', 'Sarah', 'Mike', 'Lisa', 'Tom', 'Maria'])} {np.random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Garcia'])}",
        "contact_email": f"ops.{fac_id.lower()}@{operator.lower().replace(' ', '')}.com",
        "contact_phone": f"+1-{np.random.randint(200, 999)}-{np.random.randint(100, 999)}-{np.random.randint(1000, 9999)}",
        "production_capacity_bbl_day": np.random.randint(500, 5000),
        "num_wells": np.random.randint(2, 20),

        #  CORRECTED: plume_id (not id)
        "nearest_plume_id": plume["plume_id"]
    }

    facilities_data.append(facility)

# Convert to dataframe
df_facilities = pd.DataFrame(facilities_data)
df_facilities = add_cdc_columns(df_facilities, source_system='ERP_SAP')

print(f"\n Generated {len(df_facilities)} facilities mapped to plumes")

# Save table
spark.createDataFrame(df_facilities).write.mode("overwrite").format("delta").saveAsTable("bronze.bronze_facilities_new")


print("\nSample records:")
print(df_facilities[['facility_id','nearest_plume_id','latitude','longitude']].head().to_string(index=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df2 = df.select(
    *[F.col("plume_bounds")[i].alias(f"tag_{i}") for i in range(0, 5)]
)
df2.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
