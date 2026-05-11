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

# MARKDOWN ********************

# # CARBON MAPPER API TO GEOJSON - INCREMENTAL LOAD
# ## Enhanced notebook for Fabric Lakehouse with incremental updates
# ---
# **Features:**
# - ✅ Automatic GeoJSON conversion
# - ✅ Incremental loading (only new data)
# - ✅ Deduplication by plume_id
# - ✅ File storage in Lakehouse
# - ✅ Archive management
# - ✅ Delta table updates

# CELL ********************

# CELL 1: Import Libraries
# ============================================================================
import requests
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, max as spark_max, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

print("✅ Libraries imported successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# CELL 2: Configuration Parameters
# ============================================================================
# ⚠️ IMPORTANT: Mark this cell as "Parameter Cell" in notebook settings
# These can be overridden by Data Pipeline parameters

# API Configuration
base_url = "https://api.carbonmapper.org/api/v1/"
endpoint = "catalog/plumes/annotated"
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzYyMTY3NTQ0LCJpYXQiOjE3NjE1NjI3NDQsImp0aSI6Ijg4ZDk2ZWI0NDBlMjQ5NTdiZjkyNzlkZjgwOTE2NzFmIiwic2NvcGUiOiJzdGFjIGNhdGFsb2c6cmVhZCIsImdyb3VwcyI6IlB1YmxpYyIsImFsbF9ncm91cF9uYW1lcyI6eyJjb21tb24iOlsiUHVibGljIl19LCJvcmdhbml6YXRpb25zIjoiIiwic2V0dGluZ3MiOnt9LCJpc19zdGFmZiI6ZmFsc2UsImlzX3N1cGVydXNlciI6ZmFsc2UsInVzZXJfaWQiOjE1NzQyfQ.F1CsQr1ZuWtzLPFy2Zgy8PewmtFUkAB48miSqy5_4K8"

# Query parameters
bbox = [-125, 24, -66, 49]  # US bounding box
sector = "1B2"  # Oil & Gas production (use None for all sectors)
page_limit = 1000

# Incremental load settings
enable_incremental = False  # Set to False for full reload
lookback_days = 7  # How many days to look back if no previous data exists

# File paths
geojson_output_path = "/lakehouse/default/Files/carbon_mapper/plumes_latest.geojson"
geojson_archive_path = "/lakehouse/default/Files/carbon_mapper/archive/"

# Delta table name
bronze_table = "bronze_CarbonMapper"

print("✅ Configuration loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 3: Helper Functions
# ============================================================================

def get_last_ingestion_date():
    """
    Get the most recent scene_timestamp from bronze table for incremental load
    Returns: datetime string or None
    """
    try:
        spark = SparkSession.builder.getOrCreate()
        
        # Check if table exists
        if spark.catalog.tableExists(bronze_table):
            df = spark.table(bronze_table)
            
            # Get max scene_timestamp
            max_timestamp = df.select(spark_max("scene_timestamp")).collect()[0][0]
            
            if max_timestamp:
                print(f"✅ Last ingestion date found: {max_timestamp}")
                return max_timestamp
        else:
            print(f"⚠️ Table '{bronze_table}' does not exist. Will perform full load.")
            return None
            
    except Exception as e:
        print(f"⚠️ Error getting last ingestion date: {e}")
        return None
    
    return None


def calculate_date_range(enable_incremental, lookback_days):
    """
    Calculate start_date and end_date for API query
    """
    end_date = datetime.utcnow()
    
    if enable_incremental:
        last_date = get_last_ingestion_date()
        
        if last_date:
            # Parse the last date and add 1 second to avoid duplicates
            try:
                start_date = datetime.fromisoformat(last_date.replace('Z', '+00:00'))
                start_date = start_date + timedelta(seconds=1)
            except:
                # If parsing fails, use lookback
                start_date = end_date - timedelta(days=lookback_days)
        else:
            # No previous data, use lookback period
            start_date = end_date - timedelta(days=lookback_days)
    else:
        # Full load - go back to earliest data
        start_date = datetime(2020, 1, 1)
    
    return start_date.strftime("%Y-%m-%dT%H:%M:%SZ"), end_date.strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_carbon_mapper_data(base_url, endpoint, token, params):
    """
    Fetch all pages of data from Carbon Mapper API
    Returns: list of records
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    all_records = []
    page = 0
    page_size = params["limit"]
    
    print(f"🔄 Starting data fetch with params: {params}")
    
    while True:
        params["offset"] = page * page_size
        
        try:
            response = requests.get(base_url + endpoint, headers=headers, params=params)
            
            if response.status_code != 200:
                print(f"⚠️ Error: {response.status_code} - {response.text}")
                break
            
            data = response.json()
            items = data.get("items", [])
            
            if not items:
                print(f"✅ All pages fetched. Total records: {len(all_records)}")
                break
            
            all_records.extend(items)
            page += 1
            print(f"📄 Page {page} fetched: {len(items)} records")
            
        except Exception as e:
            print(f"❌ Error fetching page {page}: {e}")
            break
    
    return all_records


def convert_to_geojson(records):
    """
    Convert Carbon Mapper records to GeoJSON FeatureCollection
    """
    features = []
    
    for record in records:
        # Extract geometry
        geometry = record.get("geometry_json", {})
        
        # If geometry is not in proper format, construct it
        if not geometry or "coordinates" not in geometry:
            continue
        
        # Build properties (exclude geometry_json to avoid duplication)
        properties = {k: v for k, v in record.items() if k != "geometry_json"}
        
        # Create GeoJSON feature
        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": properties
        }
        
        features.append(feature)
    
    # Create FeatureCollection
    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "record_count": len(features),
            "source": "Carbon Mapper API"
        },
        "features": features
    }
    
    return geojson


def save_geojson_to_lakehouse(geojson_data, output_path, archive_path=None):
    """
    Save GeoJSON to Lakehouse Files section
    Optionally create archived version with timestamp
    """
    try:
        # Create directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)
        
        # Save main GeoJSON file
        with open(output_path, 'w') as f:
            json.dump(geojson_data, f, indent=2)
        
        print(f"✅ GeoJSON saved to: {output_path}")
        
        # Create archived copy with timestamp
        if archive_path:
            os.makedirs(archive_path, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_file = os.path.join(archive_path, f"plumes_{timestamp}.geojson")
            
            with open(archive_file, 'w') as f:
                json.dump(geojson_data, f, indent=2)
            
            print(f"📦 Archived copy saved to: {archive_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error saving GeoJSON: {e}")
        return False


def merge_incremental_geojson(new_geojson, existing_path):
    """
    Merge new GeoJSON features with existing file
    Remove duplicates based on plume_id
    """
    try:
        # Load existing GeoJSON if file exists
        if os.path.exists(existing_path):
            with open(existing_path, 'r') as f:
                existing_geojson = json.load(f)
            
            existing_features = existing_geojson.get("features", [])
            print(f"📂 Existing GeoJSON has {len(existing_features)} features")
        else:
            existing_features = []
            print(f"📂 No existing GeoJSON file found, creating new one")
        
        # Create a dictionary of existing features by plume_id
        existing_dict = {}
        for feature in existing_features:
            plume_id = feature.get("properties", {}).get("plume_id")
            if plume_id:
                existing_dict[plume_id] = feature
        
        # Add new features, overwriting duplicates
        new_features = new_geojson.get("features", [])
        for feature in new_features:
            plume_id = feature.get("properties", {}).get("plume_id")
            if plume_id:
                existing_dict[plume_id] = feature  # This overwrites if exists
        
        # Convert back to list
        merged_features = list(existing_dict.values())
        
        # Create final GeoJSON
        merged_geojson = {
            "type": "FeatureCollection",
            "metadata": {
                "generated_at": datetime.utcnow().isoformat() + "Z",
                "record_count": len(merged_features),
                "source": "Carbon Mapper API",
                "merge_info": {
                    "existing_count": len(existing_features),
                    "new_count": len(new_features),
                    "final_count": len(merged_features)
                }
            },
            "features": merged_features
        }
        
        print(f"🔄 Merged: {len(existing_features)} existing + {len(new_features)} new = {len(merged_features)} total (after deduplication)")
        
        return merged_geojson
        
    except Exception as e:
        print(f"❌ Error merging GeoJSON: {e}")
        return new_geojson


print("✅ Helper functions defined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 4: Main Execution - Fetch Data
# ============================================================================

# Calculate date range for incremental load
start_date, end_date = calculate_date_range(enable_incremental, lookback_days)

print(f"\n{'='*60}")
print(f"📅 Date Range: {start_date} to {end_date}")
print(f"🔄 Incremental Mode: {enable_incremental}")
print(f"{'='*60}\n")

# Set up query parameters
params = {
    "limit": page_limit,
    "offset": 0,
    "bbox": bbox,
    "start_date": start_date,
    "end_date": end_date,
    "sector": sector
}

# Fetch data from API
all_records = fetch_carbon_mapper_data(base_url, endpoint, token, params)

print(f"\n✅ Total records fetched: {len(all_records)}")

# Store for next cell
records_count = len(all_records)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 5: Convert to GeoJSON and Save
# ============================================================================

if all_records and len(all_records) > 0:
    print(f"\n🔄 Converting {len(all_records)} records to GeoJSON...")
    
    # Convert to GeoJSON
    new_geojson = convert_to_geojson(all_records)
    
    print(f"✅ GeoJSON created with {len(new_geojson['features'])} features")
    
    # Merge with existing file if incremental mode
    if enable_incremental:
        print(f"\n🔄 Merging with existing GeoJSON file...")
        final_geojson = merge_incremental_geojson(new_geojson, geojson_output_path)
    else:
        print(f"\n📝 Full reload mode - replacing existing file...")
        final_geojson = new_geojson
    
    # Save to Lakehouse
    print(f"\n💾 Saving GeoJSON to Lakehouse...")
    save_success = save_geojson_to_lakehouse(
        final_geojson, 
        geojson_output_path, 
        geojson_archive_path
    )
    
    if save_success:
        print(f"\n✅ GeoJSON file ready for Fabric Maps!")
        print(f"📍 Location: {geojson_output_path}")
        print(f"📊 Total features: {len(final_geojson['features'])}")
    
else:
    print(f"\n⚠️ No new records to process")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 6: Update Bronze Delta Table
# ============================================================================

if all_records and len(all_records) > 0:
    print(f"\n{'='*60}")
    print(f"💾 Updating Bronze Delta Table: {bronze_table}")
    print(f"{'='*60}\n")
    
    spark = SparkSession.builder.getOrCreate()
    
    # Convert records to JSON strings for Spark
    json_rdd = spark.sparkContext.parallelize([json.dumps(r) for r in all_records])
    
    # Let Spark infer schema
    new_df = spark.read.json(json_rdd)
    
    print(f"✅ New DataFrame created: {new_df.count()} records, {len(new_df.columns)} columns")
    
    # Determine write mode
    if enable_incremental and spark.catalog.tableExists(bronze_table):
        write_mode = "append"
        print(f"🔄 Appending to existing table...")
    else:
        write_mode = "overwrite"
        print(f"📝 Overwriting table (full load)...")
    
    # Write to Delta table
    (new_df.write
        .format("delta")
        .mode(write_mode)
        .option("mergeSchema", "true")  # Allow schema evolution
        .saveAsTable(bronze_table))
    
    print(f"✅ Delta table updated successfully!")
    
    # Show summary
    final_df = spark.table(bronze_table)
    total_count = final_df.count()
    
    print(f"\n📊 Final Statistics:")
    print(f"   - Records in this load: {new_df.count()}")
    print(f"   - Total records in table: {total_count}")
    print(f"   - Table name: {bronze_table}")
    
    # Display sample
    print(f"\n📋 Sample data:")
    display(final_df.select("plume_id", "scene_timestamp", "gas", "sector", "emission_auto", "geometry_json").limit(5))
    
else:
    print(f"\n⚠️ No records to update in Delta table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 7: Summary and Validation
# ============================================================================

print(f"\n{'='*60}")
print(f"🎉 EXECUTION SUMMARY")
print(f"{'='*60}")
print(f"📅 Date Range: {start_date} to {end_date}")
print(f"🔄 Incremental Mode: {enable_incremental}")
print(f"📊 Records Fetched: {records_count}")
print(f"📍 GeoJSON Path: {geojson_output_path}")
print(f"💾 Delta Table: {bronze_table}")

if os.path.exists(geojson_output_path):
    file_size = os.path.getsize(geojson_output_path) / (1024 * 1024)  # MB
    print(f"📦 GeoJSON File Size: {file_size:.2f} MB")
    
    # Check if file is too large for Fabric Maps
    if file_size > 20:
        print(f"⚠️ WARNING: File size exceeds 20 MB Fabric Maps limit!")
        print(f"   Consider converting to PMTiles format or filtering data")
else:
    print(f"⚠️ GeoJSON file not found")

print(f"\n✅ Next Steps:")
print(f"   1. Open Fabric Maps in your workspace")
print(f"   2. Click 'Add data items' → Select your Lakehouse")
print(f"   3. Navigate to Files/carbon_mapper/")
print(f"   4. Select 'plumes_latest.geojson'")
print(f"   5. Configure your map visualization!")
print(f"\n{'='*60}")
print(f"\n🗺️ Your dynamic Carbon Mapper pipeline is ready!")
print(f"Schedule this notebook to run automatically for continuous updates.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
