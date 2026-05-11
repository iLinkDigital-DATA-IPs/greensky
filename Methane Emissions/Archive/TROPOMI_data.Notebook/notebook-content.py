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

%pip install planetary-computer pystac-client xarray h5netcdf fsspec aiohttp
%pip install azure-eventhub pandas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import planetary_computer
import pystac_client
import xarray as xr
import fsspec
import pandas as pd
import numpy as np

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

longitude = -94.5786
latitude = 39.0997

geometry = {
    "type": "Point",
    "coordinates": [longitude, latitude],
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

search = catalog.search(
    collections="sentinel-5p-l2-netcdf",
    intersects=geometry,
    datetime="2025-12-11/2025-12-11",  # Changed to past dates
    query={
        "s5p:processing_mode": {"eq": "OFFL"}, 
        "s5p:product_name": {"eq": "ch4"}
    },
)

items = list(search.items())

# Display basic info about found items
if len(items) > 0:
    print("\nAvailable Items:")
    for i, item in enumerate(items):
        print(f"  Item {i}: {item.id}")
        print(f"    Date: {item.datetime}")
        print(f"    Assets: {list(item.assets.keys())}")
        print()
else:
    print(" No data found")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(items) > 0:
    
    log_table_name = "sentinel5p_ch4_item_log"

    new_item_ids = [item.id for item in items]

    existing_item_ids = []
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
        existing_log_df = spark.sql(f"SELECT item_id FROM {log_table_name}").toPandas()
        
        if len(existing_log_df) > 0:
            existing_item_ids = existing_log_df['item_id'].tolist()
            print(f"Found {len(existing_item_ids)} existing items in log table")
        else:
            print(f"Log table exists but is empty (first run)")
            
    except Exception as e:
        print(f"Could not read log table (might be first run)")
    
    # Filter out items that already exist in log
    items_to_process = [item for item in items if item.id not in existing_item_ids]
    
    print(f"\nTotal items found: {len(items)}")
    print(f"Already processed: {len(existing_item_ids)}")
    print(f"New items to process: {len(items_to_process)}")
    
    if len(items_to_process) == 0:
        items = []
    else:
      
        # Create log entries for NEW items IMMEDIATELY
        log_entries = []
        ingestion_datetime = pd.Timestamp.now()
        
        for idx, item in enumerate(items_to_process):
            item_id = item.id
            id_parts = item_id.split('_')
            
            start_time_str = id_parts[4]
            end_time_str = id_parts[5]
            orbit_number = id_parts[6]
            
            item_date = pd.to_datetime(start_time_str, format='%Y%m%dT%H%M%S').date()
            item_start_datetime = pd.to_datetime(start_time_str, format='%Y%m%dT%H%M%S')
            item_end_datetime = pd.to_datetime(end_time_str, format='%Y%m%dT%H%M%S')
            
            items_on_same_date = sum(1 for i in items if 
                                     pd.to_datetime(i.id.split('_')[4], format='%Y%m%dT%H%M%S').date() == item_date)
            
            same_date_items = [i for i in items if 
                              pd.to_datetime(i.id.split('_')[4], format='%Y%m%dT%H%M%S').date() == item_date]
            sequence_on_date = same_date_items.index(item) + 1
            
            log_entry = {
                'item_id': item_id,
                'item_number': len(existing_item_ids) + idx + 1,
                'item_date': item_date,
                'item_start_datetime': item_start_datetime,
                'item_end_datetime': item_end_datetime,
                'orbit_number': orbit_number,
                'items_on_same_date': items_on_same_date,
                'sequence_on_date': sequence_on_date,
                'ingestion_datetime': ingestion_datetime,
                'ingestion_date': ingestion_datetime.date(),
                'product_type': 'CH4',
                'satellite': 'Sentinel-5P',
                'processing_mode': 'OFFL',
                'collection': 'sentinel-5p-l2-netcdf'
            }
            log_entries.append(log_entry)
        
        log_df = pd.DataFrame(log_entries)
        
        # Save log entries immediately
        try:
            from notebookutils import mssparkutils
            
            mssparkutils.lakehouse.write_table(
                name=log_table_name,
                df=log_df,
                mode="append"
            )

            
        except Exception as e:
            try:
                spark_log_df = spark.createDataFrame(log_df)
                spark_log_df.write.format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(log_table_name)

                
            except Exception as e2:
                print(f" Error updating log: {str(e2)}")
        
        # Update items list to only new items
        items = items_to_process
       
else:
    items = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(items)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(items) > 0:
    f = fsspec.open(items[0].assets["ch4"].href).open()
    ds = xr.open_dataset(f, group="PRODUCT", engine="h5netcdf")
    
    print(ds)
    print("\n")
    

    for dim, size in ds.sizes.items():
        print(f"  {dim}: {size}")
    print("\n")
        
    if len(ds.data_vars) == 0:
        print(ds)

    else:
        for var in ds.data_vars:
            print(f"\n  Variable: {var}")
            print(f"    Shape: {ds[var].shape}")
            print(f"    Dimensions: {ds[var].dims}")
            print(f"    Data Type: {ds[var].dtype}")
            
            # Safely get attributes
            attrs = ds[var].attrs
            print(f"    Description: {attrs.get('long_name', attrs.get('standard_name', 'N/A'))}")
            print(f"    Units: {attrs.get('units', 'N/A')}")
       
    if len(ds.coords) == 0:
        print(" No coordinate variables found")
    else:
        for coord in ds.coords:
            print(f"\n  Coordinate: {coord}")
            print(f"    Shape: {ds[coord].shape}")
            print(f"    Data Type: {ds[coord].dtype}")
            
            # Safely get attributes
            attrs = ds[coord].attrs
            print(f"    Description: {attrs.get('long_name', attrs.get('standard_name', 'N/A'))}")
            print(f"    Units: {attrs.get('units', 'N/A')}")
    
    # Summary of all available columns
    print("\n" + "="*70)
    print("SUMMARY: ALL AVAILABLE COLUMNS")
    print("="*70)
    all_columns = list(ds.data_vars) + list(ds.coords)
    print(f"\nTotal columns available: {len(all_columns)}")
    
    if len(all_columns) > 0:
        print("\nColumn Name → Data Type:")
        for col in all_columns:
            if col in ds.data_vars:
                print(f"  {col:40s} → {str(ds[col].dtype):15s} [DATA VARIABLE]")
            else:
                print(f"  {col:40s} → {str(ds[col].dtype):15s} [COORDINATE]")
    else:
        print("\n No columns found. The dataset structure might be different.")
        print("\nDataset info:")
        print(ds)
        
        # Try to explore all groups in the file
        print("\n" + "="*70)
        print("EXPLORING FILE STRUCTURE")
        print("="*70)
        print("Attempting to list all groups in the NetCDF file...")
        try:
            import h5netcdf
            f_temp = fsspec.open(items[0].assets["ch4"].href).open()
            with h5netcdf.File(f_temp, 'r') as hf:
                print("\nAvailable groups:")
                def print_structure(name, obj):
                    print(f"  {name}")
                hf.visititems(print_structure)
        except Exception as e:
            print(f"Could not explore file structure: {e}")
    
    print()
    
else:
    print(" No items found")
    ds = None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if ds is not None:    
    # Define the variables we want to extract
    selected_vars = [
        'latitude',
        'longitude',
        'scanline',
        'ground_pixel',
        'time_utc',
        'qa_value',
        'methane_mixing_ratio',
        'methane_mixing_ratio_precision'
    ]
    
    
    # Create a subset dataset with only selected variables
    ds_subset = ds[selected_vars]
    
    # Convert to dataframe
    df = ds_subset.to_dataframe().reset_index()
    
    # Remove duplicate columns if any (sometimes coordinates are duplicated)
    df = df.loc[:, ~df.columns.duplicated()]

    # Remove rows where methane mixing ratio is null or 0
    df = df[df['methane_mixing_ratio'].notna()]  # Remove null values    
    df = df[df['methane_mixing_ratio'] != 0]  # Remove zero values    
    
    # Sort by time_utc if it's datetime, otherwise by scanline
    try:
        if df['time_utc'].dtype == 'object':
            df['time_utc'] = pd.to_datetime(df['time_utc'])
        df = df.sort_values(by='time_utc').reset_index(drop=True)
    except:
        df = df.sort_values(by='scanline').reset_index(drop=True)
    
    # Extract date and time components from time_utc
    if pd.api.types.is_datetime64_any_dtype(df['time_utc']):
        df['date'] = df['time_utc'].dt.date
        df['time'] = df['time_utc'].dt.time
    
    # Reorder columns for better readability
    column_order = ['time_utc', 'date', 'time', 'latitude', 'longitude', 
                    'scanline', 'ground_pixel', 'methane_mixing_ratio', 
                    'methane_mixing_ratio_precision', 'qa_value']
    
    # Only include columns that exist
    column_order = [col for col in column_order if col in df.columns]
    other_cols = [col for col in df.columns if col not in column_order]
    df = df[column_order + other_cols]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
import time
from datetime import datetime, timedelta
import threading
import json
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configurable Variables
EVENT_HUB_CONNECTION_STR = ""
EVENT_HUB_NAME = ""
EVENTS_PER_SECOND = 10

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

producer = EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
