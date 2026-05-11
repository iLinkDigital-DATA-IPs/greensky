# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%pip install planetary-computer pystac-client xarray fsspec h5netcdf azure-kusto-data azure-kusto-ingest

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
from datetime import datetime, timedelta
import json
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Eventhouse connection details
EVENTHOUSE_CLUSTER_URI = "https://trd-b5gpfrcqchk3m40g53.z2.kusto.fabric.microsoft.com"
EVENTHOUSE_DATABASE = "GreenSky_EH"
EVENTHOUSE_TABLE = "sentinel5p_ch4_data"

# India center coordinates
LONGITUDE = 79.109
LATITUDE = 22.746

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_date_range():
    """Get date range: 4 days back from today"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=4)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def search_sentinel_data(start_date, end_date):
    """Search for Sentinel-5P CH4 data"""
    print(f"Searching for data between {start_date} and {end_date}")
    
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    
    geometry = {
        "type": "Point",
        "coordinates": [LONGITUDE, LATITUDE],
    }
    
    search = catalog.search(
        collections="sentinel-5p-l2-netcdf",
        intersects=geometry,
        datetime=f"{start_date}/{end_date}",
        query={
            "s5p:processing_mode": {"eq": "OFFL"}, 
            "s5p:product_name": {"eq": "ch4"}
        },
    )
    
    items = list(search.items())
    print(f"Found {len(items)} items")
    return items

def extract_ch4_data(item):
    """Extract CH4 data from a Sentinel-5P item"""
    try:
        f = fsspec.open(item.assets["ch4"].href).open()
        ds = xr.open_dataset(f, group="PRODUCT", engine="h5netcdf")
        
        # Extract relevant data
        data_records = []
        
        # Get time dimension
        time_data = ds['time'].values if 'time' in ds else ds['delta_time'].values
        
        # Get CH4 mixing ratio
        ch4_mixing_ratio = ds['methane_mixing_ratio'].values
        ch4_mixing_ratio_precision = ds['methane_mixing_ratio_precision'].values if 'methane_mixing_ratio_precision' in ds else None
        
        # Get coordinates
        latitude_data = ds['latitude'].values
        longitude_data = ds['longitude'].values
        
        # Get quality flag if available
        qa_value = ds['qa_value'].values if 'qa_value' in ds else None
        
        # Flatten arrays and create records
        for i in range(len(time_data)):
            for j in range(ch4_mixing_ratio.shape[1]):
                if not pd.isna(ch4_mixing_ratio[i, j]):
                    record = {
                        'timestamp': pd.Timestamp(time_data[i]).isoformat(),
                        'item_id': item.id,
                        'latitude': float(latitude_data[i, j]) if latitude_data.ndim > 1 else float(latitude_data[i]),
                        'longitude': float(longitude_data[i, j]) if longitude_data.ndim > 1 else float(longitude_data[i]),
                        'ch4_mixing_ratio': float(ch4_mixing_ratio[i, j]),
                        'ch4_mixing_ratio_precision': float(ch4_mixing_ratio_precision[i, j]) if ch4_mixing_ratio_precision is not None else None,
                        'qa_value': float(qa_value[i, j]) if qa_value is not None else None,
                        'ingestion_time': datetime.now().isoformat()
                    }
                    data_records.append(record)
        
        ds.close()
        return data_records
    
    except Exception as e:
        print(f"Error extracting data from item {item.id}: {str(e)}")
        return []

def create_eventhouse_table(kcsb, database):
    """Create Eventhouse table if it doesn't exist"""
    create_table_command = f"""
    .create-merge table {EVENTHOUSE_TABLE} (
        timestamp: datetime,
        item_id: string,
        latitude: real,
        longitude: real,
        ch4_mixing_ratio: real,
        ch4_mixing_ratio_precision: real,
        qa_value: real,
        ingestion_time: datetime
    )
    """
    
    try:
        client = KustoClient(kcsb)
        client.execute(database, create_table_command)
        print(f"Table {EVENTHOUSE_TABLE} created or already exists")
    except KustoServiceError as e:
        print(f"Error creating table: {str(e)}")

def ingest_to_eventhouse(records, kcsb, database):
    """Ingest records to Eventhouse"""
    if not records:
        print("No records to ingest")
        return
    
    print(f"Ingesting {len(records)} records to Eventhouse")
    
    # Convert records to DataFrame
    df = pd.DataFrame(records)
    
    # Create ingest client
    ingest_client = QueuedIngestClient(kcsb)
    
    # Set ingestion properties
    ingestion_props = IngestionProperties(
        database=database,
        table=EVENTHOUSE_TABLE,
        data_format=DataFormat.CSV,
    )
    
    # Ingest data
    try:
        ingest_client.ingest_from_dataframe(df, ingestion_properties=ingestion_props)
        print(f"Successfully queued {len(records)} records for ingestion")
    except Exception as e:
        print(f"Error ingesting data: {str(e)}")

def check_for_new_data(items, kcsb, database):
    """Check which items are new (not already in Eventhouse)"""
    if not items:
        return []
    
    # Query existing item IDs
    query = f"{EVENTHOUSE_TABLE} | distinct item_id | project item_id"
    
    try:
        client = KustoClient(kcsb)
        response = client.execute(database, query)
        existing_ids = set([row['item_id'] for row in response.primary_results[0]])
        
        # Filter out items that already exist
        new_items = [item for item in items if item.id not in existing_ids]
        print(f"Found {len(new_items)} new items out of {len(items)} total items")
        return new_items
    
    except Exception as e:
        print(f"Error checking for existing data: {str(e)}")
        # If error, process all items
        return items

# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main():
    """Main pipeline execution"""
    print("=" * 70)
    print("Sentinel-5P CH4 Data Pipeline Started")
    print(f"Execution time: {datetime.now().isoformat()}")
    print("=" * 70)
    
    # Setup Eventhouse connection
    # Use Azure AD authentication in Fabric
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(EVENTHOUSE_CLUSTER_URI)
    
    # Create table if it doesn't exist
    create_eventhouse_table(kcsb, EVENTHOUSE_DATABASE)
    
    # Get date range
    start_date, end_date = get_date_range()
    
    # Search for data
    items = search_sentinel_data(start_date, end_date)
    
    if not items:
        print("No items found for the specified date range")
        return
    
    # Check for new items
    new_items = check_for_new_data(items, kcsb, EVENTHOUSE_DATABASE)
    
    if not new_items:
        print("No new items to process")
        return
    
    # Process each new item
    all_records = []
    for idx, item in enumerate(new_items, 1):
        print(f"\nProcessing item {idx}/{len(new_items)}: {item.id}")
        records = extract_ch4_data(item)
        all_records.extend(records)
    
    # Ingest to Eventhouse
    if all_records:
        #ingest_to_eventhouse(all_records, kcsb, EVENTHOUSE_DATABASE)
        print(f"\nPipeline completed successfully!")
        print(f"Total records processed: {len(all_records)}")
    else:
        print("\nNo valid records extracted from items")
    
    print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if __name__ == "__main__":
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
