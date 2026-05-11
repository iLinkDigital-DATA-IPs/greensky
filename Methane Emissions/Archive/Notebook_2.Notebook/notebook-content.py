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

%pip install xarray cfgrib eccodes pandas numpy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install cdsapi pandas pyarrow

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import cdsapi
import pandas as pd
import xarray as xr
from datetime import datetime, timedelta
import os
import tempfile
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CDS_API_URL = "https://ads.atmosphere.copernicus.eu/api"
CDS_API_KEY = "01b3bc16e-9d94-4f15-bed4-4c60d78a2f27"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url: https://ads.atmosphere.copernicus.eu/api
key: 0d0703ed-c18f-4544-a341-73e9d7772cdf

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

LAKEHOUSE_PATH = "Files/copernicus_methane/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

PRESSURE_LEVELS = [
    "1", "2", "3", "5", "7", "10", "20", "30", "50", "70",
    "100", "150", "200", "250", "300", "400", "500",
    "600", "700", "800", "850", "900", "925", "950", "1000"
]

MODEL_LEVEL = "137"
LEADTIME_HOURS = ["0", "3", "6"]
VARIABLE = "methane"
FORECAST_TYPE = "forecast"

# Geographic area - Whole available region [North, West, South, East]
GEOGRAPHICAL_AREA = [90, -180, -90, 180]  # Global coverage

# Data format
DATA_FORMAT = "grib"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def setup_cds_client():
    """Initialize CDS API client with proper configuration"""
    
    # Validate API key format
    if not CDS_API_KEY or CDS_API_KEY == "1b3bc16e-9d94-4f15-bed4-4c60d78a2f27":
        raise ValueError(
            "Please set your CDS_API_KEY\n"
            "Get it from: https://ads.atmosphere.copernicus.eu/user/login\n"
            "Navigate to your profile and copy the API key"
        )
    
    # Create .cdsapirc configuration
    # The new format just needs the API key (no UID)
    cds_config = f"url: {CDS_API_URL}\nkey: {CDS_API_KEY}"
    
    # Write config to home directory
    config_path = os.path.expanduser("~/.cdsapirc")
    
    try:
        with open(config_path, "w") as f:
            f.write(cds_config)
        print(f"✅ CDS API configuration written to {config_path}")
    except Exception as e:
        print(f"⚠️ Warning: Could not write config file: {e}")
    
    # Initialize client
    try:
        client = cdsapi.Client(url=CDS_API_URL, key=CDS_API_KEY)
        print("✅ CDS API client initialized successfully")
        return client
    except Exception as e:
        raise ConnectionError(f"Failed to initialize CDS client: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_methane_data(client, target_date=None, leadtime_hours=None, pressure_levels=None):
    """
    Fetch methane data from Copernicus CAMS with all parameters properly configured
    
    Parameters:
    - client: CDS API client
    - target_date: date string in format 'YYYY-MM-DD' or list of dates (default: today)
    - leadtime_hours: list of leadtime hours as strings (default: LEADTIME_HOURS)
    - pressure_levels: list of pressure levels as strings (default: PRESSURE_LEVELS)
    
    Returns:
    - Path to downloaded GRIB file
    """
    
    # Set defaults
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")
    
    if leadtime_hours is None:
        leadtime_hours = LEADTIME_HOURS
    
    if pressure_levels is None:
        pressure_levels = PRESSURE_LEVELS
    
    # Ensure target_date is a list for API
    if isinstance(target_date, str):
        date_list = [target_date]
    else:
        date_list = target_date
    
    print(f"\n{'='*70}")
    print(f"FETCHING METHANE DATA")
    print(f"{'='*70}")
    print(f"Date(s): {date_list}")
    print(f"Variable: {VARIABLE}")
    print(f"Type: {FORECAST_TYPE}")
    print(f"Pressure levels: {len(pressure_levels)} levels ({pressure_levels[0]} to {pressure_levels[-1]} hPa)")
    print(f"Leadtime hours: {leadtime_hours}")
    print(f"Geographic area: Global ({GEOGRAPHICAL_AREA})")
    print(f"Format: {DATA_FORMAT}")
    print(f"{'='*70}\n")
    
    # Create temporary file for download
    temp_file = tempfile.NamedTemporaryFile(suffix='.grib', delete=False)
    temp_path = temp_file.name
    temp_file.close()
    
    try:
        # Build API request parameters - PROPERLY FORMATTED
        request_params = {
            'variable': VARIABLE,
            'date': date_list,  # Single date or list of dates
            'type': FORECAST_TYPE,
            'leadtime_hour': leadtime_hours,  # List of hours as strings
            'pressure_level': pressure_levels,  # List of pressure levels as strings
            'area': GEOGRAPHICAL_AREA,  # [N, W, S, E]
            'format': DATA_FORMAT
        }
        
        print("📡 API Request Parameters:")
        print(json.dumps(request_params, indent=2))
        print("\n⏳ Submitting request to Copernicus ADS...")
        print("   This may take several minutes depending on data volume...")
        
        # Submit request to Copernicus
        client.retrieve(
            'cams-global-greenhouse-gas-forecasts',
            request_params,
            temp_path
        )
        
        # Check file size
        file_size_mb = os.path.getsize(temp_path) / (1024 * 1024)
        print(f"\n✅ Data downloaded successfully!")
        print(f"   File: {temp_path}")
        print(f"   Size: {file_size_mb:.2f} MB")
        
        return temp_path
        
    except Exception as e:
        print(f"\n❌ Error fetching data: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_grib_to_dataframe(grib_path):
    """
    Convert GRIB file to pandas DataFrame with proper column mapping
    """
    print("\n⚙️ Processing GRIB file...")
    
    try:
        # Open GRIB file with xarray using cfgrib engine
        print("   Opening GRIB file with xarray...")
        ds = xr.open_dataset(grib_path, engine='cfgrib')
        
        print(f"   Dataset dimensions: {dict(ds.dims)}")
        print(f"   Dataset variables: {list(ds.data_vars)}")
        print(f"   Dataset coordinates: {list(ds.coords)}")
        
        # Convert to DataFrame
        print("   Converting to DataFrame...")
        df = ds.to_dataframe().reset_index()
        
        # Rename columns to be more user-friendly
        column_mapping = {
            'time': 'forecast_time',
            'valid_time': 'valid_time',
            'step': 'leadtime_hours',
            'isobaricInhPa': 'pressure_level_hpa',
            'latitude': 'latitude',
            'longitude': 'longitude',
            'ch4': 'methane_kg_per_kg',  # Methane mass mixing ratio
            'ch4_c': 'methane_column',    # If column integrated data exists
        }
        
        # Apply renaming for columns that exist
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df.rename(columns={old_col: new_col}, inplace=True)
        
        # Add metadata columns
        df['ingestion_timestamp'] = datetime.now()
        df['data_source'] = 'copernicus_cams'
        df['variable'] = 'methane'
        df['model'] = 'cams_global_ghg_forecast'
        df['model_level'] = MODEL_LEVEL
        
        # Convert leadtime to hours if it's in different format
        if 'leadtime_hours' in df.columns:
            # Sometimes step is in timedelta format, convert to hours
            if df['leadtime_hours'].dtype == 'timedelta64[ns]':
                df['leadtime_hours'] = df['leadtime_hours'].dt.total_seconds() / 3600
            df['leadtime_hours'] = df['leadtime_hours'].astype(int)
        
        # Data type optimization
        if 'pressure_level_hpa' in df.columns:
            df['pressure_level_hpa'] = df['pressure_level_hpa'].astype('int16')
        if 'leadtime_hours' in df.columns:
            df['leadtime_hours'] = df['leadtime_hours'].astype('int8')
        
        # Sort by time and location for better compression
        sort_columns = [col for col in ['forecast_time', 'pressure_level_hpa', 'latitude', 'longitude'] 
                       if col in df.columns]
        if sort_columns:
            df = df.sort_values(sort_columns)
        
        print(f"\n✅ Processing complete!")
        print(f"   Records: {len(df):,}")
        print(f"   Columns: {len(df.columns)}")
        print(f"   Memory usage: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
        
        # Display data summary
        print(f"\n📊 Data Summary:")
        if 'pressure_level_hpa' in df.columns:
            print(f"   Pressure levels: {sorted(df['pressure_level_hpa'].unique())}")
        if 'leadtime_hours' in df.columns:
            print(f"   Leadtime hours: {sorted(df['leadtime_hours'].unique())}")
        if 'methane_kg_per_kg' in df.columns:
            print(f"   Methane range: {df['methane_kg_per_kg'].min():.2e} to {df['methane_kg_per_kg'].max():.2e} kg/kg")
        
        return df
        
    except Exception as e:
        print(f"❌ Error processing GRIB file: {e}")
        print(f"   Make sure cfgrib and eccodes are installed:")
        print(f"   %pip install cfgrib eccodes")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_to_lakehouse(df, lakehouse_path, date_str):
    """
    Save DataFrame to Fabric Lakehouse in partitioned Parquet format
    """
    print(f"\n💾 Saving data to Lakehouse...")
    
    # Extract year, month, day from date string
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    year = date_obj.strftime("%Y")
    month = date_obj.strftime("%m")
    day = date_obj.strftime("%d")
    
    # Create partition path
    partition_path = f"{lakehouse_path}year={year}/month={month}/day={day}/"
    
    # Ensure directory exists
    os.makedirs(partition_path, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"{partition_path}methane_{timestamp}.parquet"
    
    try:
        # Save as Parquet with optimal compression
        df.to_parquet(
            file_path, 
            index=False, 
            compression='snappy',
            engine='pyarrow'
        )
        
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        
        print(f"✅ Data saved successfully!")
        print(f"   Path: {file_path}")
        print(f"   Size: {file_size_mb:.2f} MB")
        print(f"   Records: {len(df):,}")
        
        return file_path
        
    except Exception as e:
        print(f"❌ Error saving to Lakehouse: {e}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_to_delta_table(df, table_name="methane_data"):
    """
    Save directly to Delta table in Fabric (requires Spark)
    """
    print(f"\n💾 Saving to Delta table: {table_name}...")
    
    try:
        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(df)
        
        # Write to Delta table with schema evolution enabled
        spark_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "false") \
            .saveAsTable(table_name)
        
        print(f"✅ Data appended to Delta table: {table_name}")
        print(f"   Records added: {len(df):,}")
        
        # Show table info
        row_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
        print(f"   Total records in table: {row_count:,}")
        
    except Exception as e:
        print(f"❌ Error saving to Delta table: {e}")
        raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_data(df):
    """
    Validate methane data quality
    """
    print(f"\n🔍 Validating data quality...")
    
    checks = []
    all_passed = True
    
    # Check 1: Expected number of pressure levels
    if 'pressure_level_hpa' in df.columns:
        expected_levels = set([int(x) for x in PRESSURE_LEVELS])
        actual_levels = set(df['pressure_level_hpa'].unique())
        passed = expected_levels == actual_levels
        checks.append(('Pressure Levels', passed, f"Expected {len(expected_levels)}, Got {len(actual_levels)}"))
        all_passed = all_passed and passed
    
    # Check 2: No null methane values
    if 'methane_kg_per_kg' in df.columns:
        null_count = df['methane_kg_per_kg'].isnull().sum()
        passed = null_count == 0
        checks.append(('No Null Methane Values', passed, f"Null count: {null_count}"))
        all_passed = all_passed and passed
    
    # Check 3: Expected leadtime hours
    if 'leadtime_hours' in df.columns:
        expected_hours = set([int(x) for x in LEADTIME_HOURS])
        actual_hours = set(df['leadtime_hours'].unique())
        passed = expected_hours.issubset(actual_hours)
        checks.append(('Leadtime Hours', passed, f"Expected {expected_hours}, Got {actual_hours}"))
        all_passed = all_passed and passed
    
    # Check 4: Reasonable value range for methane (typically 1e-9 to 5e-6 kg/kg)
    if 'methane_kg_per_kg' in df.columns:
        value_range = df['methane_kg_per_kg'].between(0, 1e-5).all()
        min_val = df['methane_kg_per_kg'].min()
        max_val = df['methane_kg_per_kg'].max()
        checks.append(('Value Range', value_range, f"Range: {min_val:.2e} to {max_val:.2e}"))
        all_passed = all_passed and value_range
    
    # Check 5: Geographic coverage
    if 'latitude' in df.columns and 'longitude' in df.columns:
        lat_range = (df['latitude'].min(), df['latitude'].max())
        lon_range = (df['longitude'].min(), df['longitude'].max())
        passed = lat_range[0] <= -80 and lat_range[1] >= 80  # Near global
        checks.append(('Geographic Coverage', passed, f"Lat: {lat_range}, Lon: {lon_range}"))
        all_passed = all_passed and passed
    
    # Print results
    print(f"\n{'Check':<30} {'Status':<10} {'Details'}")
    print(f"{'-'*70}")
    for check_name, passed, details in checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{check_name:<30} {status:<10} {details}")
    
    print(f"\n{'='*70}")
    if all_passed:
        print("✅ All validation checks passed!")
    else:
        print("⚠️ Some validation checks failed - review data carefully")
    print(f"{'='*70}\n")
    
    return all_passed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main(run_date=None, save_format="parquet", validate=True):
    """
    Main execution function with all parameters properly used
    
    Parameters:
    - run_date: date string 'YYYY-MM-DD' or list of dates (default: today)
    - save_format: 'parquet' or 'delta' (default: 'parquet')
    - validate: whether to run data quality validation (default: True)
    
    Returns:
    - DataFrame with methane data
    """
    
    start_time = datetime.now()
    
    try:
        print("\n" + "="*70)
        print("COPERNICUS CAMS METHANE DATA INGESTION")
        print("="*70)
        
        # Initialize CDS client
        print("\n🔧 Step 1: Initializing Copernicus CDS API client...")
        client = setup_cds_client()
        
        # Set target date
        if run_date is None:
            run_date = datetime.now().strftime("%Y-%m-%d")
        
        # Fetch data with ALL parameters properly configured
        print(f"\n🌍 Step 2: Fetching methane data from Copernicus...")
        grib_path = fetch_methane_data(
            client, 
            target_date=run_date,
            leadtime_hours=LEADTIME_HOURS,
            pressure_levels=PRESSURE_LEVELS
        )
        
        # Process GRIB to DataFrame
        print(f"\n⚙️ Step 3: Processing GRIB file...")
        df = process_grib_to_dataframe(grib_path)
        
        # Display sample data
        print(f"\n📋 Sample Data (first 5 rows):")
        print(df.head().to_string())
        
        # Validate data quality
        if validate:
            print(f"\n🔍 Step 4: Validating data quality...")
            validation_passed = validate_data(df)
        
        # Save data
        print(f"\n💾 Step 5: Saving data to Fabric...")
        if save_format == "delta":
            save_to_delta_table(df)
        else:
            save_to_lakehouse(df, LAKEHOUSE_PATH, run_date if isinstance(run_date, str) else run_date[0])
        
        # Cleanup temporary file
        if os.path.exists(grib_path):
            os.remove(grib_path)
            print(f"🧹 Cleaned up temporary file: {grib_path}")
        
        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds()
        
        print(f"\n{'='*70}")
        print(f"✅ DATA INGESTION COMPLETED SUCCESSFULLY!")
        print(f"{'='*70}")
        print(f"Execution time: {execution_time:.2f} seconds")
        print(f"Records processed: {len(df):,}")
        print(f"Data date: {run_date}")
        print(f"{'='*70}\n")
        
        return df
        
    except Exception as e:
        print(f"\n{'='*70}")
        print(f"❌ ERROR IN DATA INGESTION")
        print(f"{'='*70}")
        print(f"Error: {e}")
        print(f"{'='*70}\n")
        raise



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def batch_ingest(start_date, end_date, save_format="parquet"):
    """
    Ingest data for a date range (one request per day)
    
    Parameters:
    - start_date: 'YYYY-MM-DD'
    - end_date: 'YYYY-MM-DD'
    - save_format: 'parquet' or 'delta'
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    total_days = (end - start).days + 1
    print(f"\n🚀 Starting batch ingestion for {total_days} days")
    print(f"   From: {start_date}")
    print(f"   To: {end_date}\n")
    
    success_count = 0
    fail_count = 0
    
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        
        try:
            print(f"\n{'#'*70}")
            print(f"Processing date {success_count + fail_count + 1}/{total_days}: {date_str}")
            print(f"{'#'*70}")
            
            main(run_date=date_str, save_format=save_format)
            success_count += 1
            
        except Exception as e:
            print(f"❌ Failed for date {date_str}: {e}")
            fail_count += 1
        
        current += timedelta(days=1)
    
    print(f"\n{'='*70}")
    print(f"BATCH INGESTION COMPLETE")
    print(f"{'='*70}")
    print(f"✅ Successful: {success_count}/{total_days}")
    print(f"❌ Failed: {fail_count}/{total_days}")
    print(f"{'='*70}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def check_existing_data(date_str):
    """Check if data already exists for a given date"""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    partition_path = f"{LAKEHOUSE_PATH}year={date_obj.year}/month={date_obj.month:02d}/day={date_obj.day:02d}/"
    
    if os.path.exists(partition_path):
        files = os.listdir(partition_path)
        if files:
            print(f"⚠️ Data already exists for {date_str}: {len(files)} file(s)")
            return True
    return False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def query_delta_table(table_name="methane_data", limit=10):
    """
    Query the Delta table to verify data
    """
    query = f"""
    SELECT 
        forecast_time,
        pressure_level_hpa,
        leadtime_hours,
        COUNT(*) as record_count,
        AVG(methane_kg_per_kg) as avg_methane,
        MIN(methane_kg_per_kg) as min_methane,
        MAX(methane_kg_per_kg) as max_methane
    FROM {table_name}
    GROUP BY forecast_time, pressure_level_hpa, leadtime_hours
    ORDER BY forecast_time DESC, pressure_level_hpa
    LIMIT {limit}
    """
    
    return spark.sql(query).display()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def print_configuration():
    """Print current configuration for verification"""
    print("\n" + "="*70)
    print("CURRENT CONFIGURATION")
    print("="*70)
    print(f"Variable: {VARIABLE}")
    print(f"Pressure Levels: {len(PRESSURE_LEVELS)} levels")
    print(f"  {PRESSURE_LEVELS}")
    print(f"Model Level: {MODEL_LEVEL}")
    print(f"Leadtime Hours: {LEADTIME_HOURS}")
    print(f"Forecast Type: {FORECAST_TYPE}")
    print(f"Geographic Area: {GEOGRAPHICAL_AREA} (Global)")
    print(f"Data Format: {DATA_FORMAT}")
    print(f"Lakehouse Path: {LAKEHOUSE_PATH}")
    print(f"API URL: {CDS_API_URL}")
    print(f"API Key Configured: {'Yes' if CDS_API_KEY != 'YOUR_UID:YOUR_API_KEY_HERE' else 'No - PLEASE SET!'}")
    print("="*70 + "\n")

# Run this to verify your configuration
# print_configuration()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TEST BLOCK 1: Install and Import Libraries
# ============================================
print("Installing required packages...")
%pip install boto3 s3fs xarray netCDF4 h5netcdf pandas pyarrow --quiet
print("✓ Packages installed successfully\n")

import boto3
import s3fs
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json

print("✓ All libraries imported successfully")
print(f"boto3 version: {boto3.__version__}")
print(f"xarray version: {xr.__version__}")
print(f"pandas version: {pd.__version__}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ============================================
# TEST BLOCK 2: AWS S3 Authentication
# ============================================
print("\n" + "="*50)
print("TEST: AWS S3 Authentication")
print("="*50)

# AWS Credentials (from your provided credentials)
AWS_CREDENTIALS = {
    "accessKeyId": "ASIAVSNEGXJAKVTHPGGR",
    "secretAccessKey": "dUt7PFG3umaADMN2ThB1AQmIg/Z0dtwKd80qkSOW",
    "sessionToken": "IQoJb3JpZ2luX2VjELf//////////wEaCXVzLXdlc3QtMiJIMEYCIQDO47xTDmJlNsGJk2bX04kdd0O3NDnUJf0yrmvXpkvcBQIhAPaarzAiCWeztVLWMLfqIArys+n3eEezXFCuw2xM/XdQKtcCCID//////////wEQAxoMMzgzMTMzMzM0MDgwIgxGkYaLA2mHX6BwVzYqqwKjsnawtaYmPu3LINbxCwAYgIZT3aDAGVBFBoit2+ywCXQ8aRmVMbquX4BiJ05WYJqo4tR0m+AMpIJgwMMQ3WiiWFn+Kk63DOdCgYlTaW9EsmQY6ygImu4FD6QC2nyWFLzlGWu9dYffLJ4jS/cSvG1gw7bUyIHluLqbO/EkYZR67E9mk5KMDrL9kozz5EFugJjJQJXgP5Qjwp+erfXxxZWVn0CdyJurG47kJNlooyr5JtC+Jk3VJaeE/CObYLkiCwy0K42ie5OA5obDndsgyC4rRPRo33ULYo+J4Up7NVE67JkFQZQKxuSvr6QGFd28jiM6gQ2WLvQQ28FS4S8ex3OHAtmxxABkXz8na1yTmkxjwlZNzJhyN903vb12zrt+SlxVkWb4kKN0sUBBeDCYvJrJBjqcAW2Uc+Q3nnkIwiZojEOTfIbDdKvlL1ft0pCNaxK063uD5xOl+X1+2I7mzsL8QIrfKSNQ0Ph9+KU8fPPA4Ga1fsPHZKmGTWBFX/veFFTIqcRkXIyNt6xs0qfXifEIprwy9ZwOuFQ6OnTgau6EdSOlz+4OLh5ivtdu4aI3Bv8By44vRCfWjGmNm+aWYoVqNGSnT9fD/oFlfOwuBxgi2g==",
    "expiration": "2025-11-26 07:28:40+00:00"
}

# GES DISC S3 bucket info
S3_BUCKET = "gesdisc-cumulus-prod-protected"
S3_PREFIX = "TROPOMI_L2__CH4____HiR/S5P_L2__CH4____HiR.2/"

try:
    # Create S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_CREDENTIALS['accessKeyId'],
        aws_secret_access_key=AWS_CREDENTIALS['secretAccessKey'],
        aws_session_token=AWS_CREDENTIALS['sessionToken'],
        region_name='us-west-2'
    )
    
    # Test connection by listing bucket
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=S3_PREFIX,
        MaxKeys=1
    )
    
    print("✓ AWS S3 Connection SUCCESSFUL!")
    print(f"✓ Bucket: {S3_BUCKET}")
    print(f"✓ Credentials valid until: {AWS_CREDENTIALS['expiration']}")
    
except Exception as e:
    print(f"✗ AWS S3 Connection Error: {str(e)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
