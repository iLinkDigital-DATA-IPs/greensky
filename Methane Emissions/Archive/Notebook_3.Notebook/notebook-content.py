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

%pip install boto3 --quiet
%pip install rasterio --quiet
%pip install numpy --quiet


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 2: Import Libraries
# ============================================================================

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

print("✓ All libraries imported successfully")
print(f"✓ Spark Version: {spark.version}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 3: Configuration and Parameters
# ============================================================================

# ===== CONFIGURATION =====
S3_BUCKET = "meeo-s5p"
S3_REGION = "eu-central-1"
PRODUCT_TYPE = "L2__CH4___"
PROCESSING_STREAM = "COGT/OFFL"  # Options: COGT/OFFL, COGT/NRTI, OFFL, NRTI

# ===== LAKEHOUSE PATHS =====
LAKEHOUSE_BASE = "/lakehouse/default/Files"
RAW_DATA_PATH = f"{LAKEHOUSE_BASE}/sentinel5p/methane/raw"
METADATA_PATH = f"{LAKEHOUSE_BASE}/sentinel5p/methane/metadata"

# ===== DATE PARAMETERS =====
# For single date testing (use this for now)
TARGET_DATE = "2024-11-15"  # Format: YYYY-MM-DD

# For pipeline parameter (uncomment when integrating with pipeline)
# TARGET_DATE = spark.conf.get("pipeline.parameters.TargetDate", "2024-11-15")

# For dynamic date (7 days ago - uncomment when ready)
# TARGET_DATE = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

print("="*80)
print("SENTINEL-5P METHANE DATA INGESTION - CONFIGURATION")
print("="*80)
print(f"S3 Bucket       : s3://{S3_BUCKET}")
print(f"Product Type    : {PRODUCT_TYPE}")
print(f"Processing Stream: {PROCESSING_STREAM}")
print(f"Target Date     : {TARGET_DATE}")
print(f"Raw Data Path   : {RAW_DATA_PATH}")
print("="*80)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 4: Initialize S3 Client (Anonymous Access)
# ============================================================================

# Create S3 client with unsigned requests (no AWS credentials needed)
s3_client = boto3.client(
    's3',
    config=Config(signature_version=UNSIGNED),
    region_name=S3_REGION
)

print("✓ S3 Client initialized (Anonymous/Public Access)")
print(f"✓ Connected to bucket: {S3_BUCKET}")

# Test connection
try:
    response = s3_client.head_bucket(Bucket=S3_BUCKET)
    print("✓ S3 Bucket is accessible")
except Exception as e:
    print(f"✗ Error accessing bucket: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 5: Helper Functions
# ============================================================================

def construct_s3_prefix(target_date, processing_stream, product_type):
    """
    Construct S3 prefix path from date and parameters
    
    Args:
        target_date: Date string in format YYYY-MM-DD
        processing_stream: e.g., COGT/OFFL
        product_type: e.g., L2__CH4___
    
    Returns:
        S3 prefix path
    """
    date_obj = datetime.strptime(target_date, "%Y-%m-%d")
    year = date_obj.strftime("%Y")
    month = date_obj.strftime("%m")
    day = date_obj.strftime("%d")
    
    prefix = f"{processing_stream}/{product_type}/{year}/{month}/{day}/"
    return prefix, year, month, day


def list_s3_files(bucket, prefix, file_filter="_methane_mixing_ratio.tif"):
    """
    List all files in S3 bucket with given prefix and filter
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix path
        file_filter: String to filter files (default: methane files)
    
    Returns:
        List of dictionaries with file information
    """
    files_list = []
    
    try:
        # Use paginator for large result sets
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_key = obj['Key']
                    
                    # Apply filter
                    if file_filter in file_key:
                        files_list.append({
                            'key': file_key,
                            'filename': os.path.basename(file_key),
                            'size_bytes': obj['Size'],
                            'size_mb': round(obj['Size'] / (1024 * 1024), 2),
                            'last_modified': obj['LastModified'].isoformat(),
                            'e_tag': obj['ETag'].strip('"')
                        })
        
        return files_list
    
    except Exception as e:
        print(f"✗ Error listing files: {str(e)}")
        return []


def download_file(bucket, s3_key, local_path):
    """
    Download single file from S3 to local path
    
    Args:
        bucket: S3 bucket name
        s3_key: S3 object key
        local_path: Local destination path
    
    Returns:
        Boolean: True if success, False if failed
    """
    try:
        # Create directory if doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Download file
        s3_client.download_file(bucket, s3_key, local_path)
        return True
    
    except Exception as e:
        print(f"✗ Error downloading {s3_key}: {str(e)}")
        return False


print("✓ Helper functions defined successfully")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 6: List Available Files from S3
# ============================================================================

# Construct S3 path
s3_prefix, year, month, day = construct_s3_prefix(
    TARGET_DATE, 
    PROCESSING_STREAM, 
    PRODUCT_TYPE
)

print(f"\n{'='*80}")
print(f"LISTING FILES FROM S3")
print(f"{'='*80}")
print(f"S3 Path: s3://{S3_BUCKET}/{s3_prefix}")
print(f"Searching for: *_methane_mixing_ratio.tif files")
print(f"{'='*80}\n")

# List files
methane_files = list_s3_files(S3_BUCKET, s3_prefix)

# Display results
if len(methane_files) == 0:
    print("❌ NO FILES FOUND!")
    print("\nPossible reasons:")
    print("  1. Date is too recent (OFFL data has ~7 day delay)")
    print("  2. No data available for this date")
    print("  3. Wrong path or product type")
    print(f"\nTry changing TARGET_DATE to 7-10 days ago")
else:
    print(f"✓ Found {len(methane_files)} methane files")
    total_size_mb = sum([f['size_mb'] for f in methane_files])
    print(f"✓ Total size: {total_size_mb:.2f} MB")
    
    print("\n" + "-"*80)
    print("SAMPLE FILES:")
    print("-"*80)
    for i, file_info in enumerate(methane_files[:3], 1):
        print(f"\n{i}. {file_info['filename']}")
        print(f"   Size: {file_info['size_mb']} MB")
        print(f"   Modified: {file_info['last_modified']}")
    
    if len(methane_files) > 3:
        print(f"\n... and {len(methane_files) - 3} more files")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 7: Download Files to Lakehouse
# ============================================================================

if len(methane_files) > 0:
    print(f"\n{'='*80}")
    print(f"DOWNLOADING FILES TO LAKEHOUSE")
    print(f"{'='*80}\n")
    
    # Create local directory
    local_dir = f"{RAW_DATA_PATH}/{TARGET_DATE}"
    os.makedirs(local_dir, exist_ok=True)
    print(f"✓ Created directory: {local_dir}\n")
    
    # Download counters
    success_count = 0
    failed_count = 0
    download_results = []
    
    # Download each file
    for i, file_info in enumerate(methane_files, 1):
        s3_key = file_info['key']
        filename = file_info['filename']
        local_path = f"{local_dir}/{filename}"
        
        print(f"[{i}/{len(methane_files)}] Downloading: {filename}")
        print(f"    Size: {file_info['size_mb']} MB ... ", end="")
        
        # Download file
        if download_file(S3_BUCKET, s3_key, local_path):
            success_count += 1
            print("✓ SUCCESS")
            
            # Add to results
            file_info['local_path'] = local_path
            file_info['download_status'] = 'success'
            file_info['download_timestamp'] = datetime.now().isoformat()
            download_results.append(file_info)
        else:
            failed_count += 1
            print("✗ FAILED")
            
            file_info['local_path'] = None
            file_info['download_status'] = 'failed'
            file_info['download_timestamp'] = datetime.now().isoformat()
            download_results.append(file_info)
    
    # Summary
    print(f"\n{'='*80}")
    print(f"DOWNLOAD SUMMARY")
    print(f"{'='*80}")
    print(f"✓ Successfully downloaded: {success_count} files")
    if failed_count > 0:
        print(f"✗ Failed downloads: {failed_count} files")
    print(f"📂 Files saved to: {local_dir}")
    print(f"{'='*80}\n")
    
else:
    print("\n⚠️ No files to download. Skipping download phase.")
    download_results = []


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TROUBLESHOOTING CELL 1: Test S3 Connection and List Root
# ============================================================================
# Run this cell to verify S3 access and see what's available

import boto3
from botocore import UNSIGNED
from botocore.client import Config

S3_BUCKET = "meeo-s5p"
S3_REGION = "eu-central-1"

# Create S3 client
s3_client = boto3.client(
    's3',
    config=Config(signature_version=UNSIGNED),
    region_name=S3_REGION
)

print("Testing S3 Access...")
print("="*80)

# Test 1: Check bucket accessibility
try:
    response = s3_client.head_bucket(Bucket=S3_BUCKET)
    print("✓ Bucket is accessible")
except Exception as e:
    print(f"✗ Cannot access bucket: {str(e)}")

# Test 2: List root level folders
print("\nRoot level folders in bucket:")
print("-"*80)
try:
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET,
        Delimiter='/',
        MaxKeys=100
    )
    
    if 'CommonPrefixes' in response:
        for prefix in response['CommonPrefixes']:
            print(f"  📁 {prefix['Prefix']}")
    else:
        print("  No folders found at root level")
except Exception as e:
    print(f"✗ Error: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TROUBLESHOOTING CELL 2: Check Available Processing Streams
# ============================================================================
# This checks what processing streams (OFFL, NRTI, COGT) are available

print("\n" + "="*80)
print("CHECKING AVAILABLE PROCESSING STREAMS")
print("="*80)

processing_streams = ['OFFL', 'NRTI', 'COGT/OFFL', 'COGT/NRTI', 'RPRO']

for stream in processing_streams:
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{stream}/",
            Delimiter='/',
            MaxKeys=10
        )
        
        if 'CommonPrefixes' in response or 'Contents' in response:
            print(f"✓ {stream}/ - EXISTS")
        else:
            print(f"✗ {stream}/ - NOT FOUND")
    except Exception as e:
        print(f"✗ {stream}/ - ERROR: {str(e)}")


# ============================================================================
# TROUBLESHOOTING CELL 3: Check Available Product Types
# ============================================================================
# This checks what product types are available under OFFL

print("\n" + "="*80)
print("CHECKING AVAILABLE PRODUCT TYPES IN OFFL/")
print("="*80)

try:
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix='OFFL/',
        Delimiter='/',
        MaxKeys=100
    )
    
    if 'CommonPrefixes' in response:
        print("Available products in OFFL/:")
        for prefix in response['CommonPrefixes']:
            product = prefix['Prefix'].replace('OFFL/', '').rstrip('/')
            print(f"  📦 {product}")
            
            # Check if it's the CH4 product
            if 'CH4' in product:
                print(f"     ⭐ METHANE PRODUCT FOUND!")
    else:
        print("No products found in OFFL/")
except Exception as e:
    print(f"✗ Error: {str(e)}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TROUBLESHOOTING CELL 4: Check Recent Dates with Data
# ============================================================================
# This finds what dates actually have data available

print("\n" + "="*80)
print("FINDING DATES WITH AVAILABLE DATA")
print("="*80)

from datetime import datetime, timedelta

# Try different paths
test_paths = [
    'OFFL/L2__CH4___',
    'COGT/OFFL/L2__CH4___',
    'NRTI/L2__CH4___',
    'COGT/NRTI/L2__CH4___'
]

for base_path in test_paths:
    print(f"\nChecking: {base_path}/")
    print("-"*80)
    
    # Check last 30 days
    found_dates = []
    
    for days_ago in range(1, 31):
        check_date = datetime.now() - timedelta(days=days_ago)
        year = check_date.strftime("%Y")
        month = check_date.strftime("%m")
        day = check_date.strftime("%d")
        
        prefix = f"{base_path}/{year}/{month}/{day}/"
        
        try:
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=prefix,
                MaxKeys=1  # Just check if any file exists
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                found_dates.append(f"{year}-{month}-{day}")
                
                # Show first date found
                if len(found_dates) == 1:
                    print(f"  ✓ Data found for: {year}-{month}-{day}")
                    
                    # List some files
                    file_count = response['KeyCount']
                    print(f"    Files in this date: ~{file_count}+")
                    
                    if len(found_dates) >= 3:
                        break
        except:
            continue
    
    if found_dates:
        print(f"\n  ✓ Found data in {len(found_dates)} recent dates")
        print(f"    Most recent: {found_dates[0]}")
        if len(found_dates) > 1:
            print(f"    Oldest checked: {found_dates[-1]}")
    else:
        print(f"  ✗ No data found in last 30 days")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TROUBLESHOOTING CELL 5: Deep Dive - List Actual Files for a Specific Path
# ============================================================================
# Once you find a working path, use this to see actual files

print("\n" + "="*80)
print("LISTING ACTUAL FILES (DEEP DIVE)")
print("="*80)

# MODIFY THESE BASED ON RESULTS FROM CELL 4:
TEST_PATH = "OFFL/L2__CH4___/2025/08/01"  # Change this to a working path from Cell 4

print(f"\nListing files in: s3://{S3_BUCKET}/{TEST_PATH}/")
print("-"*80)

try:
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=f"{TEST_PATH}/",
        MaxKeys=20  # Show first 20 files
    )
    
    if 'Contents' in response:
        files = response['Contents']
        print(f"✓ Found {len(files)} files (showing first 20)")
        
        # Categorize files
        nc_files = [f for f in files if f['Key'].endswith('.nc')]
        tif_files = [f for f in files if f['Key'].endswith('.tif')]
        methane_files = [f for f in files if '_methane_mixing_ratio' in f['Key'] or 'CH4' in f['Key']]
        
        print(f"\nFile breakdown:")
        print(f"  NetCDF files (.nc): {len(nc_files)}")
        print(f"  GeoTIFF files (.tif): {len(tif_files)}")
        print(f"  Methane-specific files: {len(methane_files)}")
        
        print(f"\nSample files:")
        for i, obj in enumerate(files[:5], 1):
            filename = obj['Key'].split('/')[-1]
            size_mb = obj['Size'] / (1024 * 1024)
            print(f"  {i}. {filename}")
            print(f"     Size: {size_mb:.2f} MB")
            
    else:
        print("✗ No files found at this path")
        
except Exception as e:
    print(f"✗ Error: {str(e)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TROUBLESHOOTING CELL 6: Test Different Date Formats
# ============================================================================
# Sometimes the issue is date format or leading zeros

print("\n" + "="*80)
print("TESTING DIFFERENT DATE/PATH COMBINATIONS")
print("="*80)

test_date = datetime.now() - timedelta(days=10)

# Different path variations to test
path_variations = [
    f"OFFL/L2__CH4___/{test_date.strftime('%Y/%m/%d')}",           # 2024/11/10
    f"OFFL/L2__CH4___/{test_date.strftime('%Y/%#m/%#d')}",         # 2024/11/10 (no leading zeros - Windows)
    f"OFFL/L2__CH4___/{test_date.strftime('%Y/%-m/%-d')}",         # 2024/11/10 (no leading zeros - Unix)
    f"COGT/OFFL/L2__CH4___/{test_date.strftime('%Y/%m/%d')}",     # With COGT prefix
    f"NRTI/L2__CH4___/{test_date.strftime('%Y/%m/%d')}",          # NRTI instead
]

print(f"Testing with date: {test_date.strftime('%Y-%m-%d')} (10 days ago)\n")

for path in path_variations:
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{path}/",
            MaxKeys=1
        )
        
        if 'Contents' in response and len(response['Contents']) > 0:
            print(f"✓ WORKS: {path}/")
            print(f"  Sample file: {response['Contents'][0]['Key']}")
        else:
            print(f"✗ No data: {path}/")
    except Exception as e:
        print(f"✗ Error: {path}/ - {str(e)}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


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

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/S5P_OFFL_L2__CH4____20251129T011651_20251129T025820_42118_03_020901_20251130T213125.nc.csv")
# df now is a Spark DataFrame containing CSV data from "Files/S5P_OFFL_L2__CH4____20251129T011651_20251129T025820_42118_03_020901_20251130T213125.nc.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
