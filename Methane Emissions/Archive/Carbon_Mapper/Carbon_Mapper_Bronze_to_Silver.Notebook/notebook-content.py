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

# # Carbon Mapper: Silver Table Refinement
# 
# This notebook refines the `silver_CarbonMapper` table by:
# - **Filtering by sector**: Keep only sector `1B2` records
# - **Filtering by geography**: Keep only North America coordinates
# - **Removing unnecessary columns**: Clean up columns not needed for analysis
# - **Keeping temporal columns**: `modified` and `published_at` are retained
# - **Overwriting the silver table**: Save the refined data back to `silver_CarbonMapper`

# MARKDOWN ********************

# ## 📍 North America Bounding Box
# 
# We'll use these approximate boundaries:
# - **Longitude**: -168° to -52° (West to East)
# - **Latitude**: 7° to 84° (South to North)
# 
# This covers:
# - United States (including Alaska)
# - Canada
# - Mexico
# - Central America
# - Caribbean islands

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# Initialize Spark
spark = SparkSession.builder.getOrCreate()
print("✅ Spark Session Initialized")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Load Current Silver Table

# CELL ********************

print("Loading current silver table...")
silver_df = spark.table("silver.silver_CarbonMapper")

original_count = silver_df.count()
original_cols = len(silver_df.columns)

print(f"✅ Original Records: {original_count:,}")
print(f"✅ Original Columns: {original_cols}")

# Show current schema
print("\nCurrent columns:")
for col_name in sorted(silver_df.columns):
    print(f"  - {col_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Define North America Boundaries

# CELL ********************

# North America bounding box
NA_MIN_LON = -168.0  # Western boundary (includes Alaska)
NA_MAX_LON = -52.0   # Eastern boundary
NA_MIN_LAT = 7.0     # Southern boundary (includes Central America)
NA_MAX_LAT = 84.0    # Northern boundary (Arctic Canada)

print("North America Bounding Box:")
print(f"  Longitude: {NA_MIN_LON}° to {NA_MAX_LON}°")
print(f"  Latitude: {NA_MIN_LAT}° to {NA_MAX_LAT}°")
print("\nThis includes: USA, Canada, Mexico, Central America, Caribbean")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Apply Filters

# CELL ********************

print("Applying filters...\n")

# Filter 1: Sector = "1B2"
sector_filtered = silver_df.filter(col("sector") == "1B2")
sector_count = sector_filtered.count()
print(f"✅ After sector filter (1B2): {sector_count:,} records")
print(f"   Removed: {original_count - sector_count:,} records")

# Filter 2: North America geography (point location)
na_filtered = sector_filtered.filter(
    (col("longitude") >= NA_MIN_LON) & 
    (col("longitude") <= NA_MAX_LON) &
    (col("latitude") >= NA_MIN_LAT) & 
    (col("latitude") <= NA_MAX_LAT)
)
na_count = na_filtered.count()
print(f"\n✅ After North America filter: {na_count:,} records")
print(f"   Removed: {sector_count - na_count:,} records")

# Filter 3: North America geography (bounding box should also be in NA)
# This ensures the entire plume bounding box is within North America
refined_df = na_filtered.filter(
    (col("plume_bounds_min_lon") >= NA_MIN_LON) &
    (col("plume_bounds_max_lon") <= NA_MAX_LON) &
    (col("plume_bounds_min_lat") >= NA_MIN_LAT) &
    (col("plume_bounds_max_lat") <= NA_MAX_LAT)
)
refined_count = refined_df.count()
print(f"\n✅ After plume bounds filter: {refined_count:,} records")
print(f"   Removed: {na_count - refined_count:,} records (plumes extending outside NA)")

print(f"\n" + "="*60)
print(f"TOTAL RECORDS AFTER FILTERING: {refined_count:,}")
print(f"TOTAL REMOVED: {original_count - refined_count:,} ({((original_count - refined_count)/original_count*100):.1f}%)")
print("="*60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Remove Unnecessary Columns
# 
# We'll remove columns that are typically not needed for analysis.
# 
# **✅ KEEPING: `modified` and `published_at` columns as requested**

# CELL ********************

# Define columns to remove
columns_to_remove = [
    # Technical/internal columns
    "id",                          # Internal database ID
    "collection",                  # Collection metadata
    "processing_software",         # Software version
    
    # Redundant with other fields
    "cmf_type",                    # Redundant with emission_cmf_type
    "emission_cmf_type",           # Type info, often redundant
    "emission_version",            # Version info
    
    # Flags that may not be needed
    "hide_emission",               # Boolean flag
    "is_offshore",                 # Boolean flag (we're filtering to NA anyway)
    
    # Quality/metadata that may not be needed for basic analysis
    "plume_quality",               # Quality indicator (optional)
    "off_nadir",                   # Sensor angle
    "gsd",                         # Ground sample distance
    
    # Status (optional)
    "status",                      # Usually all will be "published" after filtering
    
    # Mission phase (optional)
    "mission_phase"                # Typically "production"
    
    # NOTE: KEEPING modified and published_at as per user request
]

print("Columns to remove:")
for col_name in columns_to_remove:
    if col_name in refined_df.columns:
        print(f"  ❌ {col_name}")

# Get columns to keep
columns_to_keep = [col for col in refined_df.columns if col not in columns_to_remove]

# Select only the columns we want to keep
final_df = refined_df.select(columns_to_keep)

print(f"\n✅ Columns after removal: {len(final_df.columns)}")
print(f"   Removed: {len(columns_to_remove)} columns")
print(f"\n✅ KEPT: modified and published_at columns (as requested)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Review Final Schema
# 
# Let's see what columns we're keeping:

# CELL ********************

print("\n" + "="*60)
print("FINAL COLUMNS (Organized by Category)")
print("="*60)

# Categorize columns for better readability
id_cols = [c for c in columns_to_keep if c in ["plume_id", "scene_id"]]
platform_cols = [c for c in columns_to_keep if c in ["platform", "instrument"]]
gas_cols = [c for c in columns_to_keep if c in ["gas", "sector"]]
time_cols = [c for c in columns_to_keep if c in ["scene_timestamp", "modified", "published_at"]]
geo_cols = [c for c in columns_to_keep if "lon" in c.lower() or "lat" in c.lower() or "geometry" in c.lower()]
emission_cols = [c for c in columns_to_keep if "emission" in c.lower() or "wind" in c.lower()]
other_cols = [c for c in columns_to_keep if c not in id_cols + platform_cols + gas_cols + time_cols + geo_cols + emission_cols]

print("\n🆔 Identity Columns:")
for col in id_cols:
    print(f"   • {col}")

print("\n🛰️ Platform Columns:")
for col in platform_cols:
    print(f"   • {col}")

print("\n💨 Gas/Sector Columns:")
for col in gas_cols:
    print(f"   • {col}")

print("\n⏰ Temporal Columns:")
for col in time_cols:
    print(f"   • {col}")

print("\n📍 Geography Columns:")
for col in geo_cols:
    print(f"   • {col}")

print("\n📊 Emission/Environmental Columns:")
for col in emission_cols:
    print(f"   • {col}")

if other_cols:
    print("\n📋 Other Columns:")
    for col in other_cols:
        print(f"   • {col}")

print(f"\n{'='*60}")
print(f"Total columns in refined table: {len(final_df.columns)}")
print(f"{'='*60}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6: Preview the Refined Data

# CELL ********************

print("\nPreview of refined data:")
display(final_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Show statistics for key columns
print("\nStatistics for emission and location data:")
final_df.select(
    "emission_auto", 
    "emission_uncertainty_auto",
    "longitude", 
    "latitude",
    "wind_speed_avg_auto"
).describe().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check distribution by platform and sector
print("\nDistribution by Platform:")
final_df.groupBy("platform").count().orderBy("count", ascending=False).show()

print("\nDistribution by Gas:")
final_df.groupBy("gas").count().orderBy("count", ascending=False).show()

print("\nDistribution by Sector (should all be 1B2):")
final_df.groupBy("sector").count().orderBy("count", ascending=False).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7: Check Temporal Columns
# 
# Let's verify that `modified` and `published_at` columns are present:

# CELL ********************

print("\nVerifying temporal columns are present:")
temporal_cols = ["scene_timestamp", "modified", "published_at"]
for col in temporal_cols:
    if col in final_df.columns:
        print(f"  ✅ {col} - PRESENT")
    else:
        print(f"  ❌ {col} - MISSING")

# Show sample temporal data
print("\nSample temporal data:")
final_df.select("plume_id", "scene_timestamp", "modified", "published_at").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 8: Overwrite the Silver Table

# CELL ********************

print("\n" + "="*60)
print("WRITING REFINED DATA TO SILVER TABLE")
print("="*60)

print("\n⚠️  This will OVERWRITE the existing silver_CarbonMapper table!")
print("\nChanges:")
print(f"  • Records: {original_count:,} → {refined_count:,} (removed {original_count - refined_count:,})")
print(f"  • Columns: {original_cols} → {len(final_df.columns)} (removed {original_cols - len(final_df.columns)})")
print(f"  • Filters: Sector=1B2, North America geography only")
print(f"  • Kept temporal columns: modified, published_at")

# Write to silver table (overwrite mode)
(final_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.silver_CarbonMapper"))

print("\n✅ Silver table overwritten successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 9: Verify the Updated Table

# CELL ********************

print("\nVerifying the updated silver table...")
verified_df = spark.table("silver.silver_CarbonMapper")

verify_count = verified_df.count()
verify_cols = len(verified_df.columns)

print(f"\n✅ Verified Record Count: {verify_count:,}")
print(f"✅ Verified Column Count: {verify_cols}")

# Check that all records are sector 1B2
sector_check = verified_df.select("sector").distinct().collect()
print(f"\n✅ Sectors in table: {[row.sector for row in sector_check]}")

# Verify temporal columns are present
print("\n✅ Temporal columns check:")
for col in ["scene_timestamp", "modified", "published_at"]:
    if col in verified_df.columns:
        print(f"   ✓ {col} is present")
    else:
        print(f"   ✗ {col} is missing")

# Check geographic bounds
print("\n✅ Geographic bounds check:")
stats = verified_df.select(
    "longitude", "latitude",
    "plume_bounds_min_lon", "plume_bounds_max_lon",
    "plume_bounds_min_lat", "plume_bounds_max_lat"
).summary("min", "max").collect()

print("\nCoordinate ranges:")
for stat in stats:
    print(f"  {stat[0]}:")
    print(f"    Longitude: {float(stat[1]):.2f}° to {float(stat[1]):.2f}°")
    print(f"    Latitude: {float(stat[2]):.2f}° to {float(stat[2]):.2f}°")
    break

display(verified_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🎉 Transformation Complete!
# 
# ### Summary of Changes:
# 
# ✅ **Filtered by Sector**: Only sector `1B2` records retained  
# ✅ **Filtered by Geography**: Only North America coordinates  
# ✅ **Removed Unnecessary Columns**: Cleaned up metadata and technical fields  
# ✅ **KEPT Temporal Columns**: `modified` and `published_at` are retained  
# ✅ **Overwritten Silver Table**: Updated `silver_CarbonMapper` with refined data  
# 
# ### Your refined table now contains:
# - Only 1B2 sector emissions (Oil and Natural Gas)
# - Only plumes in North America
# - Essential columns for analysis INCLUDING temporal metadata
# - Clean, optimized data ready for downstream use!
# 
# ### Temporal Columns Available:
# - `scene_timestamp` - When the observation was made
# - `modified` - Last modification timestamp
# - `published_at` - Publication timestamp
# 
# ### Next Steps:
# 1. Run analytics on the refined data
# 2. Create time-series visualizations using the temporal columns
# 3. Build aggregated Gold layer tables
# 4. Export for use in other tools

