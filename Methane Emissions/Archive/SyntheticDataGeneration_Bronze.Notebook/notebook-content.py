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
# META           "id": "c6a226c1-ff14-4c23-ad10-4ffe7c4d23db"
# META         },
# META         {
# META           "id": "e0efee7f-4d05-4685-b178-768ed7635e44"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================================
# CELL 1: Configuration and Imports
# ============================================================================
# Purpose: Set up environment and import required libraries
# Compatible with: Microsoft Fabric Notebooks
# Run this cell first to initialize the notebook

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
    print(f"✅ Bronze schema '{BRONZE_SCHEMA}' is ready")
    
    # Verify schema exists
    schemas = spark.sql("SHOW SCHEMAS").collect()
    schema_names = [row.namespace for row in schemas]
    
    if BRONZE_SCHEMA in schema_names:
        print(f"✅ Confirmed: '{BRONZE_SCHEMA}' schema exists")
    else:
        print(f"⚠️  Warning: '{BRONZE_SCHEMA}' schema not found in list")
    
except Exception as e:
    print(f"⚠️  Error creating schema: {e}")
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
        
        print(f"✅ Saved {len(df)} records to {full_table_name}")
        return True
    except Exception as e:
        print(f"❌ Error saving {table_name}: {str(e)}")
        return False



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 4: Generate BRONZE - Facilities Master Data
# ============================================================================
# Purpose: Create facilities master data representing oil & gas production sites
# Source System: ERP (e.g., SAP)
# Update Frequency: Daily batch
# Table: bronze_facilities

print("\n" + "=" * 80)
print("GENERATING BRONZE LAYER - TABLE 1: FACILITIES")
print("=" * 80)

facilities_data = []
facility_ids = [f"FAC-{i:04d}" for i in range(1, 101)]  # 100 facilities

operators = [
    "XYZ Energy", "Apex Oil & Gas", "Summit Petroleum", 
    "Highland Resources", "Vista Energy Corp", "Frontier Oil", 
    "Pinnacle Resources", "Crestwood Energy",
    "Silver Peak Oil", "Mountain View Resources"
]

for idx, fac_id in enumerate(facility_ids):
    # Distribute facilities across USA O&G regions
    region_name = list(USA_OG_REGIONS.keys())[idx % len(USA_OG_REGIONS)]
    region = USA_OG_REGIONS[region_name]
    
    lat, lon = generate_random_coords(region_name)
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
        "latitude": lat,
        "longitude": lon,
        "state": region['state'],
        "county": f"{region['state']}-County-{np.random.randint(1, 20):02d}",
        "basin": region_name,
        "sector_code": "1B2",  # IPCC sector for Oil & Gas
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
        "num_wells": np.random.randint(2, 20)
    }
    facilities_data.append(facility)

df_facilities = pd.DataFrame(facilities_data)
df_facilities = add_cdc_columns(df_facilities, source_system='ERP_SAP')

print(f"\n✅ Generated {len(df_facilities)} facilities")
print(f"   Regions: {df_facilities['basin'].nunique()}")
print(f"   States: {sorted(df_facilities['state'].unique())}")
print(f"   Operators: {df_facilities['operator'].nunique()}")

# Save to lakehouse
save_to_lakehouse(df_facilities, "bronze_facilities")

# Show sample
print("\nSample records:")
print(df_facilities[['facility_id', 'facility_name', 'basin', 'state', 'latitude', 'longitude', 'epa_threshold_kg_hr']].head(3).to_string(index=False))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 5: Generate BRONZE - Equipment Inventory
# ============================================================================
# Purpose: Equipment assets at each facility (tanks, compressors, etc.)
# Source System: Asset Management System
# Update Frequency: Daily batch
# Table: bronze_equipment

print("\n" + "=" * 80)
print("GENERATING BRONZE LAYER - TABLE 2: EQUIPMENT")
print("=" * 80)

equipment_data = []
equipment_types = {
    "Tank": {
        "capacity_range": (100, 1000), 
        "unit": "bbl", 
        "leak_probability": 0.15,
        "description": "Storage tank for oil/condensate"
    },
    "Compressor": {
        "capacity_range": (500, 5000), 
        "unit": "hp", 
        "leak_probability": 0.10,
        "description": "Gas compression equipment"
    },
    "Separator": {
        "capacity_range": (50, 500), 
        "unit": "bbl/day", 
        "leak_probability": 0.08,
        "description": "Oil-water-gas separator"
    },
    "Wellhead": {
        "capacity_range": (100, 2000), 
        "unit": "bbl/day", 
        "leak_probability": 0.12,
        "description": "Well control equipment"
    },
    "Dehydrator": {
        "capacity_range": (10, 100), 
        "unit": "MMscf/day", 
        "leak_probability": 0.11,
        "description": "Natural gas dehydration"
    },
    "Pneumatic Device": {
        "capacity_range": (1, 50), 
        "unit": "scf/hr", 
        "leak_probability": 0.20,
        "description": "Pneumatic controllers/pumps"
    },
    "PRV": {
        "capacity_range": (500, 3000), 
        "unit": "psi", 
        "leak_probability": 0.14,
        "description": "Pressure Relief Valve"
    }
}

equipment_id_counter = 1
for fac_id in facility_ids:
    num_equipment = np.random.randint(4, 12)
    
    for _ in range(num_equipment):
        equip_type = np.random.choice(list(equipment_types.keys()))
        specs = equipment_types[equip_type]
        install_year = np.random.randint(2015, 2025)
        
        equipment = {
            "equipment_id": f"EQ-{equipment_id_counter:06d}",
            "facility_id": fac_id,
            "equipment_type": equip_type,
            "equipment_tag": f"{equip_type[:3].upper()}-{np.random.randint(100, 999)}",
            "manufacturer": np.random.choice([
                "Cameron", "Weatherford", "Baker Hughes", 
                "Schlumberger", "Halliburton", "National Oilwell Varco"
            ]),
            "model": f"Model-{np.random.choice(['X', 'Z', 'Pro'])}{np.random.randint(100, 999)}",
            "serial_number": f"SN-{np.random.randint(100000, 999999)}",
            "capacity": np.random.randint(*specs["capacity_range"]),
            "capacity_unit": specs["unit"],
            "installation_date": f"{install_year}-{np.random.randint(1, 12):02d}-{np.random.randint(1, 28):02d}",
            "last_inspection_date": (datetime.now() - timedelta(days=np.random.randint(1, 365))).strftime("%Y-%m-%d"),
            "next_inspection_date": (datetime.now() + timedelta(days=np.random.randint(1, 180))).strftime("%Y-%m-%d"),
            "status": np.random.choice(
                ["Operational", "Operational", "Operational", "Maintenance", "Standby"], 
                p=[0.80, 0.10, 0.05, 0.03, 0.02]
            ),
            "criticality": np.random.choice(["High", "Medium", "Low"], p=[0.3, 0.5, 0.2]),
            "leak_history_flag": np.random.random() < specs["leak_probability"]
        }
        equipment_data.append(equipment)
        equipment_id_counter += 1

df_equipment = pd.DataFrame(equipment_data)
df_equipment = add_cdc_columns(df_equipment, source_system='ASSET_MGMT_SYSTEM')

print(f"\n✅ Generated {len(df_equipment)} equipment items")
print(f"   Equipment types distribution:")
for equip_type, count in df_equipment['equipment_type'].value_counts().items():
    print(f"      • {equip_type}: {count}")
print(f"   High-risk equipment (leak history): {df_equipment['leak_history_flag'].sum()}")

# Save to lakehouse
save_to_lakehouse(df_equipment, "bronze_equipment")

# Show sample
print("\nSample records:")
print(df_equipment[['equipment_id', 'facility_id', 'equipment_type', 'equipment_tag', 'status', 'leak_history_flag']].head(3).to_string(index=False))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 6: Generate BRONZE - SCADA Operations Data (Streaming)
# ============================================================================
# Purpose: Real-time sensor data from equipment (pressure, temperature, etc.)
# Source System: SCADA (Wonderware, OSIsoft PI, etc.)
# Update Frequency: Real-time (1-minute intervals in production)
# Table: bronze_scada_operations
# NOTE: Generating 90 days of hourly data dynamically up to today

print("\n" + "=" * 80)
print("GENERATING BRONZE LAYER - TABLE 3: SCADA OPERATIONS DATA")
print("=" * 80)
print("⚠️  Note: Generating 90 days of HOURLY data dynamically to TODAY")
print("    In production, this would be 1-minute streaming data")
print("=" * 80)

operations_data = []
start_date = datetime.now() - timedelta(days=90)
end_date = datetime.now()

# Focus on high-priority equipment (most likely to leak)
priority_equipment = df_equipment[
    df_equipment['equipment_type'].isin(['Tank', 'PRV', 'Compressor', 'Pneumatic Device'])
].head(150)  # Limit to 150 equipment for manageable data size

print(f"\nGenerating data for {len(priority_equipment)} priority equipment items...")
print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
print(f"Frequency: Hourly (2,160 measurements per equipment)")
print(f"Total measurements: {len(priority_equipment) * 90 * 24:,}")

for idx, equipment in priority_equipment.iterrows():
    if idx % 30 == 0:
        print(f"  Processing equipment {idx + 1}/{len(priority_equipment)}...")
    
    # Equipment-specific normal operating parameters
    if equipment['equipment_type'] == 'Tank':
        normal_pressure = np.random.uniform(800, 900)
        normal_temp = np.random.uniform(70, 80)
        normal_level = np.random.uniform(60, 80)
    elif equipment['equipment_type'] == 'Compressor':
        normal_pressure = np.random.uniform(1200, 1500)
        normal_temp = np.random.uniform(150, 180)
        normal_level = None
    elif equipment['equipment_type'] == 'PRV':
        normal_pressure = np.random.uniform(500, 800)
        normal_temp = np.random.uniform(70, 90)
        normal_level = None
    else:
        normal_pressure = np.random.uniform(300, 600)
        normal_temp = np.random.uniform(60, 80)
        normal_level = None
    
    # Determine if equipment has degradation trend (simulates leak development)
    has_leak_trend = equipment['leak_history_flag'] and np.random.random() < 0.3
    
    current_datetime = start_date
    hour_count = 0
    
    while current_datetime <= end_date:
        hour_count += 1
        
        # Simulate occasional pressure spikes/anomalies
        is_anomaly = np.random.random() < 0.015  # 1.5% anomaly rate
        
        # Simulate leak development over time
        if has_leak_trend and hour_count > 1440:  # After 60 days
            leak_intensity = min((hour_count - 1440) / 720, 1.0)  # Gradual increase
            pressure = normal_pressure + (np.random.uniform(50, 150) * leak_intensity)
            temp_variance = np.random.uniform(5, 15) * leak_intensity
            is_anomaly = True if leak_intensity > 0.5 else is_anomaly
        elif is_anomaly:
            pressure = normal_pressure + np.random.uniform(-50, 100)
            temp_variance = np.random.uniform(-5, 15)
        else:
            pressure = normal_pressure + np.random.normal(0, 15)
            temp_variance = np.random.uniform(-2, 3)
        
        operation = {
            "measurement_id": f"SCADA-{len(operations_data) + 1:010d}",
            "equipment_id": equipment['equipment_id'],
            "facility_id": equipment['facility_id'],
            "measurement_timestamp": current_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "pressure_psi": round(max(0, pressure), 2),
            "temperature_f": round(normal_temp + temp_variance, 2),
            "level_percent": round(normal_level + np.random.normal(0, 5), 2) if normal_level is not None else None,
            "flow_rate_bbl_hr": round(np.random.uniform(10, 50), 2) if equipment['equipment_type'] != 'PRV' else None,
            "vibration_mm_s": round(np.random.uniform(0.5, 5.0), 2) if equipment['equipment_type'] == 'Compressor' else None,
            "is_anomaly": is_anomaly,
            "anomaly_score": round(np.random.uniform(0.7, 0.99), 3) if is_anomaly else round(np.random.uniform(0.0, 0.3), 3)
        }
        operations_data.append(operation)
        
        current_datetime += timedelta(hours=1)

df_operations = pd.DataFrame(operations_data)
df_operations = add_cdc_columns(df_operations, record_type="STREAMING_INSERT", source_system='SCADA_WONDERWARE')
df_operations['_ingestion_timestamp'] = datetime.now()

print(f"\n✅ Generated {len(df_operations):,} SCADA measurements")
print(f"   Anomalies detected: {df_operations['is_anomaly'].sum():,} ({df_operations['is_anomaly'].mean()*100:.2f}%)")
print(f"   Equipment with leak trends simulated: {priority_equipment['leak_history_flag'].sum()}")
print(f"   Date range: {df_operations['measurement_timestamp'].min()} to {df_operations['measurement_timestamp'].max()}")

# Save to lakehouse
save_to_lakehouse(df_operations, "bronze_scada_operations")

# Show sample
print("\nSample records (showing anomalies):")
sample = df_operations[df_operations['is_anomaly'] == True].head(3)
if len(sample) > 0:
    print(sample[['measurement_id', 'equipment_id', 'measurement_timestamp', 'pressure_psi', 'temperature_f', 'anomaly_score']].to_string(index=False))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 7: Generate BRONZE - Maintenance History
# ============================================================================
# Purpose: Historical maintenance records for equipment
# Source System: CMMS (Computerized Maintenance Management System - e.g., Maximo)
# Update Frequency: Event-driven
# Table: bronze_maintenance_history

print("\n" + "=" * 80)
print("GENERATING BRONZE LAYER - TABLE 4: MAINTENANCE HISTORY")
print("=" * 80)

maintenance_data = []
maintenance_types = ["Preventive", "Corrective", "Inspection", "Emergency", "PRV Replacement", "Leak Repair"]

for idx, equipment in df_equipment.iterrows():
    num_records = np.random.randint(2, 10)
    
    for _ in range(num_records):
        maint_type = np.random.choice(maintenance_types, p=[0.35, 0.25, 0.20, 0.08, 0.07, 0.05])
        days_ago = np.random.randint(1, 1095)  # Up to 3 years
        maint_date = datetime.now() - timedelta(days=days_ago)
        
        if maint_type == "PRV Replacement":
            description = "Pressure Relief Valve replaced - exceeded service life"
            cost = np.random.uniform(2000, 8000)
            duration = np.random.uniform(2, 6)
        elif maint_type == "Emergency":
            description = "Emergency repair - abnormal pressure readings"
            cost = np.random.uniform(5000, 25000)
            duration = np.random.uniform(4, 12)
        elif maint_type == "Leak Repair":
            description = "Leak detected and sealed - gasket/valve replacement"
            cost = np.random.uniform(3000, 15000)
            duration = np.random.uniform(3, 8)
        else:
            description = f"Routine {maint_type.lower()} maintenance"
            cost = np.random.uniform(500, 5000)
            duration = np.random.uniform(1, 4)
        
        maintenance = {
            "maintenance_id": f"MAINT-{len(maintenance_data) + 1:07d}",
            "work_order_number": f"WO-{np.random.randint(100000, 999999)}",
            "equipment_id": equipment['equipment_id'],
            "facility_id": equipment['facility_id'],
            "maintenance_type": maint_type,
            "maintenance_date": maint_date.strftime("%Y-%m-%d"),
            "completion_date": (maint_date + timedelta(hours=int(duration))).strftime("%Y-%m-%d"),
            "description": description,
            "technician_id": f"TECH-{np.random.randint(1, 50):03d}",
            "technician_name": f"Technician {np.random.randint(1, 50)}",
            "duration_hours": round(duration, 1),
            "cost_usd": round(cost, 2),
            "parts_replaced": np.random.choice([True, False], p=[0.4, 0.6]),
            "parts_cost_usd": round(np.random.uniform(100, cost * 0.3), 2) if np.random.random() < 0.4 else 0,
            "downtime_hours": round(np.random.uniform(0, duration * 1.5), 1),
            "maint_result": np.random.choice(["Completed", "Completed", "Partially Completed"], p=[0.90, 0.08, 0.02])
        }
        maintenance_data.append(maintenance)

df_maintenance = pd.DataFrame(maintenance_data)
df_maintenance = add_cdc_columns(df_maintenance, source_system='CMMS_MAXIMO')

print(f"\n✅ Generated {len(df_maintenance)} maintenance records")
print(f"   Maintenance types:")
for mtype, count in df_maintenance['maintenance_type'].value_counts().items():
    print(f"      • {mtype}: {count}")
print(f"   Total maintenance cost: ${df_maintenance['cost_usd'].sum():,.2f}")

# Save to lakehouse
save_to_lakehouse(df_maintenance, "bronze_maintenance_history")

print("\nSample records:")
print(df_maintenance[['maintenance_id', 'equipment_id', 'maintenance_type', 'maintenance_date', 'cost_usd']].head(3).to_string(index=False))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 8: Generate BRONZE - Regulatory Compliance
# ============================================================================
# Purpose: EPA quarterly reports and compliance status
# Source System: EPA Reporting System
# Update Frequency: Quarterly + Event-driven
# Table: bronze_regulatory_compliance

print("\n" + "=" * 80)
print("GENERATING BRONZE LAYER - TABLE 5: REGULATORY COMPLIANCE")
print("=" * 80)

compliance_data = []
for fac_id in facility_ids:
    # Quarterly EPA reports for last 2 years
    for quarter in range(1, 5):
        for year in [2023, 2024, 2025]:
            if year == 2025 and quarter > 3:  # Don't create future quarters
                continue
                
            reporting_date = datetime(year, quarter*3, 15)
            if reporting_date > datetime.now():
                continue
            
            total_emissions = round(np.random.uniform(5000, 80000), 2)
            facility = df_facilities[df_facilities['facility_id'] == fac_id].iloc[0]
            threshold = facility['epa_threshold_kg_hr'] * 24 * 90  # Quarterly threshold
            
            compliance = {
                "compliance_id": f"COMP-{len(compliance_data) + 1:07d}",
                "facility_id": fac_id,
                "report_type": "EPA Quarterly GHG Report",
                "reporting_period": f"Q{quarter}-{year}",
                "submission_date": reporting_date.strftime("%Y-%m-%d"),
                "total_emissions_kg": total_emissions,
                "threshold_kg": threshold,
                "threshold_exceeded": total_emissions > threshold,
                "compliance_status": "Compliant" if total_emissions <= threshold else np.random.choice(["Under Review", "Action Required"], p=[0.7, 0.3]),
                "fine_amount_usd": 0 if total_emissions <= threshold else round(np.random.uniform(5000, 75000), 2) if np.random.random() < 0.3 else 0,
                "notes": f"{'Threshold exceeded by ' + str(round((total_emissions - threshold)/threshold * 100, 1)) + '%' if total_emissions > threshold else 'Within limits'}"
            }
            compliance_data.append(compliance)

df_compliance = pd.DataFrame(compliance_data)
df_compliance = add_cdc_columns(df_compliance, source_system='EPA_REPORTING_SYSTEM')

print(f"\n✅ Generated {len(df_compliance)} quarterly compliance reports")
print(f"   Reporting periods: {df_compliance['reporting_period'].nunique()}")
print(f"   Threshold violations: {df_compliance['threshold_exceeded'].sum()} ({df_compliance['threshold_exceeded'].mean()*100:.1f}%)")
print(f"   Total fines: ${df_compliance['fine_amount_usd'].sum():,.2f}")

# Save to lakehouse
save_to_lakehouse(df_compliance, "bronze_regulatory_compliance")

print("\nSample records:")
print(df_compliance[['compliance_id', 'facility_id', 'reporting_period', 'total_emissions_kg', 'compliance_status']].head(3).to_string(index=False))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 9: Generate BRONZE - LDAR History
# ============================================================================
# Purpose: Leak Detection and Repair program history
# Source System: LDAR Program Database
# Update Frequency: Event-driven
# Table: bronze_ldar_history

print("\n" + "=" * 80)
print("GENERATING BRONZE LAYER - TABLE 6: LDAR HISTORY")
print("=" * 80)

ldar_data = []
for idx, equipment in df_equipment[df_equipment['leak_history_flag'] == True].iterrows():
    num_leaks = np.random.randint(1, 5)
    
    for _ in range(num_leaks):
        days_ago = np.random.randint(30, 1095)
        detection_date = datetime.now() - timedelta(days=days_ago)
        repair_days = np.random.randint(1, 45)
        
        leak_rate = round(np.random.lognormal(3.5, 1.2), 2)  # Log-normal distribution
        
        ldar = {
            "ldar_id": f"LDAR-{len(ldar_data) + 1:07d}",
            "equipment_id": equipment['equipment_id'],
            "facility_id": equipment['facility_id'],
            "detection_date": detection_date.strftime("%Y-%m-%d %H:%M:%S"),
            "detection_method": np.random.choice(
                ["Handheld IR Camera", "Optical Gas Imaging", "Satellite (Carbon Mapper)", "Acoustic Sensor", "FLIR"], 
                p=[0.35, 0.30, 0.15, 0.12, 0.08]
            ),
            "leak_rate_kg_hr": leak_rate,
            "leak_concentration_ppm": round(np.random.uniform(500, 50000), 0),
            "repair_date": (detection_date + timedelta(days=repair_days)).strftime("%Y-%m-%d"),
            "repair_duration_days": repair_days,
            "leak_component": np.random.choice(["Valve", "Flange", "Connector", "Seal", "PRV", "Tank Hatch", "Pump Seal"]),
            "leak_cause": np.random.choice([
                "Valve failure", "Gasket degradation", "Corrosion", 
                "PRV malfunction", "Connection loose", "Equipment wear", "Thermal cycling"
            ]),
            "repair_action": np.random.choice(["Component replacement", "Gasket replacement", "Tightening", "Welding", "Valve rebuild"]),
            "repair_cost_usd": round(np.random.uniform(1000, 20000), 2),
            "estimated_loss_kg": round(leak_rate * repair_days * 24, 2),
            "verified_repair": np.random.choice([True, False], p=[0.95, 0.05])
        }
        ldar_data.append(ldar)

df_ldar = pd.DataFrame(ldar_data)
df_ldar = add_cdc_columns(df_ldar, source_system='LDAR_PROGRAM_SYSTEM')

print(f"\n✅ Generated {len(df_ldar)} leak events")
print(f"   Average leak rate: {df_ldar['leak_rate_kg_hr'].mean():.2f} kg/hr")
print(f"   Total estimated emissions loss: {df_ldar['estimated_loss_kg'].sum():,.2f} kg")
print(f"   Detection methods:")
for method, count in df_ldar['detection_method'].value_counts().items():
    print(f"      • {method}: {count}")

# Save to lakehouse
save_to_lakehouse(df_ldar, "bronze_ldar_history")

print("\nSample records:")
print(df_ldar[['ldar_id', 'equipment_id', 'detection_date', 'leak_rate_kg_hr', 'repair_duration_days']].head(3).to_string(index=False))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 10: Generate BRONZE - Work Orders
# ============================================================================
# Purpose: Work orders for repairs and maintenance
# Source System: Work Management System
# Update Frequency: Real-time
# Table: bronze_work_orders

print("\n" + "=" * 80)
print("GENERATING BRONZE LAYER - TABLE 7: WORK ORDERS")
print("=" * 80)

work_orders = []
for idx in range(250):
    creation_date = datetime.now() - timedelta(days=np.random.randint(1, 365))
    priority = np.random.choice(["Critical", "High", "Medium", "Low"], p=[0.12, 0.28, 0.42, 0.18])
    
    if priority == "Critical":
        daily_loss = round(np.random.uniform(10000, 18000), 2)
        deadline_days = np.random.randint(1, 5)
        fine_potential = round(np.random.uniform(5000, 15000), 2)
    elif priority == "High":
        daily_loss = round(np.random.uniform(4000, 10000), 2)
        deadline_days = np.random.randint(5, 15)
        fine_potential = round(np.random.uniform(2000, 8000), 2)
    elif priority == "Medium":
        daily_loss = round(np.random.uniform(1000, 4000), 2)
        deadline_days = np.random.randint(15, 30)
        fine_potential = round(np.random.uniform(0, 3000), 2)
    else:
        daily_loss = round(np.random.uniform(200, 1000), 2)
        deadline_days = np.random.randint(30, 60)
        fine_potential = 0
    
    days_since_creation = (datetime.now() - creation_date).days
    if days_since_creation < deadline_days * 0.3:
        status = "Open"
    elif days_since_creation < deadline_days * 0.7:
        status = "In Progress"
    elif days_since_creation < deadline_days:
        status = np.random.choice(["In Progress", "Completed"], p=[0.4, 0.6])
    else:
        status = np.random.choice(["Completed", "Closed"], p=[0.7, 0.3])
    
    work_order = {
        "work_order_id": f"WO-{idx + 1:07d}",
        "facility_id": np.random.choice(facility_ids),
        "equipment_id": np.random.choice(df_equipment['equipment_id'].tolist()) if np.random.random() > 0.15 else None,
        "priority": priority,
        "wo_type": np.random.choice(["Leak Repair", "Preventive Maintenance", "Inspection", "Emergency Response"]),
        "created_date": creation_date.strftime("%Y-%m-%d %H:%M:%S"),
        "deadline_date": (creation_date + timedelta(days=deadline_days)).strftime("%Y-%m-%d"),
        "status": status,
        "assigned_team": np.random.choice(["Field Team Alpha", "Field Team Beta", "Field Team Gamma", "Field Team Delta", "Emergency Response Unit"]),
        "assigned_tech_id": f"TECH-{np.random.randint(1, 50):03d}",
        "estimated_daily_loss_usd": daily_loss,
        "actual_daily_loss_usd": round(daily_loss * np.random.uniform(0.8, 1.2), 2) if status in ["Completed", "Closed"] else None,
        "regulatory_fine_potential_usd": fine_potential,
        "travel_distance_miles": round(np.random.uniform(5, 250), 1),
        "estimated_duration_hours": round(np.random.uniform(2, 24), 1),
        "actual_duration_hours": round(np.random.uniform(2, 30), 1) if status in ["Completed", "Closed"] else None,
        "completion_date": (creation_date + timedelta(days=np.random.randint(1, deadline_days + 10))).strftime("%Y-%m-%d") if status in ["Completed", "Closed"] else None
    }
    work_orders.append(work_order)

df_work_orders = pd.DataFrame(work_orders)
df_work_orders = add_cdc_columns(df_work_orders, source_system='WORK_MGMT_SYSTEM')

print(f"\n✅ Generated {len(df_work_orders)} work orders")
print(f"   Status distribution:")
for status, count in df_work_orders['status'].value_counts().items():
    print(f"      • {status}: {count}")
print(f"   Priority distribution:")
for priority, count in df_work_orders['priority'].value_counts().items():
    print(f"      • {priority}: {count}")
print(f"   Total potential production loss: ${df_work_orders['estimated_daily_loss_usd'].sum():,.2f}/day")

# Save to lakehouse
save_to_lakehouse(df_work_orders, "bronze_work_orders")

print("\nSample records:")
print(df_work_orders[['work_order_id', 'facility_id', 'priority', 'status', 'estimated_daily_loss_usd']].head(3).to_string(index=False))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
