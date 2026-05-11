# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "427f0431-b084-4858-82dd-1bfa55380658",
# META       "default_lakehouse_name": "GreenSky_Lakehouse",
# META       "default_lakehouse_workspace_id": "060ba34b-f1a3-4509-a6e2-36d1e736a8eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "427f0431-b084-4858-82dd-1bfa55380658"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **gold_emission_events**


# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.gold_emission_events (
# MAGIC     event_id STRING,
# MAGIC     event_date DATE,
# MAGIC     facility_key INT,
# MAGIC     equipment_key INT,
# MAGIC     emission_kg_per_hour DECIMAL(18,2),
# MAGIC     emission_severity STRING,
# MAGIC     duration_hours DECIMAL(8,2),
# MAGIC     total_methane_loss_mcf DECIMAL(18,2),
# MAGIC     financial_impact_usd DECIMAL(18,2),
# MAGIC     compliance_risk_score INT,
# MAGIC     scada_anomaly_detected BOOLEAN,
# MAGIC     work_order_generated STRING,
# MAGIC     detection_source STRING,
# MAGIC     response_time_hours DECIMAL(8,2)
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# **dim_facility**

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS GreenSky_Lakehouse.gold.dim_facility (
# MAGIC     facility_key INT,
# MAGIC     facility_id STRING,
# MAGIC     facility_name STRING,
# MAGIC     basin STRING,
# MAGIC     latitude DECIMAL(10,7),
# MAGIC     longitude DECIMAL(11,7),
# MAGIC     facility_age_years INT,
# MAGIC     production_tier STRING
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# **dim_equipment**

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS GreenSky_Lakehouse.gold.dim_equipment (
# MAGIC     equipment_key INT,
# MAGIC     equipment_tag STRING,
# MAGIC     equipment_type STRING,
# MAGIC     manufacturer STRING,
# MAGIC     criticality STRING
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# **dim_date**

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS GreenSky_Lakehouse.gold.dim_date (
# MAGIC     date_key INT,
# MAGIC     full_date DATE,
# MAGIC     year INT,
# MAGIC     quarter INT,
# MAGIC     month INT,
# MAGIC     day_of_week INT
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from pyspark.sql.window import Window 
from datetime import datetime, timedelta 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configuration 

LAKEHOUSE = "greensky" 

SILVER_SCHEMA = "silver" 

GOLD_SCHEMA = "gold" 

print("✓ Configuration loaded")
print(f"  Source: {LAKEHOUSE}.{SILVER_SCHEMA}")
print(f"  Target: {LAKEHOUSE}.{GOLD_SCHEMA}") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL: Generate Date Dimension 
# Coverage: 2024-01-01 to 2026-12-31 
from datetime import date, timedelta 

start_date = date(2024, 1, 1) 
end_date = date(2026, 12, 31) 
date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)] 

date_data = [] 

for d in date_range: 
    date_data.append(( 
        int(d.strftime('%Y%m%d')),  # date_key: 20241201 
        d, 
        d.year, 
        (d.month - 1) // 3 + 1,  # quarter 
        d.month, 
        d.day, 
        d.isocalendar()[1],  # week_of_year 
        d.weekday() + 1,  # day_of_week (1=Monday) 
        d.strftime('%A'),  # day_name 
        d.strftime('%B'),  # month_name 
        1 if d.weekday() < 5 else 0  # is_weekday 
    )) 

date_schema = StructType([ 
    StructField("date_key", IntegerType(), False), 
    StructField("full_date", DateType(), False), 
    StructField("year", IntegerType(), False), 
    StructField("quarter", IntegerType(), False), 
    StructField("month", IntegerType(), False), 
    StructField("day", IntegerType(), False), 
    StructField("week_of_year", IntegerType(), False), 
    StructField("day_of_week", IntegerType(), False), 
    StructField("day_name", StringType(), False), 
    StructField("month_name", StringType(), False), 
    StructField("is_weekday", IntegerType(), False) 
]) 

dim_date_df = spark.createDataFrame(date_data, schema=date_schema) 

# Write dimension table 

# dim_date_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_date") 

print(f"✓ Created dim_date: {dim_date_df.count():,} rows") 

display(dim_date_df.limit(5)) 

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The Kusto cluster uri to write the data. The query Uri is of the form https://<>.kusto.data.microsoft.com 
kustoUri = "https://trd-b5gpfrcqchk3m40g53.z2.kusto.fabric.microsoft.com"
# The database to write the data
database = "GreenSky_Events"
# The table to write the data 
table    = "dim_date"
# The access credentials for the write
accessToken = mssparkutils.credentials.getToken(kustoUri)

# Write data to a Kusto table
dim_date_df.write.\
format("com.microsoft.kusto.spark.synapse.datasource").\
option("kustoCluster",kustoUri).\
option("kustoDatabase",database).\
option("kustoTable", table).\
option("accessToken", accessToken ).\
option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load facility master from bronze (since it's master data) 
facility_master = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.facility_master")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dimension with additional attributes 

dim_facility = facility_master.select( 
    monotonically_increasing_id().alias("facility_key"), 
    col("facility_id"), 
    col("facility_name"), 
    col("facility_type"), 
    col("basin"), 
    col("operator"), 
    col("latitude"), 
    col("longitude"), 
    col("production_start_date"), 
    col("daily_oil_bbl"), 
    col("daily_gas_mcf"), 
    col("well_count"), 
    col("active_status"), 
    col("epa_facility_id"), 
    # Derived attributes 
    datediff(current_date(), col("production_start_date")).cast("int").alias("facility_age_days"), 
    (datediff(current_date(), col("production_start_date")) / 365).cast("int").alias("facility_age_years"), 
    when(col("daily_oil_bbl") > 2000, "HIGH") 
        .when(col("daily_oil_bbl") > 1000, "MEDIUM") 
        .otherwise("LOW").alias("production_tier"), 
    # BOE calculation (6:1 gas-to-oil ratio) 
    (col("daily_oil_bbl") + (col("daily_gas_mcf") / 6.0)).alias("daily_boe") 
) 

# Write dimension 

# dim_facility.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable("gold.dim_facility") 

print(f"✓ Created dim_facility: {dim_facility.count():,} rows") 

display(dim_facility.limit(5)) 

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The Kusto cluster uri to write the data. The query Uri is of the form https://<>.kusto.data.microsoft.com 
kustoUri = "https://trd-b5gpfrcqchk3m40g53.z2.kusto.fabric.microsoft.com"
# The database to write the data
database = "GreenSky_Events"
# The table to write the data 
table    = "dim_facility"
# The access credentials for the write
accessToken = mssparkutils.credentials.getToken(kustoUri)

# Write data to a Kusto table
dim_facility.write.\
format("com.microsoft.kusto.spark.synapse.datasource").\
option("kustoCluster",kustoUri).\
option("kustoDatabase",database).\
option("kustoTable", table).\
option("accessToken", accessToken ).\
option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL : Create Equipment Dimension 
equipment_registry  = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.equipment_registry ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_equipment = equipment_registry.select( 
    monotonically_increasing_id().alias("equipment_key"), 
    col("equipment_id"), 
    col("facility_id"), 
    col("equipment_tag"), 
    col("equipment_type"), 
    col("manufacturer"), 
    col("model"), 
    col("install_date"), 
    col("last_maintenance_date"), 
    col("criticality"), 
    # Derived attributes 
    datediff(current_date(), col("install_date")).cast("int").alias("equipment_age_days"), 
    (datediff(current_date(), col("install_date")) / 365).cast("int").alias("equipment_age_years"), 
    when(col("last_maintenance_date").isNull(), 9999) 
        .otherwise(datediff(current_date(), col("last_maintenance_date"))) 
        .cast("int").alias("days_since_maintenance") 
) 

# Write dimension 

# dim_equipment.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_equipment") 

print(f"✓ Created dim_equipment: {dim_equipment.count():,} rows") 

display(dim_equipment) 

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The Kusto cluster uri to write the data. The query Uri is of the form https://<>.kusto.data.microsoft.com 
kustoUri = "https://trd-b5gpfrcqchk3m40g53.z2.kusto.fabric.microsoft.com"
# The database to write the data
database = "GreenSky_Events"
# The table to write the data 
table    = "dim_equipment"
# The access credentials for the write
accessToken = mssparkutils.credentials.getToken(kustoUri)

# Write data to a Kusto table
dim_equipment.write.\
format("com.microsoft.kusto.spark.synapse.datasource").\
option("kustoCluster",kustoUri).\
option("kustoDatabase",database).\
option("kustoTable", table).\
option("accessToken", accessToken ).\
option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# -------------------------------------------------------------


# CELL ********************

# CELL 6: Load Source Tables 
# Description: Load all required source tables for fact table creation 
# Sources: Carbon Mapper plumes, dimensions, SCADA, production, work orders 
plumes  = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.carbon_mapper_plumes")
facilities = spark.sql("SELECT * FROM GreenSky_Lakehouse.gold.dim_facility ")
equipment  = spark.sql("SELECT * FROM GreenSky_Lakehouse.gold.dim_equipment ")
date_dim = spark.sql("SELECT * FROM GreenSky_Lakehouse.gold.dim_date ")
production  = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.daily_production ")
scada  = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.scada_realtime ")
work_orders = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.maintenance_wo ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(plumes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# CELL : Geospatial Attribution (Plumes to Facilities) 
from pyspark.sql.functions import udf 
from math import radians, cos, sin, asin, sqrt 

def haversine_distance(lon1, lat1, lon2, lat2): 
    """Calculate distance in meters between two lat/lon points""" 
    if None in [lon1, lat1, lon2, lat2]: 
        return None 
    lon1, lat1, lon2, lat2 = map(radians, [float(lon1), float(lat1), float(lon2), float(lat2)]) 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2 
    c = 2 * asin(sqrt(a)) 
    return c * 6371000  # Earth radius in meters 

# Register UDF 

distance_udf = udf(haversine_distance, DoubleType()) 

# Cross join plumes with facilities and filter by distance 

plumes_attributed = plumes.alias("p").crossJoin(facilities.alias("f")) .withColumn( 
        "distance_meters", 
        distance_udf( 
            col("p.plume_longitude"), 
            col("p.plume_latitude"), 
            col("f.longitude"), 
            col("f.latitude") 
        ) 
    ).filter(col("distance_meters") <= 100).select( 
        col("p.plume_id"), 
        col("p.datetime").alias("detection_timestamp"), 
        to_date(col("p.datetime")).alias("detection_date"), 
        col("f.facility_key"), 
        col("f.facility_id"), 
        col("p.emission_auto").alias("emission_kg_per_hour"), 
        col("p.emission_severity"), 
        col("distance_meters") 
    ) 

 

print(f"✓ Attributed {plumes_attributed.count()} plumes to facilities") 

# display(plumes_attributed) 

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL : Calculate SCADA Anomalies 
# Get pressure readings with facility context 

scada_pressure = scada.filter(col("measurement_type") == "pressure").select( 
        col("facility_id"), 
        col("equipment_tag"), 
        col("timestamp"), 
        col("measurement_value").alias("pressure_psi") 
    ) 

# Calculate pressure change for each facility 

window_spec = Window.partitionBy("facility_id", "equipment_tag").orderBy("timestamp") 

scada_anomalies = (
    scada_pressure
        .withColumn("first_pressure", first("pressure_psi").over(window_spec))
        .withColumn("last_pressure", last("pressure_psi").over(window_spec))
        .withColumn(
            "pressure_change_pct",
            ((col("last_pressure") - col("first_pressure")) / col("first_pressure") * 100)
        )
        .groupBy("facility_id", "equipment_tag")
        .agg(
            avg("pressure_psi").alias("avg_pressure_psi"),
            first("pressure_change_pct").alias("pressure_change_pct")
        )
)

print(f"✓ Identified {scada_anomalies.count()} SCADA anomalies") 

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(scada_anomalies)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# CELL : Calculate Production Trends 
production_window = Window.partitionBy("facility_id").orderBy("production_date") 

production_trends =( 
    production
        .withColumn("first_oil", first("oil_volume_bbl").over(production_window)) 
        .withColumn("last_oil", last("oil_volume_bbl").over(production_window)) 
        .withColumn( 
        "production_change_pct", 
        ((col("last_oil") - col("first_oil")) / col("first_oil") * 100) 
        ) 
        .groupBy("facility_id") 
        .agg( 
            avg("oil_volume_bbl").alias("avg_oil_bbl_per_day"), 
            avg("gas_volume_mcf").alias("avg_gas_mcf_per_day"), 
            first("production_change_pct").alias("production_change_pct") 
    )
 )

print("✓ Calculated production trends") 

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(production_trends)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# CELL : Calculate Financial Impact 
# Description: Convert emission rates to financial impact (revenue loss) 
# Conversion: kg/hr → MCF → USD using gas market price 
# Assumptions: 24-hour leak duration, $2.50/MCF gas price 

GAS_PRICE = 2.50  # $/MCF 
METHANE_DENSITY = 0.0007168  # metric tons per cubic meter at STP 
MCF_TO_KG = 19.01  # kg per MCF (thousand cubic feet) 

fact_emission_events =(
     plumes_attributed
        .withColumn( 
            "event_id", 
        concat(lit("EVENT-"), date_format(col("detection_timestamp"), "yyyyMMdd"), lit("-"), col("plume_id")) 
        ) 
        .withColumn( 
            "date_key", 
            date_format(col("detection_date"), "yyyyMMdd").cast("int") 
        ) 
        .withColumn( 
            "duration_hours", 
            lit(24.0)  # Assume 24-hour leak duration for demo 
        ) 
        .withColumn( 
            "total_methane_kg", 
            col("emission_kg_per_hour") * col("duration_hours") 
        ) 
        .withColumn( 
            "total_methane_loss_mcf", 
            col("total_methane_kg") / lit(MCF_TO_KG) 
        ) 
        .withColumn( 
            "financial_impact_usd", 
            col("total_methane_loss_mcf") * lit(GAS_PRICE) 
        ) 
)  
print("✓ Calculated financial impact") 

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact_emission_events)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

fact_with_scada = (
    fact_emission_events
        .join(
            scada_anomalies.select(
                col("facility_id").alias("scada_facility_id"),
                col("equipment_tag").alias("scada_equipment_tag"),
                col("pressure_change_pct")
            ),
            (
                (fact_emission_events.facility_id == col("scada_facility_id")) 
            ),
            "left"
        )
        .withColumn(
            "scada_anomaly_detected",
            when(col("pressure_change_pct").isNotNull(), lit(1)).otherwise(lit(0))
        )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact_with_scada)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Cell 12: Join with Equipment Dimension

fact_with_equipment = (
    fact_with_scada
        .join(
            equipment.select(
                col("equipment_key"),
                col("equipment_tag").alias("equip_tag")
            ),
            (
                (fact_with_scada.scada_equipment_tag == col("equip_tag"))
            ),
            "left"
        )
)

print("✓ Joined with equipment dimension")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact_with_equipment)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Cell 13: Calculate Compliance Risk Score

ldar = spark.table(f"{LAKEHOUSE}.methane_intelligence.ldar_inspections")

ldar_risk = (
    ldar.select(
        col("facility_id"),
        col("days_since_inspection"),
        col("leaks_delayed"),
        when(col("days_since_inspection") > 90, 3)
            .when(col("leaks_delayed") > 0, 2)
            .otherwise(0)
            .alias("ldar_risk_points")
    )
)

fact_with_compliance = (
    fact_with_equipment
        .join(
            ldar_risk.select(
                col("facility_id").alias("ldar_facility_id"),
                col("ldar_risk_points")
            ),
            fact_with_equipment.facility_id == col("ldar_facility_id"),
            "left"
        )
        .withColumn(
            "compliance_risk_score",
            (
                when(col("emission_severity") == "SUPER_EMITTER", 3).otherwise(0)
                + when(col("scada_anomaly_detected") == 1, 2).otherwise(0)
                + coalesce(col("ldar_risk_points"), lit(0))
            )
        )
)

print("✓ Calculated compliance risk scores")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact_with_compliance)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Cell 14: Add Work Order Context

wo_summary = (
    work_orders
        .filter(col("category") == "LEAK_REPAIR")
        .groupBy("facility_id")
        .agg(collect_list("work_order_id").alias("related_work_orders"))
)

fact_final = (
    fact_with_compliance
        .join(
            wo_summary.select(
                col("facility_id").alias("wo_facility_id"),
                col("related_work_orders")
            ),
            fact_with_compliance.facility_id == col("wo_facility_id"),
            "left"
        )
        .withColumn(
            "work_order_generated",
            when(size(col("related_work_orders")) > 0,
                 element_at(col("related_work_orders"), 1)
            ).otherwise(lit(None))
        )
        .withColumn("detection_source", lit("SATELLITE"))
        .withColumn("response_time_hours", lit(None).cast("double"))
)

print("✓ Added work order context")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 15: Write Fact Table - Emission Events

gold_emission_events = (
    fact_final.select(
        col("event_id"),
        col("date_key"),
        col("detection_date").alias("event_date"),
        col("detection_timestamp"),
        col("facility_key"),
        coalesce(col("equipment_key"), lit(-1)).alias("equipment_key"),
        col("emission_kg_per_hour"),
        col("emission_severity"),
        col("duration_hours"),
        round(col("total_methane_loss_mcf"), 2).alias("total_methane_loss_mcf"),
        round(col("financial_impact_usd"), 2).alias("financial_impact_usd"),
        col("compliance_risk_score"),
        col("scada_anomaly_detected"),
        col("work_order_generated"),
        col("detection_source"),
        col("response_time_hours"),
        col("distance_meters")
    ).distinct()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_emission_events.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.gold_emission_events") 

print(f"✓ Created fact_emission_events: {gold_emission_events.count():,} rows")
display(gold_emission_events.orderBy(col("financial_impact_usd").desc()).limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The Kusto cluster uri to write the data. The query Uri is of the form https://<>.kusto.data.microsoft.com 
kustoUri = "https://trd-b5gpfrcqchk3m40g53.z2.kusto.fabric.microsoft.com"
# The database to write the data
database = "GreenSky_Events"
# The table to write the data 
table    = "gold_emission_events"
# The access credentials for the write
accessToken = mssparkutils.credentials.getToken(kustoUri)

# Write data to a Kusto table
gold_emission_events.write.\
format("com.microsoft.kusto.spark.synapse.datasource").\
option("kustoCluster",kustoUri).\
option("kustoDatabase",database).\
option("kustoTable", table).\
option("accessToken", accessToken ).\
option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(gold_emission_events)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql.functions import *
from datetime import datetime, timedelta

# Read the table
df = gold_emission_events

# Get dates
today = datetime.now().date()
yesterday = (datetime.now() - timedelta(days=1)).date()

print(f"Randomly distributing events between {yesterday} and {today}")

# Randomly assign today or yesterday to each row
df_updated = (df
    .withColumn("random_date", 
                when(rand() > 0.5, lit(today))
                .otherwise(lit(yesterday))
                .cast("date"))
    .withColumn("event_date", col("random_date"))
    .withColumn("detection_timestamp", 
                concat(
                    date_format(col("random_date"), "yyyy-MM-dd"),
                    lit(" "),
                    date_format(col("detection_timestamp"), "HH:mm:ss")
                ).cast("timestamp"))
    .withColumn("date_key", date_format(col("random_date"), "yyyyMMdd").cast("int"))
    .withColumn("event_id", 
                concat(
                    lit("EVENT-"),
                    date_format(col("random_date"), "yyyyMMdd"),
                    lit("-"),
                    substring(col("event_id"), 17, 100)
                ))
    .drop("random_date")
)

# Show distribution
print("\n=== Date Distribution ===")
df_updated.groupBy("event_date").count().orderBy("event_date").show()

# Save
df_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.gold_emission_events")

print("✓ Dates randomly distributed between TODAY and YESTERDAY!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Save
df_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.gold_emission_events_extra")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.maintenance_wo ")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM GreenSky_Lakehouse.gold.dim_equipment ")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The Kusto cluster uri to write the data. The query Uri is of the form https://<>.kusto.data.microsoft.com 
kustoUri = "https://trd-b5gpfrcqchk3m40g53.z2.kusto.fabric.microsoft.com"
# The database to write the data
database = "GreenSky_Events"
# The table to write the data 
table    = "dim_equipment"
# The access credentials for the write
accessToken = mssparkutils.credentials.getToken(kustoUri)

# Write data to a Kusto table
df.write.\
format("com.microsoft.kusto.spark.synapse.datasource").\
option("kustoCluster",kustoUri).\
option("kustoDatabase",database).\
option("kustoTable", table).\
option("accessToken", accessToken ).\
option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
