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

# **Satellite Detection to Facility Attribution**

# CELL ********************

# Geospatial join: Match satellite plumes to facilities within 100m
plumes = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.carbon_mapper_plumes")
facilities = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.facility_master")

# UDF for distance calculation (Haversine formula)
from pyspark.sql.types import DoubleType
from math import radians, cos, sin, asin, sqrt

def haversine_distance(lon1, lat1, lon2, lat2):
    """Calculate distance between two lat/lon points in meters"""
    # Handle None values
    if None in (lon1, lat1, lon2, lat2):
        return None
    
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371000  # Earth radius in meters
    return c * r

# Register UDF with proper return type
from pyspark.sql.functions import udf
distance_udf = udf(haversine_distance, DoubleType())

# Register for SQL usage
spark.udf.register("distance_meters", haversine_distance, DoubleType())

# Create temp views for SQL query
plumes.createOrReplaceTempView("plumes")
facilities.createOrReplaceTempView("facilities")

attributed_plumes = spark.sql("""
    WITH distances AS (
        SELECT 
            p.plume_id,
            p.datetime,
            p.emission_auto,
            p.emission_severity,
            f.facility_id,
            f.facility_name,
            f.daily_gas_mcf,
            distance_meters(
                CAST(p.plume_longitude AS DOUBLE), 
                CAST(p.plume_latitude AS DOUBLE),
                CAST(f.longitude AS DOUBLE), 
                CAST(f.latitude AS DOUBLE)
            ) as distance_meters
        FROM plumes p
        CROSS JOIN facilities f
    )
    SELECT *
    FROM distances
    WHERE distance_meters <= 100 AND distance_meters IS NOT NULL
    ORDER BY datetime DESC, distance_meters ASC
""")

print("Satellite Plumes Attributed to Facilities (within 100m radius):")
display(attributed_plumes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

attributed_plumes.write.mode("overwrite").saveAsTable("silver.attributed_plumes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Correlate Emissions with SCADA Anomalies & Production Decline**

# CELL ********************

# Multi-table correlation query - FIXED for MS Fabric
# Standalone version - no dependencies on other notebooks

# First, create the temp views we need
plumes = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.carbon_mapper_plumes")
facilities = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.facility_master")

plumes.createOrReplaceTempView("plumes")
facilities.createOrReplaceTempView("facilities")

# Now run the correlation query
correlation_query = """
WITH plume_facilities AS (
    SELECT 
        p.plume_id,
        p.datetime as detection_time,
        p.emission_auto,
        p.emission_severity,
        f.facility_id,
        f.facility_name
    FROM plumes p
    CROSS JOIN facilities f
    WHERE sqrt(
        pow((CAST(p.plume_longitude AS DOUBLE) - CAST(f.longitude AS DOUBLE)) * 111000 * cos(radians(CAST(f.latitude AS DOUBLE))), 2) +
        pow((CAST(p.plume_latitude AS DOUBLE) - CAST(f.latitude AS DOUBLE)) * 111000, 2)
    ) <= 100
),

scada_pressure_analysis AS (
    SELECT 
        facility_id,
        AVG(measurement_value) as avg_pressure,
        MIN(measurement_value) as min_pressure,
        MAX(measurement_value) as max_pressure,
        (MAX(CASE WHEN rn = max_rn THEN measurement_value END) - 
         MAX(CASE WHEN rn = 1 THEN measurement_value END)) / 
        NULLIF(MAX(CASE WHEN rn = 1 THEN measurement_value END), 0) * 100 as pressure_change_pct
    FROM (
        SELECT 
            facility_id,
            measurement_value,
            ROW_NUMBER() OVER (PARTITION BY facility_id ORDER BY timestamp ASC) as rn,
            COUNT(*) OVER (PARTITION BY facility_id) as max_rn
        FROM GreenSky_Lakehouse.bronze.scada_realtime
        WHERE measurement_type = 'pressure'
          AND measurement_value IS NOT NULL
    ) ranked
    GROUP BY facility_id
    HAVING (MAX(CASE WHEN rn = max_rn THEN measurement_value END) - 
            MAX(CASE WHEN rn = 1 THEN measurement_value END)) / 
           NULLIF(MAX(CASE WHEN rn = 1 THEN measurement_value END), 0) * 100 < -10
),

production_decline_analysis AS (
    SELECT 
        facility_id,
        AVG(oil_volume_bbl) as avg_oil_production,
        (MAX(CASE WHEN rn = max_rn THEN oil_volume_bbl END) - 
         MAX(CASE WHEN rn = 1 THEN oil_volume_bbl END)) / 
        NULLIF(MAX(CASE WHEN rn = 1 THEN oil_volume_bbl END), 0) * 100 as production_change_pct
    FROM (
        SELECT 
            facility_id,
            oil_volume_bbl,
            ROW_NUMBER() OVER (PARTITION BY facility_id ORDER BY production_date ASC) as rn,
            COUNT(*) OVER (PARTITION BY facility_id) as max_rn
        FROM GreenSky_Lakehouse.bronze.daily_production
        WHERE oil_volume_bbl IS NOT NULL
    ) ranked
    GROUP BY facility_id
    HAVING (MAX(CASE WHEN rn = max_rn THEN oil_volume_bbl END) - 
            MAX(CASE WHEN rn = 1 THEN oil_volume_bbl END)) / 
           NULLIF(MAX(CASE WHEN rn = 1 THEN oil_volume_bbl END), 0) * 100 < -10
)

SELECT 
    pf.plume_id,
    pf.facility_id,
    pf.facility_name,
    pf.detection_time,
    ROUND(pf.emission_auto, 2) as emission_kg_hr,
    pf.emission_severity,
    ROUND(sa.pressure_change_pct, 2) as pressure_change_pct,
    ROUND(sa.avg_pressure, 2) as avg_pressure_psi,
    ROUND(pd.production_change_pct, 2) as production_change_pct,
    ROUND(pd.avg_oil_production, 2) as avg_oil_bbl_per_day,
    CASE 
        WHEN sa.pressure_change_pct IS NOT NULL AND pd.production_change_pct IS NOT NULL THEN 'PRESSURE_AND_PRODUCTION'
        WHEN sa.pressure_change_pct IS NOT NULL THEN 'PRESSURE_ANOMALY'
        WHEN pd.production_change_pct IS NOT NULL THEN 'PRODUCTION_DECLINE'
        ELSE 'NO_ANOMALY'
    END as anomaly_type,
    CASE 
        WHEN pf.emission_severity = 'SUPER_EMITTER' AND sa.pressure_change_pct < -15 THEN 'CRITICAL'
        WHEN pf.emission_severity = 'SUPER_EMITTER' OR (sa.pressure_change_pct < -15 AND pd.production_change_pct < -15) THEN 'HIGH'
        WHEN sa.pressure_change_pct IS NOT NULL OR pd.production_change_pct IS NOT NULL THEN 'MEDIUM'
        ELSE 'LOW'
    END as risk_level
FROM plume_facilities pf
LEFT JOIN scada_pressure_analysis sa ON pf.facility_id = sa.facility_id
LEFT JOIN production_decline_analysis pd ON pf.facility_id = pd.facility_id
WHERE sa.facility_id IS NOT NULL OR pd.facility_id IS NOT NULL
ORDER BY 
    CASE 
        WHEN pf.emission_severity = 'SUPER_EMITTER' THEN 1
        WHEN pf.emission_severity = 'HIGH' THEN 2
        WHEN pf.emission_severity = 'MEDIUM' THEN 3
        ELSE 4
    END,
    pf.emission_auto DESC
"""

correlated_events = spark.sql(correlation_query)
print("High-Risk Events: Satellite Detection + SCADA/Production Anomalies")
display(correlated_events)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

correlated_events.write.mode("overwrite").saveAsTable("silver.correlated_events")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Compliance Risk Scoring**

# CELL ********************

# Compliance Risk Assessment - Standalone for MS Fabric
# No dependencies on other notebooks

# First, create the temp views we need
plumes = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.carbon_mapper_plumes")
facilities = spark.sql("SELECT * FROM GreenSky_Lakehouse.bronze.facility_master")

plumes.createOrReplaceTempView("plumes")
facilities.createOrReplaceTempView("facilities")

# Run the compliance risk query
compliance_risk = spark.sql("""
WITH plume_facility_matches AS (
    SELECT 
        f.facility_id,
        p.plume_id,
        p.emission_auto
    FROM facilities f
    CROSS JOIN plumes p
    WHERE sqrt(
        pow((CAST(p.plume_longitude AS DOUBLE) - CAST(f.longitude AS DOUBLE)) * 111000 * cos(radians(CAST(f.latitude AS DOUBLE))), 2) +
        pow((CAST(p.plume_latitude AS DOUBLE) - CAST(f.latitude AS DOUBLE)) * 111000, 2)
    ) <= 100
),
attributed_emissions AS (
    SELECT 
        f.facility_id,
        f.facility_name,
        COUNT(pfm.plume_id) as detection_count,
        MAX(pfm.emission_auto) as max_emission,
        AVG(pfm.emission_auto) as avg_emission
    FROM facilities f
    LEFT JOIN plume_facility_matches pfm ON f.facility_id = pfm.facility_id
    GROUP BY f.facility_id, f.facility_name
),
ldar_status AS (
    SELECT 
        facility_id,
        days_since_inspection,
        leaks_delayed,
        compliance_status
    FROM GreenSky_Lakehouse.bronze.ldar_inspections
),
work_order_history AS (
    SELECT 
        facility_id,
        COUNT(*) as total_wo_count,
        SUM(CASE WHEN category = 'LEAK_REPAIR' THEN 1 ELSE 0 END) as leak_repair_count
    FROM GreenSky_Lakehouse.bronze.maintenance_wo
    GROUP BY facility_id
)
SELECT 
    ae.facility_id,
    ae.facility_name,
    ae.detection_count,
    ROUND(ae.max_emission, 2) as max_emission_kg_hr,
    ROUND(ae.avg_emission, 2) as avg_emission_kg_hr,
    ls.days_since_inspection,
    ls.leaks_delayed,
    ls.compliance_status,
    wo.leak_repair_count,
    wo.total_wo_count,
    (CASE WHEN ae.detection_count > 0 THEN 3 ELSE 0 END +
     CASE WHEN ae.max_emission > 2000 THEN 3 ELSE 0 END +
     CASE WHEN ls.days_since_inspection > 90 THEN 2 ELSE 0 END +
     CASE WHEN ls.leaks_delayed > 0 THEN 2 ELSE 0 END +
     CASE WHEN wo.leak_repair_count >= 3 THEN 2 ELSE 0 END) as compliance_risk_score,
    CASE 
        WHEN (CASE WHEN ae.detection_count > 0 THEN 3 ELSE 0 END +
              CASE WHEN ae.max_emission > 2000 THEN 3 ELSE 0 END +
              CASE WHEN ls.days_since_inspection > 90 THEN 2 ELSE 0 END +
              CASE WHEN ls.leaks_delayed > 0 THEN 2 ELSE 0 END +
              CASE WHEN wo.leak_repair_count >= 3 THEN 2 ELSE 0 END) >= 7 THEN 'CRITICAL'
        WHEN (CASE WHEN ae.detection_count > 0 THEN 3 ELSE 0 END +
              CASE WHEN ae.max_emission > 2000 THEN 3 ELSE 0 END +
              CASE WHEN ls.days_since_inspection > 90 THEN 2 ELSE 0 END +
              CASE WHEN ls.leaks_delayed > 0 THEN 2 ELSE 0 END +
              CASE WHEN wo.leak_repair_count >= 3 THEN 2 ELSE 0 END) >= 4 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as risk_category
FROM attributed_emissions ae
LEFT JOIN ldar_status ls ON ae.facility_id = ls.facility_id
LEFT JOIN work_order_history wo ON ae.facility_id = wo.facility_id
WHERE ae.detection_count > 0
ORDER BY compliance_risk_score DESC
""")

print("Compliance Risk Assessment (Facilities with Detections):")
display(compliance_risk)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

compliance_risk =compliance_risk.dropDuplicates(['facility_id'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(compliance_risk)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM GreenSky_Lakehouse.silver.compliance_risk LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

compliance_risk.write.mode("overwrite").saveAsTable("silver.compliance_risk")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
