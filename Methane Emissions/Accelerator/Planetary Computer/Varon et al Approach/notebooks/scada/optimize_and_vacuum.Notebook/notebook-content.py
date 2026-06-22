# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2d020c6b-6189-4bf7-9180-18c5717317e5",
# META       "default_lakehouse_name": "Operations_LH",
# META       "default_lakehouse_workspace_id": "f455d12f-81e4-45ae-9bb7-b195846025fe",
# META       "known_lakehouses": [
# META         {
# META           "id": "2d020c6b-6189-4bf7-9180-18c5717317e5"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "cf70e84c-e5f3-9589-4218-88cc1ae7b47d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# =====================================================================
# 90_optimize_and_vacuum
# Delta maintenance on the gold/silver tables: OPTIMIZE (compaction) and
# VACUUM (retain 168h of history). ZORDER large facts by their common filter
# keys. Mode-independent; safe to run after every backfill or daily load.
# =====================================================================


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run config_and_seeds

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
RETAIN_HOURS = 168

ZORDER = {
    "gold.fact_facility_daily_snapshot": "facility_sk, date_sk",
    "gold.fact_plume_detection":        "detect_ts",
    "gold.fact_emission_episode":       "facility_sk",
    "gold.fact_work_order":             "facility_sk, created_ts",
    "gold.fact_compliance_event":       "facility_sk, event_ts",
    "gold.fact_maintenance":            "facility_sk, maintenance_ts",
    "gold.fact_financial_impact":       "facility_sk, date_sk",
    "gold.sensor_telemetry":            "facility_sk, reading_ts",
    "gold.fact_production_daily":       "facility_sk, date_sk",
}

def list_tables(schema):
    try:
        return [f"{schema}.{r['tableName']}" for r in spark.sql(f"SHOW TABLES IN {schema}").collect()]
    except Exception:
        return []

tables = list_tables("gold") + list_tables("silver")
for t in tables:
    try:
        if t in ZORDER:
            spark.sql(f"OPTIMIZE {t} ZORDER BY ({ZORDER[t]})")
        else:
            spark.sql(f"OPTIMIZE {t}")
        spark.sql(f"VACUUM {t} RETAIN {RETAIN_HOURS} HOURS")
        print(f"  optimized + vacuumed {t}")
    except Exception as e:
        print(f"  skip {t}: {e}")
print("delta maintenance complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # DEBUG


# CELL ********************

raw = spark.table("bronze.plume_raw")
print(f"bronze.plume_raw rows: {raw.count()}")
raw.select("scene_id", "plume_id", "emission_rate_kg_s").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fpd = spark.table("gold.fact_plume_detection")
print(f"total rows: {fpd.count()}")
fpd.groupBy("is_synthetic").count().show()
fpd.where("is_synthetic = false").select("plume_id", "detect_ts", "scene_id").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

raw = spark.table("bronze.plume_raw")
test = (raw.select("scene_id")
    .withColumn("p1_emit",      F.regexp_extract("scene_id", r"emit(\d{8})", 1))
    .withColumn("p2_underscore", F.regexp_extract("scene_id", r"_(\d{8})[Tt_]", 1))
    .withColumn("p3_scn",       F.regexp_extract("scene_id", r"SCN-(\d{8})", 1))
    .withColumn("p4_fallback",  F.regexp_extract("scene_id", r"(20[12]\d[01]\d[0-3]\d)", 1))
)
test.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the try block succeeds and what it prints
raw = spark.table("bronze.plume_raw")
print(f"bronze.plume_raw exists: {raw.count()} rows")
print(f"columns: {raw.columns}")
print(f"scene_id sample: {[r.scene_id for r in raw.select('scene_id').take(5)]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
