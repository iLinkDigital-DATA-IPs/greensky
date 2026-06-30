# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "70049c2e-4fc1-4441-80d3-1380dd23d5fb",
# META       "default_lakehouse_name": "Operations_LH",
# META       "default_lakehouse_workspace_id": "f455d12f-81e4-45ae-9bb7-b195846025fe",
# META       "known_lakehouses": [
# META         {
# META           "id": "70049c2e-4fc1-4441-80d3-1380dd23d5fb"
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

# 22_ingest_real_plumes → incremental append of new real plumes to fact_plume_detection


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

RUN_MODE = "incremental"
try:
    RUN_MODE = getArgument("run_mode", "incremental")
except Exception:
    pass
RUN_MODE = str(RUN_MODE).lower()

_default_end   = pd.Timestamp.now("UTC").floor("D").strftime("%Y-%m-%d")
_default_start = (pd.Timestamp.now("UTC").floor("D") - pd.Timedelta(days=7)).strftime("%Y-%m-%d")

try:
    _start_override = getArgument("start_date", "")
    _end_override   = getArgument("end_date",   "")
except Exception:
    _start_override = ""
    _end_override   = ""

if RUN_MODE == "incremental":
    WINDOW_START = pd.Timestamp.now("UTC").floor("D") - pd.Timedelta(days=7)
    WINDOW_END   = pd.Timestamp.now("UTC").floor("D")
else:
    WINDOW_START = pd.Timestamp(REAL_PLUME_START)
    WINDOW_END   = pd.Timestamp(REAL_PLUME_END)

if _start_override:
    WINDOW_START = pd.Timestamp(_start_override)
if _end_override:
    WINDOW_END   = pd.Timestamp(_end_override)

print(f"Ingesting real plumes: {WINDOW_START.date()} to {WINDOW_END.date()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

spark.table("bronze.plume_raw") \
    .withColumn("parsed_date", F.to_date(F.col("scene_id").substr(7, 8), "yyyyMMdd")) \
    .groupBy("parsed_date") \
    .count() \
    .orderBy("parsed_date", ascending=False) \
    .show(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read raw real plumes from source, filtered to ingest window
raw = spark.table("bronze.plume_raw")

raw = raw.filter(
    (F.to_date(F.col("scene_id").substr(7, 8), "yyyyMMdd") >= F.lit(WINDOW_START.date())) &
    (F.to_date(F.col("scene_id").substr(7, 8), "yyyyMMdd") <  F.lit(WINDOW_END.date()))
)

exclude = {"detect_ts", "episode_sk", "is_synthetic"}

# Try to get schema from existing gold table; if it doesn't exist yet, use all raw columns
try:
    gold_cols = {f.name for f in spark.table("gold.fact_plume_detection").schema}
    available = [c for c in raw.columns if c in gold_cols and c not in exclude]
    print("Schema sourced from existing gold.fact_plume_detection")
except Exception:
    available = [c for c in raw.columns if c not in exclude]
    print("gold.fact_plume_detection not found — using all raw columns for first run")

print(f"Source rows in window: {raw.count()}")
print(f"Mapped columns ({len(available)}): {available}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get gold schema as a type map (only if table already exists)
try:
    gold_schema = {f.name: f.dataType for f in spark.table("gold.fact_plume_detection").schema}
    existing_schema = list(gold_schema.keys())
    table_exists = True
except Exception:
    gold_schema = {}
    existing_schema = None
    table_exists = False

# Select available columns and cast each to match gold
real = raw.select(*available)

if table_exists:
    for col_name in available:
        if col_name in gold_schema:
            real = real.withColumn(col_name, F.col(col_name).cast(gold_schema[col_name]))

# Add the new columns
real = (real
    .withColumn("detect_ts", F.to_date(F.col("scene_id").substr(7, 8), "yyyyMMdd").cast("timestamp"))
    .withColumn("episode_sk", F.lit(None).cast("long"))
    .withColumn("is_synthetic", F.lit(False))
)

# Fill missing columns and reorder to match gold schema (only if table pre-existed)
if table_exists:
    for c in existing_schema:
        if c not in real.columns:
            real = real.withColumn(c, F.lit(None))
    real = real.select(*existing_schema)

if RUN_MODE == "incremental":
    real.write.mode("append").format("delta").saveAsTable("gold.fact_plume_detection")
else:
    real.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").saveAsTable("gold.fact_plume_detection")

print(f"Wrote {real.count()} new real plumes (table_exists={table_exists})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
