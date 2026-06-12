# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "11ca0f84-52dc-44fc-9270-538fcda8f1ad",
# META       "default_lakehouse_name": "Operations_LH",
# META       "default_lakehouse_workspace_id": "060ba34b-f1a3-4509-a6e2-36d1e736a8eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "11ca0f84-52dc-44fc-9270-538fcda8f1ad"
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
except:
    pass
RUN_MODE = str(RUN_MODE).lower()

if RUN_MODE == "incremental":
    WINDOW_START = pd.Timestamp.now('UTC').floor("D") - pd.Timedelta(days=7)
    WINDOW_END   = pd.Timestamp.now('UTC').floor("D")
else:
    WINDOW_START = pd.Timestamp(REAL_PLUME_START)
    WINDOW_END   = pd.Timestamp(REAL_PLUME_END)

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

# Columns present in both source and gold (excluding computed/derived ones added later)
gold_cols = {f.name for f in spark.table("gold.fact_plume_detection").schema}
exclude   = {"detect_ts", "episode_sk", "is_synthetic"}

available = [c for c in raw.columns if c in gold_cols and c not in exclude]

print(f"Source rows in window: {raw.count()}")
print(f"Mapped columns ({len(available)}): {available}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get gold schema as a type map
gold_schema = {f.name: f.dataType for f in spark.table("gold.fact_plume_detection").schema}

# Select available columns and cast each to match gold
real = raw.select(*available)
for col_name in available:
    if col_name in gold_schema:
        real = real.withColumn(col_name, F.col(col_name).cast(gold_schema[col_name]))

# Add the new columns
real = (real.withColumn("detect_ts", F.to_date(F.col("scene_id").substr(7, 8), "yyyyMMdd").cast("timestamp"))
            .withColumn("episode_sk", F.lit(None).cast("long"))
            .withColumn("is_synthetic", F.lit(False)))

# Fill missing columns
existing_schema = [f.name for f in spark.table("gold.fact_plume_detection").schema]
for c in existing_schema:
    if c not in real.columns:
        real = real.withColumn(c, F.lit(None))
real = real.select(*existing_schema)

real.write.mode("append").format("delta").saveAsTable("gold.fact_plume_detection")
print(f"Appended {real.count()} new real plumes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
