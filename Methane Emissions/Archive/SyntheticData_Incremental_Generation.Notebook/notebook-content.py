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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM GreenSky_LH.bronze.bronze_scada_operations LIMIT 100")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 1: Imports & central config
from datetime import datetime, timedelta
import hashlib
import random
import math

from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable

# ---------- CONFIG - adjust if your env/names differ ----------
CONTROL_TABLE = "control.incremental_watermark"   # small Delta table to track last processed per bronze table
INGEST_LOG_TABLE = "control.ingestion_log"         # optional log table
MAX_ROWS_PER_RUN = 20000                            # safety cap (adjust)
SOURCE_NAME = "SCADA_SYNTH"                        # source identifier for watermarking / logs

# Bronze tables that already exist in lakehouse
BRONZE_FACILITIES = "bronze.brz_facilities"
BRONZE_EQUIPMENTS = "bronze.brz_equipment"
BRONZE_OPERATORS = "bronze.brz_operators"
BRONZE_SCADA = "bronze.brz_scada_operations"  # main time-series table

# For deterministic id generation
def md5_str(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Control table helpers (create/get/update watermark) & ingestion logging
def ensure_control_table():
    if not spark._jsparkSession.catalog().tableExists(CONTROL_TABLE):
        spark.sql(f"""
          CREATE TABLE {CONTROL_TABLE} (
            table_name STRING,
            last_processed_ts TIMESTAMP,
            last_processed_id STRING,
            updated_at TIMESTAMP
          )
          USING delta
        """)
    if not spark._jsparkSession.catalog().tableExists(INGEST_LOG_TABLE):
        spark.sql(f"""
          CREATE TABLE {INGEST_LOG_TABLE} (
            source STRING,
            table_name STRING,
            run_ts TIMESTAMP,
            rows_generated LONG,
            status STRING,
            message STRING
          )
          USING delta
        """)

def get_watermark(table_name):
    ensure_control_table()
    row = spark.sql(f"SELECT last_processed_ts, last_processed_id FROM {CONTROL_TABLE} WHERE table_name = '{table_name}'").collect()
    if row:
        return row[0]["last_processed_ts"], row[0]["last_processed_id"]
    else:
        # if missing, return a default: 90 days back for time-series tables; None for ids
        default_ts = datetime.now() - timedelta(days=90)
        return default_ts, None

def update_watermark(table_name, last_ts=None, last_id=None):
    # upsert into control table
    if last_ts is None:
        last_ts_sql = "NULL"
    else:
        last_ts_sql = f"cast('{last_ts}' as timestamp)"
    last_id_sql = "NULL" if last_id is None else f"'{last_id}'"
    spark.sql(f"""
      MERGE INTO {CONTROL_TABLE} tgt
      USING (SELECT '{table_name}' as table_name, {last_ts_sql} as last_processed_ts, {last_id_sql} as last_processed_id, current_timestamp() as updated_at) src
      ON tgt.table_name = src.table_name
      WHEN MATCHED THEN UPDATE SET tgt.last_processed_ts = src.last_processed_ts, tgt.last_processed_id = src.last_processed_id, tgt.updated_at = src.updated_at
      WHEN NOT MATCHED THEN INSERT (table_name, last_processed_ts, last_processed_id, updated_at) VALUES (src.table_name, src.last_processed_ts, src.last_processed_id, src.updated_at)
    """)

def log_ingest(source, table_name, rows_generated, status, message=""):
    ensure_control_table()  # also ensures ingestion log exists
    spark.sql(f"INSERT INTO {INGEST_LOG_TABLE} VALUES ('{source}', '{table_name}', current_timestamp(), {rows_generated}, '{status}', '{message}')")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Helper to add CDC-ish columns to DataFrame (pyspark df)
def add_cdc_columns(spark_df):
    """
    Adds standard CDC columns used by your reference notebook.
    - _created_at: ingestion timestamp (current)
    - _modified_at: same as created for inserts
    - _record_version: int (1 for newly generated)
    - _is_current: True for generated inserts
    - _operation: descriptive (INSERT/UPDATE)
    - _record_hash: md5 of business fields (string)
    """
    # compute a simple composite hash from concatenated columns (excluding internal timestamps if present).
    # We assume the caller already included business columns in df.
    cols = [c for c in spark_df.columns if not c.startswith("_")]
    concat_expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols])
    enriched = (spark_df
                .withColumn("_created_at", F.current_timestamp())
                .withColumn("_modified_at", F.current_timestamp())
                .withColumn("_record_version", F.lit(1))
                .withColumn("_is_current", F.lit(True))
                .withColumn("_operation", F.lit("INSERT"))
                .withColumn("_record_hash", F.md5(concat_expr))
               )
    return enriched


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Facilities incremental generator & append
from datetime import datetime

table_name = BRONZE_FACILITIES
last_ts, last_id = get_watermark(table_name)

# We'll generate new facility rows by deterministic incremental id using last_id or start index.
def generate_facilities(existing_count_hint=0, n_new=1):
    """
    Create 'n_new' new facility records.
    Business fields: facility_id, name, location (city, lat, lon), capacity
    """
    import pandas as pd
    new_rows = []
    base_index = 0
    if last_id:
        # last_id format: FAC-000123 -> extract number
        try:
            base_index = int(last_id.split("-")[-1])
        except Exception:
            base_index = existing_count_hint
    for i in range(1, n_new+1):
        idx = base_index + i
        facility_id = f"FAC-{idx:06d}"
        name = f"Facility-{idx:04d}"
        # pseudo-random but deterministic-ish location
        lat = 12.9 + ((idx % 90) * 0.01)
        lon = 77.5 + ((idx % 180) * 0.01)
        city = ["Bengaluru","Chennai","Pune","Hyderabad","Delhi"][idx % 5]
        capacity = 100 + (idx % 400)
        new_rows.append((facility_id, name, city, float(lat), float(lon), int(capacity)))
    pdf = pd.DataFrame(new_rows, columns=["facility_id","facility_name","city","latitude","longitude","capacity"])
    sdf = spark.createDataFrame(pdf)
    return sdf

# generate (tune n_new as you like); keep it small for safety
fac_sdf = generate_facilities(n_new=1)
fac_sdf = add_cdc_columns(fac_sdf)

# Append to bronze facility table
rows_before = spark.table(table_name).count() if spark._jsparkSession.catalog().tableExists(table_name) else 0
#fac_sdf.write.format("delta").mode("append").saveAsTable(table_name)
rows_appended = fac_sdf.count()
log_ingest(SOURCE_NAME, table_name, rows_appended, "SUCCESS", f"Appended {rows_appended} facility rows from last_id {last_id}")
# Update watermark: set last_processed_id to the last generated id
last_generated_id = fac_sdf.orderBy(F.desc("facility_id")).limit(1).collect()[0]["facility_id"]
update_watermark(table_name, last_ts=None, last_id=last_generated_id)
print(f"Facilities: appended {rows_appended} rows, new last_id={last_generated_id}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Equipment incremental generator & append
table_name = BRONZE_EQUIPMENTS
last_ts, last_id = get_watermark(table_name)

def generate_equipment(n_new=10, facility_pool=50):
    """
    Generates equipment rows with:
    equipment_id, facility_id (FK), equipment_type, install_date
    """
    import pandas as pd
    new_rows = []
    base_index = 0
    if last_id:
        try:
            base_index = int(last_id.split("-")[-1])
        except:
            base_index = 0
    for i in range(1, n_new+1):
        idx = base_index + i
        equipment_id = f"EQP-{idx:07d}"
        # assign facility deterministically (FAC-00000X)
        fac_idx = (idx % facility_pool) + 1
        facility_id = f"FAC-{fac_idx:06d}"
        eq_type = ["PUMP","VALVE","MOTOR","SENSOR"][idx % 4]
        # install_date spread across last 3 years
        install_days_ago = (idx % 1000)
        install_date = (datetime.now() - timedelta(days=install_days_ago)).date().isoformat()
        new_rows.append((equipment_id, facility_id, eq_type, install_date))
    pdf = pd.DataFrame(new_rows, columns=["equipment_id","facility_id","equipment_type","install_date"])
    sdf = spark.createDataFrame(pdf)
    return sdf

eqp_sdf = generate_equipment(n_new=20, facility_pool=50)
eqp_sdf = add_cdc_columns(eqp_sdf)

# Append to bronze equipment table
# eqp_sdf.write.format("delta").mode("append").saveAsTable(table_name)
rows_appended = eqp_sdf.count()
log_ingest(SOURCE_NAME, table_name, rows_appended, "SUCCESS", f"Appended {rows_appended} equipment rows")
last_generated_id = eqp_sdf.orderBy(F.desc("equipment_id")).limit(1).collect()[0]["equipment_id"]
update_watermark(table_name, last_ts=None, last_id=last_generated_id)
print(f"Equipment: appended {rows_appended} rows, new last_id={last_generated_id}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
