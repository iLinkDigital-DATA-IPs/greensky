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
# 60_gen_sensor_telemetry  ->  gold.sensor_telemetry  (+ gold.plume_detection_hot)
# The ONLY Spark-native (distributed) generator. Builds a reading every
# SENSOR_INTERVAL_HOURS for each current sensor across the run window, overlays
# elevated CH4 + exceedance_flag when the sensor's equipment had an active
# emission episode at that instant.
#
# DEFAULT write target is the gold.sensor_telemetry DELTA table (the empty shell
# already present). If you later stand up Operations_EH and grant the workspace
# managed identity the `Database Ingestor` role, flip WRITE_TO_KUSTO = True and
# set KUSTO_CLUSTER (Phase 3/10 of the guide).
#
# Backfill volume ~= n_sensors * (window_days * 24 / interval). For 1,400 sensors
# over 5 years at 4h cadence that is ~15M rows. Set SENSOR_BACKFILL_DAYS to a
# small number on an F4 dev capacity; leave None for the full history.
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

# ── run_mode handling (works in pipeline runs and interactively) ──────
# Pipeline passes base parameter `run_mode`; interactive falls back to backfill.
RUN_MODE = "backfill"
try:
    RUN_MODE = getArgument("run_mode", "backfill")      # set by pipeline activity
except Exception:
    pass
RUN_MODE = (str(RUN_MODE) or "backfill").lower()

INCREMENTAL_LOOKBACK_DAYS = 1
if RUN_MODE == "incremental":
    WINDOW_END   = pd.Timestamp.utcnow().floor("D")
    WINDOW_START = WINDOW_END - pd.Timedelta(days=INCREMENTAL_LOOKBACK_DAYS)
    WRITE_MODE   = "append"
else:
    WINDOW_START = pd.Timestamp(HISTORY_START)
    WINDOW_END   = pd.Timestamp(REAL_PLUME_END)
    WRITE_MODE   = "overwrite"

def _add_date_sk(sdf, ts_col):
    """date_sk matches dim_date (yyyyMMdd as bigint)."""
    return sdf.withColumn("date_sk", F.date_format(F.col(ts_col), "yyyyMMdd").cast("long"))

def write_new_fact(sdf, table):
    """Create/append a NEW gold fact table."""
    w = sdf.write.mode(WRITE_MODE).format("delta")
    if WRITE_MODE == "overwrite":
        w = w.option("overwriteSchema", "true")
    w.saveAsTable(table)
    print(f"{table}: {sdf.count()} rows ({WRITE_MODE})")

print(f"RUN_MODE={RUN_MODE}  window={WINDOW_START.date()}..{WINDOW_END.date()}  write={WRITE_MODE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

WRITE_TO_KUSTO       = False                  # see header; requires Operations_EH
KUSTO_CLUSTER        = "https://<your-eventhouse-query-uri>"
KUSTO_DB             = "Operations_KQL"
SENSOR_BACKFILL_DAYS = None                    # e.g. 30 for dev; None = full window
try:
    SENSOR_BACKFILL_DAYS = int(getArgument("sensor_backfill_days", "0")) or SENSOR_BACKFILL_DAYS
except Exception:
    pass

interval_h = int(SENSOR_INTERVAL_HOURS)
start_ts = WINDOW_START
end_ts   = WINDOW_END
if RUN_MODE != "incremental" and SENSOR_BACKFILL_DAYS:
    start_ts = end_ts - pd.Timedelta(days=int(SENSOR_BACKFILL_DAYS))

n_slots = max(1, int((end_ts - start_ts).total_seconds() // 3600 // interval_h))
print(f"telemetry window {start_ts.date()}..{end_ts.date()}  slots/sensor={n_slots}  interval={interval_h}h")

sensors = (spark.table("gold.dim_sensor").filter("is_current = true")
             .select("sensor_id", "equipment_sk", "facility_sk", "detection_limit_kg_hr"))

slots = (spark.range(0, n_slots)
           .withColumn("reading_ts",
                       F.expr(f"timestampadd(HOUR, CAST(id AS INT) * {interval_h}, timestamp'{start_ts}')")))

tel = sensors.crossJoin(slots)

# deterministic pseudo-random per (sensor, slot)
seed = F.abs(F.hash(F.concat_ws("|", F.lit(MASTER_SEED), F.col("sensor_id"), F.col("id"))))
u1 = (seed % F.lit(100003)) / F.lit(100003.0)
u2 = ((seed * F.lit(31)) % F.lit(100003)) / F.lit(100003.0)
hod = F.hour("reading_ts")
diurnal = 0.05 * F.sin((hod / 24.0) * 2 * 3.14159)

# baseline telemetry only — no episode overlay
tel = (tel
          .withColumn("baseline_ppm", F.round(1.9 + 0.15 * u1 + diurnal, 4))
          .withColumn("ch4_ppm", F.round(F.col("baseline_ppm") + 0.05 * u2, 4))
          .withColumn("exceedance_flag", F.col("ch4_ppm") > (F.col("baseline_ppm") * 1.5))
          .withColumn("wind_speed_ms", F.round(F.lit(CALIBRATION["wind_speed_mean"]) + (u1 - 0.5) * 4.0, 2))
          .withColumn("wind_dir_deg",  F.round(u2 * 360.0, 1))
          .withColumn("sensor_status", F.when(u1 < 0.01, F.lit("Fault")).otherwise(F.lit("OK")))
          .withColumn("ingest_ts", F.current_timestamp()))

cols = ["sensor_id","equipment_sk","facility_sk","reading_ts","ch4_ppm","baseline_ppm",
        "exceedance_flag","wind_speed_ms","wind_dir_deg","sensor_status","ingest_ts"]
out = (tel.withColumn("equipment_sk", F.col("equipment_sk").cast("long"))
          .withColumn("facility_sk",  F.col("facility_sk").cast("long"))
          .select(*cols))

if WRITE_TO_KUSTO:
    (out.write.format("com.microsoft.kusto.spark.datasource")
        .option("kustoCluster", KUSTO_CLUSTER).option("kustoDatabase", KUSTO_DB)
        .option("kustoTable", "sensor_telemetry").option("authType", "AadWorkloadIdentity")
        .mode("Append").save())
    print("telemetry appended to Eventhouse Operations_KQL.sensor_telemetry")
else:
    out.write.mode(WRITE_MODE).format("delta").saveAsTable("gold.sensor_telemetry")
    print(f"gold.sensor_telemetry written ({WRITE_MODE})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── gold.plume_detection_hot : recent real plumes for real-time tiles ─
# Matches the empty shell schema. anchor_distance_km = haversine to nearest anchor.
hot = (spark.table("gold.fact_plume_detection")
         .where(F.col("detect_ts") >= F.lit(str(WINDOW_START)))
         .select("plume_id","detect_ts","source_lat","source_lon",
                 "emission_rate_kg_s","emission_rate_confidence",
                 "wind_speed_ms","wind_dir_deg","is_synthetic")).toPandas()
if len(hot):
    import numpy as _np
    best = None
    for a in ANCHORS.values():
        d = haversine_km(hot.source_lat.values, hot.source_lon.values, a["lat"], a["lon"])
        best = d if best is None else _np.minimum(best, d)
    hot["anchor_distance_km"] = _np.round(best, 3)
else:
    hot["anchor_distance_km"] = []
hot_cols = ["plume_id","detect_ts","source_lat","source_lon","emission_rate_kg_s",
            "emission_rate_confidence","wind_speed_ms","wind_dir_deg","is_synthetic","anchor_distance_km"]
hsdf = (spark.createDataFrame(hot[hot_cols] if len(hot) else pd.DataFrame(columns=hot_cols))
          .withColumn("detect_ts", F.col("detect_ts").cast("timestamp")))
hsdf.write.mode(WRITE_MODE).format("delta").saveAsTable("gold.plume_detection_hot")
print(f"gold.plume_detection_hot: {hsdf.count()} rows ({WRITE_MODE})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
