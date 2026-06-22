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
# 30_gen_maintenance  ->  gold.fact_maintenance
# Two streams: (1) scheduled preventive maintenance on the equipment's
# inspection cadence, (2) corrective repairs triggered by emission episodes.
# Uses the SHARED equipment_condition_index from config so PM/corrective mix
# stays consistent with the episode hazard model (no label leakage: condition
# is built from age/service/reliability, not from emissions).
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

eq = spark.table("silver.equipment_geo").toPandas()

mfr_rel    = dict(MANUFACTURERS)
etype_p    = {k: v for k, v in EQUIPMENT_TYPES.items()}
LABOR_RATE = 95.0  # USD / hour

# Ensure all timestamps are tz-naive for comparison consistency
H_START      = pd.Timestamp(HISTORY_START).tz_localize(None)
H_END        = pd.Timestamp(REAL_PLUME_END).tz_localize(None)
_WIN_START   = pd.Timestamp(WINDOW_START).tz_localize(None)
_WIN_END     = pd.Timestamp(WINDOW_END).tz_localize(None)

rows = []; mid = 0

# ── Scheduled preventive maintenance only ────────────────────────────
for e in eq.itertuples():
    r    = get_rng("maint_pm", e.equipment_id)
    insp = max(30, int(e.inspection_frequency_days))
    install = pd.Timestamp(e.install_date).tz_localize(None) if pd.Timestamp(e.install_date).tzinfo else pd.Timestamp(e.install_date)
    t = max(install, H_START) + pd.Timedelta(days=int(r.integers(0, insp)))
    while t <= H_END:
        if _WIN_START <= t <= _WIN_END:
            mid += 1
            labor = float(r.uniform(2, 9))
            parts = float(r.gamma(2.0, 220.0))
            rows.append({
                "maintenance_sk":  mid,
                "equipment_sk":    int(e.equipment_sk),
                "facility_sk":     int(e.facility_sk),
                "team_sk":         int(r.integers(1, 9)),
                "contractor_sk":   int(r.integers(1, 7)) if r.random() < 0.25 else pd.NA,
                "maintenance_ts":  t.to_pydatetime(),
                "maintenance_type": "Preventive",
                "trigger":         "Scheduled",
                "downtime_hours":  round(labor * float(r.uniform(0.4, 1.2)), 2),
                "labor_hours":     round(labor, 2),
                "parts_cost_usd":  round(parts, 2),
                "total_cost_usd":  round(labor * LABOR_RATE + parts, 2),
                "root_cause":      pd.NA,
                "episode_sk":      pd.NA,
                "is_completed":    True,
            })
        t = t + pd.Timedelta(days=insp + int(r.integers(-12, 13)))

mp = pd.DataFrame(rows)

# Explicitly type the nullable integer columns so Spark can infer schema
mp["contractor_sk"] = mp["contractor_sk"].astype("Int64")
mp["episode_sk"]    = mp["episode_sk"].astype("Int64")
mp["root_cause"]    = mp["root_cause"].astype("object")

print(f"maintenance rows: {len(mp)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sdf = spark.createDataFrame(mp).withColumn("maintenance_ts", F.col("maintenance_ts").cast("timestamp"))
sdf = _add_date_sk(sdf, "maintenance_ts")
write_new_fact(sdf, "gold.fact_maintenance")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
