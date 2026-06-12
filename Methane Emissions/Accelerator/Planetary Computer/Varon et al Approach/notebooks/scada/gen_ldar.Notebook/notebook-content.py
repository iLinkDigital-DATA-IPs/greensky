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

# =====================================================================
# 32_gen_ldar  ->  gold.fact_ldar_survey
# Facility-level Leak Detection And Repair campaigns (quarterly-ish), the
# regulatory counterpart to per-asset inspections. Leaks detected scale with
# the facility's active-episode count in the surrounding quarter.
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

fac = spark.table("gold.dim_facility").filter("is_current = true").select("facility_sk").toPandas()
ep  = (spark.table("gold.fact_emission_episode")
         .select("facility_sk","start_ts").toPandas())
ep["start_ts"] = pd.to_datetime(ep["start_ts"])

H_START = pd.Timestamp(HISTORY_START); H_END = pd.Timestamp(REAL_PLUME_END)
PROGRAMS = ["NSPS_OOOOb", "State", "Voluntary"]
SURVEY_CADENCE_DAYS = 90
rows = []; sid = 0

for f in fac.itertuples():
    r = get_rng("ldar", int(f.facility_sk))
    f_ep = ep[ep.facility_sk == f.facility_sk]
    t = H_START + pd.Timedelta(days=int(r.integers(0, SURVEY_CADENCE_DAYS)))
    while t <= H_END:
        if WINDOW_START <= t <= WINDOW_END:
            sid += 1
            q_lo = t - pd.Timedelta(days=SURVEY_CADENCE_DAYS)
            active = int(((f_ep.start_ts >= q_lo) & (f_ep.start_ts <= t)).sum())
            surveyed = int(r.integers(40, 400))
            detected = int(min(surveyed, r.poisson(0.6 + 0.8 * active)))
            repaired = int(detected * float(r.uniform(0.7, 1.0)))
            rows.append({
                "survey_sk": sid, "facility_sk": int(f.facility_sk),
                "team_sk": int(r.integers(1, 9)), "survey_ts": t.to_pydatetime(),
                "survey_method": str(r.choice(["OGI","Method21","Aerial"], p=[.6,.25,.15])),
                "regulatory_program": str(r.choice(PROGRAMS, p=[.5,.35,.15])),
                "components_surveyed": surveyed, "leaks_detected": detected,
                "leaks_repaired": repaired,
                "repair_cost_usd": round(repaired * float(r.uniform(400, 1800)), 2),
                "duration_hours": round(float(r.uniform(3, 12)), 2),
            })
        t = t + pd.Timedelta(days=SURVEY_CADENCE_DAYS + int(r.integers(-7, 8)))

lp = pd.DataFrame(rows)
print(f"ldar surveys: {len(lp)}  total leaks detected={int(lp.leaks_detected.sum()) if len(lp) else 0}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sdf = spark.createDataFrame(lp).withColumn("survey_ts", F.col("survey_ts").cast("timestamp"))
sdf = _add_date_sk(sdf, "survey_ts")
write_new_fact(sdf, "gold.fact_ldar_survey")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
