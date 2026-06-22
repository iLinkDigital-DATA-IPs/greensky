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
# 31_gen_inspection  ->  gold.fact_inspection
# Scheduled component inspections on each asset's inspection_frequency_days.
# Leak-found probability rises with equipment age (condition index) and is
# elevated when an emission episode was active near the inspection date.
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

mfr_rel = dict(MANUFACTURERS)
H_START = pd.Timestamp(HISTORY_START); H_END = pd.Timestamp(REAL_PLUME_END)
_ref = pd.Timestamp(REAL_PLUME_END)
METHODS = ["OGI", "Method21", "AVO", "CMS"]
rows = []; iid = 0

for e in eq.itertuples():
    r    = get_rng("insp", e.equipment_id)
    insp = max(30, int(e.inspection_frequency_days))
    cond = equipment_condition_index(
        age_years=max(0.1, (_ref - pd.Timestamp(e.install_date)).days / 365.0),
        life_years=int(e.expected_life_years),
        days_since_service=insp,
        insp_days=insp,
        reliability_index=mfr_rel.get(e.manufacturer, 1.0),
        leak_propensity=EQUIPMENT_TYPES.get(e.equipment_type, {}).get("leak_propensity", 0.6),
    )
    t = max(pd.Timestamp(e.install_date), H_START) + pd.Timedelta(days=int(r.integers(0, insp)))
    while t <= H_END:
        if WINDOW_START <= t <= WINDOW_END:
            iid += 1
            p_leak = float(min(0.6, 0.04 + 0.55 * cond))  # condition index only, no episode boost
            leak = r.random() < p_leak
            n_checked = int(r.integers(5, 60))
            n_leaks   = int(r.integers(1, max(2, int(n_checked * 0.15)))) if leak else 0
            result = ("Leak Found" if n_leaks > 0
                      else ("Repair Needed" if r.random() < 0.05 else "Pass"))
            rows.append({
                "inspection_sk": iid, "equipment_sk": int(e.equipment_sk),
                "facility_sk": int(e.facility_sk), "team_sk": int(r.integers(1, 9)),
                "inspection_ts": t.to_pydatetime(),
                "inspection_type": str(r.choice(["Routine","Regulatory","FollowUp"], p=[.7,.2,.1])),
                "method": str(r.choice(METHODS, p=[.45,.25,.2,.1])),
                "components_checked": n_checked, "leaks_found": n_leaks,
                "result": result,
                "duration_hours": round(float(r.uniform(0.5, 4.0)), 2),
            })
        t = t + pd.Timedelta(days=insp + int(r.integers(-10, 11)))

ip = pd.DataFrame(rows)
print(f"inspection rows: {len(ip)}  leak_rate={ (ip.leaks_found>0).mean() if len(ip) else 0 :.1%}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sdf = spark.createDataFrame(ip).withColumn("inspection_ts", F.col("inspection_ts").cast("timestamp"))
sdf = _add_date_sk(sdf, "inspection_ts")
write_new_fact(sdf, "gold.fact_inspection")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
