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
ep = (spark.table("gold.fact_emission_episode")
        .select("episode_sk","equipment_sk","facility_sk","start_ts","end_ts",
                "peak_rate_kg_s","total_mass_kg","root_cause")
        .toPandas())
ep["end_ts"] = pd.to_datetime(ep["end_ts"])

mfr_rel  = dict(MANUFACTURERS)
etype_p  = {k: v for k, v in EQUIPMENT_TYPES.items()}
LABOR_RATE = 95.0   # USD / hour

H_START = pd.Timestamp(HISTORY_START)
H_END   = pd.Timestamp(REAL_PLUME_END)
rows = []; mid = 0

# ── (1) Scheduled preventive maintenance ─────────────────────────────
for e in eq.itertuples():
    r    = get_rng("maint_pm", e.equipment_id)
    insp = max(30, int(e.inspection_frequency_days))
    t = max(pd.Timestamp(e.install_date), H_START) + pd.Timedelta(days=int(r.integers(0, insp)))
    while t <= H_END:
        if WINDOW_START <= t <= WINDOW_END:
            mid += 1
            labor = float(r.uniform(2, 9))
            parts = float(r.gamma(2.0, 220.0))
            rows.append({
                "maintenance_sk": mid, "equipment_sk": int(e.equipment_sk),
                "facility_sk": int(e.facility_sk), "team_sk": int(r.integers(1, 9)),
                "contractor_sk": (int(r.integers(1, 7)) if r.random() < 0.25 else None),
                "maintenance_ts": t.to_pydatetime(),
                "maintenance_type": "Preventive", "trigger": "Scheduled",
                "downtime_hours": round(labor * float(r.uniform(0.4, 1.2)), 2),
                "labor_hours": round(labor, 2),
                "parts_cost_usd": round(parts, 2),
                "total_cost_usd": round(labor * LABOR_RATE + parts, 2),
                "root_cause": None, "episode_sk": None, "is_completed": True,
            })
        t = t + pd.Timedelta(days=insp + int(r.integers(-12, 13)))

# ── (2) Corrective maintenance from emission episodes ────────────────
for a in ep.itertuples():
    end_ts = pd.Timestamp(a.end_ts)
    repair_ts = end_ts + pd.Timedelta(hours=float(get_rng("maint_corr", a.episode_sk).gamma(2.0, 30.0)))
    if not (WINDOW_START <= repair_ts <= WINDOW_END):
        continue
    r   = get_rng("maint_corr2", a.episode_sk)
    sev = float(a.peak_rate_kg_s)
    big = sev > 0.5
    labor = float(r.uniform(4, 22)) * (1.6 if big else 1.0)
    parts = float(r.gamma(2.5, 550.0)) * (2.0 if big else 1.0)
    mid += 1
    rows.append({
        "maintenance_sk": mid, "equipment_sk": int(a.equipment_sk),
        "facility_sk": int(a.facility_sk), "team_sk": int(r.integers(1, 9)),
        "contractor_sk": (int(r.integers(1, 7)) if r.random() < 0.45 else None),
        "maintenance_ts": repair_ts.to_pydatetime(),
        "maintenance_type": "Corrective", "trigger": "Episode",
        "downtime_hours": round(labor * float(r.uniform(0.6, 1.6)), 2),
        "labor_hours": round(labor, 2),
        "parts_cost_usd": round(parts, 2),
        "total_cost_usd": round(labor * LABOR_RATE + parts, 2),
        "root_cause": a.root_cause, "episode_sk": int(a.episode_sk), "is_completed": True,
    })

mp = pd.DataFrame(rows)
print(f"maintenance rows: {len(mp)}  "
      f"(corrective={int((mp.trigger=='Episode').sum()) if len(mp) else 0})")

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
