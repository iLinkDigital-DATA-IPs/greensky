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
# 51_gen_compliance  ->  gold.fact_compliance_event
# Discrete regulatory events evaluated against dim_regulation thresholds.
# Primary driver: observed plumes whose rate exceeds the NSPS OOOOb "Other
# Large Release Event" threshold (>100 kg/hr). Plus a thinner stream of
# State (TX RRC) venting/flaring events.
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

reg = spark.table("gold.dim_regulation").toPandas()
reg_sk = dict(zip(reg.regulation_id, reg.regulation_sk))
NSPS_SK = int(reg_sk.get("NSPS_OOOOb", 1)); TXRRC_SK = int(reg_sk.get("TX_RRC", NSPS_SK))

# Primary-attributed facility per plume
prim = (spark.table("gold.bridge_plume_facility_attribution")
          .where(F.col("primary_flag") & F.col("facility_sk").isNotNull())
          .select("plume_id", "facility_sk"))
plumes = (spark.table("gold.fact_plume_detection")
            .select("plume_id","detect_ts","emission_rate_kg_s")
            .where((F.col("detect_ts") >= F.lit(str(WINDOW_START))) &
                   (F.col("detect_ts") <= F.lit(str(WINDOW_END)))))
df = prim.join(plumes, "plume_id", "inner").toPandas()
print(f"candidate plume events: {len(df)}")

sc = FINE_SCENARIOS[ACTIVE_FINE_SCENARIO]
fine_lo, fine_hi = sc.get("nsps_state_violation_fine_usd", (5000, 75000))
r = get_rng("compliance")
rows = []; cid = 0

for _, row in df.iterrows():
    rate_kg_hr = float(row.emission_rate_kg_s) * 3600.0
    # ── NSPS OLRE check ──
    if rate_kg_hr > 100.0:
        cid += 1
        sev = "Critical" if rate_kg_hr > 500 else ("Major" if rate_kg_hr > 250 else "Minor")
        rows.append({
            "compliance_sk": cid, "facility_sk": int(row.facility_sk), "equipment_sk": None,
            "regulation_sk": NSPS_SK, "event_ts": pd.Timestamp(row.detect_ts).to_pydatetime(),
            "event_type": "OLRE", "measured_value": round(rate_kg_hr, 1),
            "threshold_value": 100.0, "threshold_unit": "kg_hr_OLRE",
            "is_violation": True, "severity": sev,
            "fine_usd": round(float(r.uniform(fine_lo, fine_hi)) if r.random() < 0.5 else 0.0, 2),
            "status": "Reported", "plume_id": row.plume_id,
        })
    # ── State venting/flaring (thinner, not all are violations) ──
    if r.random() < 0.05:
        cid += 1
        viol = r.random() < 0.4
        rows.append({
            "compliance_sk": cid, "facility_sk": int(row.facility_sk), "equipment_sk": None,
            "regulation_sk": TXRRC_SK, "event_ts": pd.Timestamp(row.detect_ts).to_pydatetime(),
            "event_type": str(r.choice(["Venting","Flaring"])), "measured_value": round(rate_kg_hr, 1),
            "threshold_value": 0.0, "threshold_unit": "event",
            "is_violation": viol, "severity": ("Major" if viol else "Minor"),
            "fine_usd": round(float(r.uniform(fine_lo, fine_hi)) if viol else 0.0, 2),
            "status": ("Reported" if viol else "Closed"), "plume_id": row.plume_id,
        })

cp = pd.DataFrame(rows)
if len(cp) == 0:
    cp = pd.DataFrame([{
        "compliance_sk": 1, "facility_sk": -1, "equipment_sk": None, "regulation_sk": NSPS_SK,
        "event_ts": WINDOW_START.to_pydatetime(), "event_type": "None", "measured_value": 0.0,
        "threshold_value": 100.0, "threshold_unit": "kg_hr_OLRE", "is_violation": False,
        "severity": "Minor", "fine_usd": 0.0, "status": "Closed", "plume_id": None,
    }])
print(f"compliance events: {len(cp)}  violations={int(cp.is_violation.sum())}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cp["equipment_sk"] = cp["equipment_sk"].astype("Int64")  # pandas nullable integer
sdf = (spark.createDataFrame(cp)
         .withColumn("event_ts", F.col("event_ts").cast("timestamp"))
         .withColumn("equipment_sk", F.col("equipment_sk").cast("long")))
sdf = _add_date_sk(sdf, "event_ts")
write_new_fact(sdf, "gold.fact_compliance_event")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
