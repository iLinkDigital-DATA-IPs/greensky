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
# 70_build_snapshots  ->  gold.fact_facility_daily_snapshot  +  gold.facility_kpi_snapshot_6h
# Aggregates the downstream facts to facility grain. Health/risk scores (kept
# OUT of dim_build because they are volatile) are computed here. Run this LAST,
# after 30/31/32/50/51/52/60 have populated their tables.
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
RUN_MODE = "backfill"
try:
    RUN_MODE = getArgument("run_mode", "backfill")
except Exception:
    pass
RUN_MODE = (str(RUN_MODE) or "backfill").lower()

try:
    _start_override = getArgument("start_date", "")
    _end_override   = getArgument("end_date",   "")
except Exception:
    _start_override = ""
    _end_override   = ""

INCREMENTAL_LOOKBACK_DAYS = 1
if RUN_MODE == "incremental":
    WINDOW_END   = pd.Timestamp.utcnow().floor("D")
    WINDOW_START = WINDOW_END - pd.Timedelta(days=INCREMENTAL_LOOKBACK_DAYS)
    WRITE_MODE   = "append"
else:
    WINDOW_START = pd.Timestamp(HISTORY_START)
    WINDOW_END   = pd.Timestamp.utcnow().floor("D")  # updated: was pd.Timestamp(REAL_PLUME_END)
    WRITE_MODE   = "overwrite"

# Override with explicit dates if passed — these take precedence over run_mode
if _start_override:
    WINDOW_START = pd.Timestamp(_start_override)
if _end_override:
    WINDOW_END = pd.Timestamp(_end_override)

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

from pyspark.sql import functions as F

def safe_table(name):
    try:
        return spark.table(name)
    except Exception as e:
        print(f"  (skip {name}: {e})")
        return None

W0, W1 = F.lit(str(WINDOW_START)), F.lit(str(WINDOW_END))
print(f"Filtering plumes between {str(WINDOW_START)} and {str(WINDOW_END)}")

# ── facility x day emission aggregates (from attributed plumes) ───────
bridge = (spark.table("gold.bridge_plume_facility_attribution")
            .where(F.col("facility_sk").isNotNull())
            .select("plume_id","facility_sk","allocation_factor","primary_flag"))
plumes = (spark.table("gold.fact_plume_detection")
            .select("plume_id","detect_ts","emission_rate_kg_s")
            .filter("is_synthetic = false")
            .where((F.col("detect_ts") >= W0) & (F.col("detect_ts") <= W1)))

# Add this to confirm plumes are being found before proceeding
print(f"Plumes in window: {plumes.count()}")
print(f"Bridge rows: {bridge.count()}")

pj = (bridge.join(plumes, "plume_id", "inner")
        .withColumn("d", F.to_date("detect_ts"))
        .withColumn("alloc_rate", F.col("emission_rate_kg_s") * F.col("allocation_factor")))

print(f"Joined rows: {pj.count()}")

emis = (pj.groupBy("facility_sk", "d")
          .agg(F.countDistinct(F.when(F.col("primary_flag"), F.col("plume_id"))).alias("active_plumes"),
               F.sum("alloc_rate").alias("emission_rate_kg_s_sum"))
          .withColumn("total_emissions_kg", F.col("emission_rate_kg_s_sum") * F.lit(3600.0)))

# ── work orders open per facility-day & MTTR ──────────────────────────
wo = safe_table("gold.fact_work_order")
if wo is not None:
    wo_open = (wo.withColumn("d", F.to_date("created_ts"))
                 .groupBy("facility_sk", "d")
                 .agg(F.sum(F.when(F.col("status") != "Closed", 1).otherwise(0)).alias("open_tickets"),
                      F.avg("resolution_time_hours").alias("mean_time_to_repair_hr")))
else:
    wo_open = emis.select("facility_sk", "d").withColumn("open_tickets", F.lit(0)) \
                  .withColumn("mean_time_to_repair_hr", F.lit(None).cast("double"))

# ── compliance violations per facility-day ────────────────────────────
ce = safe_table("gold.fact_compliance_event")
if ce is not None:
    comp = (ce.where(F.col("is_violation")).withColumn("d", F.to_date("event_ts"))
              .groupBy("facility_sk", "d").agg(F.count("*").alias("compliance_violations")))
else:
    comp = emis.select("facility_sk", "d").withColumn("compliance_violations", F.lit(0))

# ── per-facility equipment health (volatile measure) ──────────────────
eq = spark.table("silver.equipment_geo").toPandas()
mfr_rel = dict(MANUFACTURERS); _ref = pd.Timestamp.now("UTC").replace(tzinfo=None)
def _health(row):
    cond = equipment_condition_index(
        age_years=max(0.1, (_ref - pd.Timestamp(row.install_date)).days/365.0),
        life_years=int(row.expected_life_years),
        days_since_service=int(row.inspection_frequency_days),
        insp_days=int(row.inspection_frequency_days),
        reliability_index=mfr_rel.get(row.manufacturer, 1.0),
        leak_propensity=EQUIPMENT_TYPES.get(row.equipment_type, {}).get("leak_propensity", 0.6))
    return 1.0 - cond
eq["health"] = eq.apply(_health, axis=1)
health_pdf = eq.groupby("facility_sk")["health"].mean().reset_index()
health = spark.createDataFrame(health_pdf).withColumnRenamed("health", "equipment_health_score")
# ── assemble daily snapshot ───────────────────────────────────────────
snap = (emis.join(wo_open, ["facility_sk","d"], "outer")
            .join(comp,    ["facility_sk","d"], "outer")
            .join(health,  "facility_sk", "left")
            .na.fill({"active_plumes":0,"open_tickets":0,"compliance_violations":0,
                      "emission_rate_kg_s_sum":0.0,"total_emissions_kg":0.0,
                      "equipment_health_score":0.7}))

GAS_PRICE = float(GAS_PRICE_USD_PER_MCF); KG_MCF = float(KG_CH4_PER_MCF)
snap = (snap
        .withColumn("daily_financial_impact",
                    F.round(F.col("total_emissions_kg") / F.lit(KG_MCF) * F.lit(GAS_PRICE), 2))
        .withColumn("risk_score", F.round(
            F.least(F.lit(100.0),
                    40.0 * (1.0 - F.col("equipment_health_score"))
                    + 8.0 * F.col("open_tickets")
                    + 12.0 * F.col("compliance_violations")
                    + 5.0 * F.col("active_plumes")), 2))
        .withColumn("snapshot_date", F.to_timestamp("d"))
        .withColumn("date_sk", F.date_format("d", "yyyyMMdd").cast("long"))
        .withColumn("mean_time_to_repair_hr", F.col("mean_time_to_repair_hr").cast("double")))

snap_cols = ["date_sk","facility_sk","snapshot_date","active_plumes","open_tickets",
             "compliance_violations","risk_score","daily_financial_impact",
             "mean_time_to_repair_hr","equipment_health_score","emission_rate_kg_s_sum",
             "total_emissions_kg"]
snap_out = (snap.where(F.col("facility_sk").isNotNull())
                .withColumn("active_plumes", F.col("active_plumes").cast("int"))
                .withColumn("open_tickets", F.col("open_tickets").cast("int"))
                .withColumn("compliance_violations", F.col("compliance_violations").cast("int"))
                .select(*snap_cols))
write_new_fact(snap_out, "gold.fact_facility_daily_snapshot")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── 6-hour KPI snapshot -> existing empty shell facility_kpi_snapshot_6h ──
# Schema is fixed by the shell (active_plumes/open_tickets/compliance_violations = INT).
tel = None
try:
    tel = spark.table("gold.sensor_telemetry").where((F.col("reading_ts") >= W0) & (F.col("reading_ts") <= W1))
except Exception as e:
    print(f"  (sensor_telemetry not available, 6h snapshot from plumes only: {e})")

if tel is not None and tel.head(1):
    kpi = (tel.withColumn("snapshot_ts", F.date_trunc("HOUR", F.col("reading_ts")))
              .withColumn("snapshot_ts", F.expr("timestampadd(HOUR, -(hour(snapshot_ts) % 6), snapshot_ts)"))
              .groupBy("snapshot_ts", "facility_sk")
              .agg(F.sum(F.when(F.col("exceedance_flag"), 1).otherwise(0)).alias("exceedances"),
                   F.avg("ch4_ppm").alias("avg_ch4")))
else:
    kpi = (pj.withColumn("snapshot_ts", F.date_trunc("DAY", F.col("detect_ts")))
             .groupBy("snapshot_ts", "facility_sk")
             .agg(F.count("*").alias("exceedances"), F.avg("emission_rate_kg_s").alias("avg_ch4")))

# join the daily measures onto the 6h grain (approximate; one daily value per bin)
daily_small = snap_out.select("facility_sk",
                              F.col("snapshot_date").cast("date").alias("d"),
                              "active_plumes","open_tickets","compliance_violations",
                              "risk_score","daily_financial_impact","mean_time_to_repair_hr",
                              "equipment_health_score","emission_rate_kg_s_sum")
kpi6 = (kpi.withColumn("d", F.to_date("snapshot_ts"))
           .join(daily_small, ["facility_sk","d"], "left")
           .na.fill({"active_plumes":0,"open_tickets":0,"compliance_violations":0,
                     "risk_score":0.0,"daily_financial_impact":0.0,
                     "equipment_health_score":0.7,"emission_rate_kg_s_sum":0.0}))
kpi6_cols = ["snapshot_ts","facility_sk","active_plumes","open_tickets","compliance_violations",
             "risk_score","monthly_financial_impact","mean_time_to_repair_hr",
             "equipment_health_score","emission_rate_kg_s_sum"]
kpi6_out = (kpi6
            .withColumn("monthly_financial_impact", F.round(F.col("daily_financial_impact") * F.lit(30.0), 2))
            .withColumn("active_plumes", F.col("active_plumes").cast("int"))
            .withColumn("open_tickets", F.col("open_tickets").cast("int"))
            .withColumn("compliance_violations", F.col("compliance_violations").cast("int"))
            .withColumn("mean_time_to_repair_hr", F.col("mean_time_to_repair_hr").cast("double"))
            .select(*kpi6_cols))
kpi6_out.write.mode(WRITE_MODE).format("delta").saveAsTable("gold.facility_kpi_snapshot_6h")
print(f"gold.facility_kpi_snapshot_6h: {kpi6_out.count()} rows ({WRITE_MODE})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gold.fact_facility_daily_snapshot") \
    .filter("snapshot_date >= '2026-05-19'") \
    .selectExpr(
        "count(*) as rows",
        "sum(total_emissions_kg) as total_kg",
        "sum(active_plumes) as total_plumes"
    ).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
