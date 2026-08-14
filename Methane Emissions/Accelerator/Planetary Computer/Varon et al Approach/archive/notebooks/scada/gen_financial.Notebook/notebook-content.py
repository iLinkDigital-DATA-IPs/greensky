# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f689a941-8f87-46d7-997b-ac53bc83c5c5",
# META       "default_lakehouse_name": "Operations_LH",
# META       "default_lakehouse_workspace_id": "640876ea-6158-4ffd-8598-5eb210e088a0",
# META       "known_lakehouses": [
# META         {
# META           "id": "f689a941-8f87-46d7-997b-ac53bc83c5c5"
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
# 50_gen_financial  ->  gold.fact_production_daily  +  gold.fact_financial_impact
# fact_production_daily: facility x day gas/oil volumes (feeds the Power BI
#   "Methane Intensity %" measure which divides by SUM(gross_gas_mcf)).
# fact_financial_impact: per attributed plume-facility. Convention matches the
#   model's DAX: attributed_kg = emission_rate_kg_s * 3600 * allocation_factor.
#   Fine scenario = ACTIVE_FINE_SCENARIO (default reporting_only -> no WEC fee;
#   discrete NSPS/State violation fines for releases above the OLRE threshold).
#   P10/P50/P90 derived from a lognormal uncertainty band tied to the plume's
#   emission_rate_confidence.
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
    WINDOW_END   = pd.Timestamp(REAL_PLUME_END)
    WRITE_MODE   = "overwrite"

if _start_override:
    WINDOW_START = pd.Timestamp(_start_override)
if _end_override:
    WINDOW_END   = pd.Timestamp(_end_override)

def _add_date_sk(sdf, ts_col):
    return sdf.withColumn("date_sk", F.date_format(F.col(ts_col), "yyyyMMdd").cast("long"))

def write_new_fact(sdf, table):
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

# ── fact_production_daily (Spark-native: 350 facilities x ~2,200 days) ─
fac = spark.table("gold.dim_facility").filter("is_current = true").select(
        "facility_sk", "facility_type", "commission_date")
dts = spark.table("gold.dim_date").select(
        F.col("date_sk"), F.col("date").alias("prod_ts"))

prod = (fac.crossJoin(dts)
          .where((F.col("prod_ts") >= F.lit(str(WINDOW_START))) &
                 (F.col("prod_ts") <= F.lit(str(WINDOW_END)))))

# Deterministic per-(facility,day) pseudo-random multiplier via hashing.
h = F.abs(F.hash(F.concat_ws("|", F.lit(MASTER_SEED), F.col("facility_sk"), F.col("date_sk"))))
u = (h % F.lit(100000)) / F.lit(100000.0)        # ~uniform[0,1)
base = F.when(F.col("facility_type") == "Gas Processing Plant", 6000.0) \
        .when(F.col("facility_type") == "Compression Station", 3500.0) \
        .otherwise(2200.0)
operating = (u > 0.02)                            # ~2% downtime days
prod = (prod
        .withColumn("gross_gas_mcf", F.round(base * (0.6 + 0.8 * u) * F.when(operating, 1.0).otherwise(0.0), 1))
        .withColumn("oil_bbl",  F.round(F.col("gross_gas_mcf") * (0.05 + 0.1 * u), 1))
        .withColumn("water_bbl", F.round(F.col("gross_gas_mcf") * (0.2 + 0.3 * u), 1))
        .withColumn("operating_hours", F.when(operating, F.round(24 * (0.9 + 0.1 * u), 1)).otherwise(F.lit(0.0)))
        .withColumn("downtime_flag", ~operating)
        .select("facility_sk", "date_sk", F.col("prod_ts").alias("prod_date"),
                "gross_gas_mcf", "oil_bbl", "water_bbl", "operating_hours", "downtime_flag"))
write_new_fact(prod, "gold.fact_production_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── fact_financial_impact (per attributed plume-facility) ─────────────
_conf_map = {"high": 0.85, "medium": 0.60, "low": 0.35}

bridge = (spark.table("gold.bridge_plume_facility_attribution")
            .where(F.col("facility_sk").isNotNull() & (F.col("allocation_factor") > 0))
            .select("plume_id","facility_sk","allocation_factor","attribution_confidence"))

plumes = (spark.table("gold.fact_plume_detection")
            .select("plume_id","detect_ts","emission_rate_kg_s",
                    F.when(F.col("emission_rate_confidence") == "high",   0.85)
                     .when(F.col("emission_rate_confidence") == "medium", 0.60)
                     .when(F.col("emission_rate_confidence") == "low",    0.35)
                     .otherwise(F.col("emission_rate_confidence").cast("double"))
                     .alias("emission_rate_confidence"))
            .filter("is_synthetic = false"))

j = bridge.join(plumes, "plume_id", "inner") \
          .where((F.col("detect_ts") >= F.lit(str(WINDOW_START))) &
                 (F.col("detect_ts") <= F.lit(str(WINDOW_END))))
fin = j.toPandas()
print(f"attributed plume-facility rows: {len(fin)}")

reg = spark.table("gold.dim_regulation").select("regulation_id","regulation_sk").toPandas()
reg_sk = dict(zip(reg.regulation_id, reg.regulation_sk))
NSPS_SK = int(reg_sk.get("NSPS_OOOOb", reg_sk.get("TX_RRC", 1)))
RPT_SK  = int(reg_sk.get("TX_RRC", NSPS_SK))

sc = FINE_SCENARIOS[ACTIVE_FINE_SCENARIO]
use_social = bool(sc.get("use_social_cost", False))
fine_lo, fine_hi = sc.get("nsps_state_violation_fine_usd", (5000, 75000))
gwp = GWP_OPTIONS[GWP_DEFAULT]
r = get_rng("financial")

recs = []
for i, row in fin.iterrows():
    alloc_kg   = float(row.emission_rate_kg_s) * 3600.0 * float(row.allocation_factor)
    lost_gas   = alloc_kg / KG_CH4_PER_MCF * GAS_PRICE_USD_PER_MCF
    co2e_t     = alloc_kg / 1000.0 * gwp
    rate_kg_hr = float(row.emission_rate_kg_s) * 3600.0
    is_olre    = rate_kg_hr > 100.0
    nsps_fine  = float(r.uniform(fine_lo, fine_hi)) if (is_olre and r.random() < 0.5) else 0.0
    social     = (co2e_t * SOCIAL_COST_CO2_USD_T) if use_social else 0.0
    wec        = 0.0
    total      = lost_gas + nsps_fine + social + wec
    _c = row.emission_rate_confidence
    if pd.isna(_c) or _c is None:
        conf = 0.5
    elif isinstance(_c, str):
        conf = _conf_map.get(str(_c).strip().lower(), 0.5)
    else:
        conf = float(_c)
    sigma = 0.55 * (1.0 - min(max(conf, 0.0), 0.95))
    recs.append({
        "facility_sk": int(row.facility_sk), "regulation_sk": (NSPS_SK if is_olre else RPT_SK),
        "plume_id": row.plume_id, "detect_ts": pd.Timestamp(row.detect_ts).to_pydatetime(),
        "period_month": pd.Timestamp(row.detect_ts).strftime("%Y-%m"),
        "attributed_emissions_kg": round(alloc_kg, 3),
        "gwp_co2e_tonnes": round(co2e_t, 4),
        "lost_gas_value_usd": round(lost_gas, 2),
        "nsps_violation_fine_usd": round(nsps_fine, 2),
        "social_cost_usd": round(social, 2),
        "wec_charge_usd": round(wec, 2),
        "total_financial_impact_usd_p10": round(total * float(np.exp(-1.2816 * sigma)), 2),
        "total_financial_impact_usd_p50": round(total, 2),
        "total_financial_impact_usd_p90": round(total * float(np.exp( 1.2816 * sigma)), 2),
        "fine_scenario": ACTIVE_FINE_SCENARIO,
    })

fp = pd.DataFrame(recs)
if len(fp) == 0:
    fp = pd.DataFrame([{
        "facility_sk": -1, "regulation_sk": RPT_SK, "plume_id": None,
        "detect_ts": WINDOW_START.to_pydatetime(), "period_month": WINDOW_START.strftime("%Y-%m"),
        "attributed_emissions_kg": 0.0, "gwp_co2e_tonnes": 0.0, "lost_gas_value_usd": 0.0,
        "nsps_violation_fine_usd": 0.0, "social_cost_usd": 0.0, "wec_charge_usd": 0.0,
        "total_financial_impact_usd_p10": 0.0, "total_financial_impact_usd_p50": 0.0,
        "total_financial_impact_usd_p90": 0.0, "fine_scenario": ACTIVE_FINE_SCENARIO,
    }])
sdf = spark.createDataFrame(fp).withColumn("detect_ts", F.col("detect_ts").cast("timestamp"))
sdf = _add_date_sk(sdf, "detect_ts")
write_new_fact(sdf, "gold.fact_financial_impact")
print(f"  P50 total impact: ${fp.total_financial_impact_usd_p50.sum():,.0f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
