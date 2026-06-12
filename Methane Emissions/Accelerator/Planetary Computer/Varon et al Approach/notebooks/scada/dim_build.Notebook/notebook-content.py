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
# Builds all gold dimensions. Static dims are overwrite; facility/equipment/
# sensor/regulation are SCD2 via Delta MERGE. Health/risk scores are NOT here
# (they are volatile measures → live in snapshot facts; see notebook 70).
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

from delta.tables import DeltaTable

def write_overwrite(pdf, table):
    sdf = spark.createDataFrame(pdf)
    sdf.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(table)
    print(f"{table}: {sdf.count()} rows")

def scd2_merge(pdf, table, business_key, attr_cols):
    """Type-2 merge: close changed current rows, insert new versions. row_hash over attrs."""
    pdf = pdf.copy()
    pdf["row_hash"] = pdf[attr_cols].astype(str).agg("|".join, axis=1)\
                         .map(lambda s: hashlib.md5(s.encode()).hexdigest())
    pdf["effective_from"] = pd.Timestamp.utcnow().normalize()
    pdf["effective_to"]   = pd.Timestamp("2999-12-31")
    pdf["is_current"]     = True
    src = spark.createDataFrame(pdf)
    if not spark.catalog.tableExists(table):
        src.write.format("delta").saveAsTable(table)
        print(f"{table}: initial load {src.count()} rows"); return
    tgt = DeltaTable.forName(spark, table)
    # 1) expire current rows whose hash changed
    (tgt.alias("t").merge(src.alias("s"),
        f"t.{business_key} = s.{business_key} AND t.is_current = true")
        .whenMatchedUpdate(condition="t.row_hash <> s.row_hash",
            set={"is_current": F.lit(False), "effective_to": F.current_timestamp()})
        .execute())
    # 2) insert new versions for changed/new keys
    existing = tgt.toDF().filter("is_current = true").select(business_key, "row_hash")
    to_insert = src.join(existing, on=[business_key], how="left_anti")  # new keys
    changed = src.join(existing.withColumnRenamed("row_hash","cur_hash"), business_key, "inner")\
                 .filter("row_hash <> cur_hash").drop("cur_hash")
    to_insert.unionByName(changed).write.format("delta").mode("append").saveAsTable(table)
    print(f"{table}: SCD2 merge complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ── dim_date ──────────────────────────────────────────────────────
dates = pd.date_range(HISTORY_START, REAL_PLUME_END, freq="D")
dim_date = pd.DataFrame({
    "date_sk": dates.strftime("%Y%m%d").astype(int),
    "date": dates,
    "year": dates.year, "quarter": dates.quarter, "month": dates.month,
    "week": dates.isocalendar().week.astype(int), "day_of_week": dates.dayofweek,
    "is_weekend": dates.dayofweek >= 5,
    "season": pd.Categorical(((dates.month % 12)//3).map({0:"Winter",1:"Spring",2:"Summer",3:"Fall"})),
    "fiscal_period": dates.strftime("%Y-%m"),
})
write_overwrite(dim_date, "gold.dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ── dim_operator ──────────────────────────────────────────────────
op_rng = get_rng("operators")
dim_operator = pd.DataFrame({
    "operator_sk": range(1, N_OPERATORS+1),
    "operator_name": [f"Operator {i}" for i in range(1, N_OPERATORS+1)],
    "operator_tier": op_rng.choice(["Major","Independent","Small Cap"], size=N_OPERATORS, p=[.25,.5,.25]),
})
write_overwrite(dim_operator, "gold.dim_operator")

# CELL ── dim_manufacturer / dim_equipment_type ─────────────────────────
dim_mfr = pd.DataFrame({"manufacturer_sk": range(1, len(MANUFACTURERS)+1),
                        "manufacturer": list(MANUFACTURERS),
                        "reliability_index": list(MANUFACTURERS.values())})
write_overwrite(dim_mfr, "gold.dim_manufacturer")

dim_etype = pd.DataFrame([{"equipment_type_sk": i+1, "equipment_type": k,
                           "default_expected_life_years": v["life"],
                           "default_inspection_freq_days": v["insp_days"],
                           "leak_propensity": v["leak_propensity"]}
                          for i,(k,v) in enumerate(EQUIPMENT_TYPES.items())])
write_overwrite(dim_etype, "gold.dim_equipment_type")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ── dim_facility (geographic clustering) ────────────────────────
frng = get_rng("facilities")
rows, used_names = [], set()

for fid in range(1, N_FACILITIES + 1):
    lat, lon, anchor, band = sample_facility_location(frng)
    place = frng.choice(PLACES)
    desc  = frng.choice(sorted(FAC_DESCRIPTORS))
    name  = f"{place} {desc}"
    suffix = 2
    while name in used_names:
        name = f"{place} {desc} {suffix}"; suffix += 1
    used_names.add(name)

    commission = pd.Timestamp(
        HISTORY_START - timedelta(days=int(frng.integers(0, 365 * 15)))
    )
    rows.append({
        "facility_sk":    fid,
        "facility_id":    f"FAC-{fid:04d}",
        "facility_name":  name,
        "operator_sk":    int(frng.integers(1, N_OPERATORS + 1)),
        "facility_type":  frng.choice(FACILITY_TYPES),
        "basin":          "Permian",
        "country":        "USA",
        "state":          "TX",
        "anchor_site":    anchor,
        "region_band":    band,
        "latitude":       lat,
        "longitude":      lon,
        "commission_date": commission,
        "active_flag":    bool(frng.random() > 0.04),
    })

fac_pdf = pd.DataFrame(rows)

# SCD2 tracking columns — included now so schema is ready for incremental runs
fac_pdf["effective_from"] = pd.Timestamp.utcnow().floor("D")
fac_pdf["effective_to"]   = pd.Timestamp("2999-12-31")
fac_pdf["is_current"]     = True

spark.createDataFrame(fac_pdf) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_facility")

print(f"gold.dim_facility: {len(fac_pdf)} rows written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ── dim_equipment (realistic age distribution) ──────────────────
erng = get_rng("equipment")
etype_list = list(EQUIPMENT_TYPES.items())
mfr_list   = list(MANUFACTURERS.items())
eq_rows = []; eq_id = 0

for f in fac_pdf.itertuples():
    n_eq = int(erng.integers(EQUIP_PER_FACILITY[0], EQUIP_PER_FACILITY[1] + 1))
    for _ in range(n_eq):
        eq_id += 1
        etype, ev = etype_list[erng.integers(0, len(etype_list))]
        mfr, _rel = mfr_list[erng.integers(0, len(mfr_list))]

        max_age = max(1, (pd.Timestamp(REAL_PLUME_END) - f.commission_date).days // 365)

        if erng.random() < 0.35 and max_age >= 9:   # legacy cohort — only if facility old enough
            left  = min(8, max_age - 1)
            right = max_age
            mode  = max(left, min(int(max_age * 0.7), right))
            age   = min(max_age, int(erng.triangular(left, mode, right)))
        else:                                         # newer cohort
            right = max(1, int(max_age * 0.6))
            mode  = max(0, min(int(max_age * 0.3), right))
            age   = int(erng.triangular(0, mode, right))

        install = pd.Timestamp(
            REAL_PLUME_END - timedelta(days=age * 365 + int(erng.integers(0, 365)))
        )

        crit = erng.choice(
            ["Low", "Medium", "High", "Critical"],
            p=np.array([0.4, 0.3, 0.2, 0.1]) if ev["crit_bias"] < 0.6
              else np.array([0.2, 0.3, 0.3, 0.2])
        )
        eq_rows.append({
            "equipment_sk":              eq_id,
            "equipment_id":              f"EQ-{eq_id:05d}",
            "facility_sk":               f.facility_sk,
            "facility_id":               f.facility_id,
            "equipment_name":            f"{etype} {eq_id:05d}",
            "equipment_type":            etype,
            "manufacturer":              mfr,
            "serial_number":             f"SN{erng.integers(10**7, 10**8)}",
            "install_date":              install,
            "expected_life_years":       ev["life"],
            "criticality":               crit,
            "sensor_coverage":           bool(erng.random() < 0.6),
            "inspection_frequency_days": ev["insp_days"],
            "facility_lat":              f.latitude,
            "facility_lon":              f.longitude,
            "status":                    erng.choice(
                                             ["Operating", "Standby", "Down"],
                                             p=[0.9, 0.07, 0.03]
                                         ),
        })

eq_pdf = pd.DataFrame(eq_rows)

# ── gold.dim_equipment (without geo columns) ──────────────────────────
dim_eq = eq_pdf.drop(columns=["facility_lat", "facility_lon"]).copy()
dim_eq["effective_from"] = pd.Timestamp.utcnow().floor("D")
dim_eq["effective_to"]   = pd.Timestamp("2999-12-31")
dim_eq["is_current"]     = True

spark.createDataFrame(dim_eq) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_equipment")

# ── silver.equipment_geo (carries lat/lon for episode + attribution) ──
spark.createDataFrame(eq_pdf) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.equipment_geo")

print(f"gold.dim_equipment:    {len(dim_eq)} rows written")
print(f"silver.equipment_geo:  {len(eq_pdf)} rows written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ── dim_sensor (4 per facility, on highest-criticality assets) ─────
srng = get_rng("sensors")
crit_rank = {"Critical": 3, "High": 2, "Medium": 1, "Low": 0}
sensor_rows = []; sid = 0

for f in fac_pdf.itertuples():
    cand = eq_pdf[eq_pdf.facility_sk == f.facility_sk].copy()
    cand["rank"] = cand.criticality.map(crit_rank)
    chosen = cand.sort_values("rank", ascending=False).head(SENSORS_PER_FACILITY)
    for c in chosen.itertuples():
        sid += 1
        sensor_rows.append({
            "sensor_sk":              sid,
            "sensor_id":              f"SNS-{sid:05d}",
            "equipment_sk":           c.equipment_sk,
            "facility_sk":            f.facility_sk,
            "sensor_type":            srng.choice(["Point", "OGI", "CMS"], p=[0.5, 0.2, 0.3]),
            "detection_limit_kg_hr":  float(srng.uniform(0.5, 5.0)),
            "reading_interval_hours": SENSOR_INTERVAL_HOURS,
            "install_date":           c.install_date,
            "status":                 "Active",
        })

sensor_pdf = pd.DataFrame(sensor_rows)
sensor_pdf["effective_from"] = pd.Timestamp.utcnow().floor("D")
sensor_pdf["effective_to"]   = pd.Timestamp("2999-12-31")
sensor_pdf["is_current"]     = True

spark.createDataFrame(sensor_pdf) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_sensor")

print(f"gold.dim_sensor: {len(sensor_pdf)} rows written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ── dim_regulation (scenario rows) ───────────────────────────────
reg_rows = []
reg_rows.append({
    "regulation_id":    "WEC",
    "regulation_type":  "Waste Emissions Charge",
    "jurisdiction":     "Federal",
    "threshold_value":  25000,
    "threshold_unit":   "tCO2e_yr",
    "fine_model":       "per_tonne_above_threshold",
    "scenario_tag":     "wec_hypothetical",
    "effective_from":   pd.Timestamp("2024-01-01"),
    "note":             "40 CFR Part 99; rule repealed CRA Mar 2025; not collected; statutory charge to 2034",
})
reg_rows.append({
    "regulation_id":    "NSPS_OOOOb",
    "regulation_type":  "NSPS OOOOb/OOOOc",
    "jurisdiction":     "Federal",
    "threshold_value":  100,
    "threshold_unit":   "kg_hr_OLRE",
    "fine_model":       "discrete_violation",
    "scenario_tag":     "reporting_only",
    "effective_from":   pd.Timestamp("2024-03-01"),
    "note":             "Other Large Release Events >100 kg/hr still reportable",
})
reg_rows.append({
    "regulation_id":    "TX_RRC",
    "regulation_type":  "TX RRC venting/flaring",
    "jurisdiction":     "State",
    "threshold_value":  0,
    "threshold_unit":   "event",
    "fine_model":       "discrete_violation",
    "scenario_tag":     "reporting_only",
    "effective_from":   pd.Timestamp("2021-01-01"),
    "note":             "State rule",
})

reg_pdf = pd.DataFrame(reg_rows)
reg_pdf["regulation_sk"] = range(1, len(reg_pdf) + 1)
reg_pdf["effective_to"]  = pd.Timestamp("2999-12-31")
reg_pdf["is_current"]    = True

spark.createDataFrame(reg_pdf) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_regulation")

print(f"gold.dim_regulation: {len(reg_pdf)} rows written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ── dim_team / dim_contractor ─────────────────────────────────────
trng = get_rng("teams")

teams = pd.DataFrame({
    "team_sk":       range(1, 9),
    "team_name":     [f"Field Crew {c}" for c in "ABCDEFGH"],
    "discipline":    trng.choice(["Mechanical", "Instrumentation", "LDAR", "Operations"], 8),
    "shift":         trng.choice(["Day", "Night", "Swing"], 8),
    "base_location": trng.choice(PLACES, 8),
})

spark.createDataFrame(teams) \
    .write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_team")

print(f"gold.dim_team:       {len(teams)} rows written")

contractors = pd.DataFrame({
    "contractor_sk":   range(1, 7),
    "contractor_name": [f"Contractor {i}" for i in range(1, 7)],
    "specialty":       trng.choice(["Compression", "Pipeline", "Tanks", "General"], 6),
    "quality_rating":  np.round(trng.uniform(0.6, 0.98, 6), 2),
})

spark.createDataFrame(contractors) \
    .write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_contractor")

print(f"gold.dim_contractor: {len(contractors)} rows written")
print("dimensions complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
