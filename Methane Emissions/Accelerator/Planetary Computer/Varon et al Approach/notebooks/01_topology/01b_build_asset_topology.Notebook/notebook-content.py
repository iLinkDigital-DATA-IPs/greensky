# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5d5c8002-789e-4319-81d1-a60f08a77996",
# META       "default_lakehouse_name": "greensky_v2_lakehouse",
# META       "default_lakehouse_workspace_id": "640876ea-6158-4ffd-8598-5eb210e088a0",
# META       "known_lakehouses": [
# META         {
# META           "id": "5d5c8002-789e-4319-81d1-a60f08a77996"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "cf70e84c-e5f3-9589-4218-88cc1ae7b47d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 01b — Build Asset Topology
#
# Writes **`dim_equipment`** and **`dim_sensor`**, both hanging off `dim_facility` from
# `01a`. Counts follow `N_FACILITIES` x `EQUIP_PER_FACILITY` / `SENSORS_PER_FACILITY`.
#
# ### No coordinates on assets
# V1 wrote equipment geography twice — `silver.equipment_geo` carried `facility_lat`/`_lon`
# copied off the facility, and `build_attribution` later rewrote both the copy and the
# dimension from a different seed. That is how the estate ended up with two coordinate pairs
# that disagreed.
#
# Here `dim_equipment` and `dim_sensor` carry **`facility_id` and nothing geographic**.
# Anything needing an asset's position joins `dim_facility`. There is exactly one place a
# coordinate can be edited, so the pairs cannot drift — and there is no `equipment_geo`
# table to fall out of sync.
#
# ```sql
# SELECT e.equipment_id, f.facility_lat, f.facility_lon
# FROM dim_equipment e JOIN dim_facility f USING (facility_id)
# ```
#
# ### Writes
# `dim_equipment`, `dim_sensor`, both `mode("overwrite")`.

# CELL ********************

%run 01_topology_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fac_pdf = (spark.table("dim_facility")
                .filter("is_current = true")
                .toPandas()
                .sort_values("facility_sk")
                .reset_index(drop=True))

assert len(fac_pdf) == N_FACILITIES, (
    f"dim_facility holds {len(fac_pdf)} current rows, expected {N_FACILITIES} -- "
    "rerun 01a_build_facility_topology first"
)
assert (fac_pdf["topology_seed"] == TOPOLOGY_SEED).all(), (
    "dim_facility was built with a different TOPOLOGY_SEED than this notebook is using; "
    "rerun 01a so the facility and asset layers come from one seed"
)

print(f"loaded {len(fac_pdf)} facilities from dim_facility (seed {TOPOLOGY_SEED})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Generate equipment
#
# Age is drawn against each facility's own `commission_date`, so no asset can predate the
# site it sits on. Two cohorts: a legacy tail (35 %, only where the facility is old enough)
# and a newer population — this is V1's distribution, which was sound.

# CELL ********************

erng = get_rng("equipment")
etype_list = list(EQUIPMENT_TYPES.items())
mfr_list = list(MANUFACTURERS)

eq_rows = []
eq_id = 0

for f in fac_pdf.itertuples():
    n_eq = int(erng.integers(EQUIP_PER_FACILITY[0], EQUIP_PER_FACILITY[1] + 1))
    for _ in range(n_eq):
        eq_id += 1
        etype, ev = etype_list[erng.integers(0, len(etype_list))]
        mfr = mfr_list[erng.integers(0, len(mfr_list))]

        max_age = max(1, (pd.Timestamp(TOPOLOGY_AS_OF) - f.commission_date).days // 365)

        if erng.random() < 0.35 and max_age >= 9:        # legacy cohort
            left = min(8, max_age - 1)
            right = max_age
            mode = max(left, min(int(max_age * 0.7), right))
            age = min(max_age, int(erng.triangular(left, mode, right)))
        else:                                             # newer cohort
            right = max(1, int(max_age * 0.6))
            mode = max(0, min(int(max_age * 0.3), right))
            age = int(erng.triangular(0, mode, right))

        install = pd.Timestamp(
            TOPOLOGY_AS_OF - timedelta(days=age * 365 + int(erng.integers(0, 365)))
        )
        # An asset cannot predate its facility.
        install = max(install, f.commission_date)

        crit = str(erng.choice(
            ["Low", "Medium", "High", "Critical"],
            p=[0.4, 0.3, 0.2, 0.1] if ev["crit_bias"] < 0.6 else [0.2, 0.3, 0.3, 0.2],
        ))

        eq_rows.append({
            "equipment_sk":              eq_id,
            "equipment_id":              f"EQ-{eq_id:05d}",
            "facility_sk":               f.facility_sk,
            "facility_id":               f.facility_id,
            "equipment_name":            f"{etype} {eq_id:05d}",
            "equipment_type":            etype,
            "manufacturer":              mfr,
            "reliability_index":         MANUFACTURERS[mfr],
            "serial_number":             f"SN{erng.integers(10**7, 10**8)}",
            "install_date":              install,
            "expected_life_years":       ev["life"],
            "inspection_frequency_days": ev["insp_days"],
            "leak_propensity":           ev["leak_propensity"],
            "criticality":               crit,
            "sensor_coverage":           bool(erng.random() < 0.6),
            "status":                    str(erng.choice(
                                             ["Operating", "Standby", "Down"],
                                             p=[0.90, 0.07, 0.03])),
        })

eq_pdf = pd.DataFrame(eq_rows)
eq_pdf["effective_from"] = pd.Timestamp(TOPOLOGY_AS_OF)
eq_pdf["effective_to"]   = pd.Timestamp("2999-12-31")
eq_pdf["is_current"]     = True
eq_pdf["topology_seed"]  = TOPOLOGY_SEED

print(f"{len(eq_pdf):,} assets across {eq_pdf['facility_id'].nunique()} facilities")
print(f"assets per facility: min {eq_pdf.groupby('facility_id').size().min()},"
      f" median {int(eq_pdf.groupby('facility_id').size().median())},"
      f" max {eq_pdf.groupby('facility_id').size().max()}")
print()
print("equipment_type distribution:")
for t, n in eq_pdf["equipment_type"].value_counts().items():
    print(f"  {t:<20}{n:>7,}  ({n/len(eq_pdf):5.1%})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Generate sensors
#
# `SENSORS_PER_FACILITY` instruments per site, placed on the highest-criticality assets —
# V1's rule, and a sensible one: you monitor what matters.

# CELL ********************

srng = get_rng("sensors")
crit_rank = {"Critical": 3, "High": 2, "Medium": 1, "Low": 0}
sensor_types = list(SENSOR_TYPES)
sensor_probs = [SENSOR_TYPES[t] for t in sensor_types]

sensor_rows = []
sid = 0

for f in fac_pdf.itertuples():
    cand = eq_pdf[eq_pdf["facility_sk"] == f.facility_sk].copy()
    if cand.empty:
        continue
    cand["rank"] = cand["criticality"].map(crit_rank)
    # Tie-break on equipment_sk so the chosen set does not depend on row order.
    chosen = cand.sort_values(["rank", "equipment_sk"], ascending=[False, True]) \
                 .head(SENSORS_PER_FACILITY)

    for c in chosen.itertuples():
        sid += 1
        sensor_rows.append({
            "sensor_sk":              sid,
            "sensor_id":              f"SNS-{sid:05d}",
            "equipment_sk":           c.equipment_sk,
            "equipment_id":           c.equipment_id,
            "facility_sk":            f.facility_sk,
            "facility_id":            f.facility_id,
            "sensor_type":            str(srng.choice(sensor_types, p=sensor_probs)),
            "detection_limit_kg_hr":  float(srng.uniform(0.5, 5.0)),
            "reading_interval_hours": SENSOR_INTERVAL_HOURS,
            "install_date":           c.install_date,
            "status":                 "Active",
        })

sen_pdf = pd.DataFrame(sensor_rows)
sen_pdf["effective_from"] = pd.Timestamp(TOPOLOGY_AS_OF)
sen_pdf["effective_to"]   = pd.Timestamp("2999-12-31")
sen_pdf["is_current"]     = True
sen_pdf["topology_seed"]  = TOPOLOGY_SEED

print(f"{len(sen_pdf):,} sensors across {sen_pdf['facility_id'].nunique()} facilities")
print()
print("sensor_type distribution:")
for t, n in sen_pdf["sensor_type"].value_counts().items():
    print(f"  {t:<10}{n:>6,}  ({n/len(sen_pdf):5.1%})")
print()
print("criticality of instrumented assets:")
instrumented = eq_pdf[eq_pdf["equipment_id"].isin(sen_pdf["equipment_id"])]
for c, n in instrumented["criticality"].value_counts().items():
    print(f"  {c:<10}{n:>6,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Referential and schema integrity

# CELL ********************

fac_ids = set(fac_pdf["facility_id"])

# --- keys -------------------------------------------------------------------------------
assert eq_pdf["equipment_id"].is_unique, "equipment_id is not unique"
assert eq_pdf["equipment_sk"].is_unique, "equipment_sk is not unique"
assert sen_pdf["sensor_id"].is_unique, "sensor_id is not unique"
assert sen_pdf["sensor_sk"].is_unique, "sensor_sk is not unique"

# --- referential integrity ---------------------------------------------------------------
orphan_eq = set(eq_pdf["facility_id"]) - fac_ids
assert not orphan_eq, f"equipment referencing unknown facilities: {sorted(orphan_eq)[:5]}"

orphan_sen_f = set(sen_pdf["facility_id"]) - fac_ids
assert not orphan_sen_f, f"sensors referencing unknown facilities: {sorted(orphan_sen_f)[:5]}"

orphan_sen_e = set(sen_pdf["equipment_id"]) - set(eq_pdf["equipment_id"])
assert not orphan_sen_e, f"sensors referencing unknown equipment: {sorted(orphan_sen_e)[:5]}"

# Every facility carries at least one asset -- an empty site would silently drop out of
# any asset-level rollup.
uncovered = fac_ids - set(eq_pdf["facility_id"])
assert not uncovered, f"{len(uncovered)} facilities have no equipment: {sorted(uncovered)[:5]}"

# --- no geography on assets --------------------------------------------------------------
# The structural fix for the V1 two-coordinate-pair defect. If a lat/lon column ever appears
# on an asset table it is a copy of dim_facility, and copies drift.
geo_like = {"facility_lat", "facility_lon", "equipment_lat", "equipment_lon",
            "latitude", "longitude", "lat", "lon", "sensor_lat", "sensor_lon"}
for label, frame in (("dim_equipment", eq_pdf), ("dim_sensor", sen_pdf)):
    found = geo_like & set(frame.columns)
    assert not found, (
        f"{label} carries coordinate column(s) {sorted(found)}. Asset tables hold no "
        "geography -- join dim_facility on facility_id. V1's silver.equipment_geo copied "
        "facility coordinates onto equipment, and the copy and the original then diverged."
    )

# --- temporal sanity ---------------------------------------------------------------------
eq_dates = eq_pdf.merge(
    fac_pdf[["facility_id", "commission_date"]], on="facility_id", how="left"
)
early = eq_dates[eq_dates["install_date"] < eq_dates["commission_date"]]
assert early.empty, (
    f"{len(early)} asset(s) installed before their facility was commissioned:\n"
    f"{early[['equipment_id', 'facility_id', 'install_date', 'commission_date']].head().to_string(index=False)}"
)

future = eq_pdf[eq_pdf["install_date"] > pd.Timestamp(TOPOLOGY_AS_OF)]
assert future.empty, f"{len(future)} asset(s) installed after the as-of date"

print("OK  keys unique")
print("OK  every asset and sensor resolves to a known facility")
print("OK  every facility carries at least one asset")
print("OK  no coordinate columns on dim_equipment or dim_sensor")
print("OK  no asset predates its facility or postdates the as-of date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write `dim_equipment` and `dim_sensor`

# CELL ********************

dim_equipment = spark.createDataFrame(eq_pdf)
(dim_equipment.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("dim_equipment"))
print(f"dim_equipment: {dim_equipment.count():,} rows x {len(eq_pdf.columns)} columns")

dim_sensor = spark.createDataFrame(sen_pdf)
(dim_sensor.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("dim_sensor"))
print(f"dim_sensor:    {dim_sensor.count():,} rows x {len(sen_pdf.columns)} columns")

display(dim_equipment.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Verify the join path
#
# Asset geography comes from `dim_facility` and only from there. This is the query any
# downstream notebook should use.

# CELL ********************

geo = spark.sql("""
    SELECT e.equipment_id, e.equipment_type, e.facility_id,
           f.facility_name, f.facility_lat, f.facility_lon, f.region_band
    FROM dim_equipment e
    JOIN dim_facility f ON e.facility_id = f.facility_id AND f.is_current = true
""")

n_eq = spark.table("dim_equipment").count()
n_joined = geo.count()
assert n_joined == n_eq, (
    f"join returned {n_joined:,} rows for {n_eq:,} assets -- the facility join is not 1:1"
)
print(f"OK  all {n_joined:,} assets resolve geography through dim_facility")

display(geo.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary

# CELL ********************

print("=" * 72)
print("ASSET TOPOLOGY REBUILT")
print("=" * 72)
print(f"  seed             {TOPOLOGY_SEED}")
print(f"  facilities       {len(fac_pdf):,}   (from 01a)")
print(f"  assets           {len(eq_pdf):,}")
print(f"  sensors          {len(sen_pdf):,}")
print(f"  asset geography  via dim_facility join on facility_id -- not stored on assets")
print()
print("  tables written   dim_equipment, dim_sensor")
print()
print("  V1 comparison    Operations_LH held 350 facilities / 10,575 assets / 1,400")
print("                   sensors. This estate is deliberately smaller: N_FACILITIES is")
print("                   sized against the detection rate so the Facility Operations")
print("                   dashboard page is not empty for most sites -- see the coverage")
print("                   cell at the end of 01a. V1's silver.equipment_geo has no")
print("                   counterpart here by design; asset geography comes from the join.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
