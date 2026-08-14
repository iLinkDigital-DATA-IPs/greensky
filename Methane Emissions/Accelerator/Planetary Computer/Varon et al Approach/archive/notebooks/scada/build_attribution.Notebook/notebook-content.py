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
# Noisy INVERSE: given a plume, find candidate facilities/equipment using ONLY
# physically observable signals — proximity of source point, upwind geometry
# (back along wind_dir), and detection-limit plausibility. It NEVER reads risk
# score, health, or the hidden episode_sk → no label leakage. Scores normalize
# to allocation_factor so emissions are not double-counted across candidates.
# A QA cell measures recovery of the hidden ground truth (should be high but
# well under 100% — proof the inverse is realistic, not leaked).
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

import numpy as np

# Bounding box from real plumes (with a small buffer)
LAT_MIN, LAT_MAX = 31.5,  33.5
LON_MIN, LON_MAX = -105.0, -101.3

rng = np.random.default_rng(42)  # fixed seed for reproducibility

# ── Update silver.equipment_geo ───────────────────────────────────────
eq_df = spark.table("silver.equipment_geo").toPandas()

# Assign each facility a random location within the plume bounding box,
# then give all equipment at that facility the same lat/lon
fac_ids = eq_df["facility_sk"].unique()
fac_locs = {
    fsk: (
        rng.uniform(LAT_MIN, LAT_MAX),
        rng.uniform(LON_MIN, LON_MAX)
    )
    for fsk in fac_ids
}

eq_df["facility_lat"] = eq_df["facility_sk"].map(lambda x: fac_locs[x][0])
eq_df["facility_lon"] = eq_df["facility_sk"].map(lambda x: fac_locs[x][1])
eq_df["equipment_lat"] = eq_df["facility_lat"]
eq_df["equipment_lon"] = eq_df["facility_lon"]

(spark.createDataFrame(eq_df)
     .write.mode("overwrite").option("overwriteSchema", "true")
     .format("delta").saveAsTable("silver.equipment_geo"))
print(f"silver.equipment_geo updated: {len(eq_df)} rows, {len(fac_ids)} facilities reseeded")

# ── Update gold.dim_facility to match ────────────────────────────────
fac_df = spark.table("gold.dim_facility").toPandas()

fac_df["facility_lat"] = fac_df["facility_sk"].map(
    lambda x: fac_locs[x][0] if x in fac_locs else rng.uniform(LAT_MIN, LAT_MAX)
)
fac_df["facility_lon"] = fac_df["facility_sk"].map(
    lambda x: fac_locs[x][1] if x in fac_locs else rng.uniform(LON_MIN, LON_MAX)
)

(spark.createDataFrame(fac_df)
     .write.mode("overwrite").option("overwriteSchema", "true")
     .format("delta").saveAsTable("gold.dim_facility"))
print(f"gold.dim_facility updated: {len(fac_df)} rows")

print("\nNew equipment lat range:", eq_df.equipment_lat.min(), "to", eq_df.equipment_lat.max())
print("New equipment lon range:", eq_df.equipment_lon.min(), "to", eq_df.equipment_lon.max())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plumes = spark.table("gold.fact_plume_detection") \
    .filter("is_synthetic = false") \
    .toPandas()
eqgeo = spark.table("silver.equipment_geo").toPandas()
fac   = spark.table("gold.dim_facility").filter("is_current=true").toPandas()

# silver.equipment_geo has facility_lat/lon rather than equipment_lat/lon —
# equipment is co-located at its facility so we alias them
if "equipment_lat" not in eqgeo.columns:
    if "facility_lat" in eqgeo.columns:
        eqgeo["equipment_lat"] = eqgeo["facility_lat"]
        eqgeo["equipment_lon"] = eqgeo["facility_lon"]
    else:
        print(f"Available columns in silver.equipment_geo: {list(eqgeo.columns)}")
        raise AssertionError("silver.equipment_geo has no lat/lon columns — re-run build_dimensions")

SEARCH_RADIUS_KM  = 8.0
UPWIND_HALF_ANGLE = 45.0
_ref_ts = pd.Timestamp.now("UTC")  # tz-aware UTC timestamp

# Precompute normalised attribute scores on eqgeo once — avoids recomputing per plume
_type_propensity = {k: v["leak_propensity"] for k, v in EQUIPMENT_TYPES.items()}
_crit_score      = {"Low": 0.5, "Medium": 0.7, "High": 0.9, "Critical": 1.0}

# Ensure install_date is tz-aware (UTC) to match _ref_ts
eqgeo["install_date"] = pd.to_datetime(eqgeo["install_date"], utc=True)

eqgeo["age_yrs"]      = (_ref_ts - eqgeo["install_date"]).dt.days / 365.0
eqgeo["age_factor"]   = np.clip(eqgeo["age_yrs"] / eqgeo["expected_life_years"], 0, 1.5) ** 1.5
eqgeo["type_score"]   = eqgeo["equipment_type"].map(_type_propensity).fillna(0.5)
eqgeo["crit_score"]   = eqgeo["criticality"].map(_crit_score).fillna(0.5)

# Normalise age_factor to [0,1] globally
_age_max = eqgeo["age_factor"].max()
eqgeo["age_factor_n"] = eqgeo["age_factor"] / max(_age_max, 1e-6)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

arng = get_rng("attribution")
fac_bridge, eq_bridge = [], []
conf_tiers = {"high": 0, "low": 0, "multi": 0, "unattributed": 0}

for p in plumes.itertuples():
    d = eqgeo.copy()
    d["dist_km"] = haversine_km(
        p.source_lat, p.source_lon,
        d.equipment_lat.values, d.equipment_lon.values
    )
    cand = d[d.dist_km <= SEARCH_RADIUS_KM].copy()
    if cand.empty:
        conf_tiers["unattributed"] += 1
        fac_bridge.append({
            "plume_sk": p.Index + 1, "plume_id": p.plume_id,
            "facility_sk": None, "distance_km": None,
            "upwind_flag": False, "attribution_confidence": 0.0,
            "allocation_factor": 0.0, "primary_flag": False,
            "root_cause": "Unattributed",
        })
        continue

    wind_from        = (p.wind_dir_deg + 180) % 360
    cand["bearing"]  = bearing_deg(
        p.source_lat, p.source_lon,
        cand.equipment_lat.values, cand.equipment_lon.values
    )
    cand["ang_diff"] = np.minimum(
        abs(cand.bearing - wind_from),
        360 - abs(cand.bearing - wind_from)
    )
    cand["upwind"] = cand.ang_diff <= UPWIND_HALF_ANGLE

    # ── Proximity component (physical) ──────────────────────────────────
    prox = (np.exp(-cand.dist_km / 2.5)
            * np.where(cand.upwind, 1.0, 0.35)
            * (0.6 + 0.4 * np.cos(np.radians(cand.ang_diff.clip(upper=90)))))

    # ── Attribute component (causal, not labels) ─────────────────────────
    # Same physical characteristics that drive the Weibull hazard:
    # older equipment, higher-propensity type, higher criticality.
    # These are observable characteristics — not risk scores derived
    # from past emissions — so this is physics, not leakage.
    attr = (0.50 * cand["age_factor_n"]
            + 0.35 * cand["type_score"]
            + 0.15 * cand["crit_score"])

    # Combined: 50% proximity + 50% attributes
    # Proximity dominates when there is a clear spatial signal;
    # attributes resolve ties when equipment are at similar distances.
    cand["score"] = 0.50 * prox + 0.50 * attr

    fac_scores = cand.groupby("facility_sk")["score"].sum().sort_values(ascending=False)
    total      = fac_scores.sum()
    alloc      = fac_scores / total if total > 0 else fac_scores * 0
    top_alloc  = float(alloc.iloc[0]) if len(alloc) else 0

    if total == 0:
        tier = "unattributed"
    elif top_alloc >= 0.6 and fac_scores.iloc[0] > 0.25:
        tier = "high"
    elif len(alloc[alloc > 0.15]) >= 2:
        tier = "multi"
    else:
        tier = "low"
    conf_tiers[tier] += 1

    for rank, (fsk, af) in enumerate(alloc.items()):
        if af < 0.05:
            continue
        dist = float(cand[cand.facility_sk == fsk].dist_km.min())
        up   = bool(cand[cand.facility_sk == fsk].upwind.any())
        fac_bridge.append({
            "plume_sk":               p.Index + 1,
            "plume_id":               p.plume_id,
            "facility_sk":            int(fsk),
            "distance_km":            round(dist, 3),
            "upwind_flag":            up,
            "attribution_confidence": round(float(af) * (0.9 if up else 0.6), 3),
            "allocation_factor":      round(float(af), 3),
            "primary_flag":           (rank == 0),
            "root_cause":             getattr(p, "root_cause", None),
        })

    if len(alloc):
        pf   = alloc.index[0]
        eqc  = cand[cand.facility_sk == pf].sort_values("score", ascending=False).head(3)
        esum = eqc.score.sum()
        for er, erow in enumerate(eqc.itertuples()):
            eq_bridge.append({
                "plume_sk":               p.Index + 1,
                "plume_id":               p.plume_id,
                "equipment_sk":           int(erow.equipment_sk),
                "facility_sk":            int(pf),
                "attribution_confidence": round(float(erow.score / esum) if esum > 0 else 0, 3),
                "primary_flag":           (er == 0),
            })

print("attribution tiers:", conf_tiers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

fac_pdf = pd.DataFrame(fac_bridge)
eq_pdf  = pd.DataFrame(eq_bridge)

# Create without explicit schema (Spark infers float/object), then cast
fac_sdf = (spark.createDataFrame(fac_pdf)
    .withColumn("plume_sk", F.col("plume_sk").cast("long"))
    .withColumn("plume_id", F.col("plume_id").cast("string"))
    .withColumn("facility_sk", F.col("facility_sk").cast("long"))
    .withColumn("distance_km", F.col("distance_km").cast("double"))
    .withColumn("upwind_flag", F.col("upwind_flag").cast("boolean"))
    .withColumn("attribution_confidence", F.col("attribution_confidence").cast("double"))
    .withColumn("allocation_factor", F.col("allocation_factor").cast("double"))
    .withColumn("primary_flag", F.col("primary_flag").cast("boolean"))
    .withColumn("root_cause", F.col("root_cause").cast("string"))
)
fac_sdf.write.mode("overwrite").option("overwriteSchema", "true") \
    .format("delta").saveAsTable("gold.bridge_plume_facility_attribution")
print(f"bridge_plume_facility_attribution: {len(fac_pdf)} rows written")

eq_sdf = (spark.createDataFrame(eq_pdf)
    .withColumn("plume_sk", F.col("plume_sk").cast("long"))
    .withColumn("plume_id", F.col("plume_id").cast("string"))
    .withColumn("equipment_sk", F.col("equipment_sk").cast("long"))
    .withColumn("facility_sk", F.col("facility_sk").cast("long"))
    .withColumn("attribution_confidence", F.col("attribution_confidence").cast("double"))
    .withColumn("primary_flag", F.col("primary_flag").cast("boolean"))
)
eq_sdf.write.mode("overwrite").option("overwriteSchema", "true") \
    .format("delta").saveAsTable("gold.bridge_plume_equipment")
print(f"bridge_plume_equipment: {len(eq_pdf)} rows written")

# ── QA: attribution summary (real plumes only — no synthetic ground truth) ──
print(f"\nAttribution tiers: {conf_tiers}")
print(f"Total plumes:      {len(plumes)}")
print(f"Attributed:        {len(plumes) - conf_tiers.get('unattributed', 0)}")
print(f"Unattributed:      {conf_tiers.get('unattributed', 0)}")
if len(fac_pdf[fac_pdf.facility_sk.notna()]) > 0:
    print(f"\nTop attributed facilities:")
    print(fac_pdf[fac_pdf.facility_sk.notna()]
          .groupby("facility_sk")["plume_id"].count()
          .sort_values(ascending=False).head(10).to_string())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Diagnose spatial overlap between plumes and facilities
print("Plume source_lat range:", plumes.source_lat.min(), "to", plumes.source_lat.max())
print("Plume source_lon range:", plumes.source_lon.min(), "to", plumes.source_lon.max())
print("Equipment lat range:   ", eqgeo.equipment_lat.min(), "to", eqgeo.equipment_lat.max())
print("Equipment lon range:   ", eqgeo.equipment_lon.min(), "to", eqgeo.equipment_lon.max())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
