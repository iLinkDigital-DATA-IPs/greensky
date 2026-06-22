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
# GROUND TRUTH (hidden). For each equipment, a Weibull hazard driven by the
# shared condition index produces leak-onset events over the 5-yr window.
# Episode rates are log-normal so ~5% of sources produce ~50%+ of total mass
# (super-emitters). Plumes and sensor telemetry are NOISY OBSERVATIONS of these
# episodes (notebooks 21/22, 60). Attribution (40) is a noisy INVERSE that never
# sees this table — that is what prevents label leakage.
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

from pyspark.sql import Window
eq = spark.table("silver.equipment_geo").toPandas()
etype_params = {k: v for k, v in EQUIPMENT_TYPES.items()}
mfr_rel = dict(MANUFACTURERS)
total_days = (REAL_PLUME_END - HISTORY_START).days

# Add per-equipment coordinate jitter if not already present.
# Each asset gets a unique position within ~145m of its facility centre
# so attribution can distinguish equipment within the same facility.
if "equipment_lat" not in eq.columns or "equipment_lon" not in eq.columns:
    _coord_rng = get_rng("equipment_coords")
    eq["equipment_lat"] = eq["facility_lat"] + _coord_rng.normal(0, 0.0013, len(eq))
    eq["equipment_lon"] = eq["facility_lon"] + _coord_rng.normal(0, 0.0013, len(eq))
    # Persist so attribution notebook reads the same coordinates
    (spark.createDataFrame(eq)
          .write.mode("overwrite")
          .option("overwriteSchema", "true")
          .format("delta")
          .saveAsTable("silver.equipment_geo"))
    print(f"equipment_lat/lon added and persisted ({len(eq)} rows)")
else:
    print("equipment_lat/lon already present")

def season_mult(d: date):
    m = d.month
    winter      = 1.25 if m in (12, 1, 2) else 1.0
    summer_tank = 1.10 if m in (6, 7, 8)  else 1.0
    return winter * summer_tank

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

episodes = []; ep_id = 0
_history_start_ts = pd.Timestamp(HISTORY_START)
_plume_end_ts     = pd.Timestamp(REAL_PLUME_END)

for e in eq.itertuples():
    rng = get_rng("episode", e.equipment_id)

    age = max(0.1, (_plume_end_ts - pd.to_datetime(e.install_date)).days / 365.0)
    p   = etype_params[e.equipment_type]
    rel = mfr_rel[e.manufacturer]

    k   = 1.8
    lam = e.expected_life_years * 0.9
    base_hazard = (k / lam) * (age / lam) ** (k - 1)
    crit_w = {"Low": 0.8, "Medium": 1.0, "High": 1.2, "Critical": 1.4}[e.criticality]

    exp_events = base_hazard * total_days / 365.0 * p["leak_propensity"] * rel * crit_w * 0.9
    n_events   = rng.poisson(max(0.05, exp_events))

    for _ in range(int(n_events)):
        onset = _history_start_ts + timedelta(
            days=int(rng.integers(0, total_days)),
            hours=int(rng.integers(0, 24)),
        )
        intermittent = rng.random() < 0.35
        dur_h = float(rng.gamma(2.0, 6.0)) if intermittent else float(rng.gamma(2.5, 48.0))

        mu, sig = CALIBRATION["lognorm_mu"], CALIBRATION["lognorm_sigma"]
        rate    = float(rng.lognormal(mu + 0.15 * crit_w, sig))
        rate   *= season_mult(onset.date())

        if rng.random() < 0.015:
            rate *= float(rng.uniform(8, 25))

        root = rng.choice(
            ["Seal failure", "Valve leak", "Tank flashing", "Unlit flare",
             "Corrosion", "Pneumatic device", "Compressor blowby", "Unknown"],
            p=[.18, .16, .14, .08, .12, .12, .12, .08],
        )

        ep_id += 1
        mass   = rate * dur_h * 3600.0
        end_ts = onset + timedelta(hours=dur_h)

        episodes.append({
            "episode_sk":      ep_id,
            "equipment_sk":    e.equipment_sk,
            "facility_sk":     e.facility_sk,
            "equipment_id":    e.equipment_id,
            "facility_id":     e.facility_id,
            # ── KEY FIX: use equipment-level coords, not facility-level ──
            # Tight noise (±55m) = instrument localisation uncertainty only.
            # Previously used facility_lat ±220m which made all assets
            # at a facility indistinguishable → 2.7% attribution accuracy.
            "source_lat":      e.equipment_lat + float(rng.normal(0, 0.0005)),
            "source_lon":      e.equipment_lon + float(rng.normal(0, 0.0005)),
            "start_ts":        onset,
            "end_ts":          end_ts,
            "duration_hours":  round(dur_h, 2),
            "peak_rate_kg_s":  round(rate, 5),
            "mean_rate_kg_s":  round(rate * 0.7, 5),
            "total_mass_kg":   round(mass * 0.7, 3),
            "root_cause":      root,
            "is_intermittent": bool(intermittent),
        })

ep_pdf = pd.DataFrame(episodes)
print(f"episodes: {len(ep_pdf)}")
if len(ep_pdf):
    s    = ep_pdf.total_mass_kg.sort_values(ascending=False)
    top5 = s.head(max(1, int(len(s) * 0.05))).sum() / s.sum()
    print(f"  top 5% of episodes = {top5:.1%} of total mass (target ~50%+)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema_ts = ["start_ts","end_ts"]
sdf = spark.createDataFrame(ep_pdf)
for c in schema_ts:
    sdf = sdf.withColumn(c, F.col(c).cast("timestamp"))
sdf.write.mode("overwrite").option("overwriteSchema","true").format("delta")\
   .saveAsTable("gold.fact_emission_episode")
print("fact_emission_episode written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
