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
# A plume is a NOISY OBSERVATION of an emission episode at a satellite/aerial
# overpass instant. Detection is gated by rate vs instrument detection limit,
# wind speed (too calm → no plume shape; too strong → diluted), plume–wind
# alignment, and cloud cover. Quantified rate carries noise → emission_rate_kg_s
# differs from the true episode rate, and emission_rate_confidence reflects SNR.
# Output schema is a superset of the real plume schema so real + synthetic union.
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

ep = spark.table("gold.fact_emission_episode").toPandas()
ep["start_ts"] = pd.to_datetime(ep["start_ts"]); ep["end_ts"] = pd.to_datetime(ep["end_ts"])

# Detection probability for a single overpass over an active episode.
def p_detect(rate_kg_s, wind_ms, det_limit_kg_hr):
    rate_kg_hr = rate_kg_s * 3600.0
    if rate_kg_hr < det_limit_kg_hr * 0.5:
        return 0.02
    snr = rate_kg_hr / max(det_limit_kg_hr, 0.1)
    base = 1 - np.exp(-snr / 6.0)                       # saturating with magnitude
    wind_factor = np.clip(1 - abs(wind_ms - 4.0)/8.0, 0.2, 1.0)  # best near 4 m/s
    return float(np.clip(base * wind_factor, 0.0, 0.97))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ── generate synthetic plumes ────────────────────────────────────
prng = get_rng("synthetic_plumes")
plumes = []; pl_id = 0

# Debug: print actual values before using them
print(f"HISTORY_START    : {HISTORY_START}  (type: {type(HISTORY_START).__name__})")
print(f"SYNTH_PLUME_END  : {SYNTH_PLUME_END}  (type: {type(SYNTH_PLUME_END).__name__})")
print(f"REAL_PLUME_START : {REAL_PLUME_START}")
print(f"REAL_PLUME_END   : {REAL_PLUME_END}")
print(f"episodes loaded  : {len(ep)}")

overpass_days = pd.date_range(
    start=pd.Timestamp(HISTORY_START),
    end=pd.Timestamp(SYNTH_PLUME_END),
    freq="D"
)

if len(overpass_days) == 0:
    raise RuntimeError(
        f"overpass_days is empty — HISTORY_START ({HISTORY_START}) must be "
        f"before SYNTH_PLUME_END ({SYNTH_PLUME_END}). "
        "Re-run 00_config_and_seeds in this session and check HISTORY_YEARS."
    )

print(f"overpass window  : {len(overpass_days)} days  "
      f"({overpass_days[0].date()} → {overpass_days[-1].date()})")

for d in overpass_days:
    if prng.random() < 0.30:
        continue
    overpass_ts = d + pd.Timedelta(hours=float(prng.uniform(10, 14)))
    active = ep[(ep.start_ts <= overpass_ts) & (ep.end_ts >= overpass_ts)]
    for a in active.itertuples():
        wind      = max(0.5, float(prng.normal(CALIBRATION["wind_speed_mean"],
                                               CALIBRATION["wind_speed_sd"])))
        det_limit = float(prng.uniform(1.0, 4.0))
        if prng.random() > p_detect(a.peak_rate_kg_s, wind, det_limit):
            continue
        pl_id += 1
        wind_dir   = float(prng.uniform(0, 360))
        obs_rate   = a.peak_rate_kg_s * float(prng.lognormal(0, 0.35))
        rate_kg_hr = obs_rate * 3600
        conf       = float(np.clip(rate_kg_hr / (det_limit * 8), 0.1, 0.98))
        major      = float(np.clip(0.2 + obs_rate * 4 + wind * 0.05, 0.1, 6))
        minor      = major * float(prng.uniform(0.25, 0.6))
        offset_km  = wind * 0.1
        dlat = (offset_km / 111.0) * np.cos(np.radians(wind_dir))
        dlon = (offset_km / (111.0 * np.cos(np.radians(a.source_lat)))) \
               * np.sin(np.radians(wind_dir))
        ime_kg = obs_rate * 3600 * float(prng.uniform(0.3, 0.8))
        plumes.append({
            "plume_id":                 f"SP-{pl_id:06d}",
            "scene_id":                 f"SCN-{d.strftime('%Y%m%d')}",
            "detect_ts":                overpass_ts,
            "centroid_lat":             a.source_lat + dlat,
            "centroid_lon":             a.source_lon + dlon,
            "source_lat": a.source_lat + float(prng.normal(0, 0.001)),
            "source_lon": a.source_lon + float(prng.normal(0, 0.001)),
            "n_pixels":                 int(major * minor * 5000),
            "area_km2":                 round(np.pi * major * minor / 4, 4),
            "max_delta_ch4_ppb":        round(obs_rate * 2000 + float(prng.uniform(50, 300)), 1),
            "mean_delta_ch4_ppb":       round(obs_rate * 800  + float(prng.uniform(20, 120)), 1),
            "ime_ppb_km2":              round(ime_kg * 1.5, 2),
            "ime_kg":                   round(ime_kg, 2),
            "major_axis_km":            round(major, 3),
            "minor_axis_km":            round(minor, 3),
            "elongation":               round(major / max(minor, 1e-3), 2),
            "orientation_deg":          round(wind_dir + float(prng.normal(0, 15)), 1),
            "wind_speed_ms":            round(wind, 2),
            "wind_dir_deg":             round(wind_dir, 1),
            "angle_plume_wind_deg":     round(abs(float(prng.normal(0, 20))), 1),
            "wind_aligned":             bool(prng.random() < 0.75),
            "emission_rate_confidence": round(conf, 3),
            "emission_rate_kg_s":       round(obs_rate, 5),
            "episode_sk":               int(a.episode_sk),
            "is_synthetic":             True,
        })

syn_pdf = pd.DataFrame(plumes)
print(f"synthetic plumes : {len(syn_pdf)}  "
      f"(~{len(syn_pdf) / max(1, len(overpass_days)):.1f}/day  "
      f"target ~{CALIBRATION['daily_detections']:.0f})")

if len(syn_pdf) == 0:
    raise RuntimeError(
        "No synthetic plumes generated. "
        f"episodes loaded={len(ep)} — if 0, re-run notebook 20 first."
    )

# CELL ── ingest real plumes + union ───────────────────────────────────
# ── FIXED: ingest real plumes with ACTUAL detection dates ─────────────
real_cols = ["plume_id","scene_id","centroid_lat","centroid_lon","source_lat","source_lon",
             "n_pixels","area_km2","max_delta_ch4_ppb","mean_delta_ch4_ppb","ime_ppb_km2",
             "ime_kg","major_axis_km","minor_axis_km","elongation","orientation_deg",
             "wind_speed_ms","wind_dir_deg","angle_plume_wind_deg","wind_aligned",
             "emission_rate_confidence","emission_rate_kg_s"]

syn_sdf = spark.createDataFrame(syn_pdf).withColumn("detect_ts", F.col("detect_ts").cast("timestamp"))

try:
    raw = spark.table("bronze.plume_raw")
    available = [c for c in real_cols if c in raw.columns]
    real = raw.select(*available)

    # Cast types to match gold schema
    real = (real
        .withColumn("plume_id", F.col("plume_id").cast("string"))
        .withColumn("n_pixels", F.col("n_pixels").cast("long"))
        .withColumn("emission_rate_confidence", F.col("emission_rate_confidence").cast("double")))

    # ── Parse ACTUAL detect_ts from scene_id ──────────────────────────
    # Try multiple patterns; adjust to your data's actual format:
    #   Pattern 1: "emit20260515t112300..."  → extract 8-digit date after "emit"
    #   Pattern 2: "EMIT_L2BCH4PLM_V001_20260515T112300_..."  → date after 3rd underscore
    #   Pattern 3: "SCN-20260515"  → date after "SCN-"
    #   Fallback:  extract any 8-digit sequence that looks like yyyyMMdd

    real = real.withColumn("_date_str",
        F.coalesce(
            # Pattern 1: "emit" prefix
            F.regexp_extract("scene_id", r"emit(\d{8})", 1),
            # Pattern 2: underscore-delimited with date segment
            F.regexp_extract("scene_id", r"_(\d{8})[Tt_]", 1),
            # Pattern 3: "SCN-" prefix
            F.regexp_extract("scene_id", r"SCN-(\d{8})", 1),
            # Fallback: any 8-digit run matching 20xx date
            F.regexp_extract("scene_id", r"(20[12]\d[01]\d[0-3]\d)", 1),
        ))

    # Convert to timestamp; fall back to REAL_PLUME_START only if parsing fails
    real = (real
        .withColumn("detect_ts",
            F.when(F.length("_date_str") == 8,
                   F.to_timestamp(F.col("_date_str"), "yyyyMMdd"))
             .otherwise(F.lit(REAL_PLUME_START.isoformat()).cast("timestamp")))
        .drop("_date_str")
        .withColumn("episode_sk", F.lit(None).cast("long"))
        .withColumn("is_synthetic", F.lit(False)))

    # Log how many got real dates vs fallback
    parsed = real.where(F.col("detect_ts") != F.lit(REAL_PLUME_START.isoformat()).cast("timestamp")).count()
    total = real.count()
    print(f"real plumes: {total} total, {parsed} with parsed dates, "
          f"{total - parsed} fell back to {REAL_PLUME_START}")

    # Align columns to synthetic schema
    for c in syn_sdf.columns:
        if c not in real.columns:
            real = real.withColumn(c, F.lit(None))
    real = real.select(syn_sdf.columns)
    all_plumes = syn_sdf.unionByName(real)
    print(f"real plumes joined: {total}")
except Exception as ex:
    print(f"real plumes not joined: {ex}")
    all_plumes = syn_sdf

all_plumes.write.mode("overwrite").option("overwriteSchema", "true").format("delta") \
    .saveAsTable("gold.fact_plume_detection")
print(f"gold.fact_plume_detection: {all_plumes.count()} rows written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

real_cols = ["plume_id","scene_id","centroid_lat","centroid_lon","source_lat","source_lon",
             "n_pixels","area_km2","max_delta_ch4_ppb","mean_delta_ch4_ppb","ime_ppb_km2",
             "ime_kg","major_axis_km","minor_axis_km","elongation","orientation_deg",
             "wind_speed_ms","wind_dir_deg","angle_plume_wind_deg","wind_aligned",
             "emission_rate_confidence","emission_rate_kg_s"]
syn_sdf = spark.createDataFrame(syn_pdf).withColumn("detect_ts", F.col("detect_ts").cast("timestamp"))

try:
    real = spark.table("bronze.plume_raw").filter(F.col("is_synthetic")==F.lit(False))
    real = (real.select(*[c for c in real_cols if c in real.columns])
                .withColumn("detect_ts", F.lit(REAL_PLUME_START.isoformat()).cast("timestamp"))
                .withColumn("episode_sk", F.lit(None).cast("long"))
                .withColumn("is_synthetic", F.lit(False)))
    # align columns
    for c in syn_sdf.columns:
        if c not in real.columns:
            real = real.withColumn(c, F.lit(None))
    real = real.select(syn_sdf.columns)
    all_plumes = syn_sdf.unionByName(real)
except Exception as ex:
    print("no real plumes found, writing synthetic only:", ex)
    all_plumes = syn_sdf

all_plumes.write.mode("overwrite").option("overwriteSchema","true").format("delta")\
          .saveAsTable("gold.fact_plume_detection")
print("fact_plume_detection rows:", all_plumes.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Ingest real plumes with ACTUAL detection dates ────────────────────
# Run AFTER synthetic plumes are written to gold.fact_plume_detection

from pyspark.sql import functions as F
# Step 1: Read what's already there (synthetic)
existing = spark.table("gold.fact_plume_detection")
existing_cols = existing.columns
existing_schema = {f.name: f.dataType for f in existing.schema}
print(f"Step 1: existing fact_plume_detection = {existing.count()} rows")
print(f"  columns: {existing_cols}")

# Step 2: Read bronze
raw = spark.table("bronze.plume_raw")
print(f"Step 2: bronze.plume_raw = {raw.count()} rows")

# Step 3: Parse detect_ts from scene_id format "scene_YYYYMMDD_HHMMSS"
real = (raw
    .withColumn("_date_part", F.regexp_extract("scene_id", r"scene_(\d{8})_(\d{6})", 1))
    .withColumn("_time_part", F.regexp_extract("scene_id", r"scene_(\d{8})_(\d{6})", 2))
    .withColumn("_dt_str", F.concat(F.col("_date_part"), F.lit("T"), F.col("_time_part")))
    .withColumn("detect_ts", F.to_timestamp("_dt_str", "yyyyMMdd'T'HHmmss"))
    .drop("_date_part", "_time_part", "_dt_str"))

parsed_count = real.where(F.col("detect_ts").isNotNull()).count()
null_count = real.where(F.col("detect_ts").isNull()).count()
print(f"Step 3: parsed dates = {parsed_count}, null dates = {null_count}")
real.select("scene_id", "detect_ts").show(5, truncate=False)

# Step 4: Cast types to match gold schema
real = (real
    .withColumn("plume_id", F.col("plume_id").cast("string"))
    .withColumn("n_pixels", F.col("n_pixels").cast("long"))
    .withColumn("emission_rate_confidence", F.col("emission_rate_confidence").cast("double"))
    .withColumn("episode_sk", F.lit(None).cast("long"))
    .withColumn("is_synthetic", F.lit(False)))
print(f"Step 4: casts applied")

# Step 5: Add any missing columns (fill with null, matching type)
for col_name in existing_cols:
    if col_name not in real.columns:
        real = real.withColumn(col_name, F.lit(None).cast(existing_schema[col_name]))
        print(f"  added missing column: {col_name}")

# Drop any extra columns not in the target schema
real = real.select(*existing_cols)
print(f"Step 5: schema aligned — {len(real.columns)} columns")

# Step 6: Union and write
combined = existing.unionByName(real)
print(f"Step 6: combined = {combined.count()} rows "
      f"(synthetic={existing.count()}, real={real.count()})")

combined.write.mode("overwrite").option("overwriteSchema", "true") \
    .format("delta").saveAsTable("gold.fact_plume_detection")

# Step 7: Verify
verify = spark.table("gold.fact_plume_detection")
verify.groupBy("is_synthetic").count().show()
verify.where("is_synthetic = false").select("plume_id", "detect_ts", "scene_id") \
    .orderBy("detect_ts").show(10, truncate=False)
print("DONE — real plumes ingested with actual May/June 2026 dates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
