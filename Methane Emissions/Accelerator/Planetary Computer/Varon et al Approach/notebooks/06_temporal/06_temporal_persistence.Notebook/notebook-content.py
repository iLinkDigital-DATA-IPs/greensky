# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5d5c8002-789e-4319-81d1-a60f08a77996",
# META       "default_lakehouse_name": "greensky_lakehouse",
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

# ### Load configs:

# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load plume catalog:

# CELL ********************

import pandas as pd
import numpy as np

plumes = spark.table("gold_plume_catalog").toPandas()
print(f"Loaded {len(plumes)} plumes")

plumes["detection_date"] = pd.to_datetime(plumes["detection_date"])
print(f"Date range: {plumes['detection_date'].min()} to {plumes['detection_date'].max()}")
print(f"Distinct scenes: {plumes['scene_id'].nunique()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Match plumes to emission sites by proximity:

# CELL ********************

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = (np.sin(dlat/2)**2 +
         np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2)
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

match_radius_km = CONFIG["persistence_match_radius_km"]  # 5 km

plumes_sorted = plumes.sort_values("detection_date").reset_index(drop=True)

# Greedy site assignment: each plume joins nearest existing site or creates a new one
sites = []
site_counter = 0

for _, plume in plumes_sorted.iterrows():
    plat = plume["source_lat"]
    plon = plume["source_lon"]
    pid = plume["plume_id"]

    matched_site = None
    min_dist = float("inf")

    for site in sites:
        dist = haversine_km(plat, plon, site["lat"], site["lon"])
        if dist <= match_radius_km and dist < min_dist:
            matched_site = site
            min_dist = dist

    if matched_site is not None:
        matched_site["plume_ids"].append(pid)
        # Update site centroid as running average
        n = len(matched_site["plume_ids"])
        matched_site["lat"] = (matched_site["lat"] * (n - 1) + plat) / n
        matched_site["lon"] = (matched_site["lon"] * (n - 1) + plon) / n
    else:
        site_counter += 1
        sites.append({
            "site_id": site_counter,
            "lat": plat,
            "lon": plon,
            "plume_ids": [pid],
        })

print(f"Emission sites identified: {len(sites)}")
print(f"Sites with repeat detections: {sum(1 for s in sites if len(s['plume_ids']) > 1)}")
print(f"Max detections at a single site: {max(len(s['plume_ids']) for s in sites)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Classify sites by persistence:

# CELL ********************

site_records = []

for site in sites:
    pids = site["plume_ids"]
    site_plumes = plumes_sorted[plumes_sorted["plume_id"].isin(pids)]

    detection_count = len(pids)
    first_detection = site_plumes["detection_date"].min()
    last_detection = site_plumes["detection_date"].max()
    observation_span_days = (last_detection - first_detection).total_seconds() / 86400

    # Classify
    if detection_count == 1:
        persistence = "single"
    elif detection_count == 2:
        persistence = "intermittent"
    elif observation_span_days <= 30:
        persistence = "persistent"
    else:
        persistence = "chronic"

    # Aggregate emission stats
    avg_rate = site_plumes["emission_rate_kg_h"].mean()
    max_rate = site_plumes["emission_rate_kg_h"].max()
    total_ime = site_plumes["ime_kg"].sum()

    # Dominant confidence
    conf_counts = site_plumes["confidence"].value_counts()
    dominant_confidence = conf_counts.index[0] if len(conf_counts) > 0 else "unknown"

    # Attributed facility (most common)
    if "attributed_facility_name" in site_plumes.columns:
        fac_counts = site_plumes["attributed_facility_name"].value_counts()
        top_facility = fac_counts.index[0] if len(fac_counts) > 0 else None
    else:
        top_facility = None

    # List of detection dates for temporal analysis
    detection_dates = sorted(site_plumes["detection_date"].dt.strftime("%Y-%m-%d").tolist())

    site_records.append({
        "site_id": site["site_id"],
        "site_lat": site["lat"],
        "site_lon": site["lon"],
        "detection_count": detection_count,
        "first_detection": first_detection,
        "last_detection": last_detection,
        "observation_span_days": round(observation_span_days, 1),
        "persistence": persistence,
        "avg_emission_rate_kg_h": round(avg_rate, 2),
        "max_emission_rate_kg_h": round(max_rate, 2),
        "total_ime_kg": round(total_ime, 2),
        "dominant_confidence": dominant_confidence,
        "attributed_facility": top_facility,
        "detection_dates": str(detection_dates),
        "plume_ids": str(site["plume_ids"]),
    })

sites_df = pd.DataFrame(site_records)

print("=== Persistence Classification ===")
print(sites_df["persistence"].value_counts().to_string())
print()
print("=== Detection Count Distribution ===")
print(sites_df["detection_count"].value_counts().sort_index().to_string())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary and high-priority sites:

# CELL ********************

print("=" * 60)
print("EMISSION SITE SUMMARY")
print("=" * 60)
print(f"Total emission sites: {len(sites_df)}")
print(f"Date range: {plumes['detection_date'].min().date()} to {plumes['detection_date'].max().date()}")
print()

for p_type in ["chronic", "persistent", "intermittent", "single"]:
    subset = sites_df[sites_df["persistence"] == p_type]
    if len(subset) > 0:
        print(f"--- {p_type.upper()} ({len(subset)} sites) ---")
        print(f"  Avg emission rate: {subset['avg_emission_rate_kg_h'].mean():.1f} kg/h")
        print(f"  Max emission rate: {subset['max_emission_rate_kg_h'].max():.1f} kg/h")
        print(f"  Total detections: {subset['detection_count'].sum()}")
        print(f"  Avg detection count: {subset['detection_count'].mean():.1f}")
        print()

print("=== TOP 10 PRIORITY SITES (by total IME) ===")
top_sites = sites_df.nlargest(10, "total_ime_kg")
print(top_sites[[
    "site_id", "site_lat", "site_lon", "detection_count",
    "persistence", "avg_emission_rate_kg_h", "total_ime_kg",
    "attributed_facility"
]].to_string())

print()
print("=== REPEAT EMITTERS ===")
repeats = sites_df[sites_df["detection_count"] > 1].sort_values(
    "detection_count", ascending=False
)
if len(repeats) > 0:
    print(f"{len(repeats)} sites with 2+ detections:")
    print(repeats[[
        "site_id", "site_lat", "site_lon", "detection_count",
        "observation_span_days", "persistence",
        "avg_emission_rate_kg_h", "attributed_facility",
        "detection_dates"
    ]].to_string())
else:
    print("No repeat emitters detected in this 1-month window")
    print("This is expected with ~27 scenes over 30 days at TROPOMI resolution")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write Gold tables:

# CELL ********************

# Write emission sites
sites_spark = spark.createDataFrame(sites_df)
sites_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_emission_sites")
print(f"Written {len(sites_df)} sites to gold_emission_sites")

# Plume-to-site mapping
plume_site_map = []
for site in sites:
    for pid in site["plume_ids"]:
        plume_site_map.append({
            "plume_id": pid,
            "site_id": site["site_id"],
        })

map_df = pd.DataFrame(plume_site_map)
map_spark = spark.createDataFrame(map_df)
map_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_plume_site_mapping")
print(f"Written {len(map_df)} plume-to-site mappings")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validate all Gold tables:

# CELL ********************

print("=" * 60)
print("GOLD LAYER STATUS")
print("=" * 60)

gold_tables = [
    "gold_plume_catalog",
    "gold_flagged_large_clusters",
    "gold_emission_sites",
    "gold_plume_site_mapping",
    "ref_facilities",
]

for table in gold_tables:
    try:
        count = spark.table(table).count()
        cols = len(spark.table(table).columns)
        print(f"  {table}: {count:,} rows, {cols} columns")
    except Exception:
        print(f"  {table}: NOT FOUND")

print()
print("=" * 60)
print("FULL DATA LINEAGE")
print("=" * 60)

bronze_pixels = spark.table("bronze_methane_pixels").count()
bronze_weather = spark.table("bronze_weather").count()
bronze_era5 = spark.table("bronze_era5_wind").count()
silver = spark.table("silver_plume_ready_pixels").count()
gold_plumes = spark.table("gold_plume_catalog").count()
gold_sites = spark.table("gold_emission_sites").count()
gold_flagged = spark.table("gold_flagged_large_clusters").count()

print(f"  bronze_methane_pixels:       {bronze_pixels:>10,} rows")
print(f"  bronze_weather:              {bronze_weather:>10,} rows")
print(f"  bronze_era5_wind:            {bronze_era5:>10,} rows")
print(f"  silver_plume_ready_pixels:   {silver:>10,} rows")
print(f"  gold_plume_catalog:          {gold_plumes:>10,} rows")
print(f"  gold_emission_sites:         {gold_sites:>10,} rows")
print(f"  gold_flagged_large_clusters: {gold_flagged:>10,} rows")
print()
print(f"  Permian Basin pixels: {silver:,}")
print(f"  -> Detected plumes: {gold_plumes} ({100*gold_plumes/silver:.2f}%)")
print(f"  -> Emission sites: {gold_sites}")
print(f"  -> Flagged large clusters: 20 ({gold_flagged} pixels)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
