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
# META     }
# META   }
# META }

# CELL ********************

# =============================================================================
# Purpose: For each plume in the catalog (or just the new ones), assign
# candidate facilities with confidence scores using tiered geometry +
# wind-cone logic. Writes to fact_plume_attribution. Idempotent by
# (plume_id, candidate_facility_id) -> attribution_id.
# 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ./config_and_utils


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import math
import hashlib
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


incremental_only = True   # if False, re-attributes all plumes from scratch

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build a single facility catalog from the dimensions, with location, type,
# operator, and emission prior.
 
def build_facility_catalog() -> pd.DataFrame:
 
    pads = spark.table(fqn("silver", "dim_well_pad")).select(
        F.col("pad_id").alias("facility_id"),
        F.lit("pad").alias("facility_type"),
        "operator_id",
        F.col("latitude").alias("lat"),
        F.col("longitude").alias("lon"),
        F.col("emission_propensity").alias("emission_prior"),
    ).toPandas()
 
    stations = spark.table(fqn("silver", "dim_compressor_station")).select(
        F.col("station_id").alias("facility_id"),
        F.lit("compressor_station").alias("facility_type"),
        "operator_id",
        F.col("latitude").alias("lat"),
        F.col("longitude").alias("lon"),
        F.col("emission_propensity").alias("emission_prior"),
    ).toPandas()
 
    plants = spark.table(fqn("silver", "dim_processing_plant")).select(
        F.col("plant_id").alias("facility_id"),
        F.lit("processing_plant").alias("facility_type"),
        "operator_id",
        F.col("latitude").alias("lat"),
        F.col("longitude").alias("lon"),
        F.lit(0.45).alias("emission_prior"),   # plants get a moderate fixed prior
    ).toPandas()
 
    pipes = spark.table(fqn("silver", "dim_pipeline_segment")).select(
        F.col("segment_id").alias("facility_id"),
        F.lit("pipeline_segment").alias("facility_type"),
        F.lit("OP_UNKNOWN").alias("operator_id"),     # pipelines don't have owner in v1
        F.col("midpoint_lat").alias("lat"),
        F.col("midpoint_lon").alias("lon"),
        F.lit(0.25).alias("emission_prior"),
    ).toPandas()
 
    return pd.concat([pads, stations, plants, pipes], ignore_index=True)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Estimate the true source point. The plume catalog already has source_lat /
# source_lon. We trust those when wind_aligned=True. Otherwise we back-project
# the centroid upwind by a heuristic distance.
 
 
def estimate_source_point(plume_row) -> tuple:
    if plume_row.get("wind_aligned", False):
        return float(plume_row["source_lat"]), float(plume_row["source_lon"])
 
    # Back-project centroid upwind. Wind blows TO direction (wind_dir_deg as
    # meteorological convention is "from direction"; we assume the catalog
    # uses "to direction" — adjust here if the pipeline uses the other
    # convention. Defensive: treat as "wind blows from this direction" by
    # adding 180.
    wind_from_deg = float(plume_row["wind_dir_deg"])
    wind_to_deg = (wind_from_deg + 180.0) % 360.0
    # Distance upwind ~ wind_speed * 1000s (rough plume travel time)
    wind_speed = float(plume_row.get("wind_speed_ms", 3.0))
    offset_km = min(max(wind_speed * 1.0, 2.0), 10.0)
 
    # Move upwind: from centroid, go opposite of wind_to_deg
    upwind_bearing = (wind_to_deg + 180.0) % 360.0
    lat0 = float(plume_row["centroid_lat"])
    lon0 = float(plume_row["centroid_lon"])
    new_lat, new_lon = _move_point(lat0, lon0, offset_km, upwind_bearing)
    return new_lat, new_lon
 
 
def _move_point(lat, lon, distance_km, bearing_deg_):
    """
    Move a point distance_km along bearing_deg from (lat, lon).
    Spherical earth approximation.
    """
    R = 6371.0088
    br = math.radians(bearing_deg_)
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)
    d_over_R = distance_km / R
    lat2 = math.asin(math.sin(lat1) * math.cos(d_over_R)
                     + math.cos(lat1) * math.sin(d_over_R) * math.cos(br))
    lon2 = lon1 + math.atan2(
        math.sin(br) * math.sin(d_over_R) * math.cos(lat1),
        math.cos(d_over_R) - math.sin(lat1) * math.sin(lat2),
    )
    return math.degrees(lat2), math.degrees(lon2)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Attribution kernel — pure NumPy/pandas, no Spark inside the loop
 
 
def attribute_single_plume(plume_row, facilities_pd, attr_cfg):
    src_lat, src_lon = estimate_source_point(plume_row)
    wind_from_deg = float(plume_row["wind_dir_deg"])
    wind_aligned = bool(plume_row.get("wind_aligned", False))
    quality = str(plume_row.get("emission_rate_confidence", "medium")).lower()
 
    f_lat = facilities_pd["lat"].to_numpy()
    f_lon = facilities_pd["lon"].to_numpy()
 
    # Distance from estimated source to every facility
    d_km = haversine_km(src_lat, src_lon, f_lat, f_lon)
 
    # Bearing from source to each facility
    brg = bearing_deg(src_lat, src_lon, f_lat, f_lon)
 
    # Wind blows FROM wind_from_deg, so upwind direction (toward leak source)
    # from a candidate facility is wind_from_deg. From the source point
    # looking at where the leak could be: the cone points opposite of
    # downwind, i.e. opposite of (wind_from_deg + 180) = wind_from_deg.
    # Half-angle window:
    cone_center = wind_from_deg
    diff = angular_diff_deg(brg, cone_center)
    within_cone = (diff <= attr_cfg["tier2_cone_half_angle_deg"]) & (d_km <= attr_cfg["tier2_cone_length_km"])
 
    tier = np.full(len(facilities_pd), -1, dtype=np.int32)
    tier[d_km <= attr_cfg["tier1_radius_km"]] = 1
    tier[(tier == -1) & within_cone] = 2
    tier[(tier == -1) & (d_km <= attr_cfg["tier3_radius_km"])] = 3
    mask = tier > 0
    if not mask.any():
        return None
 
    decay_lookup = attr_cfg["tier_decay_length_km"]
    # YAML int-keys come back as strings if loaded that way; coerce
    decay = np.array([
        float(decay_lookup.get(int(t), decay_lookup.get(str(int(t)), 10.0)))
        for t in tier[mask]
    ])
 
    d_sel = d_km[mask]
    distance_term = np.exp(-d_sel / decay)
    within_sel = within_cone[mask]
    wind_term = np.where(within_sel, 1.0, 0.5)
 
    prior_term = facilities_pd["emission_prior"].to_numpy()[mask]
 
    quality_map = {"high": 1.0, "medium": 0.7, "low": 0.4}
    plume_quality = quality_map.get(quality, 0.7)
 
    score = distance_term * wind_term * prior_term * plume_quality
    if wind_aligned:
        score = score * (1.0 + attr_cfg["wind_alignment_bonus"])
    score = np.clip(score, 0.0, 1.0)
 
    # Confidence tier
    conf_tier = np.where(
        (score >= 0.6) & (tier[mask] == 1), "high",
        np.where(
            (score >= 0.3) | ((score >= 0.6) & (tier[mask] == 2)),
            "medium", "low",
        ),
    )
 
    # Best candidate flag
    best_idx = int(np.argmax(score))
 
    sel_facilities = facilities_pd.iloc[mask].reset_index(drop=True)
 
    out = []
    for i in range(len(sel_facilities)):
        fid = sel_facilities.iloc[i]["facility_id"]
        attribution_id = deterministic_id("ATT", plume_row["plume_id"], fid)
        out.append({
            "attribution_id":          attribution_id,
            "plume_id":                int(plume_row["plume_id"]),
            "scene_id":                str(plume_row["scene_id"]),
            "candidate_facility_id":   fid,
            "candidate_facility_type": sel_facilities.iloc[i]["facility_type"],
            "candidate_operator_id":   sel_facilities.iloc[i]["operator_id"],
            "tier":                    int(tier[mask][i]),
            "distance_km":             float(d_sel[i]),
            "within_wind_cone":        bool(within_sel[i]),
            "wind_alignment_score":    float(wind_term[i]),
            "facility_emission_prior": float(prior_term[i]),
            "confidence_score":        float(round(score[i], 6)),
            "confidence_tier":         str(conf_tier[i]),
            "is_best_candidate":       i == best_idx,
            "coupled_event_id":        None,
        })
    return out
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Driver
 
 
def existing_attributed_plume_ids():
    table = fqn("silver", "fact_plume_attribution")
    if not spark.catalog.tableExists(table):
        return set()
    df = spark.sql(f"SELECT DISTINCT plume_id FROM {table}")
    return {r.plume_id for r in df.collect()}
 
 
def main_attribution():
    plume_table = CONFIG["paths"]["plume_catalog_table"]
    plumes = spark.table(plume_table).toPandas()
    print(f"Loaded {len(plumes)} plumes from catalog.")
 
    if incremental_only:
        done = existing_attributed_plume_ids()
        plumes = plumes[~plumes["plume_id"].isin(done)]
        print(f"Incremental: {len(plumes)} new plumes to attribute.")
        if len(plumes) == 0:
            return
 
    facilities = build_facility_catalog()
    print(f"Facility catalog: {len(facilities)} rows.")
 
    attr_cfg = CONFIG["attribution"]
    # Normalize key types in tier_decay_length_km
    attr_cfg["tier_decay_length_km"] = {
        int(k): float(v) for k, v in attr_cfg["tier_decay_length_km"].items()
    }
 
    all_rows = []
    for _, p in plumes.iterrows():
        rows = attribute_single_plume(p.to_dict(), facilities, attr_cfg)
        if rows:
            all_rows.extend(rows)
 
    if not all_rows:
        print("No attributions produced (no facilities in range of any plume).")
        return
 
    pdf = pd.DataFrame(all_rows)
    sdf = spark.createDataFrame(pdf, schema=SCHEMA_ATTRIBUTION)
 
    table = fqn("silver", "fact_plume_attribution")
    if not spark.catalog.tableExists(table):
        (sdf.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table))
        print(f"Created {table} with {len(pdf)} rows.")
    else:
        # MERGE upsert on attribution_id
        sdf.createOrReplaceTempView("staging_attr")
        spark.sql(f"""
            MERGE INTO {table} t
            USING staging_attr s
            ON t.attribution_id = s.attribution_id
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Merged {len(pdf)} rows into {table}.")
 
 
main_attribution()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
