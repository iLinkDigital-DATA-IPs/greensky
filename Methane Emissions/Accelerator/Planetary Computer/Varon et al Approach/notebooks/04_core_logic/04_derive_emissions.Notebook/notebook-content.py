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

# ### Load config and dependencies:

# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Imports and load Silver data:

# CELL ********************

import numpy as np
import pandas as pd
from pyspark.sql.functions import (
    col, lit, sqrt, sin, cos, atan2, radians, degrees,
    avg, min as spark_min, max as spark_max, count as spark_count,
    sum as spark_sum, abs as spark_abs, expr,
    row_number, dense_rank, collect_list, struct,
    unix_timestamp, when, array, udf, percentile_approx
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, ArrayType,
    StructType, StructField
)
from pyspark.sql.window import Window

# Load plume-ready pixels
silver = spark.table("silver_plume_ready_pixels")
total_pixels = silver.count()
print(f"Loaded {total_pixels:,} plume-ready pixels")
print(f"CH4 range: {silver.select(spark_min('ch4'), spark_max('ch4')).first()}")
print(f"Distinct STAC IDs: {silver.select('stac_id').distinct().count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 1: Scene Separation:

# CELL ********************

# Scenes are groups of observations separated by > 10 minutes
# Group by stac_id first, then split by time gaps within each stac_id

scene_gap_seconds = CONFIG["scene_gap_minutes"] * 60  # 600 seconds

# Each stac_id is one satellite overpass file
# Within a stac_id, all pixels share roughly the same acquisition time
# Different stac_ids with overlapping times are the same scene

# Get distinct stac_ids with their time ranges
stac_times = silver.groupBy("stac_id").agg(
    spark_min("datetime").alias("scene_start"),
    spark_max("datetime").alias("scene_end"),
    spark_count("*").alias("pixel_count")
).orderBy("scene_start")

stac_times_pdf = stac_times.toPandas()
print(f"Total STAC IDs: {len(stac_times_pdf)}")

# Assign scene IDs by grouping stac_ids with < 10 min gap
stac_times_pdf = stac_times_pdf.sort_values("scene_start").reset_index(drop=True)
stac_times_pdf["time_gap_s"] = (
    stac_times_pdf["scene_start"] - stac_times_pdf["scene_end"].shift(1)
).dt.total_seconds()
stac_times_pdf["new_scene"] = (
    stac_times_pdf["time_gap_s"].isna() | 
    (stac_times_pdf["time_gap_s"] > scene_gap_seconds)
)
stac_times_pdf["scene_id"] = stac_times_pdf["new_scene"].cumsum()

print(f"Scenes identified: {stac_times_pdf['scene_id'].nunique()}")
print("\nScene summary:")
print(stac_times_pdf[["stac_id", "scene_start", "pixel_count", "scene_id"]].to_string())

# Create mapping DataFrame and join back
scene_map = spark.createDataFrame(
    stac_times_pdf[["stac_id", "scene_id"]]
)
silver = silver.join(scene_map, on="stac_id", how="inner")
print(f"\nPixels after scene assignment: {silver.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 2: Background Estimation:

# CELL ********************

# For each pixel, find ~30 nearest neighbors within the same scene
# Use the 15th percentile of their CH4 values as local background
# This runs per-scene, so convert to Pandas for each scene

from scipy.spatial import cKDTree

def estimate_background(scene_pdf, n_neighbors=None, percentile=None):
    """
    For each pixel in a scene, estimate background CH4 from nearest neighbors.
    Uses kNN with 15th percentile.
    """
    if n_neighbors is None:
        n_neighbors = CONFIG["background_neighbors"]
    if percentile is None:
        percentile = CONFIG["background_percentile"]
    
    coords = scene_pdf[["latitude", "longitude"]].values
    ch4_vals = scene_pdf["ch4"].values
    
    if len(coords) < n_neighbors + 1:
        # Too few pixels -- use scene median as background
        bg = np.full(len(coords), np.median(ch4_vals))
        return bg
    
    # Build KD-tree for fast neighbor lookup
    tree = cKDTree(coords)
    
    # Query n_neighbors + 1 (includes self)
    _, indices = tree.query(coords, k=min(n_neighbors + 1, len(coords)))
    
    # For each pixel, compute percentile of neighbor CH4 values
    bg = np.zeros(len(coords))
    for i in range(len(coords)):
        neighbor_idx = indices[i]
        # Exclude self (first neighbor is always self with distance 0)
        neighbor_ch4 = ch4_vals[neighbor_idx[1:]]
        bg[i] = np.percentile(neighbor_ch4, percentile * 100)
    
    return bg

# Process each scene
silver_pdf = silver.toPandas()
print(f"Processing {silver_pdf['scene_id'].nunique()} scenes...")

all_results = []
for scene_id, scene_group in silver_pdf.groupby("scene_id"):
    scene_group = scene_group.copy()
    
    # Estimate background
    bg = estimate_background(scene_group)
    scene_group["ch4_background"] = bg
    scene_group["ch4_enhancement"] = scene_group["ch4"] - bg
    
    all_results.append(scene_group)
    print(f"  Scene {scene_id}: {len(scene_group)} pixels, "
          f"avg bg={bg.mean():.1f} ppb, "
          f"avg enhancement={scene_group['ch4_enhancement'].mean():.1f} ppb")

enhanced_pdf = pd.concat(all_results, ignore_index=True)
print(f"\nTotal pixels with background: {len(enhanced_pdf):,}")
print(f"Enhancement range: {enhanced_pdf['ch4_enhancement'].min():.1f} to {enhanced_pdf['ch4_enhancement'].max():.1f} ppb")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 3: Candidate Pixel Detection (MAD threshold):

# CELL ********************

# A pixel is a plume candidate when:
# delta_CH4 > max(3 * MAD, 6 ppb)

mad_sigma = CONFIG["mad_sigma"]
enhancement_floor = CONFIG["enhancement_floor_ppb"]

candidates_list = []

for scene_id, scene_group in enhanced_pdf.groupby("scene_id"):
    enhancements = scene_group["ch4_enhancement"].values
    
    # Median Absolute Deviation of the background distribution
    median_enh = np.median(enhancements)
    mad = np.median(np.abs(enhancements - median_enh))
    
    # MAD to sigma conversion (1 MAD ~ 0.6745 sigma for normal distribution)
    mad_scaled = mad * 1.4826  # scale factor to match standard deviation
    
    # Threshold
    threshold = max(mad_sigma * mad_scaled, enhancement_floor)
    
    # Flag candidates
    scene_group = scene_group.copy()
    scene_group["is_candidate"] = scene_group["ch4_enhancement"] > threshold
    scene_group["detection_threshold"] = threshold
    scene_group["scene_mad"] = mad_scaled
    
    n_candidates = scene_group["is_candidate"].sum()
    print(f"  Scene {scene_id}: MAD={mad_scaled:.2f} ppb, "
          f"threshold={threshold:.2f} ppb, "
          f"candidates={n_candidates}/{len(scene_group)}")
    
    candidates_list.append(scene_group)

detected_pdf = pd.concat(candidates_list, ignore_index=True)
total_candidates = detected_pdf["is_candidate"].sum()
print(f"\nTotal candidate pixels: {total_candidates} / {len(detected_pdf)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 4: Plume Clustering (Union-Find):

# CELL ********************

# Group candidate pixels within 12 km radius using Union-Find
# Then filter by cluster size (3-15 pixels) and shape

class UnionFind:
    def __init__(self, n):
        self.parent = list(range(n))
        self.rank = [0] * n
    
    def find(self, x):
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    
    def union(self, x, y):
        px, py = self.find(x), self.find(y)
        if px == py:
            return
        if self.rank[px] < self.rank[py]:
            px, py = py, px
        self.parent[py] = px
        if self.rank[px] == self.rank[py]:
            self.rank[px] += 1

def haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance in km between two points."""
    R = 6371.0
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

cluster_radius = CONFIG["cluster_radius_km"]
min_pixels = CONFIG["min_cluster_pixels"]
max_pixels = CONFIG["max_cluster_pixels"]
shape_threshold = CONFIG["shape_threshold"]

# Work only with candidate pixels
candidates_only = detected_pdf[detected_pdf["is_candidate"]].copy()
print(f"Clustering {len(candidates_only)} candidate pixels...")

all_plumes = []
flagged_large = []
plume_counter = 0

for scene_id, scene_candidates in candidates_only.groupby("scene_id"):
    if len(scene_candidates) < min_pixels:
        continue
    
    coords = scene_candidates[["latitude", "longitude"]].values
    n = len(coords)
    
    # Build Union-Find
    uf = UnionFind(n)
    
    # Connect pixels within cluster_radius
    for i in range(n):
        for j in range(i + 1, n):
            dist = haversine_km(
                coords[i, 0], coords[i, 1],
                coords[j, 0], coords[j, 1]
            )
            if dist <= cluster_radius:
                uf.union(i, j)
    
    # Extract clusters
    from collections import defaultdict
    clusters = defaultdict(list)
    for i in range(n):
        clusters[uf.find(i)].append(i)
    
    for cluster_root, member_indices in clusters.items():
        cluster_size = len(member_indices)
        cluster_data = scene_candidates.iloc[member_indices].copy()
        
        # Size filter
        if cluster_size < min_pixels:
            continue
        
        # Shape filter (aspect ratio)
        lats = cluster_data["latitude"].values
        lons = cluster_data["longitude"].values
        
        if cluster_size >= 2:
            # Compute aspect ratio from coordinate spread
            lat_range = lats.max() - lats.min()
            lon_range = (lons.max() - lons.min()) * np.cos(np.radians(lats.mean()))
            
            if min(lat_range, lon_range) > 0:
                aspect_ratio = max(lat_range, lon_range) / min(lat_range, lon_range)
            else:
                aspect_ratio = float("inf")
            
            if aspect_ratio > shape_threshold:
                continue
        else:
            aspect_ratio = 1.0
        
        # Large cluster handling
        if cluster_size > max_pixels:
            plume_counter += 1
            cluster_data["plume_id"] = plume_counter
            cluster_data["plume_flag"] = "large_cluster"
            cluster_data["aspect_ratio"] = aspect_ratio
            flagged_large.append(cluster_data)
            print(f"  Scene {scene_id}: FLAGGED large cluster "
                  f"({cluster_size} pixels, aspect={aspect_ratio:.1f})")
            continue
        
        # Valid plume
        plume_counter += 1
        cluster_data["plume_id"] = plume_counter
        cluster_data["plume_flag"] = "valid"
        cluster_data["aspect_ratio"] = aspect_ratio
        all_plumes.append(cluster_data)

print(f"\nValid plumes: {len(all_plumes)}")
print(f"Flagged large clusters: {len(flagged_large)}")
print(f"Total plume IDs assigned: {plume_counter}")

if all_plumes:
    plumes_pdf = pd.concat(all_plumes, ignore_index=True)
    print(f"Total pixels in valid plumes: {len(plumes_pdf)}")
else:
    plumes_pdf = pd.DataFrame()
    print("WARNING: No valid plumes detected. Check thresholds.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 5: Wind Alignment:

# CELL ********************

# For each plume, compute plume orientation via principal-axis analysis
# Compare with wind direction
# Plumes aligned within 75 degrees get higher confidence

if len(plumes_pdf) == 0:
    print("No plumes to process. Skipping wind alignment.")
else:
    wind_threshold = CONFIG["wind_alignment_threshold_deg"]
    
    plume_summaries = []
    
    for plume_id, plume_group in plumes_pdf.groupby("plume_id"):
        lats = plume_group["latitude"].values
        lons = plume_group["longitude"].values
        
        # Plume centroid
        centroid_lat = lats.mean()
        centroid_lon = lons.mean()
        
        # Principal axis analysis (PCA on coordinates)
        if len(lats) >= 3:
            coords_centered = np.column_stack([
                (lats - centroid_lat) * 111.0,  # convert to km
                (lons - centroid_lon) * 94.0
            ])
            cov_matrix = np.cov(coords_centered.T)
            eigenvalues, eigenvectors = np.linalg.eigh(cov_matrix)
            
            # Principal axis direction (largest eigenvalue)
            principal_axis = eigenvectors[:, np.argmax(eigenvalues)]
            plume_orientation = np.degrees(np.arctan2(principal_axis[1], principal_axis[0])) % 360
        else:
            plume_orientation = 0.0
        
        # Average wind direction from ERA5 u/v components
        mean_u = plume_group["era5_u10"].mean()
        mean_v = plume_group["era5_v10"].mean()
        wind_direction = np.degrees(np.arctan2(mean_u, mean_v)) % 360
        wind_speed = np.sqrt(mean_u**2 + mean_v**2)
        
        # Angular difference (smallest angle between two directions)
        angle_diff = abs(plume_orientation - wind_direction)
        if angle_diff > 180:
            angle_diff = 360 - angle_diff
        # Plumes can align in either direction along the axis
        if angle_diff > 90:
            angle_diff = 180 - angle_diff
        
        # Confidence based on wind alignment
        if angle_diff <= wind_threshold:
            wind_confidence = "high"
        else:
            wind_confidence = "medium"
        
        plume_summaries.append({
            "plume_id": plume_id,
            "plume_orientation_deg": plume_orientation,
            "wind_direction_deg": wind_direction,
            "wind_alignment_deg": angle_diff,
            "wind_confidence": wind_confidence,
            "era5_wind_speed_ms": wind_speed,
        })
    
    wind_df = pd.DataFrame(plume_summaries)
    print("Wind alignment results:")
    print(wind_df[["plume_id", "wind_alignment_deg", "wind_confidence", "era5_wind_speed_ms"]].to_string())
    
    # Merge wind alignment back to plume pixels
    plumes_pdf = plumes_pdf.merge(wind_df, on="plume_id", how="left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 6: IME Calculation:

# CELL ********************

# Integrated Methane Enhancement (IME)
# Convert enhancement from ppb to physical mass (kg)
#
# IME = sum over pixels of: delta_CH4 * column_density * pixel_area * M_CH4 / M_air
#
# Simplified approach for TROPOMI:
# - TROPOMI pixel area ~ 5.5 km x 7 km = 38.5 km^2
# - Dry air column ~ 2.12e25 molecules/m^2 (standard atmosphere)
# - CH4 molecular weight: 16.04 g/mol
# - Dry air molecular weight: 28.97 g/mol
# - 1 ppb = 1e-9 mol/mol

PIXEL_AREA_M2 = 5500.0 * 7000.0  # 38.5 km^2 in m^2
DRY_AIR_COLUMN = 2.12e25  # molecules/m^2
AVOGADRO = 6.022e23
M_CH4 = 16.04e-3  # kg/mol
M_AIR = 28.97e-3  # kg/mol

# Conversion factor: 1 ppb enhancement over 1 TROPOMI pixel -> kg CH4
# mass = delta_ppb * 1e-9 * (DRY_AIR_COLUMN / AVOGADRO) * M_CH4 * PIXEL_AREA_M2
PPB_TO_KG = 1e-9 * (DRY_AIR_COLUMN / AVOGADRO) * M_CH4 * PIXEL_AREA_M2

print(f"Conversion factor: 1 ppb over 1 pixel = {PPB_TO_KG:.6f} kg CH4")
print(f"  = {PPB_TO_KG * 1000:.4f} g CH4")

if len(plumes_pdf) > 0:
    ime_results = []
    
    for plume_id, plume_group in plumes_pdf.groupby("plume_id"):
        enhancements = plume_group["ch4_enhancement"].values
        
        # IME = sum of per-pixel mass contributions
        ime_kg = np.sum(enhancements * PPB_TO_KG)
        
        # Plume characteristics
        n_pixels = len(plume_group)
        plume_area_km2 = n_pixels * (5.5 * 7.0)  # approximate
        peak_enhancement = enhancements.max()
        mean_enhancement = enhancements.mean()
        
        # Source location estimate (pixel with highest enhancement)
        peak_idx = enhancements.argmax()
        source_lat = plume_group.iloc[peak_idx]["latitude"]
        source_lon = plume_group.iloc[peak_idx]["longitude"]
        
        # Scene info
        scene_id = plume_group["scene_id"].iloc[0]
        detection_date = plume_group["datetime"].iloc[0]
        stac_id = plume_group["stac_id"].iloc[0]
        
        ime_results.append({
            "plume_id": plume_id,
            "scene_id": scene_id,
            "stac_id": stac_id,
            "detection_date": detection_date,
            "source_lat": source_lat,
            "source_lon": source_lon,
            "n_pixels": n_pixels,
            "plume_area_km2": plume_area_km2,
            "peak_ch4_enhancement_ppb": peak_enhancement,
            "mean_ch4_enhancement_ppb": mean_enhancement,
            "ime_kg": ime_kg,
        })
    
    ime_df = pd.DataFrame(ime_results)
    print(f"\nIME calculated for {len(ime_df)} plumes:")
    print(ime_df[["plume_id", "n_pixels", "plume_area_km2", 
                   "mean_ch4_enhancement_ppb", "ime_kg"]].to_string())
else:
    ime_df = pd.DataFrame()
    print("No plumes for IME calculation.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 7: Emission Rate with Wind-Dependent T_mix (Tier 1):

# CELL ********************

# Emission Rate = IME / T_mix
# T_mix = L / U_eff (wind-dependent, Varon et al. 2018)
# L = sqrt(plume_area) in meters
# U_eff = effective wind speed from ERA5

if len(ime_df) > 0 and len(plumes_pdf) > 0:
    # Merge wind info into IME results
    wind_info = plumes_pdf.groupby("plume_id").agg({
        "era5_u10": "mean",
        "era5_v10": "mean",
        "era5_blh": "mean",
        "wind_speed_10m": "mean",
    }).reset_index()
    
    wind_info["era5_wind_speed"] = np.sqrt(
        wind_info["era5_u10"]**2 + wind_info["era5_v10"]**2
    )
    
    ime_df = ime_df.merge(wind_info, on="plume_id", how="left")
    
    # Also merge wind alignment
    if "wind_alignment_deg" in wind_df.columns:
        ime_df = ime_df.merge(
            wind_df[["plume_id", "wind_alignment_deg", "wind_confidence"]],
            on="plume_id", how="left"
        )
    
    min_wind = CONFIG["min_wind_speed_ms"]
    fallback_tmix = CONFIG["mixing_time_fallback_s"]
    
    emission_results = []
    
    for _, row in ime_df.iterrows():
        # Characteristic plume scale
        L_m = np.sqrt(row["plume_area_km2"] * 1e6)  # km^2 -> m^2 -> sqrt
        
        # Effective wind speed (use ERA5)
        U_eff = row["era5_wind_speed"]
        
        # Wind-dependent mixing time
        if U_eff > min_wind and L_m > 0:
            t_mix = L_m / U_eff
            t_mix_method = "wind_dependent"
        else:
            t_mix = fallback_tmix
            t_mix_method = "fixed_48h_fallback"
        
        # Emission rate
        emission_rate = row["ime_kg"] / t_mix  # kg/s
        
        emission_results.append({
            "plume_id": row["plume_id"],
            "L_m": L_m,
            "U_eff_ms": U_eff,
            "t_mix_s": t_mix,
            "t_mix_method": t_mix_method,
            "emission_rate_kg_s": emission_rate,
            "emission_rate_kg_h": emission_rate * 3600,
            "emission_rate_t_h": emission_rate * 3.6,
        })
    
    emission_df = pd.DataFrame(emission_results)
    ime_df = ime_df.merge(emission_df, on="plume_id", how="left")
    
    print("Emission rates calculated:")
    print(ime_df[["plume_id", "ime_kg", "U_eff_ms", "t_mix_s", 
                   "t_mix_method", "emission_rate_kg_h"]].to_string())
else:
    print("No plumes for emission calculation.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 8: Monte Carlo Uncertainty Estimation (Tier 1):

# CELL ********************

# Propagate uncertainty through: CH4 retrieval noise + wind speed uncertainty
# Output: 5th and 95th percentile emission rate bounds

if len(ime_df) > 0:
    n_mc = CONFIG["mc_samples"]
    wind_unc_frac = CONFIG["wind_uncertainty_fraction"]
    min_wind = CONFIG["min_wind_speed_ms"]
    
    uncertainty_results = []
    
    for _, row in ime_df.iterrows():
        plume_id = row["plume_id"]
        ime_kg = row["ime_kg"]
        U_eff = row["U_eff_ms"]
        L_m = row["L_m"]
        n_pixels = row["n_pixels"]
        mean_enh = row["mean_ch4_enhancement_ppb"]
        
        # Estimate CH4 retrieval noise (~10 ppb per pixel for TROPOMI)
        ch4_noise_ppb = 10.0
        
        mc_rates = []
        for _ in range(n_mc):
            # Perturb IME: add noise to each pixel's enhancement
            # Total IME noise scales as sqrt(n_pixels) * per_pixel_noise
            ime_noise = np.random.normal(0, ch4_noise_ppb * np.sqrt(n_pixels) * PPB_TO_KG)
            perturbed_ime = max(0, ime_kg + ime_noise)
            
            # Perturb wind speed
            wind_noise = np.random.normal(0, U_eff * wind_unc_frac)
            perturbed_wind = max(min_wind, U_eff + wind_noise)
            
            # Compute emission rate
            if L_m > 0 and perturbed_wind > min_wind:
                t_mix = L_m / perturbed_wind
            else:
                t_mix = CONFIG["mixing_time_fallback_s"]
            
            rate = perturbed_ime / t_mix
            mc_rates.append(rate)
        
        mc_rates = np.array(mc_rates)
        
        uncertainty_results.append({
            "plume_id": plume_id,
            "emission_rate_p5_kg_s": np.percentile(mc_rates, 5),
            "emission_rate_p50_kg_s": np.percentile(mc_rates, 50),
            "emission_rate_p95_kg_s": np.percentile(mc_rates, 95),
            "emission_rate_p5_kg_h": np.percentile(mc_rates, 5) * 3600,
            "emission_rate_p50_kg_h": np.percentile(mc_rates, 50) * 3600,
            "emission_rate_p95_kg_h": np.percentile(mc_rates, 95) * 3600,
            "uncertainty_ratio": (
                np.percentile(mc_rates, 95) / np.percentile(mc_rates, 50)
                if np.percentile(mc_rates, 50) > 0 else float("inf")
            ),
        })
    
    unc_df = pd.DataFrame(uncertainty_results)
    ime_df = ime_df.merge(unc_df, on="plume_id", how="left")
    
    print("Uncertainty estimation complete:")
    print(ime_df[["plume_id", "emission_rate_kg_h", 
                   "emission_rate_p5_kg_h", "emission_rate_p50_kg_h",
                   "emission_rate_p95_kg_h", "uncertainty_ratio"]].to_string())
else:
    print("No plumes for uncertainty estimation.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 9: Assign overall confidence:

# CELL ********************

if len(ime_df) > 0:
    def assign_confidence(row):
        # Combine wind alignment and uncertainty into overall confidence
        wind_conf = row.get("wind_confidence", "low")
        unc_ratio = row.get("uncertainty_ratio", float("inf"))
        n_pixels = row.get("n_pixels", 0)
        
        score = 0
        
        # Wind alignment
        if wind_conf == "high":
            score += 2
        elif wind_conf == "medium":
            score += 1
        
        # Uncertainty (lower ratio = more certain)
        if unc_ratio < 2.0:
            score += 2
        elif unc_ratio < 3.0:
            score += 1
        
        # Pixel count (more pixels = more robust)
        if n_pixels >= 5:
            score += 1
        
        if score >= 4:
            return "high"
        elif score >= 2:
            return "medium"
        else:
            return "low"
    
    ime_df["confidence"] = ime_df.apply(assign_confidence, axis=1)
    
    print("Confidence distribution:")
    print(ime_df["confidence"].value_counts().to_string())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 10: Write Gold table:

# CELL ********************

if len(ime_df) > 0:
    # Select final columns for gold_plume_catalog
    gold_columns = [
        "plume_id", "scene_id", "stac_id", "detection_date",
        "source_lat", "source_lon",
        "n_pixels", "plume_area_km2",
        "peak_ch4_enhancement_ppb", "mean_ch4_enhancement_ppb",
        "ime_kg",
        "emission_rate_kg_s", "emission_rate_kg_h", "emission_rate_t_h",
        "emission_rate_p5_kg_s", "emission_rate_p50_kg_s", "emission_rate_p95_kg_s",
        "emission_rate_p5_kg_h", "emission_rate_p50_kg_h", "emission_rate_p95_kg_h",
        "t_mix_s", "t_mix_method",
        "U_eff_ms", "L_m",
        "wind_alignment_deg", "wind_confidence",
        "uncertainty_ratio", "confidence",
    ]
    
    # Only include columns that exist
    available_columns = [c for c in gold_columns if c in ime_df.columns]
    gold_pdf = ime_df[available_columns]
    
    gold_spark = spark.createDataFrame(gold_pdf)
    
    gold_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("gold_plume_catalog")
    
    print(f"Written {len(gold_pdf)} plumes to gold_plume_catalog")
    
    # Also write flagged large clusters
    if flagged_large:
        flagged_pdf = pd.concat(flagged_large, ignore_index=True)
        flagged_spark = spark.createDataFrame(
            flagged_pdf[["plume_id", "scene_id", "latitude", "longitude",
                         "ch4", "ch4_enhancement", "plume_flag", "aspect_ratio"]]
        )
        flagged_spark.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("gold_flagged_large_clusters")
        print(f"Written {len(flagged_pdf)} pixels in {len(flagged_large)} flagged large clusters")
    else:
        print("No large clusters flagged")
else:
    print("WARNING: No plumes detected. Gold table not written.")
    print("Consider adjusting detection thresholds in 00_config:")
    print(f"  Current MAD sigma: {CONFIG['mad_sigma']}")
    print(f"  Current enhancement floor: {CONFIG['enhancement_floor_ppb']} ppb")
    print(f"  Current min cluster size: {CONFIG['min_cluster_pixels']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation summary:

# CELL ********************

if len(ime_df) > 0:
    print("=" * 60)
    print("GREEN SKY DETECTION SUMMARY")
    print("=" * 60)
    print(f"Input pixels (Permian Basin): {total_pixels:,}")
    print(f"Scenes processed: {silver_pdf['scene_id'].nunique()}")
    print(f"Candidate pixels: {total_candidates}")
    print(f"Valid plumes detected: {len(ime_df)}")
    print(f"Flagged large clusters: {len(flagged_large)}")
    print()
    print("--- Emission Rate Summary ---")
    print(f"  Min:    {ime_df['emission_rate_kg_h'].min():.1f} kg/h")
    print(f"  Median: {ime_df['emission_rate_kg_h'].median():.1f} kg/h")
    print(f"  Mean:   {ime_df['emission_rate_kg_h'].mean():.1f} kg/h")
    print(f"  Max:    {ime_df['emission_rate_kg_h'].max():.1f} kg/h")
    print()
    print("--- T_mix Method ---")
    print(ime_df["t_mix_method"].value_counts().to_string())
    print()
    print("--- Confidence ---")
    print(ime_df["confidence"].value_counts().to_string())
    print()
    print("--- Uncertainty (p95/p50 ratio) ---")
    print(f"  Min:    {ime_df['uncertainty_ratio'].min():.2f}")
    print(f"  Median: {ime_df['uncertainty_ratio'].median():.2f}")
    print(f"  Max:    {ime_df['uncertainty_ratio'].max():.2f}")
    print()
    
    # Physical plausibility check
    print("--- Plausibility Checks ---")
    implausible = ime_df[ime_df["emission_rate_t_h"] > 100]
    if len(implausible) > 0:
        print(f"  WARNING: {len(implausible)} plumes with rate > 100 t/h (very large)")
    else:
        print("  All emission rates < 100 t/h (plausible)")
    
    low_wind = ime_df[ime_df["t_mix_method"] == "fixed_48h_fallback"]
    if len(low_wind) > 0:
        print(f"  {len(low_wind)} plumes used 48h fallback (low wind)")
    else:
        print("  All plumes used wind-dependent T_mix")
    
    print()
    print("--- Full Plume Catalog ---")
    display_cols = ["plume_id", "source_lat", "source_lon", "n_pixels",
                    "ime_kg", "emission_rate_kg_h", "emission_rate_p5_kg_h",
                    "emission_rate_p95_kg_h", "confidence"]
    available = [c for c in display_cols if c in ime_df.columns]
    print(ime_df[available].to_string())
else:
    print("No plumes detected in this dataset.")
    print("This could mean:")
    print("  1. No significant methane enhancements in this time/area")
    print("  2. Detection thresholds too strict")
    print("  3. Data quality issue")
    print()
    print("Debug info:")
    print(f"  Total pixels: {total_pixels}")
    print(f"  Candidate pixels: {total_candidates}")
    print(f"  Enhancement range: {enhanced_pdf['ch4_enhancement'].min():.1f} to {enhanced_pdf['ch4_enhancement'].max():.1f} ppb")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 
