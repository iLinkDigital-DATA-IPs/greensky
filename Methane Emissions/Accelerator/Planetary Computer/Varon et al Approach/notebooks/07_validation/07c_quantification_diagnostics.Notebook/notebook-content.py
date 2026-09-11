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

# # 07c — Quantification Diagnostics
#
# Read-only diagnostic notebook. **Does not write any tables** — every cell only prints or
# plots.
#
# `07b_detection_diagnostics` established that the CH₄ enhancements in `gold_plume_catalog`
# are real (mean enhancement above the TROPOMI noise floor), that the discarded large
# clusters are not the missing super-emitters, and that emission rate is nearly insensitive
# to `mad_sigma` (plume count moves ~44x across sigma 2→5 while median rate moves only
# ~1.6x). That points at the quantification path, not the detection threshold, as the
# source of the ~100x gap against Carbon Mapper. This notebook tests two hypotheses about
# where in that path it lives: (1) plume geometry / background contamination, and (2) the
# characteristic length scale `L` used in `T_mix = L / U_eff`.
#
# **Inputs:** `gold_plume_catalog`, `silver_plume_ready_pixels`, `validation_carbon_mapper_plumes`
#
# **Output:** none.

# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Imports, shared constants, and load Gold/Silver tables:

# CELL ********************

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.spatial import cKDTree
from collections import defaultdict

# ── IME conversion factor, now supplied by 00_config (%run above): PIXEL_AREA_M2,
# AVOGADRO, M_CH4, M_AIR, DRY_AIR_COLUMN_PER_CM2, DRY_AIR_COLUMN_PER_M2 and PPB_TO_KG.
# These used to be copy-pasted here from 04_derive_emissions. That duplication is how a
# cm^2/m^2 unit error -- which made every emission rate low by a factor of 10,000 --
# came to sit in three notebooks at once; see the unit-error note in 00_config. ──

print(f"PPB_TO_KG (from 00_config): {PPB_TO_KG:.3f} kg CH4 per ppb per pixel")

# ── Haversine distance, duplicated from 04_derive_emissions.Notebook (Step 4: Plume
# Clustering). Must stay in sync with that notebook. ──
def haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance in km between two points (lat2/lon2 may be arrays)."""
    R = 6371.0
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

# ── Load Gold + Silver tables (read-only) ──
plumes_pdf = spark.table("gold_plume_catalog").toPandas()
print(f"gold_plume_catalog: {len(plumes_pdf)} plumes, {len(plumes_pdf.columns)} columns")

silver_pdf = spark.table("silver_plume_ready_pixels").toPandas()
print(f"silver_plume_ready_pixels: {len(silver_pdf):,} pixels")

try:
    cm_pdf = spark.table("validation_carbon_mapper_plumes").toPandas()
    print(f"validation_carbon_mapper_plumes: {len(cm_pdf)} plumes")
except Exception:
    cm_pdf = pd.DataFrame()
    print("validation_carbon_mapper_plumes: table not found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Shared computation — scene separation, both background methods, and plume-membership
# ### reconstruction (computed once here, cached, and reused by every cell below)
#
# Reproduces 04_derive_emissions Steps 1-4 (scene separation, kNN background, MAD candidate
# detection, Union-Find clustering) against `silver_plume_ready_pixels`, all duplicated from
# that notebook and flagged inline — must stay in sync with it. Also computes an alternative
# annulus background per pixel (Cell 2's hypothesis). Reconstructed clusters are matched back
# to `gold_plume_catalog` rows by `(scene_id, source_lat, source_lon)` rather than by
# `plume_id`, because `plume_id` is an unstable iteration-order counter (see `CLAUDE.md`
# "Known issues") and is not safe to use as a join key across separate runs.

# CELL ********************

# ── Step 1 (duplicated from 04_derive_emissions): scene separation ──
scene_gap_seconds = CONFIG["scene_gap_minutes"] * 60

stac_times_pdf = (
    silver_pdf.groupby("stac_id")
    .agg(scene_start=("datetime", "min"), scene_end=("datetime", "max"), pixel_count=("datetime", "size"))
    .reset_index()
    .sort_values("scene_start")
    .reset_index(drop=True)
)
stac_times_pdf["time_gap_s"] = (
    stac_times_pdf["scene_start"] - stac_times_pdf["scene_end"].shift(1)
).dt.total_seconds()
stac_times_pdf["new_scene"] = (
    stac_times_pdf["time_gap_s"].isna() | (stac_times_pdf["time_gap_s"] > scene_gap_seconds)
)
stac_times_pdf["scene_id"] = stac_times_pdf["new_scene"].cumsum()

diag_pixels = silver_pdf.merge(stac_times_pdf[["stac_id", "scene_id"]], on="stac_id", how="inner")
print(f"Scenes reproduced from silver_plume_ready_pixels: {diag_pixels['scene_id'].nunique()}")


# ── Step 2 (duplicated from 04_derive_emissions): kNN background estimation ──
def estimate_background_knn(scene_pdf, n_neighbors=None, percentile=None):
    if n_neighbors is None:
        n_neighbors = CONFIG["background_neighbors"]
    if percentile is None:
        percentile = CONFIG["background_percentile"]

    coords = scene_pdf[["latitude", "longitude"]].values
    ch4_vals = scene_pdf["ch4"].values

    if len(coords) < n_neighbors + 1:
        return np.full(len(coords), np.median(ch4_vals))

    tree = cKDTree(coords)
    _, indices = tree.query(coords, k=min(n_neighbors + 1, len(coords)))

    bg = np.zeros(len(coords))
    for i in range(len(coords)):
        neighbor_ch4 = ch4_vals[indices[i][1:]]
        bg[i] = np.percentile(neighbor_ch4, percentile * 100)
    return bg


# ── Alternative background hypothesis (Cell 2): annulus, 25-100 km, excluding anything
# closer. Same 15th-percentile-of-ch4 approach as the kNN method (CONFIG['background_percentile'])
# so the two are comparable — only the neighbourhood definition changes. Local flat-earth
# projection (deg -> km via 111.0 / 94.0) matches the approximation already used throughout
# 03_join_data.Notebook and 04_derive_emissions.Notebook. ──
def estimate_background_annulus(scene_pdf, inner_km=25.0, outer_km=100.0, percentile=None):
    if percentile is None:
        percentile = CONFIG["background_percentile"]

    lat_km = scene_pdf["latitude"].values * 111.0
    lon_km = scene_pdf["longitude"].values * 94.0
    coords_km = np.column_stack([lat_km, lon_km])
    ch4_vals = scene_pdf["ch4"].values

    tree = cKDTree(coords_km)
    bg = np.full(len(coords_km), np.nan)
    for i in range(len(coords_km)):
        idx_outer = [j for j in tree.query_ball_point(coords_km[i], r=outer_km) if j != i]
        if not idx_outer:
            bg[i] = np.median(ch4_vals)
            continue
        cand = np.array(idx_outer)
        d = np.sqrt(((coords_km[cand] - coords_km[i]) ** 2).sum(axis=1))
        ring = cand[(d >= inner_km) & (d <= outer_km)]
        bg[i] = np.percentile(ch4_vals[ring], percentile * 100) if len(ring) > 0 else np.median(ch4_vals)
    return bg


enhanced_rows = []
for scene_id, scene_group in diag_pixels.groupby("scene_id"):
    scene_group = scene_group.copy()
    scene_group["ch4_background_knn"] = estimate_background_knn(scene_group)
    scene_group["ch4_enhancement_knn"] = scene_group["ch4"] - scene_group["ch4_background_knn"]
    scene_group["ch4_background_annulus"] = estimate_background_annulus(scene_group)
    scene_group["ch4_enhancement_annulus"] = scene_group["ch4"] - scene_group["ch4_background_annulus"]
    enhanced_rows.append(scene_group)

enhanced_diag = pd.concat(enhanced_rows, ignore_index=True)
print(f"Both backgrounds estimated for {len(enhanced_diag):,} pixels across "
      f"{enhanced_diag['scene_id'].nunique()} scenes")


# ── Steps 3-4 (duplicated from 04_derive_emissions): MAD candidate detection + Union-Find
# clustering, run once at CONFIG['mad_sigma'] (the value actually used to produce
# gold_plume_catalog) to reconstruct plume membership. ──
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


mad_sigma = CONFIG["mad_sigma"]
enhancement_floor = CONFIG["enhancement_floor_ppb"]
cluster_radius = CONFIG["cluster_radius_km"]
min_pixels = CONFIG["min_cluster_pixels"]
max_pixels = CONFIG["max_cluster_pixels"]
shape_threshold = CONFIG["shape_threshold"]

candidate_rows = []
for scene_id, scene_group in enhanced_diag.groupby("scene_id"):
    enhancements = scene_group["ch4_enhancement_knn"].values
    median_enh = np.median(enhancements)
    mad_scaled = np.median(np.abs(enhancements - median_enh)) * 1.4826
    threshold = max(mad_sigma * mad_scaled, enhancement_floor)

    scene_group = scene_group.copy()
    scene_group["is_candidate"] = scene_group["ch4_enhancement_knn"] > threshold
    candidate_rows.append(scene_group)

detected_diag = pd.concat(candidate_rows, ignore_index=True)
candidates_only = detected_diag[detected_diag["is_candidate"]].copy()

reconstructed_clusters = {}  # (scene_id, source_lat, source_lon) -> member pixel DataFrame
n_reconstructed = 0

for scene_id, scene_candidates in candidates_only.groupby("scene_id"):
    if len(scene_candidates) < min_pixels:
        continue
    coords = scene_candidates[["latitude", "longitude"]].values
    n = len(coords)
    uf = UnionFind(n)
    for i in range(n):
        for j in range(i + 1, n):
            if haversine_km(coords[i, 0], coords[i, 1], coords[j, 0], coords[j, 1]) <= cluster_radius:
                uf.union(i, j)

    clusters = defaultdict(list)
    for i in range(n):
        clusters[uf.find(i)].append(i)

    for member_indices in clusters.values():
        cluster_size = len(member_indices)
        if cluster_size < min_pixels or cluster_size > max_pixels:
            continue

        cluster_data = scene_candidates.iloc[member_indices]
        lats, lons = cluster_data["latitude"].values, cluster_data["longitude"].values
        if cluster_size >= 2:
            lat_range = lats.max() - lats.min()
            lon_range = (lons.max() - lons.min()) * np.cos(np.radians(lats.mean()))
            aspect_ratio = (
                max(lat_range, lon_range) / min(lat_range, lon_range)
                if min(lat_range, lon_range) > 0 else float("inf")
            )
            if aspect_ratio > shape_threshold:
                continue

        peak_idx = cluster_data["ch4_enhancement_knn"].values.argmax()
        source_lat = round(float(cluster_data.iloc[peak_idx]["latitude"]), 6)
        source_lon = round(float(cluster_data.iloc[peak_idx]["longitude"]), 6)
        reconstructed_clusters[(scene_id, source_lat, source_lon)] = cluster_data.copy()
        n_reconstructed += 1

print(f"Reconstructed {n_reconstructed} clusters passing size/shape filters at mad_sigma={mad_sigma}")

# ── Match gold_plume_catalog rows to reconstructed clusters ──
plume_pixel_map = {}
unmatched_plume_ids = []
for _, gp in plumes_pdf.iterrows():
    key = (gp["scene_id"], round(float(gp["source_lat"]), 6), round(float(gp["source_lon"]), 6))
    if key in reconstructed_clusters:
        plume_pixel_map[gp["plume_id"]] = reconstructed_clusters[key]
    else:
        unmatched_plume_ids.append(gp["plume_id"])

print(f"Matched {len(plume_pixel_map)} / {len(plumes_pdf)} gold_plume_catalog plumes to "
      f"reconstructed member pixels")
if unmatched_plume_ids:
    print(f"WARNING: unmatched plume_ids (excluded from Cells 1, 2, 3, 5): {unmatched_plume_ids}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 1 — Plume geometry audit
#
# For each reconstructed accepted plume: pixel count, maximum pairwise distance between
# member pixels, mean nearest-neighbour distance between member pixels, the number of pixel
# pairs closer than 8 km (a proxy for true adjacency given ~5.5 km TROPOMI pixel spacing),
# and `L_m` as currently computed by `sqrt(n_pixels * pixel_area)`. The question: are these
# contiguous plumes, or scattered pixels linked only by the 12 km connection radius?

# CELL ********************

geometry_rows = []
for plume_id, pix in plume_pixel_map.items():
    n_pixels = len(pix)
    lats, lons = pix["latitude"].values, pix["longitude"].values

    pair_dists = [
        haversine_km(lats[i], lons[i], lats[j], lons[j])
        for i in range(n_pixels) for j in range(i + 1, n_pixels)
    ]
    pair_dists = np.array(pair_dists) if pair_dists else np.array([0.0])

    if n_pixels > 1:
        nn_dists = [
            min(haversine_km(lats[i], lons[i], lats[j], lons[j]) for j in range(n_pixels) if j != i)
            for i in range(n_pixels)
        ]
        mean_nn_km = float(np.mean(nn_dists))
    else:
        mean_nn_km = 0.0

    plume_area_km2 = n_pixels * (5.5 * 7.0)
    L_m_current = float(np.sqrt(plume_area_km2 * 1e6))

    geometry_rows.append({
        "plume_id": plume_id,
        "n_pixels": n_pixels,
        "max_pairwise_dist_km": round(float(pair_dists.max()), 2),
        "mean_nn_dist_km": round(mean_nn_km, 2),
        "pixel_pairs_under_8km": int((pair_dists <= 8.0).sum()) if n_pixels > 1 else 0,
        "total_pixel_pairs": len(pair_dists) if n_pixels > 1 else 0,
        "L_m_current": round(L_m_current, 1),
    })

geometry_df = pd.DataFrame(geometry_rows).sort_values("plume_id").reset_index(drop=True)
print(f"Plume geometry reconstructed for {len(geometry_df)} plumes")
print(geometry_df.to_string(index=False))

print()
print("=== Distribution of mean nearest-neighbour distance (km) across plumes ===")
print(geometry_df["mean_nn_dist_km"].describe().to_string())

fig, ax = plt.subplots(figsize=(9, 5))
ax.hist(geometry_df["mean_nn_dist_km"], bins=20, color="#55a868", edgecolor="white")
ax.axvline(5.5, color="black", linestyle="--", label="~TROPOMI along-track pixel spacing (5.5 km)")
ax.set_xlabel("Mean nearest-neighbour distance within plume (km)")
ax.set_ylabel("Number of plumes")
ax.set_title("Plume geometry — contiguous plumes vs scattered pixels")
ax.legend()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 2 — Background method comparison
#
# The current `estimate_background` takes the 15th percentile of the 30 nearest neighbours.
# With ~5.5 km pixel spacing that neighbourhood spans roughly 30 km — comparable to the
# plume itself — so plume pixels may be contaminating their own background. Compares against
# an annulus background (15th percentile of same-scene pixels 25-100 km away, computed in
# the shared cell above) for the pixels belonging to accepted plumes only.

# CELL ********************

matched_keys = set()
for pix in plume_pixel_map.values():
    for lat, lon in zip(pix["latitude"].round(6), pix["longitude"].round(6)):
        matched_keys.add((lat, lon))

plume_member_pixels = enhanced_diag[
    enhanced_diag.apply(
        lambda r: (round(r["latitude"], 6), round(r["longitude"], 6)) in matched_keys, axis=1
    )
].copy()

print(f"Accepted-plume member pixels: {len(plume_member_pixels)}")
print()
print("=== Background method comparison (accepted-plume pixels only) ===")
print(f"Mean kNN background (15th pct of 30 nearest neighbours): "
      f"{plume_member_pixels['ch4_background_knn'].mean():.2f} ppb")
print(f"Mean annulus background (15th pct, 25-100 km ring):      "
      f"{plume_member_pixels['ch4_background_annulus'].mean():.2f} ppb")
print(f"Mean difference (annulus - kNN):                          "
      f"{(plume_member_pixels['ch4_background_annulus'] - plume_member_pixels['ch4_background_knn']).mean():.2f} ppb")
print()
print(f"Mean enhancement under kNN background:     {plume_member_pixels['ch4_enhancement_knn'].mean():.2f} ppb")
print(f"Mean enhancement under annulus background: {plume_member_pixels['ch4_enhancement_annulus'].mean():.2f} ppb")

fig, ax = plt.subplots(figsize=(9, 5))
ax.hist(plume_member_pixels["ch4_enhancement_knn"], bins=20, alpha=0.5,
        label="kNN background", color="#4c72b0")
ax.hist(plume_member_pixels["ch4_enhancement_annulus"], bins=20, alpha=0.5,
        label="Annulus background (25-100km)", color="#c44e52")
ax.set_xlabel("Per-pixel CH4 enhancement (ppb)")
ax.set_ylabel("Number of pixels")
ax.set_title("Accepted-plume pixels — enhancement under kNN vs annulus background")
ax.legend()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 3 — Rate recomputation under each hypothesis
#
# Recomputes `ime_kg` and `emission_rate_kg_h` for all reconstructed plumes under four
# combinations of background method and `L`: (a) current kNN background, current
# `L = sqrt(n_pixels * pixel_area)`; (b) annulus background, current `L`; (c) kNN
# background, `L` = maximum pairwise distance between member pixels; (d) annulus background,
# max-pairwise `L`. `U_eff` and the Varon `T_mix = L / U_eff` formula are unchanged from
# 04_derive_emissions Step 7 throughout — only background and `L` vary.

# CELL ********************

min_wind = CONFIG["min_wind_speed_ms"]
fallback_tmix = CONFIG["mixing_time_fallback_s"]


def compute_rate(pix, enhancement_col, L_mode):
    """Recompute IME + emission rate for one plume's member pixels under a chosen
    background method (enhancement_col) and L definition (L_mode). U_eff and the
    T_mix = L / U_eff formula are the same as 04_derive_emissions Step 7."""
    n_pixels = len(pix)
    ime_kg = float(np.sum(pix[enhancement_col].values * PPB_TO_KG))

    if L_mode == "sqrt_area":
        plume_area_km2 = n_pixels * (5.5 * 7.0)
        L_m = float(np.sqrt(plume_area_km2 * 1e6))
    else:  # "max_pairwise"
        lats, lons = pix["latitude"].values, pix["longitude"].values
        if n_pixels > 1:
            pair_dists = [
                haversine_km(lats[i], lons[i], lats[j], lons[j])
                for i in range(n_pixels) for j in range(i + 1, n_pixels)
            ]
            L_m = float(max(pair_dists) * 1000.0)  # km -> m
        else:
            L_m = float(np.sqrt((5.5 * 7.0) * 1e6))

    mean_u, mean_v = pix["era5_u10"].mean(), pix["era5_v10"].mean()
    U_eff = float(np.sqrt(mean_u ** 2 + mean_v ** 2)) if pd.notna(mean_u) and pd.notna(mean_v) else np.nan

    if pd.notna(U_eff) and U_eff > min_wind and L_m > 0:
        t_mix = L_m / U_eff
    else:
        t_mix = fallback_tmix

    return ime_kg, L_m, (ime_kg / t_mix) * 3600


COMBINATIONS = [
    ("a_knn_sqrtarea",     "ch4_enhancement_knn",     "sqrt_area"),
    ("b_annulus_sqrtarea", "ch4_enhancement_annulus", "sqrt_area"),
    ("c_knn_maxpair",      "ch4_enhancement_knn",     "max_pairwise"),
    ("d_annulus_maxpair",  "ch4_enhancement_annulus", "max_pairwise"),
]

recompute_rows = []
for plume_id, pix in plume_pixel_map.items():
    row = {"plume_id": plume_id}
    for label, enh_col, l_mode in COMBINATIONS:
        _, _, rate_kgh = compute_rate(pix, enh_col, l_mode)
        row[f"{label}_rate_kg_h"] = rate_kgh
    recompute_rows.append(row)

recompute_df = pd.DataFrame(recompute_rows)
base_median = recompute_df["a_knn_sqrtarea_rate_kg_h"].median()

summary_rows = []
for label, _, _ in COMBINATIONS:
    col = f"{label}_rate_kg_h"
    summary_rows.append({
        "combination": label,
        "median_kg_h": round(recompute_df[col].median(), 3),
        "max_kg_h": round(recompute_df[col].max(), 2),
        "median_ratio_to_a": round(recompute_df[col].median() / base_median, 3) if base_median else None,
    })

summary_df = pd.DataFrame(summary_rows)
print(f"Recomputed rates for {len(recompute_df)} / {len(plumes_pdf)} accepted plumes")
print()
print("a = current kNN background, current L = sqrt(n_pixels * pixel_area)")
print("b = annulus background (25-100km), current L")
print("c = current kNN background, L = max pairwise distance between member pixels")
print("d = annulus background, L = max pairwise distance")
print()
print("=== Emission rate under each background/L combination ===")
print(summary_df.to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 4 — Matched-pair comparison against Carbon Mapper
#
# Spatial matching (nearest Carbon Mapper plume within 5 km, regardless of date) is
# duplicated from `07b_detection_diagnostics.Notebook` Cell 4 — must stay in sync with that
# notebook, since notebooks don't share in-memory state and each run recomputes its own
# match set. For each of the four combinations from Cell 3, reports the median ratio of
# Carbon Mapper rate to Green Sky rate over the matched pairs.
#
# **Note:** this matching has no date constraint, and Carbon Mapper's aircraft and EMIT
# instruments have far lower detection limits than TROPOMI, so exact agreement is not the
# target — closing the gap from ~157x to single digits or low tens would be.

# CELL ********************

MATCH_RADIUS_KM = 5.0
matches = []

if cm_pdf.empty or "cm_emission_rate" not in cm_pdf.columns:
    print("validation_carbon_mapper_plumes not available or missing cm_emission_rate — skipping.")
else:
    cm_matchable = cm_pdf.dropna(subset=["cm_lat", "cm_lon"])
    for _, gp in plumes_pdf.iterrows():
        if len(cm_matchable) == 0:
            break
        dists = haversine_km(
            gp["source_lat"], gp["source_lon"],
            cm_matchable["cm_lat"].values, cm_matchable["cm_lon"].values
        )
        min_idx = int(np.argmin(dists))
        min_dist = float(dists[min_idx])
        if min_dist <= MATCH_RADIUS_KM:
            cm_row = cm_matchable.iloc[min_idx]
            matches.append({
                "plume_id": gp["plume_id"],
                "cm_emission_rate_kg_h": cm_row["cm_emission_rate"],
                "distance_km": round(min_dist, 2),
            })

matches_df = pd.DataFrame(matches)
print(f"Green Sky plumes with a Carbon Mapper match within {MATCH_RADIUS_KM:.0f} km "
      f"(any date): {len(matches_df)} / {len(plumes_pdf)}")

if len(matches_df) == 0:
    print("No spatial matches — skipping ratio comparison.")
else:
    merged = matches_df.merge(recompute_df, on="plume_id", how="left")

    ratio_rows = []
    for label, _, _ in COMBINATIONS:
        col = f"{label}_rate_kg_h"
        valid = merged.dropna(subset=[col, "cm_emission_rate_kg_h"])
        valid = valid[valid[col] > 0]
        ratio = valid["cm_emission_rate_kg_h"] / valid[col]
        ratio_rows.append({
            "combination": label,
            "n_matched_pairs": len(valid),
            "median_cm_over_gs_ratio": round(ratio.median(), 1) if len(valid) else None,
        })

    ratio_df = pd.DataFrame(ratio_rows)
    print()
    print("=== Median (Carbon Mapper rate / Green Sky rate) for matched pairs, per combination ===")
    print(ratio_df.to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 5 — Single-plume deep dive
#
# Picks the accepted plume with the highest `emission_rate_kg_h`. Plots its scene's raw CH₄
# field as a scatter over lat/lon coloured by value, with the plume's member pixels marked,
# alongside enhancement under the kNN background and under the annulus background on a
# shared colour scale. For visual judgement of whether the enhancement pattern looks like a
# coherent downwind plume or a scattered set of noisy pixels.

# CELL ********************

top_plume = plumes_pdf.loc[plumes_pdf["emission_rate_kg_h"].idxmax()]
top_plume_id = top_plume["plume_id"]
top_scene_id = top_plume["scene_id"]

print(f"Deep-diving plume_id={top_plume_id} (scene_id={top_scene_id}), "
      f"emission_rate_kg_h={top_plume['emission_rate_kg_h']:.2f}, n_pixels={top_plume['n_pixels']}")

if top_plume_id not in plume_pixel_map:
    print("Could not locate reconstructed member pixels for this plume (see match warning above).")
else:
    scene_pixels = enhanced_diag[enhanced_diag["scene_id"] == top_scene_id].copy()
    member_pixels = plume_pixel_map[top_plume_id]
    member_keys = set(zip(member_pixels["latitude"].round(6), member_pixels["longitude"].round(6)))
    is_member = scene_pixels.apply(
        lambda r: (round(r["latitude"], 6), round(r["longitude"], 6)) in member_keys, axis=1
    )

    enh_vmin = min(scene_pixels["ch4_enhancement_knn"].min(), scene_pixels["ch4_enhancement_annulus"].min())
    enh_vmax = max(scene_pixels["ch4_enhancement_knn"].max(), scene_pixels["ch4_enhancement_annulus"].max())

    fig, axes = plt.subplots(1, 3, figsize=(20, 6))

    sc0 = axes[0].scatter(scene_pixels["longitude"], scene_pixels["latitude"],
                           c=scene_pixels["ch4"], cmap="viridis", s=25)
    axes[0].scatter(scene_pixels.loc[is_member, "longitude"], scene_pixels.loc[is_member, "latitude"],
                     facecolors="none", edgecolors="red", s=90, linewidths=1.5, label="plume member pixels")
    axes[0].set_title(f"Raw CH4 (ppb) — scene {top_scene_id}")
    axes[0].legend(loc="upper right")
    fig.colorbar(sc0, ax=axes[0])

    sc1 = axes[1].scatter(scene_pixels["longitude"], scene_pixels["latitude"],
                           c=scene_pixels["ch4_enhancement_knn"], cmap="magma", s=25,
                           vmin=enh_vmin, vmax=enh_vmax)
    axes[1].scatter(scene_pixels.loc[is_member, "longitude"], scene_pixels.loc[is_member, "latitude"],
                     facecolors="none", edgecolors="cyan", s=90, linewidths=1.5)
    axes[1].set_title("Enhancement — kNN background")
    fig.colorbar(sc1, ax=axes[1])

    sc2 = axes[2].scatter(scene_pixels["longitude"], scene_pixels["latitude"],
                           c=scene_pixels["ch4_enhancement_annulus"], cmap="magma", s=25,
                           vmin=enh_vmin, vmax=enh_vmax)
    axes[2].scatter(scene_pixels.loc[is_member, "longitude"], scene_pixels.loc[is_member, "latitude"],
                     facecolors="none", edgecolors="cyan", s=90, linewidths=1.5)
    axes[2].set_title("Enhancement — annulus background (25-100km)")
    fig.colorbar(sc2, ax=axes[2])

    for ax in axes:
        ax.set_xlabel("longitude")
        ax.set_ylabel("latitude")

    plt.tight_layout()
    plt.show()

    print()
    print("=== Member pixel coordinates and values ===")
    display_cols = ["latitude", "longitude", "ch4", "ch4_enhancement_knn", "ch4_enhancement_annulus"]
    print(
        member_pixels[display_cols]
        .sort_values("ch4_enhancement_knn", ascending=False)
        .round(3)
        .to_string(index=False)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 6 — Findings (fill in)
#
# _To be completed after reviewing Cells 1-5._
#
# 1. **Geometry (Cell 1).** Are the 109 plumes spatially contiguous (mean nearest-neighbour
#    distance well under the 12 km connection radius, most pixel pairs under 8 km), or are
#    they scattered pixels stitched together by the connection radius alone?
#
# 2. **Background contamination (Cell 2).** Does the annulus background differ meaningfully
#    from the kNN background for plume pixels? If the kNN background is pulling in plume
#    pixels themselves, kNN enhancement should be systematically lower than annulus
#    enhancement — is that what the histogram shows?
#
# 3. **Rate sensitivity (Cell 3).** Which single change — background method or `L` — moves
#    the median/max emission rate the most? Is either hypothesis, alone or combined,
#    large enough to matter against a ~100x gap?
#
# 4. **Matched-pair ratio (Cell 4).** Does any combination bring the median Carbon
#    Mapper/Green Sky ratio down meaningfully from the ~157x baseline? Into the "single
#    digits to low tens" range that would count as closing the gap?
#
# 5. **Visual check (Cell 5).** Does the highest-rate plume look like a coherent downwind
#    plume under either background method, or does it look like noise that happened to
#    cluster?
#
# 6. **Overall verdict:** _______________
