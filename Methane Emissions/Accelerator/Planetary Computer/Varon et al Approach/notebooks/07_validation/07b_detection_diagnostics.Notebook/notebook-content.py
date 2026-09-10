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

# # 07b — Detection Diagnostics
#
# Read-only diagnostic notebook. **Does not write any tables** — every cell only prints or
# displays.
#
# Answers one question: are the 109 plumes in `gold_plume_catalog` real methane point
# sources, or artefacts sitting at the TROPOMI instrument noise floor?
#
# **Inputs:** `gold_plume_catalog`, `gold_flagged_large_clusters`, `silver_plume_ready_pixels`,
# `validation_cams_plumes`, `validation_carbon_mapper_plumes`
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

# ── IME conversion factor, duplicated from 04_derive_emissions.Notebook (Step 6: IME
# Calculation). Must stay in sync with that notebook — if the pixel area, dry-air column,
# or molecular weights change there, they need to change here too. ──
PIXEL_AREA_M2 = 5500.0 * 7000.0   # TROPOMI pixel area, 5.5 km x 7.0 km (pre-Aug-2019 value)
DRY_AIR_COLUMN = 2.12e25          # molecules/m^2
AVOGADRO = 6.022e23
M_CH4 = 16.04e-3                  # kg/mol
PPB_TO_KG = 1e-9 * (DRY_AIR_COLUMN / AVOGADRO) * M_CH4 * PIXEL_AREA_M2

print(f"PPB_TO_KG (duplicated from 04_derive_emissions): {PPB_TO_KG:.6f} kg CH4 per ppb per pixel")

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

try:
    flagged_pdf = spark.table("gold_flagged_large_clusters").toPandas()
    n_flagged_clusters = flagged_pdf["plume_id"].nunique() if not flagged_pdf.empty else 0
    print(f"gold_flagged_large_clusters: {len(flagged_pdf)} pixels, {n_flagged_clusters} clusters")
except Exception:
    flagged_pdf = pd.DataFrame()
    print("gold_flagged_large_clusters: table not found")

silver_pdf = spark.table("silver_plume_ready_pixels").toPandas()
print(f"silver_plume_ready_pixels: {len(silver_pdf):,} pixels")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 1 — The discarded large clusters
#
# `gold_flagged_large_clusters` holds clusters rejected by `max_cluster_pixels` (see
# `CONFIG['max_cluster_pixels']` in 00_config) in 04_derive_emissions. Hypothesis under
# test: the real super-emitters are being discarded by the pixel cap, not the small
# artefacts that end up in `gold_plume_catalog`.
#
# `gold_flagged_large_clusters` does not carry ERA5 wind columns (dropped at write time in
# 04_derive_emissions Step 10) — they are recovered here via an exact-match join back to
# `silver_plume_ready_pixels` on `(latitude, longitude)`, the same pixel identity used
# throughout the pipeline.

# CELL ********************

if flagged_pdf.empty:
    print("No flagged large clusters found in gold_flagged_large_clusters — nothing to check.")
else:
    wind_lookup = silver_pdf[["latitude", "longitude", "era5_u10", "era5_v10"]].drop_duplicates(
        subset=["latitude", "longitude"]
    )
    flagged_with_wind = flagged_pdf.merge(wind_lookup, on=["latitude", "longitude"], how="left")

    min_wind = CONFIG["min_wind_speed_ms"]
    fallback_tmix = CONFIG["mixing_time_fallback_s"]

    cluster_rows = []
    for plume_id, grp in flagged_with_wind.groupby("plume_id"):
        n_pixels = len(grp)
        mean_enh = grp["ch4_enhancement"].mean()
        max_enh = grp["ch4_enhancement"].max()

        # Spatial extent — same deg->km conversion as 04_derive_emissions Steps 4-5
        lat_range_km = (grp["latitude"].max() - grp["latitude"].min()) * 111.0
        lon_range_km = (grp["longitude"].max() - grp["longitude"].min()) * 94.0
        extent_km = float(np.sqrt(lat_range_km**2 + lon_range_km**2))

        # IME + T_mix, same logic as 04_derive_emissions Steps 6-7
        ime_kg = float((grp["ch4_enhancement"] * PPB_TO_KG).sum())
        plume_area_km2 = n_pixels * (5.5 * 7.0)
        L_m = float(np.sqrt(plume_area_km2 * 1e6))

        mean_u, mean_v = grp["era5_u10"].mean(), grp["era5_v10"].mean()
        if pd.notna(mean_u) and pd.notna(mean_v):
            U_eff = float(np.sqrt(mean_u ** 2 + mean_v ** 2))
        else:
            U_eff = np.nan

        if pd.notna(U_eff) and U_eff > min_wind and L_m > 0:
            t_mix, t_mix_method = L_m / U_eff, "wind_dependent"
        else:
            t_mix, t_mix_method = fallback_tmix, "fixed_48h_fallback"

        cluster_rows.append({
            "plume_id": plume_id,
            "scene_id": grp["scene_id"].iloc[0],
            "n_pixels": n_pixels,
            "mean_ch4_enhancement_ppb": round(mean_enh, 2),
            "max_ch4_enhancement_ppb": round(max_enh, 2),
            "spatial_extent_km": round(extent_km, 2),
            "ime_kg": round(ime_kg, 2),
            "U_eff_ms": round(U_eff, 2) if pd.notna(U_eff) else None,
            "t_mix_method": t_mix_method,
            "implied_emission_rate_kg_h": round((ime_kg / t_mix) * 3600, 2),
        })

    clusters_df = pd.DataFrame(cluster_rows).sort_values(
        "implied_emission_rate_kg_h", ascending=False
    ).reset_index(drop=True)

    print(f"Flagged large clusters: {len(clusters_df)}")
    print(f"Pixel count range: {clusters_df['n_pixels'].min()} - {clusters_df['n_pixels'].max()} "
          f"(max_cluster_pixels cap = {CONFIG['max_cluster_pixels']})")
    print()
    print("=== Implied emission rate if these clusters had NOT been discarded, sorted descending ===")
    print(clusters_df.to_string(index=False))

    accepted_max = plumes_pdf["emission_rate_kg_h"].max()
    discarded_max = clusters_df["implied_emission_rate_kg_h"].max()
    print()
    print(f"Max emission rate among ACCEPTED plumes (gold_plume_catalog): {accepted_max:.2f} kg/h")
    print(f"Max implied emission rate among DISCARDED clusters:           {discarded_max:.2f} kg/h")
    if discarded_max > accepted_max:
        print("-> At least one discarded cluster implies a HIGHER rate than any accepted plume.")
    else:
        print("-> No discarded cluster implies a higher rate than the largest accepted plume.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 2 — Accepted-plume enhancement vs instrument noise
#
# For each plume in `gold_plume_catalog`, back out the mean per-pixel CH₄ enhancement in
# ppb from `ime_kg`, `n_pixels`, and `PPB_TO_KG` (the same conversion factor as
# 04_derive_emissions Step 6). Compare against ~10-20 ppb, the approximate TROPOMI
# single-sounding XCH4 precision — enhancements at or below this band are not reliably
# distinguishable from retrieval noise.

# CELL ********************

plumes_pdf["backed_out_mean_enh_ppb"] = plumes_pdf["ime_kg"] / (plumes_pdf["n_pixels"] * PPB_TO_KG)

# Sanity check against the value 04_derive_emissions wrote directly
if "mean_ch4_enhancement_ppb" in plumes_pdf.columns:
    max_abs_diff = (
        plumes_pdf["backed_out_mean_enh_ppb"] - plumes_pdf["mean_ch4_enhancement_ppb"]
    ).abs().max()
    print(f"Sanity check: backed-out enhancement vs stored mean_ch4_enhancement_ppb, "
          f"max abs diff = {max_abs_diff:.6f} ppb")

NOISE_FLOOR_LOW_PPB = 10.0
NOISE_FLOOR_HIGH_PPB = 20.0

fig, ax = plt.subplots(figsize=(9, 5))
ax.hist(plumes_pdf["backed_out_mean_enh_ppb"], bins=20, color="#4c72b0", edgecolor="white")
ax.axvspan(
    NOISE_FLOOR_LOW_PPB, NOISE_FLOOR_HIGH_PPB, color="red", alpha=0.15,
    label=f"~TROPOMI single-sounding XCH4 precision ({NOISE_FLOOR_LOW_PPB:.0f}-{NOISE_FLOOR_HIGH_PPB:.0f} ppb)"
)
ax.set_xlabel("Mean per-pixel CH4 enhancement (ppb), backed out from IME")
ax.set_ylabel("Number of plumes")
ax.set_title("gold_plume_catalog — mean enhancement vs instrument noise floor")
ax.legend()
plt.show()

below_floor = plumes_pdf[plumes_pdf["backed_out_mean_enh_ppb"] < NOISE_FLOOR_HIGH_PPB]
frac_below = len(below_floor) / len(plumes_pdf) if len(plumes_pdf) else float("nan")
print(f"Plumes with mean enhancement < {NOISE_FLOOR_HIGH_PPB:.0f} ppb: "
      f"{len(below_floor)} / {len(plumes_pdf)} ({frac_below * 100:.1f}%)")
print(plumes_pdf["backed_out_mean_enh_ppb"].describe().to_string())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 3 — Comparison against CAMS/SRON
#
# **Note:** `validation_cams_plumes` covers 2021 (Schuit et al. 2023); `gold_plume_catalog`
# covers June-July 2026. There is no temporal overlap, so this is a **distribution**
# comparison of typical emission-rate magnitude — not a matched, plume-to-plume comparison.

# CELL ********************

try:
    cams_pdf = spark.table("validation_cams_plumes").toPandas()
except Exception:
    cams_pdf = pd.DataFrame()

if cams_pdf.empty or "cams_emission_rate_kgh" not in cams_pdf.columns:
    print("validation_cams_plumes not available or missing cams_emission_rate_kgh — skipping.")
else:
    gs_rates = plumes_pdf["emission_rate_kg_h"].dropna()
    cams_rates = cams_pdf["cams_emission_rate_kgh"].dropna()

    print(f"Green Sky (gold_plume_catalog): n={len(gs_rates)}")
    print(gs_rates.describe().to_string())
    print()
    print(f"CAMS/SRON (validation_cams_plumes): n={len(cams_rates)}")
    print(cams_rates.describe().to_string())

    gs_median, cams_median = gs_rates.median(), cams_rates.median()
    print()
    print(f"Median emission rate — Green Sky: {gs_median:.2f} kg/h | CAMS: {cams_median:.2f} kg/h")
    if gs_median > 0:
        print(f"CAMS median / Green Sky median = {cams_median / gs_median:.1f}x")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 4 — Comparison against Carbon Mapper, with spatial matching
#
# Same distribution comparison as Cell 3, using `validation_carbon_mapper_plumes`
# (`cm_emission_rate`, already in kg/h). Additionally attempts nearest-neighbor spatial
# matching within 5 km between Carbon Mapper plumes and `gold_plume_catalog` plumes,
# **regardless of date** — this is a spatial hotspot check, not the 27-plume co-temporal
# match described in `changelog.Notebook`.

# CELL ********************

try:
    cm_pdf = spark.table("validation_carbon_mapper_plumes").toPandas()
except Exception:
    cm_pdf = pd.DataFrame()

if cm_pdf.empty or "cm_emission_rate" not in cm_pdf.columns:
    print("validation_carbon_mapper_plumes not available or missing cm_emission_rate — skipping.")
else:
    gs_rates = plumes_pdf["emission_rate_kg_h"].dropna()
    cm_rates = cm_pdf["cm_emission_rate"].dropna()

    print(f"Green Sky (gold_plume_catalog): n={len(gs_rates)}")
    print(gs_rates.describe().to_string())
    print()
    print(f"Carbon Mapper (validation_carbon_mapper_plumes): n={len(cm_rates)}")
    print(cm_rates.describe().to_string())

    gs_median, cm_median = gs_rates.median(), cm_rates.median()
    print()
    print(f"Median emission rate — Green Sky: {gs_median:.2f} kg/h | Carbon Mapper: {cm_median:.2f} kg/h")
    if gs_median > 0:
        print(f"Carbon Mapper median / Green Sky median = {cm_median / gs_median:.1f}x")

    # ── Spatial matching: nearest neighbor within 5 km, any date ──
    MATCH_RADIUS_KM = 5.0

    cm_matchable = cm_pdf.dropna(subset=["cm_lat", "cm_lon"])
    matches = []
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
                "gs_emission_rate_kg_h": gp["emission_rate_kg_h"],
                "cm_plume_id": cm_row["cm_plume_id"],
                "cm_emission_rate_kg_h": cm_row["cm_emission_rate"],
                "distance_km": round(min_dist, 2),
            })

    matches_df = pd.DataFrame(matches)
    print()
    print(f"Green Sky plumes with a Carbon Mapper match within {MATCH_RADIUS_KM:.0f} km "
          f"(any date): {len(matches_df)} / {len(plumes_pdf)}")

    if len(matches_df) > 0:
        matches_df["rate_ratio_cm_over_gs"] = (
            matches_df["cm_emission_rate_kg_h"] / matches_df["gs_emission_rate_kg_h"]
        )
        print(matches_df.to_string(index=False))
        print()
        print(f"Median rate ratio (Carbon Mapper / Green Sky) for matched pairs: "
              f"{matches_df['rate_ratio_cm_over_gs'].median():.1f}x")
    else:
        print("No spatial matches within the search radius.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 5 — MAD threshold sensitivity
#
# Reproduces 04_derive_emissions Steps 1-2 (scene separation, kNN background estimation)
# once against `silver_plume_ready_pixels`, then Steps 3-4 and 6-7 (candidate detection,
# clustering, IME, emission rate) once per `mad_sigma` value. All of the logic in this cell
# is duplicated from 04_derive_emissions — it must stay in sync with that notebook. Nothing
# is written; this is purely a what-if comparison.

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
def estimate_background(scene_pdf, n_neighbors=None, percentile=None):
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


enhanced_rows = []
for scene_id, scene_group in diag_pixels.groupby("scene_id"):
    scene_group = scene_group.copy()
    bg = estimate_background(scene_group)
    scene_group["ch4_background"] = bg
    scene_group["ch4_enhancement"] = scene_group["ch4"] - bg
    enhanced_rows.append(scene_group)

enhanced_diag = pd.concat(enhanced_rows, ignore_index=True)
print(f"Background estimated for {len(enhanced_diag):,} pixels across "
      f"{enhanced_diag['scene_id'].nunique()} scenes")


# ── UnionFind, duplicated from 04_derive_emissions Step 4 ──
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


def detect_and_quantify(enhanced_pdf, mad_sigma):
    """Steps 3, 4, 6, 7 from 04_derive_emissions, parameterized by mad_sigma.
    Duplicated here for the sensitivity sweep — must stay in sync with that notebook."""
    enhancement_floor = CONFIG["enhancement_floor_ppb"]
    cluster_radius = CONFIG["cluster_radius_km"]
    min_pixels = CONFIG["min_cluster_pixels"]
    max_pixels = CONFIG["max_cluster_pixels"]
    shape_threshold = CONFIG["shape_threshold"]
    min_wind = CONFIG["min_wind_speed_ms"]
    fallback_tmix = CONFIG["mixing_time_fallback_s"]

    per_scene_stats = []
    candidates_list = []
    for scene_id, scene_group in enhanced_pdf.groupby("scene_id"):
        enhancements = scene_group["ch4_enhancement"].values
        median_enh = np.median(enhancements)
        mad = np.median(np.abs(enhancements - median_enh))
        mad_scaled = mad * 1.4826
        threshold = max(mad_sigma * mad_scaled, enhancement_floor)

        scene_group = scene_group.copy()
        scene_group["is_candidate"] = scene_group["ch4_enhancement"] > threshold
        n_candidates = int(scene_group["is_candidate"].sum())

        per_scene_stats.append({
            "scene_id": scene_id, "mad_sigma": mad_sigma, "mad_ppb": round(mad_scaled, 2),
            "threshold_ppb": round(threshold, 2), "candidate_pixels": n_candidates,
        })
        candidates_list.append(scene_group)

    detected_pdf = pd.concat(candidates_list, ignore_index=True)
    candidates_only = detected_pdf[detected_pdf["is_candidate"]].copy()

    plume_rows = []
    plume_counter = 0
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

            plume_counter += 1
            enhancements = cluster_data["ch4_enhancement"].values
            ime_kg = float(np.sum(enhancements * PPB_TO_KG))
            plume_area_km2 = cluster_size * (5.5 * 7.0)
            L_m = float(np.sqrt(plume_area_km2 * 1e6))

            mean_u = cluster_data["era5_u10"].mean()
            mean_v = cluster_data["era5_v10"].mean()
            U_eff = (
                float(np.sqrt(mean_u ** 2 + mean_v ** 2))
                if pd.notna(mean_u) and pd.notna(mean_v) else np.nan
            )

            if pd.notna(U_eff) and U_eff > min_wind and L_m > 0:
                t_mix = L_m / U_eff
            else:
                t_mix = fallback_tmix

            plume_rows.append({
                "plume_id": plume_counter,
                "scene_id": scene_id,
                "n_pixels": cluster_size,
                "emission_rate_kg_h": (ime_kg / t_mix) * 3600,
            })

    plumes_out = pd.DataFrame(plume_rows)
    stats_out = pd.DataFrame(per_scene_stats)
    return stats_out, plumes_out


# ── Baseline: current CONFIG['mad_sigma'] ──
baseline_stats, baseline_plumes = detect_and_quantify(enhanced_diag, CONFIG["mad_sigma"])

if len(baseline_plumes) > 0:
    baseline_plume_counts = baseline_plumes.groupby("scene_id").size().rename("plume_count")
    baseline_stats = baseline_stats.merge(baseline_plume_counts, on="scene_id", how="left")
    baseline_stats["plume_count"] = baseline_stats["plume_count"].fillna(0).astype(int)
else:
    baseline_stats["plume_count"] = 0

print(f"=== Per-scene MAD / threshold / candidates / plumes at mad_sigma={CONFIG['mad_sigma']} (current config) ===")
print(baseline_stats.to_string(index=False))

# ── Sweep mad_sigma ──
sweep_rows = []
for sigma in [2, 3, 4, 5]:
    _, sweep_plumes = detect_and_quantify(enhanced_diag, sigma)
    sweep_rows.append({
        "mad_sigma": sigma,
        "plume_count": len(sweep_plumes),
        "median_emission_rate_kg_h": (
            round(sweep_plumes["emission_rate_kg_h"].median(), 2) if len(sweep_plumes) else None
        ),
    })

sweep_df = pd.DataFrame(sweep_rows)
print()
print("=== MAD sigma sensitivity: plume count and median emission rate ===")
print(sweep_df.to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 6 — Verdict (fill in)
#
# _To be completed after reviewing Cells 1-5._
#
# 1. **Pixel cap discarding real emitters?** Do any flagged large clusters (Cell 1) imply a
#    higher emission rate than the largest accepted plume? Are they spatially/physically
#    plausible single sources, or diffuse regional enhancements?
#
# 2. **Noise floor.** What fraction of accepted plumes (Cell 2) have mean enhancement below
#    the ~10-20 ppb TROPOMI precision band? Does that change confidence in the 0.6-14.3 kg/h
#    range reported in `changelog.Notebook`?
#
# 3. **CAMS comparison (Cell 3).** Is the gap between Green Sky and CAMS median emission
#    rates consistent with a resolution/detection-threshold difference (CAMS only detects
#    super-emitters), or does it suggest Green Sky is detecting something other than point
#    sources?
#
# 4. **Carbon Mapper comparison (Cell 4).** How many spatial matches were found within 5 km?
#    For matched pairs, is the rate ratio consistent with the resolution-dilution
#    explanation in `changelog.Notebook`, or is it inconsistent enough to suggest false
#    positives?
#
# 5. **MAD sensitivity (Cell 5).** How much does plume count and median emission rate change
#    between mad_sigma=2 and mad_sigma=5? A detection method whose output changes drastically
#    over this range is more consistent with thresholding noise than with detecting real,
#    discrete sources.
#
# 6. **Overall verdict:** _______________
