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
# **Updated for the corrected pipeline.** Since this notebook was first written,
# `04_derive_emissions` gained across-track destriping (per `(stac_id, ground_pixel)`
# detector column) and collinear-cluster rejection, and a units error was fixed:
# `DRY_AIR_COLUMN` held the dry-air column in molecules/cm² but was multiplied by a pixel
# area in m², so every `ime_kg` and every emission rate was low by a factor of 10⁴. The
# physical constants now live in `00_config` and are shared by 04, 07b and this notebook —
# see the unit-error note there for the derivation.
#
# The question the notebook originally asked — where the ~100x gap against Carbon Mapper
# came from — is answered: it was the units error, not plume geometry and not the length
# scale `L`. The catalogue now holds 75 plumes, median 29.4 t/h (min 3.2, max 131.5),
# against a pre-correction median of 4.3 kg/h. The job now is to confirm the corrected
# pipeline is sound and that the new rate scale is defensible.
#
# - **Cell 0** — did destriping work? Detector-column and granule composition per plume.
# - **Cells 1-2** — plume geometry and background contamination (original questions).
# - **Cells 3-4** — background/`L` sweep and Carbon Mapper matched pairs, kept structurally
#   unchanged from the pre-correction run so the two are directly comparable. Both sides are
#   now in sensible units.
# - **Cell 5** — single-plume visual deep dive.
# - **Cell 6** — external validation of the corrected rate scale against CAMS (primary
#   benchmark) and Carbon Mapper, plus a detection-density check.
# - **Cell 7** — the two remaining tracked biases, quantified against this catalogue.
#
# **Inputs:** `gold_plume_catalog`, `silver_plume_ready_pixels`, `validation_cams_plumes`,
# `validation_carbon_mapper_plumes`
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

try:
    cams_pdf = spark.table("validation_cams_plumes").toPandas()
    print(f"validation_cams_plumes: {len(cams_pdf)} plumes")
except Exception:
    cams_pdf = pd.DataFrame()
    print("validation_cams_plumes: table not found")

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


# ── Step 2b (duplicated from 04_derive_emissions): across-track destriping ──
# Each across-track detector column carries its own calibration bias, which appears in the
# enhancement field as an along-track stripe. Absent striping a column's median enhancement
# should be ~zero, so a systematic offset is the stripe and is subtracted. The median is
# robust: a few genuine plume pixels in a column cannot move it.
#
# Grouped on the (stac_id, ground_pixel) PAIR, never ground_pixel alone — ground_pixel is
# granule-relative, not orbit-relative, so the same number in two granules is two different
# physical detector columns. Must stay in sync with 04_derive_emissions Step 2b.
#
# Both backgrounds are destriped, not just the kNN one, so that Cell 3's sweep varies only
# the background definition and not whether destriping was applied. The kNN output keeps
# 04's exact column name, `ch4_enhancement_destriped`, because that is the path which
# reproduces gold_plume_catalog.
destripe_enabled = CONFIG["destripe_enabled"]
destripe_min_scanlines = CONFIG["destripe_min_scanlines"]

DESTRIPE_PAIRS = [
    ("ch4_enhancement_knn", "ch4_enhancement_destriped"),
    ("ch4_enhancement_annulus", "ch4_enhancement_annulus_destriped"),
]
DETECT_COL = "ch4_enhancement_destriped"

destriped_rows = []
destripe_groups_total = 0
destripe_groups_skipped = 0
destripe_corrections = []

for scene_id, scene_group in enhanced_diag.groupby("scene_id"):
    scene_group = scene_group.copy()
    for src_col, dst_col in DESTRIPE_PAIRS:
        scene_group[dst_col] = scene_group[src_col]
    scene_group["stripe_correction_ppb"] = 0.0
    scene_group["destripe_applied"] = False

    if destripe_enabled:
        for _pair, column_pixels in scene_group.groupby(["stac_id", "ground_pixel"]):
            destripe_groups_total += 1
            if column_pixels["scanline"].nunique() < destripe_min_scanlines:
                destripe_groups_skipped += 1
                continue
            idx = column_pixels.index
            for src_col, dst_col in DESTRIPE_PAIRS:
                correction = float(np.median(column_pixels[src_col].values))
                scene_group.loc[idx, dst_col] = scene_group.loc[idx, src_col] - correction
                if src_col == "ch4_enhancement_knn":
                    scene_group.loc[idx, "stripe_correction_ppb"] = correction
                    destripe_corrections.append(correction)
            scene_group.loc[idx, "destripe_applied"] = True

    destriped_rows.append(scene_group)

enhanced_diag = pd.concat(destriped_rows, ignore_index=True)

if destripe_enabled and destripe_corrections:
    corr = np.array(destripe_corrections)
    print(f"Destriped {destripe_groups_total - destripe_groups_skipped} of "
          f"{destripe_groups_total} (stac_id, ground_pixel) groups "
          f"({destripe_groups_skipped} skipped for <{destripe_min_scanlines} scanlines); "
          f"correction min/median/max = {corr.min():.2f} / {np.median(corr):.2f} / "
          f"{corr.max():.2f} ppb")
elif destripe_enabled:
    print(f"Destriping applied to no groups: all {destripe_groups_total} skipped for "
          f"<{destripe_min_scanlines} scanlines")
else:
    print("Destriping disabled in CONFIG — ch4_enhancement_destriped == ch4_enhancement_knn")


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
collinearity_max_r2 = CONFIG["collinearity_max_r2"]
collinearity_min_pixels = CONFIG["collinearity_min_pixels"]


# ── Total-least-squares line fit, duplicated from 04_derive_emissions Step 4. Must stay
# in sync with that notebook. ──
def principal_axis(lats, lons):
    """PCA on the km-projected coordinates.

    Returns (first_component_vector, variance_explained_fraction); the fraction is the
    collinearity measure, 1.0 meaning the pixels lie exactly on a line.
    """
    pts = np.column_stack([
        (lats - lats.mean()) * 111.0,
        (lons - lons.mean()) * 94.0,
    ])
    eigenvalues, eigenvectors = np.linalg.eigh(np.cov(pts.T))
    order = np.argsort(eigenvalues)[::-1]
    eigenvalues = eigenvalues[order]
    eigenvectors = eigenvectors[:, order]
    total_var = eigenvalues.sum()
    frac = float(eigenvalues[0] / total_var) if total_var > 0 else 1.0
    return eigenvectors[:, 0], frac

# Detection and the MAD run on the destriped enhancement, as in 04_derive_emissions
# Step 3 — the stripe bias would otherwise inflate both the candidate enhancements and
# the scene MAD.
candidate_rows = []
for scene_id, scene_group in enhanced_diag.groupby("scene_id"):
    enhancements = scene_group[DETECT_COL].values
    median_enh = np.median(enhancements)
    mad_scaled = np.median(np.abs(enhancements - median_enh)) * 1.4826
    threshold = max(mad_sigma * mad_scaled, enhancement_floor)

    scene_group = scene_group.copy()
    scene_group["is_candidate"] = scene_group[DETECT_COL] > threshold
    candidate_rows.append(scene_group)

detected_diag = pd.concat(candidate_rows, ignore_index=True)
candidates_only = detected_diag[detected_diag["is_candidate"]].copy()

reconstructed_clusters = {}  # (scene_id, source_lat, source_lon) -> member pixel DataFrame
n_reconstructed = 0
n_rejected_collinear = 0
n_rejected_single_column = 0

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
        else:
            aspect_ratio = 1.0

        # ── Collinearity / single-detector-column rejection, duplicated from
        # 04_derive_emissions Step 4. Applied before the shape filter, as it is there.
        # Without this the reconstruction would register clusters that 04 rejected;
        # those would simply fail to match a gold row, but reproducing 04's filter chain
        # keeps n_reconstructed meaningful. Keyed on the (stac_id, ground_pixel) pair. ──
        n_unique_locations = len(set(zip(lats, lons)))
        n_column_pairs = len(set(zip(cluster_data["stac_id"].values,
                                     cluster_data["ground_pixel"].values)))
        variance_explained = float("nan")
        if n_unique_locations >= collinearity_min_pixels:
            _axis, variance_explained = principal_axis(lats, lons)

        if n_column_pairs == 1:
            n_rejected_single_column += 1
            continue
        if (n_unique_locations >= collinearity_min_pixels
                and variance_explained > collinearity_max_r2):
            n_rejected_collinear += 1
            continue

        if cluster_size >= 2 and aspect_ratio > shape_threshold:
            continue

        peak_idx = cluster_data[DETECT_COL].values.argmax()
        source_lat = round(float(cluster_data.iloc[peak_idx]["latitude"]), 6)
        source_lon = round(float(cluster_data.iloc[peak_idx]["longitude"]), 6)
        reconstructed_clusters[(scene_id, source_lat, source_lon)] = cluster_data.copy()
        n_reconstructed += 1

print(f"Reconstructed {n_reconstructed} clusters passing size/shape/collinearity filters "
      f"at mad_sigma={mad_sigma}")
print(f"  rejected for collinearity: {n_rejected_collinear}, "
      f"for single-column membership: {n_rejected_single_column}")

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

# ### Cell 0 — Did destriping work? Detector-column and granule composition
#
# `04_derive_emissions` now destripes per `(stac_id, ground_pixel)` and rejects clusters
# whose pixels fit a line too well or come from a single detector column. This cell checks
# what survived.
#
# **Detector columns per plume.** A real plume is a patch of air and should span several
# across-track detector columns. A stripe is one column by construction. Any accepted plume
# with exactly one distinct `(stac_id, ground_pixel)` pair is an artifact that got through
# rejection, and is flagged explicitly below.
#
# **Granules per plume.** `03_join_data` deduplicates on `(orbit, latitude, longitude)`, so
# there are no exact coordinate duplicates. But NRTI and OFFL geolocate the same ground
# slightly differently, so the same physical pixel can appear twice at coordinates a few
# metres apart and survive dedup. A plume drawing pixels from two granules would then carry
# roughly twice the pixels, and therefore roughly twice the IME, for the same ground truth.
# Cell 7 quantifies what correcting that would do.

# CELL ********************

composition_rows = []
for plume_id, pix in plume_pixel_map.items():
    composition_rows.append({
        "plume_id": plume_id,
        "n_pixels": len(pix),
        "n_column_pairs": len(set(zip(pix["stac_id"].values, pix["ground_pixel"].values))),
        "n_granules": int(pix["stac_id"].nunique()),
        "n_ground_pixel_values": int(pix["ground_pixel"].nunique()),
    })

composition_df = (
    pd.DataFrame(composition_rows)
    .merge(plumes_pdf[["plume_id", "emission_rate_kg_h", "n_pixels"]]
           .rename(columns={"n_pixels": "n_pixels_catalog"}),
           on="plume_id", how="left")
    .sort_values("plume_id")
    .reset_index(drop=True)
)

print(f"Composition reconstructed for {len(composition_df)} of {len(plumes_pdf)} "
      f"accepted plumes")
print()

# ── Detector columns per plume ──
print("=== Distinct (stac_id, ground_pixel) pairs per plume ===")
print(composition_df["n_column_pairs"].describe().to_string())
print()
print("Distribution:")
col_dist = composition_df["n_column_pairs"].value_counts().sort_index()
for n_cols, n_plumes in col_dist.items():
    print(f"  {n_cols:2d} column(s): {n_plumes:3d} plume(s)"
          f"   {'#' * int(n_plumes)}")

single_column_plumes = composition_df[composition_df["n_column_pairs"] == 1]
print()
if len(single_column_plumes) > 0:
    print(f"*** FLAG: {len(single_column_plumes)} accepted plume(s) occupy a SINGLE "
          f"detector column. These are striping artifacts that survived rejection: ***")
    print(single_column_plumes.to_string(index=False))
else:
    print("No accepted plume occupies a single detector column — rejection did its job.")

# A plume spanning two granules can show more column pairs than distinct ground_pixel
# values, because ground_pixel numbering restarts per granule. Where the two differ, the
# plume is drawing on more than one granule.
mismatched = composition_df[
    composition_df["n_column_pairs"] != composition_df["n_ground_pixel_values"]
]
print()
print(f"Plumes where distinct column pairs != distinct ground_pixel values: "
      f"{len(mismatched)} (these necessarily span >1 granule)")

# ── Granules per plume ──
print()
print("=== Distinct stac_id (granules) contributing pixels per plume ===")
gran_dist = composition_df["n_granules"].value_counts().sort_index()
for n_gran, n_plumes in gran_dist.items():
    print(f"  {n_gran} granule(s): {n_plumes:3d} plume(s)")

multi_granule_df = composition_df[composition_df["n_granules"] > 1]
multi_granule_plume_ids = set(multi_granule_df["plume_id"])
total_member_pixels = int(composition_df["n_pixels"].sum())
multi_granule_pixels = int(multi_granule_df["n_pixels"].sum())

print()
print(f"Plumes drawing on more than one granule: {len(multi_granule_df)} / "
      f"{len(composition_df)}")
if total_member_pixels:
    print(f"Pixels in those plumes: {multi_granule_pixels} / {total_member_pixels} "
          f"({100.0 * multi_granule_pixels / total_member_pixels:.1f}% of all "
          f"accepted-plume pixels)")
if len(multi_granule_df) > 0:
    print()
    print("Multi-granule plumes (candidates for NRTI/OFFL double counting):")
    print(multi_granule_df.to_string(index=False))
    print()
    print("Per-granule pixel split for each:")
    for plume_id in multi_granule_df["plume_id"]:
        counts = plume_pixel_map[plume_id]["stac_id"].value_counts()
        parts = ", ".join(f"{sid}={cnt}" for sid, cnt in counts.items())
        print(f"  plume {plume_id}: {parts}")

fig, axes = plt.subplots(1, 2, figsize=(14, 4.5))
axes[0].hist(composition_df["n_column_pairs"],
             bins=range(1, int(composition_df["n_column_pairs"].max()) + 2),
             color="#4c72b0", edgecolor="white", align="left")
axes[0].axvline(1.5, color="red", linestyle="--",
                label="single detector column (artifact)")
axes[0].set_xlabel("Distinct (stac_id, ground_pixel) pairs in plume")
axes[0].set_ylabel("Number of plumes")
axes[0].set_title("Detector columns spanned per accepted plume")
axes[0].legend()

axes[1].hist(composition_df["n_granules"],
             bins=range(1, int(composition_df["n_granules"].max()) + 2),
             color="#dd8452", edgecolor="white", align="left")
axes[1].set_xlabel("Distinct granules (stac_id) contributing pixels")
axes[1].set_ylabel("Number of plumes")
axes[1].set_title("Granules contributing per accepted plume")

plt.tight_layout()
plt.show()

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

    # Pixel area comes from PIXEL_AREA_M2 in 00_config, not a local literal. This
    # notebook used to define its own copy of the pixel area and the IME constants;
    # that duplication is what let a cm^2/m^2 unit error sit in 04, 07b and 07c at once,
    # unnoticed, making every emission rate low by a factor of 10,000.
    plume_area_km2 = n_pixels * (PIXEL_AREA_M2 / 1e6)
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
# Enhancements are the destriped ones throughout, matching 04_derive_emissions.
print(f"Mean enhancement under kNN background (destriped):     "
      f"{plume_member_pixels['ch4_enhancement_destriped'].mean():.2f} ppb")
print(f"Mean enhancement under annulus background (destriped): "
      f"{plume_member_pixels['ch4_enhancement_annulus_destriped'].mean():.2f} ppb")
print(f"  (pre-destriping, kNN: {plume_member_pixels['ch4_enhancement_knn'].mean():.2f} ppb; "
      f"annulus: {plume_member_pixels['ch4_enhancement_annulus'].mean():.2f} ppb)")

fig, ax = plt.subplots(figsize=(9, 5))
ax.hist(plume_member_pixels["ch4_enhancement_destriped"], bins=20, alpha=0.5,
        label="kNN background (destriped)", color="#4c72b0")
ax.hist(plume_member_pixels["ch4_enhancement_annulus_destriped"], bins=20, alpha=0.5,
        label="Annulus background 25-100km (destriped)", color="#c44e52")
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
        # PIXEL_AREA_M2 from 00_config — see the note in Cell 1 on why this must not be
        # a local literal.
        plume_area_km2 = n_pixels * (PIXEL_AREA_M2 / 1e6)
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
            # Single-pixel fallback: the length scale of one pixel, from 00_config.
            L_m = float(np.sqrt(PIXEL_AREA_M2))

    mean_u, mean_v = pix["era5_u10"].mean(), pix["era5_v10"].mean()
    U_eff = float(np.sqrt(mean_u ** 2 + mean_v ** 2)) if pd.notna(mean_u) and pd.notna(mean_v) else np.nan

    if pd.notna(U_eff) and U_eff > min_wind and L_m > 0:
        t_mix = L_m / U_eff
    else:
        t_mix = fallback_tmix

    return ime_kg, L_m, (ime_kg / t_mix) * 3600


# Same four combinations as the pre-correction run, so the two are directly comparable.
# Only the enhancement columns change: both backgrounds are now destriped, matching
# 04_derive_emissions.
COMBINATIONS = [
    ("a_knn_sqrtarea",     "ch4_enhancement_destriped",         "sqrt_area"),
    ("b_annulus_sqrtarea", "ch4_enhancement_annulus_destriped", "sqrt_area"),
    ("c_knn_maxpair",      "ch4_enhancement_destriped",         "max_pairwise"),
    ("d_annulus_maxpair",  "ch4_enhancement_annulus_destriped", "max_pairwise"),
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
# target.
#
# **The ~157x baseline this cell was written against no longer applies.** That gap was the
# cm²/m² units error, now fixed, and the ratio has inverted — Green Sky rates now sit
# above Carbon Mapper's, which is what a coarser instrument with a higher detection limit
# should show. The cell is left structurally unchanged so the four combinations stay
# directly comparable to the pre-correction run; read the ratio alongside Cell 6, where
# CAMS is the like-for-like benchmark.

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

    enh_vmin = min(scene_pixels["ch4_enhancement_destriped"].min(),
                   scene_pixels["ch4_enhancement_annulus_destriped"].min())
    enh_vmax = max(scene_pixels["ch4_enhancement_destriped"].max(),
                   scene_pixels["ch4_enhancement_annulus_destriped"].max())

    fig, axes = plt.subplots(1, 3, figsize=(20, 6))

    sc0 = axes[0].scatter(scene_pixels["longitude"], scene_pixels["latitude"],
                           c=scene_pixels["ch4"], cmap="viridis", s=25)
    axes[0].scatter(scene_pixels.loc[is_member, "longitude"], scene_pixels.loc[is_member, "latitude"],
                     facecolors="none", edgecolors="red", s=90, linewidths=1.5, label="plume member pixels")
    axes[0].set_title(f"Raw CH4 (ppb) — scene {top_scene_id}")
    axes[0].legend(loc="upper right")
    fig.colorbar(sc0, ax=axes[0])

    sc1 = axes[1].scatter(scene_pixels["longitude"], scene_pixels["latitude"],
                           c=scene_pixels["ch4_enhancement_destriped"], cmap="magma", s=25,
                           vmin=enh_vmin, vmax=enh_vmax)
    axes[1].scatter(scene_pixels.loc[is_member, "longitude"], scene_pixels.loc[is_member, "latitude"],
                     facecolors="none", edgecolors="cyan", s=90, linewidths=1.5)
    axes[1].set_title("Enhancement — kNN background, destriped")
    fig.colorbar(sc1, ax=axes[1])

    sc2 = axes[2].scatter(scene_pixels["longitude"], scene_pixels["latitude"],
                           c=scene_pixels["ch4_enhancement_annulus_destriped"], cmap="magma", s=25,
                           vmin=enh_vmin, vmax=enh_vmax)
    axes[2].scatter(scene_pixels.loc[is_member, "longitude"], scene_pixels.loc[is_member, "latitude"],
                     facecolors="none", edgecolors="cyan", s=90, linewidths=1.5)
    axes[2].set_title("Enhancement — annulus background (25-100km), destriped")
    fig.colorbar(sc2, ax=axes[2])

    for ax in axes:
        ax.set_xlabel("longitude")
        ax.set_ylabel("latitude")

    plt.tight_layout()
    plt.show()

    print()
    print("=== Member pixel coordinates and values ===")
    display_cols = ["latitude", "longitude", "stac_id", "ground_pixel", "scanline",
                    "ch4", "ch4_enhancement_knn", "ch4_enhancement_destriped",
                    "ch4_enhancement_annulus_destriped"]
    print(
        member_pixels[display_cols]
        .sort_values("ch4_enhancement_destriped", ascending=False)
        .round(3)
        .to_string(index=False)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 6 — External validation at the corrected scale
#
# The rate distribution is now ~10⁴ larger than in the pre-correction run, so the external
# benchmarks mean something different: the question is no longer "why are we 100x low" but
# "is this scale right".
#
# **CAMS is the primary benchmark.** `validation_cams_plumes` is the Schuit et al. (2023)
# TROPOMI plume catalogue. It is derived from the same instrument, so it shares the same
# detection limit, the same ~5.5 km ground sampling, the same retrieval, and the same
# underlying physics. Whatever TROPOMI can and cannot see, both catalogues inherit equally.
# Agreement with CAMS on both magnitude and count is the real test.
#
# **Carbon Mapper is a weaker benchmark and should not be read as ground truth.** Its
# plumes come from targeted aircraft surveys and EMIT, whose detection limits are far
# lower — tens to low hundreds of kg/h against TROPOMI's several t/h. It therefore
# legitimately sees a population of sources TROPOMI physically cannot, so its rate
# distribution is expected to sit lower and a ratio away from 1.0 is not by itself an
# error. The matched pairs in Cell 4 also carry no date constraint, so a "match" pairs a
# Green Sky plume with whatever Carbon Mapper saw at that location at any time.
#
# **Detection density** is a separate check from magnitude: 75 plumes above ~3 t/h, in a
# 30-day window, over one basin. Normalising both catalogues per unit area per day tests
# whether the *count* is believable independently of whether the *rates* are.

# CELL ********************

gs_rates_kgh = plumes_pdf["emission_rate_kg_h"].dropna()
gs_rates_th = gs_rates_kgh / 1000.0

print("=== Green Sky (gold_plume_catalog), corrected scale ===")
print(f"n = {len(gs_rates_kgh)}")
print(gs_rates_kgh.describe().to_string())
print(f"  median: {gs_rates_kgh.median():,.0f} kg/h  ({gs_rates_th.median():.1f} t/h)")


def quartile_row(name, rates_kgh):
    r = pd.Series(rates_kgh).dropna()
    if len(r) == 0:
        return {"source": name, "n": 0, "p25_kg_h": None, "median_kg_h": None,
                "p75_kg_h": None, "median_t_h": None}
    return {
        "source": name,
        "n": len(r),
        "p25_kg_h": round(float(r.quantile(0.25)), 1),
        "median_kg_h": round(float(r.median()), 1),
        "p75_kg_h": round(float(r.quantile(0.75)), 1),
        "median_t_h": round(float(r.median()) / 1000.0, 2),
    }


rows = [quartile_row("Green Sky (this pipeline)", gs_rates_kgh)]

cams_rates_kgh = pd.Series(dtype=float)
if not cams_pdf.empty and "cams_emission_rate_kgh" in cams_pdf.columns:
    cams_rates_kgh = cams_pdf["cams_emission_rate_kgh"].dropna()
    rows.append(quartile_row("CAMS / SRON (TROPOMI) — PRIMARY", cams_rates_kgh))
else:
    print("\nvalidation_cams_plumes unavailable or missing cams_emission_rate_kgh.")

cm_rates_kgh = pd.Series(dtype=float)
if not cm_pdf.empty and "cm_emission_rate" in cm_pdf.columns:
    cm_rates_kgh = cm_pdf["cm_emission_rate"].dropna()
    rows.append(quartile_row("Carbon Mapper (aircraft/EMIT)", cm_rates_kgh))
else:
    print("\nvalidation_carbon_mapper_plumes unavailable or missing cm_emission_rate.")

dist_df = pd.DataFrame(rows)
print()
print("=== Rate distributions, all in kg/h ===")
print(dist_df.to_string(index=False))

gs_median = float(gs_rates_kgh.median()) if len(gs_rates_kgh) else float("nan")
print()
print("=== Ratio of benchmark median to Green Sky median ===")
if len(cams_rates_kgh) and gs_median > 0:
    r = float(cams_rates_kgh.median()) / gs_median
    print(f"  CAMS / Green Sky:          {r:6.2f}x   "
          f"(PRIMARY — same instrument, same detection limit)")
if len(cm_rates_kgh) and gs_median > 0:
    r = float(cm_rates_kgh.median()) / gs_median
    print(f"  Carbon Mapper / Green Sky: {r:6.2f}x   "
          f"(lower detection limit — expected to sit below TROPOMI)")

if len(cams_rates_kgh) or len(cm_rates_kgh):
    fig, ax = plt.subplots(figsize=(10, 5))
    series = [("Green Sky", gs_rates_kgh, "#4c72b0")]
    if len(cams_rates_kgh):
        series.append(("CAMS (TROPOMI)", cams_rates_kgh, "#55a868"))
    if len(cm_rates_kgh):
        series.append(("Carbon Mapper", cm_rates_kgh, "#c44e52"))
    positive = [np.log10(s[1][s[1] > 0]) for s in series if (s[1] > 0).any()]
    if positive:
        lo = min(float(p.min()) for p in positive)
        hi = max(float(p.max()) for p in positive)
        bins = np.linspace(lo, hi, 30)
        for label, s, colour in series:
            s = s[s > 0]
            if len(s):
                ax.hist(np.log10(s), bins=bins, alpha=0.5, label=f"{label} (n={len(s)})",
                        color=colour)
        ax.set_xlabel("log10(emission rate, kg/h)")
        ax.set_ylabel("Number of plumes")
        ax.set_title("Emission rate distributions — corrected Green Sky scale vs benchmarks")
        ax.legend()
        plt.show()

# ── Detection density ──
# Plumes per 1e6 km^2 per day. Green Sky uses the CONFIG bbox; CAMS was filtered to the
# same bbox with a 0.5 deg pad in 07_ingest_validation, so it covers a larger area and
# must be normalised against that larger area, not the bare bbox.
print()
print("=== Detection density ===")


def bbox_area_km2(min_lat, max_lat, min_lon, max_lon):
    mean_lat = (min_lat + max_lat) / 2.0
    return ((max_lat - min_lat) * 111.0) * ((max_lon - min_lon) * 111.0 *
                                            np.cos(np.radians(mean_lat)))


gs_area = bbox_area_km2(BBOX["min_lat"], BBOX["max_lat"], BBOX["min_lon"], BBOX["max_lon"])
gs_start = pd.Timestamp(CONFIG["start_date"])
gs_end = pd.Timestamp(CONFIG["end_date"])
gs_days = (gs_end - gs_start).days + 1
gs_density = len(gs_rates_kgh) / gs_area / gs_days * 1e6

print(f"  Green Sky: {len(gs_rates_kgh)} plumes over {gs_area:,.0f} km^2 "
      f"x {gs_days} days")
print(f"             = {gs_density:.3f} plumes per 1e6 km^2 per day")
print(f"             (catalogue min {gs_rates_th.min():.1f} t/h, so this is a "
      f"density above roughly that threshold)")

CAMS_PAD_DEG = 0.5  # must match the pad used in 07_ingest_validation
if len(cams_rates_kgh) and "cams_date" in cams_pdf.columns:
    cams_dates = pd.to_datetime(cams_pdf["cams_date"], errors="coerce").dropna()
    if len(cams_dates):
        cams_area = bbox_area_km2(
            BBOX["min_lat"] - CAMS_PAD_DEG, BBOX["max_lat"] + CAMS_PAD_DEG,
            BBOX["min_lon"] - CAMS_PAD_DEG, BBOX["max_lon"] + CAMS_PAD_DEG,
        )
        cams_days = (cams_dates.max() - cams_dates.min()).days + 1
        cams_density = len(cams_pdf) / cams_area / cams_days * 1e6
        print()
        print(f"  CAMS:      {len(cams_pdf)} plumes over {cams_area:,.0f} km^2 "
              f"x {cams_days} days")
        print(f"             ({cams_dates.min().date()} to {cams_dates.max().date()})")
        print(f"             = {cams_density:.3f} plumes per 1e6 km^2 per day")
        if cams_density > 0:
            print()
            print(f"  Green Sky / CAMS detection density: {gs_density / cams_density:.2f}x")
        print()
        print("  Caveats on this comparison, all of which bias it and none of which are")
        print("  corrected for here:")
        print("   - CAMS covers 2021; this catalogue covers 2026. Permian emissions and")
        print("     TROPOMI processing have both changed in between.")
        print("   - Neither density accounts for observation-day coverage. Cloud, QA")
        print("     filtering and orbit gaps mean neither catalogue had a usable")
        print("     observation on every day of its window, so both densities are")
        print("     understated by an unknown and probably different factor.")
        print("   - Schuit et al. applied their own detection criteria; agreement in")
        print("     count does not imply the same plumes would be found.")
    else:
        print("  CAMS: cams_date column present but unparseable — density skipped.")
elif len(cams_rates_kgh):
    print("  CAMS: no cams_date column — density skipped.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 7 — Remaining known biases, quantified against this catalogue
#
# Two tracked issues still inflate the current rates. This cell estimates each one against
# the actual 75 plumes rather than in the abstract. **It changes nothing** —
# `04_derive_emissions` is untouched; this only measures what correcting them would do.
#
# **Bias 1 — pixel area.** `PIXEL_AREA_M2` uses the pre-August-2019 TROPOMI along-track
# size of 7 km. From August 2019 the along-track sampling improved to 5.5 km, so for 2026
# data the pixel is 5.5 x 5.5 km, not 5.5 x 7.0. The area enters twice and partly cancels:
# `ime_kg` scales linearly with pixel area, while `L_m = sqrt(n_pixels * area)` scales with
# its square root, and rate = IME / (L / U). Net, rate scales as sqrt(area ratio).
#
# **Bias 2 — multi-granule double counting.** Plumes identified in Cell 0 as drawing pixels
# from more than one granule may be counting the same ground twice, because NRTI and OFFL
# geolocate identically-sourced pixels a few metres apart and so survive the
# `(orbit, latitude, longitude)` dedup in `03_join_data`. Recomputed here keeping only the
# granule that contributes the most pixels.

# CELL ********************

PIXEL_AREA_ALT_M2 = 5500.0 * 5500.0   # post-Aug-2019 TROPOMI pixel, 5.5 x 5.5 km
area_ratio = PIXEL_AREA_ALT_M2 / PIXEL_AREA_M2

print(f"Current pixel area:   {PIXEL_AREA_M2 / 1e6:.2f} km^2  (5.5 x 7.0, pre-Aug-2019)")
print(f"Corrected pixel area: {PIXEL_AREA_ALT_M2 / 1e6:.2f} km^2  (5.5 x 5.5, post-Aug-2019)")
print(f"Area ratio: {area_ratio:.4f}")
print()


def rate_kg_h(pix, enh_col, pixel_area_m2):
    """IME -> L -> T_mix -> rate for one plume's pixels at a given pixel area.

    Mirrors 04_derive_emissions Steps 6-7 exactly, with the pixel area parameterised so
    the alternative can be evaluated. PPB_TO_KG scales linearly with pixel area, so it is
    rescaled by the same ratio rather than recomputed from the constants.
    """
    n_pixels = len(pix)
    if n_pixels == 0:
        return float("nan")
    ppb_to_kg = PPB_TO_KG * (pixel_area_m2 / PIXEL_AREA_M2)
    ime_kg = float(np.sum(pix[enh_col].values * ppb_to_kg))
    L_m = float(np.sqrt(n_pixels * pixel_area_m2))

    mean_u, mean_v = pix["era5_u10"].mean(), pix["era5_v10"].mean()
    U_eff = float(np.sqrt(mean_u ** 2 + mean_v ** 2)) if pd.notna(mean_u) and pd.notna(mean_v) else np.nan
    if pd.notna(U_eff) and U_eff > min_wind and L_m > 0:
        t_mix = L_m / U_eff
    else:
        t_mix = fallback_tmix
    return (ime_kg / t_mix) * 3600.0


bias_rows = []
for plume_id, pix in plume_pixel_map.items():
    baseline = rate_kg_h(pix, DETECT_COL, PIXEL_AREA_M2)

    # Bias 1: corrected pixel area, all pixels retained
    area_fixed = rate_kg_h(pix, DETECT_COL, PIXEL_AREA_ALT_M2)

    # Bias 2: current pixel area, only the dominant granule's pixels
    is_multi = plume_id in multi_granule_plume_ids
    if is_multi:
        dominant = pix["stac_id"].value_counts().idxmax()
        pix_dom = pix[pix["stac_id"] == dominant]
    else:
        pix_dom = pix
    granule_fixed = rate_kg_h(pix_dom, DETECT_COL, PIXEL_AREA_M2)

    # Both corrections together
    both_fixed = rate_kg_h(pix_dom, DETECT_COL, PIXEL_AREA_ALT_M2)

    bias_rows.append({
        "plume_id": plume_id,
        "n_pixels": len(pix),
        "multi_granule": is_multi,
        "n_pixels_dominant_granule": len(pix_dom),
        "baseline_kg_h": baseline,
        "area_fixed_kg_h": area_fixed,
        "granule_fixed_kg_h": granule_fixed,
        "both_fixed_kg_h": both_fixed,
    })

bias_df = pd.DataFrame(bias_rows).sort_values("plume_id").reset_index(drop=True)

print(f"Recomputed for {len(bias_df)} of {len(plumes_pdf)} accepted plumes "
      f"(those with reconstructed member pixels)")
print(f"Baseline median from this recomputation: {bias_df['baseline_kg_h'].median():,.0f} kg/h "
      f"({bias_df['baseline_kg_h'].median() / 1000:.1f} t/h)")
print(f"Catalogue median for cross-check:        "
      f"{plumes_pdf['emission_rate_kg_h'].median():,.0f} kg/h "
      f"({plumes_pdf['emission_rate_kg_h'].median() / 1000:.1f} t/h)")
print()


def shift_line(label, col, subset=None):
    d = bias_df if subset is None else bias_df[subset]
    if len(d) == 0:
        print(f"  {label:<42} n=0 — nothing to report")
        return
    base_med = d["baseline_kg_h"].median()
    new_med = d[col].median()
    ratio = new_med / base_med if base_med else float("nan")
    print(f"  {label:<42} n={len(d):3d}  "
          f"median {base_med / 1000:7.2f} -> {new_med / 1000:7.2f} t/h   "
          f"({ratio:.3f}x, {100 * (ratio - 1):+.1f}%)")


print("=== Effect of each bias on the median emission rate ===")
shift_line("Bias 1: pixel area 5.5x7.0 -> 5.5x5.5", "area_fixed_kg_h")
shift_line("Bias 2: dominant granule only (all plumes)", "granule_fixed_kg_h")
shift_line("Bias 2: dominant granule only (multi only)", "granule_fixed_kg_h",
           subset=bias_df["multi_granule"])
print()
shift_line("Both corrections combined", "both_fixed_kg_h")

print()
print(f"Analytic check on bias 1: rate scales as sqrt(area ratio) = "
      f"{np.sqrt(area_ratio):.4f}, because IME scales with area and L with sqrt(area).")

n_multi = int(bias_df["multi_granule"].sum())
print()
print(f"Multi-granule plumes affected by bias 2: {n_multi} / {len(bias_df)}")
if n_multi:
    pix_dropped = int((bias_df.loc[bias_df["multi_granule"], "n_pixels"]
                       - bias_df.loc[bias_df["multi_granule"], "n_pixels_dominant_granule"]).sum())
    print(f"Pixels dropped when keeping only the dominant granule: {pix_dropped}")
    print()
    print("Per-plume detail for multi-granule plumes:")
    print(bias_df[bias_df["multi_granule"]].round(1).to_string(index=False))
else:
    print("No plume draws on more than one granule, so bias 2 currently has no effect "
          "on this catalogue. That is a property of this run, not a guarantee — rerun "
          "this cell after any change to the ingest date range or processing_mode mix.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cell 8 — Findings (fill in)
#
# _To be completed after reviewing Cells 0-7._
#
# **Note on run-to-run variability.** The Monte Carlo uncertainty estimation in
# `04_derive_emissions` Step 8 calls `np.random.normal` without seeding the generator, so
# `emission_rate_p5_kg_h`, `emission_rate_p95_kg_h` and `uncertainty_ratio` differ between
# runs on an identical plume set — the confidence distribution moved from 63 high / 12
# medium to 66 high / 9 medium across two such runs. Anything in this notebook that reads
# p5, p95, `uncertainty_ratio` or `confidence` will therefore vary run to run and should
# not be quoted to more precision than that wobble. `emission_rate_kg_h` itself is
# deterministic and is not affected.
#
# 1. **Destriping (Cell 0).** How many detector columns does a typical accepted plume
#    span? Did any plume survive with a single `(stac_id, ground_pixel)` pair — i.e. did a
#    striping artifact get through rejection? If so, is the collinearity threshold too
#    loose, or did the cluster pick up one stray pixel from a neighbouring column and so
#    dodge the single-column test?
#
# 2. **Granule composition (Cell 0).** How many plumes draw on more than one granule, and
#    what share of accepted-plume pixels do they hold? Is NRTI/OFFL double counting a
#    material effect on this catalogue or a negligible one?
#
# 3. **Geometry (Cell 1).** Are the plumes spatially contiguous (mean nearest-neighbour
#    distance well under the 12 km connection radius, most pixel pairs under 8 km), or are
#    they scattered pixels stitched together by the connection radius alone?
#
# 4. **Background contamination (Cell 2).** Does the annulus background still differ
#    meaningfully from the kNN background now that both are destriped? If the kNN
#    background is pulling in plume pixels themselves, kNN enhancement should be
#    systematically lower than annulus enhancement — is that what the histogram shows?
#
# 5. **Rate sensitivity (Cell 3).** Which single change — background method or `L` — moves
#    the median rate the most? Now that the units error is out of the way, are these
#    second-order effects or do they still move the answer by a factor that matters?
#
# 6. **Matched-pair ratio (Cell 4).** What is the median Carbon Mapper / Green Sky ratio
#    now, and in which direction? Remember Carbon Mapper's detection limit is far lower, so
#    a ratio below 1.0 is expected rather than alarming.
#
# 7. **Visual check (Cell 5).** Does the highest-rate plume look like a coherent downwind
#    plume, or like noise that happened to cluster? Do its member pixels spread across
#    several detector columns and scanlines, or line up along one?
#
# 8. **Magnitude against CAMS (Cell 6).** How close is the median to the CAMS median?
#    CAMS is the like-for-like benchmark — same instrument, same detection limit — so
#    what does the ratio say about whether the corrected scale is right?
#
# 9. **Detection density (Cell 6).** Is 75 plumes in 30 days over the Permian plausible
#    against CAMS's own density? If the densities disagree by a large factor, is that the
#    detection threshold, the observation-day coverage neither figure corrects for, or a
#    genuine difference between 2021 and 2026?
#
# 10. **Remaining biases (Cell 7).** How large is the combined effect of the pixel-area and
#     multi-granule corrections? Is it big enough to change the conclusion drawn in 8 and
#     9, or does the answer hold either way?
#
# 11. **Overall verdict:** _______________
#
# 12. **Next actions:** _______________
