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
stac_times_pdf["scene_seq"] = stac_times_pdf["new_scene"].cumsum()

# The grouping above is unchanged; only the label is. scene_id used to be scene_seq, a
# counter over whichever stac_ids are in this run's window, so it renumbered on every
# rerun. It is now the scene group's own start (its minimum scene_start) in UTC -- see
# scene_label in 00_config. Groups are separated by more than scene_gap_minutes, so two
# groups cannot share a start second; asserted rather than assumed.
SESSION_TZ = spark.conf.get("spark.sql.session.timeZone")
stac_times_pdf["scene_id"] = scene_label(
    stac_times_pdf.groupby("scene_seq")["scene_start"].transform("min"), SESSION_TZ)
assert stac_times_pdf["scene_id"].nunique() == stac_times_pdf["scene_seq"].nunique(), (
    "two scene groups share a scene_id -- their starts fall in the same UTC second")

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

# ### Step 2b: Across-Track Destriping:

# CELL ********************

# Remove per-detector-column bias from the enhancement field.
#
# TROPOMI images the whole ~2600 km swath onto a 2D detector array in a single shot.
# Each across-track detector column (ground_pixel) carries its own calibration bias,
# which shows up in retrieved XCH4 as a stripe running along-track.
# 07c_quantification_diagnostics traced the highest-rate accepted "plume" to exactly
# this: seven perfectly collinear, evenly spaced pixels stepping 0.049 deg in latitude
# -- the along-track pixel size -- i.e. one detector column, not a plume.
#
# Correction: absent striping, the median enhancement of a detector column should be
# about zero, because that column samples ordinary background nearly everywhere along
# the track. A systematic non-zero median IS the stripe, so subtract it per column.
#
# This runs on the enhancement field (output of step 2) rather than on raw XCH4 on
# purpose: the kNN background has already removed the large-scale latitudinal and
# terrain structure, so what survives in a column median is instrument bias rather
# than geophysical signal.
#
# The median is robust, and that robustness is the assumption the whole method rests
# on. A handful of genuine plume pixels sitting in one column cannot meaningfully
# shift it -- more than half of the column's pixels would have to fall inside a plume
# before the median moved at all. That is why a real plume survives destriping and a
# stripe does not.
#
# GROUPING KEY -- (stac_id, ground_pixel), NEVER ground_pixel alone.
# scanline and ground_pixel are granule-relative, not orbit-relative: two granules
# from the same orbit each number their pixels from zero, so ground_pixel = 200 in an
# NRTI granule and ground_pixel = 200 in an OFFL granule are different physical
# detector columns. This is the same reason the dedup key in 03_join_data had to move
# to (orbit, latitude, longitude). Grouping by ground_pixel across a whole scene would
# pool unrelated columns together and smear every correction toward zero.

for _required in ("stac_id", "ground_pixel", "scanline"):
    if _required not in enhanced_pdf.columns:
        raise KeyError(
            f"Destriping needs '{_required}' in silver_plume_ready_pixels. "
            "Re-run 03_join_data -- it carries the swath-index columns through."
        )

destripe_enabled = CONFIG["destripe_enabled"]
destripe_min_scanlines = CONFIG["destripe_min_scanlines"]
destripe_max_correction = CONFIG["destripe_max_correction_ppb"]

# Column used for candidate detection, MAD, clustering and IME from here on.
# ch4_enhancement is kept alongside it so the two can be compared.
DETECT_COL = "ch4_enhancement_destriped"

destriped_parts = []
all_corrections = []      # every applied correction, for the overall distribution
granules_per_scene = {}   # scene_id -> number of granules (distinct stac_ids)
n_groups_total = 0
n_groups_skipped = 0
n_groups_over_cap = 0

print(f"Destriping enabled: {destripe_enabled} "
      f"(min scanlines per column: {destripe_min_scanlines}, "
      f"warn above {destripe_max_correction} ppb)")
print()

for scene_id, scene_group in enhanced_pdf.groupby("scene_id"):
    scene_group = scene_group.copy()

    # Default: destriped == raw enhancement. Columns skipped for too few scanlines,
    # and every column when destriping is disabled, keep this value -- so DETECT_COL
    # is always populated and always safe to use downstream.
    scene_group[DETECT_COL] = scene_group["ch4_enhancement"]
    scene_group["stripe_correction_ppb"] = 0.0
    scene_group["destripe_applied"] = False

    n_granules = scene_group["stac_id"].nunique()
    granules_per_scene[scene_id] = n_granules

    if not destripe_enabled:
        destriped_parts.append(scene_group)
        continue

    scene_corrections = []
    scene_groups_total = 0
    scene_groups_skipped = 0

    for (_stac_id, _ground_pixel), column_pixels in scene_group.groupby(
            ["stac_id", "ground_pixel"]):
        scene_groups_total += 1

        # Too few rows to trust the median -- leave this column unchanged.
        if column_pixels["scanline"].nunique() < destripe_min_scanlines:
            scene_groups_skipped += 1
            continue

        correction = float(np.median(column_pixels["ch4_enhancement"].values))
        idx = column_pixels.index
        scene_group.loc[idx, DETECT_COL] = (
            scene_group.loc[idx, "ch4_enhancement"] - correction
        )
        scene_group.loc[idx, "stripe_correction_ppb"] = correction
        scene_group.loc[idx, "destripe_applied"] = True

        scene_corrections.append(correction)
        if abs(correction) > destripe_max_correction:
            n_groups_over_cap += 1

    n_groups_total += scene_groups_total
    n_groups_skipped += scene_groups_skipped
    all_corrections.extend(scene_corrections)
    destriped_parts.append(scene_group)

    if scene_corrections:
        c = np.array(scene_corrections)
        corr_txt = (f"correction min/median/max = "
                    f"{c.min():.2f} / {np.median(c):.2f} / {c.max():.2f} ppb")
    else:
        corr_txt = "no columns corrected"
    print(f"  Scene {scene_id}: {n_granules} granule(s), "
          f"{scene_groups_total} (stac_id, ground_pixel) group(s), "
          f"{scene_groups_skipped} skipped (<{destripe_min_scanlines} scanlines), "
          f"{corr_txt}")

destriped_pdf = pd.concat(destriped_parts, ignore_index=True)

print()
print("--- Stripe correction distribution (all scenes) ---")
if all_corrections:
    c = np.array(all_corrections)
    print(f"  Columns corrected: {len(c)} of {n_groups_total} group(s); "
          f"{n_groups_skipped} skipped for <{destripe_min_scanlines} scanlines")
    for p in (0, 5, 25, 50, 75, 95, 100):
        print(f"    p{p:<3d} {np.percentile(c, p):8.2f} ppb")
    print(f"  Mean |correction|: {np.abs(c).mean():.2f} ppb")
    if n_groups_over_cap:
        print(f"  WARNING: {n_groups_over_cap} column(s) exceeded "
              f"destripe_max_correction_ppb = {destripe_max_correction} ppb. "
              "A correction that large is not an ordinary detector bias -- check "
              "whether the column is genuinely striped or the background estimate "
              "failed there.")
    else:
        print(f"  All corrections within destripe_max_correction_ppb "
              f"= {destripe_max_correction} ppb")
elif destripe_enabled:
    print(f"  No corrections applied: all {n_groups_total} group(s) skipped for "
          f"<{destripe_min_scanlines} scanlines")
else:
    print("  Destriping disabled (CONFIG['destripe_enabled'] = False)")

print()
print(f"Enhancement range (raw):       "
      f"{destriped_pdf['ch4_enhancement'].min():.1f} to "
      f"{destriped_pdf['ch4_enhancement'].max():.1f} ppb")
print(f"Enhancement range (destriped): "
      f"{destriped_pdf[DETECT_COL].min():.1f} to "
      f"{destriped_pdf[DETECT_COL].max():.1f} ppb")

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
#
# Detection and the MAD both run on the destriped enhancement (DETECT_COL) now. The
# stripe bias inflates the enhancement of pixels in a hot column *and* the scene MAD,
# so leaving it in manufactures candidates at one end while desensitising the
# threshold at the other.

mad_sigma = CONFIG["mad_sigma"]
enhancement_floor = CONFIG["enhancement_floor_ppb"]


def detect_candidates(pdf, enh_col, verbose=True):
    """MAD-threshold candidate detection on `enh_col`, scene by scene.

    Returns a copy of `pdf` with is_candidate / detection_threshold / scene_mad set.
    Factored into a function so the same detection can be re-run on the un-destriped
    field for the before/after comparison reported in the summary cell.
    """
    out = []

    for scene_id, scene_group in pdf.groupby("scene_id"):
        enhancements = scene_group[enh_col].values

        # Median Absolute Deviation of the background distribution
        median_enh = np.median(enhancements)
        mad = np.median(np.abs(enhancements - median_enh))

        # MAD to sigma conversion (1 MAD ~ 0.6745 sigma for normal distribution)
        mad_scaled = mad * 1.4826  # scale factor to match standard deviation

        # Threshold
        threshold = max(mad_sigma * mad_scaled, enhancement_floor)

        # Flag candidates
        scene_group = scene_group.copy()
        scene_group["is_candidate"] = scene_group[enh_col] > threshold
        scene_group["detection_threshold"] = threshold
        scene_group["scene_mad"] = mad_scaled

        if verbose:
            n_candidates = scene_group["is_candidate"].sum()
            print(f"  Scene {scene_id}: MAD={mad_scaled:.2f} ppb, "
                  f"threshold={threshold:.2f} ppb, "
                  f"candidates={n_candidates}/{len(scene_group)}")

        out.append(scene_group)

    return pd.concat(out, ignore_index=True)


detected_pdf = detect_candidates(destriped_pdf, DETECT_COL)
total_candidates = detected_pdf["is_candidate"].sum()
print(f"\nTotal candidate pixels: {total_candidates} / {len(detected_pdf)}")

# Diagnostic-only second pass on the raw enhancement, so the summary cell can report
# what destriping actually changed. Nothing downstream reads detected_raw_pdf.
if destripe_enabled:
    detected_raw_pdf = detect_candidates(destriped_pdf, "ch4_enhancement", verbose=False)
    total_candidates_raw = detected_raw_pdf["is_candidate"].sum()
    print(f"Candidate pixels before destriping: {total_candidates_raw} "
          f"/ {len(detected_raw_pdf)}")
else:
    detected_raw_pdf = detected_pdf
    total_candidates_raw = total_candidates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 4: Plume Clustering (Union-Find):

# CELL ********************

# Group candidate pixels within 12 km radius using Union-Find
# Then filter by cluster size (3-15 pixels), collinearity, and shape

from collections import defaultdict


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

def coords_km(lats, lons):
    """Centre lat/lon on their mean and project to km (local flat-earth approximation)."""
    return np.column_stack([
        (lats - lats.mean()) * 111.0,
        (lons - lons.mean()) * 94.0,
    ])

def principal_axis(lats, lons):
    """Total-least-squares line fit to pixel coordinates: PCA on the km projection.

    Returns (first_component_vector, variance_explained_fraction). The fraction of
    variance carried by the first component is the collinearity measure -- 1.0 means
    the pixels lie exactly on a line. Shared by the collinearity filter below and by
    plume orientation in step 5, which is the same fit read a different way.
    """
    pts = coords_km(lats, lons)
    cov_matrix = np.cov(pts.T)
    eigenvalues, eigenvectors = np.linalg.eigh(cov_matrix)
    order = np.argsort(eigenvalues)[::-1]
    eigenvalues = eigenvalues[order]
    eigenvectors = eigenvectors[:, order]
    total_var = eigenvalues.sum()
    frac = float(eigenvalues[0] / total_var) if total_var > 0 else 1.0
    return eigenvectors[:, 0], frac

cluster_radius = CONFIG["cluster_radius_km"]
min_pixels = CONFIG["min_cluster_pixels"]
max_pixels = CONFIG["max_cluster_pixels"]
shape_threshold = CONFIG["shape_threshold"]
collinearity_max_r2 = CONFIG["collinearity_max_r2"]
collinearity_min_pixels = CONFIG["collinearity_min_pixels"]


def cluster_plumes(candidates, apply_collinearity=True, verbose=True):
    """Union-Find clustering plus the size / collinearity / shape filters.

    Returns (valid, flagged_large, rejected, plume_counter); the first three are lists
    of per-cluster pixel frames. Each frame carries cluster_idx, a WITHIN-RUN index used
    only to key the intermediate frames of steps 5-8. It is not an identifier: plume_id
    needs source_lat/source_lon, which step 6 computes, so it is assigned there.
    `apply_collinearity=False` reproduces the pre-B3
    filter chain (size + shape only) and is used for the like-for-like before/after
    destriping comparison in the summary cell.
    """
    valid = []
    flagged = []
    rejected = []
    counter = 0

    for scene_id, scene_candidates in candidates.groupby("scene_id"):
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
        clusters = defaultdict(list)
        for i in range(n):
            clusters[uf.find(i)].append(i)

        for cluster_root, member_indices in clusters.items():
            cluster_size = len(member_indices)
            cluster_data = scene_candidates.iloc[member_indices].copy()

            # Size filter
            if cluster_size < min_pixels:
                continue

            lats = cluster_data["latitude"].values
            lons = cluster_data["longitude"].values

            # Shape metric (aspect ratio from coordinate spread). Hoisted above the
            # filters so it can be recorded on rejected clusters too; the filter
            # itself still fires below, on the same condition as before.
            if cluster_size >= 2:
                lat_range = lats.max() - lats.min()
                lon_range = (lons.max() - lons.min()) * np.cos(np.radians(lats.mean()))

                if min(lat_range, lon_range) > 0:
                    aspect_ratio = max(lat_range, lon_range) / min(lat_range, lon_range)
                else:
                    aspect_ratio = float("inf")
            else:
                aspect_ratio = 1.0

            # --- Collinearity / single-detector-column rejection ---
            # A stripe is one detector column, so its pixels march along-track in a
            # straight, evenly spaced line. Two tests catch that:
            #   1. the pixels fit a line too well (first principal component carries
            #      more than collinearity_max_r2 of the variance), or
            #   2. every pixel comes from one detector column, at any cluster size.
            #
            # Test 2 is keyed on the (stac_id, ground_pixel) PAIR, never ground_pixel
            # alone: ground_pixel is granule-relative, so the same number in two
            # granules is two different physical columns (see the note in step 2b).
            #
            # These run before the aspect-ratio filter purely for bookkeeping. The
            # accepted set is identical either way, but going first means striping
            # artefacts land in gold_rejected_collinear where they can be inspected,
            # instead of being dropped silently by the shape filter.
            n_unique_locations = len(set(zip(lats, lons)))
            n_column_pairs = len(set(zip(
                cluster_data["stac_id"].values,
                cluster_data["ground_pixel"].values,
            )))

            if n_unique_locations >= collinearity_min_pixels:
                _axis, variance_explained = principal_axis(lats, lons)
            else:
                variance_explained = float("nan")

            reject_reason = None
            if apply_collinearity:
                if n_column_pairs == 1:
                    reject_reason = "single_column"
                elif (n_unique_locations >= collinearity_min_pixels
                      and variance_explained > collinearity_max_r2):
                    reject_reason = "collinear"

            if reject_reason is not None:
                counter += 1
                cluster_data["cluster_idx"] = counter
                cluster_data["plume_flag"] = reject_reason
                cluster_data["aspect_ratio"] = aspect_ratio
                cluster_data["variance_explained"] = variance_explained
                cluster_data["n_column_pairs"] = n_column_pairs
                # Would this cluster have become a valid plume without the new
                # filters? Recorded so the summary can count the pre-rejection stage
                # on the same size + shape basis as every other stage.
                cluster_data["would_be_valid"] = bool(
                    cluster_size <= max_pixels
                    and not (cluster_size >= 2 and aspect_ratio > shape_threshold)
                )
                rejected.append(cluster_data)
                if verbose:
                    print(f"  Scene {scene_id}: REJECTED {reject_reason} cluster "
                          f"({cluster_size} pixels, "
                          f"var_explained={variance_explained:.4f}, "
                          f"distinct (stac_id, ground_pixel)={n_column_pairs})")
                continue

            # Shape filter
            if cluster_size >= 2 and aspect_ratio > shape_threshold:
                continue

            # Large cluster handling
            if cluster_size > max_pixels:
                counter += 1
                cluster_data["cluster_idx"] = counter
                cluster_data["plume_flag"] = "large_cluster"
                cluster_data["aspect_ratio"] = aspect_ratio
                flagged.append(cluster_data)
                if verbose:
                    print(f"  Scene {scene_id}: FLAGGED large cluster "
                          f"({cluster_size} pixels, aspect={aspect_ratio:.1f})")
                continue

            # Valid plume
            counter += 1
            cluster_data["cluster_idx"] = counter
            cluster_data["plume_flag"] = "valid"
            cluster_data["aspect_ratio"] = aspect_ratio
            valid.append(cluster_data)

    return valid, flagged, rejected, counter


# Work only with candidate pixels
candidates_only = detected_pdf[detected_pdf["is_candidate"]].copy()
print(f"Clustering {len(candidates_only)} candidate pixels...")

all_plumes, flagged_large, rejected_collinear, plume_counter = cluster_plumes(
    candidates_only, apply_collinearity=True, verbose=True
)

n_rejected_collinear = sum(
    1 for c in rejected_collinear if c["plume_flag"].iloc[0] == "collinear"
)
n_rejected_single_column = sum(
    1 for c in rejected_collinear if c["plume_flag"].iloc[0] == "single_column"
)

print(f"\nValid plumes: {len(all_plumes)}")
print(f"Flagged large clusters: {len(flagged_large)}")
print(f"Rejected, pixels fit a line (var explained > {collinearity_max_r2}): "
      f"{n_rejected_collinear}")
print(f"Rejected, single (stac_id, ground_pixel) column: {n_rejected_single_column}")
print(f"Clusters indexed this run (cluster_idx, not plume_id): {plume_counter}")

if all_plumes:
    plumes_pdf = pd.concat(all_plumes, ignore_index=True)
    print(f"Total pixels in valid plumes: {len(plumes_pdf)}")
else:
    plumes_pdf = pd.DataFrame()
    print("WARNING: No valid plumes detected. Check thresholds.")

# Diagnostic-only pass on the un-destriped candidates, collinearity rejection off, so
# the summary can compare the plume count before destriping against the count after it
# on the same size + shape basis. Nothing downstream reads plumes_raw_list.
if destripe_enabled:
    plumes_raw_list, _flagged_raw, _rejected_raw, _counter_raw = cluster_plumes(
        detected_raw_pdf[detected_raw_pdf["is_candidate"]].copy(),
        apply_collinearity=False, verbose=False
    )
    print(f"Plumes before destriping (size + shape only): {len(plumes_raw_list)}")
else:
    plumes_raw_list = all_plumes + [
        c for c in rejected_collinear if bool(c["would_be_valid"].iloc[0])
    ]

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
    
    for cluster_idx, plume_group in plumes_pdf.groupby("cluster_idx"):
        lats = plume_group["latitude"].values
        lons = plume_group["longitude"].values
        
        # Plume centroid
        centroid_lat = lats.mean()
        centroid_lon = lons.mean()
        
        # Principal axis analysis -- the same PCA fit the collinearity filter
        # uses (principal_axis, defined in step 4), read here for orientation
        # instead of for variance explained.
        if len(lats) >= 3:
            axis_vec, _variance_explained = principal_axis(lats, lons)
            plume_orientation = np.degrees(np.arctan2(axis_vec[1], axis_vec[0])) % 360
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
            "cluster_idx": cluster_idx,
            "plume_orientation_deg": plume_orientation,
            "wind_direction_deg": wind_direction,
            "wind_alignment_deg": angle_diff,
            "wind_confidence": wind_confidence,
            "era5_wind_speed_ms": wind_speed,
        })
    
    wind_df = pd.DataFrame(plume_summaries)
    print("Wind alignment results:")
    print(wind_df[["cluster_idx", "wind_alignment_deg", "wind_confidence", "era5_wind_speed_ms"]].to_string())
    
    # Merge wind alignment back to plume pixels
    plumes_pdf = plumes_pdf.merge(wind_df, on="cluster_idx", how="left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Plume identifiers:
#
# `plume_id` is `plume_key(scene_id, source_lat, source_lon)` from `00_config`: content, not
# iteration order, so a rerun on the same window reproduces every ID. It is assigned in step 6,
# where `source_lat` / `source_lon` (the peak-enhancement pixel) first exist; until then the
# frames carry `cluster_idx`, a within-run index. Flagged and rejected clusters never reach
# step 6, so they take the same rule over their own peak pixel below.

# CELL ********************

# Digest-level check: Spark's sha2 over the same key string must give the same hex as
# hashlib, so anyone recomputing a plume_id in SQL gets the one 04 wrote. This is 02d's
# layer 1 (is the string being hashed right?). There is no layer 2: 02d goes on to check a
# 63-bit reduction to a bigint, but plume_id keeps a hex prefix and is never reduced, so
# there is nothing further to verify. The key strings come from plume_key_string -- Python's
# .5f -- because Spark's format_string may round a decimal tie differently (00_config).
from pyspark.sql.functions import sha2, concat, substring

_pg = spark.createDataFrame(
    [(_s, plume_key_string(_s, _a, _o), _p) for _s, _a, _o, _k, _p in PLUME_ID_GOLDEN],
    "scene_id string, key string, expected string",
).withColumn("actual", concat(lit("PL-"), substring(sha2(col("key"), 256), 1, 12)))
_bad = _pg.filter("actual <> expected").collect()
assert not _bad, (
    f"Spark's sha2 disagrees with plume_key on {_bad[0]}. Both hash the same string, so "
    "the digest itself differs -- check the string's encoding, not the coordinates.")
print(f"OK  Spark sha2 matches plume_key on {len(PLUME_ID_GOLDEN)} golden vectors "
      "(digest level; no reduction applies to a hex-prefix ID)")


def assign_plume_ids(cluster_frames):
    """plume_id for clusters that never reach step 6, from their own peak-enhancement pixel.

    Step 6 defines source_lat/source_lon as the pixel with the highest DETECT_COL, first
    occurrence on a tie (argmax). The same rule applies here, so a cluster's plume_id means
    the same thing in every table that carries one. Clusters are disjoint sets of pixels, so
    no two share a peak pixel and the IDs are unique across all three tables -- asserted at
    the end of the notebook.
    """
    for c in cluster_frames:
        i = int(c[DETECT_COL].values.argmax())
        c["plume_id"] = plume_key(c["scene_id"].iloc[0],
                                  c["latitude"].iloc[i], c["longitude"].iloc[i])
    return cluster_frames


flagged_large = assign_plume_ids(flagged_large)
rejected_collinear = assign_plume_ids(rejected_collinear)
print(f"plume_id assigned to {len(flagged_large)} flagged and {len(rejected_collinear)} "
      "rejected clusters; valid plumes get theirs in step 6")

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
# - TROPOMI pixel area ~ 5.5 km x 7 km = 38.5 km^2   (PIXEL_AREA_M2, m^2)
# - Dry air column ~ 2.12e25 molecules/cm^2 = 2.12e29 molecules/m^2 (standard
#   atmosphere; DRY_AIR_COLUMN_PER_CM2 and DRY_AIR_COLUMN_PER_M2)
# - CH4 molecular weight: 16.04 g/mol                (M_CH4, kg/mol)
# - Dry air molecular weight: 28.97 g/mol            (M_AIR, kg/mol)
# - Avogadro constant                                (AVOGADRO, molecules/mol)
# - 1 ppb = 1e-9 mol/mol
#
# All of the above, and PPB_TO_KG itself, now come from 00_config (%run at the top of
# this notebook) rather than being defined here. They used to live in this cell and be
# copy-pasted into 07b and 07c, which is how a cm^2/m^2 unit error came to sit in three
# notebooks at once -- see the unit-error note in 00_config for the full derivation.
# The assertion that guards PPB_TO_KG lives there too, next to the computation.

print(f"Conversion factor: 1 ppb over 1 pixel = {PPB_TO_KG:.6f} kg CH4")
print(f"  = {PPB_TO_KG * 1000:.4f} g CH4")

if len(plumes_pdf) > 0:
    ime_results = []
    
    for cluster_idx, plume_group in plumes_pdf.groupby("cluster_idx"):
        # IME is computed on the destriped enhancement (step 2b); the raw
        # ch4_enhancement column is kept on the frame for comparison.
        enhancements = plume_group[DETECT_COL].values
        
        # IME = sum of per-pixel mass contributions
        ime_kg = np.sum(enhancements * PPB_TO_KG)
        
        # Plume characteristics
        n_pixels = len(plume_group)
        # Derived from PIXEL_AREA_M2 (00_config) rather than a second 5.5 x 7.0 km
        # literal, so the TROPOMI pixel area has exactly one definition. This matters
        # because plume_area_km2 feeds L_m, L_m feeds t_mix, and t_mix divides the IME
        # to give the emission rate -- a pixel area that disagrees with PIXEL_AREA_M2
        # propagates into every emission rate the pipeline produces.
        # The value is unchanged: PIXEL_AREA_M2 / 1e6 is 38.5 km^2. Correcting the
        # pre-Aug-2019 pixel size itself is a separately tracked task.
        plume_area_km2 = n_pixels * (PIXEL_AREA_M2 / 1e6)  # approximate
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
            # The plume summary is where the identifier is assigned: source_lat and
            # source_lon exist only from here. cluster_idx stays as the key for steps 7-8
            # so their row order -- and so the Monte Carlo's draw order -- is unchanged.
            "cluster_idx": cluster_idx,
            "plume_id": plume_key(scene_id, source_lat, source_lon),
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
    wind_info = plumes_pdf.groupby("cluster_idx").agg({
        "era5_u10": "mean",
        "era5_v10": "mean",
        "era5_blh": "mean",
        "wind_speed_10m": "mean",
    }).reset_index()
    
    wind_info["era5_wind_speed"] = np.sqrt(
        wind_info["era5_u10"]**2 + wind_info["era5_v10"]**2
    )
    
    ime_df = ime_df.merge(wind_info, on="cluster_idx", how="left")
    
    # Also merge wind alignment
    if "wind_alignment_deg" in wind_df.columns:
        ime_df = ime_df.merge(
            wind_df[["cluster_idx", "wind_alignment_deg", "wind_confidence"]],
            on="cluster_idx", how="left"
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
            "cluster_idx": row["cluster_idx"],
            "L_m": L_m,
            "U_eff_ms": U_eff,
            "t_mix_s": t_mix,
            "t_mix_method": t_mix_method,
            "emission_rate_kg_s": emission_rate,
            "emission_rate_kg_h": emission_rate * 3600,
            "emission_rate_t_h": emission_rate * 3.6,
        })
    
    emission_df = pd.DataFrame(emission_results)
    ime_df = ime_df.merge(emission_df, on="cluster_idx", how="left")
    
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
        cluster_idx = row["cluster_idx"]
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
            "cluster_idx": cluster_idx,
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
    ime_df = ime_df.merge(unc_df, on="cluster_idx", how="left")
    
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

    # The previous catalog, read before it is overwritten, for the plume-set comparison in
    # the last cell. On the first run after plume_id became a string this is the old
    # counter-keyed catalog, and the comparison is by (stac_id, source position) instead.
    try:
        prev_catalog_pdf = spark.table("gold_plume_catalog").select(
            "plume_id", "stac_id", "source_lat", "source_lon").toPandas()
    except Exception:
        prev_catalog_pdf = None

    # overwriteSchema because plume_id and scene_id change type, bigint to string, on the
    # first run after this change; Delta rejects a type change on a plain overwrite. It also
    # drops 05's attribution columns until 05 runs again -- which is the order the pipeline
    # runs in anyway, and 05 already writes with overwriteSchema.
    gold_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
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
            .option("overwriteSchema", "true") \
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

# gold_rejected_collinear is written outside the "any valid plumes" branch on purpose:
# the case worth inspecting most is a run where collinearity rejection removed
# everything, and gating this on valid plumes would throw exactly that evidence away.
if rejected_collinear:
    rejected_pdf = pd.concat(rejected_collinear, ignore_index=True)
    rejected_spark = spark.createDataFrame(
        rejected_pdf[["plume_id", "scene_id", "latitude", "longitude",
                      "ch4", "ch4_enhancement", "ch4_enhancement_destriped",
                      "stac_id", "ground_pixel", "scanline",
                      "plume_flag", "aspect_ratio",
                      "variance_explained", "n_column_pairs", "would_be_valid"]]
    )
    rejected_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("gold_rejected_collinear")
    print(f"Written {len(rejected_pdf)} pixels in {len(rejected_collinear)} rejected "
          f"clusters to gold_rejected_collinear "
          f"({n_rejected_collinear} collinear, {n_rejected_single_column} single-column)")
else:
    print("No clusters rejected for collinearity or single-column membership")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation summary:

# CELL ********************

# --- Pipeline stage accounting (destriping and collinearity rejection) ---
# Emission rates for the intermediate stages are recomputed here for comparison only;
# nothing in this block feeds the gold tables. It mirrors steps 6 and 7
# (IME -> L / U_eff -> rate). If the IME conversion or the T_mix formula changes
# there, change it here too or the stage comparison silently drifts.

def stage_emission_rates_kg_h(cluster_frames, enh_col):
    rates = []
    for cluster_data in cluster_frames:
        ime_kg = np.sum(cluster_data[enh_col].values * PPB_TO_KG)
        # Single-source pixel area, as in step 6 -- see the note there on why an
        # inconsistency here would propagate into every emission rate.
        plume_area_km2 = len(cluster_data) * (PIXEL_AREA_M2 / 1e6)
        L_m = np.sqrt(plume_area_km2 * 1e6)
        U_eff = np.sqrt(
            cluster_data["era5_u10"].mean()**2 + cluster_data["era5_v10"].mean()**2
        )
        if U_eff > CONFIG["min_wind_speed_ms"] and L_m > 0:
            t_mix = L_m / U_eff
        else:
            t_mix = CONFIG["mixing_time_fallback_s"]
        rates.append(ime_kg / t_mix * 3600.0)
    return np.array(rates)


def _median_rate_txt(rates):
    return f"{np.median(rates):8.2f} kg/h" if len(rates) else "     n/a (no plumes)"


# Every stage is size + shape filtered, so the counts are like for like.
stage_before_destripe = plumes_raw_list
stage_after_destripe = all_plumes + [
    c for c in rejected_collinear if bool(c["would_be_valid"].iloc[0])
]
stage_after_collinear = all_plumes

rates_before_destripe = stage_emission_rates_kg_h(
    stage_before_destripe, "ch4_enhancement")
rates_after_destripe = stage_emission_rates_kg_h(
    stage_after_destripe, "ch4_enhancement_destriped")
rates_after_collinear = stage_emission_rates_kg_h(
    stage_after_collinear, "ch4_enhancement_destriped")

print("=" * 60)
print("PIPELINE STAGE ACCOUNTING")
print("=" * 60)
print(f"  Candidate pixels before destriping: {total_candidates_raw}")
print(f"  Candidate pixels after  destriping: {total_candidates}")
print()
print(f"  Plumes before destriping:             {len(stage_before_destripe):4d}   "
      f"median rate {_median_rate_txt(rates_before_destripe)}")
print(f"  Plumes after  destriping:             {len(stage_after_destripe):4d}   "
      f"median rate {_median_rate_txt(rates_after_destripe)}")
print(f"  Plumes before collinearity rejection: {len(stage_after_destripe):4d}   "
      f"median rate {_median_rate_txt(rates_after_destripe)}")
print(f"  Plumes after  collinearity rejection: {len(stage_after_collinear):4d}   "
      f"median rate {_median_rate_txt(rates_after_collinear)}")
print(f"    rejected, pixels fit a line:      {n_rejected_collinear}")
print(f"    rejected, single detector column: {n_rejected_single_column}")
if len(ime_df) > 0 and "emission_rate_kg_h" in ime_df.columns:
    print(f"  (cross-check, catalog median: "
          f"{ime_df['emission_rate_kg_h'].median():.2f} kg/h)")
print()

print("--- Granules per scene ---")
# Destriping groups are per-granule, so a group only has as many scanlines as its
# granule contributes. If scenes routinely hold several granules, each
# (stac_id, ground_pixel) group covers a shorter along-track run and its median is a
# noisier stripe estimate -- which is why this distribution is printed.
if granules_per_scene:
    gcounts = np.array(list(granules_per_scene.values()))
    print(f"  Scenes: {len(gcounts)}")
    print(f"  Granules per scene, min/median/max: "
          f"{gcounts.min()} / {np.median(gcounts):.1f} / {gcounts.max()}")
    for n_gran, n_scenes in pd.Series(gcounts).value_counts().sort_index().items():
        print(f"    {n_gran} granule(s): {n_scenes} scene(s)")
    if gcounts.max() > 1:
        print("  NOTE: some scenes hold more than one granule. Those scenes' "
              "destriping groups have fewer scanlines each and a correspondingly "
              "noisier median. destripe_min_scanlines guards this -- check the skip "
              "counts printed in step 2b.")
else:
    print("  No scenes processed")
print()

if len(ime_df) > 0:
    print("=" * 60)
    print("GREEN SKY DETECTION SUMMARY")
    print("=" * 60)
    print(f"Input pixels (Permian Basin): {total_pixels:,}")
    print(f"Scenes processed: {silver_pdf['scene_id'].nunique()}")
    print(f"Candidate pixels: {total_candidates}")
    print(f"Valid plumes detected: {len(ime_df)}")
    print(f"Flagged large clusters: {len(flagged_large)}")
    print(f"Rejected collinear clusters: {n_rejected_collinear}")
    print(f"Rejected single-column clusters: {n_rejected_single_column}")
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

# ### Identifier checks, and the rerun regression:
#
# `plume_id` must be unique, well-formed and never numeric, so a counter cannot silently
# return. Then the check that matters: **a rerun on the same input produces the same plume_id
# set.** Every run records its input fingerprint (per-`stac_id` pixel count, time range and
# CH₄ range of `silver_plume_ready_pixels`, plus `CONFIG`) and its sorted `plume_id`s in
# `gold_plume_id_runs`; if an earlier run had the same fingerprint, the two sets must be
# identical.
#
# **This check fires only on Fabric.** It compares two real executions against real silver
# data; the offline harness (`tools/harness/harness_plume_ids.py`) can show that the IDs do not
# depend on row or iteration order within one execution, but not that two executions agree.
# The first run after this change has no earlier fingerprint to compare with — **run 04 twice**
# to exercise it.

# CELL ********************

import re
import json
from pyspark.sql.functions import current_timestamp

ID_RUNS_TABLE = "gold_plume_id_runs"

valid_ids = ime_df["plume_id"].tolist() if len(ime_df) > 0 else []
flag_ids = [c["plume_id"].iloc[0] for c in flagged_large]
rej_ids = [c["plume_id"].iloc[0] for c in rejected_collinear]
all_ids = valid_ids + flag_ids + rej_ids

# --- unique, well-formed, never numeric -----------------------------------------------------
_dup = pd.Series(valid_ids)[pd.Series(valid_ids).duplicated()].tolist()
assert not _dup, f"plume_id not unique in gold_plume_catalog: {_dup[:5]}"
_dup = pd.Series(all_ids)[pd.Series(all_ids).duplicated()].tolist()
assert not _dup, (f"plume_id shared between gold_plume_catalog, gold_flagged_large_clusters "
                  f"and gold_rejected_collinear: {_dup[:5]}")
_num = [p for p in all_ids if re.fullmatch(r"\d+", str(p))]
assert not _num, f"numeric plume_id(s) {_num[:5]} -- an iteration-order counter has returned"
_bad = [p for p in all_ids if not re.fullmatch(r"PL-[0-9a-f]{12}", str(p))]
assert not _bad, f"malformed plume_id(s): {_bad[:5]}"
_bad = sorted({s for s in stac_times_pdf["scene_id"] if not re.fullmatch(r"SCN-\d{8}T\d{6}", s)})
assert not _bad, f"malformed scene_id(s): {_bad[:5]}"
if len(ime_df) > 0:
    _tbl = spark.table("gold_plume_catalog")
    _types = dict(_tbl.dtypes)
    assert _types["plume_id"] == "string" and _types["scene_id"] == "string", (
        f"gold_plume_catalog stores plume_id as {_types['plume_id']} and scene_id as "
        f"{_types['scene_id']}; both must be string")
    _n, _d = _tbl.count(), _tbl.select("plume_id").distinct().count()
    assert _n == _d == len(valid_ids), (
        f"gold_plume_catalog holds {_n} rows, {_d} distinct plume_id, {len(valid_ids)} expected")
print(f"OK  plume_id unique: {len(set(valid_ids))} distinct in gold_plume_catalog, "
      f"{len(set(all_ids))} across it, gold_flagged_large_clusters and gold_rejected_collinear")
print(r"OK  no plume_id matches ^\d+$; every plume_id is PL-<12 hex>, every scene_id SCN-<UTC>")

# A plume whose peak enhancement is shared by two distinct pixels would take its source
# position -- and so its plume_id -- from whichever comes first in row order. Expected zero.
_ties = 0
if len(plumes_pdf):
    for _, g in plumes_pdf.groupby("cluster_idx"):
        top = g[g[DETECT_COL] == g[DETECT_COL].max()]
        _ties += int(len(set(zip(top["latitude"], top["longitude"]))) > 1)
print(f"    plumes whose peak enhancement is tied between distinct pixels: {_ties}")

# --- first run after the change: the same plume set as the counter-keyed catalog? ----------
_prev = globals().get("prev_catalog_pdf")
if (_prev is not None and len(_prev)
        and _prev["plume_id"].astype(str).str.fullmatch(r"\d+").all()):
    def _pos(df):
        return set(zip(df["stac_id"], df["source_lat"].round(5), df["source_lon"].round(5)))
    _old = _pos(_prev)
    _new = _pos(ime_df) if len(ime_df) else set()
    print(f"\nprevious gold_plume_catalog used numeric plume_ids: {len(_old)} plumes; this "
          f"run {len(_new)}; matched by (stac_id, source position) {len(_old & _new)}")
    print("    an identifier-only change should match every one -- a difference means the")
    print("    input or CONFIG changed since that run, or detection itself changed")

# --- the rerun regression -------------------------------------------------------------------
_fp_rows = (silver.groupBy("stac_id")
            .agg(spark_count("*").alias("n"), spark_min("datetime").alias("t0"),
                 spark_max("datetime").alias("t1"), spark_min("ch4").alias("c0"),
                 spark_max("ch4").alias("c1"))
            .orderBy("stac_id").collect())
INPUT_FINGERPRINT = id_digest(json.dumps([[str(v) for v in r] for r in _fp_rows]),
                              json.dumps(CONFIG, sort_keys=True, default=str))
_ids_cat, _ids_all = sorted(valid_ids), sorted(all_ids)
_run = {"input_fingerprint": INPUT_FINGERPRINT, "n_plumes": len(_ids_cat),
        "catalog_digest": id_digest(*_ids_cat), "all_ids_digest": id_digest(*_ids_all),
        "plume_ids": ",".join(_ids_cat)}

_prior = None
if spark.catalog.tableExists(ID_RUNS_TABLE):
    _r = (spark.table(ID_RUNS_TABLE).filter(col("input_fingerprint") == INPUT_FINGERPRINT)
          .orderBy(col("run_ts").desc()).limit(1).collect())
    _prior = _r[0] if _r else None

# recorded before the assertion, so a failing run still leaves its evidence
(spark.createDataFrame([_run]).withColumn("run_ts", current_timestamp())
 .write.format("delta").mode("append").saveAsTable(ID_RUNS_TABLE))

if _prior is None:
    print(f"\nno earlier run on this input and CONFIG in {ID_RUNS_TABLE} -- this run is "
          "recorded; run 04 again unchanged and this check compares the two")
else:
    if ((_prior["catalog_digest"], _prior["all_ids_digest"])
            != (_run["catalog_digest"], _run["all_ids_digest"])):
        _was = set(_prior["plume_ids"].split(",")) if _prior["plume_ids"] else set()
        raise AssertionError(
            f"plume_id set changed on a rerun with identical input and CONFIG (fingerprint "
            f"{INPUT_FINGERPRINT[:12]}, earlier run {_prior['run_ts']}).\n"
            f"  catalog: {_prior['n_plumes']} -> {len(_ids_cat)} plumes; new "
            f"{sorted(set(_ids_cat) - _was)[:5]}, gone {sorted(_was - set(_ids_cat))[:5]}\n"
            "  plume_id is meant to be a pure function of the data. Look first at the tie "
            "count above, then at anything order-dependent upstream of source_lat/source_lon.")
    print(f"\nOK  rerun regression: {len(_ids_cat)} plume_ids identical to the run at "
          f"{_prior['run_ts']} on the same input (fingerprint {INPUT_FINGERPRINT[:12]})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 
