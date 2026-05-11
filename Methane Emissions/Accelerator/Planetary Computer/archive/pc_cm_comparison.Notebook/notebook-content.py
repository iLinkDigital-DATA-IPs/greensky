# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "95f7f55e-9d06-4699-9483-754709a1d397",
# META       "default_lakehouse_name": "Planetary_computer_LH",
# META       "default_lakehouse_workspace_id": "060ba34b-f1a3-4509-a6e2-36d1e736a8eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "95f7f55e-9d06-4699-9483-754709a1d397"
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

# # Carbon Mapper vs. Planetary Computer — Methane Emission Comparison
# 
# ## Overview
# 
# Validates IME-derived emission estimates (TROPOMI / Planetary Computer) against
# Carbon Mapper airborne measurements over the Permian Basin.
# 
# **Pipeline position**: runs after `derive_plume_data` and `derive_emissions_data`.
# 
# ## Methodology
# 
# | Step | Description |
# |------|-------------|
# | 1 | Load PC emissions from `dbo.emission_rates_daily` |
# | 2 | Fetch Carbon Mapper plumes from public API (cached to `dbo.carbon_mapper_2026`) |
# | 3 | Hierarchical spatial-temporal matching (Tier 1: exact date; Tier 2: ±1 day) |
# | 4 | Accuracy metrics (MAE, RMSE, bias) by confidence tier |
# | 5 | DBSCAN cluster quality analysis (k-distance plot, parameter sensitivity) |
# | 6 | Root cause decomposition of systematic bias |
# 
# ## References
# 
# - Varon et al. (2018) — IME method: https://amt.copernicus.org/articles/11/5673/2018/
# - Pandey et al. (2025) — Plume scaling: https://pubs.acs.org/doi/full/10.1021/acs.est.4c07415
# 
# ## Table of Contents
# 1. Configuration
# 2. Data Loading
# 3. Exploratory Diagnostics + DBSCAN Quality
# 4. Plume Matching
# 5. Accuracy Evaluation
# 6. Visualizations
# 7. Root Cause Analysis
# 8. Summary & Export


# CELL ********************

import os
import gc
import json
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import requests
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns

from sklearn.neighbors import BallTree
from sklearn.decomposition import PCA
from sklearn.metrics import mean_absolute_error, mean_squared_error

from pyspark.sql.functions import col, when, isnan, avg

sns.set_style("whitegrid")
plt.rcParams.update({"figure.dpi": 120, "font.size": 11})
print("Imports OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CONFIGURATION  <a id="config"></a>
# All tunable parameters live here — do not hardcode elsewhere.
# ============================================================

# --- Study Region (Permian Basin) ---
BBOX = {"lat_min": 25, "lat_max": 35, "lon_min": -105, "lon_max": -90}

# --- DBSCAN Parameters (must match derive_plume_data.ipynb) ---
EPS_KM      = 10    # Neighbourhood radius in km
MIN_SAMPLES = 5     # Minimum cluster size (core point criterion)

# --- Hierarchical Matching Thresholds ---
# Outer hard gates — candidates outside these are physically implausible.
# Rationale:
#   150 km: TROPOMI wind-advection shift at 5–15 m/s over 3–10 h
#   ±3 days: Carbon Mapper campaign episodicity
#   ±2.5 OOM: full observed PC/CM ratio range (up to ~300x = 2.5 log-orders)
#             PC IME is field-scale; CM is single-plume → gap is expected, not an error
OUTER_SPATIAL_KM  = 150.0
OUTER_DATE_DELTA  = 3
LOG_EMIT_TOL      = 2.5     # |log10(PC/CM)| tolerance — replaces linear ratio bounds

# Score decay scales (Gaussian 1/e point for each dimension)
SCORE_SPATIAL_KM  = 50.0    # 50 km ≈ TROPOMI footprint + 1-sigma wind shift
SCORE_TEMPORAL_D  = 1.5     # same-day → score ≈ 1.0; ±3 days → score ≈ 0.07
SCORE_EMIT_OOM    = 1.0     # 1 order of magnitude difference → score ≈ 0.37

# Confidence tier thresholds on the combined score [0, 1]
SCORE_TIER1_MIN   = 0.50
SCORE_TIER2_MIN   = 0.20

# Keep these so Section 5 plot labels don't break
TIER1_SPATIAL_KM  = SCORE_SPATIAL_KM
TIER2_SPATIAL_KM  = OUTER_SPATIAL_KM
TIER1_DATE_DELTA  = 0       # legacy — no longer used in matching
TIER2_DATE_DELTA  = 1       # legacy
TIER1_RATIO_BOUNDS = (0.25, 4.0)   # legacy display only
TIER2_RATIO_BOUNDS = (0.20, 6.0)   # legacy display only

# --- Outlier Detection ---
IQR_FENCE = 1.5    # Tukey fence multiplier

# --- Carbon Mapper API ---
# Store the bearer token as a Synapse secret or environment variable.
# Never hardcode tokens in notebook source.
CM_API_URL    = "https://api.carbonmapper.org/api/v1/catalog/plumes/annotated"
CM_API_TOKEN  = os.environ.get("CM_API_TOKEN", "")
CM_START_DATE = "2026-03-01T00:00:00Z"
CM_END_DATE   = "2026-04-08T23:59:59Z"

# --- Delta Table Names ---
TABLE_PC_EMISSIONS = "dbo.emission_rates_daily"
TABLE_PC_PLUMES    = "dbo.methane_plumes_dbscan"
TABLE_PC_RAW       = "dbo.planetary_comp_raw_data"
TABLE_CM_CACHE     = "dbo.carbon_mapper_2026"
TABLE_COMPARISON   = "dbo.ime_vs_cm_comparison"

print("Configuration loaded.")
print(f"  Region  : lat {BBOX['lat_min']}–{BBOX['lat_max']} | lon {BBOX['lon_min']}–{BBOX['lon_max']}")
print(f"  DBSCAN  : eps={EPS_KM} km, min_samples={MIN_SAMPLES}")
print(f"  Tier 1  : ±{TIER1_DATE_DELTA} day(s), {TIER1_SPATIAL_KM} km, ratio {TIER1_RATIO_BOUNDS}")
print(f"  Tier 2  : ±{TIER2_DATE_DELTA} day(s), {TIER2_SPATIAL_KM} km, ratio {TIER2_RATIO_BOUNDS}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Section 1: Data Loading  <a id="loading"></a>
# 
# Three sources are loaded and normalised to a common schema:
# 
# | Source | Table | Key columns |
# |--------|-------|-------------|
# | PC emissions | `dbo.emission_rates_daily` | `plume_id`, `date`, `emission_auto` |
# | PC geometry  | `dbo.methane_plumes_dbscan` | `cluster`, `latitude`, `longitude` |
# | Carbon Mapper | `dbo.carbon_mapper_2026` (cached) | `plume_id`, `scene_timestamp`, `emission_auto` |

# CELL ********************

# ============================================================
# 1.1 — Load PC Emissions + Centroid Geometry from Lakehouse
# ============================================================

pc_emissions = spark.sql(f"""
    SELECT
        CAST(plume_id AS STRING)  AS plume_id,
        date,
        emission_auto             AS emission_kg_hr,
        emission_uncertainty_auto AS uncertainty_kg_hr,
        emission_severity
    FROM {TABLE_PC_EMISSIONS}
    WHERE emission_auto IS NOT NULL
""").toPandas()

# Per-cluster centroid: average lat/lon of all pixels in the cluster
pc_geometry = spark.sql(f"""
    SELECT
        CAST(cluster AS STRING) AS plume_id,
        AVG(latitude)           AS latitude,
        AVG(longitude)          AS longitude
    FROM {TABLE_PC_PLUMES}
    WHERE cluster != -1
    GROUP BY cluster
""").toPandas()

# Normalise types
pc_emissions["plume_id"] = pc_emissions["plume_id"].astype(str).str.strip()
pc_geometry["plume_id"]  = pc_geometry["plume_id"].astype(str).str.strip()
pc_emissions["date"]     = pd.to_datetime(pc_emissions["date"]).dt.normalize()
pc_geometry["latitude"]  = pd.to_numeric(pc_geometry["latitude"],  errors="coerce")
pc_geometry["longitude"] = pd.to_numeric(pc_geometry["longitude"], errors="coerce")

# Join geometry onto emission records, then apply bbox filter
pc = (pc_emissions
      .merge(pc_geometry, on="plume_id", how="inner")
      .query(f"{BBOX['lat_min']} <= latitude <= {BBOX['lat_max']} and "
             f"{BBOX['lon_min']} <= longitude <= {BBOX['lon_max']}")
      .copy())

print(f"PC records (in bbox) : {len(pc):,}")
print(f"Unique plume IDs     : {pc['plume_id'].nunique():,}")
print(f"Date range           : {pc['date'].min().date()} \u2192 {pc['date'].max().date()}")
print(f"\nEmission rate (kg/hr):")
print(pc["emission_kg_hr"].describe().round(0).to_string())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#DEBUG
# ── Type diagnostic ──────────────────────────────────────────
pc_emit  = spark.sql("SELECT plume_id, typeof(plume_id) AS type_emit  FROM dbo.emission_rates_daily   LIMIT 3")
pc_plume = spark.sql("SELECT cluster,  typeof(cluster)  AS type_plume FROM dbo.methane_plumes_dbscan  LIMIT 3")
pc_emit.show()
pc_plume.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 1.2 — Carbon Mapper Data  (API \u2192 cached Delta table)
# ============================================================

def _parse_point_coords(coords):
    """Extract (lon, lat) from a GeoJSON Point coordinates list."""
    try:
        if isinstance(coords, list) and len(coords) >= 2:
            return float(coords[0]), float(coords[1])
    except Exception:
        pass
    return None, None


def fetch_carbon_mapper(api_url, token, start_date, end_date, bbox, n_chunks=4):
    """
    Page through the CM annotated-plumes endpoint over tiled sub-bboxes
    and return a deduplicated DataFrame.

    Tiling avoids the API per-request result-count limit for large regions.
    """
    if not token:
        raise ValueError(
            "CM_API_TOKEN is empty.\n"
            "Set it with: os.environ['CM_API_TOKEN'] = '<your_token>'"
        )
    headers   = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    lon_step  = (bbox["lon_max"] - bbox["lon_min"]) / n_chunks
    sub_bboxes = [
        [bbox["lon_min"] + i * lon_step, bbox["lat_min"],
         bbox["lon_min"] + (i + 1) * lon_step, bbox["lat_max"]]
        for i in range(n_chunks)
    ]

    records, seen = [], set()
    for chunk in sub_bboxes:
        page = 0
        while True:
            r = requests.get(
                api_url, headers=headers, timeout=30,
                params={"limit": 1000, "offset": page * 1000, "bbox": chunk,
                        "start_date": start_date, "end_date": end_date},
            )
            if r.status_code != 200:
                print(f"  Warning: HTTP {r.status_code} on chunk {chunk[:2]}")
                break
            data = r.json().get("items", [])
            if not data:
                break
            for rec in data:
                pid = rec.get("plume_id")
                if pid and pid not in seen:
                    records.append(rec)
                    seen.add(pid)
            print(f"  Chunk {chunk[:2]}, page {page}: {len(data)} records")
            if len(data) < 1000:
                break
            page += 1

    if not records:
        return pd.DataFrame()
    df = pd.json_normalize(records)
    if "geometry_json.coordinates" in df.columns:
        df[["longitude", "latitude"]] = df["geometry_json.coordinates"].apply(
            lambda x: pd.Series(_parse_point_coords(x))
        )
    return df


# Load from cache if available, else call API
try:
    cm_raw = spark.sql(f"SELECT * FROM {TABLE_CM_CACHE}").toPandas()
    print(f"Loaded {len(cm_raw):,} CM records from cache ({TABLE_CM_CACHE})")
    _from_api = False
except Exception:
    print("Cache not found — calling Carbon Mapper API...")
    cm_raw = fetch_carbon_mapper(CM_API_URL, CM_API_TOKEN,
                                 CM_START_DATE, CM_END_DATE, BBOX)
    print(f"Fetched {len(cm_raw):,} unique records")
    _from_api = True

# Normalise numeric columns
for col_name in ["latitude", "longitude", "emission_auto", "emission_uncertainty_auto"]:
    if col_name in cm_raw.columns:
        cm_raw[col_name] = pd.to_numeric(cm_raw[col_name], errors="coerce")

ts_col = "scene_timestamp" if "scene_timestamp" in cm_raw.columns else None
if ts_col:
    cm_raw[ts_col] = pd.to_datetime(cm_raw[ts_col], errors="coerce", utc=True)

# Bbox safeguard
cm_raw = (cm_raw
          .loc[cm_raw["latitude"].between(BBOX["lat_min"], BBOX["lat_max"]) &
               cm_raw["longitude"].between(BBOX["lon_min"], BBOX["lon_max"])]
          .dropna(subset=["latitude", "longitude", "emission_auto"])
          .copy())

# Persist to Delta when freshly fetched
if _from_api and len(cm_raw) > 0:
    keep = [c for c in ["plume_id", "scene_timestamp", "latitude", "longitude",
                        "emission_auto", "emission_uncertainty_auto", "sector"]
            if c in cm_raw.columns]
    (spark.createDataFrame(cm_raw[keep])
         .dropDuplicates(["plume_id"])
         .write.mode("overwrite").format("delta").saveAsTable(TABLE_CM_CACHE))
    print(f"Cached {len(cm_raw):,} records \u2192 {TABLE_CM_CACHE}")

print(f"\nCM records after cleaning: {len(cm_raw):,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 1.3 — Preprocess & Align to Common Schema
# ============================================================

# CM: floor to date, aggregate multiple overpasses to daily mean
ts_col = "scene_timestamp" if "scene_timestamp" in cm_raw.columns else "date"
if ts_col == "scene_timestamp":
    cm_raw["date"] = cm_raw[ts_col].dt.tz_localize(None).dt.normalize()
else:
    cm_raw["date"] = pd.to_datetime(cm_raw["date"]).dt.normalize()

agg_cols = {"emission_auto": "mean", "emission_uncertainty_auto": "mean"}
if "sector" in cm_raw.columns:
    agg_cols["sector"] = "first"

cm = (
    cm_raw
    .groupby(["plume_id", "date", "latitude", "longitude"], as_index=False)
    .agg(**{k: (k, v) for k, v in agg_cols.items()})
    .rename(columns={"emission_auto": "emission_kg_hr",
                     "emission_uncertainty_auto": "uncertainty_kg_hr"})
    .dropna(subset=["emission_kg_hr"])
)

# Hard study bounds — prevents stale pre-study PC records from
# shifting the overlap window outside the CM campaign period.
STUDY_START = pd.Timestamp("2026-03-01")
STUDY_END   = pd.Timestamp("2026-04-08")

overlap_start = STUDY_START
overlap_end   = STUDY_END
common_dates  = len(set(pc["date"]) & set(cm["date"]))

# Confirm coverage inside the window
pc_study = pc[(pc["date"] >= overlap_start) & (pc["date"] <= overlap_end)]
cm_study = cm[(cm["date"] >= overlap_start) & (cm["date"] <= overlap_end)]
print(f"PC plumes in study window : {len(pc_study):,}")
print(f"CM plumes in study window : {len(cm_study):,}")
assert len(pc_study) > 0, "No PC plumes in study window — check STUDY_START/END or table contents"
assert len(cm_study) > 0, "No CM plumes in study window — check CM API date range"

print(f"{'':30} {'PC':>10} {'CM':>10}")
print("-" * 52)
for label, pv, cv in [
    ("Records",              f"{len(pc):,}",                   f"{len(cm):,}"),
    ("Unique plume IDs",     f"{pc['plume_id'].nunique():,}",  f"{cm['plume_id'].nunique():,}"),
    ("Observation dates",    f"{pc['date'].nunique()}",        f"{cm['date'].nunique()}"),
    ("Date range start",     str(pc["date"].min().date()),     str(cm["date"].min().date())),
    ("Date range end",       str(pc["date"].max().date()),     str(cm["date"].max().date())),
]:
    print(f"{label:30} {pv:>10} {cv:>10}")

print(f"\nOverlapping window : {overlap_start.date()} \u2192 {overlap_end.date()}")
print(f"Common dates       : {common_dates}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Section 2: Exploratory Diagnostics + DBSCAN Quality  <a id="diagnostics"></a>
# 
# ### 2.1 Coverage Overview
# Emission rate distributions, temporal density, and geographic spread of both datasets.
# 
# ### 2.2 DBSCAN Cluster Quality
# PC plumes detected by DBSCAN feed directly into the IME emission model.  Cluster
# quality issues (over-splitting, over-merging, artifacts) propagate as comparison noise.
# 
# Evaluated here:
# - **K-distance plot** — confirms whether `eps=10 km` sits near the natural density elbow
# - **Cluster size and spatial compactness** — checks physical plausibility
# - **Parameter sensitivity guidance** — eps range relative to TROPOMI pixel size (~5.6 km)

# CELL ********************

# ============================================================
# 2.1 — Coverage Overview
# ============================================================

fig, axes = plt.subplots(1, 3, figsize=(16, 4))

# Log-scale emission distributions
ax = axes[0]
ax.hist(np.log10(pc["emission_kg_hr"].clip(lower=1)), bins=30, alpha=0.65,
        color="steelblue", edgecolor="white", label=f"PC (n={len(pc):,})")
ax.hist(np.log10(cm["emission_kg_hr"].clip(lower=1)), bins=30, alpha=0.65,
        color="tomato",    edgecolor="white", label=f"CM (n={len(cm):,})")
ax.set_xlabel("log\u2081\u2080(emission rate)  [kg/hr]")
ax.set_ylabel("Count")
ax.set_title("Emission Rate Distributions")
ax.legend()

# Daily plume counts
ax = axes[1]
pc_daily = pc.groupby("date").size()
cm_daily = cm.groupby("date").size()
ax.bar(pc_daily.index, pc_daily.values, width=0.8, alpha=0.60,
       color="steelblue", label="PC")
ax2t = ax.twinx()
ax2t.bar(cm_daily.index, cm_daily.values, width=0.6, alpha=0.50,
         color="tomato", label="CM")
ax.set_xlabel("Date"); ax.tick_params(axis="x", rotation=45)
ax.set_ylabel("PC count", color="steelblue")
ax2t.set_ylabel("CM count", color="tomato")
ax.set_title("Daily Plume Counts")

# Geographic scatter
ax = axes[2]
ax.scatter(pc["longitude"], pc["latitude"], s=4, alpha=0.25, color="steelblue", label="PC")
ax.scatter(cm["longitude"], cm["latitude"], s=18, alpha=0.65,
           color="tomato", marker="^", label="CM")
ax.set_xlabel("Longitude"); ax.set_ylabel("Latitude")
ax.set_title("Geographic Distribution")
ax.legend(markerscale=2, fontsize=9)
ax.grid(True, alpha=0.25)

plt.suptitle("Dataset Overview: PC vs Carbon Mapper", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

print(f"\n{'Metric':<35} {'PC':>12} {'CM':>12}")
print("-" * 60)
for label, pv, cv in [
    ("Median emission (kg/hr)", pc["emission_kg_hr"].median(), cm["emission_kg_hr"].median()),
    ("Mean emission (kg/hr)",   pc["emission_kg_hr"].mean(),   cm["emission_kg_hr"].mean()),
    ("Std dev (kg/hr)",         pc["emission_kg_hr"].std(),    cm["emission_kg_hr"].std()),
]:
    print(f"{label:<35} {pv:>12,.0f} {cv:>12,.0f}")
print(f"{'PC/CM count ratio':<35} {len(pc)/max(len(cm),1):>11.1f}x")
print("\nNote: PC detects far more plumes than CM because TROPOMI is a daily-revisit satellite "
      "while Carbon Mapper is an aircraft with targeted campaign coverage.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 2.2 — DBSCAN Cluster Quality Analysis
# ============================================================

# Load stored plume pixels (already filtered and clustered)
plume_px = spark.sql(f"""
    SELECT cluster, latitude, longitude, ch4
    FROM {TABLE_PC_PLUMES}
    WHERE cluster != -1
""").toPandas()

for c in ["latitude", "longitude", "ch4"]:
    plume_px[c] = pd.to_numeric(plume_px[c], errors="coerce")
plume_px = plume_px.dropna()


def bbox_diagonal_km(group):
    """Bounding-box diagonal of a cluster (km)."""
    if len(group) < 2:
        return 0.0
    lat_km = (group["latitude"].max() - group["latitude"].min()) * 111.0
    lon_km = ((group["longitude"].max() - group["longitude"].min())
              * 111.0 * np.cos(np.radians(group["latitude"].mean())))
    return float(np.sqrt(lat_km**2 + lon_km**2))


cluster_stats = plume_px.groupby("cluster").agg(
    n_pixels=("latitude", "count"),
    ch4_mean=("ch4", "mean"),
    ch4_std =("ch4", "std"),
).reset_index()
diag = plume_px.groupby("cluster").apply(bbox_diagonal_km, include_groups=False)
cluster_stats["extent_km"] = cluster_stats["cluster"].map(diag)
cluster_stats["ch4_cv"]    = cluster_stats["ch4_std"] / cluster_stats["ch4_mean"].replace(0, np.nan)

print("=== DBSCAN Cluster Quality ===")
print(f"Total clusters : {len(cluster_stats):,}")
for label, col_name in [("Pixels / cluster", "n_pixels"),
                        ("Spatial extent (km)", "extent_km"),
                        ("CH4 coeff-of-variation", "ch4_cv")]:
    s = cluster_stats[col_name].describe()
    print(f"  {label:<30}  median={s['50%']:.2f}  mean={s['mean']:.2f}  "
          f"min={s['min']:.2f}  max={s['max']:.2f}")

# --- K-distance plot (1% sample from raw pixel table) ---
print("\n--- K-distance plot (1% sample) ---")
try:
    raw_s = spark.sql(f"""
        SELECT latitude, longitude, ch4, DATE(datetime) AS date
        FROM {TABLE_PC_RAW}
        TABLESAMPLE (1 PERCENT)
        WHERE latitude  BETWEEN {BBOX['lat_min']} AND {BBOX['lat_max']}
          AND longitude BETWEEN {BBOX['lon_min']} AND {BBOX['lon_max']}
    """).toPandas().replace([np.inf, -np.inf], np.nan).dropna()

    best_day = raw_s.groupby("date").size().idxmax()
    day_df   = raw_s[raw_s["date"] == best_day]
    high_px  = day_df[day_df["ch4"] > day_df["ch4"].quantile(0.95)]

    if len(high_px) > MIN_SAMPLES + 1:
        coords_rad = np.radians(high_px[["latitude", "longitude"]].to_numpy())
        tree       = BallTree(coords_rad, metric="haversine")
        dists, _   = tree.query(coords_rad, k=MIN_SAMPLES + 1)
        kth_km     = np.sort(dists[:, MIN_SAMPLES])[::-1] * 6371.0
        elbow_i    = int(np.argmax(np.abs(np.diff(kth_km))))
        elbow_eps  = float(kth_km[elbow_i])

        fig, axes = plt.subplots(1, 2, figsize=(14, 4))

        ax = axes[0]
        ax.plot(kth_km, lw=1.5, color="steelblue", label=f"k={MIN_SAMPLES} distance")
        ax.axhline(EPS_KM,    color="red",   ls="--", lw=2,
                   label=f"Current eps = {EPS_KM} km")
        ax.axhline(elbow_eps, color="green", ls=":",  lw=1.5,
                   label=f"Elbow \u2248 {elbow_eps:.1f} km")
        ax.set_xlabel("Points sorted by k-NN distance (desc.)")
        ax.set_ylabel(f"{MIN_SAMPLES}-NN distance (km)")
        ax.set_title(f"K-Distance Plot  (sample day: {best_day})")
        ax.legend(fontsize=9); ax.grid(True, alpha=0.3)

        diff_km = elbow_eps - EPS_KM
        verdict = (f"eps={EPS_KM} km is CONSISTENT with the elbow \u2014 well-chosen."
                   if abs(diff_km) < 2.0
                   else f"eps={EPS_KM} km differs from elbow by {diff_km:+.1f} km \u2014 "
                        f"consider re-evaluating in derive_plume_data.ipynb.")
        print(f"  Sample day : {best_day}  ({len(high_px)} high-CH4 pixels)")
        print(f"  Current eps: {EPS_KM} km  |  Elbow estimate: {elbow_eps:.1f} km")
        print(f"  {verdict}")
    else:
        fig, axes = plt.subplots(1, 2, figsize=(14, 4))
        axes[0].text(0.5, 0.5, "Insufficient sample pixels", ha="center", va="center",
                     transform=axes[0].transAxes)
        print(f"  Skipped: only {len(high_px)} pixels above threshold on {best_day}")

except Exception as e:
    fig, axes = plt.subplots(1, 2, figsize=(14, 4))
    axes[0].text(0.5, 0.5, f"Error: {e}", ha="center", va="center",
                 transform=axes[0].transAxes, fontsize=8)
    print(f"  K-distance skipped: {e}")

# Right panel: cluster size histogram
ax = axes[1]
ax.hist(cluster_stats["n_pixels"], bins=30, color="steelblue", edgecolor="white", alpha=0.85)
ax.axvline(MIN_SAMPLES, color="red", ls="--", lw=1.5,
           label=f"min_samples = {MIN_SAMPLES}")
ax.axvline(cluster_stats["n_pixels"].median(), color="orange", ls="--", lw=1.5,
           label=f"median = {cluster_stats['n_pixels'].median():.0f} px")
ax.set_xlabel("Pixels per cluster")
ax.set_ylabel("Count")
ax.set_title("Cluster Size Distribution")
ax.legend(fontsize=9); ax.grid(True, alpha=0.3, axis="y")

plt.tight_layout()
plt.show()

# Parameter sensitivity table
TROPOMI_PX_KM = 5.6
print("\n=== eps Parameter Sensitivity Guide ===")
print(f"{'eps (km)':>9} | {'TROPOMI pixels':>14} | Interpretation")
print("-" * 72)
for eps_val, note in [
    (5,  "< 1 px \u2014 too tight; fragments connected plumes"),
    (7,  "1.2 px \u2014 tight; may split elongated wind-advected plumes"),
    (10, "1.8 px \u2014 current; validated for Permian Basin TROPOMI"),
    (12, "2.1 px \u2014 may merge spatially adjacent but distinct sources"),
    (15, "2.7 px \u2014 over-merging risk in dense source fields"),
]:
    marker = "  \u25c4 CURRENT" if eps_val == EPS_KM else ""
    print(f"{eps_val:>9} | {eps_val/TROPOMI_PX_KM:>14.1f} | {note}{marker}")
print(f"\nmin_samples={MIN_SAMPLES}: cluster must span \u2265{MIN_SAMPLES*3.5:.0f} km\u00b2 \u2014 "
      f"filters single-pixel hotspots.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Section 3: Plume Matching  <a id="matching"></a>
# 
# Matching is hierarchical: Tier 1 runs first; any PC plume already matched is
# excluded from Tier 2.  Each CM plume is matched to **at most one** PC plume.
# 
# | Tier | Date criterion | Spatial radius | PC/CM ratio window | Confidence |
# |------|---------------|----------------|---------------------|------------|
# | 1 | Exact same day | 30 km | 0.25 – 4.0× | High |
# | 2 | ±1 day | 50 km | 0.20 – 6.0× | Medium |
# 
# The **emission ratio filter** prevents pairing sources of wildly different magnitudes
# (e.g. a 5 kg/hr CM plume matched to a 20 000 kg/hr PC cluster are clearly not the
# same source, even if spatially proximate).

# CELL ********************

# ============================================================
# 3.1 — Matching Utility Functions  (revised)
# ============================================================

def haversine_km(lat1, lon1, lat2_arr, lon2_arr):
    """Vectorised great-circle distance (km) from one point to an array."""
    R = 6371.0
    lat1_r, lon1_r = np.radians(float(lat1)), np.radians(float(lon1))
    lat2_r = np.radians(np.asarray(lat2_arr, dtype=float))
    lon2_r = np.radians(np.asarray(lon2_arr, dtype=float))
    dlat, dlon = lat2_r - lat1_r, lon2_r - lon1_r
    a = (np.sin(dlat / 2) ** 2
         + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlon / 2) ** 2)
    return R * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def _match_score(dist_km, date_diff_days, log_emit_diff):
    """
    Geometric mean of three Gaussian decay components.
    All inputs are non-negative scalars; output is in (0, 1].

    Using geometric mean (not arithmetic) so a single very-bad dimension
    cannot be masked by two good ones.
    """
    s_sp = np.exp(-0.5 * (dist_km        / SCORE_SPATIAL_KM) ** 2)
    s_tm = np.exp(-0.5 * (date_diff_days / SCORE_TEMPORAL_D ) ** 2)
    s_em = np.exp(-0.5 * (log_emit_diff  / SCORE_EMIT_OOM   ) ** 2)
    return float((s_sp * s_tm * s_em) ** (1.0 / 3.0))


def _run_tier(pc_df, cm_df, spatial_km, date_delta, ratio_bounds,
              exclude_cm_idx, exclude_pc_ids, tier_label):
    """
    DEPRECATED signature kept for backward compatibility — now delegates to
    run_scored_matching().  tier_label is ignored; tiers are assigned by score.
    Called from cell 3.2 unchanged.
    """
    # Build exclusion-aware subsets
    pc_sub = pc_df[~pc_df["plume_id"].isin(exclude_pc_ids)].copy()
    cm_sub = cm_df[~cm_df.index.isin(exclude_cm_idx)].copy()
    matched_sub = run_scored_matching(pc_sub, cm_sub)
    if matched_sub.empty:
        return [], exclude_cm_idx, exclude_pc_ids

    used_cm_new    = exclude_cm_idx | set(
        cm_df[cm_df["plume_id"].isin(matched_sub["cm_plume_id"])].index)
    matched_pc_new = exclude_pc_ids | set(matched_sub["pc_plume_id"])

    # Convert to list-of-dicts for compatibility with the original caller
    rows = matched_sub.to_dict(orient="records")
    return rows, used_cm_new, matched_pc_new


def run_scored_matching(pc_df, cm_df):
    used_cm, rows = set(), []

    for _, pc_row in pc_df.iterrows():
        # Gate 1: date window
        lo    = pc_row["date"] - pd.Timedelta(days=OUTER_DATE_DELTA)
        hi    = pc_row["date"] + pd.Timedelta(days=OUTER_DATE_DELTA)
        cands = cm_df[(cm_df["date"] >= lo) & (cm_df["date"] <= hi)].copy()
        if cands.empty:
            continue

        # Gate 2: spatial
        dists   = haversine_km(pc_row["latitude"], pc_row["longitude"],
                               cands["latitude"].values, cands["longitude"].values)
        sp_mask = dists <= OUTER_SPATIAL_KM
        cands   = cands.iloc[sp_mask]          # <-- iloc with numpy bool array
        dists   = dists[sp_mask]
        if cands.empty:
            continue

        # Gate 3: log-emission
        pc_log    = np.log10(max(float(pc_row["emission_kg_hr"]), 1e-3))
        cm_logs   = np.log10(cands["emission_kg_hr"].clip(lower=1e-3).values)
        log_diffs = np.abs(pc_log - cm_logs)
        em_mask   = log_diffs <= LOG_EMIT_TOL
        cands     = cands.iloc[em_mask]        # <-- iloc with numpy bool array
        dists     = dists[em_mask]
        log_diffs = log_diffs[em_mask]
        cm_logs   = cm_logs[em_mask]
        if cands.empty:
            continue

        # Remove already-matched CM plumes
        avail     = np.array([idx not in used_cm for idx in cands.index])
        cands     = cands.iloc[avail]          # <-- numpy bool, no .values needed
        dists     = dists[avail]
        log_diffs = log_diffs[avail]
        cm_logs   = cm_logs[avail]
        if cands.empty:
            continue

        # Score every remaining candidate
        date_diffs = np.array([abs((pc_row["date"] - r["date"]).days)
                               for _, r in cands.iterrows()])
        scores     = np.array([_match_score(dists[i], date_diffs[i], log_diffs[i])
                               for i in range(len(cands))])

        best_i  = int(np.argmax(scores))
        cm_idx  = cands.index[best_i]
        cm_row  = cands.iloc[best_i]
        best_sc = float(scores[best_i])

        tier = (1 if best_sc >= SCORE_TIER1_MIN else
                2 if best_sc >= SCORE_TIER2_MIN else 3)

        rows.append({
            "pc_plume_id":          pc_row["plume_id"],
            "pc_date":              pc_row["date"],
            "pc_lat":               float(pc_row["latitude"]),
            "pc_lon":               float(pc_row["longitude"]),
            "pc_emission_kg_hr":    float(pc_row["emission_kg_hr"]),
            "pc_uncertainty_kg_hr": float(pc_row.get("uncertainty_kg_hr", np.nan)),
            "cm_plume_id":          cm_row["plume_id"],
            "cm_date":              cm_row["date"],
            "cm_lat":               float(cm_row["latitude"]),
            "cm_lon":               float(cm_row["longitude"]),
            "cm_emission_kg_hr":    float(cm_row["emission_kg_hr"]),
            "cm_uncertainty_kg_hr": float(cm_row.get("uncertainty_kg_hr", np.nan)),
            "distance_km":          float(dists[best_i]),
            "date_delta_days":      int(date_diffs[best_i]),
            "log_emit_diff":        float(log_diffs[best_i]),
            "emission_ratio":       float(10 ** (pc_log - float(cm_logs[best_i]))),
            "match_score":          best_sc,
            "match_tier":           tier,
        })
        used_cm.add(cm_idx)

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows).sort_values("match_score", ascending=False).reset_index(drop=True)
    q1, q3 = out["log_emit_diff"].quantile([0.25, 0.75])
    out["is_outlier"] = out["log_emit_diff"] > (q3 + 1.5 * (q3 - q1))
    return out


print("Matching utilities loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# MATCHING DIAGNOSTIC — run before Section 3.2
# ============================================================
# Define overlap slices here so this cell can run independently of 3.2
pc_ov = pc[(pc["date"] >= overlap_start) & (pc["date"] <= overlap_end)].copy()
cm_ov = cm[(cm["date"] >= overlap_start) & (cm["date"] <= overlap_end)].copy()

print("=== Date alignment ===")
pc_dates_in_window = set(pc_ov["date"])
cm_dates_in_window = set(cm_ov["date"])
shared_dates = pc_dates_in_window & cm_dates_in_window
print(f"PC dates in window  : {len(pc_dates_in_window)}")
print(f"CM dates in window  : {len(cm_dates_in_window)}")
print(f"Shared dates        : {len(shared_dates)}")
if not shared_dates:
    print("  ⚠  ZERO shared dates — likely tz-aware vs tz-naive mismatch.")
    print(f"  PC sample: {repr(sorted(pc_dates_in_window)[0])}")
    print(f"  CM sample: {repr(sorted(cm_dates_in_window)[0])}")

print("\n=== Emission scale (log) ===")
pc_log_med = np.log10(pc_ov["emission_kg_hr"].median())
cm_log_med = np.log10(cm_ov["emission_kg_hr"].median())
print(f"PC log10 median     : {pc_log_med:.2f}  ({pc_ov['emission_kg_hr'].median():,.0f} kg/hr)")
print(f"CM log10 median     : {cm_log_med:.2f}  ({cm_ov['emission_kg_hr'].median():,.0f} kg/hr)")
print(f"log10 gap           : {pc_log_med - cm_log_med:+.2f} OOM  "
      f"(tolerance: ±{LOG_EMIT_TOL}  →  "
      f"{'WITHIN bounds' if abs(pc_log_med - cm_log_med) <= LOG_EMIT_TOL else 'OUTSIDE bounds — widen LOG_EMIT_TOL'})")
print("Note: PC IME is field-scale (cluster of sources); CM is single-plume.")
print("      This gap is physical, not a unit error.  Log-space tolerance is correct.")

print("\n=== Filter funnel (outer gates only) ===")
# Gate 1: date
date_cands = 0
for _, pc_row in pc_ov.iterrows():
    lo = pc_row["date"] - pd.Timedelta(days=OUTER_DATE_DELTA)
    hi = pc_row["date"] + pd.Timedelta(days=OUTER_DATE_DELTA)
    date_cands += len(cm_ov[(cm_ov["date"] >= lo) & (cm_ov["date"] <= hi)])

# Gate 2: spatial (any date)
cm_rad = np.radians(cm_ov[["latitude", "longitude"]].to_numpy())
pc_rad = np.radians(pc_ov[["latitude", "longitude"]].to_numpy())
nn_km  = BallTree(cm_rad, metric="haversine").query(pc_rad, k=1)[0][:, 0] * 6371.0
spatial_cands = int((nn_km <= OUTER_SPATIAL_KM).sum())

print(f"After date gate  (±{OUTER_DATE_DELTA}d)      : {date_cands:,} candidate pairs")
print(f"PC within spatial gate ({OUTER_SPATIAL_KM:.0f} km): {spatial_cands:,} / {len(pc_ov):,} PC plumes")
print(f"  (median PC→CM distance: {np.median(nn_km):.1f} km  |  "
      f"75th-pct: {np.percentile(nn_km, 75):.1f} km)")

if date_cands == 0:
    print("\n  ✗ DOMINANT FAILURE: DATE GATE — fix date normalisation first.")
elif spatial_cands == 0:
    print("\n  ✗ DOMINANT FAILURE: SPATIAL GATE — check coordinates are decimal degrees.")
else:
    print(f"\n  ✓ Candidates survive to scorer.  Running full match...")

print("\n=== Manual probe on first shared date ===")
if shared_dates:
    test_date = sorted(shared_dates)[0]
    pc_test = pc_ov[pc_ov["date"] == test_date]
    cm_test = cm_ov[cm_ov["date"] == test_date]
    print(f"Date: {test_date.date()}  |  PC: {len(pc_test)}  |  CM: {len(cm_test)}")
    print(f"  {'PC_ID':>8}  {'PC_emit':>10}  {'nearest_CM_km':>14}  "
          f"{'CM_emit':>10}  {'log_diff':>9}  {'within_gate'}")
    for _, pr in pc_test.head(5).iterrows():
        dists = haversine_km(pr["latitude"], pr["longitude"],
                             cm_test["latitude"].values, cm_test["longitude"].values)
        bi    = np.argmin(dists)
        cm_b  = cm_test.iloc[bi]
        ld    = abs(np.log10(max(pr["emission_kg_hr"], 1e-3)) -
                    np.log10(max(cm_b["emission_kg_hr"], 1e-3)))
        ok    = "✓" if dists[bi] <= OUTER_SPATIAL_KM and ld <= LOG_EMIT_TOL else "✗"
        print(f"  {str(pr['plume_id']):>8}  {pr['emission_kg_hr']:>10,.0f}  "
              f"{dists[bi]:>14.1f}  {cm_b['emission_kg_hr']:>10,.0f}  "
              f"{ld:>9.2f}  {ok}")
else:
    print("  No shared dates — temporal gate will use ±3 day window.")
    print("  First PC date:", sorted(pc_dates_in_window)[0].date(),
          " | First CM date:", sorted(cm_dates_in_window)[0].date())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 3.2 — Matching  (score-based, replaces hierarchical tiers)
# ============================================================

pc_ov = pc[(pc["date"] >= overlap_start) & (pc["date"] <= overlap_end)].copy()
cm_ov = cm[(cm["date"] >= overlap_start) & (cm["date"] <= overlap_end)].copy()

print(f"Overlap window : {overlap_start.date()} → {overlap_end.date()}")
print(f"PC plumes      : {len(pc_ov):,}")
print(f"CM plumes      : {len(cm_ov):,}")

matched = run_scored_matching(pc_ov, cm_ov)

if matched.empty:
    print("\nNo matches found. Re-run the MATCHING DIAGNOSTIC cell above — "
          "the dominant failure gate is identified there.")
else:
    t1 = matched[matched["match_tier"] == 1]
    t2 = matched[matched["match_tier"] == 2]
    t3 = matched[matched["match_tier"] == 3]
    print(f"\n=== Matching Results ===")
    print(f"Total matched pairs        : {len(matched)}")
    print(f"  Tier 1 (score ≥ {SCORE_TIER1_MIN:.2f})      : {len(t1)}  ({100*len(t1)/len(matched):.1f}%)")
    print(f"  Tier 2 (score ≥ {SCORE_TIER2_MIN:.2f})      : {len(t2)}  ({100*len(t2)/len(matched):.1f}%)")
    print(f"  Tier 3 (low-confidence)  : {len(t3)}  ({100*len(t3)/len(matched):.1f}%)")
    print(f"Unmatched PC plumes        : {len(pc_ov) - len(matched):,}")
    print(f"Unmatched CM plumes        : {len(cm_ov) - len(matched):,}")
    print(f"\nMatch score:    median={matched['match_score'].median():.3f}  "
          f"min={matched['match_score'].min():.3f}")
    print(f"Distance (km):  median={matched['distance_km'].median():.1f}  "
          f"max={matched['distance_km'].max():.1f}")
    print(f"Date Δ (days):  median={matched['date_delta_days'].median():.0f}  "
          f"max={matched['date_delta_days'].max()}")
    print(f"|log10 PC/CM|:  median={matched['log_emit_diff'].median():.2f}  "
          f"max={matched['log_emit_diff'].max():.2f}")
    print(f"Emit ratio:     median={matched['emission_ratio'].median():.1f}x  "
          f"max={matched['emission_ratio'].max():.1f}x")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 3.3 — Match Diagnostics: Why Is the Match Rate Low?
# ============================================================

print("=" * 65)
print("ROOT CAUSE ANALYSIS: Match Rate Diagnostics")
print("=" * 65)

# 1. Temporal overlap
pc_dates = set(pc_ov["date"])
cm_dates = set(cm_ov["date"])
common_ov = pc_dates & cm_dates
print(f"\n1. TEMPORAL OVERLAP")
print(f"   PC unique dates : {len(pc_dates)}")
print(f"   CM unique dates : {len(cm_dates)}")
print(f"   Common dates    : {len(common_ov)} ({100*len(common_ov)/max(len(pc_dates),1):.1f}% of PC dates)")
for name, dates in [("PC", sorted(pc_dates)), ("CM", sorted(cm_dates))]:
    if len(dates) > 1:
        gaps = [(dates[i+1]-dates[i]).days for i in range(len(dates)-1)]
        print(f"   {name} max gap : {max(gaps)} days | mean : {np.mean(gaps):.1f} days")

# 2. Geographic distribution
print(f"\n2. GEOGRAPHIC DISTRIBUTION")
for name, df in [("PC", pc_ov), ("CM", cm_ov)]:
    print(f"   {name}: lat [{df['latitude'].min():.1f}, {df['latitude'].max():.1f}]  "
          f"lon [{df['longitude'].min():.1f}, {df['longitude'].max():.1f}]  n={len(df):,}")

# 3. Nearest-neighbour spatial analysis (any date)
print(f"\n3. NEAREST-NEIGHBOUR DISTANCES (spatial, any date)")
cm_rad = np.radians(cm_ov[["latitude", "longitude"]].to_numpy())
pc_rad = np.radians(pc_ov[["latitude", "longitude"]].to_numpy())
nn_km  = BallTree(cm_rad, metric="haversine").query(pc_rad, k=1)[0][:, 0] * 6371.0
for pct in [25, 50, 75, 95]:
    print(f"   PC\u2192CM {pct}th-pct nearest : {np.percentile(nn_km, pct):.1f} km")
print(f"   % of PC within {TIER1_SPATIAL_KM} km of any CM: {100*(nn_km<=TIER1_SPATIAL_KM).mean():.1f}%")
print(f"   % of PC within {TIER2_SPATIAL_KM} km of any CM: {100*(nn_km<=TIER2_SPATIAL_KM).mean():.1f}%")

# 4. Detection scale
print(f"\n4. SENSOR DETECTION SCALE")
print(f"   PC detects {len(pc_ov)/max(len(cm_ov),1):.1f}x more plumes than CM")
print(f"   PC median emission : {pc_ov['emission_kg_hr'].median():,.0f} kg/hr")
print(f"   CM median emission : {cm_ov['emission_kg_hr'].median():,.0f} kg/hr")

print(f"\n5. INTERPRETATION")
print("   Low match rate is EXPECTED for cross-sensor comparison:")
print("   \u2022 TROPOMI (PC) \u2014 satellite, daily global revisit, ~5\u00d73.5 km pixel")
print("   \u2022 Carbon Mapper \u2014 aircraft, targeted campaigns, high spatial resolution")
print("   \u2022 Different sensors detect different source size classes")
print("   \u2192 Matched pairs characterise SYSTEMATIC BIAS, not coverage equivalence")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Section 4: Accuracy Evaluation  <a id="accuracy"></a>
# 
# | Metric | Formula | Interpretation |
# |--------|---------|----------------|
# | MAE | mean|PC − CM| | Average absolute error magnitude |
# | RMSE | √mean(PC−CM)² | Penalises large errors more |
# | Bias | mean(PC − CM) | Positive = systematic overestimate |
# | Relative bias | mean((PC−CM)/CM) | Normalised; comparable across studies |
# 
# **Acceptance criterion**: relative bias ≤ ±50% is acceptable for satellite IME
# estimates (Varon et al. 2018).  Higher values trigger root cause investigation
# (Section 6).

# CELL ********************

# ============================================================
# 4.1 — Accuracy Metrics — All Matched Pairs
# ============================================================

if matched.empty:
    print("No matched pairs.")
else:
    pc_v = matched["pc_emission_kg_hr"].values
    cm_v = matched["cm_emission_kg_hr"].values
    mae      = float(mean_absolute_error(cm_v, pc_v))
    rmse     = float(np.sqrt(mean_squared_error(cm_v, pc_v)))
    bias     = float((pc_v - cm_v).mean())
    rel_bias = float(((pc_v - cm_v) / cm_v).mean() * 100)

    unc_rows = matched.dropna(subset=["pc_uncertainty_kg_hr", "cm_uncertainty_kg_hr"])
    if len(unc_rows) > 0:
        within_pct = float(
            ((unc_rows["pc_emission_kg_hr"]
              >= unc_rows["cm_emission_kg_hr"] - unc_rows["cm_uncertainty_kg_hr"]) &
             (unc_rows["pc_emission_kg_hr"]
              <= unc_rows["cm_emission_kg_hr"] + unc_rows["cm_uncertainty_kg_hr"])).mean() * 100
        )
    else:
        within_pct = float("nan")

    print("=== IME Model vs Carbon Mapper \u2014 All Matched Pairs ===\n")
    print(f"{'Metric':<38} {'Value':>14}")
    print("-" * 54)
    for label, val in [
        ("Matched pairs",             f"{len(matched)}"),
        ("MAE (kg/hr)",               f"{mae:,.0f}"),
        ("RMSE (kg/hr)",              f"{rmse:,.0f}"),
        ("Mean bias (kg/hr)",         f"{bias:+,.0f}"),
        ("Relative bias (%)",         f"{rel_bias:+.1f}%"),
        ("Within CM uncertainty (%)", f"{within_pct:.1f}%" if not np.isnan(within_pct) else "N/A"),
    ]:
        print(f"{label:<38} {val:>14}")

    if abs(rel_bias) <= 50:
        status = "ACCEPTABLE \u2014 within IME method uncertainty (Varon et al. 2018)"
    elif abs(rel_bias) <= 100:
        status = "MARGINAL \u2014 exceeds expected IME uncertainty; investigate (Section 6)"
    else:
        status = "SIGNIFICANT \u2014 investigate root cause (Section 6)"
    print(f"\nAssessment : {status}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 4.2 — Tier-Stratified Accuracy Metrics
# ============================================================

if matched.empty:
    print("No matched pairs.")
else:
    def _tier_metrics(df):
        if len(df) < 2:
            return {k: float("nan") for k in
                    ["n", "mae", "rmse", "bias", "rel_bias", "ratio_mean", "dist_mean"]}
        pc = df["pc_emission_kg_hr"].values
        cm = df["cm_emission_kg_hr"].values
        return {
            "n":          len(df),
            "mae":        float(mean_absolute_error(cm, pc)),
            "rmse":       float(np.sqrt(mean_squared_error(cm, pc))),
            "bias":       float((pc - cm).mean()),
            "rel_bias":   float(((pc - cm) / cm).mean() * 100),
            "ratio_mean": float(df["emission_ratio"].mean()),
            "dist_mean":  float(df["distance_km"].mean()),
        }

    t1, t2 = matched[matched["match_tier"] == 1], matched[matched["match_tier"] == 2]
    m1, m2, ma = _tier_metrics(t1), _tier_metrics(t2), _tier_metrics(matched)

    def _f(v, fmt):
        return "N/A" if v != v else format(v, fmt.strip())

    print("=== Accuracy by Confidence Tier ===\n")
    hdr = f"{'Metric':<30} {'Tier 1 (High)':>15} {'Tier 2 (Med)':>14} {'Combined':>11}"
    print(hdr)
    print("-" * len(hdr))

    rows = [
        ("Pairs",              _f(m1["n"],          ".0f"),   _f(m2["n"],          ".0f"),   _f(ma["n"],          ".0f")),
        ("MAE (kg/hr)",        _f(m1["mae"],         ",.0f"),  _f(m2["mae"],         ",.0f"),  _f(ma["mae"],         ",.0f")),
        ("RMSE (kg/hr)",       _f(m1["rmse"],        ",.0f"),  _f(m2["rmse"],        ",.0f"),  _f(ma["rmse"],        ",.0f")),
        ("Bias (kg/hr)",       _f(m1["bias"],        "+,.0f"), _f(m2["bias"],        "+,.0f"), _f(ma["bias"],        "+,.0f")),
        ("Relative bias",      _f(m1["rel_bias"],    "+.1f") + "%", _f(m2["rel_bias"], "+.1f") + "%", _f(ma["rel_bias"], "+.1f") + "%"),
        ("Emission ratio (x)", _f(m1["ratio_mean"],  ".2f"),   _f(m2["ratio_mean"],  ".2f"),   _f(ma["ratio_mean"],  ".2f")),
        ("Avg distance (km)",  _f(m1["dist_mean"],   ".1f"),   _f(m2["dist_mean"],   ".1f"),   _f(ma["dist_mean"],   ".1f")),
    ]

    for label, v1, v2, va in rows:
        print(f"{label:<30} {v1:>15} {v2:>14} {va:>11}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Section 5: Visualizations  <a id="viz"></a>

# CELL ********************

# ============================================================
# 5.1 — Emission Scatter and Residuals (Tier-Stratified)
# ============================================================

if matched.empty:
    print("No matched pairs.")
else:
    t1 = matched[matched["match_tier"] == 1]
    t2 = matched[matched["match_tier"] == 2]
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    ax = axes[0]
    if not t1.empty:
        ax.scatter(t1["cm_emission_kg_hr"], t1["pc_emission_kg_hr"],
                   c="#2ecc71", s=90, alpha=0.85, edgecolors="darkgreen", lw=1.2,
                   label=f"Tier 1 \u2014 exact date (n={len(t1)})", zorder=3)
    if not t2.empty:
        ax.scatter(t2["cm_emission_kg_hr"], t2["pc_emission_kg_hr"],
                   c="#f39c12", s=60, alpha=0.65, edgecolors="darkorange", lw=0.8,
                   marker="^", label=f"Tier 2 \u2014 \u00b11 day (n={len(t2)})", zorder=2)
    if "is_outlier" in matched.columns and matched["is_outlier"].any():
        out = matched[matched["is_outlier"]]
        ax.scatter(out["cm_emission_kg_hr"], out["pc_emission_kg_hr"],
                   s=140, facecolors="none", edgecolors="red", lw=2,
                   label="Outlier (IQR)", zorder=4)
    lim = max(matched["cm_emission_kg_hr"].max(), matched["pc_emission_kg_hr"].max()) * 1.15
    ax.plot([0,lim],[0,lim],"k--",lw=1,alpha=0.5,label="1:1 line")
    ax.fill_between([0,lim],[0,0],[lim*1.5,lim*1.5],alpha=0.06,color="green")
    ax.fill_between([0,lim],[0,0],[lim*0.5,lim*0.5],alpha=0.06,color="green",label="\u00b150% band")
    ax.set_xlim(0,lim); ax.set_ylim(0,lim*1.2)
    ax.set_xlabel("Carbon Mapper emission (kg/hr)", fontsize=11)
    ax.set_ylabel("PC IME-derived emission (kg/hr)", fontsize=11)
    ax.set_title("PC vs CM Emission Rates", fontsize=12, fontweight="bold")
    ax.legend(fontsize=9); ax.grid(True, alpha=0.25)

    ax = axes[1]
    res = matched["pc_emission_kg_hr"] - matched["cm_emission_kg_hr"]
    tier_colors = matched["match_tier"].map({1:"#2ecc71", 2:"#f39c12", 3:"#aaaaaa"}).tolist()
    ax.scatter(matched["cm_emission_kg_hr"], res,
                c=tier_colors, s=55, alpha=0.75, edgecolors="grey", lw=0.5)
    ax.axhline(0,         color="black", ls="--", lw=1)
    ax.axhline(res.mean(),color="red",   ls=":",  lw=1.8,
               label=f"Mean bias: {res.mean():+,.0f} kg/hr")
    ax.fill_between([0, matched["cm_emission_kg_hr"].max()*1.05],
                    res.mean()-res.std(), res.mean()+res.std(),
                    alpha=0.10, color="red", label="\u00b11 std")
    ax.set_xlabel("Carbon Mapper emission (kg/hr)", fontsize=11)
    ax.set_ylabel("Residual: PC \u2212 CM (kg/hr)", fontsize=11)
    ax.set_title("Residuals vs Reference Emission", fontsize=12, fontweight="bold")
    ax.legend(fontsize=9); ax.grid(True, alpha=0.25)

    plt.suptitle("Emission Accuracy: IME (PC) vs Carbon Mapper",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 5.2 — Distance, Ratio, and Residual Distributions
# ============================================================

if matched.empty:
    print("No matched pairs.")
else:
    t1 = matched[matched["match_tier"]==1]
    t2 = matched[matched["match_tier"]==2]
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))
    kw1 = dict(alpha=0.75, color="#2ecc71", edgecolor="darkgreen", linewidth=1.2)
    kw2 = dict(alpha=0.60, color="#f39c12", edgecolor="darkorange", linewidth=1.0)

    ax = axes[0]
    if not t1.empty: ax.hist(t1["distance_km"], bins=12, **kw1, label="Tier 1")
    if not t2.empty: ax.hist(t2["distance_km"], bins=12, **kw2, label="Tier 2")
    ax.axvline(TIER1_SPATIAL_KM, color="#2ecc71", ls="--", lw=1.5,
               label=f"T1 limit ({TIER1_SPATIAL_KM} km)")
    ax.axvline(TIER2_SPATIAL_KM, color="#f39c12", ls="--", lw=1.5,
               label=f"T2 limit ({TIER2_SPATIAL_KM} km)")
    ax.set_xlabel("Match distance (km)"); ax.set_ylabel("Count")
    ax.set_title("Spatial Distance"); ax.legend(fontsize=8); ax.grid(True, alpha=0.3, axis="y")

    ax = axes[1]
    if not t1.empty: ax.hist(t1["emission_ratio"], bins=12, **kw1, label="Tier 1")
    if not t2.empty: ax.hist(t2["emission_ratio"], bins=12, **kw2, label="Tier 2")
    ax.axvline(1.0, color="black", ls="--", lw=2, label="Perfect agreement")
    ax.axvline(matched["emission_ratio"].mean(), color="red", ls=":", lw=1.5,
               label=f"Mean {matched['emission_ratio'].mean():.2f}x")
    ax.set_xlabel("PC / CM emission ratio"); ax.set_ylabel("Count")
    ax.set_title("Emission Ratio Distribution"); ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3, axis="y")

    ax = axes[2]
    res = matched["pc_emission_kg_hr"] - matched["cm_emission_kg_hr"]
    ax.hist(res, bins=15, edgecolor="black", alpha=0.78, color="steelblue")
    ax.axvline(0,                color="black",  ls="--", lw=1)
    ax.axvline(res.mean(),       color="red",    ls="--", lw=2,
               label=f"Mean {res.mean():+.0f}")
    ax.axvline(float(np.median(res)), color="orange", ls="--", lw=1.5,
               label=f"Median {np.median(res):+.0f}")
    ax.set_xlabel("Residual: PC \u2212 CM (kg/hr)"); ax.set_ylabel("Count")
    ax.set_title("Residual Distribution"); ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3, axis="y")

    plt.suptitle("Match Quality Distributions", fontsize=12, fontweight="bold")
    plt.tight_layout()
    plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 5.3 — Geographic Distribution: Matched vs Unmatched
# ============================================================

if matched.empty:
    print("No matched pairs.")
else:
    fig, ax = plt.subplots(figsize=(13, 7))

    m_pc_ids = set(matched["pc_plume_id"])
    m_cm_ids = set(matched["cm_plume_id"])

    ax.scatter(pc_ov.loc[~pc_ov["plume_id"].isin(m_pc_ids),"longitude"],
               pc_ov.loc[~pc_ov["plume_id"].isin(m_pc_ids),"latitude"],
               s=5, alpha=0.20, color="steelblue",
               label=f"PC unmatched (n={len(pc_ov)-len(matched):,})")
    ax.scatter(cm_ov.loc[~cm_ov["plume_id"].isin(m_cm_ids),"longitude"],
               cm_ov.loc[~cm_ov["plume_id"].isin(m_cm_ids),"latitude"],
               s=22, alpha=0.40, color="tomato", marker="^",
               label=f"CM unmatched (n={len(cm_ov)-len(matched):,})")

    t1 = matched[matched["match_tier"]==1]
    t2 = matched[matched["match_tier"]==2]

    for _, row in t1.iterrows():
        ax.plot([row["pc_lon"],row["cm_lon"]],[row["pc_lat"],row["cm_lat"]],
                color="#2ecc71", alpha=0.65, lw=1.2, zorder=3)
    for _, row in t2.iterrows():
        ax.plot([row["pc_lon"],row["cm_lon"]],[row["pc_lat"],row["cm_lat"]],
                color="#f39c12", alpha=0.50, lw=0.9, zorder=3)

    if not t1.empty:
        ax.scatter(t1["pc_lon"], t1["pc_lat"], s=65, color="#2ecc71",
                   edgecolors="darkgreen", lw=1.2, zorder=4,
                   label=f"Tier 1 matched (n={len(t1)})")
    if not t2.empty:
        ax.scatter(t2["pc_lon"], t2["pc_lat"], s=45, color="#f39c12",
                   edgecolors="darkorange", lw=0.9, marker="^", zorder=4,
                   label=f"Tier 2 matched (n={len(t2)})")

    ax.set_xlim(BBOX["lon_min"]-0.5, BBOX["lon_max"]+0.5)
    ax.set_ylim(BBOX["lat_min"]-0.5, BBOX["lat_max"]+0.5)
    ax.set_xlabel("Longitude", fontsize=11)
    ax.set_ylabel("Latitude",  fontsize=11)
    ax.set_title("Matched vs Unmatched Plumes (lines connect PC\u2013CM pairs)",
                 fontsize=12, fontweight="bold")
    ax.legend(markerscale=1.5, fontsize=9, loc="lower right")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Section 6: Root Cause Analysis  <a id="rootcause"></a>
# 
# The IME emission formula is:
# 
# $$Q \; [\text{kg/hr}] = \frac{U \; [\text{m/s}] \times \text{IME} \; [\text{kg}]}{L \; [\text{m}]} \times 3600$$
# 
# If PC systematically overestimates relative to CM, the driver is one or more of:
# 
# | Component | How it inflates Q | How to test |
# |-----------|-------------------|-------------|
# | **Plume length (L)** | Too short \u2192 Q \u221d 1/L | Check % of plumes at minimum floor (33.6 km) |
# | **IME** | Background underestimated \u2192 enhancement inflated | Compare 25th-pct background to TROPOMI L4 |
# | **Wind speed (U)** | Station wind > true plume-height wind | Compare weather-station vs ERA5 reanalysis |
# | **Cluster splitting** | One CM plume \u2192 N small PC clusters | Compare cluster extents to CM plume sizes |

# CELL ********************

# ============================================================
# 6.1 — Systematic Bias Decomposition
# ============================================================

print("=" * 65)
print("BIAS DECOMPOSITION")
print("=" * 65)

if matched.empty:
    print("No matched pairs.")
else:
    pc_med = float(matched["pc_emission_kg_hr"].median())
    cm_med = float(matched["cm_emission_kg_hr"].median())
    ratio  = pc_med / cm_med if cm_med > 0 else float("nan")
    print(f"\nPC median emission : {pc_med:,.0f} kg/hr")
    print(f"CM median emission : {cm_med:,.0f} kg/hr")
    print(f"Median PC/CM ratio : {ratio:.2f}x  ({100*(ratio-1):+.0f}%)")

    # Replace with:
    MIN_LEN_M = 2 * 5600   # 11 200 m — updated floor (was 6 × 5600)

    print("\n--- Component 1: Plume Length (L) ---")
    pct_below_floor = float(
        (cluster_stats["extent_km"] < MIN_LEN_M / 1000).mean() * 100
    )
    print(f"   Pipeline minimum floor : {MIN_LEN_M/1000:.1f} km")
    print(f"   Cluster extent median  : {cluster_stats['extent_km'].median():.1f} km")
    print(f"   Clusters below floor   : {pct_below_floor:.1f}%")
    if pct_below_floor > 30:
        print(f"   WARNING: {pct_below_floor:.0f}% of clusters are smaller than the floor.")
        print("   The floor value dominates most plume length estimates,")
        print("   inflating emission rates when true plumes are short.")
        print("   ACTION: re-evaluate MIN_PLUME_LENGTH_M in derive_emissions_data.ipynb")
    else:
        print("   OK: most plumes use PCA-computed length (not the floor).")

    print("\n--- Component 2: Background CH4 & IME ---")
    print("   Pipeline: daily 25th percentile of all TROPOMI pixels")
    print("   Observed: 1 884\u20131 933 ppb (from derive_emissions_data output)")
    print("   Atmospheric baseline (Permian Basin 2026): ~1 850\u20131 880 ppb")
    print("   If pipeline background > true baseline: enhancement is UNDERESTIMATED \u2192 lower IME")
    print("   ACTION: cross-check against TROPOMI L4CH4 gridded product.")

    print("\n--- Component 3: Wind Speed (U) ---")
    print("   Pipeline: daily-average ground-station wind (km/h \u00f7 3.6 \u2192 m/s)")
    print("   Observed: mean 7.4 m/s, range 3.5\u201311.4 m/s")
    print("   CM likely uses: ERA5 reanalysis or aircraft-measured wind at plume altitude")
    print("   Known issue: station surface wind < plume-height wind (boundary-layer effect)")
    print("   ACTION: replace with ERA5 925 hPa (\u2248750 m) daily wind for study area.")

    print("\n--- Component 4: DBSCAN Cluster Splitting ---")
    print(f"   PC cluster size median: {cluster_stats['n_pixels'].median():.0f} pixels")
    print("   If one large CM source = N small PC clusters:")
    print("   Each PC cluster has smaller IME and shorter L \u2192 net effect on Q is ambiguous.")
    print("   ACTION: inspect Section 5.3 map for clusters of PC points near a single CM plume.")

    print("\n" + "=" * 65)
    print("MOST LIKELY CAUSE RANKING")
    print("=" * 65)
    if ratio > 2.5:
        print(f"  1. Plume length floor ({MIN_LEN_M/1000:.1f} km) \u2014 strongest candidate (can explain 2\u20133x bias)")
        print("  2. Wind speed (surface station vs ERA5) \u2014 secondary (~10\u201320% effect)")
        print("  3. Background CH4 \u2014 minor (<5% given observed range)")
    elif ratio > 1.5:
        print(f"  Moderate overestimate ({ratio:.2f}x). Check plume length distribution.")
        print("  Run derive_emissions_data.ipynb and inspect plume_length_m histogram.")
    else:
        print("  Bias within IME method uncertainty \u2014 no dominant single cause identified.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Section 7: Summary & Export  <a id="summary"></a>

# CELL ********************

# ============================================================
# 7.1 — Compact Metrics Summary + Actionable Recommendations
# ============================================================

metrics = {
    "study_region": "Permian Basin",
    "period":       f"{CM_START_DATE[:10]} to {CM_END_DATE[:10]}",
    "dbscan":       {"eps_km": EPS_KM, "min_samples": MIN_SAMPLES},
    "data": {
        "pc_plumes":    int(len(pc)),
        "cm_plumes":    int(len(cm)),
        "pc_dates":     int(pc["date"].nunique()),
        "cm_dates":     int(cm["date"].nunique()),
        "common_dates": int(common_dates),
    },
}

if not matched.empty:
    pc_v = matched["pc_emission_kg_hr"].values
    cm_v = matched["cm_emission_kg_hr"].values
    t1   = matched[matched["match_tier"]==1]
    t2   = matched[matched["match_tier"]==2]

    metrics["matching"] = {
        "total_pairs": int(len(matched)),
        "tier1_pairs": int(len(t1)),
        "tier2_pairs": int(len(t2)),
        "tier1_pct":   round(100*len(t1)/len(matched), 1),
    }
    metrics["accuracy_all"] = {
        "mae_kg_hr":    round(float(mean_absolute_error(cm_v, pc_v))),
        "rmse_kg_hr":   round(float(np.sqrt(mean_squared_error(cm_v, pc_v)))),
        "bias_kg_hr":   round(float((pc_v-cm_v).mean())),
        "rel_bias_pct": round(float(((pc_v-cm_v)/cm_v).mean()*100), 1),
    }
    if len(t1) >= 2:
        metrics["accuracy_tier1"] = {
            "rmse_kg_hr":   round(float(np.sqrt(mean_squared_error(
                t1["cm_emission_kg_hr"], t1["pc_emission_kg_hr"])))),
            "rel_bias_pct": round(float(
                ((t1["pc_emission_kg_hr"]-t1["cm_emission_kg_hr"])
                 /t1["cm_emission_kg_hr"]).mean()*100), 1),
        }
    if "is_outlier" in matched.columns:
        metrics["outliers"] = {
            "count": int(matched["is_outlier"].sum()),
            "pct":   round(float(100*matched["is_outlier"].mean()), 1),
        }

print(json.dumps(metrics, indent=2))

if not matched.empty:
    rb        = metrics["accuracy_all"]["rel_bias_pct"]
    t1_pct    = metrics["matching"]["tier1_pct"]
    n_out     = metrics.get("outliers", {}).get("count", 0)
    MIN_LEN_M = 2 * 5600

    print("\n" + "=" * 65)
    print("RECOMMENDATIONS")
    print("=" * 65)

    print(f"\n1. CALIBRATION CONFIDENCE")
    if t1_pct >= 50:
        print(f"   STRONG: {t1_pct:.0f}% of matches are same-day Tier 1.")
        print("   \u2192 Tier 1 RMSE is a reliable accuracy baseline for reporting.")
    else:
        print(f"   WEAK: only {t1_pct:.0f}% same-day matches.")
        print("   \u2192 Expand CM API date range or increase PC observation frequency.")

    print(f"\n2. BIAS CORRECTION (current: {rb:+.1f}%)")
    if abs(rb) > 50:
        pct_floor = float((cluster_stats["extent_km"] < MIN_LEN_M/1000).mean()*100)
        print("   Priority actions:")
        if pct_floor > 30:
            print(f"   a) Raise MIN_PLUME_LENGTH_M in derive_emissions_data.ipynb")
            print(f"      ({pct_floor:.0f}% of clusters are below the {MIN_LEN_M/1000:.1f} km floor)")
        print("   b) Replace weather-station wind with ERA5 925 hPa reanalysis")
        print("   c) Verify background CH4 vs TROPOMI L4CH4 gridded product")
    else:
        print("   Bias within IME method uncertainty \u2014 no systematic correction required.")

    print(f"\n3. DBSCAN PARAMETER REVIEW")
    print(f"   Current: eps={EPS_KM} km, min_samples={MIN_SAMPLES}")
    print("   \u2192 Review k-distance plot (Section 2.2) for eps validation")
    print("   \u2192 If emission bias correlates with cluster size, sweep eps \u2208 [7, 8, 9, 10, 11, 12]")
    print("   \u2192 Run the sweep in derive_plume_data.ipynb (full raw-pixel access)")

    print(f"\n4. OUTLIER HANDLING")
    if n_out > 0:
        print(f"   {n_out} pair(s) flagged \u2014 investigate:")
        print("   \u2022 Different source types (point source vs diffuse emission)")
        print("   \u2022 CM quality flags (check 'sector' column in carbon_mapper_2026)")
    else:
        print("   No outliers \u2014 emission scale agreement is consistent.")

    print(f"\n5. IMMEDIATE NEXT STEPS")
    print("   [1] In derive_emissions_data.ipynb: plot plume_length_m distribution")
    print(f"   [2] Obtain ERA5 wind for matched plume dates; recompute emission_auto")
    print("   [3] Compare TROPOMI L4CH4 background vs pipeline 25th-pct values")
    print(f"   [4] Extend CM date range beyond {CM_END_DATE[:10]} when API allows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 7.2 — Save Comparison Results to Delta Table
# ============================================================

if matched.empty:
    print("No matched pairs to save.")
else:
    save_cols = [
        "pc_plume_id", "pc_date", "pc_lat", "pc_lon",
        "pc_emission_kg_hr", "pc_uncertainty_kg_hr",
        "cm_plume_id", "cm_date", "cm_lat", "cm_lon",
        "cm_emission_kg_hr", "cm_uncertainty_kg_hr",
        "distance_km", "date_delta_days", "match_tier", "emission_ratio",
    ]
    if "is_outlier" in matched.columns:
        save_cols.append("is_outlier")

    save_df = matched[save_cols].copy().replace([np.inf, -np.inf], np.nan)

    (spark.createDataFrame(save_df)
         .write.mode("overwrite").format("delta")
         .option("overwriteSchema", "true")
         .saveAsTable(TABLE_COMPARISON))

    print(f"Saved {len(save_df):,} matched pairs \u2192 {TABLE_COMPARISON}")
    display(spark.table(TABLE_COMPARISON).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(spark.table("dbo.weather").columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
