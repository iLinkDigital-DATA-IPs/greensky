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

# # CH₄ Plume Detection Pipeline
# Consumes silver pixel table from Notebook 1 · Derives scene grouping · Detects plumes · Writes catalog

# CELL ********************

# Spark
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, LongType, TimestampType

# Scientific (used per-scene in Pandas)
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from scipy.stats import median_abs_deviation

# Viz
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
import matplotlib.cm as cm
import warnings
warnings.filterwarnings('ignore')

print('Imports OK')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CFG = {
    'silver_table':        'Planetary_computer_LH.silver.ch4_plume_ready',
    'plume_catalog_table': 'Planetary_computer_LH.silver.ch4_plume_catalog',

    'scene_gap_threshold_s': 600,

    'n_bg_neighbors': 30,
    'bg_quantile':    0.25,        # was 0.5 — lower quantile gives cleaner background
                                   # separation in regions with diffuse baseline emissions

    'k_sigma':         3.0,        # was 2.0 — 2σ produces too many false positives from
                                   # background noise; 3σ is the satellite CH4 literature standard
    'noise_floor_ppb': 5.0,

    'connectivity_radius_km': 8.0, # was 100.0 — TROPOMI pixel diagonal is ~7.8 km
                                   # (5.5 km × √2). 100 km merges physically unrelated
                                   # sources into inflated fake plumes. Use ~1× pixel diagonal.

    'min_pixels':    3,
    'max_elongation': 20.0,

    'pixel_area_km2': 38.5,        # TROPOMI nadir pixel ~5.5 × 7 km = 38.5 km² — correct

    # ── Physical constants — tighten mixing layer height ─────────────────
    # 1000 m is the global mean. Permian Basin has a well-mixed ABL of
    # ~500–800 m during daytime overpass (~13:30 LT). Overestimating H
    # directly overestimates IME and emission rates.
    # Use 700 m as a conservative Permian-specific default.
    'mixing_layer_height_m':    700.0,   # was 1000.0
    'air_density_kg_m3':        1.225,
    'molar_mass_ratio_ch4_air': 16.04 / 28.97,

    'max_wind_angle_deg': 60.0,
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Load & Validate

# CELL ********************

# Required columns from Notebook 1 silver output.
REQUIRED_COLS = ['latitude', 'longitude', 'ch4', 'qa_value', 'datetime']

df_silver = spark.read.table(CFG['silver_table'])

missing = [c for c in REQUIRED_COLS if c not in df_silver.columns]
if missing:
    raise ValueError(f'Missing required columns: {missing}. Available: {sorted(df_silver.columns)}')

df_silver = (
    df_silver
    .withColumn('latitude',  F.col('latitude').cast(DoubleType()))
    .withColumn('longitude', F.col('longitude').cast(DoubleType()))
    .withColumn('ch4',       F.col('ch4').cast(DoubleType()))
    .withColumn('qa_value',  F.col('qa_value').cast(DoubleType()))
    .withColumn('datetime',  F.to_timestamp(F.col('datetime').cast(StringType())))
    .dropna(subset=REQUIRED_COLS)
)

stats = df_silver.agg(
    F.count('*').alias('n_pixels'),
    F.min('ch4').alias('ch4_min'),
    F.max('ch4').alias('ch4_max'),
    F.min('latitude').alias('lat_min'),
    F.max('latitude').alias('lat_max'),
    F.min('datetime').alias('t_min'),
    F.max('datetime').alias('t_max'),
).collect()[0]

print(f'Pixels:    {stats["n_pixels"]:,}')
print(f'CH4:       {stats["ch4_min"]:.1f} – {stats["ch4_max"]:.1f} ppb')
print(f'Lat:       {stats["lat_min"]:.2f} – {stats["lat_max"]:.2f}')
print(f'Time:      {stats["t_min"]} → {stats["t_max"]}')
print(f'Columns:   {df_silver.columns}')

# ── Constrain to Permian Basin — highest CM flight density ───────────────
# CM has 92 plumes in this corridor vs your TROPOMI's 16.
# Without this filter your plumes scatter across the hemisphere
# and most have no CM overpass within 300 km.
STUDY_BBOX = {
    'lat_min': 28.0, 'lat_max': 36.0,
    'lon_min': -105.0, 'lon_max': -97.0,
}

df_silver = df_silver.filter(
    (F.col('latitude').between(STUDY_BBOX['lat_min'],  STUDY_BBOX['lat_max'])) &
    (F.col('longitude').between(STUDY_BBOX['lon_min'], STUDY_BBOX['lon_max']))
)

stats_bbox = df_silver.agg(
    F.count('*').alias('n'),
    F.min('latitude').alias('lat_min'),
    F.max('latitude').alias('lat_max'),
    F.min('longitude').alias('lon_min'),
    F.max('longitude').alias('lon_max'),
).collect()[0]

print(f'\nAfter Permian bbox filter:')
print(f'  Pixels: {stats_bbox["n"]:,}')
print(f'  Lat:    {stats_bbox["lat_min"]:.2f} → {stats_bbox["lat_max"]:.2f}')
print(f'  Lon:    {stats_bbox["lon_min"]:.2f} → {stats_bbox["lon_max"]:.2f}')

if stats_bbox['n'] == 0:
    raise ValueError(
        'No pixels in Permian bbox — your silver table has no data in this region. '
        'Check upstream ingestion covers lon -105 to -97, lat 28 to 36.'
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Derive Scene ID (Overpass Grouping)

# CELL ********************

def derive_scene_id(
    df: DataFrame,
    gap_threshold_s: int = CFG['scene_gap_threshold_s'],
) -> DataFrame:
    """
    Assign a scene_id to each pixel based on acquisition time gaps.

    Physical basis: TROPOMI orbits every ~98.8 min. Pixels within one overpass
    are seconds apart; consecutive overpasses are ~95 min apart.
    Any gap > gap_threshold_s = new overpass = new scene.

    For large datasets, call per acquisition_date to avoid a full-table sort:
        df.filter(F.col('acquisition_date') == date).transform(derive_scene_id)

    Returns df with added columns: scene_id (STRING), scene_index (LONG)
    """
    df = df.withColumn('_ts', F.unix_timestamp(F.col('datetime')))

    w_global = Window.orderBy('_ts')

    df = df.withColumn('_prev_ts', F.lag('_ts', 1).over(w_global))

    df = df.withColumn(
        '_is_new_scene',
        F.when(
            F.col('_prev_ts').isNull() |
            ((F.col('_ts') - F.col('_prev_ts')) > gap_threshold_s),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    df = df.withColumn(
        'scene_index',
        F.sum('_is_new_scene').over(w_global).cast(LongType())
    )

    w_scene = Window.partitionBy('scene_index')
    df = (
        df
        .withColumn('_anchor', F.min('datetime').over(w_scene))
        .withColumn('scene_id', F.concat(
            F.lit('scene_'),
            F.date_format(F.col('_anchor'), 'yyyyMMdd_HHmmss')
        ))
        .drop('_ts', '_prev_ts', '_is_new_scene', '_anchor')
    )

    scene_summary = (
        df.groupBy('scene_id').agg(
            F.count('*').alias('n_pixels'),
            F.min('datetime').alias('t_start'),
            F.max('datetime').alias('t_end'),
        ).orderBy('t_start')
    )

    n_scenes = scene_summary.count()
    print(f'Scenes derived: {n_scenes}  (gap_threshold={gap_threshold_s}s)')
    scene_summary.show(truncate=False)

    return df

print('derive_scene_id() defined')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver = derive_scene_id(df_silver, gap_threshold_s=CFG['scene_gap_threshold_s'])

# Enumerate scenes for the detection loop
scene_ids = [
    r['scene_id'] for r in
    df_silver.select('scene_id').distinct().orderBy('scene_id').collect()
]
print(f'Processing {len(scene_ids)} scene(s): {scene_ids}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Plume Detection Functions

# CELL ********************

def estimate_background(
    df_scene: pd.DataFrame,
    n_neighbors: int   = CFG['n_bg_neighbors'],
    bg_quantile: float = CFG['bg_quantile'],
) -> pd.DataFrame:
    df = df_scene.copy()

    # Use per-pixel latitude for cosine correction rather than scene mean.
    # At scene edges (e.g. lat 28 vs 36) the scene-mean introduces ~3% x-distance
    # error which biases k-NN neighbour selection at the boundaries.
    df['x_km'] = (df['longitude'] - df['longitude'].mean()) \
                 * 111.32 * np.cos(np.radians(df['latitude']))
    df['y_km'] = (df['latitude'] - df['latitude'].mean()) * 110.574

    coords = df[['x_km', 'y_km']].values
    k = min(n_neighbors + 1, len(df))
    tree = cKDTree(coords)
    _, idxs = tree.query(coords, k=k)

    ch4_vals = df['ch4'].values
    df['ch4_bg'] = np.array([
        np.quantile(ch4_vals[idxs[i, 1:]], bg_quantile)
        for i in range(len(df))
    ])
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def compute_anomaly(
    df_scene: pd.DataFrame,
    k_sigma:     float = CFG['k_sigma'],
    noise_floor: float = CFG['noise_floor_ppb'],
) -> pd.DataFrame:
    df = df_scene.copy()
    df['delta_ch4'] = df['ch4'] - df['ch4_bg']

    # Compute MAD only on the background distribution (pixels below 90th percentile).
    # Including strong enhancement pixels in MAD inflates the threshold and causes
    # real plumes — especially weaker ones — to fall below it.
    bg_mask = df['delta_ch4'] < df['delta_ch4'].quantile(0.90)
    local_mad = float(median_abs_deviation(df.loc[bg_mask, 'delta_ch4'], nan_policy='omit'))
    threshold = max(k_sigma * local_mad, noise_floor)

    df['local_mad']    = local_mad
    df['threshold']    = threshold
    df['is_candidate'] = df['delta_ch4'] > threshold

    print(f'  MAD={local_mad:.2f} ppb | threshold={threshold:.2f} ppb | candidates={df["is_candidate"].sum()}')
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def label_plumes(
    df_scene: pd.DataFrame,
    connectivity_radius_km: float = CFG['connectivity_radius_km'],
    min_pixels:             int   = CFG['min_pixels'],
) -> pd.DataFrame:
    """
    Connected-component labeling via proximity graph + Union-Find.

    Each spatially contiguous group of candidate pixels above threshold = one plume.
    This is the standard definition in satellite methane literature (Carbon Mapper,
    GHGSat, Irakulis-Loitxate et al. 2021) — NOT DBSCAN, NOT k-means.

    Adds column: plume_id (float, NaN for non-plume pixels)
    """
    df = df_scene.copy()
    df['plume_id'] = np.nan

    candidates = df[df['is_candidate']].copy()
    if len(candidates) == 0:
        print('  No candidates — no plumes')
        return df

    coords = candidates[['x_km', 'y_km']].values
    pairs  = cKDTree(coords).query_pairs(r=connectivity_radius_km)

    parent = list(range(len(candidates)))
    rank   = [0] * len(candidates)

    def _find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def _union(x, y):
        px, py = _find(x), _find(y)
        if px == py: return
        if rank[px] < rank[py]: px, py = py, px
        parent[py] = px
        if rank[px] == rank[py]: rank[px] += 1

    for i, j in pairs:
        _union(i, j)

    raw_comp   = np.array([_find(i) for i in range(len(candidates))])
    comp_sizes = pd.Series(raw_comp).value_counts()
    valid      = comp_sizes[comp_sizes >= min_pixels].index
    comp_map   = {c: i + 1 for i, c in enumerate(sorted(valid))}

    assigned = np.array([comp_map.get(c, 0) for c in raw_comp], dtype=float)
    assigned[assigned == 0] = np.nan
    df.loc[candidates.index, 'plume_id'] = assigned

    print(f'  Components: {len(comp_sizes)} | filtered: {(comp_sizes < min_pixels).sum()} | valid plumes: {len(valid)}')
    return df

print('label_plumes() defined')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def compute_plume_features(
    df_scene:              pd.DataFrame,
    scene_id:              str,
    pixel_area_km2:        float = CFG['pixel_area_km2'],
    mixing_layer_height_m: float = CFG['mixing_layer_height_m'],
    air_density_kg_m3:     float = CFG['air_density_kg_m3'],
    molar_mass_ratio:      float = CFG['molar_mass_ratio_ch4_air'],
) -> pd.DataFrame:

    plume_px = df_scene.dropna(subset=['plume_id'])
    if len(plume_px) == 0:
        return pd.DataFrame()

    ime_conversion = (
        1e-9                        # ppb → mol/mol
        * pixel_area_km2 * 1e6      # km² → m²
        * air_density_kg_m3         # kg/m³
        * molar_mass_ratio          # kg CH4 / kg air
        * mixing_layer_height_m     # m — column height
    )

    records = []
    for pid, grp in plume_px.groupby('plume_id'):
        n_px        = len(grp)
        area_km2    = n_px * pixel_area_km2
        max_delta   = float(grp['delta_ch4'].max())
        mean_delta  = float(grp['delta_ch4'].mean())
        ime_ppb_km2 = float(grp['delta_ch4'].sum() * pixel_area_km2)
        ime_kg      = float(grp['delta_ch4'].sum() * ime_conversion)

        X = grp[['x_km', 'y_km']].values.copy().astype(np.float64)
        X -= X.mean(axis=0)

        if n_px >= 3:
            cov = np.cov(X.T)
            eigvals, eigvecs = np.linalg.eigh(cov)
            eigvals = np.maximum(eigvals, 0.0)

            major_ax   = 2.0 * np.sqrt(eigvals[-1])
            minor_ax   = 2.0 * np.sqrt(eigvals[0])
            elongation = major_ax / (minor_ax + 1e-6)
            orient_deg = float(np.degrees(np.arctan2(eigvecs[-1, 1], eigvecs[-1, 0])))
        else:
            major_ax = minor_ax = elongation = orient_deg = np.nan

        records.append({
            'scene_id':           scene_id,
            'plume_id':           int(pid),
            'centroid_lat':       round(float(grp['latitude'].mean()),  4),
            'centroid_lon':       round(float(grp['longitude'].mean()), 4),
            'n_pixels':           n_px,
            'area_km2':           round(area_km2,    1),
            'max_delta_ch4_ppb':  round(max_delta,   2),
            'mean_delta_ch4_ppb': round(mean_delta,  2),
            'ime_ppb_km2':        round(ime_ppb_km2, 1),
            'ime_kg':             round(ime_kg,       1),
            # ime_kg_h added for human-readable validation — not written to catalog
            'ime_kg_h':           round(ime_kg,       1),  # placeholder; rate computed in wind step
            'major_axis_km':      round(float(major_ax),   1) if np.isfinite(major_ax)   else np.nan,
            'minor_axis_km':      round(float(minor_ax),   1) if np.isfinite(minor_ax)   else np.nan,
            'elongation':         round(float(elongation), 2) if np.isfinite(elongation) else np.nan,
            'orientation_deg':    round(float(orient_deg), 1) if np.isfinite(orient_deg) else np.nan,
        })

    return pd.DataFrame(records)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def wind_aware_refinement(
    plume_df:      pd.DataFrame,
    wind_u:        float | None = None,
    wind_v:        float | None = None,
    max_angle_deg: float        = CFG['max_wind_angle_deg'],
) -> pd.DataFrame:
    df = plume_df.copy()

    if wind_u is None or wind_v is None:
        df['wind_speed_ms']        = np.nan
        df['wind_dir_deg']         = np.nan
        df['angle_plume_wind_deg'] = np.nan
        df['wind_aligned']         = pd.NA
        df['emission_rate_kg_s']   = np.nan
        return df

    wind_speed = float(np.sqrt(wind_u**2 + wind_v**2))
    wind_dir   = float(np.degrees(np.arctan2(wind_u, wind_v)))

    df['wind_speed_ms'] = round(wind_speed, 2)
    df['wind_dir_deg']  = round(wind_dir, 1)

    def _angle_diff(a, b):
        d = abs(a - b) % 180.0
        return min(d, 180.0 - d)

    df['angle_plume_wind_deg'] = df['orientation_deg'].apply(
        lambda o: round(_angle_diff(o, wind_dir), 1) if pd.notna(o) else np.nan
    )
    df['wind_aligned'] = df['angle_plume_wind_deg'] <= max_angle_deg

    if wind_speed > 0:
        orient_rad = np.radians(df['orientation_deg'].fillna(wind_dir))
        wind_rad   = np.arctan2(wind_u, wind_v)
        cos_angle  = np.abs(np.cos(orient_rad - wind_rad))

        L_raw_m = (df['major_axis_km'].fillna(1.0) * 1e3).clip(lower=10_000.0)
        L_eff_m = (L_raw_m * cos_angle).clip(lower=10_000.0)

        # Only compute emission rate for wind-aligned plumes.
        # For misaligned plumes, cos_angle → 0 makes L_eff → 10 km (the floor)
        # which produces artificially high rates. Mark them NaN instead.
        emission_rate = df['ime_kg'] * wind_speed / L_eff_m
        df['emission_rate_kg_s'] = np.where(
            df['wind_aligned'].fillna(False),
            emission_rate.round(4),
            np.nan   # misaligned — rate is not physically meaningful
        )
    else:
        df['emission_rate_kg_s'] = np.nan

    print(f'  Wind: {wind_speed:.1f} m/s @ {wind_dir:.0f}° | aligned: {df["wind_aligned"].sum()}/{len(df)}')
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def detect_plumes(
    df_scene_pd: pd.DataFrame,
    scene_id:    str,
    wind_u:      float | None = None,
    wind_v:      float | None = None,
    cfg:         dict         = CFG,
    verbose:     bool         = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Full detection pipeline for a single scene (Pandas, CPU-bound).

    Steps: background → anomaly → connected components → features → wind filter.

    Returns (pixel_df_enriched, plume_feature_df).
    Both are empty DataFrames if the scene has too few pixels or no detections.
    """
    if verbose:
        print(f'\n── Scene: {scene_id} ({len(df_scene_pd)} pixels) ──')

    if len(df_scene_pd) < cfg['n_bg_neighbors'] + 5:
        print(f'  SKIP: too few pixels for background estimation')
        return pd.DataFrame(), pd.DataFrame()

    df = estimate_background(df_scene_pd, cfg['n_bg_neighbors'], cfg['bg_quantile'])
    df = compute_anomaly(df, cfg['k_sigma'], cfg['noise_floor_ppb'])
    df = label_plumes(df, cfg['connectivity_radius_km'], cfg['min_pixels'])

    pf = compute_plume_features(
        df, scene_id,
        cfg['pixel_area_km2'],
        cfg['mixing_layer_height_m'],
        cfg['air_density_kg_m3'],
        cfg['molar_mass_ratio_ch4_air'],
    )

    if len(pf) > 0:
        pf = wind_aware_refinement(pf, wind_u, wind_v, cfg['max_wind_angle_deg'])
        n_before = len(pf)
        pf = pf[pf['elongation'].isna() | (pf['elongation'] <= cfg['max_elongation'])]
        if verbose and len(pf) < n_before:
            print(f'  Elongation filter removed {n_before - len(pf)} plume(s)')

    if verbose:
        print(f'  → {len(pf)} plume(s) detected')

    return df, pf

print('detect_plumes() defined — all pipeline functions ready')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Run Detection Loop

# CELL ********************

all_pixel_dfs = []
all_plume_dfs = []

SCENE_REQUIRED = ['latitude', 'longitude', 'ch4', 'qa_value', 'datetime', 'scene_id']

for scene_id in scene_ids:
    select_cols = [
        'latitude', 'longitude', 'ch4', 'qa_value', 'datetime', 'scene_id',
    ]
    wind_cols = ['wind_u_ms', 'wind_v_ms', 'is_wind_valid']
    select_cols += [c for c in wind_cols if c in df_silver.columns]

    df_pd = (
        df_silver
        .filter(F.col('scene_id') == scene_id)
        .select(*select_cols)
        .toPandas()
    )

    # Validate before processing — catch schema drift early
    missing = [c for c in SCENE_REQUIRED if c not in df_pd.columns]
    if missing:
        print(f'  SKIP {scene_id}: missing columns {missing}')
        continue

    # Explicit dtype enforcement post-toPandas().
    # Spark→Pandas conversion can produce object-dtype floats or nullable
    # Int64/Float64 extension types; NumPy operations require concrete dtypes.
    df_pd['latitude']  = df_pd['latitude'].astype(np.float64)
    df_pd['longitude'] = df_pd['longitude'].astype(np.float64)
    df_pd['ch4']       = df_pd['ch4'].astype(np.float64)
    df_pd['qa_value']  = df_pd['qa_value'].astype(np.float64)
    df_pd['datetime']  = pd.to_datetime(df_pd['datetime'], utc=True)

    if 'wind_u_ms' in df_pd.columns:
        df_pd['wind_u_ms'] = df_pd['wind_u_ms'].astype(np.float64)
        df_pd['wind_v_ms'] = df_pd['wind_v_ms'].astype(np.float64)

    wind_u = wind_v = None
    if 'is_wind_valid' in df_pd.columns:
        wind_rows = df_pd[df_pd['is_wind_valid'] == True]
        if len(wind_rows) > 0:
            wind_u = float(wind_rows['wind_u_ms'].mean())
            wind_v = float(wind_rows['wind_v_ms'].mean())

    pixel_df, plume_df = detect_plumes(
        df_scene_pd=df_pd,
        scene_id=scene_id,
        wind_u=wind_u,
        wind_v=wind_v,
    )

    if len(pixel_df) > 0:
        all_pixel_dfs.append(pixel_df)
    if len(plume_df) > 0:
        all_plume_dfs.append(plume_df)

pixel_df_all = pd.concat(all_pixel_dfs, ignore_index=True) if all_pixel_dfs else pd.DataFrame()
plume_df_all = pd.concat(all_plume_dfs, ignore_index=True) if all_plume_dfs else pd.DataFrame()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Plume Catalog Output

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, LongType
)

# ── Explicit output schema for plume catalog ───────────────────────────────
# Every field is a scalar. BooleanType maps to Python bool (not pd.NA).
# Nullable=True on all fields — plumes with n<3 have NaN shape features.
PLUME_CATALOG_SCHEMA = StructType([
    StructField('scene_id',           StringType(),  nullable=False),
    StructField('plume_id',           IntegerType(), nullable=False),
    StructField('centroid_lat',       DoubleType(),  nullable=False),
    StructField('centroid_lon',       DoubleType(),  nullable=False),
    StructField('n_pixels',           IntegerType(), nullable=False),
    StructField('area_km2',           DoubleType(),  nullable=False),
    StructField('max_delta_ch4_ppb',  DoubleType(),  nullable=False),
    StructField('mean_delta_ch4_ppb', DoubleType(),  nullable=False),
    StructField('ime_ppb_km2',        DoubleType(),  nullable=False),
    StructField('ime_kg',             DoubleType(),  nullable=False),
    StructField('major_axis_km',      DoubleType(),  nullable=True),
    StructField('minor_axis_km',      DoubleType(),  nullable=True),
    StructField('elongation',         DoubleType(),  nullable=True),
    StructField('orientation_deg',    DoubleType(),  nullable=True),
    StructField('wind_speed_ms',      DoubleType(),  nullable=True),
    StructField('wind_dir_deg',       DoubleType(),  nullable=True),
    StructField('angle_plume_wind_deg', DoubleType(), nullable=True),
    StructField('wind_aligned',       BooleanType(), nullable=True),
    StructField('emission_rate_kg_s', DoubleType(),  nullable=True),
])

PLUME_SCHEMA_COLS = [f.name for f in PLUME_CATALOG_SCHEMA]


def clean_plume_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize plume_df_all to a clean, Spark-safe Pandas DataFrame.

    Fixes at the Pandas→Spark boundary:
    1. Drops columns not in the schema (e.g. stray debug columns).
    2. Adds missing schema columns as NaN/None (graceful — not all scenes
       produce wind features).
    3. Converts all Pandas nullable extension dtypes (BooleanDtype,
       Int64Dtype, Float64Dtype) to their NumPy equivalents. These are
       the types that produce StructType conflicts in Spark's schema
       inference because their Arrow encoding differs from plain scalars.
    4. Forces wind_aligned to plain Python bool with None for missing —
       the specific column that triggers the BooleanType vs StructType
       conflict when pd.NA mixes with True/False across concat'd frames.
    5. Validates no object-dtype or nested columns remain.
    """

    df = df.copy()

    # ── 1. Detect and report schema problems before fixing ─────────────────
    print('--- plume_df_all dtype audit ---')
    problems = []
    for col in df.columns:
        dtype = df[col].dtype
        # Pandas extension types are the primary Spark incompatibility
        is_extension = hasattr(dtype, 'numpy_dtype') or isinstance(
            dtype, (pd.BooleanDtype, pd.Int8Dtype, pd.Int16Dtype,
                    pd.Int32Dtype, pd.Int64Dtype, pd.UInt8Dtype,
                    pd.UInt16Dtype, pd.UInt32Dtype, pd.UInt64Dtype,
                    pd.Float32Dtype, pd.Float64Dtype, pd.StringDtype)
        )
        # Object dtype may hide nested dicts/lists
        has_nested = (dtype == object) and df[col].dropna().apply(
            lambda x: isinstance(x, (dict, list))
        ).any()
        if is_extension or has_nested:
            problems.append((col, str(dtype), 'extension/nested'))
        print(f'  {col:<25} {str(dtype):<20} {"⚠ PROBLEM" if (is_extension or has_nested) else "ok"}')

    if problems:
        print(f'\nFixing {len(problems)} problematic column(s): {[p[0] for p in problems]}')

    # ── 2. Drop columns not in schema, add missing ones ────────────────────
    for col in PLUME_SCHEMA_COLS:
        if col not in df.columns:
            df[col] = np.nan  # will be cast to correct type below

    extra_cols = [c for c in df.columns if c not in PLUME_SCHEMA_COLS]
    if extra_cols:
        print(f'Dropping extra columns not in schema: {extra_cols}')
        df = df.drop(columns=extra_cols)

    df = df[PLUME_SCHEMA_COLS]  # enforce column order to match schema

    # ── 3. Cast each column to its schema-defined NumPy type ───────────────
    # This converts all Pandas extension types to concrete NumPy types
    # that Spark's Arrow bridge handles without type-merge conflicts.
    DTYPE_MAP = {
        StringType():  'object',     # str columns stay as object
        IntegerType(): np.int32,
        LongType():    np.int64,
        DoubleType():  np.float64,
        BooleanType(): object,       # bool handled specially below
    }

    for field in PLUME_CATALOG_SCHEMA:
        col   = field.name
        dtype = type(field.dataType)
        target = DTYPE_MAP.get(field.dataType.__class__)

        if field.dataType.__class__ == BooleanType:
            # Convert pd.NA / np.nan / None → None; True/False → Python bool.
            # This is the direct fix for the BooleanType vs StructType conflict.
            # pd.NA cannot be stored in a NumPy bool array; use Python object
            # array with None for missing so Arrow maps it to nullable bool.
            def _to_nullable_bool(v):
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    return None
                try:
                    # pd.NA, pd.BooleanDtype values
                    if pd.isna(v):
                        return None
                except (TypeError, ValueError):
                    pass
                return bool(v)

            df[col] = df[col].apply(_to_nullable_bool)

        elif field.dataType.__class__ == StringType:
            # Ensure strings are str or None, never float NaN
            df[col] = df[col].where(df[col].notna(), other=None).astype(object)

        elif field.dataType.__class__ == IntegerType:
            # NaN cannot exist in np.int32; convert to nullable then to Int32
            # then to object so Spark receives Python int or None
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].where(df[col].notna(), other=None)
            # Convert non-None values to Python int
            df[col] = df[col].apply(lambda x: int(x) if x is not None else None)

        else:
            # DoubleType — convert to float64, keep NaN (Arrow maps to null)
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(np.float64)

    # ── 4. Final validation — no nested objects remain ─────────────────────
    for col in df.columns:
        sample = df[col].dropna()
        if len(sample) > 0:
            has_nested = sample.apply(lambda x: isinstance(x, (dict, list))).any()
            if has_nested:
                raise ValueError(
                    f'Column "{col}" still contains nested objects after cleaning. '
                    f'Sample value: {sample.iloc[0]}'
                )

    print(f'\nCleaned schema: {df.dtypes.to_dict()}')
    print(f'Shape: {df.shape}')
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(plume_df_all) > 0:
    # Normalize at the Pandas→Spark boundary before createDataFrame
    plume_df_clean = clean_plume_dataframe(plume_df_all)

    # Explicit schema — never let Spark infer types from a mixed Pandas frame
    catalog_spark = spark.createDataFrame(plume_df_clean, schema=PLUME_CATALOG_SCHEMA)

    (
        catalog_spark.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(CFG['plume_catalog_table'])
    )

    n = spark.read.table(CFG['plume_catalog_table']).count()
    print(f'Catalog written: {CFG["plume_catalog_table"]} ({n} rows)')
else:
    print('No plumes to write.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Visualisation

# CELL ********************

import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
import matplotlib.cm as cm
import pandas as pd

# --- SAFETY LIMITS ---
MAX_SCENES = 6
MAX_PIXELS_PER_SCENE = 20000

if len(pixel_df_all) == 0:
    print('No pixel data to plot')

else:
    # Keep only required columns (avoid huge hidden payloads)
    cols_to_keep = ['scene_id', 'longitude', 'latitude', 'delta_ch4', 'plume_id']
    pixel_df_all = pixel_df_all[cols_to_keep]

    # Limit number of scenes
    scenes = pixel_df_all['scene_id'].unique()[:MAX_SCENES]
    n = len(scenes)

    max_per_row = 3
    rows = (n + max_per_row - 1) // max_per_row
    rows = min(rows, 3)  # hard cap to avoid huge figures

    fig, axes = plt.subplots(rows, max_per_row, figsize=(6 * max_per_row, 5 * rows))
    axes = axes.flatten() if n > 1 else [axes]

    for i, sid in enumerate(scenes):
        ax = axes[i]

        sw = pixel_df_all[pixel_df_all['scene_id'] == sid]

        # --- SUBSAMPLE PIXELS ---
        if len(sw) > MAX_PIXELS_PER_SCENE:
            sw = sw.sample(MAX_PIXELS_PER_SCENE, random_state=42)

        pl = (
            plume_df_all[plume_df_all['scene_id'] == sid]
            if len(plume_df_all) > 0 else pd.DataFrame()
        )

        # --- BACKGROUND PIXELS ---
        bg_mask = sw['plume_id'].isna()
        ax.scatter(
            sw.loc[bg_mask, 'longitude'],
            sw.loc[bg_mask, 'latitude'],
            c=sw.loc[bg_mask, 'delta_ch4'],
            cmap='RdYlBu_r',
            norm=Normalize(-20, 20),
            s=8,
            alpha=0.3
        )

        # --- PLUME PIXELS ---
        pm = ~sw['plume_id'].isna()
        if pm.any():
            vmax = sw.loc[pm, 'delta_ch4'].quantile(0.98)

            ax.scatter(
                sw.loc[pm, 'longitude'],
                sw.loc[pm, 'latitude'],
                c=sw.loc[pm, 'delta_ch4'],
                cmap='hot',
                norm=Normalize(0, vmax),
                s=50,
                edgecolors='black',
                linewidths=0.5,
                zorder=5
            )

            # --- LABEL PLUMES ---
            for _, row in pl.iterrows():
                ax.text(
                    row['centroid_lon'],
                    row['centroid_lat'],
                    f'{int(row["plume_id"])}',
                    fontsize=9,
                    color='black',
                    ha='center',
                    va='center',
                    bbox=dict(facecolor='white', alpha=0.7, edgecolor='none')
                )

        ax.set_title(f'{sid[:12]}...\n{len(sw)} px | {len(pl)} plumes', fontsize=9)
        ax.set_xlabel('Lon')
        ax.set_ylabel('Lat')
        ax.grid(alpha=0.2)

    # Remove empty subplots
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    # Layout + colorbar
    fig.subplots_adjust(right=0.88, top=0.90)

    cbar_ax = fig.add_axes([0.90, 0.2, 0.02, 0.6])
    cbar = fig.colorbar(
        cm.ScalarMappable(norm=Normalize(-20, 20), cmap='RdYlBu_r'),
        cax=cbar_ax
    )
    cbar.set_label('ΔCH₄ [ppb]')

    fig.suptitle('CH₄ Plume Detections (Readable)', fontsize=14)

    plt.savefig('plume_detections_clean.png', dpi=150, bbox_inches='tight')
    plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import matplotlib.pyplot as plt
import numpy as np

if len(plume_df_all) >= 2:

    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    axes = axes.flatten()

    plots = [
        ('area_km2',          'Plume Area [km²]'),
        ('max_delta_ch4_ppb', 'Peak ΔCH₄ [ppb]'),
        ('ime_ppb_km2',       'IME [ppb·km²]'),
        ('ime_kg',            'IME [kg CH₄]'),
        ('major_axis_km',     'Major Axis [km]'),
        ('elongation',        'Elongation (major/minor)'),
    ]

    for ax, (col, label) in zip(axes, plots):

        if col in plume_df_all.columns and plume_df_all[col].notna().any():
            vals = plume_df_all[col].dropna()

            # --- Robust clipping to remove extreme outliers ---
            lo, hi = vals.quantile([0.01, 0.99])
            vals_clipped = vals.clip(lo, hi)

            # --- Histogram instead of bar ---
            ax.hist(vals_clipped, bins=20, alpha=0.75, edgecolor='black')

            # --- Log scale where appropriate ---
            if (vals_clipped > 0).all() and vals_clipped.max() / max(vals_clipped.min(), 1e-6) > 50:
                ax.set_xscale('log')

            # --- Median line ---
            median = vals.median()
            ax.axvline(median, linestyle='--', linewidth=1)

            ax.set_title(label, fontsize=10)
            ax.set_xlabel('')
            ax.set_ylabel('Count')
            ax.grid(alpha=0.3)

    fig.suptitle('Plume Feature Distributions (Robust)', fontsize=13)
    plt.tight_layout()
    plt.savefig('plume_features_clean.png', dpi=150)
    plt.show()

else:
    print('Need ≥ 2 plumes for distribution plot')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if 'ch4_bg' in pixel_df_all.columns and len(pixel_df_all) > 0:
    sid = pixel_df_all['scene_id'].iloc[0]
    sw  = pixel_df_all[pixel_df_all['scene_id'] == sid].sort_values('latitude')

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].scatter(sw['latitude'], sw['ch4'],    s=8, c='steelblue',  alpha=0.6, label='Observed')
    axes[0].scatter(sw['latitude'], sw['ch4_bg'], s=8, c='darkorange', alpha=0.6, label='Background')
    axes[0].set_xlabel('Latitude')
    axes[0].set_ylabel('CH₄ [ppb]')
    axes[0].set_title('Observed vs Background')
    axes[0].legend()
    axes[0].grid(alpha=0.3)

    thresh = float(sw['threshold'].iloc[0])
    mad    = float(sw['local_mad'].iloc[0])
    axes[1].hist(sw['delta_ch4'], bins=30, color='gray', alpha=0.7, edgecolor='k')
    axes[1].axvline(thresh,  color='red',   linestyle='--', lw=2, label=f'Threshold={thresh:.1f} ppb')
    axes[1].axvline(-thresh, color='blue',  linestyle='--', lw=1, alpha=0.5)
    axes[1].axvline(0,       color='black', linestyle='-',  lw=1, alpha=0.4)
    axes[1].set_xlabel('ΔCH₄ [ppb]')
    axes[1].set_ylabel('Count')
    axes[1].set_title(f'Enhancement distribution | MAD={mad:.2f} ppb')
    axes[1].legend()
    axes[1].grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig('background_qc.png', dpi=150, bbox_inches='tight')
    plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Physical Validation

# CELL ********************

if len(plume_df_all) > 0:
    print('=== Tier 1: Physical Plausibility ===')
    checks = {
        'max_delta > 5 ppb':    (plume_df_all['max_delta_ch4_ppb'] > 5).all(),
        'max_delta < 200 ppb':  (plume_df_all['max_delta_ch4_ppb'] < 200).all(),  # was 500
        'area < 10000 km²':     (plume_df_all['area_km2'] < 10000).all(),
        'elongation < 20':      (plume_df_all['elongation'].dropna() < 20).all(),
        # New: emission rates should be in the range seen for Permian O&G
        # Typical well pad: 0.01–5 kg/s. Above 50 kg/s is a major blowout.
        'emission_rate < 50 kg/s': (
            plume_df_all['emission_rate_kg_s'].dropna() < 50
        ).all() if 'emission_rate_kg_s' in plume_df_all.columns else True,
    }
    for check, passed in checks.items():
        print(f'  {"✓" if passed else "✗"} {check}')

    if 'emission_rate_kg_s' in plume_df_all.columns:
        q = plume_df_all['emission_rate_kg_s'].dropna() * 3600
        if len(q) > 0:
            print(f'\n  Emission rate range: {q.min():.1f} – {q.max():.1f} kg/h')
            print(f'  Median:              {q.median():.1f} kg/h')
            # Rates below ~36 kg/h (0.01 kg/s) are below TROPOMI's detection
            # limit for single-overpass retrievals — flag them
            n_below_detection = (q < 36).sum()
            if n_below_detection > 0:
                print(f'  ⚠ {n_below_detection} plume(s) below TROPOMI detection limit (~36 kg/h)')
else:
    print('No plumes to validate')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
