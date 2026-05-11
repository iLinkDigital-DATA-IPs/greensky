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

# # CH₄ Plume Validation — My Catalog vs Carbon Mapper
# Spatial + temporal matching · Unit-aligned emission comparison · Statistical metrics

# CELL ********************

# Spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

# Scientific
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from scipy import stats
import requests
import time

# Viz
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.ticker import LogLocator
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
    # ── Tables ───────────────────────────────────────────────────────────
    'plume_catalog_table': 'Planetary_computer_LH.silver.ch4_plume_catalog',
    'cm_cache_table':      'Planetary_computer_LH.silver.carbon_mapper_cache',
    'matched_table':       'Planetary_computer_LH.silver.plume_matched_catalog',

    # ── Carbon Mapper API ─────────────────────────────────────────────────
    # Public portal API — no auth required for public plume data.
    # Docs: https://api.carbonmapper.org/api/v1/docs
    'cm_api_base':        'https://api.carbonmapper.org/api/v1',
    'cm_api_limit':       500,       # records per page
    'cm_api_retries':     3,
    'cm_api_timeout_s':   30,
    'cm_api_backoff_s':   2,

    # ── Matching criteria ─────────────────────────────────────────────────
    # Spatial: haversine distance threshold for candidate match
    # 10 km is conservative for TROPOMI's ~7 km pixel; tighten to 5 km
    # if using high-resolution CM data where centroid accuracy is better.
    'match_spatial_km':    25.0,

    # Temporal: maximum allowed |t_mine - t_cm| in hours
    # Same-day within ±24h covers TROPOMI overpasses vs CM flight days.
    'match_temporal_h':    24.0,

    # ── Data quality filters ─────────────────────────────────────────────
    'filter_wind_aligned': False,     # True = restrict to wind_aligned==True
    'outlier_clip_pct':    0.01,      # clip top/bottom 1% of emission ratios

    # ── Units ─────────────────────────────────────────────────────────────
    # Mine: kg/s   CM: kg/hr   → multiply mine × 3600 for apples-to-apples
    'my_unit':             'kg/s',
    'cm_unit':             'kg/hr',
    'my_to_kg_hr':         3600.0,    # conversion factor: kg/s → kg/hr
    'match_spatial_km':    25.0,   # was 10.0 in CFG (overridden to 150 in run block)
    'd_scale_km':          15.0,   # Gaussian decay: 50% score at ~10 km
    't_scale_h':           48.0,   # 48h covers same-day ± overpass window
    'score_threshold':     0.10,   # was 0.01 — too permissive; 0.10 filters noise matches

    'filter_wind_aligned': True,   # was False — only compare plumes with valid wind
                                   # alignment; unaligned plumes have unreliable emission
                                   # rates and pollute the correlation statistics
    'outlier_clip_pct':    0.05,   # was 0.01 — 1% clip on n<20 pairs removes real data
}

print('Configuration loaded')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Load My Plume Catalog

# CELL ********************

def load_my_plumes(
    table_name:          str   = CFG['plume_catalog_table'],
    filter_wind_aligned: bool  = CFG['filter_wind_aligned'],
    min_emission_kg_hr:  float = 0.0,
) -> pd.DataFrame:
    """
    Load my plume catalog from Delta. Converts emission_rate to kg/hr.
    Returns Pandas DataFrame — catalog is small enough post-filtering.
    """
    df_spark = spark.read.table(table_name)

    # Drop rows missing the fields required for matching or comparison
    df_spark = df_spark.dropna(subset=[
        'centroid_lat', 'centroid_lon', 'emission_rate_kg_s'
    ])

    if filter_wind_aligned:
        df_spark = df_spark.filter(F.col('wind_aligned') == True)

    if min_emission_kg_hr > 0:
        min_kg_s = min_emission_kg_hr / 3600.0
        df_spark = df_spark.filter(F.col('emission_rate_kg_s') >= min_kg_s)

    df = df_spark.toPandas()

    # ── Type enforcement post-toPandas ────────────────────────────────────
    df['centroid_lat']       = df['centroid_lat'].astype(np.float64)
    df['centroid_lon']       = df['centroid_lon'].astype(np.float64)
    df['emission_rate_kg_s'] = pd.to_numeric(df['emission_rate_kg_s'], errors='coerce')

    # Unit conversion: kg/s → kg/hr for apples-to-apples comparison with CM
    df['emission_kg_hr'] = df['emission_rate_kg_s'] * CFG['my_to_kg_hr']

    # Extract full datetime from scene_id ('scene_YYYYMMDD_HHmmss')
    df['scene_datetime'] = pd.to_datetime(
        df['scene_id'].str.extract(r'scene_(\d{8}_\d{6})')[0],
        format='%Y%m%d_%H%M%S', utc=True, errors='coerce'
    )

    # Keep scene_date (date-only) for display/grouping
    df['scene_date'] = df['scene_datetime'].dt.date

    # Unique plume key
    df['my_plume_key'] = df['scene_id'] + '_p' + df['plume_id'].astype(str)

    n_total = len(df)
    n_valid_emission = df['emission_kg_hr'].notna().sum()
    print(f'My plumes loaded:         {n_total}')
    print(f'  With emission estimate:  {n_valid_emission}')
    print(f'  Emission range:          '
          f'{df["emission_kg_hr"].min():.1f} – '
          f'{df["emission_kg_hr"].max():.1f} kg/hr')

    return df

print('load_my_plumes() defined')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Carbon Mapper Data

# CELL ********************

import os
import time
import requests
import pandas as pd
import numpy as np


# ── Auth token — set via environment variable ─────────────
CM_API_TOKEN = os.environ.get('CM_API_TOKEN', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzc5MDk2NTM5LCJpYXQiOjE3Nzg0OTE3MzksImp0aSI6IjJmOTFjOTMxMjM5NDQxZTJiYzhkNTYyY2Y4NDU0YmYwIiwic2NvcGUiOiJzdGFjIGNhdGFsb2c6cmVhZCIsImdyb3VwcyI6IlB1YmxpYyIsImFsbF9ncm91cF9uYW1lcyI6eyJjb21tb24iOlsiUHVibGljIl19LCJvcmdhbml6YXRpb25zIjoiIiwic2V0dGluZ3MiOnt9LCJpc19zdGFmZiI6ZmFsc2UsImlzX3N1cGVydXNlciI6ZmFsc2UsInVzZXJfaWQiOjMyMjQyfQ.BjNq1DtBrMpuRajeD2zhB50oTiNbZyvaaWcTD2OlRTE')
CM_API_URL   = 'https://api.carbonmapper.org/api/v1/catalog/plumes/annotated'


def _parse_point_coords(coords) -> tuple[float | None, float | None]:
    """Extract (lon, lat) from a GeoJSON Point coordinates list."""
    try:
        if isinstance(coords, list) and len(coords) >= 2:
            return float(coords[0]), float(coords[1])
    except Exception:
        pass
    return None, None


def _fetch_cm_page(
    session:    requests.Session,
    url:        str,
    params:     list,          # list of (key, value) tuples — required for repeated bbox params
    attempt_n:  int   = 3,
    backoff_s:  float = 2.0,
) -> list:
    """
    Fetch one page. params MUST be a list of tuples so that repeated
    'bbox' keys are preserved. A dict would collapse them to one value.
    Raises immediately on 4xx. Retries on 5xx / timeout.
    """
    for attempt in range(attempt_n):
        try:
            r = session.get(url, params=params, timeout=30)

            # Log the actual encoded URL — lets you verify bbox encoding
            print(f'  GET {r.url}')

            if 400 <= r.status_code < 500:
                raise RuntimeError(
                    f'CM API {r.status_code} on {r.url}\n'
                    f'Response: {r.text[:400]}\n'
                    f'Check: endpoint URL, token validity, bbox/date params.'
                )

            if r.status_code == 429 or r.status_code >= 500:
                wait = backoff_s * (2 ** attempt)
                print(f'  HTTP {r.status_code} — retrying in {wait:.0f}s')
                time.sleep(wait)
                continue

            r.raise_for_status()

            payload = r.json()
            items   = payload.get('items', payload.get('results', payload.get('data', [])))
            print(f'  → {r.status_code}  items={len(items)}  total={payload.get("total", "?")}')
            return items

        except RuntimeError:
            raise
        except Exception as e:
            if attempt == attempt_n - 1:
                print(f'  CM API failed after {attempt_n} attempts: {e}')
                return []
            time.sleep(backoff_s * (2 ** attempt))

    return []


def _parse_cm_record(rec: dict) -> dict | None:
    """
    Parse one CM annotated-plume record.
    Handles both raw nested dict and pd.json_normalize'd flat keys.
    """
    try:
        coords = None
        if 'geometry_json' in rec and isinstance(rec['geometry_json'], dict):
            coords = rec['geometry_json'].get('coordinates')
        elif 'geometry_json.coordinates' in rec:
            coords = rec['geometry_json.coordinates']

        lon, lat = _parse_point_coords(coords)

        if lat is None:
            lat = rec.get('latitude') or rec.get('lat')
            lon = rec.get('longitude') or rec.get('lon')

        if lat is None or lon is None:
            return None

        emission    = rec.get('emission_auto')
        uncertainty = rec.get('emission_uncertainty_auto')
        timestamp   = rec.get('scene_timestamp', '')
        date_str    = str(timestamp)[:10] if timestamp else None

        return {
            'cm_id':                         str(rec.get('plume_id', '')),
            'cm_lat':                        float(lat),
            'cm_lon':                        float(lon),
            'cm_emission_kg_hr':             float(emission)     if emission    is not None else None,
            'cm_emission_uncertainty_kg_hr': float(uncertainty)  if uncertainty is not None else None,
            'cm_scene_timestamp':            str(timestamp),
            'cm_date':                       date_str,
            'cm_instrument':                 str(rec.get('instrument', '')),
            'cm_gas':                        str(rec.get('gas', 'CH4')),
            'cm_sector':                     str(rec.get('sector', '')),
        }
    except Exception as e:
        print(f'  _parse_cm_record failed: {e}  rec_keys={list(rec.keys())[:8]}')
        return None


def load_carbon_mapper(
    bbox:          dict,
    date_start:    str,
    date_end:      str,
    token:         str  = CM_API_TOKEN,
    cache_table:   str  = CFG['cm_cache_table'],
    force_refresh: bool = False,
    n_chunks:      int  = 4,
    page_size:     int  = 1000,
) -> pd.DataFrame:
    """
    Fetch Carbon Mapper annotated plumes, cache to Delta.

    Endpoint:   /api/v1/catalog/plumes/annotated  (Bearer token required)
    Response:   {"items": [...], "total": N}
    Pagination: offset-based
    Tiling:     bbox split into n_chunks longitude strips
    bbox param: passed as repeated query params (?bbox=v1&bbox=v2&bbox=v3&bbox=v4)
                NOT comma-joined — Django Ninja requires the repeated-param form.
    """

    if not token:
        raise ValueError(
            'CM_API_TOKEN is empty.\n'
            "Set it with: os.environ['CM_API_TOKEN'] = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzc3MzY4MDg5LCJpYXQiOjE3NzY3NjMyODksImp0aSI6ImIyYTYyZGNiN2Y0ZDRjNjk5YjMyNTc3ODc3YWRhZGM4Iiwic2NvcGUiOiJzdGFjIGNhdGFsb2c6cmVhZCIsImdyb3VwcyI6IlB1YmxpYyIsImFsbF9ncm91cF9uYW1lcyI6eyJjb21tb24iOlsiUHVibGljIl19LCJvcmdhbml6YXRpb25zIjoiIiwic2V0dGluZ3MiOnt9LCJpc19zdGFmZiI6ZmFsc2UsImlzX3N1cGVydXNlciI6ZmFsc2UsInVzZXJfaWQiOjMyMjQyfQ.hpFbJUI28QLFgwxhJntI7mvCAGeiEng5x72YChkI7yQ'"
        )

    required_bbox_keys = ['lon_min', 'lat_min', 'lon_max', 'lat_max']
    missing_keys = [k for k in required_bbox_keys if k not in bbox]
    if missing_keys:
        raise ValueError(f'bbox missing keys: {missing_keys}')

    try:
        start_dt = pd.to_datetime(date_start)
        end_dt   = pd.to_datetime(date_end)
        if start_dt > end_dt:
            raise ValueError(f'date_start {date_start} is after date_end {date_end}')
    except Exception as e:
        raise ValueError(f'Invalid date range: {e}')

    # ── Try Delta cache first ─────────────────────────────────────────────
    if not force_refresh:
        try:
            df_cached = spark.read.table(cache_table).toPandas()
            if len(df_cached) > 0:
                df_cached['cm_lat']            = df_cached['cm_lat'].astype(np.float64)
                df_cached['cm_lon']            = df_cached['cm_lon'].astype(np.float64)
                df_cached['cm_emission_kg_hr'] = pd.to_numeric(
                    df_cached['cm_emission_kg_hr'], errors='coerce'
                )
                print(f'CM cache loaded: {len(df_cached):,} records from {cache_table}')
                return df_cached
        except Exception:
            print('No cache found — fetching from API')

    # ── Tile bbox into n_chunks longitude strips ──────────────────────────
    lon_step   = (bbox['lon_max'] - bbox['lon_min']) / n_chunks
    sub_bboxes = [
        [
            bbox['lon_min'] + i * lon_step,        # lon_min of chunk
            bbox['lat_min'],
            bbox['lon_min'] + (i + 1) * lon_step,  # lon_max of chunk
            bbox['lat_max'],
        ]
        for i in range(n_chunks)
    ]

    session = requests.Session()
    session.headers.update({
        'Authorization': f'Bearer {token}',
        'Accept':        'application/json',
    })

    all_records = []
    seen_ids    = set()

    for chunk_idx, chunk_bbox in enumerate(sub_bboxes):
        print(f'\nChunk {chunk_idx + 1}/{n_chunks}: '
              f'lon [{chunk_bbox[0]:.3f}, {chunk_bbox[2]:.3f}]')
        page = 0

        while True:
            # ── CRITICAL FIX ──────────────────────────────────────────────
            # Build params as a list of tuples so 'bbox' appears four times
            # in the query string: ?bbox=lon_min&bbox=lat_min&...
            # A dict would collapse all bbox values to one key — 422 error.
            # Django Ninja's pagination parser requires the repeated form.
            params = [
                ('limit',      page_size),
                ('offset',     page * page_size),
                ('bbox',       round(chunk_bbox[0], 6)),  # lon_min
                ('bbox',       round(chunk_bbox[1], 6)),  # lat_min
                ('bbox',       round(chunk_bbox[2], 6)),  # lon_max
                ('bbox',       round(chunk_bbox[3], 6)),  # lat_max
                ('start_date', date_start),
                ('end_date',   date_end),
            ]

            items = _fetch_cm_page(session, CM_API_URL, params)

            new_count = 0
            for rec in items:
                pid = rec.get('plume_id')
                if pid and pid not in seen_ids:
                    all_records.append(rec)
                    seen_ids.add(pid)
                    new_count += 1

            print(f'  Page {page}: {len(items)} returned, {new_count} new unique')

            if len(items) < page_size:
                break
            page += 1
            time.sleep(0.15)

    if not all_records:
        print('\nWARNING: no CM records returned.')
        print(f'  URL:        {CM_API_URL}')
        print(f'  Date range: {date_start} → {date_end}')
        print(f'  bbox:       {bbox}')
        print('  Check: token validity, bbox covers CM flight paths, date range is valid')
        return pd.DataFrame(columns=[
            'cm_id', 'cm_lat', 'cm_lon', 'cm_emission_kg_hr',
            'cm_emission_uncertainty_kg_hr', 'cm_scene_timestamp',
            'cm_date', 'cm_instrument', 'cm_gas', 'cm_sector',
        ])

    # ── Parse ─────────────────────────────────────────────────────────────
    df_norm = pd.json_normalize(all_records)
    records_parsed = [
        _parse_cm_record(row.to_dict())
        for _, row in df_norm.iterrows()
    ]
    records_parsed = [r for r in records_parsed if r is not None]

    df = pd.DataFrame(records_parsed)

    df = (
        df.loc[
            df['cm_lat'].between(bbox['lat_min'], bbox['lat_max']) &
            df['cm_lon'].between(bbox['lon_min'], bbox['lon_max'])
        ]
        .dropna(subset=['cm_lat', 'cm_lon', 'cm_emission_kg_hr'])
        .drop_duplicates(subset=['cm_id'])
        .reset_index(drop=True)
    )

    print(df[['cm_date', 'cm_lat', 'cm_lon']].agg({
    'cm_date': ['min', 'max'],
    'cm_lat':  ['min', 'max'],
    'cm_lon':  ['min', 'max'],
}))

    print(f'\nCM records fetched and parsed: {len(df):,}')
    if len(df) > 0:
        print(f'  Emission range: {df["cm_emission_kg_hr"].min():.1f} – '
              f'{df["cm_emission_kg_hr"].max():.1f} kg/hr')

    # ── Write to Delta cache ──────────────────────────────────────────────
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    CM_SCHEMA = StructType([
        StructField('cm_id',                         StringType(), True),
        StructField('cm_lat',                        DoubleType(), True),
        StructField('cm_lon',                        DoubleType(), True),
        StructField('cm_emission_kg_hr',             DoubleType(), True),
        StructField('cm_emission_uncertainty_kg_hr', DoubleType(), True),
        StructField('cm_scene_timestamp',            StringType(), True),
        StructField('cm_date',                       StringType(), True),
        StructField('cm_instrument',                 StringType(), True),
        StructField('cm_gas',                        StringType(), True),
        StructField('cm_sector',                     StringType(), True),
    ])

    for col in ['cm_emission_kg_hr', 'cm_emission_uncertainty_kg_hr', 'cm_lat', 'cm_lon']:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype(np.float64)
    for col in ['cm_id', 'cm_scene_timestamp', 'cm_date', 'cm_instrument', 'cm_gas', 'cm_sector']:
        df[col] = df[col].where(df[col].notna(), other=None).astype(object)

    (
        spark.createDataFrame(df, schema=CM_SCHEMA)
        .write.format('delta').mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(cache_table)
    )
    print(f'Cached → {cache_table}')

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Plume Matching

# CELL ********************

def _haversine_km(lat1: np.ndarray, lon1: np.ndarray,
                  lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    """Vectorised haversine distance in km."""
    R = 6371.0
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = (np.sin(dlat / 2) ** 2
         + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2))
         * np.sin(dlon / 2) ** 2)
    return 2 * R * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def match_plumes(
    df_mine:         pd.DataFrame,
    df_cm:           pd.DataFrame,
    spatial_km:      float = CFG['match_spatial_km'],
    temporal_h:      float = CFG['match_temporal_h'],
    # Scoring decay scales
    d_scale_km:      float = 20.0,   # spatial Gaussian decay — 50% score at ~14km
    t_scale_h:       float = 72.0,   # temporal Gaussian decay — 50% score at ~50h
    # Thresholds
    score_threshold: float = 0.05,   # minimum combined score to consider a match
    allow_one_to_many: bool = False,  # if True, one CM plume can match many of mine
    # Diagnostic output
    debug:           bool  = True,
) -> pd.DataFrame:
    """
    Scoring-based plume matcher.

    Replaces hard temporal/spatial cutoffs with Gaussian decay scores:
        spatial_score  = exp(-dist_km²  / (2 * d_scale_km²))
        temporal_score = exp(-dt_h²     / (2 * t_scale_h²))
        score          = spatial_score * temporal_score

    Greedy nearest-first assignment after scoring. Hard spatial_km cap
    is kept as a pre-filter to bound compute; set spatial_km=200 to
    effectively disable it.
    """
    mine_valid = df_mine.dropna(subset=['centroid_lat', 'centroid_lon', 'emission_kg_hr']).copy()
    cm_valid   = df_cm.dropna(subset=['cm_lat', 'cm_lon', 'cm_emission_kg_hr']).copy()

    if len(mine_valid) == 0 or len(cm_valid) == 0:
        print('WARNING: insufficient data for matching after null drop')
        return pd.DataFrame()

    # ── Timestamp parsing ─────────────────────────────────────────────────
    # Use scene_date (datetime) for mine — tz_localize not convert
    mine_valid['_t'] = pd.to_datetime(
        mine_valid.get('scene_datetime', mine_valid['scene_date']),
        utc=True, errors='coerce'
    )

    # CRITICAL: prefer full cm_scene_timestamp over date-only cm_date
    # cm_date is truncated to midnight — causes all deltas to be multiples of 24h
    if 'cm_scene_timestamp' in cm_valid.columns:
        cm_valid['_t'] = pd.to_datetime(
            cm_valid['cm_scene_timestamp'], utc=True, errors='coerce'
        )
        n_fallback = cm_valid['_t'].isna().sum()
        if n_fallback > 0:
            # Fall back to cm_date for records where full timestamp failed
            fallback_t = pd.to_datetime(
                cm_valid.loc[cm_valid['_t'].isna(), 'cm_date'],
                format='%Y-%m-%d', utc=True, errors='coerce'
            )
            cm_valid.loc[cm_valid['_t'].isna(), '_t'] = fallback_t
            print(f'  CM timestamp: {n_fallback} records fell back to date-only')
    else:
        cm_valid['_t'] = pd.to_datetime(
            cm_valid['cm_date'], format='%Y-%m-%d', utc=True, errors='coerce'
        )

    n_mine_nat = mine_valid['_t'].isna().sum()
    n_cm_nat   = cm_valid['_t'].isna().sum()
    if n_mine_nat > 0:
        print(f'WARNING: {n_mine_nat} mine timestamps are NaT')
    if n_cm_nat > 0:
        print(f'WARNING: {n_cm_nat} CM timestamps are NaT')

    if debug:
        print(f'\n── Temporal overlap diagnostic ──')
        mine_t_min = mine_valid['_t'].min()
        mine_t_max = mine_valid['_t'].max()
        cm_t_min   = cm_valid['_t'].dropna().min()
        cm_t_max   = cm_valid['_t'].dropna().max()
        print(f'  My date range:  {mine_t_min.date()} → {mine_t_max.date()}')
        print(f'  CM date range:  {cm_t_min.date()} → {cm_t_max.date()}')
        overlap_start = max(mine_t_min, cm_t_min)
        overlap_end   = min(mine_t_max, cm_t_max)
        if overlap_start < overlap_end:
            print(f'  Overlap window: {overlap_start.date()} → {overlap_end.date()}')
            n_mine_in = mine_valid['_t'].between(overlap_start, overlap_end).sum()
            n_cm_in   = cm_valid['_t'].between(overlap_start, overlap_end).sum()
            print(f'  My plumes in overlap:  {n_mine_in} / {len(mine_valid)}')
            print(f'  CM plumes in overlap:  {n_cm_in} / {len(cm_valid)}')
        else:
            print(f'  ⚠ NO TEMPORAL OVERLAP — date ranges do not intersect!')
            print(f'    This is likely why you have so few temporal candidates.')

    # ── KD-tree spatial pre-filter ────────────────────────────────────────
    def _to_xyz(lat_deg, lon_deg):
        lat = np.radians(lat_deg)
        lon = np.radians(lon_deg)
        return np.column_stack([
            np.cos(lat) * np.cos(lon),
            np.cos(lat) * np.sin(lon),
            np.sin(lat),
        ])

    R_earth_km   = 6371.0
    chord_thresh = 2.0 * np.sin(spatial_km / (2.0 * R_earth_km))

    cm_valid   = cm_valid.reset_index(drop=True)
    mine_valid = mine_valid.reset_index(drop=True)

    cm_xyz   = _to_xyz(cm_valid['cm_lat'].values,        cm_valid['cm_lon'].values)
    mine_xyz = _to_xyz(mine_valid['centroid_lat'].values, mine_valid['centroid_lon'].values)

    tree            = cKDTree(cm_xyz)
    candidate_lists = tree.query_ball_point(mine_xyz, r=chord_thresh)

    n_spatial = sum(len(c) for c in candidate_lists)
    print(f'\nSpatial candidates ({spatial_km} km):  {n_spatial}')

    # ── Nearest-CM diagnostics (regardless of temporal filter) ───────────
    if debug:
        dists_all, idxs_all = tree.query(mine_xyz, k=1)
        # chord → km
        dists_km_all = 2 * R_earth_km * np.arcsin(np.clip(dists_all / 2, 0, 1))
        print(f'\n── Nearest-CM distance distribution (all my plumes) ──')
        for pct in [25, 50, 75, 90, 100]:
            print(f'  p{pct:3d}: {np.percentile(dists_km_all, pct):.1f} km')

        # Nearest-CM time delta ignoring spatial filter
        dt_nearest = []
        for my_idx in range(len(mine_valid)):
            cm_idx = int(idxs_all[my_idx])
            my_t = mine_valid.loc[my_idx, '_t']
            cm_t = cm_valid.loc[cm_idx, '_t']
            if pd.notna(my_t) and pd.notna(cm_t):
                dt_nearest.append(abs((my_t - cm_t).total_seconds()) / 3600.0)
        if dt_nearest:
            print(f'\n── Nearest-CM time delta distribution (nearest spatial match) ──')
            for pct in [25, 50, 75, 90, 100]:
                print(f'  p{pct:3d}: {np.percentile(dt_nearest, pct):.1f} h')

    # ── Score each candidate pair ─────────────────────────────────────────
    pending = []  # (score, dist_km, dt_h, my_idx, cm_idx)

    for my_idx, cm_candidates in enumerate(candidate_lists):
        if not cm_candidates:
            continue

        my_lat = mine_valid.loc[my_idx, 'centroid_lat']
        my_lon = mine_valid.loc[my_idx, 'centroid_lon']
        my_t   = mine_valid.loc[my_idx, '_t']

        for cm_idx in cm_candidates:
            dist_km = float(_haversine_km(
                np.array([my_lat]), np.array([my_lon]),
                np.array([cm_valid.loc[cm_idx, 'cm_lat']]),
                np.array([cm_valid.loc[cm_idx, 'cm_lon']]),
            )[0])

            cm_t = cm_valid.loc[cm_idx, '_t']
            dt_h = np.nan
            if pd.notna(my_t) and pd.notna(cm_t):
                dt_h = abs((my_t - cm_t).total_seconds()) / 3600.0

            spatial_score  = np.exp(-(dist_km ** 2) / (2 * d_scale_km ** 2))
            # be more generous when timestamp is unknown (date-only fallback)
            # Treat it as a weak positive rather than neutral
            temporal_score = (
                np.exp(-(dt_h ** 2) / (2 * t_scale_h ** 2))
                if not np.isnan(dt_h) else 0.75  # unknown time → assume plausible
            )
            score = spatial_score * temporal_score

            if score >= score_threshold:
                pending.append((score, dist_km, dt_h, my_idx, cm_idx))

    # Sort by score descending — best match first
    pending.sort(key=lambda x: -x[0])
    print(f'Scored candidates (score ≥ {score_threshold}):  {len(pending)}')

    if not pending:
        print('No candidates above score threshold.')
        print('Suggestions:')
        print('  → Increase spatial_km (currently {spatial_km}) or d_scale_km')
        print('  → Increase t_scale_h or decrease score_threshold')
        print('  → Check temporal overlap in diagnostic output above')
        return pd.DataFrame()

    # ── Greedy assignment ─────────────────────────────────────────────────
    matched_my  = set()
    matched_cm  = set()
    match_pairs = []

    for score, dist_km, dt_h, my_idx, cm_idx in pending:
        if my_idx in matched_my:
            continue
        if cm_idx in matched_cm and not allow_one_to_many:
            continue
        matched_my.add(my_idx)
        if not allow_one_to_many:
            matched_cm.add(cm_idx)
        match_pairs.append((my_idx, cm_idx, dist_km, dt_h, score))

    # ── Build output DataFrame ────────────────────────────────────────────
    rows = []
    for my_idx, cm_idx, dist_km, dt_h, score in match_pairs:
        m  = mine_valid.loc[my_idx]
        cm = cm_valid.loc[cm_idx]

        my_em = float(m['emission_kg_hr'])
        cm_em = float(cm['cm_emission_kg_hr'])

        rows.append({
            'my_plume_key':             m['my_plume_key'],
            'cm_id':                    cm['cm_id'],
            'my_lat':                   round(float(m['centroid_lat']),  4),
            'my_lon':                   round(float(m['centroid_lon']),  4),
            'cm_lat':                   round(float(cm['cm_lat']),       4),
            'cm_lon':                   round(float(cm['cm_lon']),       4),
            'distance_km':              round(dist_km, 3),
            'my_date':                  str(m['scene_date'].date()) if pd.notna(m['scene_date']) else None,
            'cm_date':                  cm['cm_date'],
            'time_delta_h':             round(dt_h, 2) if not np.isnan(dt_h) else None,
            'match_score':              round(score, 4),
            'my_emission_kg_hr':        round(my_em, 2),
            'cm_emission_kg_hr':        round(cm_em, 2),
            'cm_emission_unc_kg_hr':    round(float(cm['cm_emission_uncertainty_kg_hr']), 2)
                                        if pd.notna(cm['cm_emission_uncertainty_kg_hr']) else None,
            'emission_ratio_my_cm':     round(my_em / cm_em, 4) if cm_em > 0 else None,
            'emission_diff_kg_hr':      round(my_em - cm_em, 2),
            'my_area_km2':              float(m.get('area_km2',          np.nan)),
            'my_max_delta_ppb':         float(m.get('max_delta_ch4_ppb', np.nan)),
            'my_ime_kg':                float(m.get('ime_kg',            np.nan)),
            'my_wind_speed_ms':         float(m.get('wind_speed_ms',     np.nan)),
            'my_wind_aligned':          bool(m.get('wind_aligned', False)),
            'cm_instrument':            cm.get('cm_instrument', ''),
        })

    matched = pd.DataFrame(rows)

    print(f'\nMatching results:')
    print(f'  My plumes:        {len(mine_valid)}')
    print(f'  CM plumes:        {len(cm_valid)}')
    print(f'  Matched pairs:    {len(matched)}')
    print(f'  Match rate (my):  {100*len(matched)/len(mine_valid):.1f}%')
    if len(matched) > 0:
        print(f'  Median distance:  {matched["distance_km"].median():.2f} km')
        print(f'  Median score:     {matched["match_score"].median():.3f}')
        if matched['time_delta_h'].notna().any():
            print(f'  Median Δt:        {matched["time_delta_h"].median():.1f} h')

    return matched

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Comparison Metrics

# CELL ********************

def compute_metrics(matched: pd.DataFrame,
                    outlier_clip_pct: float = CFG['outlier_clip_pct']) -> dict:
    """
    Compute statistical comparison metrics on matched emission pairs.
    Clips extreme ratio outliers before computing distributional stats.
    """
    df = matched.dropna(subset=['my_emission_kg_hr', 'cm_emission_kg_hr']).copy()

    if len(df) < 3:
        print('WARNING: fewer than 3 matched pairs — metrics unreliable')
        return {}

    my = df['my_emission_kg_hr'].values
    cm = df['cm_emission_kg_hr'].values

    # Clip ratio outliers (top/bottom 1%)
    ratio = my / np.where(cm > 0, cm, np.nan)
    lo, hi = np.nanpercentile(ratio, [outlier_clip_pct * 100,
                                       (1 - outlier_clip_pct) * 100])
    mask = (ratio >= lo) & (ratio <= hi)
    n_clipped = (~mask).sum()
    my_c, cm_c = my[mask], cm[mask]

    if len(my_c) < 3:
        my_c, cm_c = my, cm  # fallback to unclipped if too aggressive

    pearson_r,  pearson_p  = stats.pearsonr(np.log1p(my_c), np.log1p(cm_c))
    spearman_r, spearman_p = stats.spearmanr(my_c, cm_c)

    rmse         = float(np.sqrt(np.mean((my_c - cm_c) ** 2)))
    mean_bias    = float(np.mean(my_c - cm_c))          # > 0: mine overestimates
    median_ratio = float(np.median(ratio[mask]))         # 1.0 = perfect
    mean_ratio   = float(np.mean(ratio[mask]))
    valid_log = (my_c > 0) & (cm_c > 0)
    log_bias = float(np.mean(np.log(my_c[valid_log] / cm_c[valid_log]))) \
               if valid_log.sum() > 0 else np.nan

    m = {
        'n_pairs':          len(df),
        'n_after_clip':     int(mask.sum()),
        'n_clipped':        int(n_clipped),
        'pearson_r':        round(pearson_r,  4),
        'pearson_p':        round(pearson_p,  4),
        'spearman_r':       round(spearman_r, 4),
        'spearman_p':       round(spearman_p, 4),
        'rmse_kg_hr':       round(rmse,         2),
        'mean_bias_kg_hr':  round(mean_bias,     2),
        'median_ratio':     round(median_ratio,  4),
        'mean_ratio':       round(mean_ratio,    4),
        'log_bias':         round(log_bias,      4),
        'my_median_kg_hr':  round(float(np.median(my_c)), 2),
        'cm_median_kg_hr':  round(float(np.median(cm_c)), 2),
    }

    print('\n=== Emission Comparison Metrics ===')
    print(f'  Pairs (total / after clip):  {m["n_pairs"]} / {m["n_after_clip"]}')
    print(f'  Pearson r  (log-space):      {m["pearson_r"]:.3f}  (p={m["pearson_p"]:.3e})')
    print(f'  Spearman r:                  {m["spearman_r"]:.3f}  (p={m["spearman_p"]:.3e})')
    print(f'  RMSE:                        {m["rmse_kg_hr"]:.1f} kg/hr')
    print(f'  Mean bias (mine − CM):       {m["mean_bias_kg_hr"]:+.1f} kg/hr')
    print(f'  Median ratio (mine/CM):      {m["median_ratio"]:.3f}  (1.0 = no bias)')
    print(f'  Log bias:                    {m["log_bias"]:+.3f}  (0 = no bias)')
    print(f'  My median:                   {m["my_median_kg_hr"]:.1f} kg/hr')
    print(f'  CM median:                   {m["cm_median_kg_hr"]:.1f} kg/hr')

    return m

print('compute_metrics() defined')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Visualisations

# CELL ********************

def plot_comparison(matched: pd.DataFrame, metrics: dict) -> None:
    """
    Four-panel comparison figure:
      A. Scatter: my vs CM emission (log scale) with 1:1 and best-fit lines
      B. Histogram: emission ratio distribution
      C. Bland-Altman: bias vs magnitude
      D. CDF of emissions: my vs CM
    """
    df = matched.dropna(subset=['my_emission_kg_hr', 'cm_emission_kg_hr']).copy()
    if len(df) == 0:
        print('No data to plot')
        return

    my    = df['my_emission_kg_hr'].values
    cm    = df['cm_emission_kg_hr'].values
    ratio = my / np.where(cm > 0, cm, np.nan)

    fig = plt.figure(figsize=(16, 12))
    gs  = gridspec.GridSpec(2, 2, hspace=0.38, wspace=0.32)

    # ── A: Scatter log-log ────────────────────────────────────────────────
    ax0 = fig.add_subplot(gs[0, 0])
    sc  = ax0.scatter(cm, my, c=df['distance_km'], cmap='plasma_r',
                      s=40, alpha=0.75, edgecolors='k', linewidths=0.3, zorder=3)
    plt.colorbar(sc, ax=ax0, label='Match distance (km)', shrink=0.85)

    lim_lo = max(min(cm.min(), my.min()) * 0.5, 1.0)
    lim_hi = max(cm.max(), my.max()) * 2.0
    ax0.plot([lim_lo, lim_hi], [lim_lo, lim_hi], 'k--', lw=1.2,
             alpha=0.6, label='1:1')

    mask = (cm > 0) & (my > 0)
    if mask.sum() >= 3:
        slope, intercept, _, _, _ = stats.linregress(
            np.log10(cm[mask]), np.log10(my[mask])
        )
        x_fit = np.array([lim_lo, lim_hi])
        y_fit = 10 ** (slope * np.log10(x_fit) + intercept)
        ax0.plot(x_fit, y_fit, 'r-', lw=1.5, alpha=0.8,
                 label=f'OLS (slope={slope:.2f})')

    ax0.set_xscale('log')
    ax0.set_yscale('log')
    ax0.set_xlim(lim_lo, lim_hi)
    ax0.set_ylim(lim_lo, lim_hi)
    ax0.set_xlabel('Carbon Mapper emission [kg/hr]', fontsize=11)
    ax0.set_ylabel('My emission [kg/hr]', fontsize=11)
    ax0.set_title(
        f'Emission Comparison (n={len(df)})\n'
        f'Pearson r={metrics.get("pearson_r", np.nan):.3f}  '
        f'Spearman r={metrics.get("spearman_r", np.nan):.3f}',
        fontsize=10
    )
    ax0.legend(fontsize=8)
    ax0.grid(True, which='both', alpha=0.25)

    # ── B: Ratio histogram ────────────────────────────────────────────────
    ax1 = fig.add_subplot(gs[0, 1])
    ratio_clean   = ratio[np.isfinite(ratio)]
    lo_c          = np.percentile(ratio_clean, 2)
    hi_c          = np.percentile(ratio_clean, 98)
    ratio_clipped = ratio_clean[(ratio_clean >= lo_c) & (ratio_clean <= hi_c)]

    ax1.hist(ratio_clipped, bins=30, color='steelblue', alpha=0.75, edgecolor='k')
    ax1.axvline(1.0, color='k', linestyle='--', lw=1.5, label='Ratio = 1 (perfect)')
    ax1.axvline(np.median(ratio_clipped), color='firebrick', linestyle='-',
                lw=1.5, label=f'Median = {np.median(ratio_clipped):.2f}')
    ax1.set_xlabel('My emission / CM emission', fontsize=11)
    ax1.set_ylabel('Count', fontsize=11)
    ax1.set_title('Emission Ratio Distribution\n(2–98% clipped)', fontsize=10)
    ax1.legend(fontsize=8)
    ax1.grid(axis='y', alpha=0.3)

    # ── C: Bland-Altman ───────────────────────────────────────────────────
    ax2 = fig.add_subplot(gs[1, 0])
    mean_em   = (my + cm) / 2.0
    diff_em   = my - cm
    mean_diff = float(np.mean(diff_em))
    std_diff  = float(np.std(diff_em))

    ax2.scatter(mean_em, diff_em, s=30, alpha=0.65,
                c='darkorange', edgecolors='k', linewidths=0.3)
    ax2.axhline(mean_diff, color='red', lw=1.5, linestyle='--',
                label=f'Mean bias: {mean_diff:+.1f} kg/hr')
    ax2.axhline(mean_diff + 1.96 * std_diff, color='gray', lw=1, linestyle=':',
                label=f'+1.96σ: {mean_diff + 1.96 * std_diff:.1f}')
    ax2.axhline(mean_diff - 1.96 * std_diff, color='gray', lw=1, linestyle=':',
                label=f'-1.96σ: {mean_diff - 1.96 * std_diff:.1f}')
    ax2.axhline(0, color='black', lw=0.8, alpha=0.4)
    ax2.set_xscale('log')
    ax2.set_xlabel('Mean emission (Mine + CM) / 2  [kg/hr]', fontsize=10)
    ax2.set_ylabel('Mine − CM  [kg/hr]', fontsize=10)
    ax2.set_title('Bland-Altman: Bias vs Magnitude', fontsize=10)
    ax2.legend(fontsize=7)
    ax2.grid(alpha=0.3)

    # ── D: CDF comparison ─────────────────────────────────────────────────
    ax3 = fig.add_subplot(gs[1, 1])
    for vals, label, color in [
        (np.sort(my), 'Mine',          'steelblue'),
        (np.sort(cm), 'Carbon Mapper', 'firebrick'),
    ]:
        cdf = np.arange(1, len(vals) + 1) / len(vals)
        ax3.plot(vals, cdf, lw=1.8, label=label, color=color)

    ks_stat, ks_p = stats.ks_2samp(my, cm)
    ax3.set_xscale('log')
    ax3.set_xlabel('Emission [kg/hr]', fontsize=11)
    ax3.set_ylabel('CDF', fontsize=11)
    ax3.set_title(
        f'Emission Distribution (CDF)\nKS stat={ks_stat:.3f} p={ks_p:.2e}',
        fontsize=10
    )
    ax3.legend(fontsize=9)
    ax3.grid(which='both', alpha=0.25)

    fig.suptitle(
        f'My Plumes vs Carbon Mapper — Emission Validation\n'
        f'RMSE={metrics.get("rmse_kg_hr", np.nan):.1f} kg/hr  '
        f'Bias={metrics.get("mean_bias_kg_hr", np.nan):+.1f} kg/hr  '
        f'Median ratio={metrics.get("median_ratio", np.nan):.3f}',
        fontsize=12, fontweight='bold'
    )

    plt.savefig('plume_validation.png', dpi=150, bbox_inches='tight')
    plt.show()
    print('Saved: plume_validation.png')

print('plot_comparison() defined')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Run Pipeline

# CELL ********************

# In the Run Pipeline block, replace the date range derivation with this:
from datetime import datetime, timezone

print('Loading my plume catalog...')
df_mine = load_my_plumes(
    table_name=CFG['plume_catalog_table'],
    filter_wind_aligned=CFG['filter_wind_aligned'],
)

if len(df_mine) > 0:
    pad = 1.0
    # TROPOMI date range — keep as-is for reference
    my_date_start = (
        df_mine['scene_date'].min().strftime('%Y-%m-%d')
        if df_mine['scene_date'].notna().any()
        else '2024-01-01'
    )
    my_date_end = (
        df_mine['scene_date'].max().strftime('%Y-%m-%d')
        if df_mine['scene_date'].notna().any()
        else datetime.now(timezone.utc).strftime('%Y-%m-%d')
    )
    bbox = {
        'lon_min': float(df_mine['centroid_lon'].min() - pad),
        'lat_min': float(df_mine['centroid_lat'].min() - pad),
        'lon_max': float(df_mine['centroid_lon'].max() + pad),
        'lat_max': float(df_mine['centroid_lat'].max() + pad),
    }

    # CM fetch window: go back 12 months from TROPOMI start
    # This captures all CM flights over your bbox regardless of
    # whether they overlap your TROPOMI dates
    cm_date_start = '2025-01-01'
    cm_date_end   = my_date_end

    print(f'Derived bbox:          {bbox}')
    print(f'My TROPOMI range:      {my_date_start} → {my_date_end}')
    print(f'CM fetch range:        {cm_date_start} → {cm_date_end}')
else:
    bbox          = {'lon_min': -106.0, 'lat_min': 25.0, 'lon_max': -90.0, 'lat_max': 36.0}
    my_date_start = '2024-01-01'
    my_date_end   = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    cm_date_start = '2025-01-01'
    cm_date_end   = my_date_end

print('\nLoading Carbon Mapper data...')
df_cm = load_carbon_mapper(
    bbox=bbox,
    date_start=cm_date_start,
    date_end=cm_date_end,
    token=CM_API_TOKEN,
    force_refresh=True,   # bust cache — wider date range now
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: refresh cache with full CM history over your bbox
df_cm = load_carbon_mapper(
    bbox=bbox,
    date_start='2025-01-01',
    date_end=my_date_end,
    token=CM_API_TOKEN,
    force_refresh=True,
)
print(f'Full CM cache: {len(df_cm):,} plumes')

# Step 2: filter to temporal window for matching
cm_t = pd.to_datetime(df_cm['cm_scene_timestamp'], utc=True, errors='coerce')
cm_t = cm_t.fillna(pd.to_datetime(df_cm['cm_date'], utc=True, errors='coerce'))

temporal_buffer_days = 30
overlap_start = pd.Timestamp(my_date_start, tz='UTC') - pd.Timedelta(days=temporal_buffer_days)
overlap_end   = pd.Timestamp(my_date_end,   tz='UTC') + pd.Timedelta(days=temporal_buffer_days)

df_cm_overlap = df_cm[cm_t.between(overlap_start, overlap_end)].copy()
print(f'CM plumes in ±{temporal_buffer_days}d window: {len(df_cm_overlap)} / {len(df_cm)}')

# Fix: scene_date may be a datetime.date (not datetime.datetime) depending on
# how load_my_plumes extracted it. Normalise to tz-aware Timestamp so that
# both .date() calls and tz_localize inside match_plumes work without error.
df_mine = df_mine.copy()
df_mine['scene_date'] = pd.to_datetime(df_mine['scene_date'], utc=True, errors='coerce')

# Step 3: match against the filtered set
print('Matching plumes...')
matched = match_plumes(
    df_mine=df_mine,
    df_cm=df_cm_overlap,
    spatial_km=       CFG['match_spatial_km'],
    d_scale_km=       CFG['d_scale_km'],
    t_scale_h=        CFG['t_scale_h'],
    score_threshold=  CFG['score_threshold'],
    allow_one_to_many=False,
    debug=            True,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#DEBUG
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(14, 8))

# All CM plumes in window
ax.scatter(df_cm_overlap['cm_lon'], df_cm_overlap['cm_lat'],
           s=15, alpha=0.5, c='red', label=f'CM plumes ({len(df_cm_overlap)})', zorder=3)

# My TROPOMI plumes
ax.scatter(df_mine['centroid_lon'], df_mine['centroid_lat'],
           s=25, alpha=0.7, c='blue', marker='^',
           label=f'My TROPOMI plumes ({len(df_mine)})', zorder=4)

# Matched pairs
if len(matched) > 0:
    ax.scatter(matched['my_lon'], matched['my_lat'],
               s=80, c='blue', marker='^', edgecolors='gold',
               linewidths=2, label='Matched TROPOMI', zorder=5)
    ax.scatter(matched['cm_lon'], matched['cm_lat'],
               s=80, c='red', edgecolors='gold',
               linewidths=2, label='Matched CM', zorder=5)
    for _, row in matched.iterrows():
        ax.plot([row['my_lon'], row['cm_lon']],
                [row['my_lat'], row['cm_lat']],
                'gold', lw=1, alpha=0.7)

ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')
ax.legend(fontsize=9)
ax.grid(alpha=0.3)
ax.set_title('TROPOMI plumes vs CM plumes — geographic overlap')
plt.tight_layout()
plt.savefig('spatial_overlap.png', dpi=150)
plt.show()

# Also print the geographic density
print('CM plume lon clusters:')
print(pd.cut(df_cm_overlap['cm_lon'], bins=8).value_counts().sort_index())
print('\nMy plume lon clusters:')
print(pd.cut(df_mine['centroid_lon'], bins=8).value_counts().sort_index())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# How many of your plumes are at locations seen in multiple scenes?
from scipy.spatial import cKDTree

coords = np.radians(df_mine[['centroid_lat', 'centroid_lon']].values)
tree   = cKDTree(coords)

# For each plume, count how many other plumes are within 50 km
R = 6371.0
chord_50km = 2 * np.sin(50 / (2 * R))
neighbor_counts = [
    len(tree.query_ball_point(coords[i], r=chord_50km)) - 1  # exclude self
    for i in range(len(df_mine))
]
df_mine['n_nearby_plumes'] = neighbor_counts

print('Plume persistence (nearby detections within 50 km):')
print(df_mine['n_nearby_plumes'].value_counts().sort_index())

print(f'\nIsolated (0 neighbors):  {(df_mine["n_nearby_plumes"] == 0).sum()}')
print(f'Seen 1+ times nearby:    {(df_mine["n_nearby_plumes"] >= 1).sum()}')
print(f'Seen 3+ times nearby:    {(df_mine["n_nearby_plumes"] >= 3).sum()}')

# Cross with CM — do persistent plumes match better?
if 'matched_strong' in df_mine.columns:
    print('\nPersistence vs match quality:')
    print(df_mine.groupby('matched_strong')['n_nearby_plumes'].describe().round(1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

metrics = {}
if len(matched) >= 3:
    metrics = compute_metrics(matched, outlier_clip_pct=CFG['outlier_clip_pct'])
else:
    print(f'Only {len(matched)} matched pairs — need ≥ 3 for meaningful metrics')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(matched) >= 3:
    plot_comparison(matched, metrics)
else:
    print('Skipping plots — insufficient matched pairs')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Write Matched Catalog to Delta

# CELL ********************

MATCHED_SCHEMA = StructType([
    StructField('my_plume_key',             StringType(),  True),
    StructField('cm_id',                    StringType(),  True),
    StructField('my_lat',                   DoubleType(),  True),
    StructField('my_lon',                   DoubleType(),  True),
    StructField('cm_lat',                   DoubleType(),  True),
    StructField('cm_lon',                   DoubleType(),  True),
    StructField('distance_km',              DoubleType(),  True),
    StructField('my_date',                  StringType(),  True),
    StructField('cm_date',                  StringType(),  True),
    StructField('time_delta_h',             DoubleType(),  True),
    StructField('my_emission_kg_hr',        DoubleType(),  True),
    StructField('cm_emission_kg_hr',        DoubleType(),  True),
    StructField('cm_emission_unc_kg_hr',    DoubleType(),  True),
    StructField('emission_ratio_my_cm',     DoubleType(),  True),
    StructField('emission_diff_kg_hr',      DoubleType(),  True),
    StructField('my_area_km2',              DoubleType(),  True),
    StructField('my_max_delta_ppb',         DoubleType(),  True),
    StructField('my_ime_kg',                DoubleType(),  True),
    StructField('my_wind_speed_ms',         DoubleType(),  True),
    StructField('my_wind_aligned',          StringType(),  True),  # stored as string to avoid BooleanType issues
    StructField('cm_instrument',            StringType(),  True),
])

if len(matched) > 0:
    # Normalise before createDataFrame — explicit schema, no inference
    matched_write = matched.copy()
    matched_write['my_wind_aligned'] = matched_write['my_wind_aligned'].apply(
        lambda x: str(x) if x is not None else None
    )
    for col in ['time_delta_h', 'emission_ratio_my_cm', 'cm_emission_unc_kg_hr',
                'my_area_km2', 'my_max_delta_ppb', 'my_ime_kg', 'my_wind_speed_ms']:
        if col in matched_write.columns:
            matched_write[col] = pd.to_numeric(matched_write[col], errors='coerce').astype(np.float64)

    matched_spark = spark.createDataFrame(matched_write, schema=MATCHED_SCHEMA)
    (
        matched_spark.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(CFG['matched_table'])
    )
    n = spark.read.table(CFG['matched_table']).count()
    print(f'Matched catalog written: {CFG["matched_table"]} ({n} rows)')
else:
    print('No matched pairs to write')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Diagnostics — Unmatched Plumes

# CELL ********************

# Identify my plumes with no CM match — useful for false positive analysis
if len(matched) > 0 and len(df_mine) > 0:
    matched_keys = set(matched['my_plume_key'])
    unmatched_mine = df_mine[~df_mine['my_plume_key'].isin(matched_keys)].copy()
    unmatched_cm = df_cm_overlap[
        df_cm_overlap['cm_emission_kg_hr'].notna() &
        ~df_cm_overlap['cm_id'].isin(set(matched['cm_id']))
    ].copy()

    print(f'My plumes not matched:   {len(unmatched_mine)} / {len(df_mine)}')
    print(f'CM plumes not matched:   {len(unmatched_cm)} / {df_cm_overlap["cm_emission_kg_hr"].notna().sum()}')

    if len(unmatched_mine) > 0:
        print('\nUnmatched my-plume emission distribution:')
        print(unmatched_mine['emission_kg_hr'].describe().round(1))

    if len(unmatched_cm) > 0:
        print('\nUnmatched CM emission distribution:')
        print(unmatched_cm['cm_emission_kg_hr'].describe().round(1))

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
