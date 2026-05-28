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
# Purpose: Single source of truth for configuration, seeded RNGs, and shared
# helpers (geo math, time math, schema definitions). Imported by every other
# notebook via %run ./config_and_utils
# =============================================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Imports and config loading

import math
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.getOrCreate()


def load_config(path: str = "/lakehouse/default/Files/config/sim_config.yaml") -> dict:
    """
    Load the master config YAML. Falls back to a default path inside the
    Operations_LH Files area. In Fabric, upload sim_config.yaml to
    Operations_LH/Files/config/.
    """
    with open(path, "r") as f:
        return yaml.safe_load(f)


CONFIG = load_config()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Seeded RNG factory


def make_rng(component_offset_key: str) -> np.random.Generator:
    """
    Build a NumPy Generator from the master seed and a component-specific
    offset. Every component (operators, facilities, scada, etc.) gets an
    independent stream so regenerating one layer does not perturb the others.
    """
    seeds = CONFIG["seeds"]
    master = int(seeds["master"])
    offset = int(seeds[component_offset_key])
    return np.random.default_rng(master + offset * 1_000_003)


def deterministic_id(prefix: str, *parts) -> str:
    """
    Hash-derived stable identifier. Used for attribution IDs and any case
    where re-running must produce the same key.
    """
    raw = "|".join(str(p) for p in parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Geo helpers — straight-line geometry is sufficient for v1


EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1, lon1, lat2, lon2):
    """
    Great-circle distance in km. Accepts scalars or NumPy arrays.
    """
    lat1r = np.radians(lat1)
    lat2r = np.radians(lat2)
    dlat = np.radians(np.asarray(lat2) - np.asarray(lat1))
    dlon = np.radians(np.asarray(lon2) - np.asarray(lon1))
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1r) * np.cos(lat2r) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    return EARTH_RADIUS_KM * c


def bearing_deg(lat1, lon1, lat2, lon2):
    """
    Initial bearing from point 1 to point 2, degrees clockwise from north.
    Used for wind-cone attribution.
    """
    lat1r = np.radians(lat1)
    lat2r = np.radians(lat2)
    dlon = np.radians(np.asarray(lon2) - np.asarray(lon1))
    x = np.sin(dlon) * np.cos(lat2r)
    y = np.cos(lat1r) * np.sin(lat2r) - np.sin(lat1r) * np.cos(lat2r) * np.cos(dlon)
    brng = np.degrees(np.arctan2(x, y))
    return (brng + 360.0) % 360.0


def angular_diff_deg(a, b):
    """
    Smallest angular difference in degrees, in [0, 180].
    """
    d = np.abs(np.asarray(a) - np.asarray(b)) % 360.0
    return np.minimum(d, 360.0 - d)


def sample_clustered_points(n: int, centers, std_deg: float, weights, rng):
    """
    Sample n (lat, lon) points from a 2D Gaussian mixture around the given
    centers, with isotropic std in degrees and per-component weights.
    Returns two arrays: lats, lons.
    """
    centers = np.asarray(centers, dtype=float)
    weights = np.asarray(weights, dtype=float)
    weights = weights / weights.sum()
    comp = rng.choice(len(centers), size=n, p=weights)
    lats = centers[comp, 0] + rng.normal(0.0, std_deg, size=n)
    lons = centers[comp, 1] + rng.normal(0.0, std_deg, size=n)
    return lats, lons


def clip_to_envelope(lats, lons):
    """
    Clamp coordinates to the configured Permian envelope so spatial outliers
    don't sneak into the dataset.
    """
    s = CONFIG["spatial"]
    return (
        np.clip(lats, s["lat_min"], s["lat_max"]),
        np.clip(lons, s["lon_min"], s["lon_max"]),
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Time helpers


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def floor_to_minute(ts: datetime, minutes: int) -> datetime:
    """
    Floor a UTC timestamp to the nearest multiple of `minutes`.
    """
    discard = timedelta(
        minutes=ts.minute % minutes,
        seconds=ts.second,
        microseconds=ts.microsecond,
    )
    return ts - discard


def scada_window_bounds():
    """
    Returns (start_utc, end_utc) for the rolling SCADA window. end is the
    current UTC time floored to the SCADA frequency; start is end minus the
    configured window length.
    """
    freq = int(CONFIG["temporal"]["scada_freq_minutes"])
    window_days = int(CONFIG["temporal"]["scada_window_days"])
    end = floor_to_minute(now_utc(), freq)
    start = end - timedelta(days=window_days)
    return start, end


def scada_tick_grid(start: datetime, end: datetime, freq_minutes: int) -> np.ndarray:
    """
    Returns a NumPy datetime64[s] array of evenly spaced 5-min ticks in
    [start, end). Excludes end to avoid double-counting on append boundaries.
    """
    total_minutes = int((end - start).total_seconds() // 60)
    n = total_minutes // freq_minutes
    base = np.datetime64(start.replace(tzinfo=None), "s")
    step = np.timedelta64(freq_minutes * 60, "s")
    return base + step * np.arange(n, dtype=np.int64)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Shared Spark schemas — keeping them centralized prevents drift across notebooks


SCHEMA_OPERATOR = T.StructType([
    T.StructField("operator_id",       T.StringType(),  False),
    T.StructField("operator_name",     T.StringType(),  False),
    T.StructField("operator_size_tier",T.StringType(),  False),
    T.StructField("market_share_pct",  T.DoubleType(),  False),
    T.StructField("hq_state",          T.StringType(),  False),
    T.StructField("founded_year",      T.IntegerType(), False),
    T.StructField("is_synthetic",      T.BooleanType(), False),
])

SCHEMA_PAD = T.StructType([
    T.StructField("pad_id",              T.StringType(),  False),
    T.StructField("operator_id",         T.StringType(),  False),
    T.StructField("pad_name",            T.StringType(),  False),
    T.StructField("sub_basin",           T.StringType(),  False),
    T.StructField("latitude",            T.DoubleType(),  False),
    T.StructField("longitude",           T.DoubleType(),  False),
    T.StructField("n_wells",             T.IntegerType(), False),
    T.StructField("install_year",        T.IntegerType(), False),
    T.StructField("pad_type",            T.StringType(),  False),
    T.StructField("emission_propensity", T.DoubleType(),  False),
    T.StructField("is_synthetic",        T.BooleanType(), False),
])

SCHEMA_WELL = T.StructType([
    T.StructField("well_id",      T.StringType(), False),
    T.StructField("pad_id",       T.StringType(), False),
    T.StructField("api_number",   T.StringType(), False),
    T.StructField("spud_date",    T.DateType(),   False),
    T.StructField("status",       T.StringType(), False),
    T.StructField("product_mix",  T.StringType(), False),
])

SCHEMA_COMPRESSOR_STATION = T.StructType([
    T.StructField("station_id",          T.StringType(),  False),
    T.StructField("operator_id",         T.StringType(),  False),
    T.StructField("station_name",        T.StringType(),  False),
    T.StructField("latitude",            T.DoubleType(),  False),
    T.StructField("longitude",           T.DoubleType(),  False),
    T.StructField("n_compressors",       T.IntegerType(), False),
    T.StructField("total_hp",            T.IntegerType(), False),
    T.StructField("commissioned_year",   T.IntegerType(), False),
    T.StructField("emission_propensity", T.DoubleType(),  False),
])

SCHEMA_PROCESSING_PLANT = T.StructType([
    T.StructField("plant_id",                T.StringType(),  False),
    T.StructField("operator_id",             T.StringType(),  False),
    T.StructField("plant_name",              T.StringType(),  False),
    T.StructField("latitude",                T.DoubleType(),  False),
    T.StructField("longitude",               T.DoubleType(),  False),
    T.StructField("capacity_mmcf_per_day",   T.DoubleType(),  False),
    T.StructField("commissioned_year",       T.IntegerType(), False),
])

SCHEMA_PIPELINE_SEGMENT = T.StructType([
    T.StructField("segment_id",          T.StringType(),  False),
    T.StructField("from_facility_id",    T.StringType(),  False),
    T.StructField("from_facility_type",  T.StringType(),  False),
    T.StructField("to_facility_id",      T.StringType(),  False),
    T.StructField("to_facility_type",    T.StringType(),  False),
    T.StructField("length_km",           T.DoubleType(),  False),
    T.StructField("diameter_in",         T.IntegerType(), False),
    T.StructField("commissioned_year",   T.IntegerType(), False),
    T.StructField("midpoint_lat",        T.DoubleType(),  False),
    T.StructField("midpoint_lon",        T.DoubleType(),  False),
])

SCHEMA_SENSOR = T.StructType([
    T.StructField("sensor_id",            T.StringType(),  False),
    T.StructField("parent_facility_id",   T.StringType(),  False),
    T.StructField("parent_facility_type", T.StringType(),  False),
    T.StructField("sensor_type",          T.StringType(),  False),
    T.StructField("unit",                 T.StringType(),  False),
    T.StructField("nominal_value",        T.DoubleType(),  False),
    T.StructField("nominal_std",          T.DoubleType(),  False),
    T.StructField("install_date",         T.DateType(),    False),
])

SCHEMA_SCADA = T.StructType([
    T.StructField("event_time",              T.TimestampType(), False),
    T.StructField("event_date",              T.DateType(),      False),
    T.StructField("ingestion_time",          T.TimestampType(), False),
    T.StructField("sensor_id",               T.StringType(),    False),
    T.StructField("parent_facility_id",      T.StringType(),    False),
    T.StructField("parent_facility_type",    T.StringType(),    False),
    T.StructField("operator_id",             T.StringType(),    False),
    T.StructField("sensor_type",             T.StringType(),    False),
    T.StructField("value",                   T.DoubleType(),    False),
    T.StructField("quality_code",            T.StringType(),    False),
    T.StructField("is_anomalous",            T.BooleanType(),   False),
    T.StructField("anomaly_source_event_id", T.StringType(),    True),
])

SCHEMA_EVENT = T.StructType([
    T.StructField("event_id",             T.StringType(),    False),
    T.StructField("event_type",           T.StringType(),    False),
    T.StructField("facility_id",          T.StringType(),    False),
    T.StructField("facility_type",        T.StringType(),    False),
    T.StructField("operator_id",          T.StringType(),    False),
    T.StructField("start_time",           T.TimestampType(), False),
    T.StructField("end_time",             T.TimestampType(), True),
    T.StructField("severity",             T.StringType(),    False),
    T.StructField("expected_ch4_kg",      T.DoubleType(),    False),
    T.StructField("triggered_by_plume_id",T.LongType(),      True),
    T.StructField("is_synthetic",         T.BooleanType(),   False),
])

SCHEMA_ATTRIBUTION = T.StructType([
    T.StructField("attribution_id",         T.StringType(),  False),
    T.StructField("plume_id",               T.LongType(),    False),
    T.StructField("scene_id",               T.StringType(),  False),
    T.StructField("candidate_facility_id",  T.StringType(),  False),
    T.StructField("candidate_facility_type",T.StringType(),  False),
    T.StructField("candidate_operator_id",  T.StringType(),  False),
    T.StructField("tier",                   T.IntegerType(), False),
    T.StructField("distance_km",            T.DoubleType(),  False),
    T.StructField("within_wind_cone",       T.BooleanType(), False),
    T.StructField("wind_alignment_score",   T.DoubleType(),  False),
    T.StructField("facility_emission_prior",T.DoubleType(),  False),
    T.StructField("confidence_score",       T.DoubleType(),  False),
    T.StructField("confidence_tier",        T.StringType(),  False),
    T.StructField("is_best_candidate",      T.BooleanType(), False),
    T.StructField("coupled_event_id",       T.StringType(),  True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Table fully-qualified names — single source

def fqn(layer: str, name: str) -> str:
    """
    Build the fully-qualified Spark table name for layer in {bronze, silver, gold}.
    """
    if layer == "bronze":
        return f"{CONFIG['paths']['bronze_prefix']}{name}"
    if layer == "silver":
        return f"{CONFIG['paths']['silver_prefix']}{name}"
    if layer == "gold":
        return f"{CONFIG['paths']['gold_prefix']}{name}"
    raise ValueError(f"Unknown layer: {layer}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
