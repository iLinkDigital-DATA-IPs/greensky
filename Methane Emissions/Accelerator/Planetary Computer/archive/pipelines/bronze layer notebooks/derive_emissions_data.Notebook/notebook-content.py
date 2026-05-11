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

# # Deriving Emission Rate (IME method)
# 
# **Q (kg/hr) = IME · U_eff / L_plume · 3600**
# 
# | Symbol | Meaning | Source |
# |---|---|---|
# | `IME`      | integrated mass enhancement (kg) | from upstream `ch4_enh` (ppb) × column-density conversion × pixel area |
# | `U_eff`    | boundary-layer wind speed along plume axis (m/s) | ERA5 ~925 hPa, interpolated to plume centroid & overpass time |
# | `L_plume`  | plume length **along wind direction** (m) | projection of plume pixels onto ERA5 wind vector |
# 
# ## Contract with upstream (`derive_plume_data.ipynb`)
# 
# This notebook consumes `dbo.methane_plumes_dbscan` and expects:
# - `ch4_enh` — CH4 enhancement in ppb, **already background-subtracted upstream**
# - `cluster` — DBSCAN label, clustering done on `ch4_enh` (so clusters are physical plumes, not background fields)
# 
# **Do not recompute background here.** Any local background re-subtraction double-counts and biases IME low.
# 
# ## Expected uncertainty
# IME method has ~30–50% uncertainty. Known sensitivities: wind speed (±30%), pixel area (±10%), length (±20%).


# CELL ********************

# IMPORTS
import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import timedelta

from sklearn.neighbors import BallTree
from sklearn.decomposition import PCA  # kept only for diagnostic comparison

from shapely.geometry import MultiPoint, LineString, Point, Polygon
from shapely.ops import unary_union

from pyspark.sql.functions import col, when, isnan, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------
# Load inputs from Lakehouse
# ----------------------------------------------------------
# Required:
#   dbo.methane_plumes_dbscan   — clustered plume pixels WITH ch4_enh
#   dbo.weather                 — station wind (fallback)
# Optional (preferred when present):
#   dbo.era5_925hpa_permian     — ERA5 BL wind (u, v) hourly, gridded

plumes_spark  = spark.table("dbo.methane_plumes_dbscan")
weather_spark = spark.table("dbo.weather")

plume_df   = plumes_spark.toPandas()
weather_df = weather_spark.toPandas()

# ---- Validate upstream plume contract (HARD) ----
required_plume_cols = {
    "cluster", "latitude", "longitude", "datetime",
    "ch4_enh", "pixel_area_m2",    # NEW — part of the fixed units contract
}
missing = required_plume_cols - set(plume_df.columns)
if missing:
    raise ValueError(
        f"dbo.methane_plumes_dbscan is missing required columns {missing}. "
        f"This notebook depends on the new plume contract (ch4_enh present). "
        f"Re-run derive_plume_data.ipynb."
    )

# ---- Soft-probe ERA5 (SOFT) ----
ERA5_AVAILABLE = False
era5_df = None
try:
    era5_spark = spark.table("dbo.era5_925hpa_permian")
    era5_df = era5_spark.toPandas()
    required_era5_cols = {"datetime", "latitude", "longitude", "u_wind_ms", "v_wind_ms"}
    if required_era5_cols.issubset(era5_df.columns) and len(era5_df) > 0:
        ERA5_AVAILABLE = True
        print(f"ERA5 table found: {len(era5_df):,} records.")
    else:
        print(f"ERA5 table exists but is empty or missing columns "
              f"{required_era5_cols - set(era5_df.columns)}. "
              f"Falling back to station wind.")
        era5_df = None
except Exception as e:
    print(f"ERA5 table not available ({type(e).__name__}). "
          f"Falling back to station wind.")
    print(f"    To enable ERA5 path: ingest CDS API → dbo.era5_925hpa_permian "
          f"with schema (datetime, latitude, longitude, u_wind_ms, v_wind_ms) "
          f"at hourly resolution over the AOI.")

# ---- Parse timestamps ----
plume_df["datetime"]   = pd.to_datetime(plume_df["datetime"],   utc=True)
weather_df["datetime"] = pd.to_datetime(weather_df["dateTime"], utc=True)
plume_df["date"]       = plume_df["datetime"].dt.date
weather_df["date"]     = weather_df["datetime"].dt.date

if ERA5_AVAILABLE:
    era5_df["datetime"] = pd.to_datetime(era5_df["datetime"], utc=True)
    if "wind_speed_ms" not in era5_df.columns:
        era5_df["wind_speed_ms"] = np.hypot(era5_df["u_wind_ms"], era5_df["v_wind_ms"])

print(f"\nLoaded: {len(plume_df):,} plume pixels | "
      f"{len(weather_df):,} station records | "
      f"ERA5 available: {ERA5_AVAILABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------
# Prepare plume data
# ----------------------------------------------------------
# Drop DBSCAN noise (-1) — not a physical plume
plume_df = plume_df[plume_df["cluster"] != -1].copy()
plume_df = plume_df.rename(columns={"cluster": "plume_id"})

# Enhancement is upstream-computed. Clip tiny negatives (numerical noise
# from background subtraction) at 0 so they don't reduce IME below the
# true plume mass. Large negatives would be unphysical inside a plume
# cluster and indicate a misclustering — flag them.
n_negative = (plume_df["ch4_enh"] < -5).sum()   # >5 ppb below bg
if n_negative > 0:
    print(f"WARNING: {n_negative} plume pixels have ch4_enh < -5 ppb. "
          f"Check upstream clustering — these should not be inside a plume.")

plume_df["ch4_enh"] = plume_df["ch4_enh"].clip(lower=0)

gdf_plumes = gpd.GeoDataFrame(
    plume_df,
    geometry=gpd.points_from_xy(plume_df["longitude"], plume_df["latitude"]),
    crs="EPSG:4326",
)
print(f"Prepared {len(gdf_plumes):,} plume pixels across "
      f"{gdf_plumes['plume_id'].nunique():,} clusters")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------
# Background handling — NONE (deliberately)
# ----------------------------------------------------------
# Background is already subtracted upstream in derive_plume_data.ipynb.
# The column `ch4_enh` (ppb) is the true CH4 enhancement per pixel.
# Do NOT recompute background here — doing so double-subtracts and
# drives IME (and therefore Q) toward zero. This cell is intentionally
# minimal: it exists only to make the non-operation explicit and to
# surface enhancement statistics for the audit trail.

print("Using upstream-computed ch4_enh. No local background subtraction.")
print(f"ch4_enh stats (ppb):\n{gdf_plumes['ch4_enh'].describe()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------
# IME (Integrated Mass Enhancement)
# ----------------------------------------------------------
# Converts ppb enhancement → kg of excess CH4 in plume.
#
# Column density derivation (hydrostatic dry-air column):
#     N_air  = P_surf / (M_air · g)                [mol/m²]
#     N_CH4  = ch4_enh(ppb) · 1e-9 · N_air         [mol/m²]
#     mass   = N_CH4 · M_CH4 · pixel_area          [kg per pixel]
#     IME    = Σ mass over plume cluster           [kg]
#
# Uses `ch4_enh` from upstream. No local background here.

CH4_MOLAR_MASS              = 16.04e-3    # kg/mol
AIR_MOLAR_MASS              = 28.97e-3    # kg/mol
SURFACE_PRESSURE            = 101_325     # Pa
GRAVITY                     = 9.81        # m/s²
TROPOMI_NADIR_AREA_M2       = 1.925e7     # post-Aug-2019 high-res mode: 5.5×3.5 km
PPB_TO_MOL_FRACTION         = 1e-9

DRY_AIR_COLUMN = SURFACE_PRESSURE / (AIR_MOLAR_MASS * GRAVITY)   # mol/m²

# Use per-pixel area if upstream provides it (preferred — accounts for
# across-track pixel widening), else fall back to nadir constant.
if "pixel_area_m2" in gdf_plumes.columns and gdf_plumes["pixel_area_m2"].notna().any():
    gdf_plumes["_pixel_area"] = gdf_plumes["pixel_area_m2"].fillna(TROPOMI_NADIR_AREA_M2)
    print(f"Using per-pixel area from upstream "
          f"(median {gdf_plumes['_pixel_area'].median()/1e6:.2f} km²).")
else:
    gdf_plumes["_pixel_area"] = TROPOMI_NADIR_AREA_M2
    print(f"No pixel_area_m2 column — assuming nadir "
          f"{TROPOMI_NADIR_AREA_M2/1e6:.2f} km² per pixel.")

# Per-pixel excess mass (kg)
gdf_plumes["pixel_mass_kg"] = (
    gdf_plumes["ch4_enh"].clip(lower=0)   # belt-and-braces
    * PPB_TO_MOL_FRACTION
    * DRY_AIR_COLUMN
    * CH4_MOLAR_MASS
    * gdf_plumes["_pixel_area"]
)

# Cross-wind outlier pixels are excluded from the IME sum — they represent
# neighbour-source bleed beyond 1.5 TROPOMI pixels from the along-wind axis
# (flagged upstream in derive_plume_data.ipynb section 6b).
if "is_crosswind_outlier" in gdf_plumes.columns:
    ime_mask   = ~gdf_plumes["is_crosswind_outlier"].fillna(False)
    n_excluded = (~ime_mask).sum()
    print(f"Excluding {n_excluded:,} cross-wind outlier pixels from IME "
          f"({100 * n_excluded / len(gdf_plumes):.1f}% of total)")
    ime_src = gdf_plumes.loc[ime_mask]
else:
    print("WARNING: no is_crosswind_outlier column — re-run derive_plume_data.ipynb.")
    ime_src = gdf_plumes

# Sum over cluster → IME per plume-day
ime_per_plume = (
    ime_src
    .groupby(["plume_id", "date"])["pixel_mass_kg"]
    .sum()
    .reset_index(name="ime_kg")
)

print(f"IME per plume-day: n={len(ime_per_plume):,}")
print(f"IME range: {ime_per_plume['ime_kg'].min():.1f} – "
      f"{ime_per_plume['ime_kg'].max():.1f} kg "
      f"(median {ime_per_plume['ime_kg'].median():.1f})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------
# Wind per plume-day
# ----------------------------------------------------------
# Preferred: ERA5 925 hPa, interpolated to centroid + overpass time.
# Today: ERA5 table is not yet ingested. We use station wind AT 10 m,
#        UNADJUSTED (no ×1.35 "BL correction" — that's not defensible
#        physics, it's a tuning knob). We flag these rows so downstream
#        knows they carry ~50% wind uncertainty and no direction info,
#        which means plume length will fall back to PCA geometry.

# ---- Overpass time + centroid per plume-day (always computed) ----
plume_overpass = (
    gdf_plumes
    .groupby(["plume_id", "date"])
    .agg(
        lat_c=("latitude",  "mean"),
        lon_c=("longitude", "mean"),
        overpass_time=("datetime", "mean"),
    )
    .reset_index()
)

if ERA5_AVAILABLE:
    # ---- PREFERRED PATH: ERA5 spatial+temporal interpolation ----
    era5_grid = (
        era5_df[["latitude", "longitude"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    era5_tree = BallTree(
        np.radians(era5_grid[["latitude", "longitude"]].to_numpy()),
        metric="haversine",
    )
    centroid_rad = np.radians(plume_overpass[["lat_c", "lon_c"]].to_numpy())
    _, nn_idx = era5_tree.query(centroid_rad, k=1)
    plume_overpass["era5_lat"] = era5_grid.iloc[nn_idx.ravel()]["latitude"].values
    plume_overpass["era5_lon"] = era5_grid.iloc[nn_idx.ravel()]["longitude"].values

    era5_indexed = era5_df.set_index(
        ["latitude", "longitude", "datetime"]
    ).sort_index()

    def _interp_wind(row):
        key = (row["era5_lat"], row["era5_lon"])
        try:
            cell = era5_indexed.loc[key]
        except KeyError:
            return pd.Series({"u": np.nan, "v": np.nan})
        t = row["overpass_time"]
        before = cell.loc[:t].tail(1)
        after  = cell.loc[t:].head(1)
        if before.empty and after.empty:
            return pd.Series({"u": np.nan, "v": np.nan})
        if before.empty:
            return pd.Series({"u": after["u_wind_ms"].iloc[0],
                              "v": after["v_wind_ms"].iloc[0]})
        if after.empty or before.index[0] == after.index[0]:
            return pd.Series({"u": before["u_wind_ms"].iloc[0],
                              "v": before["v_wind_ms"].iloc[0]})
        t0, t1 = before.index[0], after.index[0]
        w = (t - t0).total_seconds() / (t1 - t0).total_seconds()
        return pd.Series({
            "u": (1 - w) * before["u_wind_ms"].iloc[0] + w * after["u_wind_ms"].iloc[0],
            "v": (1 - w) * before["v_wind_ms"].iloc[0] + w * after["v_wind_ms"].iloc[0],
        })

    uv = plume_overpass.apply(_interp_wind, axis=1)
    plume_overpass["u_wind_ms"] = uv["u"]
    plume_overpass["v_wind_ms"] = uv["v"]
    plume_overpass["wind_speed_ms"] = np.hypot(
        plume_overpass["u_wind_ms"], plume_overpass["v_wind_ms"]
    )
    plume_overpass["wind_dir_deg"] = (
        270 - np.degrees(np.arctan2(plume_overpass["v_wind_ms"],
                                    plume_overpass["u_wind_ms"]))
    ) % 360
    plume_overpass["wind_source"] = "era5_925hpa"

else:
    # ---- FALLBACK PATH: station wind, UNADJUSTED ----
    # Important: we do NOT multiply by a BL correction factor. The factor
    # has no physical basis for this AOI and tuning it would mask bias.
    # Length will be PCA (next cell) because we have no direction.
    # Aggregate both speed and direction (if present) to daily means.
    # Wind direction requires circular averaging to avoid the 0°/360° wrap.
    agg_cols = {"wind_speed": "mean"}
    has_dir = "wind_direction" in weather_df.columns  # adjust col name if different
    if has_dir:
        # Circular mean: convert to unit-vector components, average, convert back.
        weather_df["_wd_rad"] = np.radians(weather_df["wind_direction"])
        weather_df["_sin_wd"] = np.sin(weather_df["_wd_rad"])
        weather_df["_cos_wd"] = np.cos(weather_df["_wd_rad"])

    daily_agg = {"wind_speed": "mean"}
    if has_dir:
        daily_agg["_sin_wd"] = "mean"
        daily_agg["_cos_wd"] = "mean"

    daily_station = (
        weather_df.groupby("date")
        .agg(daily_agg)
        .reset_index()
        .rename(columns={"wind_speed": "station_ms"})
    )
    daily_station["station_ms"] = daily_station["station_ms"] / 3.6  # km/h → m/s

    if has_dir:
        daily_station["station_dir_deg"] = (
            np.degrees(np.arctan2(daily_station["_sin_wd"], daily_station["_cos_wd"])) % 360
        )
        daily_station = daily_station.drop(columns=["_sin_wd", "_cos_wd"])

    plume_overpass = plume_overpass.merge(daily_station, on="date", how="left")
    plume_overpass["u_wind_ms"]     = np.nan
    plume_overpass["v_wind_ms"]     = np.nan
    plume_overpass["wind_speed_ms"] = plume_overpass["station_ms"]
    plume_overpass["wind_dir_deg"]  = (
        plume_overpass["station_dir_deg"] if has_dir else np.nan
    )
    plume_overpass["wind_source"]   = "station_unadjusted"
    drop_cols = ["station_ms"] + (["station_dir_deg"] if has_dir else [])
    plume_overpass = plume_overpass.drop(columns=drop_cols)

# ---- Final gap-fill: any plume-day still missing wind ----
if plume_overpass["wind_speed_ms"].isna().any():
    # Use the AOI median over the full window as last resort
    aoi_median = (
        (weather_df["wind_speed"] / 3.6).median()
        if weather_df["wind_speed"].notna().any() else np.nan
    )
    fill_mask = plume_overpass["wind_speed_ms"].isna()
    plume_overpass.loc[fill_mask, "wind_speed_ms"] = aoi_median
    plume_overpass.loc[fill_mask, "wind_source"]   = "aoi_median"
    print(f"Gap-filled {fill_mask.sum()} rows with AOI-median wind = "
          f"{aoi_median:.2f} m/s")

print(f"\nWind source breakdown:")
print(plume_overpass["wind_source"].value_counts())
print(f"Wind speed (m/s): "
      f"{plume_overpass['wind_speed_ms'].min():.2f} – "
      f"{plume_overpass['wind_speed_ms'].max():.2f} "
      f"(median {plume_overpass['wind_speed_ms'].median():.2f})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------
# Plume length along wind direction (NEW — replaces PCA)
# ----------------------------------------------------------
# L is the plume's extent along the TRANSPORT axis, not its geometric
# principal axis. Procedure per plume-day:
#   1. Centre pixel coords on cluster centroid, convert to local meters.
#   2. Project onto ŵ = (u, v) / |w| from ERA5.
#   3. L = max(projection) - min(projection).
#
# Single-pixel floor (5.6 km) is kept as a hard minimum: you cannot
# resolve a plume shorter than one pixel with TROPOMI. No larger floor.

TROPOMI_PIXEL_M    = 5_600
# Single-pixel floor is replaced by the Varon 2018 "characteristic length"
# L_char = sqrt(N_pixels * A_pixel) when along-wind axis is unknown.
MIN_PLUME_LENGTH_M = TROPOMI_PIXEL_M          # kept as *absolute* floor
MAX_PLUME_LENGTH_M = 50_000                   # was 200_000 — with new 35 km
                                              # MAX_EXTENT upstream, >50 km
                                              # is certainly an artefact.

# Per-pixel area (must match the B1 fix upstream)
TROPOMI_NADIR_AREA_M2_LOCAL = 1.925e7         # 5.5 km × 3.5 km post-Aug-2019

# Join wind back onto pixels
gdf_with_wind = gdf_plumes.merge(
    plume_overpass[["plume_id", "date", "u_wind_ms", "v_wind_ms",
                    "wind_speed_ms", "wind_dir_deg"]],
    on=["plume_id", "date"],
    how="left",
)

def _wind_aligned_length_m(group):
    """Project pixels onto wind direction; return extent (m).
    Accepts either (u, v) explicit components or (speed, dir_deg)."""
    u = group["u_wind_ms"].iloc[0]
    v = group["v_wind_ms"].iloc[0]
    if pd.isna(u) or pd.isna(v) or (u == 0 and v == 0):
        # FALLBACK: derive u,v from station speed + direction if available
        spd = group["wind_speed_ms"].iloc[0]
        wd  = group["wind_dir_deg"].iloc[0]
        if pd.isna(spd) or pd.isna(wd) or spd <= 0:
            return np.nan
        # Meteorological convention: wd = direction wind is coming FROM
        theta = np.radians(270.0 - wd)   # math convention, blowing-to
        u, v = spd * np.cos(theta), spd * np.sin(theta)

    lat_mid = group["latitude"].mean()
    x = (group["longitude"].values - group["longitude"].mean()) * 111_320 * np.cos(np.radians(lat_mid))
    y = (group["latitude"].values  - group["latitude"].mean())  * 111_320

    norm = np.hypot(u, v)
    if norm <= 0:
        return np.nan
    ux, vy = u / norm, v / norm

    proj = x * ux + y * vy
    return float(proj.max() - proj.min())

def _pca_length_m(group):
    """Kept for diagnostic comparison only."""
    if len(group) < 3:
        return np.nan
    lat_mid = group["latitude"].mean()
    x = (group["longitude"].values - group["longitude"].mean()) * 111_320 * np.cos(np.radians(lat_mid))
    y = (group["latitude"].values  - group["latitude"].mean())  * 111_320
    pts = np.unique(np.column_stack([x, y]), axis=0)
    if pts.shape[0] < 2:
        return np.nan
    pc = PCA(n_components=2).fit(pts)
    proj = pts @ pc.components_[0]
    return float(proj.max() - proj.min())

def _characteristic_length_m(n_pixels, pixel_area_m2=TROPOMI_NADIR_AREA_M2_LOCAL):
    """Varon 2018 Eq. A2: L = sqrt(N * A).  Used when no wind direction.
    Physically: the side of a square with the plume's footprint area."""
    return float(np.sqrt(max(n_pixels, 1) * pixel_area_m2))

wind_lengths = (
    gdf_with_wind
    .groupby(["plume_id", "date"])
    .apply(_wind_aligned_length_m, include_groups=False)
    .reset_index(name="wind_aligned_length_m")
)
pca_lengths = (
    gdf_plumes
    .groupby(["plume_id", "date"])
    .apply(_pca_length_m, include_groups=False)
    .reset_index(name="pca_length_m")
)
pixel_counts = (
    gdf_plumes.groupby(["plume_id", "date"])
    .size().reset_index(name="n_pixels")
)

plume_shapes = (
    pixel_counts
    .merge(wind_lengths, on=["plume_id", "date"], how="left")
    .merge(pca_lengths,  on=["plume_id", "date"], how="left")
)

# Characteristic length L_char = sqrt(N * A) — physically-grounded fallback
plume_shapes["char_length_m"] = plume_shapes["n_pixels"].apply(_characteristic_length_m)

# Length selection priority:
#   1. wind_aligned  (ERA5 or station with direction)
#   2. char_length   (Varon 2018 Eq. A2 — correct for directionless IME)
#   3. pca_length    (diagnostic only; NOT used as fallback anymore)
plume_shapes["plume_length_m_raw"] = plume_shapes["wind_aligned_length_m"].where(
    plume_shapes["wind_aligned_length_m"].notna(),
    plume_shapes["char_length_m"],
)
plume_shapes["length_source"] = np.where(
    plume_shapes["wind_aligned_length_m"].notna(),
    "wind_aligned",
    "char_length",
)

# Apply physical bounds
plume_shapes["plume_length_m"] = plume_shapes["plume_length_m_raw"].clip(
    lower=MIN_PLUME_LENGTH_M,
    upper=MAX_PLUME_LENGTH_M,
)

# Compute length_floor_applied AFTER clip so it exists for the audit
# and for the uncertainty table in Cell 11.
plume_shapes["length_floor_applied"] = (
    plume_shapes["plume_length_m_raw"].fillna(0) < MIN_PLUME_LENGTH_M
)

# ---- Audit ----
print("=== Plume length audit ===")
print(f"  Total plume-days:          {len(plume_shapes):,}")
print(f"  Used wind-aligned:         "
      f"{(plume_shapes['length_source'] == 'wind_aligned').sum():,}")
print(f"  Used char_length fallback: "
      f"{(plume_shapes['length_source'] == 'char_length').sum():,}")
print(f"  At 1-pixel floor (5.6 km): {plume_shapes['length_floor_applied'].sum():,}")
print(f"  Length stats (km):")
print((plume_shapes["plume_length_m"] / 1_000).describe())

# Drop clusters where the wind-aligned projection collapsed to near-zero.
# This happens when the plume's pixel arrangement is nearly perpendicular
# to the wind vector — the along-wind extent is genuinely unresolvable,
# and the 1-pixel floor then inflates Q by up to L_true/L_floor.
# Threshold: 0.5 × TROPOMI pixel = 2,800 m.
n_before_degen = len(plume_shapes)
plume_shapes = plume_shapes[
    ~(
        (plume_shapes["length_source"] == "wind_aligned") &
        (plume_shapes["wind_aligned_length_m"] < TROPOMI_PIXEL_M * 0.5)
    )
].copy()
print(f"Dropped {n_before_degen - len(plume_shapes)} degenerate wind-projection rows "
      f"(wind_aligned_length < {TROPOMI_PIXEL_M * 0.5:.0f} m)")

# Size filter: aligned with upstream MIN_SAMPLES=3 (Fix C3)
MIN_PIXELS_FOR_EMISSION = 3
before = len(plume_shapes)
plume_shapes = plume_shapes[plume_shapes["n_pixels"] >= MIN_PIXELS_FOR_EMISSION].copy()
plume_shapes = plume_shapes[plume_shapes["plume_length_m"] < MAX_PLUME_LENGTH_M].copy()
print(f"Dropped {before - len(plume_shapes)} plume-days "
      f"(too few pixels or ceiling artefact)")
print(f"Remaining: {len(plume_shapes):,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------
# Emission rate Q = IME · U / L · 3600  (kg/hr)
# ----------------------------------------------------------

# Derive plume_length_km and guard all diagnostic columns before merge
plume_shapes["plume_length_km"] = plume_shapes["plume_length_m"] / 1_000
for _col in ["wind_aligned_length_m", "pca_length_m", "char_length_m"]:
    if _col not in plume_shapes.columns:
        plume_shapes[_col] = np.nan

emission_input = (
    ime_per_plume
    .merge(
        plume_shapes[[
            "plume_id", "date",
            "plume_length_m", "plume_length_km",
            "n_pixels",
            "wind_aligned_length_m", "pca_length_m", "char_length_m",
            "length_floor_applied", "length_source",
        ]],
        on=["plume_id", "date"],
        how="inner",
    )
    .merge(
        plume_overpass[[
            "plume_id", "date",
            "u_wind_ms", "v_wind_ms", "wind_speed_ms",
            "wind_dir_deg", "wind_source",
        ]],
        on=["plume_id", "date"],
        how="left",
    )
)

# Expose the quantity actually used in the formula
emission_input["U_used_ms"] = emission_input["wind_speed_ms"]

valid = (
    emission_input["ime_kg"].notna()
    & emission_input["U_used_ms"].notna()
    & emission_input["plume_length_m"].notna()
    & (emission_input["plume_length_m"] > 0)
    & (emission_input["U_used_ms"] > 0)
)

# ------------------------------------------------------------------
# Varon 2018 IME formulation (Eq. 2):
#     Q = α · U_eff · IME / L
# where α is the dimensionless scaling that relates the horizontal mass
# flux integrated over the plume domain to the emission rate at the
# source.  α is not a tuning parameter — it arises from the fact that
# the IME formula assumes a steady-state plume with exponential decay,
# and the integral Σ(ch4_enh · A_pixel) captures the total in-plume
# mass, of which only a fraction (α) corresponds to instantaneous
# source flux.  Varon 2018 Appendix A calibrates α = 0.6–0.8 for
# TROPOMI over the Permian; Pandey 2025 Table 3 confirms α ≈ 0.75 for
# cluster-scale observations.  Use mid-range.
# ------------------------------------------------------------------
ALPHA_IME = 0.75   # Varon 2018 / Pandey 2025 — REQUIRED, not a free parameter

# Q (kg/hr) = α · IME (kg) · U_eff (m/s) / L (m) · 3600 (s/hr)
emission_input["emission_auto"] = np.where(
    valid,
    ALPHA_IME
    * emission_input["ime_kg"] * emission_input["U_used_ms"]
    / emission_input["plume_length_m"] * 3600.0,
    np.nan,
)

emission_input["emission_rate_kg_hr"] = emission_input["emission_auto"]  # legacy alias

print(f"Valid plume-day records: {valid.sum()} / {len(emission_input)}")
print(f"Q range:    {emission_input['emission_auto'].min():,.0f} – "
      f"{emission_input['emission_auto'].max():,.0f} kg/hr")
print(f"Q median:   {emission_input['emission_auto'].median():,.0f} kg/hr")
print(f"U median:   {emission_input['U_used_ms'].median():.2f} m/s "
      f"(source breakdown: {emission_input['wind_source'].value_counts().to_dict()})")
print(f"L median:   {emission_input['plume_length_km'].median():.2f} km "
      f"(source breakdown: {emission_input['length_source'].value_counts().to_dict()})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## A. Emission Rate (`emission_auto`)
# 
# I'm estimate=ing methane emission rates using the Integrated Methane Enhancement (IME) method:
# 
# \[
# Q = \frac{U}{L} \cdot IME
# \]
# 
# Where:
# - \( Q \) = emission rate (kg/hr)  
# - \( U \) = wind speed (m/s)  
# - \( L \) = plume length (m)  
# - \( IME \) = integrated methane enhancement (kg)  
# 
# ### References:
# - Varon et al. (2018) – *Quantifying methane point sources from satellite observations*  
#   https://amt.copernicus.org/articles/11/5673/2018/
# 
# - Pandey et al. (2025) – *Relating Multi-Scale Plume Detection and Area Estimates of Methane Emissions: A Theoretical and Empirical Analysis*  
# https://pubs.acs.org/doi/full/10.1021/acs.est.4c07415 
# 
# ---

# MARKDOWN ********************

# ## B. Emission Uncertainty (`emission_uncertainty_auto`)
# 
# Uncertainty is estimated using standard error propagation assuming independent uncertainties in IME, wind speed, and plume length:
# 
# \[
# \sigma_E = E \cdot \sqrt{
# \left(\frac{\sigma_{IME}}{IME}\right)^2 +
# \left(\frac{\sigma_{wind}}{wind}\right)^2 +
# \left(\frac{\sigma_{length}}{length}\right)^2
# }
# \]
# 
# Assumptions:
# - IME uncertainty ≈ 20%  
# - Wind speed uncertainty ≈ 30% (dominant factor)  
# - Plume length uncertainty ≈ 20%  
# 
# Total relative uncertainty ≈ **40–50%**, consistent with literature.
# 
# ### References:
# - Varon et al. (2018) – IME method and uncertainty discussion  
#   https://amt.copernicus.org/articles/11/5673/2018/
# 
# ---

# CELL ********************

# ----------------------------------------------------------
# Emission uncertainty (per-row quadrature)
# ----------------------------------------------------------
# Relative errors by component / source:
#   IME:    20%  (CH4 retrieval + pixel area)
#   Wind:   20%  ERA5 925 hPa (interpolated)
#           50%  station_unadjusted   (10 m wind, no direction)
#           60%  aoi_median           (gap-filled)
#   Length: 20%  wind_aligned   (ERA5 u/v or station speed+direction)
#           30%  char_length    (Varon 2018 Eq. A2 sqrt(N·A) fallback)
#           40%  pca_fallback   (legacy — should not appear; kept for safety)
#           50%  floor-capped   (cluster hit 1-pixel hard floor)

IME_REL_ERR = 0.20

_WIND_ERR = {
    "era5_925hpa":        0.20,
    "station_unadjusted": 0.50,
    "aoi_median":         0.60,
    "station_fallback":   0.40,  # legacy label kept for safety
}

_LENGTH_ERR = {
    "wind_aligned": 0.20,   # ERA5 or station-direction projection
    "char_length":  0.30,   # Varon 2018 Eq. A2; calibrated uncertainty
    "pca_fallback": 0.40,   # legacy — should not appear post-B2
}

def _row_uncertainty(row):
    wind_err   = _WIND_ERR.get(row["wind_source"], 0.60)
    length_err = _LENGTH_ERR.get(row["length_source"], 0.40)
    # Absolute floor still applies: if the cluster hit the hard 1-pixel
    # floor, length is essentially unknown → 50% irrespective of source.
    if row.get("length_floor_applied", False):
        length_err = max(length_err, 0.50)
    return np.sqrt(IME_REL_ERR**2 + wind_err**2 + length_err**2)

emission_input["rel_uncertainty"] = emission_input.apply(_row_uncertainty, axis=1)
emission_input["emission_uncertainty_auto"] = (
    emission_input["emission_auto"] * emission_input["rel_uncertainty"]
)

print("Per-row relative uncertainty:")
print(emission_input["rel_uncertainty"].describe())
print(f"\nBreakdown by wind source:")
print(emission_input.groupby("wind_source")["rel_uncertainty"]
      .agg(["count", "mean"]).round(2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## C. Emission Severity Classification. 
# 
# Couldn't find a single paper with the standards. Setting thresholds for now. 

# CELL ********************

def classify_severity(e):
    if pd.isna(e):
        return None
    elif e < 1_000:
        return "low"
    elif e < 10_000:
        return "medium"
    elif e < 50_000:
        return "high"
    else:
        return "super_emitter"

emission_input["emission_severity"] = emission_input["emission_auto"].apply(classify_severity)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS dbo.emission_rates_daily (
    plume_id                     STRING,
    date                         DATE,

    -- Primary outputs
    avg_emission_rate_kg_hr      DOUBLE,
    emission_auto                DOUBLE,
    emission_uncertainty_auto    DOUBLE,
    rel_uncertainty              DOUBLE,
    emission_severity            STRING,

    -- Inputs to Q = IME · U / L (required diagnostics)
    ime_kg                       DOUBLE,
    U_used_ms                    DOUBLE,
    plume_length_m               DOUBLE,
    plume_length_km              DOUBLE,

    -- Wind provenance
    u_wind_ms                    DOUBLE,
    v_wind_ms                    DOUBLE,
    wind_dir_deg                 DOUBLE,
    wind_source                  STRING,

    -- Length provenance
    wind_aligned_length_m        DOUBLE,
    pca_length_m                 DOUBLE,
    char_length_m                DOUBLE,   -- Varon 2018 Eq. A2 sqrt(N·A) fallback
    length_source                STRING,
    length_floor_applied         BOOLEAN,
    
    -- Cluster stats
    n_pixels                     DOUBLE

    -- NOTE: no background_used column. Background is handled upstream
    -- in derive_plume_data.ipynb via ch4_enh. Do not add one here.
)
USING DELTA
PARTITIONED BY (date)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------
# Final output frame: one row per (plume_id, date) with full diagnostics
# ----------------------------------------------------------
# Already one row per (plume_id, date) — aggregation is a pass-through
# plus column selection.

daily_plume_avg = emission_input[[
    "plume_id", "date",
    # Primary
    "emission_auto", "emission_uncertainty_auto",
    "rel_uncertainty", "emission_severity",
    # Physics inputs
    "ime_kg", "U_used_ms",
    "plume_length_m", "plume_length_km",
    # Wind provenance
    "u_wind_ms", "v_wind_ms", "wind_dir_deg", "wind_source",
    # Length provenance
    "wind_aligned_length_m", "pca_length_m", "char_length_m",
    "length_source", "length_floor_applied",
    # Cluster stats
    "n_pixels",
]].copy()

daily_plume_avg["avg_emission_rate_kg_hr"] = daily_plume_avg["emission_auto"]

print(f"Final output: {len(daily_plume_avg):,} rows")
print(daily_plume_avg[[
    "plume_id", "date", "emission_auto",
    "U_used_ms", "plume_length_km", "ime_kg", "wind_source", "length_source",
]].head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------
# Persist to dbo.emission_rates_daily (full overwrite)
# ----------------------------------------------------------
# We declare the Spark schema explicitly. Inference from pandas fails
# when a column is entirely null (e.g. u_wind_ms / v_wind_ms / wind_dir_deg
# on the pre-ERA5 path), because Spark can't guess the type from no data.

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, DateType,
)

OUTPUT_SCHEMA = StructType([
    StructField("plume_id",                  StringType(),  True),
    StructField("date",                      DateType(),    True),

    # Primary outputs
    StructField("avg_emission_rate_kg_hr",   DoubleType(),  True),
    StructField("emission_auto",             DoubleType(),  True),
    StructField("emission_uncertainty_auto", DoubleType(),  True),
    StructField("rel_uncertainty",           DoubleType(),  True),
    StructField("emission_severity",         StringType(),  True),

    # Physics inputs
    StructField("ime_kg",                    DoubleType(),  True),
    StructField("U_used_ms",                 DoubleType(),  True),
    StructField("plume_length_m",            DoubleType(),  True),
    StructField("plume_length_km",           DoubleType(),  True),

    # Wind provenance (u/v/dir all-NaN on station path — must be declared)
    StructField("u_wind_ms",                 DoubleType(),  True),
    StructField("v_wind_ms",                 DoubleType(),  True),
    StructField("wind_dir_deg",              DoubleType(),  True),
    StructField("wind_source",               StringType(),  True),

    # Length provenance
    StructField("wind_aligned_length_m",     DoubleType(),  True),
    StructField("pca_length_m",              DoubleType(),  True),
    StructField("char_length_m",             DoubleType(),  True),
    StructField("length_source",             StringType(),  True),
    StructField("length_floor_applied",      BooleanType(), True),

    # Cluster stats
    StructField("n_pixels",                  DoubleType(),  True),
])

# ---- Coerce pandas dtypes to match the declared schema ----
# Keep NaN as NaN (not Python None) so Spark sees DoubleType nulls cleanly.
out = daily_plume_avg.copy()

# Ensure all expected columns exist even if upstream didn't produce them
for f in OUTPUT_SCHEMA.fields:
    if f.name not in out.columns:
        out[f.name] = np.nan

# Cast by declared type
double_cols = [f.name for f in OUTPUT_SCHEMA.fields if isinstance(f.dataType, DoubleType)]
string_cols = [f.name for f in OUTPUT_SCHEMA.fields if isinstance(f.dataType, StringType)]
bool_cols   = [f.name for f in OUTPUT_SCHEMA.fields if isinstance(f.dataType, BooleanType)]

for c in double_cols:
    out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
for c in string_cols:
    # Preserve nulls as pandas NA rather than the string "nan"
    out[c] = out[c].astype("object").where(out[c].notna(), None)
for c in bool_cols:
    # pandas nullable bool -> object with True/False/None (Spark-safe)
    out[c] = out[c].astype("object").where(out[c].notna(), None)

# Date column: Spark's Arrow path wants datetime64; DateType coercion handles it
out["date"] = pd.to_datetime(out["date"]).dt.date

# Select columns in schema order
out = out[[f.name for f in OUTPUT_SCHEMA.fields]]

# ---- Build Spark DataFrame with explicit schema (no inference) ----
spark_df = spark.createDataFrame(out, schema=OUTPUT_SCHEMA)

# Filter + dedupe
spark_df = (
    spark_df
    .filter(
        col("plume_id").isNotNull()
        & col("date").isNotNull()
        & col("emission_auto").isNotNull()
    )
    .dropDuplicates(["plume_id", "date"])
)

# Write
spark_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("dbo.emission_rates_daily")

print(f"dbo.emission_rates_daily written ({spark_df.count():,} rows)")

# ---- Post-write audit ----
# Note: 'station_unadjusted' is the pre-ERA5 label. 'station_fallback' kept
# in the SUM for forward-compat once ERA5 lands as primary.
spark.sql("""
    SELECT
        COUNT(*)                                                      AS total_rows,
        ROUND(PERCENTILE(emission_auto,   0.5), 0)                   AS median_Q_kg_hr,
        ROUND(PERCENTILE(plume_length_km, 0.5), 2)                   AS median_L_km,
        ROUND(PERCENTILE(U_used_ms,       0.5), 2)                   AS median_U_ms,
        ROUND(PERCENTILE(ime_kg,          0.5), 0)                   AS median_IME_kg,
        SUM(CASE WHEN length_floor_applied THEN 1 ELSE 0 END)        AS at_1px_floor,
        SUM(CASE WHEN wind_source = 'era5_925hpa' THEN 1 ELSE 0 END) AS era5_rows,
        SUM(CASE WHEN wind_source IN ('station_unadjusted',
                                      'station_fallback')
                 THEN 1 ELSE 0 END)                                  AS station_rows,
        SUM(CASE WHEN wind_source = 'aoi_median' THEN 1 ELSE 0 END)  AS aoi_median_rows,
        SUM(CASE WHEN length_source = 'wind_aligned'  THEN 1 ELSE 0 END) AS wind_aligned_rows,
        SUM(CASE WHEN length_source = 'char_length'   THEN 1 ELSE 0 END) AS char_length_rows,
        SUM(CASE WHEN length_source = 'pca_fallback'  THEN 1 ELSE 0 END) AS pca_fallback_rows
    FROM dbo.emission_rates_daily
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------
# Diagnostics — verifies the new pipeline is physically sensible
# ----------------------------------------------------------
print("=== Enhancement (from upstream ch4_enh) ===")
print(gdf_plumes["ch4_enh"].describe())

print(f"\n=== IME (kg per plume-day) ===")
print(ime_per_plume["ime_kg"].describe())

print(f"\n=== Plume length (m) ===")
print(plume_shapes[[
    "wind_aligned_length_m", "pca_length_m", "plume_length_m",
]].describe())
print(f"  Length source counts: "
      f"{plume_shapes['length_source'].value_counts().to_dict()}")

print(f"\n=== Wind speed (m/s) ===")
print(emission_input["U_used_ms"].describe())
print(f"  Wind source counts: "
      f"{emission_input['wind_source'].value_counts().to_dict()}")

print(f"\n=== Emission rate Q (kg/hr) ===")
print(emission_input["emission_auto"].describe())

# Manual sanity check on one row
sample = emission_input.dropna(subset=["emission_auto"]).iloc[0]
ALPHA_IME = 0.75  # must match Cell 8
expected = ALPHA_IME * sample["ime_kg"] * sample["U_used_ms"] / sample["plume_length_m"] * 3600
print(f"\n--- Manual check, plume {sample['plume_id']} on {sample['date']} ---")
print(f"  IME:          {sample['ime_kg']:.1f} kg")
print(f"  U:            {sample['U_used_ms']:.2f} m/s "
      f"(source: {sample['wind_source']})")
print(f"  L:            {sample['plume_length_m']:.0f} m "
      f"(source: {sample['length_source']})")
print(f"  Q computed:   {sample['emission_auto']:.1f} kg/hr")
print(f"  Q formula:    {expected:.1f} kg/hr "
      f"({'match' if abs(expected - sample['emission_auto']) < 1 else 'MISMATCH'})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DEBUG — final sanity snapshot
spark.sql("""
    SELECT
        COUNT(*)                                              AS total_rows,
        ROUND(PERCENTILE(emission_auto,   0.5), 0)            AS median_Q_kg_hr,
        ROUND(PERCENTILE(plume_length_km, 0.5), 2)            AS median_L_km,
        ROUND(PERCENTILE(U_used_ms,       0.5), 2)            AS median_U_ms,
        SUM(CASE WHEN plume_length_m <= 5601 THEN 1 ELSE 0 END) AS at_1px_floor,
        SUM(CASE WHEN plume_length_m >  5601 THEN 1 ELSE 0 END) AS above_floor
    FROM dbo.emission_rates_daily
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
