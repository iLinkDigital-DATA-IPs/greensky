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

# ### Load configs:

# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run mode and window
#
# `run_mode` via `getArgument`, with optional `start_date` / `end_date` overrides, as 02a-02e
# and 03a-03e. The window is half-open, `[start_date, end_date)`: the pipeline's
# `pipeline_date` P is the day processed, so incremental is `[P, P+1)`. Backfill starts at
# `DETECTION_HISTORY_START` (00_config). Without arguments (an interactive run) P is today, UTC.
#
# STAC and Open-Meteo take inclusive date ranges, so they get `START_DATE` and `LAST_DATE`, the
# last day inside the window.

# CELL ********************

import pandas as pd

RUN_MODE = "backfill"
try:
    RUN_MODE = getArgument("run_mode", "backfill")
except Exception:
    pass
RUN_MODE = (str(RUN_MODE) or "backfill").lower()
assert RUN_MODE in ("backfill", "incremental"), (
    f"run_mode must be 'backfill' or 'incremental', got {RUN_MODE!r}"
)

try:
    _start_override = getArgument("start_date", "")
    _end_override = getArgument("end_date", "")
except Exception:
    _start_override, _end_override = "", ""

_DAY = pd.Timedelta(days=1)
_TODAY = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
HISTORY_START = pd.Timestamp(DETECTION_HISTORY_START)

if RUN_MODE == "backfill":
    WINDOW_START, WINDOW_END = HISTORY_START, _TODAY + _DAY
else:
    WINDOW_START, WINDOW_END = _TODAY, _TODAY + _DAY

if _start_override:
    WINDOW_START = pd.Timestamp(_start_override)
if _end_override:
    WINDOW_END = pd.Timestamp(_end_override)

WINDOW_START = pd.Timestamp(WINDOW_START).normalize()
WINDOW_END = pd.Timestamp(WINDOW_END).normalize()
assert WINDOW_START < WINDOW_END, f"empty window: {WINDOW_START} .. {WINDOW_END}"
assert WINDOW_START >= HISTORY_START, (
    f"window starts {WINDOW_START.date()}, before DETECTION_HISTORY_START {HISTORY_START.date()}"
)

START_DATE = WINDOW_START.strftime("%Y-%m-%d")
LAST_DATE = (WINDOW_END - _DAY).strftime("%Y-%m-%d")
print(f"RUN_MODE={RUN_MODE}  window=[{START_DATE}, {WINDOW_END.date()})  last day {LAST_DATE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Inspect the file:

# CELL ********************

import os
import re
import xarray as xr

# The ERA5 extract is resolved from the window, never named. Files are
# era5_permian_<YYYYMM first>_<YYYYMM last>.nc, covering whole months. With no file covering
# [WINDOW_START, WINDOW_END) this fails: without ERA5, 04 falls back to the fixed mixing time,
# and 03's null-rate assertion would be the only thing left to catch it.
ERA5_DIR = "/lakehouse/default/Files/reference"
_ERA5_NAME = re.compile(r"^era5_permian_(\d{6})_(\d{6})\.nc$")

def _coverage(name):
    m = _ERA5_NAME.match(name)
    if not m:
        return None
    first = pd.Timestamp(f"{m.group(1)}01")
    last = pd.Timestamp(f"{m.group(2)}01")
    return first, last + pd.offsets.MonthBegin(1)    # [first of first month, first after last)

_files = {n: _coverage(n) for n in sorted(os.listdir(ERA5_DIR))}
_files = {n: c for n, c in _files.items() if c is not None}
_covering = [n for n, (lo, hi) in _files.items() if lo <= WINDOW_START and WINDOW_END <= hi]
if not _covering:
    raise FileNotFoundError(
        f"No ERA5 extract in {ERA5_DIR} covers the window [{START_DATE}, {WINDOW_END.date()}). "
        "Available: " + (", ".join(f"{n} [{lo.date()}, {hi.date()})" for n, (lo, hi) in _files.items())
                         or "none")
        + ". Download one that covers the window; do not run without ERA5."
    )
# The narrowest covering extract, then by name, so the choice is deterministic.
ERA5_FILE = min(_covering, key=lambda n: (_files[n][1] - _files[n][0], n))
print(f"ERA5 extract: {ERA5_FILE} for window [{START_DATE}, {WINDOW_END.date()})")

ds = xr.open_dataset(f"{ERA5_DIR}/{ERA5_FILE}")

# The name only claims coverage; check the time axis itself. Hourly data must reach the last
# hour of the window.
_tname = next(c for c in ("valid_time", "time") if c in ds.coords)
_t = pd.to_datetime(ds[_tname].values)
assert _t.min() <= WINDOW_START and _t.max() >= WINDOW_END - pd.Timedelta(hours=1), (
    f"{ERA5_FILE} {_tname} runs {_t.min()} .. {_t.max()}, which does not cover the window "
    f"[{START_DATE}, {WINDOW_END.date()}). The file is truncated or misnamed."
)
print("=== Dataset Overview ===")
print(ds)
print()
print("=== Variables ===")
for var in ds.data_vars:
    print(f"  {var}: shape={ds[var].shape}, dtype={ds[var].dtype}")
    attrs = ds[var].attrs
    if "long_name" in attrs:
        print(f"    long_name: {attrs['long_name']}")
    if "units" in attrs:
        print(f"    units: {attrs['units']}")
print()
print("=== Coordinates ===")
for coord in ds.coords:
    vals = ds[coord].values
    if vals.ndim == 0:
        print(f"  {coord}: scalar value = {vals}")
    elif vals.dtype.kind in ('U', 'S', 'O'):
        print(f"  {coord}: {len(vals)} values (string/object), first={vals[0]}")
    else:
        print(f"  {coord}: {len(vals)} values, range [{vals.min()} .. {vals.max()}]")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Convert to Dataframe:

# CELL ********************

import pandas as pd

era5_pdf = ds.to_dataframe().reset_index()

print(f"Raw DataFrame: {len(era5_pdf):,} rows")
print(f"Columns: {list(era5_pdf.columns)}")
print(era5_pdf.head(3))
print()
print(era5_pdf.dtypes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Rename columns to standard names:

# CELL ********************

# Build rename map based on what we see in the data
rename_map = {}
for col_name in era5_pdf.columns:
    cl = col_name.lower()
    if cl in ["u10", "u10m"] or "u_component" in cl or "u10" in cl:
        rename_map[col_name] = "u10"
    elif cl in ["v10", "v10m"] or "v_component" in cl or "v10" in cl:
        rename_map[col_name] = "v10"
    elif cl in ["blh"] or "boundary" in cl:
        rename_map[col_name] = "boundary_layer_height"
    elif cl in ["sp"] or (cl == "surface_pressure"):
        rename_map[col_name] = "surface_pressure_era5"

print(f"Rename map: {rename_map}")
era5_pdf = era5_pdf.rename(columns=rename_map)

# Rename coordinate columns
coord_renames = {}
for col_name in era5_pdf.columns:
    cl = col_name.lower()
    if cl in ["latitude", "lat"]:
        coord_renames[col_name] = "era5_lat"
    elif cl in ["longitude", "lon"]:
        coord_renames[col_name] = "era5_lon"
    elif cl in ["time", "valid_time", "datetime", "date"]:
        coord_renames[col_name] = "era5_time"

print(f"Coordinate renames: {coord_renames}")
era5_pdf = era5_pdf.rename(columns=coord_renames)

# Drop rows with NaN
before = len(era5_pdf)
era5_pdf = era5_pdf.dropna(subset=["u10", "v10", "boundary_layer_height"])
after = len(era5_pdf)
print(f"Dropped {before - after} NaN rows")

print(f"\nFinal columns: {list(era5_pdf.columns)}")
print(f"Final shape: {era5_pdf.shape}")
print(era5_pdf.head(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validate the data makes physical sense:

# CELL ********************

print("=== ERA5 Data Validation ===")
print(f"Lat range: {era5_pdf['era5_lat'].min():.2f} to {era5_pdf['era5_lat'].max():.2f}")
print(f"Lon range: {era5_pdf['era5_lon'].min():.2f} to {era5_pdf['era5_lon'].max():.2f}")
print(f"Time range: {era5_pdf['era5_time'].min()} to {era5_pdf['era5_time'].max()}")
print(f"Grid points: {era5_pdf[['era5_lat', 'era5_lon']].drop_duplicates().shape[0]}")
print()
print(f"u10 range: {era5_pdf['u10'].min():.2f} to {era5_pdf['u10'].max():.2f} m/s")
print(f"v10 range: {era5_pdf['v10'].min():.2f} to {era5_pdf['v10'].max():.2f} m/s")
print(f"BLH range: {era5_pdf['boundary_layer_height'].min():.0f} to {era5_pdf['boundary_layer_height'].max():.0f} m")

# Sanity checks
wind_speed = (era5_pdf['u10']**2 + era5_pdf['v10']**2)**0.5
print(f"\nDerived wind speed range: {wind_speed.min():.1f} to {wind_speed.max():.1f} m/s")
print(f"Mean wind speed: {wind_speed.mean():.1f} m/s")
print(f"Mean BLH: {era5_pdf['boundary_layer_height'].mean():.0f} m")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write to Bronze Delta table:

# CELL ********************

era5_spark = spark.createDataFrame(era5_pdf)

era5_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_era5_wind")

count = spark.table("bronze_era5_wind").count()
print(f"Written {count:,} rows to bronze_era5_wind")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
