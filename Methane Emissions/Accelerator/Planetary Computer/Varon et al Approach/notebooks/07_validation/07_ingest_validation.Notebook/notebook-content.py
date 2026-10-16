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

# PARAMETERS CELL ********************

run_mode = ""
start_date = ""
end_date = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load configs

# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run mode and window
# `run_mode`, `start_date` and `end_date` arrive through the parameter cell at the top of this
# notebook, and nowhere else: Fabric hands a pipeline's base parameters to a notebook only by
# injecting a cell beneath a toggled parameter cell. That cell must stay first, ahead of
# `%run 00_config`, so the injected values exist before anything reads them.
# The window is half-open, `[start_date, end_date)`. `greensky_detection` always passes
# `end_date` (P+1 for `pipeline_date` P), and passes `start_date` as P for incremental and empty
# for backfill. An empty `start_date` on a backfill means `DETECTION_HISTORY_START` (00_config).
# Nothing else is filled in: with no `end_date`, or no `start_date` on an incremental run, this
# cell raises. An interactive run sets them in the parameter cell.
# `START_DATE` and `LAST_DATE`, the last day inside the window, are the inclusive form, for STAC
# and Open-Meteo, which take inclusive date ranges.

# CELL ********************

import re
from datetime import datetime as _dt

import pandas as pd

print(f"parameters received: run_mode={run_mode!r} start_date={start_date!r} end_date={end_date!r}")

def _param_date(name, value):
    value = str(value).strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"{name} must be yyyy-MM-dd, got {value!r}")
    return pd.Timestamp(_dt.strptime(value, "%Y-%m-%d"))

RUN_MODE = str(run_mode).strip().lower()
if RUN_MODE not in ("backfill", "incremental"):
    raise ValueError(f"run_mode must be 'backfill' or 'incremental', got {run_mode!r}")

HISTORY_START = pd.Timestamp(DETECTION_HISTORY_START)

if not str(end_date).strip():
    raise ValueError(
        "end_date is empty, so the window cannot be determined. The pipeline always passes it; "
        "for an interactive run, set end_date (exclusive, yyyy-MM-dd) in the parameter cell."
    )
WINDOW_END = _param_date("end_date", end_date)
_end_source = "end_date parameter"

if str(start_date).strip():
    WINDOW_START = _param_date("start_date", start_date)
    _start_source = "start_date parameter"
elif RUN_MODE == "backfill":
    WINDOW_START = HISTORY_START
    _start_source = "DETECTION_HISTORY_START (backfill, start_date empty)"
else:
    raise ValueError(
        "run_mode is incremental but start_date is empty, so the window cannot be determined. "
        "The pipeline passes pipeline_date as start_date; set it in the parameter cell."
    )

if not WINDOW_START < WINDOW_END:
    raise ValueError(f"empty window: [{WINDOW_START.date()}, {WINDOW_END.date()})")
if WINDOW_START < HISTORY_START:
    raise ValueError(
        f"window starts {WINDOW_START.date()}, before DETECTION_HISTORY_START {HISTORY_START.date()}"
    )

_DAY = pd.Timedelta(days=1)
START_DATE = WINDOW_START.strftime("%Y-%m-%d")
LAST_DATE = (WINDOW_END - _DAY).strftime("%Y-%m-%d")
print(f"RUN_MODE={RUN_MODE}  window=[{START_DATE}, {WINDOW_END.date()})  last day {LAST_DATE}")
print(f"  start from {_start_source}")
print(f"  end   from {_end_source}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load and explore CAMS plume catalog:

# CELL ********************

import pandas as pd
import numpy as np
import requests

cams_path = "/lakehouse/default/Files/validation/Schuit_etal2023_TROPOMI_all_plume_detections_2021.csv"

cams_raw = pd.read_csv(cams_path)
print(f"CAMS raw records: {len(cams_raw):,}")
print(f"Columns: {list(cams_raw.columns)}")
print(f"\nColumn dtypes:")
print(cams_raw.dtypes.to_string())
print(f"\nFirst 3 rows:")
print(cams_raw.head(3).to_string())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Identify and standardize column names:

# CELL ********************

# CAMS CSV column names vary by version
# Common patterns: lat/lon, source_lat/source_lon, latitude/longitude
# Inspect and map

print("=== Column value samples ===")
for col_name in cams_raw.columns:
    sample = cams_raw[col_name].dropna().head(3).tolist()
    print(f"  {col_name}: {sample}")

# Map column names
col_map = {}
for c in cams_raw.columns:
    cl = c.lower().strip()
    if cl in ["lat", "latitude", "source_lat", "plume_lat"]:
        col_map[c] = "cams_lat"
    elif cl in ["lon", "longitude", "source_lon", "plume_lon"]:
        col_map[c] = "cams_lon"
    elif cl in ["date", "datetime", "time", "observation_date"]:
        col_map[c] = "cams_date"
    elif "source_rate" in cl or "emission" in cl or "flux" in cl:
        col_map[c] = "cams_emission_rate_th"
    elif "uncertainty" in cl:
        col_map[c] = "cams_uncertainty_th"
    elif "source" in cl and "type" in cl:
        col_map[c] = "cams_source_type"

print(f"Column mapping: {col_map}")
cams = cams_raw.rename(columns=col_map)

# Fix date -- integer format YYYYMMDD
if "cams_date" in cams.columns:
    cams["cams_date"] = pd.to_datetime(cams["cams_date"].astype(str), format="%Y%m%d", errors="coerce")
    print(f"Date range: {cams['cams_date'].min()} to {cams['cams_date'].max()}")

# Convert coordinates to numeric
cams["cams_lat"] = pd.to_numeric(cams["cams_lat"], errors="coerce")
cams["cams_lon"] = pd.to_numeric(cams["cams_lon"], errors="coerce")

# Convert emission rate to numeric and add kg/h column
cams["cams_emission_rate_th"] = pd.to_numeric(cams["cams_emission_rate_th"], errors="coerce")
cams["cams_emission_rate_kgh"] = cams["cams_emission_rate_th"] * 1000  # t/h to kg/h

if "cams_uncertainty_th" in cams.columns:
    cams["cams_uncertainty_th"] = pd.to_numeric(cams["cams_uncertainty_th"], errors="coerce")
    cams["cams_uncertainty_kgh"] = cams["cams_uncertainty_th"] * 1000

print(f"\nCleaned columns: {list(cams.columns)}")
print(f"Total records: {len(cams)}")
print(cams.head(3).to_string())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Filter CAMS to Permian Basin:

# CELL ********************

pad = 0.5
permian_cams = cams[
    (cams["cams_lat"] >= BBOX["min_lat"] - pad) &
    (cams["cams_lat"] <= BBOX["max_lat"] + pad) &
    (cams["cams_lon"] >= BBOX["min_lon"] - pad) &
    (cams["cams_lon"] <= BBOX["max_lon"] + pad)
].copy()

print(f"CAMS plumes in Permian Basin: {len(permian_cams)} / {len(cams)}")

if len(permian_cams) > 0:
    print(f"Date range: {permian_cams['cams_date'].min()} to {permian_cams['cams_date'].max()}")
    print(f"Lat range: {permian_cams['cams_lat'].min():.2f} to {permian_cams['cams_lat'].max():.2f}")
    print(f"Lon range: {permian_cams['cams_lon'].min():.2f} to {permian_cams['cams_lon'].max():.2f}")
    print(f"Emission rate (t/h): {permian_cams['cams_emission_rate_th'].min():.0f} to {permian_cams['cams_emission_rate_th'].max():.0f}")
    print(f"Emission rate (kg/h): {permian_cams['cams_emission_rate_kgh'].min():.0f} to {permian_cams['cams_emission_rate_kgh'].max():.0f}")
    print(f"Source types: {permian_cams['cams_source_type'].value_counts().to_string()}")
    print(f"\nSample rows:")
    print(permian_cams.head(5).to_string())
else:
    print("No CAMS plumes found in Permian Basin")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write CAMS validation table:

# CELL ********************

if len(permian_cams) > 0:
    cams_spark = spark.createDataFrame(permian_cams)
    cams_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("validation_cams_plumes")
    print(f"Written {len(permian_cams)} CAMS plumes to validation_cams_plumes")

cams_full_spark = spark.createDataFrame(cams)
cams_full_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("validation_cams_plumes_global")
print(f"Written {len(cams)} global CAMS plumes to validation_cams_plumes_global")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check EMIT accessibility:

# CELL ********************

# Try different EMIT collection IDs
print("--- Testing EMIT collections ---")
emit_collections = [
    "EMITL2BCH4PLM.v002",
    "EMITL2BCH4PLM_002",
    "EMITL2BCH4ENH.v002",
    "EMITL2BCH4ENH_002",
]

emit_collection_id = None
for coll in emit_collections:
    url = f"https://cmr.earthdata.nasa.gov/stac/LPCLOUD/collections/{coll}"
    try:
        r = requests.get(url, timeout=15)
        print(f"  {coll}: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            print(f"    Title: {data.get('title', 'N/A')[:80]}")
            emit_collection_id = coll
    except Exception as e:
        print(f"  {coll}: {str(e)[:60]}")

# Search by keyword if none found
if emit_collection_id is None:
    print("\n--- Searching all LPCLOUD collections for EMIT CH4 ---")
    try:
        r = requests.get(
            "https://cmr.earthdata.nasa.gov/stac/LPCLOUD/collections",
            params={"limit": 200},
            timeout=30
        )
        if r.status_code == 200:
            data = r.json()
            collections = data.get("collections", [])
            emit_ch4 = [c for c in collections
                        if "emit" in c.get("id", "").lower()
                        and "ch4" in c.get("id", "").lower()]
            print(f"EMIT CH4 collections found: {len(emit_ch4)}")
            for c in emit_ch4:
                print(f"  {c['id']}: {c.get('title', 'N/A')[:80]}")
                emit_collection_id = c["id"]
    except Exception as e:
        print(f"Search error: {e}")

# If found, search for Permian Basin plumes
if emit_collection_id:
    print(f"\n--- Searching EMIT ({emit_collection_id}) for Permian Basin ---")
    try:
        search_url = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD/search"
        search_body = {
            "collections": [emit_collection_id],
            "bbox": [BBOX["min_lon"], BBOX["min_lat"], BBOX["max_lon"], BBOX["max_lat"]],
            "datetime": f"{START_DATE}T00:00:00Z/{LAST_DATE}T23:59:59Z",
            "limit": 20
        }
        r = requests.post(search_url, json=search_body, timeout=30)
        print(f"  Search status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            features = data.get("features", [])
            print(f"  EMIT plumes found: {len(features)}")
            for f in features[:5]:
                props = f.get("properties", {})
                print(f"    {f.get('id', 'N/A')}: {props.get('datetime', 'N/A')}")
    except Exception as e:
        print(f"  EMIT search error: {e}")
else:
    print("\nNo EMIT CH4 collection found. EMIT validation will be skipped.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Test Carbon Mapper API

# CELL ********************

# Token from data.carbonmapper.org -- move to Key Vault for production
CM_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzg3NTU1MTYxLCJpYXQiOjE3ODY5NTAzNjEsImp0aSI6IjQxNTE3ZWI4NDlmNTQ0MDE4NzMwNjE5Y2E4YWY0ZjZjIiwic2NvcGUiOiJzdGFjIGNhdGFsb2c6cmVhZCIsImdyb3VwcyI6IlB1YmxpYyIsImFsbF9ncm91cF9uYW1lcyI6eyJjb21tb24iOlsiUHVibGljIl19LCJvcmdhbml6YXRpb25zIjoiIiwic2V0dGluZ3MiOnt9LCJpc19zdGFmZiI6ZmFsc2UsImlzX3N1cGVydXNlciI6ZmFsc2UsInVzZXJfaWQiOjMyMjQyLCJpc19jdXN0b21fdG9rZW4iOnRydWV9.zRFgwENQWLNmb_qthos8u39Zc6Bl14XQgEinsToks9M"  # paste your token here, delete after session

print("--- Testing Carbon Mapper API ---")

base_url = "https://api.carbonmapper.org/api/v1"
CM_HEADERS = {"Authorization": f"Bearer {CM_TOKEN}"}

try:
    # bbox must be passed as separate parameters, not a comma-separated string
    params = [
        ("bbox", BBOX["min_lon"]),
        ("bbox", BBOX["min_lat"]),
        ("bbox", BBOX["max_lon"]),
        ("bbox", BBOX["max_lat"]),
        ("gas", "CH4"),
        ("limit", 100),
        ("offset", 0),
        ("sort", "desc"),
    ]

    r = requests.get(
        f"{base_url}/catalog/plumes/annotated",
        params=params,
        headers=CM_HEADERS,
        timeout=30
    )
    print(f"Plumes endpoint: {r.status_code}")
    print(f"  URL: {r.url[:150]}...")

    if r.status_code == 200:
        data = r.json()
        total = data.get("total_count", 0)
        items = data.get("items", [])
        print(f"  Total plumes in Permian Basin: {total}")
        print(f"  Items returned: {len(items)}")

        if items:
            first = items[0]
            print(f"\n  First plume keys: {list(first.keys())}")
            for key in first.keys():
                val = first[key]
                if isinstance(val, (int, float, str)) and val is not None:
                    print(f"  {key}: {val}")
                elif isinstance(val, dict) and len(str(val)) < 200:
                    print(f"  {key}: {val}")
    elif r.status_code == 401:
        print("  AUTH FAILED: Token may be expired or invalid")
    elif r.status_code == 403:
        print("  ACCESS DENIED: Network egress blocked or insufficient permissions")
    elif r.status_code == 422:
        print(f"  VALIDATION ERROR: {r.text[:500]}")
    else:
        print(f"  Response ({r.status_code}): {r.text[:300]}")

except Exception as e:
    print(f"  Error: {e}")

# Sources endpoint
print("\n--- Carbon Mapper Sources ---")
try:
    src_params = [
        ("bbox", BBOX["min_lon"]),
        ("bbox", BBOX["min_lat"]),
        ("bbox", BBOX["max_lon"]),
        ("bbox", BBOX["max_lat"]),
        ("gas", "CH4"),
        ("limit", 50),
    ]

    r2 = requests.get(
        f"{base_url}/catalog/sources",
        headers=CM_HEADERS,
        params=src_params,
        timeout=30
    )
    print(f"Sources endpoint: {r2.status_code}")
    if r2.status_code == 200:
        src_data = r2.json()
        print(f"  Total sources in Permian Basin: {src_data.get('total_count', 0)}")
        src_items = src_data.get("items", [])
        if src_items:
            for key in src_items[0].keys():
                val = src_items[0][key]
                if isinstance(val, (int, float, str)) and val is not None:
                    print(f"    {key}: {val}")
    elif r2.status_code == 422:
        print(f"  VALIDATION ERROR: {r2.text[:500]}")
    else:
        print(f"  Response ({r2.status_code}): {r2.text[:300]}")
except Exception as e:
    print(f"  Error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paginate Carbon Mapper plumes (only run if Cell 7 returned 200):

# CELL ********************

cm_plumes_all = []
offset = 0
page_size = 100
max_plumes = 2000  # cap to avoid downloading tens of thousands

print("Downloading Carbon Mapper plumes for Permian Basin...")

while True:
    try:
        params = [
            ("bbox", BBOX["min_lon"]),
            ("bbox", BBOX["min_lat"]),
            ("bbox", BBOX["max_lon"]),
            ("bbox", BBOX["max_lat"]),
            ("gas", "CH4"),
            ("limit", page_size),
            ("offset", offset),
            ("sort", "desc"),
        ]

        r = requests.get(
            f"{base_url}/catalog/plumes/annotated",
            params=params,
            headers=CM_HEADERS,
            timeout=60
        )

        if r.status_code != 200:
            print(f"  Stopped at offset {offset}: status {r.status_code}")
            break

        data = r.json()
        items = data.get("items", [])
        total = data.get("total_count", 0)

        if not items:
            break

        cm_plumes_all.extend(items)
        offset += page_size
        print(f"  Fetched {len(cm_plumes_all)} / {total} plumes...")

        if len(cm_plumes_all) >= total or len(cm_plumes_all) >= max_plumes:
            if len(cm_plumes_all) >= max_plumes:
                print(f"  Reached cap of {max_plumes} plumes (total available: {total})")
            break

    except Exception as e:
        print(f"  Error at offset {offset}: {e}")
        break

print(f"\nTotal Carbon Mapper plumes downloaded: {len(cm_plumes_all)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Parse Carbon Mapper plumes:

# CELL ********************

if cm_plumes_all:
    cm_records = []
    for plume in cm_plumes_all:
        lat = None
        lon = None

        if "latitude" in plume:
            lat = plume["latitude"]
            lon = plume["longitude"]
        elif "geometry_json" in plume and plume["geometry_json"]:
            geom = plume["geometry_json"]
            if "coordinates" in geom:
                coords = geom["coordinates"]
                if isinstance(coords[0], (int, float)):
                    lon, lat = coords[0], coords[1]
                elif isinstance(coords[0], list):
                    flat = coords[0] if isinstance(coords[0][0], (int, float)) else coords[0][0]
                    if isinstance(flat[0], list):
                        lons = [c[0] for c in flat]
                        lats = [c[1] for c in flat]
                        lon = np.mean(lons)
                        lat = np.mean(lats)

        if lat is None and "source_latitude" in plume:
            lat = plume["source_latitude"]
            lon = plume["source_longitude"]

        record = {
            "cm_plume_id": plume.get("plume_id", plume.get("id", None)),
            "cm_lat": lat,
            "cm_lon": lon,
            "cm_gas": plume.get("gas", None),
            "cm_emission_rate": plume.get("emission_rate", plume.get("emission_auto", None)),
            "cm_emission_uncertainty": plume.get("emission_uncertainty", None),
            "cm_datetime": plume.get("datetime", plume.get("scene_timestamp", None)),
            "cm_sensor": plume.get("instrument", plume.get("sensor", None)),
            "cm_source_name": plume.get("source_name", None),
            "cm_sector": plume.get("sector", None),
        }
        cm_records.append(record)

    cm_df = pd.DataFrame(cm_records)
    cm_df["cm_lat"] = pd.to_numeric(cm_df["cm_lat"], errors="coerce")
    cm_df["cm_lon"] = pd.to_numeric(cm_df["cm_lon"], errors="coerce")
    cm_df["cm_emission_rate"] = pd.to_numeric(cm_df["cm_emission_rate"], errors="coerce")

    if "cm_datetime" in cm_df.columns:
        cm_df["cm_datetime"] = pd.to_datetime(cm_df["cm_datetime"], errors="coerce")

    cm_df = cm_df.dropna(subset=["cm_lat", "cm_lon"])

    print(f"Carbon Mapper plumes parsed: {len(cm_df)}")
    if len(cm_df) > 0:
        print(f"Date range: {cm_df['cm_datetime'].min()} to {cm_df['cm_datetime'].max()}")
        if cm_df["cm_emission_rate"].notna().any():
            rates = cm_df["cm_emission_rate"].dropna()
            print(f"Emission rate range: {rates.min():.1f} to {rates.max():.1f}")
        if "cm_sensor" in cm_df.columns:
            print(f"Sensors:\n{cm_df['cm_sensor'].value_counts().to_string()}")
        print(f"\nSample rows:")
        print(cm_df.head(5).to_string())

        # Check overlap with Green Sky dates
        gs_start = pd.Timestamp("2026-06-10", tz="UTC")
        gs_end = pd.Timestamp("2026-07-09", tz="UTC")
        overlap = cm_df[
            (cm_df["cm_datetime"] >= gs_start) &
            (cm_df["cm_datetime"] <= gs_end)
        ]
        print(f"\nCarbon Mapper plumes in Green Sky date range: {len(overlap)}")
else:
    cm_df = pd.DataFrame()
    print("No Carbon Mapper plumes to parse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write Carbon Mapper table:

# CELL ********************

if len(cm_df) > 0:
    # Fill nulls so Spark can infer types
    cm_write = cm_df.copy()
    for col_name in cm_write.columns:
        if cm_write[col_name].isna().all():
            # Determine fill value based on column name
            if "rate" in col_name or "lat" in col_name or "lon" in col_name or "uncertainty" in col_name:
                cm_write[col_name] = cm_write[col_name].astype("float64")
            else:
                cm_write[col_name] = cm_write[col_name].astype("str")
    
    # Convert tz-aware datetime to tz-naive for Spark compatibility
    if "cm_datetime" in cm_write.columns and cm_write["cm_datetime"].dt.tz is not None:
        cm_write["cm_datetime"] = cm_write["cm_datetime"].dt.tz_localize(None)

    cm_spark = spark.createDataFrame(cm_write)
    cm_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("validation_carbon_mapper_plumes")
    print(f"Written {len(cm_write)} Carbon Mapper plumes to validation_carbon_mapper_plumes")

    rates = cm_df["cm_emission_rate"].dropna()
    print(f"\n=== Carbon Mapper Permian Basin Summary ===")
    print(f"Total plumes: {len(cm_df)}")
    if len(rates) > 0:
        print(f"Emission rates: {rates.min():.1f} to {rates.max():.1f}")
        print(f"  Median: {rates.median():.1f}")
        print(f"  Mean: {rates.mean():.1f}")
    if "cm_sector" in cm_df.columns and cm_df["cm_sector"].notna().any():
        print(f"Sectors:\n{cm_df['cm_sector'].value_counts().to_string()}")
else:
    print("No Carbon Mapper data available")
    print("Options:")
    print("  1. Register at https://data.carbonmapper.org for API access")
    print("  2. Browse data.carbonmapper.org, filter to Permian Basin, download CSV")
    print("  3. Contact data@carbonmapper.org for bulk access")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation data summary:

# CELL ********************

print("=" * 60)
print("VALIDATION DATA SUMMARY")
print("=" * 60)

# CAMS
print(f"\nCAMS/SRON (Schuit et al. 2023):")
print(f"  Global plumes: {len(cams)}")
print(f"  Permian Basin plumes: {len(permian_cams)}")
print(f"  Date range: 2021 (no temporal overlap with Green Sky Jun-Jul 2026)")
print(f"  Emission rates: {permian_cams['cams_emission_rate_th'].min():.0f}-{permian_cams['cams_emission_rate_th'].max():.0f} t/h")
print(f"  NOTE: CAMS rates are in t/h (super-emitters), Green Sky rates are in kg/h")
print(f"  CAMS detects sources 1000x larger than Green Sky's current detections")

# Carbon Mapper
if len(cm_df) > 0:
    print(f"\nCarbon Mapper:")
    print(f"  Permian Basin plumes: {len(cm_df)}")
    if cm_df["cm_datetime"].notna().any():
        print(f"  Date range: {cm_df['cm_datetime'].min().date()} to {cm_df['cm_datetime'].max().date()}")
    overlap_count = len(cm_df[
        (cm_df["cm_datetime"] >= pd.Timestamp("2026-06-10", tz="UTC")) &
        (cm_df["cm_datetime"] <= pd.Timestamp("2026-07-09", tz="UTC"))
    ]) if cm_df["cm_datetime"].notna().any() else 0
    print(f"  Plumes overlapping Green Sky dates: {overlap_count}")
else:
    print(f"\nCarbon Mapper: not available (API returned non-200 or blocked)")

# EMIT
print(f"\nEMIT:")
if emit_collection_id:
    print(f"  Collection found: {emit_collection_id}")
    print(f"  Permian Basin plumes in Green Sky dates: check search results above")
else:
    print(f"  Collection not found on CMR STAC")

print(f"\n--- Usable Comparisons ---")
print(f"  CAMS: spatial pattern comparison only (no temporal overlap)")
print(f"  Carbon Mapper: {'spatial + temporal if overlap exists' if len(cm_df) > 0 else 'not available'}")
print(f"  EMIT: {'check results above' if emit_collection_id else 'not available'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("gold_plume_catalog")
print(len(df.columns), "columns")
print([c for c in df.columns if c.startswith("era5")])
print("rows:", df.count(), "distinct plume_id:", df.select("plume_id").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

b = spark.read.table("bronze_ch4_pixels")
print("bronze rows:", b.count())
print("distinct stac_id:", b.select("stac_id").distinct().count())
b.groupBy("stac_id").count().orderBy("count", ascending=False).show(10, False)

s = spark.read.table("silver_plume_ready_pixels")
print("silver rows:", s.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
b = spark.read.table("bronze_ch4_pixels")
w = b.filter((F.col("datetime") >= "2026-08-16") & (F.col("datetime") < "2026-09-16"))
print("window rows:", w.count(), "| window granules:", w.select("stac_id").distinct().count())
dupes = w.groupBy("stac_id","latitude","longitude","datetime").count().filter("count > 1")
print("duplicated pixel keys:", dupes.count())
dupes.orderBy("count", ascending=False).show(5, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

old = {"PL-f8ec764f9cdf","PL-3d0e59779a8a","PL-bd75ec23b1ae","PL-881dbf57ee04",
"PL-b8b3b93d5e01","PL-da0edd6bd41e","PL-290605ca1745","PL-fba050bced02",
"PL-8a46231dbd67","PL-772b132551d7","PL-f19b099ff53b","PL-ee07d05aa443",
"PL-fc6e6c832a11","PL-094e1a99c910","PL-d95d056e7d0c","PL-4aa750c5e496",
"PL-73b184bbb246","PL-8640819172c0","PL-9af924cd9b69","PL-63d41dc780bc",
"PL-5e5dc7a6da41","PL-88f5ee2613f7","PL-f52a4cb34f9a","PL-fd527423a15b",
"PL-2c80a662469b","PL-d6024ee3f97a","PL-d7f86d52a89e","PL-c0958f29f551",
"PL-4891ab2feb97","PL-dd9cfda8fa72","PL-634b8c756084","PL-cecdbc4553e4",
"PL-875fc7e65934","PL-b823142179bc","PL-771b55350b6c","PL-140fdf382e6b",
"PL-3df7b6aaabdf","PL-8e95c5889a52","PL-62f209476b54","PL-84d936fc5c16",
"PL-e7c7f1b2ad9d","PL-7f0d68b5329a","PL-19aafb474281","PL-38ec1f72dd88",
"PL-3965e3d50edb","PL-561a8947191b","PL-a336625b917d","PL-c39055b1ca19",
"PL-f1714a3e7a5e"}

new = set(spark.read.table("gold_plume_catalog").select("plume_id").toPandas()["plume_id"])
print("baseline:", len(old), "| now:", len(new))
print("added:", sorted(new - old))
print("removed:", sorted(old - new))
print("kept:", len(new & old))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DESCRIBE HISTORY bronze_ch4_pixels") \
     .select("version","timestamp","operation","operationMetrics") \
     .orderBy("version", ascending=False).show(6, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
spark.read.table("gold_plume_catalog") \
     .filter(F.col("plume_id") == "PL-bfdb1e16e469") \
     .select("plume_id","scene_id","stac_id","detection_date","source_lat","source_lon",
             "n_pixels","emission_rate_kg_h","attributed_facility_id",
             "attribution_method","attribution_probability","attribution_distance_km",
             "t_mix_method","confidence") \
     .show(1, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in ["silver_plume_ready_pixels", "bronze_weather", "bronze_era5_wind"]:
    print("===", t)
    spark.sql(f"DESCRIBE HISTORY {t}") \
         .select("version","timestamp","operation","operationMetrics") \
         .orderBy("version", ascending=False).show(3, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

a = spark.read.format("delta").option("versionAsOf", 5).table("silver_plume_ready_pixels")
b = spark.read.format("delta").option("versionAsOf", 6).table("silver_plume_ready_pixels")

px = ["latitude","longitude","datetime","stac_id","ch4","qa_value"]
print("pixel-set rows only in v6:", b.select(*px).subtract(a.select(*px)).count())
print("pixel-set rows only in v5:", a.select(*px).subtract(b.select(*px)).count())

print("full-row diff v6-v5:", b.subtract(a).count())
print("columns:", b.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

a = spark.read.format("delta").option("versionAsOf", 5).table("silver_plume_ready_pixels")
b = spark.read.format("delta").option("versionAsOf", 6).table("silver_plume_ready_pixels")

only_v6 = b.subtract(a)
only_v5 = a.subtract(b)

print("=== only in v6 (new run) ===")
only_v6.select("stac_id","latitude","longitude","datetime","ch4","qa_value",
               "weather_dist_km","weather_weight") \
       .orderBy("latitude").show(60, False)

print("=== only in v5 (12 Oct) ===")
only_v5.select("stac_id","latitude","longitude","datetime","ch4","qa_value",
               "weather_dist_km","weather_weight") \
       .orderBy("latitude").show(60, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

only_v6.filter((F.col("stac_id").contains("20260819T193848")) &
               (F.abs(F.col("latitude") - 31.108) < 0.3) &
               (F.abs(F.col("longitude") + 103.654) < 0.3)).show(10, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

k = ["stac_id","latitude","longitude","datetime"]
print("same key, different values:", only_v6.select(*k).intersect(only_v5.select(*k)).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
b = spark.read.table("bronze_ch4_pixels")
w = b.filter((F.col("datetime") >= "2026-08-16") & (F.col("datetime") < "2026-09-16"))

d = w.groupBy("latitude","longitude","ch4").agg(
        F.countDistinct("stac_id").alias("n_granules"),
        F.collect_set("processing_mode").alias("modes"))
print("pixels in >1 granule:", d.filter("n_granules > 1").count())
d.filter("n_granules > 1").show(5, False)
print("by mode:", w.groupBy("processing_mode").agg(F.countDistinct("stac_id"), F.count("*")).show())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
