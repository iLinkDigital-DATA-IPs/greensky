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

# ### Load configs

# CELL ********************

%run 00_config

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
            "datetime": f"{CONFIG['start_date']}T00:00:00Z/{CONFIG['end_date']}T23:59:59Z",
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
