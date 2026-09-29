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

# ### Load plume catalog:

# CELL ********************

import pandas as pd
import numpy as np

plumes = spark.table("gold_plume_catalog").toPandas()
print(f"Loaded {len(plumes)} plumes")

plumes["detection_date"] = pd.to_datetime(plumes["detection_date"])
print(f"Date range: {plumes['detection_date'].min()} to {plumes['detection_date'].max()}")
print(f"Distinct scenes: {plumes['scene_id'].nunique()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Match plumes to emission sites by proximity:

# CELL ********************

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = (np.sin(dlat/2)**2 +
         np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2)
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

match_radius_km = CONFIG["persistence_match_radius_km"]  # 5 km

# plume_id breaks ties. Plumes from one scene share a detection_date, and without a tie-break
# their order -- and so which one founds a site, and every site_id after it -- came from
# whatever row order toPandas() returned. With a content-derived plume_id the order, and
# gold_plume_site_mapping with it, is reproducible across reruns.
plumes_sorted = (plumes.sort_values(["detection_date", "plume_id"], kind="mergesort")
                 .reset_index(drop=True))

# Greedy site assignment: each plume joins nearest existing site or creates a new one
sites = []
site_counter = 0

for _, plume in plumes_sorted.iterrows():
    plat = plume["source_lat"]
    plon = plume["source_lon"]
    pid = plume["plume_id"]

    matched_site = None
    min_dist = float("inf")

    for site in sites:
        dist = haversine_km(plat, plon, site["lat"], site["lon"])
        if dist <= match_radius_km and dist < min_dist:
            matched_site = site
            min_dist = dist

    if matched_site is not None:
        matched_site["plume_ids"].append(pid)
        # Update site centroid as running average
        n = len(matched_site["plume_ids"])
        matched_site["lat"] = (matched_site["lat"] * (n - 1) + plat) / n
        matched_site["lon"] = (matched_site["lon"] * (n - 1) + plon) / n
    else:
        site_counter += 1
        sites.append({
            "site_id": site_counter,
            "lat": plat,
            "lon": plon,
            "plume_ids": [pid],
        })

print(f"Emission sites identified: {len(sites)}")
print(f"Sites with repeat detections: {sum(1 for s in sites if len(s['plume_ids']) > 1)}")
print(f"Max detections at a single site: {max(len(s['plume_ids']) for s in sites)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Classify sites by persistence:

# CELL ********************

# Values that mean "missing" whatever type they arrived as. A NULL read through toPandas can
# come back as None or as float('nan'); the mode of a column of them is NaN, and NaN written
# to a string column is the string "NaN" -- which passes IS NOT NULL. 05 had exactly this in
# attributed_facility_id; 06 had it one layer down, in attributed_facility for sites whose
# plumes are all unattributed (site 27, in the top-10 priority list). "NO_FACILITY_IN_RANGE"
# is 05's retired no-match sentinel, excluded so a catalog written before 05's fix cannot
# promote it to a facility name.
NULL_LIKE = ("nan", "none", "null")
LEGACY_SENTINELS = ("NO_FACILITY_IN_RANGE",)


def _real_strings(series):
    """The non-missing, non-sentinel string values of a column."""
    return [v for v in series.tolist()
            if isinstance(v, str) and v.strip() and v.strip().lower() not in NULL_LIKE
            and v not in LEGACY_SENTINELS]


def _mode(values):
    """Most common value, ties broken alphabetically so reruns agree; None if there is none."""
    if not values:
        return None
    counts = pd.Series(values).value_counts()
    top = counts[counts == counts.max()].index
    return sorted(top)[0]


site_records = []

for site in sites:
    pids = site["plume_ids"]
    site_plumes = plumes_sorted[plumes_sorted["plume_id"].isin(pids)]

    detection_count = len(pids)
    first_detection = site_plumes["detection_date"].min()
    last_detection = site_plumes["detection_date"].max()
    observation_span_days = (last_detection - first_detection).total_seconds() / 86400

    # Classify
    if detection_count == 1:
        persistence = "single"
    elif detection_count == 2:
        persistence = "intermittent"
    elif observation_span_days <= 30:
        persistence = "persistent"
    else:
        persistence = "chronic"

    # Aggregate emission stats
    avg_rate = site_plumes["emission_rate_kg_h"].mean()
    max_rate = site_plumes["emission_rate_kg_h"].max()
    total_ime = site_plumes["ime_kg"].sum()

    # Dominant confidence: NULL, not "unknown", when no plume carries one
    dominant_confidence = _mode(_real_strings(site_plumes["confidence"]))

    # Attributed facility (most common real facility name); NULL when no plume at the site is
    # attributed. value_counts() on a column of NaNs is where the "NaN" came from.
    if "attributed_facility_name" in site_plumes.columns:
        top_facility = _mode(_real_strings(site_plumes["attributed_facility_name"]))
    else:
        top_facility = None

    # List of detection dates for temporal analysis. A missing date would render as "nan"
    # inside the string, so it is refused rather than written.
    assert site_plumes["detection_date"].notna().all(), (
        f"site {site['site_id']}: a plume with no detection_date")
    detection_dates = sorted(site_plumes["detection_date"].dt.strftime("%Y-%m-%d").tolist())

    site_records.append({
        "site_id": site["site_id"],
        "site_lat": site["lat"],
        "site_lon": site["lon"],
        "detection_count": detection_count,
        "first_detection": first_detection,
        "last_detection": last_detection,
        "observation_span_days": round(observation_span_days, 1),
        "persistence": persistence,
        "avg_emission_rate_kg_h": round(avg_rate, 2),
        "max_emission_rate_kg_h": round(max_rate, 2),
        "total_ime_kg": round(total_ime, 2),
        "dominant_confidence": dominant_confidence,
        "attributed_facility": top_facility,
        "detection_dates": str(detection_dates),
        # Plain str per element: under NumPy 2, str() of a list of numpy scalars renders
        # "[np.str_('PL-...')]" (or "[np.int64(7)]" for the old counters), not the IDs.
        "plume_ids": str([str(p) for p in site["plume_ids"]]),
    })

sites_df = pd.DataFrame(site_records)

print("=== Persistence Classification ===")
print(sites_df["persistence"].value_counts().to_string())
print()
print("=== Detection Count Distribution ===")
print(sites_df["detection_count"].value_counts().sort_index().to_string())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary and high-priority sites:

# CELL ********************

print("=" * 60)
print("EMISSION SITE SUMMARY")
print("=" * 60)
print(f"Total emission sites: {len(sites_df)}")
print(f"Date range: {plumes['detection_date'].min().date()} to {plumes['detection_date'].max().date()}")
print()

for p_type in ["chronic", "persistent", "intermittent", "single"]:
    subset = sites_df[sites_df["persistence"] == p_type]
    if len(subset) > 0:
        print(f"--- {p_type.upper()} ({len(subset)} sites) ---")
        print(f"  Avg emission rate: {subset['avg_emission_rate_kg_h'].mean():.1f} kg/h")
        print(f"  Max emission rate: {subset['max_emission_rate_kg_h'].max():.1f} kg/h")
        print(f"  Total detections: {subset['detection_count'].sum()}")
        print(f"  Avg detection count: {subset['detection_count'].mean():.1f}")
        print()

print("=== TOP 10 PRIORITY SITES (by total IME) ===")
top_sites = sites_df.nlargest(10, "total_ime_kg")
print(top_sites[[
    "site_id", "site_lat", "site_lon", "detection_count",
    "persistence", "avg_emission_rate_kg_h", "total_ime_kg",
    "attributed_facility"
]].to_string())

print()
print("=== REPEAT EMITTERS ===")
repeats = sites_df[sites_df["detection_count"] > 1].sort_values(
    "detection_count", ascending=False
)
if len(repeats) > 0:
    print(f"{len(repeats)} sites with 2+ detections:")
    print(repeats[[
        "site_id", "site_lat", "site_lon", "detection_count",
        "observation_span_days", "persistence",
        "avg_emission_rate_kg_h", "attributed_facility",
        "detection_dates"
    ]].to_string())
else:
    print("No repeat emitters detected in this 1-month window")
    print("This is expected with ~27 scenes over 30 days at TROPOMI resolution")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write Gold tables:

# CELL ********************

# Written from Python rows against an explicit schema, never inferred from pandas. A missing
# value becomes a real NULL; a non-string headed for a string column is refused rather than
# written as "NaN".
from pyspark.sql import types as T

SITES_SCHEMA = T.StructType([
    T.StructField("site_id", T.LongType()),
    T.StructField("site_lat", T.DoubleType()),
    T.StructField("site_lon", T.DoubleType()),
    T.StructField("detection_count", T.LongType()),
    T.StructField("first_detection", T.TimestampType()),
    T.StructField("last_detection", T.TimestampType()),
    T.StructField("observation_span_days", T.DoubleType()),
    T.StructField("persistence", T.StringType()),
    T.StructField("avg_emission_rate_kg_h", T.DoubleType()),
    T.StructField("max_emission_rate_kg_h", T.DoubleType()),
    T.StructField("total_ime_kg", T.DoubleType()),
    T.StructField("dominant_confidence", T.StringType()),
    T.StructField("attributed_facility", T.StringType()),
    T.StructField("detection_dates", T.StringType()),
    T.StructField("plume_ids", T.StringType()),
])
MAP_SCHEMA = T.StructType([T.StructField("plume_id", T.StringType()),
                           T.StructField("site_id", T.LongType())])


def _cell(v, field):
    """A value Spark's verifier accepts for this field; every missing value becomes None."""
    if v is None or v is pd.NA or v is pd.NaT or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, np.generic):
        v = v.item()
    if isinstance(field.dataType, T.StringType):
        if not isinstance(v, str):
            raise TypeError(f"{field.name}: non-string {v!r} ({type(v).__name__}) for a "
                            "string column -- this is how 'NaN' strings get written")
        return v
    if isinstance(field.dataType, T.LongType):
        return int(v)
    if isinstance(field.dataType, T.DoubleType):
        return float(v)
    return v


def typed_frame(pdf, schema):
    assert list(pdf.columns) == [f.name for f in schema.fields], (
        f"frame columns {list(pdf.columns)} != schema {[f.name for f in schema.fields]}")
    rows = [tuple(_cell(v, f) for v, f in zip(r, schema.fields))
            for r in pdf.itertuples(index=False, name=None)]
    return spark.createDataFrame(rows, schema)


# Write emission sites
sites_spark = typed_frame(sites_df, SITES_SCHEMA)
sites_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_emission_sites")
print(f"Written {len(sites_df)} sites to gold_emission_sites")

# Plume-to-site mapping
plume_site_map = []
for site in sites:
    for pid in site["plume_ids"]:
        plume_site_map.append({
            "plume_id": pid,
            "site_id": site["site_id"],
        })

map_df = pd.DataFrame(plume_site_map, columns=["plume_id", "site_id"])
map_df["plume_id"] = map_df["plume_id"].astype(str)
map_spark = typed_frame(map_df, MAP_SCHEMA)
# overwriteSchema: plume_id is a string now (content-derived in 04), and Delta rejects the
# bigint -> string change on a plain overwrite.
map_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_plume_site_mapping")
print(f"Written {len(map_df)} plume-to-site mappings")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation: no column holds a null that is not NULL
#
# Run directly after the write, and before the lineage summary below, because that cell still
# references bronze_methane_pixels (a tracked known issue) and would stop the notebook first.

# CELL ********************

from pyspark.sql import functions as F


def assert_no_fake_nulls(table):
    """No string column holds 'NaN' / 'nan' / 'None' / 'null', and no double column holds NaN.
    Both pass IS NOT NULL and surface only when a join or a filter downstream misbehaves."""
    df = spark.table(table)
    bad = {}
    for f in df.schema.fields:
        if isinstance(f.dataType, T.StringType):
            n = df.filter(F.lower(F.trim(F.col(f.name))).isin(*NULL_LIKE)).count()
        elif isinstance(f.dataType, (T.DoubleType, T.FloatType)):
            n = df.filter(F.isnan(F.col(f.name))).count()
        else:
            n = 0
        if n:
            bad[f.name] = n
    assert not bad, f"{table}: column(s) holding a null-like string or NaN instead of NULL: {bad}"
    return len(df.schema.fields)


_n1 = assert_no_fake_nulls("gold_emission_sites")
_n2 = assert_no_fake_nulls("gold_plume_site_mapping")

# attributed_facility is NULL exactly where no plume at the site is attributed, and otherwise
# a real facility name
_es = spark.table("gold_emission_sites")
_sent = _es.filter(F.col("attributed_facility").isin(*LEGACY_SENTINELS)).count()
assert _sent == 0, f"{_sent} site(s) carry a retired sentinel as attributed_facility"
_names = {r["facility_name"] for r in spark.table("ref_facilities").select("facility_name").collect()}
_got = {r["attributed_facility"] for r in
        _es.filter("attributed_facility IS NOT NULL").select("attributed_facility").collect()}
assert _got <= _names, f"attributed_facility not a ref_facilities name: {sorted(_got - _names)[:5]}"
# Expected from the plume ids, independently of the name-mode logic above: a site with no
# plume carrying a real attributed_facility_id should have a NULL attributed_facility.
_att_ids = (set(plumes_sorted.loc[[isinstance(v, str) and v.strip().lower() not in NULL_LIKE
                                   for v in plumes_sorted["attributed_facility_id"]], "plume_id"])
            if "attributed_facility_id" in plumes_sorted.columns else set())
_expect_null = sum(1 for s_ in sites if not set(s_["plume_ids"]) & _att_ids)
_null = _es.filter("attributed_facility IS NULL").count()
assert _null == _expect_null, (f"{_null} site(s) with NULL attributed_facility, expected "
                               f"{_expect_null} -- the sites with no attributed plume")
assert spark.table("gold_plume_site_mapping").filter("plume_id IS NULL OR site_id IS NULL").count() == 0

print(f"OK  gold_emission_sites ({_n1} columns) and gold_plume_site_mapping ({_n2}) hold no "
      "'NaN' / 'nan' / 'None' / 'null' string and no double NaN")
print(f"OK  attributed_facility is NULL for exactly the {_null} site(s) with no attributed "
      "plume, and a ref_facilities name everywhere else")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validate all Gold tables:

# CELL ********************

print("=" * 60)
print("GOLD LAYER STATUS")
print("=" * 60)

gold_tables = [
    "gold_plume_catalog",
    "gold_flagged_large_clusters",
    "gold_emission_sites",
    "gold_plume_site_mapping",
    "ref_facilities",
]

for table in gold_tables:
    try:
        count = spark.table(table).count()
        cols = len(spark.table(table).columns)
        print(f"  {table}: {count:,} rows, {cols} columns")
    except Exception:
        print(f"  {table}: NOT FOUND")

print()
print("=" * 60)
print("FULL DATA LINEAGE")
print("=" * 60)

bronze_pixels = spark.table("bronze_methane_pixels").count()
bronze_weather = spark.table("bronze_weather").count()
bronze_era5 = spark.table("bronze_era5_wind").count()
silver = spark.table("silver_plume_ready_pixels").count()
gold_plumes = spark.table("gold_plume_catalog").count()
gold_sites = spark.table("gold_emission_sites").count()
gold_flagged = spark.table("gold_flagged_large_clusters").count()

print(f"  bronze_methane_pixels:       {bronze_pixels:>10,} rows")
print(f"  bronze_weather:              {bronze_weather:>10,} rows")
print(f"  bronze_era5_wind:            {bronze_era5:>10,} rows")
print(f"  silver_plume_ready_pixels:   {silver:>10,} rows")
print(f"  gold_plume_catalog:          {gold_plumes:>10,} rows")
print(f"  gold_emission_sites:         {gold_sites:>10,} rows")
print(f"  gold_flagged_large_clusters: {gold_flagged:>10,} rows")
print()
print(f"  Permian Basin pixels: {silver:,}")
print(f"  -> Detected plumes: {gold_plumes} ({100*gold_plumes/silver:.2f}%)")
print(f"  -> Emission sites: {gold_sites}")
print(f"  -> Flagged large clusters: 20 ({gold_flagged} pixels)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
