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

# # 01a — Build Facility Topology
#
# Writes **`dim_facility`** (the enterprise facility master) and **`ref_facilities`** (the
# projection `05_attribute_facilities` consumes).
#
# `ref_facilities` becomes a **generated table with a real upstream** rather than the EPA
# scrape / `PB_nnn` grid fallback `05` builds today. Once this notebook has run, `05` should
# read `ref_facilities` instead of regenerating it — its grid fallback is retired.
#
# ### One coordinate pair
# `facility_lat` / `facility_lon`, written once, here. There is deliberately no
# `latitude` / `longitude` pair: V1 carried both and nothing recorded which was authoritative.
# `dim_equipment` (01b) stores no coordinates at all and joins on `facility_id`, so a second
# pair cannot appear without someone adding a column on purpose. The last cell asserts this.
#
# ### Facility keys are `GS-nnnn`
# Not V1's `FAC-nnnn`. 350 `FAC-` keys still exist in `Operations_LH` at different
# coordinates from a different seed; a distinct prefix makes the break visible in any query
# result that mixes the two.
#
# ### Writes
# `dim_facility` and `ref_facilities`, both `mode("overwrite")` — the estate is regenerated
# wholesale from `TOPOLOGY_SEED`, so a partial update has no meaning.

# CELL ********************

%run 01_topology_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Operators
#
# Names stay generic (`Operator 01`). The estate is synthetic and must read as synthetic —
# the accelerator's `bronze.facility_master` attributes invented emissions to named real
# companies (Occidental, XTO Energy), which this model deliberately does not do.

# CELL ********************

op_rng = get_rng("operators")

dim_operator = pd.DataFrame({
    "operator_sk": range(1, N_OPERATORS + 1),
    "operator_name": [f"Operator {i:02d}" for i in range(1, N_OPERATORS + 1)],
    "operator_tier": op_rng.choice(
        ["Major", "Independent", "Small Cap"], size=N_OPERATORS, p=[0.25, 0.50, 0.25]
    ),
})

print(f"{len(dim_operator)} operators")
print(dim_operator.to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Generate facilities
#
# Type first, then the name from the type. `name_descriptor` is persisted so validation does
# not have to depend on string parsing — though the assertion cell below parses the name
# anyway, because that is what actually catches a disagreement.

# CELL ********************

frng = get_rng("facilities")

# Exact quota, not a per-facility draw -- the realised band counts match FACILITY_SPLIT.
bands = allocate_bands(N_FACILITIES, frng)
print("band quota allocated:")
for b in FACILITY_SPLIT:
    n = bands.count(b)
    print(f"  {b:<12}{n:>5}  target {FACILITY_SPLIT[b]:.0%}  actual {n/N_FACILITIES:.1%}")
print()

rows = []
used_names = set()

for i, band in enumerate(bands, start=1):
    lat, lon, anchor = sample_facility_location(frng, band)

    # Type FIRST -- the name is derived from it, so the two cannot disagree.
    ftype = str(frng.choice(
        FACILITY_TYPES, p=[FACILITY_TYPE_WEIGHTS[t] for t in FACILITY_TYPES]
    ))
    descriptor = str(frng.choice(TYPE_DESCRIPTORS[ftype]))
    place = str(frng.choice(PLACES))

    name = f"{place} {descriptor}"
    suffix = 2
    while name in used_names:
        name = f"{place} {descriptor} {suffix}"
        suffix += 1
    used_names.add(name)

    commission = TOPOLOGY_AS_OF - timedelta(days=int(frng.integers(0, 365 * HISTORY_YEARS)))

    rows.append({
        "facility_sk":        i,
        "facility_id":        f"{FACILITY_ID_PREFIX}-{i:04d}",
        "facility_name":      name,
        "name_descriptor":    descriptor,
        "facility_type":      ftype,
        "operator_sk":        int(frng.integers(1, N_OPERATORS + 1)),
        "sub_basin":          anchor,
        "region_band":        band,
        "facility_lat":       lat,
        "facility_lon":       lon,
        "bbox_excursion_deg": bbox_excursion_deg(lat, lon),
        "country":            "USA",
        "state":              "TX",
        "commission_date":    pd.Timestamp(commission),
        "active_flag":        bool(frng.random() > 0.04),
    })

fac_pdf = pd.DataFrame(rows)

# Derived, not drawn: whether the facility is inside the detection footprint at all.
fac_pdf["in_detection_bbox"] = fac_pdf["bbox_excursion_deg"] == 0.0

fac_pdf = fac_pdf.merge(dim_operator[["operator_sk", "operator_name"]], on="operator_sk", how="left")

# SCD2 columns, so an incremental rebuild can merge rather than overwrite later.
fac_pdf["effective_from"] = pd.Timestamp(TOPOLOGY_AS_OF)
fac_pdf["effective_to"]   = pd.Timestamp("2999-12-31")
fac_pdf["is_current"]     = True
fac_pdf["topology_seed"]  = TOPOLOGY_SEED

print(f"{len(fac_pdf)} facilities generated")
print(fac_pdf[["facility_id", "facility_name", "facility_type", "sub_basin",
               "region_band", "facility_lat", "facility_lon"]].head(12).to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Geography check — band distribution and BBOX excursion
#
# This is the cell that would have caught the V1 defect. `FAC-0157` sat at latitude 29.48,
# about 110 km south of the basin and 1.02° below the BBOX floor; the assertion below caps
# any excursion at `OUTSIDE_MAX_DEG` (0.30°, ~33 km).

# CELL ********************

print(f"BBOX: lat {BBOX['min_lat']}..{BBOX['max_lat']}   lon {BBOX['min_lon']}..{BBOX['max_lon']}")
print()
print(f"{'band':<12}{'count':>7}{'share':>9}{'max excursion':>16}{'max km from anchor':>21}")
print("-" * 65)

for band in ("inside", "perimeter", "outside"):
    sub = fac_pdf[fac_pdf["region_band"] == band]
    if sub.empty:
        print(f"{band:<12}{0:>7}{'':>9}{'':>16}{'':>21}")
        continue
    km = haversine_km(
        sub["facility_lat"].values, sub["facility_lon"].values,
        np.array([ANCHORS[a]["lat"] for a in sub["sub_basin"]]),
        np.array([ANCHORS[a]["lon"] for a in sub["sub_basin"]]),
    )
    print(f"{band:<12}{len(sub):>7}{len(sub)/len(fac_pdf):>8.1%}"
          f"{sub['bbox_excursion_deg'].max():>16.3f}{km.max():>21.1f}")

print("-" * 65)
print(f"{'total':<12}{len(fac_pdf):>7}{1.0:>8.1%}"
      f"{fac_pdf['bbox_excursion_deg'].max():>16.3f}")
print()

# Quota allocation, so realised counts must equal the configured split to within rounding.
for band, share in FACILITY_SPLIT.items():
    got = int((fac_pdf["region_band"] == band).sum())
    want = int(round(share * N_FACILITIES))
    assert abs(got - want) <= 1, (
        f"band '{band}' holds {got} facilities, expected ~{want} from the quota. "
        "allocate_bands should make this exact -- a gap means the band was drawn, not allocated."
    )
print(f"OK  realised band counts match the configured split (quota allocation)")
print()
print(f"inside the detection BBOX: {int(fac_pdf['in_detection_bbox'].sum())}"
      f" / {len(fac_pdf)}  ({fac_pdf['in_detection_bbox'].mean():.1%})")
print(f"latitude  range: {fac_pdf['facility_lat'].min():.4f} .. {fac_pdf['facility_lat'].max():.4f}")
print(f"longitude range: {fac_pdf['facility_lon'].min():.4f} .. {fac_pdf['facility_lon'].max():.4f}")

# --- assertions -------------------------------------------------------------------------
worst = fac_pdf["bbox_excursion_deg"].max()
assert worst <= OUTSIDE_MAX_DEG + 1e-9, (
    f"a facility sits {worst:.3f} deg outside CONFIG['bbox'], over the {OUTSIDE_MAX_DEG} deg "
    "cap. That is the V1 generation defect (FAC-0157 was 1.02 deg out), not a perimeter case."
)

leaked = fac_pdf[(fac_pdf["region_band"] != "outside") & (fac_pdf["bbox_excursion_deg"] > 0)]
assert leaked.empty, (
    f"{len(leaked)} inside/perimeter facilities fell outside the BBOX -- the outside band "
    f"must be the only source of out-of-BBOX rows:\n{leaked[['facility_id', 'region_band']].head()}"
)

out = fac_pdf[fac_pdf["region_band"] == "outside"]
if not out.empty:
    assert (out["bbox_excursion_deg"] >= OUTSIDE_MIN_DEG - 1e-9).all(), \
        "an 'outside' facility landed inside the BBOX -- the band is not doing its job"

print()
print(f"OK  no facility more than {OUTSIDE_MAX_DEG} deg outside the BBOX (worst {worst:.3f})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Name / type consistency
#
# Construction guarantees agreement — the descriptor is drawn from `TYPE_DESCRIPTORS[ftype]`.
# The assertion is kept anyway, and deliberately validates by **parsing the generated name**
# rather than by re-reading `name_descriptor`: a check that only compared the stored
# descriptor to the map would pass even if the name string had been built from something else.
#
# `assert_descriptor_map_unique()` guards the other direction — a later edit that lists one
# descriptor under two types would make every row pass a name-derived-from-type check while
# reintroducing exactly the V1 ambiguity.

# CELL ********************

import re

rev = assert_descriptor_map_unique()
print(f"descriptor map OK: {len(rev)} descriptors, each owned by exactly one facility_type")


def parse_descriptor(name):
    """Recover the descriptor from a generated facility name.

    Names are '<place> <descriptor>' with an optional ' <n>' dedup suffix, so strip a
    trailing integer and match the longest descriptor the remainder ends with.
    """
    stem = re.sub(r"\s+\d+$", "", str(name)).strip()
    hits = [d for d in rev if stem.endswith(d)]
    return max(hits, key=len) if hits else None


checked = fac_pdf.copy()
checked["parsed_descriptor"] = checked["facility_name"].map(parse_descriptor)
checked["implied_type"] = checked["parsed_descriptor"].map(
    lambda d: rev[d][0] if d in rev else None
)

unparsed = checked[checked["parsed_descriptor"].isna()]
assert unparsed.empty, (
    f"{len(unparsed)} facility name(s) carry no recognised descriptor:\n"
    f"{unparsed[['facility_id', 'facility_name']].head().to_string(index=False)}"
)

mismatch = checked[checked["implied_type"] != checked["facility_type"]]
assert mismatch.empty, (
    f"{len(mismatch)} facility name(s) contradict facility_type -- this is the V1 defect "
    f"(\"Odessa Processing Plant\" typed Gathering System):\n"
    f"{mismatch[['facility_id', 'facility_name', 'facility_type', 'implied_type']].head().to_string(index=False)}"
)

drift = checked[checked["parsed_descriptor"] != checked["name_descriptor"]]
assert drift.empty, (
    f"{len(drift)} name(s) do not match their stored name_descriptor -- the name was built "
    "from something other than the drawn descriptor"
)

print(f"OK  all {len(fac_pdf)} facility names agree with facility_type")
print()
print("facility_type distribution:")
for t, n in fac_pdf["facility_type"].value_counts().items():
    sample = fac_pdf[fac_pdf["facility_type"] == t]["facility_name"].iloc[0]
    print(f"  {t:<24}{n:>5}  ({n/len(fac_pdf):5.1%})   e.g. {sample}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Key and schema integrity

# CELL ********************

# Keys
assert fac_pdf["facility_id"].is_unique, "facility_id is not unique"
assert fac_pdf["facility_sk"].is_unique, "facility_sk is not unique"
assert fac_pdf["facility_name"].is_unique, "facility_name is not unique"

bad_ids = fac_pdf[~fac_pdf["facility_id"].str.match(rf"^{FACILITY_ID_PREFIX}-\d{{4}}$")]
assert bad_ids.empty, f"malformed facility_id(s): {bad_ids['facility_id'].tolist()[:5]}"

# facility_sk is a deterministic 1..N sequence, NOT monotonically_increasing_id(). The
# accelerator's gold.dim_facility uses the latter, which makes its surrogate keys unstable
# across reruns -- the same class of defect CLAUDE.md tracks for V2's plume_id/scene_id.
assert fac_pdf["facility_sk"].tolist() == list(range(1, N_FACILITIES + 1)), \
    "facility_sk must be a stable 1..N sequence"

# One coordinate pair, and only one.
legacy = {"latitude", "longitude", "lat", "lon"} & set(fac_pdf.columns)
assert not legacy, (
    f"second coordinate pair present: {sorted(legacy)}. dim_facility carries exactly one pair, "
    "facility_lat/facility_lon -- V1 carried two and nothing recorded which was authoritative."
)

# No nulls in anything attribution depends on.
critical = ["facility_id", "facility_name", "facility_type", "facility_lat", "facility_lon"]
nulls = fac_pdf[critical].isna().sum()
assert nulls.sum() == 0, f"nulls in critical columns:\n{nulls[nulls > 0]}"

print("OK  keys unique, GS-nnnn format, stable 1..N surrogate sequence")
print("OK  exactly one coordinate pair (facility_lat/facility_lon)")
print("OK  no nulls in", ", ".join(critical))
print()
print(f"dim_facility: {len(fac_pdf)} rows x {len(fac_pdf.columns)} columns")
print("columns:", ", ".join(fac_pdf.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Attribution coverage — can every plume reach a facility?
#
# Runs **before the write**, so a coverage gap is visible in the same run that would
# introduce it rather than after `05` has already failed to attribute anything.
#
# For every plume in `gold_plume_catalog`, the distance to the **nearest** facility. A plume
# whose nearest facility is beyond `CONFIG["attribution_search_radius_km"]` cannot be
# attributed by `05` at all: the candidate search returns nothing, so it exits with
# `facilities_in_range = 0` before scoring or the upwind cone ever run. No amount of wind
# geometry rescues it.
#
# The latitude comparison at the end is the part that matters. A bare count says coverage is
# short; comparing the median latitude of uncovered plumes against covered ones says *where*,
# which is what identifies a missing anchor. This is exactly how the northern gap was found —
# uncovered plumes sat at median latitude 32.96 against 31.88 for covered ones, and the
# `Northwest Shelf` anchor was added to close it.
#
# Read-only, and skipped cleanly when `gold_plume_catalog` is absent or empty so `01a` still
# runs on a fresh workspace before any detection has happened.

# CELL ********************

cov_plumes = None
try:
    if spark.catalog.tableExists("gold_plume_catalog"):
        cov_plumes = spark.table("gold_plume_catalog").toPandas()
    else:
        print("gold_plume_catalog does not exist yet -- skipping the attribution coverage check.")
        print("Run 04_derive_emissions, then rerun this cell before trusting the estate's coverage.")
except Exception as exc:
    print(f"could not read gold_plume_catalog ({type(exc).__name__}: {exc}) -- skipping")

if cov_plumes is not None and len(cov_plumes) == 0:
    print("gold_plume_catalog is empty -- skipping the attribution coverage check.")
    cov_plumes = None

if cov_plumes is not None:
    _missing = [c for c in ("source_lat", "source_lon") if c not in cov_plumes.columns]
    if _missing:
        print(f"gold_plume_catalog lacks {_missing} -- cannot measure coverage")
        cov_plumes = None

if cov_plumes is not None:
    search_km = CONFIG["attribution_search_radius_km"]

    p_lat = cov_plumes["source_lat"].to_numpy(dtype=float)
    p_lon = cov_plumes["source_lon"].to_numpy(dtype=float)
    f_lat = fac_pdf["facility_lat"].to_numpy(dtype=float)
    f_lon = fac_pdf["facility_lon"].to_numpy(dtype=float)

    # plumes x facilities, then the nearest facility for each plume
    d_km = haversine_km(p_lat[:, None], p_lon[:, None], f_lat[None, :], f_lon[None, :])
    nearest_km = d_km.min(axis=1)

    print(f"{len(cov_plumes)} plumes, {len(fac_pdf)} facilities, "
          f"attribution radius {search_km:.0f} km")
    print()
    print("distance from each plume to its NEAREST facility (km):")
    for label, val in [
        ("min", nearest_km.min()),
        ("p25", np.percentile(nearest_km, 25)),
        ("median", np.median(nearest_km)),
        ("p75", np.percentile(nearest_km, 75)),
        ("p90", np.percentile(nearest_km, 90)),
        ("max", nearest_km.max()),
        ("mean", nearest_km.mean()),
    ]:
        print(f"  {label:<8}{val:>9.1f}")

    print()
    print("  histogram:")
    edges = [0, 10, 20, 30, 40, 50, 75, 100, np.inf]
    for lo_e, hi_e in zip(edges[:-1], edges[1:]):
        n_b = int(((nearest_km >= lo_e) & (nearest_km < hi_e)).sum())
        band_lbl = f"{lo_e:.0f}-{hi_e:.0f}" if np.isfinite(hi_e) else f"{lo_e:.0f}+"
        beyond = "  <- beyond attribution radius" if lo_e >= search_km else ""
        bar = "#" * int(round(40 * n_b / max(len(cov_plumes), 1)))
        print(f"  {band_lbl:>8} km {n_b:>5}  {bar}{beyond}")

    uncovered = nearest_km > search_km
    n_unc = int(uncovered.sum())
    share_unc = n_unc / len(cov_plumes)

    print()
    print(f"beyond {search_km:.0f} km (unattributable by 05): "
          f"{n_unc} / {len(cov_plumes)}  ({share_unc:.1%})")
    print(f"within  {search_km:.0f} km: {len(cov_plumes) - n_unc} / {len(cov_plumes)}  "
          f"({1 - share_unc:.1%})")

    # Direction of the gap. A count alone says coverage is short; the latitude split says
    # which part of the footprint is short, which is what points at a missing anchor.
    print()
    if n_unc and n_unc < len(cov_plumes):
        lat_unc = float(np.median(p_lat[uncovered]))
        lat_cov = float(np.median(p_lat[~uncovered]))
        lon_unc = float(np.median(p_lon[uncovered]))
        lon_cov = float(np.median(p_lon[~uncovered]))
        print("median position, uncovered vs covered plumes:")
        print(f"  {'':<12}{'latitude':>10}{'longitude':>12}{'nearest km':>13}")
        print(f"  {'uncovered':<12}{lat_unc:>10.2f}{lon_unc:>12.2f}"
              f"{np.median(nearest_km[uncovered]):>13.1f}")
        print(f"  {'covered':<12}{lat_cov:>10.2f}{lon_cov:>12.2f}"
              f"{np.median(nearest_km[~uncovered]):>13.1f}")
        print(f"  {'difference':<12}{lat_unc - lat_cov:>+10.2f}{lon_unc - lon_cov:>+12.2f}")
    elif n_unc:
        print(f"every plume is beyond {search_km:.0f} km -- no covered group to compare against")
    else:
        print(f"no uncovered plumes: median latitude of covered plumes is "
              f"{np.median(p_lat):.2f}")

    print()
    if share_unc > 0.20:
        lat_unc = float(np.median(p_lat[uncovered]))
        lon_unc = float(np.median(p_lon[uncovered]))
        print(f"NOTE  {share_unc:.1%} of plumes ({n_unc} of {len(cov_plumes)}) have no facility")
        print(f"      within the {search_km:.0f} km attribution radius. 05 will return")
        print(f"      facilities_in_range = 0 for all of them.")
        print(f"      Uncovered plumes centre on {lat_unc:.2f} N, {abs(lon_unc):.2f} W "
              f"(median nearest facility {np.median(nearest_km[uncovered]):.1f} km).")
        print(f"      Anchors currently at: "
              + ", ".join(f"{n} {a['lat']:.2f}N/{abs(a['lon']):.2f}W"
                          for n, a in ANCHORS.items()))
        print(f"      Consider an anchor near the uncovered centroid, or raising")
        print(f"      CONFIG['attribution_search_radius_km'] above {search_km:.0f}.")
    else:
        print(f"OK    {1 - share_unc:.1%} of plumes have a facility inside the "
              f"{search_km:.0f} km attribution radius.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write `dim_facility` and `ref_facilities`

# CELL ********************

dim_facility = spark.createDataFrame(fac_pdf)
(dim_facility.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("dim_facility"))
print(f"dim_facility: {dim_facility.count()} rows written")

# ref_facilities is the projection 05_attribute_facilities consumes. The first four columns
# are the contract -- 05 reads facility_lat, facility_lon, facility_id, facility_name by
# name -- and the rest is context for the attribution report.
ref_cols = [
    "facility_id", "facility_name", "facility_lat", "facility_lon",
    "facility_type", "operator_name", "sub_basin", "region_band", "in_detection_bbox",
]
ref_facilities = spark.createDataFrame(fac_pdf[ref_cols])
(ref_facilities.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("ref_facilities"))
print(f"ref_facilities: {ref_facilities.count()} rows written")

print()
print("05_attribute_facilities should now read this table rather than scraping EPA or")
print("generating the PB_nnn grid. Required columns are present:")
for c in ("facility_id", "facility_name", "facility_lat", "facility_lon"):
    print(f"  {c:<16} {'OK' if c in ref_cols else 'MISSING'}")

display(ref_facilities.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Is 150 the right scale?
#
# The estate is sized against the detection rate, not against V1. `N_FACILITIES` is the one
# knob; this cell is the evidence for whether it is set correctly.
#
# The question is whether the **Facility Operations** dashboard page has anything to show.
# With ~75 plumes over a 30-day window spread across a large estate, most facilities never
# receive a detection and the page is empty for nearly all of them. What matters is the share
# of facilities with at least one plume nearby, and how concentrated the detections are.
#
# 25 km is a reporting radius, not the attribution radius — `05` attributes with
# `CONFIG["attribution_search_radius_km"]` (50 km) and weights by wind direction, so a
# facility counted here has not necessarily been attributed a plume. Both radii are printed.
#
# Read-only: this cell writes nothing and is skipped cleanly when `gold_plume_catalog` is
# absent or empty, so `01a` still runs on a fresh workspace before any detection has happened.

# CELL ********************

PROXIMITY_RADIUS_KM = 25.0

plumes_pdf = None
try:
    if spark.catalog.tableExists("gold_plume_catalog"):
        plumes_pdf = spark.table("gold_plume_catalog").toPandas()
    else:
        print("gold_plume_catalog does not exist yet -- skipping the coverage check.")
        print("Run 04_derive_emissions, then rerun this cell to size the estate.")
except Exception as exc:
    print(f"could not read gold_plume_catalog ({type(exc).__name__}: {exc}) -- skipping")

if plumes_pdf is not None and len(plumes_pdf) == 0:
    print("gold_plume_catalog is empty -- skipping the coverage check.")
    plumes_pdf = None

if plumes_pdf is not None:
    missing = [c for c in ("source_lat", "source_lon") if c not in plumes_pdf.columns]
    if missing:
        print(f"gold_plume_catalog lacks {missing} -- cannot measure coverage")
        plumes_pdf = None

if plumes_pdf is not None:
    n_plumes = len(plumes_pdf)
    p_lat = plumes_pdf["source_lat"].to_numpy(dtype=float)
    p_lon = plumes_pdf["source_lon"].to_numpy(dtype=float)

    # facilities x plumes distance matrix -- 150 x ~75 is trivial
    f_lat = fac_pdf["facility_lat"].to_numpy(dtype=float)[:, None]
    f_lon = fac_pdf["facility_lon"].to_numpy(dtype=float)[:, None]
    dist_km = haversine_km(f_lat, f_lon, p_lat[None, :], p_lon[None, :])

    within = dist_km <= PROXIMITY_RADIUS_KM
    per_facility = within.sum(axis=1)
    attrib_km = CONFIG["attribution_search_radius_km"]
    per_facility_attrib = (dist_km <= attrib_km).sum(axis=1)

    covered = int((per_facility > 0).sum())
    covered_attrib = int((per_facility_attrib > 0).sum())

    print(f"{n_plumes} plumes in gold_plume_catalog, {len(fac_pdf)} facilities")
    print()
    print(f"within {PROXIMITY_RADIUS_KM:.0f} km of at least one plume: "
          f"{covered} / {len(fac_pdf)}  ({covered/len(fac_pdf):.1%})")
    print(f"within {attrib_km:.0f} km (05 attribution radius):       "
          f"{covered_attrib} / {len(fac_pdf)}  ({covered_attrib/len(fac_pdf):.1%})")
    print()

    print(f"plumes per facility within {PROXIMITY_RADIUS_KM:.0f} km:")
    print(f"  {'plumes':<10}{'facilities':>12}{'share':>9}")
    print("  " + "-" * 31)
    counts = pd.Series(per_facility).value_counts().sort_index()
    shown = 0
    for k, v in counts.items():
        if k >= 10:
            continue
        print(f"  {int(k):<10}{int(v):>12}{v/len(fac_pdf):>9.1%}")
        shown += int(v)
    tail = len(fac_pdf) - shown
    if tail:
        print(f"  {'10+':<10}{tail:>12}{tail/len(fac_pdf):>9.1%}")
    print("  " + "-" * 31)
    print(f"  mean {per_facility.mean():.2f}   median {int(np.median(per_facility))}   "
          f"max {int(per_facility.max())}")
    print()

    # A facility page is worth building only if a decent share of sites have something on it.
    if covered / len(fac_pdf) < 0.20:
        print(f"NOTE  only {covered/len(fac_pdf):.1%} of facilities have a plume within "
              f"{PROXIMITY_RADIUS_KM:.0f} km.")
        print("      The Facility Operations page will be empty for most of the estate.")
        print(f"      Consider lowering N_FACILITIES below {N_FACILITIES} in 01_topology_config.")
    elif covered / len(fac_pdf) > 0.80:
        print(f"NOTE  {covered/len(fac_pdf):.1%} of facilities have a plume within "
              f"{PROXIMITY_RADIUS_KM:.0f} km.")
        print("      Coverage is near-total, so the estate may be too small to be")
        print(f"      interesting. N_FACILITIES could go above {N_FACILITIES}.")
    else:
        print(f"OK    {covered/len(fac_pdf):.1%} of facilities have a plume within "
              f"{PROXIMITY_RADIUS_KM:.0f} km -- N_FACILITIES={N_FACILITIES} looks reasonable.")

    # Which sites carry the detections -- useful when picking a demo facility.
    top = fac_pdf.assign(plumes_within=per_facility) \
                 .nlargest(10, "plumes_within")[
                     ["facility_id", "facility_name", "facility_type", "region_band",
                      "plumes_within"]]
    print()
    print(f"most-detected facilities (within {PROXIMITY_RADIUS_KM:.0f} km):")
    print(top.to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary

# CELL ********************

print("=" * 72)
print("FACILITY TOPOLOGY REBUILT")
print("=" * 72)
print(f"  seed            {TOPOLOGY_SEED}  (every draw derives from this)")
print(f"  facilities      {len(fac_pdf)}")
print(f"  key format      {FACILITY_ID_PREFIX}-0001 .. {FACILITY_ID_PREFIX}-{N_FACILITIES:04d}")
print(f"  coordinates     facility_lat / facility_lon  (one pair)")
print(f"  in BBOX         {int(fac_pdf['in_detection_bbox'].sum())} / {len(fac_pdf)}")
print(f"  max excursion   {fac_pdf['bbox_excursion_deg'].max():.3f} deg"
      f"  (cap {OUTSIDE_MAX_DEG})")
print(f"  sub-basins      {', '.join(f'{k}={v}' for k, v in fac_pdf['sub_basin'].value_counts().items())}")
print(f"  name/type       verified consistent for all {len(fac_pdf)} rows")
print()
print("  tables written  dim_facility, ref_facilities")
print("  next            01b_build_asset_topology")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
