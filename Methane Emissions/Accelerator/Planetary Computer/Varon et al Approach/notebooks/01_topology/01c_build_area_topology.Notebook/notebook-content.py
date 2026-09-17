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

# # 01c — Build Area Topology
#
# Inserts the **process area** level between facility and asset. Real sites are organised
# into process areas, SCADA tags are named and grouped by area, and operations teams triage
# by area — so this level has to exist before any tag or telemetry layer sits on top of it.
#
# ```
# dim_facility  →  dim_area  →  dim_equipment  →  dim_sensor
# ```
#
# ### Writes
# - **`dim_area`** — new table, one row per process area
# - **`dim_equipment`** — rewritten with `area_sk` / `area_id` added. Additive only: every
#   existing column is preserved, and that is asserted before the write.
#
# `dim_sensor` is **not** touched. A sensor's area comes from its asset, by join.
#
# ### Area geography
# Each area sits 100–300 m from its facility centre on a deterministic bearing, so areas
# within a site are distinguishable while the site still reads as one location.
#
# **These coordinates are for plausibility and for future asset-level attribution. They are
# not something the detection layer can resolve.** A TROPOMI pixel is ~5.5 × 7.0 km, so an
# entire facility — every area in it — sits inside a fraction of one pixel. Area geography is
# three orders of magnitude below the instrument's resolving power. Any view that appears to
# attribute a plume to an area rather than a site is showing an artefact of this offset, not
# a measurement.
#
# ### Asset assignment
# Every asset belongs to exactly one area **of its own facility**, respecting equipment type:
# a Storage Tank lands in a Tank Farm, not a Compression Train. Where several of a facility's
# areas accept an equipment type, the choice is deterministic from
# `get_rng("area_assign", equipment_id)`.
#
# Where **none** accepts it, the asset falls back to the facility's primary area rather than
# being dropped. This happens because `TYPE_EQUIPMENT_WEIGHTS` gives every equipment type a
# non-zero weight at every facility type, independently of which areas that facility drew.
# The rate is measured and the notebook **fails above 10 %** — over that, the `AREA_TYPES`
# equipment lists are too narrow.

# MARKDOWN ********************

# `00_config` is already run by `01_topology_config`, which needs `CONFIG` and `BBOX` itself.
# It is run explicitly here as well so this notebook's dependencies are visible at the top of
# the file rather than inherited two levels down. Re-running it is idempotent — it only
# defines the `CONFIG` dict.

# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run 01_topology_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fac_pdf = (spark.table("dim_facility")
                .filter("is_current = true")
                .toPandas()
                .sort_values("facility_sk")
                .reset_index(drop=True))

eq_pdf = (spark.table("dim_equipment")
               .toPandas()
               .sort_values("equipment_sk")
               .reset_index(drop=True))

assert len(fac_pdf) == N_FACILITIES, (
    f"dim_facility holds {len(fac_pdf)} current rows, expected {N_FACILITIES} -- "
    "rerun 01a_build_facility_topology first"
)
assert (fac_pdf["topology_seed"] == TOPOLOGY_SEED).all(), (
    "dim_facility was built with a different TOPOLOGY_SEED than this notebook is using; "
    "rerun 01a and 01b so the whole estate comes from one seed"
)
assert (eq_pdf["topology_seed"] == TOPOLOGY_SEED).all(), (
    "dim_equipment was built with a different TOPOLOGY_SEED; rerun 01b"
)

# Captured before anything is added, so the write can prove it dropped nothing.
EQ_COLUMNS_BEFORE = list(eq_pdf.columns)
EQ_ROWS_BEFORE = len(eq_pdf)

assert "area_sk" not in EQ_COLUMNS_BEFORE and "area_id" not in EQ_COLUMNS_BEFORE, (
    "dim_equipment already carries area columns. This notebook is not idempotent against a "
    "half-applied state -- rerun 01b to rebuild dim_equipment, then rerun this notebook."
)

print(f"loaded {len(fac_pdf)} facilities and {len(eq_pdf):,} assets (seed {TOPOLOGY_SEED})")
print(f"dim_equipment columns before: {len(EQ_COLUMNS_BEFORE)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Draw each facility's areas
#
# Mandatory areas first — the ones without which the site does not function — then distinct
# optional types drawn by weight, and only then a repeat of a repeatable type (Compression
# Train A, B) to pad out to the target count.
#
# Distinct types are exhausted before any repeat on purpose: each additional distinct area
# accepts more equipment types, so preferring them minimises the fallback rate.

# CELL ********************

def draw_areas(facility_type, rng):
    """Area types for one facility, in order. First entry is the primary area."""
    spec = AREA_TYPES[facility_type]
    lo, hi = AREAS_PER_FACILITY[facility_type]
    n_target = int(rng.integers(lo, hi + 1))

    mandatory = [a for a, s in spec.items() if s["mandatory"]]
    chosen = list(mandatory)

    # distinct optional types, weighted, without replacement
    pool = [a for a, s in spec.items() if not s["mandatory"]]
    while len(chosen) < n_target and pool:
        w = np.array([spec[a]["weight"] for a in pool], dtype=float)
        pick = str(rng.choice(pool, p=w / w.sum()))
        chosen.append(pick)
        pool.remove(pick)

    # still short: repeat a repeatable type (a second compression train, more tankage)
    repeatable = [a for a, s in spec.items() if s["repeatable"]]
    while len(chosen) < n_target and repeatable:
        w = np.array([spec[a]["weight"] for a in repeatable], dtype=float)
        chosen.append(str(rng.choice(repeatable, p=w / w.sum())))

    return chosen


LETTERS = "ABCDEFGH"

area_rows = []
areas_by_facility = {}          # facility_id -> [area dicts, in order]

for f in fac_pdf.itertuples():
    arng = get_rng("areas", f.facility_id)
    chosen = draw_areas(f.facility_type, arng)

    # Suffix duplicated types A/B/...; a lone instance keeps the plain type name.
    counts = {}
    for a in chosen:
        counts[a] = counts.get(a, 0) + 1
    seen = {}

    facility_areas = []
    for ordinal, atype in enumerate(chosen, start=1):
        if counts[atype] > 1:
            seen[atype] = seen.get(atype, 0) + 1
            area_name = f"{atype} {LETTERS[seen[atype] - 1]}"
        else:
            area_name = atype

        area_id = f"{f.facility_id}-A{ordinal}"

        # Offset on a deterministic bearing, per area. Not a random walk: each area's
        # position depends only on its own area_id, so adding an area to one facility
        # cannot move the others.
        grng = get_rng("area", area_id)
        dist_m = float(grng.uniform(AREA_OFFSET_MIN_M, AREA_OFFSET_MAX_M))
        bearing = float(grng.uniform(0.0, 2.0 * np.pi))
        dlat = (dist_m * np.cos(bearing)) / 111_320.0
        dlon = (dist_m * np.sin(bearing)) / (111_320.0 * np.cos(np.radians(f.facility_lat)))

        crit_w = AREA_CRITICALITY_WEIGHTS[atype]
        crit_names = list(crit_w)
        criticality = str(grng.choice(crit_names, p=[crit_w[c] for c in crit_names]))

        rec = {
            "area_sk":        stable_key("area", area_id),
            "area_id":        area_id,
            "facility_sk":    int(f.facility_sk),
            "facility_id":    f.facility_id,
            "area_name":      area_name,
            "area_type":      atype,
            "area_lat":       float(f.facility_lat + dlat),
            "area_lon":       float(f.facility_lon + dlon),
            "criticality":    criticality,
            "is_synthetic":   True,
            "offset_m":       dist_m,           # kept for the distance assertion below
        }
        area_rows.append(rec)
        facility_areas.append(rec)

    areas_by_facility[f.facility_id] = facility_areas

area_pdf = pd.DataFrame(area_rows)
area_pdf["effective_from"] = pd.Timestamp(TOPOLOGY_AS_OF)
area_pdf["effective_to"]   = pd.Timestamp("2999-12-31")
area_pdf["is_current"]     = True
area_pdf["topology_seed"]  = TOPOLOGY_SEED

print(f"{len(area_pdf)} areas across {area_pdf['facility_id'].nunique()} facilities")
print(f"areas per facility: min {area_pdf.groupby('facility_id').size().min()}, "
      f"median {int(area_pdf.groupby('facility_id').size().median())}, "
      f"max {area_pdf.groupby('facility_id').size().max()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Assign every asset to an area of its own facility

# CELL ********************

fallback_rows = []

assign_area_sk = np.empty(len(eq_pdf), dtype=object)
assign_area_id = np.empty(len(eq_pdf), dtype=object)

facility_type_of = fac_pdf.set_index("facility_id")["facility_type"].to_dict()

# Precomputed per facility: equipment_type -> the areas of THAT facility accepting it.
# Built once rather than re-derived per asset, and keyed by facility so a candidate list can
# never contain another site's area.
accepts_by_facility = {}
for fid, areas in areas_by_facility.items():
    ft = facility_type_of[fid]
    acc = {et: [] for et in EQUIPMENT_TYPE_NAMES}
    for a in areas:
        for et in AREA_TYPES[ft][a["area_type"]]["equipment"]:
            acc[et].append(a)
    accepts_by_facility[fid] = acc

for i, e in enumerate(eq_pdf.itertuples()):
    areas = areas_by_facility[e.facility_id]      # only this facility's areas, ever
    candidates = accepts_by_facility[e.facility_id][e.equipment_type]

    if candidates:
        rng = get_rng("area_assign", e.equipment_id)
        chosen = candidates[int(rng.integers(0, len(candidates)))]
    else:
        # No area at this facility accepts this equipment type. Put it in the primary area
        # rather than dropping it, and record it -- a high rate means AREA_TYPES is too
        # narrow, not that the estate is wrong.
        chosen = areas[0]
        fallback_rows.append({
            "equipment_id": e.equipment_id,
            "facility_id": e.facility_id,
            "equipment_type": e.equipment_type,
            "assigned_area": chosen["area_name"],
        })

    assign_area_sk[i] = chosen["area_sk"]
    assign_area_id[i] = chosen["area_id"]

eq_pdf["area_sk"] = assign_area_sk.astype("int64")
eq_pdf["area_id"] = assign_area_id.astype(str)

fallback_pdf = pd.DataFrame(fallback_rows)
fallback_rate = len(fallback_pdf) / len(eq_pdf)

print(f"assigned {len(eq_pdf):,} assets to {area_pdf['area_id'].nunique()} areas")
print(f"fallback assignments: {len(fallback_pdf):,} / {len(eq_pdf):,}  ({fallback_rate:.2%})")

if len(fallback_pdf):
    print()
    print("fallback by facility type and equipment type:")
    fb = fallback_pdf.merge(fac_pdf[["facility_id", "facility_type"]], on="facility_id")
    for (ft, et), n in fb.groupby(["facility_type", "equipment_type"]).size() \
                         .sort_values(ascending=False).head(15).items():
        print(f"  {ft:<24}{et:<20}{n:>6}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation — every check fails the run, none warns

# CELL ********************

fac_types = fac_pdf.set_index("facility_id")["facility_type"].to_dict()

# --- keys --------------------------------------------------------------------------------
assert area_pdf["area_id"].is_unique, "area_id is not unique"
assert area_pdf["area_sk"].is_unique, (
    "area_sk is not unique -- a stable_key hash collision, or a duplicated area_id"
)
key_cols = ["area_sk", "area_id", "facility_sk", "facility_id", "area_name", "area_type",
            "area_lat", "area_lon", "criticality"]
nulls = area_pdf[key_cols].isna().sum()
assert nulls.sum() == 0, f"nulls in dim_area key columns:\n{nulls[nulls > 0]}"

# --- referential integrity -----------------------------------------------------------------
orphan_areas = set(area_pdf["facility_id"]) - set(fac_pdf["facility_id"])
assert not orphan_areas, f"areas referencing unknown facilities: {sorted(orphan_areas)[:5]}"

uncovered = set(fac_pdf["facility_id"]) - set(area_pdf["facility_id"])
assert not uncovered, f"{len(uncovered)} facilities have no area: {sorted(uncovered)[:5]}"

# --- area count per facility inside its type's range ---------------------------------------
per_fac = area_pdf.groupby("facility_id").size().rename("n_areas")
chk = fac_pdf[["facility_id", "facility_type"]].merge(per_fac, on="facility_id")
bad_counts = []
for r in chk.itertuples():
    lo, hi = AREAS_PER_FACILITY[r.facility_type]
    if not (lo <= r.n_areas <= hi):
        bad_counts.append((r.facility_id, r.facility_type, int(r.n_areas), lo, hi))
assert not bad_counts, f"facilities with an area count outside their type's range: {bad_counts[:5]}"

# --- every asset resolves to an area OF ITS OWN FACILITY ------------------------------------
# The cross-facility check. An assignment that is merely "a real area" is not enough; it has
# to be an area of the same site, or the hierarchy is silently broken.
assert eq_pdf["area_sk"].notna().all(), "assets with a null area_sk after assignment"
assert eq_pdf["area_id"].notna().all(), "assets with a null area_id after assignment"

area_owner = area_pdf.set_index("area_id")["facility_id"].to_dict()
unknown_area = set(eq_pdf["area_id"]) - set(area_owner)
assert not unknown_area, f"assets pointing at unknown areas: {sorted(unknown_area)[:5]}"

cross = eq_pdf[eq_pdf["area_id"].map(area_owner) != eq_pdf["facility_id"]]
assert cross.empty, (
    f"{len(cross)} asset(s) assigned to an area of a DIFFERENT facility:\n"
    f"{cross[['equipment_id', 'facility_id', 'area_id']].head().to_string(index=False)}"
)

# area_sk on dim_equipment must agree with dim_area's own mapping
sk_map = area_pdf.set_index("area_id")["area_sk"].to_dict()
sk_mismatch = eq_pdf[eq_pdf["area_id"].map(sk_map) != eq_pdf["area_sk"]]
assert sk_mismatch.empty, f"{len(sk_mismatch)} asset(s) whose area_sk disagrees with dim_area"

# --- row count unchanged ---------------------------------------------------------------------
assert len(eq_pdf) == EQ_ROWS_BEFORE, (
    f"asset count changed during assignment: {EQ_ROWS_BEFORE:,} -> {len(eq_pdf):,}"
)

# --- additive only ---------------------------------------------------------------------------
lost = set(EQ_COLUMNS_BEFORE) - set(eq_pdf.columns)
assert not lost, f"dim_equipment columns dropped: {sorted(lost)}"
added = [c for c in eq_pdf.columns if c not in EQ_COLUMNS_BEFORE]
assert added == ["area_sk", "area_id"], f"unexpected new columns on dim_equipment: {added}"

# --- area within 500 m of its facility --------------------------------------------------------
fac_ll = fac_pdf.set_index("facility_id")[["facility_lat", "facility_lon"]]
joined = area_pdf.join(fac_ll, on="facility_id")
dist_m = haversine_km(joined["area_lat"].values, joined["area_lon"].values,
                      joined["facility_lat"].values, joined["facility_lon"].values) * 1000.0
worst = float(dist_m.max())
assert worst <= AREA_OFFSET_MAX_ASSERT_M, (
    f"an area sits {worst:.1f} m from its facility, over the "
    f"{AREA_OFFSET_MAX_ASSERT_M:.0f} m cap. Areas belong on the same site."
)

# --- fallback rate ------------------------------------------------------------------------------
assert fallback_rate < 0.10, (
    f"fallback assignment rate is {fallback_rate:.2%}, at or above the 10% threshold. "
    "The AREA_TYPES equipment lists are too narrow for the equipment mix "
    "TYPE_EQUIPMENT_WEIGHTS produces -- widen the accepted types rather than raising this "
    "threshold."
)

print("OK  area_sk / area_id unique, no nulls in key columns")
print("OK  every facility has at least one area, count inside its type's range")
print("OK  every area resolves to a known facility")
print(f"OK  all {len(eq_pdf):,} assets resolve to an area of their OWN facility")
print(f"OK  asset count unchanged at {len(eq_pdf):,}; area columns added, none dropped")
print(f"OK  no area further than {AREA_OFFSET_MAX_ASSERT_M:.0f} m from its facility "
      f"(worst {worst:.1f} m, mean {dist_m.mean():.1f} m)")
print(f"OK  fallback rate {fallback_rate:.2%} is under the 10% threshold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Distributions

# CELL ********************

print("areas per facility, by facility type:")
print(f"  {'facility_type':<24}{'sites':>7}{'configured':>13}{'min':>6}{'median':>8}{'max':>6}"
      f"{'areas':>8}")
print("  " + "-" * 72)
for ft in FACILITY_TYPES:
    sub = chk[chk["facility_type"] == ft]["n_areas"]
    lo, hi = AREAS_PER_FACILITY[ft]
    if sub.empty:
        continue
    print(f"  {ft:<24}{len(sub):>7}{f'{lo}-{hi}':>13}{int(sub.min()):>6}"
          f"{int(sub.median()):>8}{int(sub.max()):>6}{int(sub.sum()):>8}")
print("  " + "-" * 72)
print(f"  {'all':<24}{len(chk):>7}{'':>13}{int(per_fac.min()):>6}"
      f"{int(per_fac.median()):>8}{int(per_fac.max()):>6}{len(area_pdf):>8}")

print()
print("assets per area:")
apa = eq_pdf.groupby("area_id").size()
apa = apa.reindex(area_pdf["area_id"], fill_value=0)
print(f"  min {int(apa.min())}   p25 {int(np.percentile(apa, 25))}   "
      f"median {int(apa.median())}   p75 {int(np.percentile(apa, 75))}   max {int(apa.max())}")
print(f"  mean {apa.mean():.1f}   areas with no asset: {int((apa == 0).sum())}")

print()
print("area_type distribution:")
for at, n in area_pdf["area_type"].value_counts().items():
    n_assets = int(eq_pdf.merge(area_pdf[["area_id", "area_type"]], on="area_id")
                         .query("area_type == @at").shape[0])
    print(f"  {at:<20}{n:>6} areas  {n/len(area_pdf):>6.1%}   {n_assets:>6} assets")

print()
print("criticality distribution:")
for c, n in area_pdf["criticality"].value_counts().items():
    print(f"  {c:<12}{n:>6}  ({n/len(area_pdf):>5.1%})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Equipment mix within each area — one facility of each type
#
# The eyeball check: does the assignment read as sensible? Storage Tanks should be in Tank
# Farms and Storage, Compressors in Compression Trains, Metering Stations in Metering.

# CELL ********************

eq_with_area = eq_pdf.merge(area_pdf[["area_id", "area_name", "area_type"]], on="area_id")

for ft in FACILITY_TYPES:
    cand = fac_pdf[fac_pdf["facility_type"] == ft]
    if cand.empty:
        continue
    f0 = cand.iloc[0]
    sub = eq_with_area[eq_with_area["facility_id"] == f0["facility_id"]]
    print("=" * 76)
    print(f"{f0['facility_id']}  {f0['facility_name']}  [{ft}]  "
          f"{len(sub)} assets in {sub['area_id'].nunique()} areas")
    print("=" * 76)
    for aid in area_pdf[area_pdf["facility_id"] == f0["facility_id"]]["area_id"]:
        arow = area_pdf[area_pdf["area_id"] == aid].iloc[0]
        in_area = sub[sub["area_id"] == aid]
        accepts = AREA_TYPES[ft][arow["area_type"]]["equipment"]
        print(f"  {arow['area_name']:<22} [{arow['criticality']:<8}] {len(in_area):>3} assets")
        if len(in_area):
            for et, n in in_area["equipment_type"].value_counts().items():
                flag = "" if et in accepts else "   <- fallback, area does not accept this"
                print(f"      {et:<20}{n:>4}{flag}")
        else:
            print("      (none)")
    print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write `dim_area` and rewrite `dim_equipment`

# CELL ********************

# offset_m was a working column for the distance assertion; it is not part of the schema.
area_out = area_pdf.drop(columns=["offset_m"])

AREA_SCHEMA = ["area_sk", "area_id", "facility_sk", "facility_id", "area_name", "area_type",
               "area_lat", "area_lon", "criticality", "is_synthetic",
               "effective_from", "effective_to", "is_current", "topology_seed"]
assert set(area_out.columns) == set(AREA_SCHEMA), (
    f"dim_area schema drift: unexpected {sorted(set(area_out.columns) - set(AREA_SCHEMA))}, "
    f"missing {sorted(set(AREA_SCHEMA) - set(area_out.columns))}"
)
area_out = area_out[AREA_SCHEMA]

dim_area = spark.createDataFrame(area_out)
(dim_area.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("dim_area"))
print(f"dim_area: {dim_area.count()} rows x {len(area_out.columns)} columns")

# dim_equipment keeps its original column order, with the two area columns appended.
eq_out = eq_pdf[EQ_COLUMNS_BEFORE + ["area_sk", "area_id"]]
dim_equipment = spark.createDataFrame(eq_out)
(dim_equipment.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("dim_equipment"))
print(f"dim_equipment: {dim_equipment.count():,} rows x {len(eq_out.columns)} columns "
      f"({len(EQ_COLUMNS_BEFORE)} original + 2 added)")

display(dim_area.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Verify the join path
#
# Asset → area → facility, all the way through. Area geography lives on `dim_area` and
# assets reach it by join, the same pattern asset geography uses for `dim_facility` —
# there is no `silver.*` copy to drift out of sync.

# CELL ********************

chain = spark.sql("""
    SELECT e.equipment_id, e.equipment_type,
           a.area_id, a.area_name, a.area_type, a.area_lat, a.area_lon,
           f.facility_id, f.facility_name, f.facility_lat, f.facility_lon
    FROM dim_equipment e
    JOIN dim_area a     ON e.area_id = a.area_id AND a.is_current = true
    JOIN dim_facility f ON a.facility_id = f.facility_id AND f.is_current = true
""")

n_eq = spark.table("dim_equipment").count()
n_chain = chain.count()
assert n_chain == n_eq, (
    f"asset -> area -> facility join returned {n_chain:,} rows for {n_eq:,} assets -- "
    "the chain is not 1:1"
)
print(f"OK  all {n_chain:,} assets resolve through dim_area to dim_facility")

display(chain.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary

# CELL ********************

print("=" * 72)
print("AREA TOPOLOGY BUILT")
print("=" * 72)
print(f"  seed             {TOPOLOGY_SEED}")
print(f"  facilities       {len(fac_pdf):,}   (from 01a, untouched)")
print(f"  areas            {len(area_pdf):,}")
print(f"  assets           {len(eq_pdf):,}   (unchanged, + area_sk / area_id)")
print(f"  area types       {area_pdf['area_type'].nunique()} distinct")
print(f"  fallback rate    {fallback_rate:.2%}  (threshold 10%)")
print(f"  area offset      mean {dist_m.mean():.0f} m, max {worst:.0f} m from facility centre")
print()
print("  tables written   dim_area (new), dim_equipment (rewritten, additive)")
print("  not touched      dim_facility, dim_sensor, every detection-layer table")
print()
print("  Area coordinates are for plausibility and future asset-level attribution only.")
print("  A TROPOMI pixel is ~5.5 x 7.0 km -- an entire facility sits inside a fraction of")
print("  one. Nothing in the detection layer can distinguish one area from another.")
print()
print("  next             SCADA tag topology, then telemetry")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
