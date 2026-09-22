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

# # 01 — Topology Config
#
# Seeds, geography, taxonomy and helpers for the rebuilt enterprise facility and asset
# topology. Every notebook in `01_topology` starts with `%run 01_topology_config`, which
# itself runs `00_config` so `CONFIG` and `BBOX` are available.
#
# This replaces the V1 model in `Operations_LH` (`archive/notebooks/scada/*`). That model is
# **reference only — do not edit it.** It is being replaced, not extended, because of three
# defects this config is built to prevent:
#
# **1. Two coordinate pairs, neither marked authoritative.** V1's `gold.dim_facility` carried
# both `latitude`/`longitude` (written by `dim_build`) and `facility_lat`/`facility_lon`
# (written later by `build_attribution`, which overwrote the dimension). Nothing recorded
# which was correct, and they disagreed about where the basin was.
# → Here there is **one** pair, `facility_lat`/`facility_lon`, written once. `dim_equipment`
# stores **no** coordinates at all and reaches geography by joining `facility_id`, so the two
# cannot drift apart again.
#
# **2. Facilities outside the detection footprint.** V1 sampled from two anchors, one of which
# (`Texas Site A`, lat 30.2) sat below V2's BBOX floor of 30.5, with an `outside` band
# reaching 0.95°. That stranded facilities near lat 29.3 — roughly 110 km south of the
# Permian. `build_attribution` then reseeded coordinates from a hard-coded box using a bare
# `np.random.default_rng(42)`, bypassing `MASTER_SEED` entirely.
# → Here the anchors are **real Permian sub-basins, all well inside `CONFIG["bbox"]`**, the
# `outside` band is placed **relative to the BBOX edge and bounded at
# `OUTSIDE_MAX_DEG`**, and every draw comes from `get_rng(...)`, which derives from
# `TOPOLOGY_SEED`. There is no bare `default_rng` anywhere in `01_topology`.
#
# **3. Names and types contradicted each other.** V1 drew `facility_name` and `facility_type`
# independently, producing "Odessa Processing Plant" typed `Gathering System` and
# "Wink Tank Battery 2" typed `Compression Station`.
# → Here the **type is drawn first and the name is built from it** via `TYPE_DESCRIPTORS`, so
# agreement holds by construction. `assert_descriptor_map_unique()` still guards the map, to
# catch a later edit that puts one descriptor under two types.
#
# What is kept from V1: the name generator. `PLACES` and the descriptor vocabulary are
# reused unchanged — they produce realistic Permian names like "Reeves County Tank Battery".
# The demo requires synthetic identity, so names stay invented; only the geography has to be
# plausible.

# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import hashlib
from datetime import date, timedelta

import numpy as np
import pandas as pd

# Master seed for the whole synthetic estate. Change this to regenerate every table in
# 01_topology from scratch. Deliberately distinct from V1's MASTER_SEED (20260602) so the
# two universes cannot be confused for one another.
TOPOLOGY_SEED = 20260915

# Business-key prefix. GS- rather than V1's FAC-: 350 FAC-nnnn keys still exist in
# Operations_LH at different coordinates from a different seed, and reusing the prefix would
# make the two estates indistinguishable in a query result. Nothing downstream of the V1 keys
# is being preserved, so there is no continuity to protect.
FACILITY_ID_PREFIX = "GS"

print(f"TOPOLOGY_SEED      = {TOPOLOGY_SEED}")
print(f"FACILITY_ID_PREFIX = {FACILITY_ID_PREFIX}-nnnn")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Deterministic RNG. get_rng("equipment", facility_id) is reproducible across reruns and
# across notebooks, and is the ONLY way randomness enters 01_topology -- a bare
# np.random.default_rng(n) anywhere here is a bug, because it silently detaches that draw
# from TOPOLOGY_SEED. That is exactly how V1's build_attribution came to reseed every
# facility coordinate from a box that no longer matched the rest of the model.

def _seed(*parts) -> int:
    h = hashlib.sha256("|".join(map(str, (TOPOLOGY_SEED, *parts))).encode()).hexdigest()
    return int(h[:16], 16) % (2 ** 32)


def get_rng(*parts) -> np.random.Generator:
    return np.random.default_rng(_seed(*parts))


def stable_key(*parts) -> int:
    """Deterministic surrogate key derived from a business key, e.g.
    stable_key("area", "GS-0001-A1").

    Unlike facility_sk / equipment_sk / sensor_sk, which are 1..N sequences assigned in
    iteration order, this is a hash of the business key. The difference matters for any
    entity whose count per parent varies: areas are drawn per facility, so a sequential
    counter would renumber every area downstream of any facility whose area count changed,
    orphaning anything keyed to the old numbers. A hash depends only on the business key,
    so an area keeps its key as long as its area_id is regenerated identically.

    TOPOLOGY_SEED is folded in deliberately. A new seed is a new synthetic universe, and
    GS-0001-A1 under one seed is not the same physical area as GS-0001-A1 under another --
    they should not share a surrogate key and silently alias in a downstream join.

    Returns a non-negative 63-bit int, so it fits Spark's bigint without wrapping negative.
    """
    h = hashlib.sha256("|".join(map(str, (TOPOLOGY_SEED, *parts))).encode()).hexdigest()
    return int(h[:16], 16) & 0x7FFF_FFFF_FFFF_FFFF


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km. Vectorised — scalars or arrays."""
    R = 6371.0
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(np.asarray(lat2) - np.asarray(lat1))
    dl = np.radians(np.asarray(lon2) - np.asarray(lon1))
    h = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(np.clip(h, 0, 1)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Geography -------------------------------------------------------------------------
# Four real Permian sub-basin anchors, all comfortably inside CONFIG["bbox"]
# (lat 30.5..33.5, lon -105.0..-101.0). Compare V1, whose second anchor "Texas Site A" sat at
# lat 30.2 -- below the BBOX floor -- and carried 40% of the estate.
#
# Every anchor plus its perimeter radius stays inside the BBOX, so the inside and perimeter
# bands cannot leak past an edge; the clamp in sample_facility_location is a guard, not a
# load-bearing step. This is asserted below, per anchor. Where an anchor has no room for the
# default radii it carries its own tighter pair rather than being moved -- see Northwest
# Shelf, and read radii through anchor_radii(), never as globals.
#
# NORTHWEST SHELF EXISTS TO COVER THE NORTHERN BBOX. Do not remove it as geologically
# arbitrary. Evidence, from running 01a/01b/05 against the three-anchor estate:
#
#   - 32 of 75 plumes attributed to no facility, every one with facilities_in_range = 0.
#     The search found no candidate at all, so this was not scoring or the upwind cone
#     rejecting candidates -- there were none to reject.
#   - Nearest facility to an unattributed plume: min 55.2 km, median 72.1 km, max 128.2 km.
#     Every one beyond attribution_search_radius_km (50 km).
#   - Attributed plumes: nearest facility median 15.3 km, max 49.9 km. The two groups do
#     not overlap at all.
#   - Unattributed plumes sat at median latitude 32.96, attributed at 31.88. The other
#     three anchors are at 31.75, 31.95 and 32.05, so facilities thinned out above ~32.5
#     while the BBOX runs to 33.5.
#   - wind_alignment_deg overlapped almost entirely between the groups (unattributed
#     4.8-82.1 deg, attributed 0.5-88.4 deg, near-identical standard deviations), ruling
#     out wind geometry as the cause.
#
# The attribution logic was behaving correctly; the estate simply did not cover the
# northern third of the footprint. The coverage cell in 01a measures this directly -- run
# it before changing any anchor.
#
# Placed at 32.90 N rather than the 33.0 N this was scoped around: an anchor plus its
# perimeter radius must stay under the clamp at max_lat - EDGE_INSET_DEG = 33.48. At the
# 0.55 perimeter in force when the anchor was added, that capped anchor latitude at 32.93;
# at 33.0 the band would have reached 33.55 and the clamp would have started binding,
# flattening facilities against the BBOX edge and making the clamp load-bearing rather than
# a guard. The radii have since widened and this anchor now carries an override instead.

ANCHORS = {
    "Midland Basin":          {"lat": 32.05, "lon": -102.10, "weight": 0.37},
    "Delaware Basin":         {"lat": 31.75, "lon": -103.70, "weight": 0.33},
    # Northwest Shelf carries its own, tighter radii. At 32.90 N it has only 0.580 deg to
    # the clamp, so the global 0.85 perimeter would be clipped flat against the BBOX ceiling
    # (~17% of its perimeter draws, ~0.9 facilities a run pinned at exactly 33.48). Rather
    # than move the anchor -- it sits where the northern coverage gap is -- or shrink the
    # radii everywhere, this cluster is simply more compressed. That is also the more
    # faithful reading: a shelf edge has less room to spread than a basin interior.
    #
    # Both radii are overridden, not just the perimeter. Setting perimeter to 0.55 alone
    # would equal REGION_RADIUS_DEG and collapse the perimeter band to zero width, pinning
    # every perimeter facility onto an exact ring. The pair preserves the global
    # region:perimeter ratio (0.55/0.85 = 0.647), so the cluster keeps its shape and only
    # its scale changes. 0.57 leaves 0.01 deg of margin to the clamp.
    "Northwest Shelf":        {"lat": 32.90, "lon": -102.20, "weight": 0.18,
                               "region_radius": 0.37, "perimeter_radius": 0.57},
    "Central Basin Platform": {"lat": 31.95, "lon": -102.90, "weight": 0.12},
}

# Widened from 0.30 / 0.55. Four clusters of radius 0.30 left large interstitial voids: at
# 150 facilities over a 3 x 4 degree BBOX -- roughly one per 300 km2 -- concentrating them
# into four islands meant plumes landing between clusters had no facility inside the 50 km
# attribution radius, even after the northern gap was closed. Unattributed plumes stopped
# being directional (median latitude 31.76 against 31.97 for attributed) but their nearest
# facility still ran 51.1 km minimum, 67.4 km median, 95.0 km maximum. Real Permian
# infrastructure is more continuous than four islands, so spreading the estate is the more
# faithful model as well as the one that closes the voids.
# Defaults. An anchor may override either with "region_radius" / "perimeter_radius" when the
# BBOX edge leaves it no room -- see Northwest Shelf above. Read them through
# anchor_radii(name), never as globals, or per-anchor overrides are silently ignored.
REGION_RADIUS_DEG    = 0.55   # ~61 km core
PERIMETER_RADIUS_DEG = 0.85   # outer band


def anchor_radii(anchor_name):
    """(region_radius, perimeter_radius) for one anchor, honouring per-anchor overrides."""
    a = ANCHORS[anchor_name]
    return (a.get("region_radius", REGION_RADIUS_DEG),
            a.get("perimeter_radius", PERIMETER_RADIUS_DEG))

# Band mix. The outside band exists to exercise attribution confidence tiers and the
# NO_FACILITY_IN_RANGE path in 05 -- it is a perimeter case, not a generation bug, so it is
# bounded relative to the BBOX rather than to an anchor.
#
# These are QUOTAS, not draw probabilities: allocate_bands() hands out exact counts, so the
# realised split matches the configured one. Drawing the band per facility instead leaves the
# realised counts to sampling noise -- at n=150 the 5% outside band would swing by roughly
# +/-2 points run to run, which makes the estate harder to reason about for no benefit.
FACILITY_SPLIT = {"inside": 0.75, "perimeter": 0.20, "outside": 0.05}

OUTSIDE_MIN_DEG = 0.05   # nearest an "outside" facility may sit to the BBOX edge
OUTSIDE_MAX_DEG = 0.30   # furthest -- asserted in 01a, ~33 km, still geologically plausible
EDGE_INSET_DEG  = 0.02   # inside/perimeter draws are clamped this far inside each edge

assert abs(sum(FACILITY_SPLIT.values()) - 1.0) < 1e-9, "FACILITY_SPLIT must sum to 1"
assert abs(sum(a["weight"] for a in ANCHORS.values()) - 1.0) < 1e-9, "ANCHOR weights must sum to 1"

MIN_BAND_WIDTH_DEG = 0.05   # perimeter band must be wider than this, or it is a ring

print("Anchors (all inside CONFIG['bbox']):")
print(f"  {'anchor':<24}{'position':>18}{'w':>6}{'region':>8}{'perim':>7}{'lat room':>10}")
for _name, _a in ANCHORS.items():
    _reg, _per = anchor_radii(_name)
    _in = (BBOX["min_lat"] <= _a["lat"] <= BBOX["max_lat"]
           and BBOX["min_lon"] <= _a["lon"] <= BBOX["max_lon"])

    # The perimeter band must fit between the anchor and the clamp, or facilities pile up
    # flat against the BBOX edge and the clamp stops being a guard. Longitude is checked at
    # the anchor's latitude, since the lon offset is divided by cos(lat).
    _lat_room = min(_a["lat"] - (BBOX["min_lat"] + EDGE_INSET_DEG),
                    (BBOX["max_lat"] - EDGE_INSET_DEG) - _a["lat"])
    _lon_half = _per / np.cos(np.radians(_a["lat"]))
    _lon_room = min(_a["lon"] - (BBOX["min_lon"] + EDGE_INSET_DEG),
                    (BBOX["max_lon"] - EDGE_INSET_DEG) - _a["lon"])

    _override = " (override)" if ("region_radius" in _a or "perimeter_radius" in _a) else ""
    _pos = "{:.2f}N {:.2f}W".format(_a["lat"], abs(_a["lon"]))
    print(f"  {_name:<24}{_pos:>18}{_a['weight']:>6.2f}"
          f"{_reg:>8.2f}{_per:>7.2f}{_lat_room:>10.3f}{_override}")

    assert _in, f"anchor {_name} is outside CONFIG['bbox'] -- this is the V1 defect"
    assert _lat_room >= _per, (
        f"anchor {_name} at lat {_a['lat']} leaves only {_lat_room:.3f} deg to the clamp, "
        f"less than its perimeter radius ({_per}). Its perimeter band would be clipped flat "
        f"against the BBOX edge. Either move the anchor inward, or give it a per-anchor "
        f'"region_radius"/"perimeter_radius" pair sized to the room it has.'
    )
    assert _lon_room >= _lon_half, (
        f"anchor {_name} at lon {_a['lon']} leaves only {_lon_room:.3f} deg to the clamp, "
        f"less than the {_lon_half:.3f} deg its perimeter band spans at that latitude."
    )
    # A perimeter radius equal to the region radius collapses the band to a ring, pinning
    # every perimeter facility at exactly that distance. Overriding only one of the pair is
    # the easy way to cause this.
    assert _per - _reg >= MIN_BAND_WIDTH_DEG, (
        f"anchor {_name} has a perimeter band {_per - _reg:.3f} deg wide "
        f"(region {_reg}, perimeter {_per}), under MIN_BAND_WIDTH_DEG "
        f"({MIN_BAND_WIDTH_DEG}). Perimeter facilities would sit on a ring rather than in a "
        "band. Override both radii together, keeping their ratio."
    )
print(f"Band split: {FACILITY_SPLIT}")
print(f"Outside band bounded to {OUTSIDE_MIN_DEG}-{OUTSIDE_MAX_DEG} deg beyond the BBOX edge")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Scale targets ---------------------------------------------------------------------
# 150 facilities, NOT V1's 350. The estate is sized against the detection rate, not against
# V1: roughly 75 plumes over a 30-day window means that at 350 sites most facilities never
# receive a detection and the Facility Operations dashboard page is empty for nearly every
# one of them. 150 raises the share of facilities with at least one nearby plume.
#
# The last cell of 01a measures this directly against gold_plume_catalog -- facilities within
# 25 km of at least one plume, and the plume-count distribution. Read that output before
# changing this number; it is the evidence for whether 150 is right.

N_FACILITIES         = 150
N_OPERATORS          = 12
SENSORS_PER_FACILITY = 4
SENSOR_INTERVAL_HOURS = 4

# Commissioning window for facilities: up to 15 years of history.
HISTORY_YEARS = 15

# A FIXED date, never date.today(). The topology is a slowly-changing dimension: it is
# regenerated deliberately -- when the seed or the scale changes -- not on every run.
# TOPOLOGY_AS_OF bounds commission_date and install_date, so a moving value would age every
# facility and asset by a day on each rerun and make "same seed, same estate" false:
# reproducibility would depend on WHEN the notebook ran, not on TOPOLOGY_SEED alone.
# This is the defect V1 carried as REAL_PLUME_END = date.today() in config_and_seeds.
# Bump this by hand, as a deliberate act, when the estate should move forward.
TOPOLOGY_AS_OF = date(2026, 9, 15)

assert TOPOLOGY_AS_OF <= date.today(), (
    f"TOPOLOGY_AS_OF ({TOPOLOGY_AS_OF}) is in the future relative to today ({date.today()}). "
    "Commission and install dates would be stamped ahead of real time, and 01b's "
    "'no asset postdates the as-of date' check would pass on dates that have not happened."
)

_age_days = (date.today() - TOPOLOGY_AS_OF).days
if _age_days > 365:
    print(f"NOTE  TOPOLOGY_AS_OF is {_age_days} days old. Asset ages are frozen at that date")
    print("      by design; bump it deliberately if the estate should move forward.")

print(f"{N_FACILITIES} facilities, {SENSORS_PER_FACILITY} sensors per facility")
print(f"as-of date: {TOPOLOGY_AS_OF}  (fixed constant, not date.today())")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Facility naming -------------------------------------------------------------------
# PLACES is V1's list, unchanged -- it is the part of the V1 generator worth keeping.
PLACES = ["Midland", "Odessa", "Pecos", "Monahans", "Kermit", "Wink", "Crane", "Andrews",
          "Stanton", "Big Spring", "Fort Stockton", "Reeves County", "Loving", "Mentone",
          "Orla", "Sand Hills", "Goldsmith", "Notrees", "Coyanosa", "Imperial"]

# Type -> the descriptors that may name a facility of that type. The union of these lists is
# exactly V1's FAC_DESCRIPTORS set; what is new is that each descriptor belongs to exactly
# one type. Name is built FROM the type (01a draws the type first), so
# "Odessa Processing Plant" can only ever be typed Gas Processing Plant.
TYPE_DESCRIPTORS = {
    "Compression Station":    ["Compression Station", "Booster Station"],
    "Gathering System":       ["Gathering Hub", "Collection Facility"],
    "Gas Processing Plant":   ["Processing Plant", "Treating Facility"],
    "Tank Battery":           ["Tank Battery"],
    "Central Delivery Point": ["Central Delivery Point"],
}

FACILITY_TYPE_WEIGHTS = {
    "Compression Station":    0.24,
    "Gathering System":       0.26,
    "Gas Processing Plant":   0.12,
    "Tank Battery":           0.28,
    "Central Delivery Point": 0.10,
}

FACILITY_TYPES = list(TYPE_DESCRIPTORS)

assert set(FACILITY_TYPE_WEIGHTS) == set(TYPE_DESCRIPTORS), \
    "FACILITY_TYPE_WEIGHTS and TYPE_DESCRIPTORS must cover the same types"
assert abs(sum(FACILITY_TYPE_WEIGHTS.values()) - 1.0) < 1e-9, \
    "FACILITY_TYPE_WEIGHTS must sum to 1"


def descriptor_to_type():
    """Reverse of TYPE_DESCRIPTORS: descriptor -> the single type that owns it."""
    rev = {}
    for ftype, descs in TYPE_DESCRIPTORS.items():
        for d in descs:
            rev.setdefault(d, []).append(ftype)
    return rev


def assert_descriptor_map_unique():
    """No descriptor may belong to two facility types.

    Construction in 01a already guarantees name/type agreement, so this cannot fail today.
    It is kept because the guarantee is only as good as the map: if someone later adds
    "Processing Plant" under Gathering System as well, names and types silently start
    disagreeing again exactly as they did in V1, and every row would still pass a
    name-derived-from-type check. This is the assertion that catches that edit.
    """
    rev = descriptor_to_type()
    dupes = {d: ts for d, ts in rev.items() if len(ts) > 1}
    assert not dupes, (
        "descriptor(s) mapped to more than one facility_type -- name/type agreement is no "
        f"longer guaranteed: {dupes}"
    )
    return rev


_rev = assert_descriptor_map_unique()
print(f"{len(FACILITY_TYPES)} facility types, {len(_rev)} descriptors, each owned by one type")
print(f"{len(PLACES)} places -> {len(PLACES) * len(_rev)} distinct name combinations available"
      f" for {N_FACILITIES} facilities")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Asset taxonomy --------------------------------------------------------------------
# Carried over from V1 unchanged. leak_propensity and crit_bias are consumed by the
# downstream episode/maintenance generators, not by 01_topology itself, but they belong with
# the type definition rather than in whichever notebook happens to need them first.

EQUIPMENT_TYPES = {
    "Compressor":       {"life": 20, "insp_days": 90,  "leak_propensity": 1.00, "crit_bias": 0.8},
    "Valve":            {"life": 25, "insp_days": 180, "leak_propensity": 0.70, "crit_bias": 0.4},
    "Separator":        {"life": 25, "insp_days": 180, "leak_propensity": 0.55, "crit_bias": 0.5},
    "Storage Tank":     {"life": 30, "insp_days": 365, "leak_propensity": 0.85, "crit_bias": 0.6},
    "Flare":            {"life": 20, "insp_days": 180, "leak_propensity": 0.60, "crit_bias": 0.7},
    "Pipeline Segment": {"life": 40, "insp_days": 365, "leak_propensity": 0.65, "crit_bias": 0.6},
    "Pump":             {"life": 15, "insp_days": 120, "leak_propensity": 0.75, "crit_bias": 0.5},
    "Metering Station": {"life": 20, "insp_days": 180, "leak_propensity": 0.45, "crit_bias": 0.4},
}

# reliability_index < 1 = more reliable (lowers downstream hazard)
MANUFACTURERS = {
    "Ariel": 0.85, "Caterpillar": 0.90, "Waukesha": 1.00, "Cameron": 0.95,
    "Baker Hughes": 0.92, "Emerson": 0.88, "Honeywell": 0.90, "Flowserve": 1.05,
}

SENSOR_TYPES = {"Point": 0.50, "OGI": 0.20, "CMS": 0.30}
assert abs(sum(SENSOR_TYPES.values()) - 1.0) < 1e-9, "SENSOR_TYPES weights must sum to 1"

EQUIPMENT_TYPE_NAMES = list(EQUIPMENT_TYPES)   # fixed order; weight vectors align to it

print(f"{len(EQUIPMENT_TYPES)} equipment types, {len(MANUFACTURERS)} manufacturers,"
      f" {len(SENSOR_TYPES)} sensor types")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Asset mix and asset count, by facility type ----------------------------------------
# Drawing equipment type uniformly gives every facility the same mix -- a tank battery ends
# up with as many compressors as a gas processing plant. The mix should follow what the site
# is for, so each facility_type carries its own weight vector over the eight equipment types.
#
# No weight is zero. A rare-but-possible combination gets a small weight (0.02) rather than
# being excluded, so no equipment type disappears from the estate entirely and downstream
# code never has to special-case an empty category.

TYPE_EQUIPMENT_WEIGHTS = {
    # Compressor  Valve  Separator  Storage Tank  Flare  Pipeline Seg  Pump  Metering
    "Gas Processing Plant": {
        "Compressor": 0.26, "Separator": 0.24, "Valve": 0.13, "Flare": 0.10,
        "Metering Station": 0.10, "Pump": 0.10, "Storage Tank": 0.04, "Pipeline Segment": 0.03,
    },
    "Compression Station": {
        "Compressor": 0.40, "Pump": 0.16, "Valve": 0.16, "Metering Station": 0.11,
        "Separator": 0.07, "Pipeline Segment": 0.05, "Flare": 0.03, "Storage Tank": 0.02,
    },
    "Gathering System": {
        "Pipeline Segment": 0.34, "Valve": 0.26, "Metering Station": 0.14, "Separator": 0.12,
        "Compressor": 0.06, "Pump": 0.04, "Storage Tank": 0.02, "Flare": 0.02,
    },
    "Tank Battery": {
        "Storage Tank": 0.40, "Separator": 0.18, "Valve": 0.15, "Flare": 0.11,
        "Pump": 0.07, "Metering Station": 0.05, "Pipeline Segment": 0.02, "Compressor": 0.02,
    },
    "Central Delivery Point": {
        "Metering Station": 0.34, "Valve": 0.22, "Pipeline Segment": 0.18, "Compressor": 0.10,
        "Separator": 0.07, "Pump": 0.05, "Storage Tank": 0.02, "Flare": 0.02,
    },
}

# Asset count scales with the facility's purpose too: a processing plant is a bigger site
# than a tank battery, and a flat 10-50 across all types washed that out.
EQUIP_COUNT_BY_TYPE = {
    "Gas Processing Plant":   (25, 50),
    "Compression Station":    (15, 35),
    "Gathering System":       (10, 25),
    "Tank Battery":           (8, 20),
    "Central Delivery Point": (12, 28),
}

# Global bound, derived rather than declared, for any validation that wants one number.
# Generation is driven by EQUIP_COUNT_BY_TYPE, never by this.
EQUIP_PER_FACILITY = (
    min(lo for lo, _ in EQUIP_COUNT_BY_TYPE.values()),
    max(hi for _, hi in EQUIP_COUNT_BY_TYPE.values()),
)

# --- validation ---------------------------------------------------------------------------
assert set(TYPE_EQUIPMENT_WEIGHTS) == set(FACILITY_TYPES), (
    "TYPE_EQUIPMENT_WEIGHTS must cover exactly the facility types in TYPE_DESCRIPTORS; "
    f"missing {set(FACILITY_TYPES) - set(TYPE_EQUIPMENT_WEIGHTS)}, "
    f"unexpected {set(TYPE_EQUIPMENT_WEIGHTS) - set(FACILITY_TYPES)}"
)
assert set(EQUIP_COUNT_BY_TYPE) == set(FACILITY_TYPES), (
    "EQUIP_COUNT_BY_TYPE must cover exactly the facility types in TYPE_DESCRIPTORS"
)

for _ft, _w in TYPE_EQUIPMENT_WEIGHTS.items():
    assert set(_w) == set(EQUIPMENT_TYPES), (
        f"{_ft}: weight vector must name every equipment type; "
        f"missing {set(EQUIPMENT_TYPES) - set(_w)}, unexpected {set(_w) - set(EQUIPMENT_TYPES)}"
    )
    assert abs(sum(_w.values()) - 1.0) < 1e-9, \
        f"{_ft}: equipment weights sum to {sum(_w.values()):.4f}, must be 1"
    assert all(v > 0 for v in _w.values()), (
        f"{_ft}: every equipment type needs a non-zero weight -- use a small weight for "
        "rare-but-possible, so no category vanishes from the estate"
    )

for _ft, (_lo, _hi) in EQUIP_COUNT_BY_TYPE.items():
    assert 0 < _lo <= _hi, f"{_ft}: invalid asset range ({_lo}, {_hi})"


def equipment_weights(facility_type):
    """Weight vector over EQUIPMENT_TYPE_NAMES, in that fixed order."""
    w = TYPE_EQUIPMENT_WEIGHTS[facility_type]
    return [w[t] for t in EQUIPMENT_TYPE_NAMES]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Process areas ----------------------------------------------------------------------
# The hierarchy is facility -> area -> asset. Real sites are organised into process areas,
# SCADA tags are named and grouped by area, and operations teams triage by area, so this
# level has to exist before any tag or telemetry layer sits on top of it.
#
# Per facility type: which areas it may contain, how likely each is, and which equipment
# types belong there. Consumed by 01c_build_area_topology.
#
#   weight      relative likelihood of an optional area being drawn
#   mandatory   always present -- the areas without which the site does not function
#   repeatable  may appear more than once at one facility (Compression Train A, B, ...);
#               only used to pad out to the target count after distinct types are exhausted
#   equipment   equipment types this area accepts
#
# TWO CONSTRAINTS SHAPED THIS MAPPING, and both are worth knowing before editing it:
#
# 1. TYPE_EQUIPMENT_WEIGHTS gives EVERY equipment type a non-zero weight at EVERY facility
#    type -- deliberately, so no category vanishes from the estate. So any facility can hold
#    any equipment type, and an asset whose type no area accepts has to go somewhere. That
#    is what the fallback in 01c is for. Keeping the fallback rate low means the mandatory
#    areas alone should accept most of the equipment weight for their facility type.
#
# 2. An equipment type accepted by only one area type is fragile: if that area is optional
#    and is not drawn, every asset of that type at that facility falls back. High-weight
#    equipment is therefore accepted by two or three areas wherever that is physically
#    honest -- a flare knockout drum really is a separator, tank vapour really does go to a
#    flare, and valves and pipe runs really are everywhere.
#
# Divergences from the shape this was scoped around, with reasons:
#   - Gathering System gains "Field Compression" and moves from 2-4 areas to 3-5 with
#     Metering mandatory. Without it, Compressor (6%), Flare (2%) and Metering Station (14%)
#     had no home at a gathering site -- 22% of its assets falling back, well over the
#     threshold.
#   - Central Delivery Point gains "Utilities". Metering and Custody Transfer between them
#     accept nothing that is not metering, valve, pipe or separator, leaving Compressor
#     (10%), Pump (5%), Storage Tank (2%) and Flare (2%) homeless.
#   - Tank Farm accepts Flare as well as Tank Battery's Vapour Recovery area. Flare carries
#     11% weight at a tank battery and Vapour Recovery is optional, so on its own it left a
#     large fallback whenever that area was not drawn.

AREA_TYPES = {
    "Gas Processing Plant": {
        "Inlet Separation":  {"weight": 0.20, "mandatory": True,  "repeatable": False,
                              "equipment": ["Separator", "Valve", "Pipeline Segment", "Pump"]},
        "Compression Train": {"weight": 0.20, "mandatory": True,  "repeatable": True,
                              "equipment": ["Compressor", "Valve", "Pump", "Separator",
                                            "Metering Station"]},
        "Treating":          {"weight": 0.22, "mandatory": False, "repeatable": False,
                              "equipment": ["Separator", "Valve", "Pump", "Storage Tank",
                                            "Flare"]},
        "Storage":           {"weight": 0.18, "mandatory": False, "repeatable": False,
                              "equipment": ["Storage Tank", "Pump", "Valve", "Flare"]},
        "Metering":          {"weight": 0.20, "mandatory": False, "repeatable": False,
                              "equipment": ["Metering Station", "Valve", "Pipeline Segment"]},
        "Flare":             {"weight": 0.20, "mandatory": False, "repeatable": False,
                              "equipment": ["Flare", "Valve", "Pipeline Segment", "Separator"]},
    },
    "Compression Station": {
        "Compression Train": {"weight": 0.25, "mandatory": True,  "repeatable": True,
                              "equipment": ["Compressor", "Valve", "Pump", "Separator"]},
        "Suction Scrubbing": {"weight": 0.20, "mandatory": True,  "repeatable": False,
                              "equipment": ["Separator", "Valve", "Pump", "Storage Tank"]},
        "Metering":          {"weight": 0.28, "mandatory": False, "repeatable": False,
                              "equipment": ["Metering Station", "Valve", "Pipeline Segment"]},
        "Utilities":         {"weight": 0.27, "mandatory": False, "repeatable": False,
                              "equipment": ["Pump", "Valve", "Storage Tank", "Flare",
                                            "Metering Station", "Pipeline Segment"]},
    },
    "Gathering System": {
        "Gathering Lines":   {"weight": 0.20, "mandatory": True,  "repeatable": True,
                              "equipment": ["Pipeline Segment", "Valve"]},
        "Separation":        {"weight": 0.20, "mandatory": True,  "repeatable": False,
                              "equipment": ["Separator", "Valve", "Storage Tank", "Pump"]},
        "Metering":          {"weight": 0.20, "mandatory": True,  "repeatable": False,
                              "equipment": ["Metering Station", "Valve", "Pipeline Segment"]},
        "Field Compression": {"weight": 0.55, "mandatory": False, "repeatable": False,
                              "equipment": ["Compressor", "Valve", "Flare", "Separator",
                                            "Pump"]},
        "Pigging":           {"weight": 0.45, "mandatory": False, "repeatable": False,
                              "equipment": ["Pipeline Segment", "Valve", "Separator"]},
    },
    "Tank Battery": {
        "Separation":        {"weight": 0.25, "mandatory": True,  "repeatable": False,
                              "equipment": ["Separator", "Valve", "Pump"]},
        "Tank Farm":         {"weight": 0.25, "mandatory": True,  "repeatable": True,
                              "equipment": ["Storage Tank", "Valve", "Pump",
                                            "Pipeline Segment", "Flare"]},
        "Vapour Recovery":   {"weight": 0.30, "mandatory": False, "repeatable": False,
                              "equipment": ["Compressor", "Valve", "Flare", "Separator"]},
        "Loadout":           {"weight": 0.20, "mandatory": False, "repeatable": False,
                              "equipment": ["Pump", "Valve", "Metering Station",
                                            "Pipeline Segment"]},
    },
    "Central Delivery Point": {
        "Metering":          {"weight": 0.25, "mandatory": True,  "repeatable": True,
                              "equipment": ["Metering Station", "Valve", "Pipeline Segment"]},
        "Custody Transfer":  {"weight": 0.25, "mandatory": True,  "repeatable": False,
                              "equipment": ["Metering Station", "Valve", "Pipeline Segment",
                                            "Separator", "Pump"]},
        "Utilities":         {"weight": 0.60, "mandatory": False, "repeatable": False,
                              "equipment": ["Pump", "Valve", "Storage Tank", "Flare",
                                            "Compressor"]},
        "Pigging":           {"weight": 0.40, "mandatory": False, "repeatable": False,
                              "equipment": ["Pipeline Segment", "Valve", "Separator"]},
    },
}

# Areas per facility, by facility type. Sized so assets divide sensibly: a Tank Battery
# holding 8 assets must not be split across 6 areas. Against EQUIP_COUNT_BY_TYPE these give
# roughly 4-8 assets per area at the small end of each range, which is a plausible area.
#
# Gathering System moved from the 2-4 this was scoped around to 3-5, because Metering had to
# become mandatory (see above) and three mandatory areas cannot fit in a 2-area minimum.
AREAS_PER_FACILITY = {
    "Gas Processing Plant":   (4, 6),
    "Compression Station":    (3, 5),
    "Gathering System":       (3, 5),
    "Tank Battery":           (2, 4),
    "Central Delivery Point": (2, 3),
}

# Criticality by area type. Areas holding rotating equipment or vapour handling rank higher;
# metering and pigging are consequential but not immediately hazardous.
AREA_CRITICALITY_WEIGHTS = {
    "Compression Train": {"Critical": 0.35, "High": 0.40, "Medium": 0.20, "Low": 0.05},
    "Field Compression": {"Critical": 0.30, "High": 0.40, "Medium": 0.25, "Low": 0.05},
    "Vapour Recovery":   {"Critical": 0.30, "High": 0.40, "Medium": 0.25, "Low": 0.05},
    "Treating":          {"Critical": 0.25, "High": 0.40, "Medium": 0.30, "Low": 0.05},
    "Flare":             {"Critical": 0.25, "High": 0.35, "Medium": 0.30, "Low": 0.10},
    "Inlet Separation":  {"Critical": 0.20, "High": 0.40, "Medium": 0.30, "Low": 0.10},
    "Separation":        {"Critical": 0.15, "High": 0.35, "Medium": 0.35, "Low": 0.15},
    "Suction Scrubbing": {"Critical": 0.15, "High": 0.35, "Medium": 0.35, "Low": 0.15},
    "Tank Farm":         {"Critical": 0.15, "High": 0.30, "Medium": 0.40, "Low": 0.15},
    "Storage":           {"Critical": 0.10, "High": 0.30, "Medium": 0.40, "Low": 0.20},
    "Custody Transfer":  {"Critical": 0.10, "High": 0.35, "Medium": 0.40, "Low": 0.15},
    "Loadout":           {"Critical": 0.05, "High": 0.25, "Medium": 0.45, "Low": 0.25},
    "Metering":          {"Critical": 0.05, "High": 0.25, "Medium": 0.45, "Low": 0.25},
    "Gathering Lines":   {"Critical": 0.05, "High": 0.25, "Medium": 0.45, "Low": 0.25},
    "Pigging":           {"Critical": 0.05, "High": 0.20, "Medium": 0.45, "Low": 0.30},
    "Utilities":         {"Critical": 0.05, "High": 0.20, "Medium": 0.45, "Low": 0.30},
}

# Area offset from the facility centre, in metres.
#
# These coordinates exist for PLAUSIBILITY and for future asset-level attribution. They are
# NOT something the detection layer can resolve and must never be presented as such: a
# TROPOMI pixel is ~5.5 x 7.0 km, so an entire facility -- every area in it -- sits inside a
# fraction of one pixel. Area-level geography is three orders of magnitude below the
# instrument's resolving power. Any dashboard that appears to attribute a plume to an area
# rather than a site is showing an artefact of this offset, not a measurement.
AREA_OFFSET_MIN_M = 100.0
AREA_OFFSET_MAX_M = 300.0
AREA_OFFSET_MAX_ASSERT_M = 500.0   # asserted in 01c; headroom over the draw above

# --- validation ---------------------------------------------------------------------------
assert set(AREA_TYPES) == set(FACILITY_TYPES), (
    "AREA_TYPES must cover exactly the facility types in TYPE_DESCRIPTORS; "
    f"missing {set(FACILITY_TYPES) - set(AREA_TYPES)}, "
    f"unexpected {set(AREA_TYPES) - set(FACILITY_TYPES)}"
)
assert set(AREAS_PER_FACILITY) == set(FACILITY_TYPES), \
    "AREAS_PER_FACILITY must cover exactly the facility types in TYPE_DESCRIPTORS"

AREA_TYPE_NAMES = sorted({a for spec in AREA_TYPES.values() for a in spec})
assert set(AREA_CRITICALITY_WEIGHTS) >= set(AREA_TYPE_NAMES), (
    "AREA_CRITICALITY_WEIGHTS is missing area type(s): "
    f"{sorted(set(AREA_TYPE_NAMES) - set(AREA_CRITICALITY_WEIGHTS))}"
)
for _at, _w in AREA_CRITICALITY_WEIGHTS.items():
    assert abs(sum(_w.values()) - 1.0) < 1e-9, \
        f"{_at}: criticality weights sum to {sum(_w.values()):.4f}, must be 1"

for _ft, _spec in AREA_TYPES.items():
    _lo, _hi = AREAS_PER_FACILITY[_ft]
    _mand = [a for a, s in _spec.items() if s["mandatory"]]
    _repeatable = [a for a, s in _spec.items() if s["repeatable"]]

    assert 0 < _lo <= _hi, f"{_ft}: invalid area range ({_lo}, {_hi})"
    # The mandatory set has to fit inside the minimum, or a facility cannot be built.
    assert len(_mand) <= _lo, (
        f"{_ft}: {len(_mand)} mandatory areas ({', '.join(_mand)}) but AREAS_PER_FACILITY "
        f"minimum is {_lo}. Raise the minimum or make an area optional."
    )
    # The maximum has to be reachable, else the target count can never be met.
    assert len(_spec) >= _hi or _repeatable, (
        f"{_ft}: only {len(_spec)} area types defined but the range reaches {_hi}, and no "
        "area is repeatable, so a facility could not be filled to its target count."
    )
    for _a, _s in _spec.items():
        assert _s["equipment"], f"{_ft}/{_a}: no equipment types accepted"
        _unknown = set(_s["equipment"]) - set(EQUIPMENT_TYPES)
        assert not _unknown, f"{_ft}/{_a}: unknown equipment type(s) {sorted(_unknown)}"
        assert _s["weight"] > 0, f"{_ft}/{_a}: weight must be positive"


def mandatory_equipment_cover(facility_type):
    """Share of a facility type's equipment weight its MANDATORY areas alone accept.

    Anything outside this is at risk of falling back to the primary area whenever the
    optional area that would have accepted it is not drawn. A diagnostic, not a guarantee:
    the realised rate depends on which optional areas each facility draws, and 01c measures
    that directly and fails above 10%.
    """
    spec = AREA_TYPES[facility_type]
    covered = {e for a, s in spec.items() if s["mandatory"] for e in s["equipment"]}
    return sum(w for t, w in TYPE_EQUIPMENT_WEIGHTS[facility_type].items() if t in covered)


print("process areas by facility type:")
# (area summary printed below; SCADA tag config follows in the next cell)
print(f"  {'facility_type':<24}{'areas':>8}{'types':>7}{'mandatory':>11}"
      f"{'mand. equip cover':>19}")
for _ft in FACILITY_TYPES:
    _lo, _hi = AREAS_PER_FACILITY[_ft]
    _spec = AREA_TYPES[_ft]
    _mand = [a for a, s in _spec.items() if s["mandatory"]]
    print(f"  {_ft:<24}{f'{_lo}-{_hi}':>8}{len(_spec):>7}{len(_mand):>11}"
          f"{mandatory_equipment_cover(_ft):>18.0%}")
print()
print("  'mand. equip cover' is the share of that facility type's equipment weight the")
print("  mandatory areas alone accept. The rest depends on which optional areas are drawn;")
print("  01c measures the realised fallback rate and fails above 10%.")
print()
print(f"  area offset {AREA_OFFSET_MIN_M:.0f}-{AREA_OFFSET_MAX_M:.0f} m from facility centre"
      f" (asserted under {AREA_OFFSET_MAX_ASSERT_M:.0f} m)")
print(f"  {len(AREA_TYPE_NAMES)} distinct area types: {', '.join(AREA_TYPE_NAMES)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- SCADA instrumentation ---------------------------------------------------------------
# dim_scada_tag is a SEPARATE registry from dim_sensor. dim_sensor holds the 600 CH4
# detectors and keeps its contract untouched, because gold.sensor_telemetry depends on its
# schema; this registry holds process measurements (pressure, flow, temperature, level,
# vibration, valve position, rpm) and is consumed by the telemetry notebook that follows.
#
# INSTRUMENTATION POLICY. Not every asset is instrumented, for two reasons that point the
# same way. Real fields meter compressors, separators, tanks and metering runs heavily and
# barely instrument valves or pipe runs at all -- and tag count is the primary control on
# telemetry volume. At 15-minute cadence each 1,000 tags costs roughly 2.9M rows per 30 days
# (1000 * 96 * 30), so the cap below is a volume decision as much as a realism one.

# An asset is instrumented if its type is instrumentable AND it clears the criticality
# bar, capped per facility. The cap is what bounds telemetry volume.
MAX_INSTRUMENTED_ASSETS_PER_FACILITY = 6
INSTRUMENTABLE_CRITICALITY = {"Critical", "High"}

# Rank order for selecting which eligible assets get instrumented, once the criticality bar
# and the per-facility cap bind. Lower sorts first. Compressors are the most consequential
# rotating equipment on a gathering system and the likeliest source of both fugitive and
# combustion emissions, so they are instrumented before anything else competing for a slot.
INSTRUMENT_PRIORITY = {
    "Compressor":       0,
    "Separator":        1,
    "Storage Tank":     2,
    "Metering Station": 3,
    "Flare":            4,
    "Pump":             5,
}
CRITICALITY_RANK = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}

# Tiering. Hot tags are chosen by ASSET CRITICALITY, not at random: the most critical assets
# get the high-frequency treatment, which is how a real historian is configured.
HOT_TAG_SHARE = 0.25          # share of tags sampled at high frequency
HOT_INTERVAL_SECONDS = 300
STANDARD_INTERVAL_SECONDS = 900

# Storage estimate input. An ASSUMPTION, not a measurement -- see the volume table in 01d.
# A narrow telemetry row is tag_sk (bigint 8) + ts (timestamp 8) + value (double 8) + a
# quality byte plus Parquet overhead; Delta's columnar encoding and run-length compression
# on the tag_sk and quality columns take the effective figure well below the raw 25 bytes.
ASSUMED_BYTES_PER_TELEMETRY_ROW = 48

# Width of the process 02b generates, as a fraction of each tag's normal half-band. It lives
# here rather than in 02b because it is the estate's process model, and because the alarm
# limits below are expressed against it -- the assertion that keeps them in reach needs both
# numbers in the same file. 02b and 02d both read it from here.
#
# At 0.33 the normal band edge sits at 3.03 sigma, so a healthy tag leaves its normal band a
# fraction of a percent of the time. That is what "normal band" should mean, and it is what
# makes annunciation alarms possible at all: the previous 0.25 put the band edge at 4.0 sigma,
# where the process never crossed it and 02d's alarm table came out effectively empty.
TELEMETRY_PROCESS_SD_FRACTION = 0.33
assert 0.0 < TELEMETRY_PROCESS_SD_FRACTION < 1.0

# Raw telemetry retention window, consumed by 02b_gen_scada_telemetry. A backfill generates
# the trailing TELEMETRY_RAW_DAYS days from TOPOLOGY_AS_OF; an incremental run takes its
# window from the table's own watermark instead. It lives here rather than in 02b because it
# is a VOLUME knob, and the three others that set volume -- HOT_TAG_SHARE, the two cadences
# and MAX_INSTRUMENTED_ASSETS_PER_FACILITY -- are already here. At the current estate that is
#   991 hot x 288 slots/day + 2,974 standard x 96 slots/day = 570,912 rows/day
# so 30 days is ~17.1M rows across both tiers.
#
# It must not exceed STATE_HISTORY_DAYS: every reading has to fall inside an interval of
# fact_asset_state, and none exist before that anchor.
TELEMETRY_RAW_DAYS = 30

# Volume guard for 02b, the sibling of MAX_STATE_ROWS_PER_30D below. 02b projects its row
# count from the tag tiers and the cadences BEFORE generating anything and fails against
# this, so an over-scaled estate refuses to start rather than falling over part way through
# a 17M-row write on a demo capacity.
MAX_TELEMETRY_ROWS_PER_RUN = 25_000_000

assert TELEMETRY_RAW_DAYS > 0, "TELEMETRY_RAW_DAYS must be positive"
assert MAX_TELEMETRY_ROWS_PER_RUN > 0, "MAX_TELEMETRY_ROWS_PER_RUN must be positive"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Tag taxonomy -------------------------------------------------------------------------
# Per instrumentable equipment type, the tags it carries. Engineering values are for Permian
# gathering and field compression.
#
# Each template carries:
#   measurement_type  one of the nine types dim_scada_tag allows
#   uom               psig, mscfd, bpd, degF, percent, in/s, rpm, inH2O, ratio, state
#   isa               ISA-5.1 instrument code used to build the tag_id
#   normal_min/max    the band the process sits in when healthy
#   alarm_lo/lolo     warning and trip below normal; None where the measurement has no
#   alarm_hi/hihi     meaningful low or high trip (vibration has no low alarm, a flare
#                     header has no low-flow trip)
#   resolution        smallest reportable increment of the instrument
#   noise_sigma       per-reading measurement noise, in uom
#   drift_per_year    calibration drift, in uom per year
#
# COMBUSTION TAGS -- pilot_flame, stack_temperature, air_fuel_ratio -- exist for a specific
# downstream reason and should not be trimmed as decoration. 04b_Multi_Gas_Cross_Correlation
# separates Fugitive Leak from Incomplete Combustion using the NO2 signature: combustion
# produces NO2 alongside CH4, a cold fugitive leak does not. For that distinction to be
# legible in the operational data rather than only in the satellite product, the SCADA layer
# needs tags that can show the combustion case happening -- a flare pilot dropping out, stack
# temperature falling as the flame dies, air-fuel ratio going rich on a compressor. Without
# them the enterprise layer cannot corroborate 04b's classification at all.

# ---- Alarm annunciation limits -------------------------------------------------------------
# THESE ARE ANNUNCIATION LIMITS, NOT EQUIPMENT PROTECTION TRIPS. The distinction is the whole
# reason this table changed.
#
# The original values were protection trips: the pressure at which a relief valve lifts, the
# vibration at which a machine is shut down to save it. Those are set from the equipment's
# damage threshold and are meant to be reached almost never -- against the process 02b
# generates they sat 4.4 to 76 sigma from centre, a median of 7, and nothing ever came close.
# 02d derives alarms by scanning telemetry against these limits, so the alarm table was
# effectively empty: 0.3 alarms per facility-month against a 10-60 design band.
#
# An annunciation limit is a different instrument. It is the point at which an operator wants
# to be told something is drifting, and it is MEANT to be crossed by ordinary process
# excursions several times a month. So the limits below are derived from the process width
# rather than from the equipment:
#
#     alarm_hi   = centre + ALARM_WARN_HALF_BANDS * half_band   (= normal_max exactly)
#     alarm_hihi = centre + ALARM_TRIP_HALF_BANDS * half_band
#     alarm_lo   = centre - ALARM_WARN_HALF_BANDS * half_band   (= normal_min exactly)
#     alarm_lolo = max(centre - ALARM_TRIP_HALF_BANDS * half_band,
#                      ALARM_TRIP_LOW_FLOOR_FRACTION * normal_min)
#
# Setting the warning limits at exactly the normal band edge is deliberate and is the cleanest
# statement of what the band means: [normal_min, normal_max] is the band inside which nothing
# is annunciated, so a reading outside it is by definition abnormal. In units of the process
# sd that 02b generates, TELEMETRY_PROCESS_SD_FRACTION * half_band, that puts every warning
# limit at 3.03 sigma and every trip limit at 3.3 to 4.2 sigma.
#
# The low trip is floored at half the normal minimum. Without that floor the arithmetic sends
# several flows and the tank vapour pressure to a NEGATIVE limit, which then clamps to zero and
# can never be reached -- a dead limit is worse than a shallow one.
#
# The numbers are written out rather than computed so an engineer reads real psig and degF
# values, but the relationship is enforced by ALARM_WARN_SIGMA_BAND below: an edit that pushes
# a limit back out to a protection trip fails the build instead of silently emptying 02d.
#
# Where a physical trip genuinely differs, that is now visible rather than conflated.
# thief_hatch_position is the clearest case: the annunciation limit is 2% open, because the
# hatch is normally shut and 2% is already abnormal, while the physical "hatch is standing
# open" condition is nearer 20%. The first belongs here; the second belongs to whatever
# safety system actually acts on it.
ALARM_WARN_HALF_BANDS = 1.00       # alarm_hi/alarm_lo, in half-bands from centre
ALARM_TRIP_HALF_BANDS = 1.35       # alarm_hihi/alarm_lolo
ALARM_TRIP_LOW_FLOOR_FRACTION = 0.50

# Where those limits must land, in units of the generated process sd. The guard that keeps a
# future edit from putting protection trips back.
ALARM_WARN_SIGMA_BAND = (2.5, 4.0)
ALARM_TRIP_SIGMA_BAND = (3.0, 5.0)

TAG_TEMPLATES = {
    "Compressor": [
        # name,                measurement_type, uom,    isa,  n_min,  n_max,  lo,     lolo,   hi,     hihi,   res,   noise, drift
        ("suction_pressure",     "pressure",       "psig",  "PT",     40.0,  120.0,   40.0,   26.0,  120.0,  134.0,   0.1,   0.8,1.5),
        ("discharge_pressure",   "pressure",       "psig",  "PT",    800.0, 1200.0,  800.0,  730.0, 1200.0, 1270.0,   1.0,   5.0,12.0),
        ("suction_temp",         "temperature",    "degF",  "TT",     60.0,  110.0,   60.0,   51.2,  110.0,  118.8,   0.1,   0.5,1.0),
        ("discharge_temp",       "temperature",    "degF",  "TT",    180.0,  280.0,  180.0,  162.5,  280.0,  297.5,   0.1,   1.5,2.0),
        ("rpm",                  "rpm",            "rpm",   "ST",    900.0, 1200.0,  900.0,  848.0, 1200.0, 1252.0,   1.0,   4.0,3.0),
        ("vibration",            "vibration",      "in/s",  "VT",     0.05,   0.25,   None,   None,   0.25,  0.285, 0.001,  0.01,0.02),
        ("flow",                 "flow",           "mscfd", "FT",   1500.0, 4500.0, 1500.0,  975.0, 4500.0, 5025.0,   1.0,  35.0,60.0),
        ("seal_gas_pressure",    "pressure",       "psig",  "PT",     45.0,   90.0,   45.0,   37.1,   90.0,   97.9,   0.1,   0.6,1.2),
        ("air_fuel_ratio",       "air_fuel_ratio", "ratio", "AT",     14.0,   17.5,   14.0,  13.39,   17.5,  18.11,  0.01,  0.08,0.15),
    ],
    "Separator": [
        ("inlet_pressure",       "pressure",       "psig",  "PT",     60.0,  260.0,   60.0,   30.0,  260.0,  295.0,   0.1,   1.2,2.5),
        ("level",                "level",          "percent","LT",     30.0,   70.0,   30.0,   23.0,   70.0,   77.0,   0.1,   0.6,1.0),
        ("temperature",          "temperature",    "degF",  "TT",     70.0,  130.0,   70.0,   59.5,  130.0,  140.5,   0.1,   0.5,1.0),
        ("gas_flow",             "flow",           "mscfd", "FT",    300.0, 2500.0,  300.0,  150.0, 2500.0, 2885.0,   1.0,  20.0,45.0),
        ("liquid_flow",          "flow",           "bpd",   "FT",     50.0,  600.0,   50.0,   25.0,  600.0,  696.0,   1.0,   6.0,12.0),
    ],
    "Storage Tank": [
        ("level",                "level",          "percent","LT",     20.0,   80.0,   20.0,   10.0,   80.0,   90.5,   0.1,   0.4,0.8),
        ("vapour_pressure",      "pressure",       "psig",  "PT",      0.5,    6.0,    0.5,   0.25,    6.0,   6.96,  0.01,   0.1,0.2),
        ("temperature",          "temperature",    "degF",  "TT",     55.0,  115.0,   55.0,   44.5,  115.0,  125.5,   0.1,   0.5,1.0),
        ("thief_hatch_position", "valve_position", "percent","ZT",      0.0,    2.0,   None,   None,    2.0,    2.4,   0.1,  0.05,0.1),
    ],
    "Flare": [
        ("pilot_flame",          "pilot_flame",    "state", "BT",      1.0,    1.0,   None,    0.0,   None,   None,   1.0,   0.0,0.0),
        ("flow",                 "flow",           "mscfd", "FT",      0.0,  150.0,   None,   None,  150.0,  176.2,   0.1,   3.0,5.0),
        ("stack_temperature",    "temperature",    "degF",  "TT",    900.0, 1800.0,  900.0,  742.0, 1800.0, 1958.0,   1.0,  12.0,20.0),
        ("air_fuel_ratio",       "air_fuel_ratio", "ratio", "AT",     15.0,   19.0,   15.0,   14.3,   19.0,   19.7,  0.01,   0.1,0.2),
    ],
    "Pump": [
        ("discharge_pressure",   "pressure",       "psig",  "PT",    120.0,  600.0,  120.0,   60.0,  600.0,  684.0,   0.5,   2.5,6.0),
        ("flow",                 "flow",           "bpd",   "FT",    100.0,  900.0,  100.0,   50.0,  900.0, 1040.0,   1.0,   8.0,15.0),
        ("vibration",            "vibration",      "in/s",  "VT",     0.04,   0.22,   None,   None,   0.22,  0.252, 0.001, 0.008,0.015),
    ],
    "Metering Station": [
        ("flow",                 "flow",           "mscfd", "FT",    500.0, 4000.0,  500.0,  250.0, 4000.0, 4612.0,   1.0,  25.0,50.0),
        ("pressure",             "pressure",       "psig",  "PT",    250.0,  900.0,  250.0,  136.0,  900.0, 1014.0,   0.5,   3.0,8.0),
        ("temperature",          "temperature",    "degF",  "TT",     50.0,  110.0,   50.0,   39.5,  110.0,  120.5,   0.1,   0.5,1.0),
        ("differential_pressure","pressure",       "inH2O", "PDT",    20.0,  180.0,   20.0,   10.0,  180.0,  208.0,   0.1,   1.2,2.5),
    ],
}
TAG_TEMPLATE_FIELDS = ("tag_name", "measurement_type", "uom", "isa", "normal_min",
                       "normal_max", "alarm_lo", "alarm_lolo", "alarm_hi", "alarm_hihi",
                       "resolution", "noise_sigma", "drift_per_year")

MEASUREMENT_TYPES = {"pressure", "flow", "temperature", "level", "vibration",
                     "valve_position", "rpm", "pilot_flame", "air_fuel_ratio"}

INSTRUMENTABLE_EQUIPMENT = set(TAG_TEMPLATES)
UNINSTRUMENTED_EQUIPMENT = set(EQUIPMENT_TYPES) - INSTRUMENTABLE_EQUIPMENT


def _alarm_sigmas(d):
    """(warning sigmas, trip sigmas) for one template, for reporting."""
    C = 0.5 * (d["normal_min"] + d["normal_max"])
    sig = TELEMETRY_PROCESS_SD_FRACTION * 0.5 * (d["normal_max"] - d["normal_min"])
    if sig <= 0:
        return [], []
    warn = [abs(d[k] - C) / sig for k in ("alarm_hi", "alarm_lo") if d[k] is not None]
    trip = [abs(d[k] - C) / sig for k in ("alarm_hihi", "alarm_lolo") if d[k] is not None]
    return warn, trip


def tag_template_dicts(equipment_type):
    """Templates for one equipment type, as dicts keyed by TAG_TEMPLATE_FIELDS."""
    return [dict(zip(TAG_TEMPLATE_FIELDS, t)) for t in TAG_TEMPLATES[equipment_type]]


# --- validation -----------------------------------------------------------------------------
assert INSTRUMENTABLE_CRITICALITY <= {"Low", "Medium", "High", "Critical"}, \
    "INSTRUMENTABLE_CRITICALITY names a criticality that does not exist"
assert MAX_INSTRUMENTED_ASSETS_PER_FACILITY > 0, "cap must be positive"
assert 0.0 < HOT_TAG_SHARE < 1.0, "HOT_TAG_SHARE must be a share strictly between 0 and 1"
assert HOT_INTERVAL_SECONDS < STANDARD_INTERVAL_SECONDS, \
    "hot tags must sample faster than standard ones"
assert set(INSTRUMENT_PRIORITY) == INSTRUMENTABLE_EQUIPMENT, (
    "INSTRUMENT_PRIORITY must rank exactly the instrumentable equipment types; "
    f"missing {sorted(INSTRUMENTABLE_EQUIPMENT - set(INSTRUMENT_PRIORITY))}, "
    f"unexpected {sorted(set(INSTRUMENT_PRIORITY) - INSTRUMENTABLE_EQUIPMENT)}"
)
assert INSTRUMENTABLE_EQUIPMENT <= set(EQUIPMENT_TYPES), (
    f"TAG_TEMPLATES names unknown equipment type(s): "
    f"{sorted(INSTRUMENTABLE_EQUIPMENT - set(EQUIPMENT_TYPES))}"
)

for _et, _tmpls in TAG_TEMPLATES.items():
    _names = [t[0] for t in _tmpls]
    assert len(_names) == len(set(_names)), f"{_et}: duplicate tag name(s) in TAG_TEMPLATES"
    for _t in _tmpls:
        assert len(_t) == len(TAG_TEMPLATE_FIELDS), (
            f"{_et}/{_t[0]}: template has {len(_t)} fields, expected "
            f"{len(TAG_TEMPLATE_FIELDS)} ({', '.join(TAG_TEMPLATE_FIELDS)})"
        )
        _d = dict(zip(TAG_TEMPLATE_FIELDS, _t))
        assert _d["measurement_type"] in MEASUREMENT_TYPES, \
            f"{_et}/{_d['tag_name']}: unknown measurement_type {_d['measurement_type']!r}"
        assert _d["normal_min"] <= _d["normal_max"], \
            f"{_et}/{_d['tag_name']}: normal_min above normal_max"
        assert _d["resolution"] > 0, f"{_et}/{_d['tag_name']}: resolution must be positive"
        assert _d["noise_sigma"] >= 0 and _d["drift_per_year"] >= 0, \
            f"{_et}/{_d['tag_name']}: noise and drift must be non-negative"
        # Alarm limits must be ordered wherever present. Nulls are legitimate -- vibration
        # has no low trip, a flare header has no low-flow trip -- so the chain is checked
        # over the values that ARE set, in order.
        _chain = [("alarm_lolo", _d["alarm_lolo"]), ("alarm_lo", _d["alarm_lo"]),
                  ("normal_min", _d["normal_min"]), ("normal_max", _d["normal_max"]),
                  ("alarm_hi", _d["alarm_hi"]), ("alarm_hihi", _d["alarm_hihi"])]
        _present = [(n, v) for n, v in _chain if v is not None]
        for (_n1, _v1), (_n2, _v2) in zip(_present, _present[1:]):
            assert _v1 <= _v2, (
                f"{_et}/{_d['tag_name']}: alarm limits out of order -- "
                f"{_n1}={_v1} must not exceed {_n2}={_v2}"
            )
        # Limits must stay within REACH of the process, or 02d derives nothing from them.
        # This is the guard against a future edit quietly restoring protection trips.
        _C = 0.5 * (_d["normal_min"] + _d["normal_max"])
        _sigma = TELEMETRY_PROCESS_SD_FRACTION * 0.5 * (_d["normal_max"] - _d["normal_min"])
        if _sigma > 0:
            for _n, _v, _sgn, _band in (
                    ("alarm_hi", _d["alarm_hi"], 1.0, ALARM_WARN_SIGMA_BAND),
                    ("alarm_lo", _d["alarm_lo"], -1.0, ALARM_WARN_SIGMA_BAND),
                    ("alarm_hihi", _d["alarm_hihi"], 1.0, ALARM_TRIP_SIGMA_BAND),
                    ("alarm_lolo", _d["alarm_lolo"], -1.0, ALARM_TRIP_SIGMA_BAND)):
                if _v is None:
                    continue
                _z = _sgn * (_v - _C) / _sigma
                assert _band[0] <= _z <= _band[1], (
                    f"{_et}/{_d['tag_name']}: {_n} = {_v} sits {_z:.1f} sigma from centre, "
                    f"outside the {_band[0]}-{_band[1]} band. These are ANNUNCIATION limits, "
                    "meant to be crossed by ordinary excursions several times a month -- a "
                    "limit further out is an equipment protection trip and will leave 02d's "
                    "alarm table empty. See the note above TAG_TEMPLATES."
                )

print("SCADA instrumentation policy:")
print(f"  instrumentable types     {', '.join(sorted(INSTRUMENTABLE_EQUIPMENT))}")
print(f"  never instrumented       {', '.join(sorted(UNINSTRUMENTED_EQUIPMENT))}")
print(f"  criticality bar          {', '.join(sorted(INSTRUMENTABLE_CRITICALITY))}")
print(f"  cap per facility         {MAX_INSTRUMENTED_ASSETS_PER_FACILITY}"
      f"  -> at most {MAX_INSTRUMENTED_ASSETS_PER_FACILITY * N_FACILITIES:,} instrumented assets")
print(f"  tiering                  {HOT_TAG_SHARE:.0%} hot at {HOT_INTERVAL_SECONDS}s,"
      f" rest at {STANDARD_INTERVAL_SECONDS}s")
print(f"  raw telemetry window     {TELEMETRY_RAW_DAYS} days, capped at "
      f"{MAX_TELEMETRY_ROWS_PER_RUN:,} rows per run")
print()
_warn_z, _trip_z = [], []
for _et2, _tm2 in TAG_TEMPLATES.items():
    for _t2 in _tm2:
        _w2, _r2 = _alarm_sigmas(dict(zip(TAG_TEMPLATE_FIELDS, _t2)))
        _warn_z += _w2
        _trip_z += _r2
print(f"  alarm limits             warning {ALARM_WARN_HALF_BANDS:.2f} half-bands "
      f"(= the normal band edge), trip {ALARM_TRIP_HALF_BANDS:.2f}")
print(f"                           = {min(_warn_z):.2f}-{max(_warn_z):.2f} sigma warning, "
      f"{min(_trip_z):.2f}-{max(_trip_z):.2f} sigma trip, at process sd "
      f"{TELEMETRY_PROCESS_SD_FRACTION:.2f} x half-band")
print(f"                           annunciation, not equipment protection -- see the note "
      f"above TAG_TEMPLATES")
print()
print(f"  {'equipment_type':<20}{'tags':>6}   tag names")
for _et in sorted(TAG_TEMPLATES, key=lambda e: INSTRUMENT_PRIORITY[e]):
    _n = [t[0] for t in TAG_TEMPLATES[_et]]
    print(f"  {_et:<20}{len(_n):>6}   {', '.join(_n)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Asset operating state ----------------------------------------------------------------
# Consumed by 02a_build_asset_state, which writes fact_asset_state as a SPARSE INTERVAL
# table: one row per state change per asset, never one row per timestamp.
#
# State is generated from ASSET CHARACTERISTICS ONLY -- type, age against expected life,
# leak propensity, inspection interval. It deliberately does NOT read fact_emission_episode
# or any other hidden ground-truth table. The telemetry generator overlays episode effects
# separately; keeping the two independent is what stops the demo becoming circular, where
# the operational data "discovers" an episode that was written into it in the first place.

STATE_HISTORY_DAYS = 90

STATES = ["Running", "Standby", "Down", "Maintenance", "Startup", "Shutdown"]
STATE_CAUSES = ["Scheduled PM", "Corrective", "Trip", "Market", "Unknown"]

# States that count as available. Standby is available -- the asset is healthy and could run;
# it is idle for commercial reasons.
AVAILABLE_STATES = {"Running", "Standby"}

# The state machine. Four chains leave Running, and every one of them returns through
# Startup, so Startup only ever precedes Running and Shutdown only ever follows it:
#
#   Running --PM due-----> Shutdown(Scheduled PM) -> Maintenance(Scheduled PM) -> Startup -> Running
#   Running --corrective-> Shutdown(Corrective)   -> Maintenance(Corrective)   -> Startup -> Running
#   Running --market-----> Shutdown(Market)       -> Standby(Market)           -> Startup -> Running
#   Running --trip-------> Down(Trip)             -> Maintenance(Corrective)   -> Startup -> Running
#                                                 \-> Startup -> Running          (spurious trip)
#
# A trip goes straight to Down with no Shutdown: that is exactly what distinguishes a trip
# from a planned stop. A spurious trip resets without a repair.
SPURIOUS_TRIP_SHARE = 0.30     # share of trips that clear without maintenance

# Dwell time per state, in hours (lo, hi). Drawn uniformly at interval creation and fixed
# there -- never re-drawn on a later run, which is what makes closure a pure function of
# elapsed time.
STATE_DWELL_HOURS = {
    "Startup":  (0.25, 1.0),    # 15-60 min, transitional
    "Shutdown": (0.25, 1.0),    # 15-60 min, transitional
    "Down":     (2.0, 14.0),    # tripped, waiting for a technician to reach a remote pad
    "Standby":  (12.0, 96.0),   # idle for commercial reasons -- counts as AVAILABLE
}
# Maintenance dwell depends on why the asset is down, not on the asset.
#
# These were first set at (3-14) and (6-40) and produced 98.8% availability estate-wide,
# with no equipment type inside the 92-97% target -- the event RATE was right but each
# event was too short. Corrective work on a remote pad means mobilising a crew and often
# parts, so 12-72 hours is the realistic figure, and it puts rotating equipment in band.
# A compressor at MTBF 26 days now loses roughly 30 hours per cycle, giving ~95%.
MAINTENANCE_DWELL_HOURS = {
    "Scheduled PM": (6.0, 24.0),
    "Corrective":   (12.0, 72.0),
}

# Running dwell. Mean time between stops, in days, for a notional asset with duty 1.0,
# leak_propensity 1.0 and age at half its expected life.
BASE_MTBF_DAYS = 26.0

# Mechanical duty. leak_propensity alone does not separate trip-prone from trip-free
# equipment -- it describes how likely something is to LEAK, not how likely it is to STOP,
# and Storage Tank sits at 0.85 which would make a static vessel trip almost as often as a
# compressor. This factor carries the rotating-vs-static distinction explicitly rather than
# overloading leak_propensity with a meaning it does not have.
STATE_DUTY_FACTOR = {
    "Compressor":       1.00,   # rotating, continuous duty, the classic tripper
    "Pump":             0.85,   # rotating, often intermittent
    "Flare":            0.55,   # pilot and igniter faults
    "Separator":        0.40,   # static vessel, level control can still upset
    "Metering Station": 0.30,   # instrumentation faults rather than mechanical
    "Valve":            0.25,
    "Storage Tank":     0.20,   # static; stops are nearly always planned
    "Pipeline Segment": 0.15,
}

# Why an unplanned stop happened, given that one did.
UNPLANNED_CAUSE_WEIGHTS = {"Trip": 0.55, "Corrective": 0.30, "Market": 0.10, "Unknown": 0.05}

# Availability guard. The meaningful bound is the LOWER one: availability collapsing means
# the state model has become churny or a chain is not returning to Running. The upper bound
# is 1.0 inclusive and only guards against an arithmetic error producing more than 100%.
#
# Exactly 100% is legitimate and common on a short window -- over one day, a type with duty
# 0.15 and an MTBF near 250 days will often have no asset change state at all, and an
# earlier upper bound of 0.9999 failed the run on precisely that. Static equipment also sits
# near 100% over long windows: a storage tank with a 365-day inspection interval and duty
# 0.20 barely stops.
#
# The expected band is what the estate should mostly sit in; 02a prints anything outside it
# without failing.
AVAILABILITY_HARD_BAND = (0.85, 1.0)
AVAILABILITY_EXPECTED_BAND = (0.92, 0.97)

# Volume guard. This is an interval table; if it ever approaches this the state model has
# become churny and the dwell times or MTBF are wrong.
MAX_STATE_ROWS_PER_30D = 200_000

# --- validation -------------------------------------------------------------------------------
assert STATE_HISTORY_DAYS > 0, "STATE_HISTORY_DAYS must be positive"
assert TELEMETRY_RAW_DAYS <= STATE_HISTORY_DAYS, (
    f"TELEMETRY_RAW_DAYS ({TELEMETRY_RAW_DAYS}) exceeds STATE_HISTORY_DAYS "
    f"({STATE_HISTORY_DAYS}). 02b conditions every reading on the asset's operating state, "
    "and fact_asset_state has no intervals before the state history anchor, so the earliest "
    "telemetry days would have nothing to join to."
)
assert set(STATE_DUTY_FACTOR) == set(EQUIPMENT_TYPES), (
    "STATE_DUTY_FACTOR must cover exactly the equipment types in EQUIPMENT_TYPES; "
    f"missing {sorted(set(EQUIPMENT_TYPES) - set(STATE_DUTY_FACTOR))}"
)
assert AVAILABLE_STATES <= set(STATES), "AVAILABLE_STATES names a state that does not exist"
assert set(MAINTENANCE_DWELL_HOURS) <= set(STATE_CAUSES), "unknown maintenance cause"
assert abs(sum(UNPLANNED_CAUSE_WEIGHTS.values()) - 1.0) < 1e-9, \
    "UNPLANNED_CAUSE_WEIGHTS must sum to 1"
assert set(UNPLANNED_CAUSE_WEIGHTS) <= set(STATE_CAUSES), "unknown unplanned cause"
assert 0.0 <= SPURIOUS_TRIP_SHARE < 1.0, "SPURIOUS_TRIP_SHARE must be a share below 1"
for _s, (_lo, _hi) in STATE_DWELL_HOURS.items():
    assert _s in STATES, f"STATE_DWELL_HOURS names unknown state {_s!r}"
    assert 0 < _lo <= _hi, f"{_s}: invalid dwell range ({_lo}, {_hi})"
for _c, (_lo, _hi) in MAINTENANCE_DWELL_HOURS.items():
    assert 0 < _lo <= _hi, f"maintenance/{_c}: invalid dwell range ({_lo}, {_hi})"
assert 0 < AVAILABILITY_HARD_BAND[0] < AVAILABILITY_HARD_BAND[1] <= 1.0, \
    "AVAILABILITY_HARD_BAND is not an ordered pair inside (0, 1]"
assert all(_f > 0 for _f in STATE_DUTY_FACTOR.values()), "duty factors must be positive"


def mtbf_days(equipment_type, age_years):
    """Mean days between unplanned stops for one asset.

    Falls with mechanical duty, with leak propensity, and with age against expected life.
    A pure function of asset characteristics -- no randomness, no episode data.
    """
    ev = EQUIPMENT_TYPES[equipment_type]
    life = max(ev["life"], 1)
    age_ratio = min(max(age_years, 0.0) / life, 1.5)
    # 0.65 when new, 1.0 at half life, 1.7 at end of life and beyond
    age_factor = 0.65 + 0.70 * age_ratio
    scale = STATE_DUTY_FACTOR[equipment_type] * ev["leak_propensity"] * age_factor
    return BASE_MTBF_DAYS / max(scale, 1e-6)


print("asset state model:")
print(f"  history window     {STATE_HISTORY_DAYS} days")
print(f"  states             {', '.join(STATES)}")
print(f"  causes             {', '.join(STATE_CAUSES)}")
print(f"  available states   {', '.join(sorted(AVAILABLE_STATES))}")
print(f"  base MTBF          {BASE_MTBF_DAYS:.0f} days at duty 1.0, propensity 1.0, half life")
print()
print(f"  {'equipment_type':<20}{'duty':>6}{'propensity':>12}{'MTBF new':>11}{'MTBF mid':>10}"
      f"{'MTBF old':>10}{'PM days':>9}")
for _et in sorted(EQUIPMENT_TYPES, key=lambda e: -STATE_DUTY_FACTOR[e]):
    _life = EQUIPMENT_TYPES[_et]["life"]
    print(f"  {_et:<20}{STATE_DUTY_FACTOR[_et]:>6.2f}"
          f"{EQUIPMENT_TYPES[_et]['leak_propensity']:>12.2f}"
          f"{mtbf_days(_et, 0.0):>11.0f}{mtbf_days(_et, _life * 0.5):>10.0f}"
          f"{mtbf_days(_et, _life):>10.0f}{EQUIPMENT_TYPES[_et]['insp_days']:>9}")
print()
print("  MTBF is days between UNPLANNED stops. Scheduled PM is a separate, calendar-driven")
print("  event following each asset's inspection_frequency_days, so state and maintenance")
print("  stay consistent with one another.")


print("asset mix and count by facility type:")
print(f"  {'facility_type':<24}{'assets':>10}   dominant equipment")
for _ft in FACILITY_TYPES:
    _lo, _hi = EQUIP_COUNT_BY_TYPE[_ft]
    _top = sorted(TYPE_EQUIPMENT_WEIGHTS[_ft].items(), key=lambda kv: -kv[1])[:3]
    _s = ", ".join(f"{k} {v:.0%}" for k, v in _top)
    print(f"  {_ft:<24}{f'{_lo}-{_hi}':>10}   {_s}")
print(f"  global bound EQUIP_PER_FACILITY = {EQUIP_PER_FACILITY} (derived)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Location sampling -----------------------------------------------------------------

def bbox_excursion_deg(lat, lon):
    """Degrees by which a point falls outside CONFIG['bbox']; 0.0 when inside.

    Chebyshev-style: the largest single-axis overshoot. Used by the 01a assertion that no
    facility strays further than OUTSIDE_MAX_DEG beyond an edge.
    """
    return float(max(
        0.0,
        BBOX["min_lat"] - lat, lat - BBOX["max_lat"],
        BBOX["min_lon"] - lon, lon - BBOX["max_lon"],
    ))


def nearest_anchor(lat, lon):
    """Name of the sub-basin anchor closest to a point, by great-circle distance."""
    return min(ANCHORS, key=lambda n: haversine_km(lat, lon, ANCHORS[n]["lat"], ANCHORS[n]["lon"]))


def allocate_bands(n, rng):
    """Exact quota allocation of n facilities across FACILITY_SPLIT, shuffled.

    Largest-remainder method: floor each share, then hand the leftover to whichever bands
    have the largest fractional part. Guarantees sum(counts) == n and that the realised
    proportions match FACILITY_SPLIT as closely as integer counts allow.

    Returns a list of n band labels in random order, so band does not correlate with
    facility_sk (an ordered list would put every outside facility at the end of the estate).
    """
    names = list(FACILITY_SPLIT)
    raw = np.array([FACILITY_SPLIT[b] * n for b in names], dtype=float)
    counts = np.floor(raw).astype(int)
    remainder = n - int(counts.sum())
    if remainder:
        # stable order: largest fractional part first, ties broken by FACILITY_SPLIT order
        order = sorted(range(len(names)), key=lambda i: (-(raw[i] - counts[i]), i))
        for k in range(remainder):
            counts[order[k % len(order)]] += 1

    assert counts.sum() == n, f"band quota {counts.sum()} != {n}"
    bands = [b for b, c in zip(names, counts) for _ in range(int(c))]
    rng.shuffle(bands)
    return bands


def sample_facility_location(rng, band):
    """Place one facility in the given band. Returns (lat, lon, anchor_name).

    inside    - within the anchor's region radius
    perimeter - between the anchor's region and perimeter radii
    outside   - just beyond a BBOX edge, offset OUTSIDE_MIN_DEG..OUTSIDE_MAX_DEG

    The band is supplied by allocate_bands rather than drawn here, so the realised split is
    exact rather than a sample from FACILITY_SPLIT.

    The outside band is positioned relative to the BBOX rather than to an anchor, which is
    what bounds it. In V1 "outside" meant "far from the anchor", and because one anchor was
    already below the BBOX those facilities landed ~110 km south of the basin. Here the
    excursion is OUTSIDE_MAX_DEG by construction and asserted in 01a.

    inside/perimeter draws are clamped EDGE_INSET_DEG inside each edge so the outside band is
    the only source of out-of-BBOX facilities. With the current anchors and radii the clamp
    never binds; it is there so that moving an anchor cannot quietly reintroduce the defect.
    """
    assert band in FACILITY_SPLIT, f"unknown band {band!r}"

    if band == "outside":
        edge = str(rng.choice(["north", "south", "east", "west"]))
        off = float(rng.uniform(OUTSIDE_MIN_DEG, OUTSIDE_MAX_DEG))
        if edge == "north":
            lat, lon = BBOX["max_lat"] + off, float(rng.uniform(BBOX["min_lon"], BBOX["max_lon"]))
        elif edge == "south":
            lat, lon = BBOX["min_lat"] - off, float(rng.uniform(BBOX["min_lon"], BBOX["max_lon"]))
        elif edge == "east":
            lat, lon = float(rng.uniform(BBOX["min_lat"], BBOX["max_lat"])), BBOX["max_lon"] + off
        else:
            lat, lon = float(rng.uniform(BBOX["min_lat"], BBOX["max_lat"])), BBOX["min_lon"] - off
        return float(lat), float(lon), nearest_anchor(lat, lon)

    anchor_name = str(rng.choice(list(ANCHORS), p=[a["weight"] for a in ANCHORS.values()]))
    a = ANCHORS[anchor_name]
    # Per-anchor radii, not the globals: an anchor hard against a BBOX edge carries a
    # tighter pair (see Northwest Shelf). Reading the globals here would ignore that and
    # reintroduce the clipping the override exists to avoid.
    region_r, perimeter_r = anchor_radii(anchor_name)
    r = (rng.uniform(0.0, region_r) if band == "inside"
         else rng.uniform(region_r, perimeter_r))
    theta = rng.uniform(0, 2 * np.pi)
    lat = a["lat"] + r * np.cos(theta)
    lon = a["lon"] + r * np.sin(theta) / np.cos(np.radians(a["lat"]))

    lat = float(np.clip(lat, BBOX["min_lat"] + EDGE_INSET_DEG, BBOX["max_lat"] - EDGE_INSET_DEG))
    lon = float(np.clip(lon, BBOX["min_lon"] + EDGE_INSET_DEG, BBOX["max_lon"] - EDGE_INSET_DEG))
    return lat, lon, anchor_name


print("Topology config loaded.")
print(f"  BBOX: lat {BBOX['min_lat']}..{BBOX['max_lat']}, lon {BBOX['min_lon']}..{BBOX['max_lon']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
