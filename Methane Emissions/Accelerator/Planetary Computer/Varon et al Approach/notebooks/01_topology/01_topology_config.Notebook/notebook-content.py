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
