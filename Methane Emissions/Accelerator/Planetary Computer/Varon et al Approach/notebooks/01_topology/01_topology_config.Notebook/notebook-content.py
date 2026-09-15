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
# Three real Permian sub-basin anchors, all comfortably inside CONFIG["bbox"]
# (lat 30.5..33.5, lon -105.0..-101.0). Compare V1, whose second anchor "Texas Site A" sat at
# lat 30.2 -- below the BBOX floor -- and carried 40% of the estate.
#
# Every anchor plus PERIMETER_RADIUS_DEG stays inside the BBOX, so the inside and perimeter
# bands cannot leak past an edge; the clamp in sample_facility_location is a guard, not a
# load-bearing step.

ANCHORS = {
    "Midland Basin":          {"lat": 32.05, "lon": -102.10, "weight": 0.45},
    "Delaware Basin":         {"lat": 31.75, "lon": -103.70, "weight": 0.40},
    "Central Basin Platform": {"lat": 31.95, "lon": -102.90, "weight": 0.15},
}

REGION_RADIUS_DEG    = 0.30   # ~33 km core
PERIMETER_RADIUS_DEG = 0.55   # outer band

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

print("Anchors (all inside CONFIG['bbox']):")
for _name, _a in ANCHORS.items():
    _in = (BBOX["min_lat"] <= _a["lat"] <= BBOX["max_lat"]
           and BBOX["min_lon"] <= _a["lon"] <= BBOX["max_lon"])
    print(f"  {_name:<24} {_a['lat']:.2f}N {abs(_a['lon']):.2f}W  w={_a['weight']:.2f}  in_bbox={_in}")
    assert _in, f"anchor {_name} is outside CONFIG['bbox'] -- this is the V1 defect"
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
EQUIP_PER_FACILITY   = (10, 50)
N_OPERATORS          = 12
SENSORS_PER_FACILITY = 4
SENSOR_INTERVAL_HOURS = 4

# Commissioning window for facilities: up to 15 years of history.
HISTORY_YEARS = 15
TOPOLOGY_AS_OF = date(2026, 9, 15)   # fixed, not date.today(): a moving as-of date makes
                                     # install_date and every derived age irreproducible

print(f"{N_FACILITIES} facilities, {EQUIP_PER_FACILITY[0]}-{EQUIP_PER_FACILITY[1]} assets each,"
      f" {SENSORS_PER_FACILITY} sensors per facility")
print(f"as-of date: {TOPOLOGY_AS_OF}")

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

print(f"{len(EQUIPMENT_TYPES)} equipment types, {len(MANUFACTURERS)} manufacturers,"
      f" {len(SENSOR_TYPES)} sensor types")

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

    inside    - within REGION_RADIUS_DEG of a sub-basin anchor
    perimeter - REGION_RADIUS_DEG..PERIMETER_RADIUS_DEG of an anchor
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
    r = (rng.uniform(0.0, REGION_RADIUS_DEG) if band == "inside"
         else rng.uniform(REGION_RADIUS_DEG, PERIMETER_RADIUS_DEG))
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
