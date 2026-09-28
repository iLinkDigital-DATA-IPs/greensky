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

# # 03a — Generate Emission Episodes
#
# Writes **`fact_emission_episode`**: one row per leak episode. An asset starts emitting at
# some rate, for some duration, from some root cause, then stops. Intervals, not a time
# series. Reads `dim_equipment`, `dim_facility`, `dim_area` and `fact_asset_state`;
# **modifies nothing else**.
#
# ### Episodes are the cause, never an effect
# This is the hidden ground truth. Plumes, SCADA telemetry and CH₄ detector readings are
# *observations* of it. So this notebook reads **no observation table**: not
# `gold_plume_catalog`, not `gold_multi_gas_signatures`, not `scada_telemetry`, not
# `sensor_telemetry`. An episode generator that read its own observations would make every
# downstream agreement circular. A plume "matching" an episode, or a SCADA signature
# "corroborating" one, would only be the generator finding what it had copied in. Every
# table read goes through `read_input()`, which refuses those names, and the run fails if one
# was read.
#
# The rate distribution *is* calibrated against the real TROPOMI catalogue, but only through
# fixed constants written into this notebook: a floor, a median and a maximum measured once
# on 2026-08-16..2026-09-15. They are never read from a table at run time. They also
# calibrate the *shape* of the rate distribution, not the number or the timing of episodes,
# so no individual episode is placed where a plume was seen.
#
# ### The hazard
# V1's Weibull hazard, driven by the shared `equipment_condition_index` in
# `01_topology_config` (age, service interval, manufacturer reliability, leak propensity), so
# the maintenance generator that follows sees the same degraded assets. The hazard uses
# `leak_propensity`, which describes **leaking**. It does not use `STATE_DUTY_FACTOR`, which
# describes **stopping** and was added for 02a precisely because the two differ. A storage
# tank rarely trips, but it leaks readily.
#
# ### Operating state
# Episodes neither start nor continue during `Maintenance` or `Down` in `fact_asset_state`.
# An asset that is not running is not leaking process gas. See the episode model below for
# how an episode under way is handled when its asset stops.
#
# ### Writes
# `fact_emission_episode` only, partitioned by `date_sk`, with `replaceWhere`. No `dim_*`
# table and no `fact_asset_state` row is modified. No plumes, telemetry or work orders.
# Everything is synthetic.

# MARKDOWN ********************

# `00_config` is already run by `01_topology_config`. It is run explicitly here as well so
# this notebook's dependencies are visible at the top of the file rather than inherited two
# levels down.

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

# MARKDOWN ********************

# ### Inputs, and the guard against reading observations
#
# The only tables this notebook may read are the estate and its operating state. Every read
# goes through `read_input()`. It refuses anything else outright, and the validation cell
# fails the run if anything outside `INPUT_TABLES` was read at all.

# CELL ********************

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

TABLE = "fact_emission_episode"
INPUT_TABLES = ("dim_equipment", "dim_facility", "dim_area", "fact_asset_state")

# Observations of the ground truth this notebook generates. An episode generator that read
# any of these would make every downstream agreement circular -- a plume "matching" an
# episode, or a SCADA signature "corroborating" one, would be the generator finding its
# own input. The list is the detection layer's outputs and every telemetry table.
OBSERVATION_TABLES = (
    "gold_plume_catalog", "gold_multi_gas_signatures", "gold_flagged_large_clusters",
    "gold_rejected_collinear", "gold_emission_sites", "gold_plume_site_mapping",
    "gold_plume_id_runs", "silver_plume_ready_pixels", "bronze_ch4_pixels",
    "bronze_no2_pixels", "validation_cams_plumes", "validation_carbon_mapper_plumes",
    "scada_telemetry", "scada_telemetry_hourly", "scada_telemetry_daily", "fact_scada_alarm",
    "sensor_telemetry",
)
assert not set(INPUT_TABLES) & set(OBSERVATION_TABLES), "an input is an observation table"

TABLES_READ = set()


def read_input(name):
    """The only way this notebook reads a table."""
    assert name not in OBSERVATION_TABLES, (
        f"{name} is an observation of the episodes this notebook generates. Reading it "
        "would make every downstream agreement between episodes and observations circular."
    )
    assert name in INPUT_TABLES, f"{name} is not a declared input of 03a: {INPUT_TABLES}"
    TABLES_READ.add(name)
    return spark.table(name)


def table_exists(name):
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False


print(f"inputs            {', '.join(INPUT_TABLES)}")
print(f"refused           {len(OBSERVATION_TABLES)} observation tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### The episode model
#
# This cell is pure: no Spark, no table, no clock. `tools/harness/harness_episodes.py`
# executes it verbatim against `01_topology_config`, so the harness checks this code, not a
# copy of it.
#
# **Onset.** For every asset and every UTC day, `get_rng("episode", equipment_id, day)` draws
# a Poisson count of onsets from that day's hazard, then each episode's parameters in a fixed
# order. A day's episodes therefore depend only on the asset, the day and the seed. They do
# not depend on how the run window was sliced, which is what makes a backfill equal to the
# sequence of incremental days.
#
# **Hazard.** V1's Weibull, `h = (k/λ)(t/λ)^(k-1)` with `k = 1.8` and `λ = 0.9 × life`, is
# evaluated at the *condition-equivalent age* `t = cond × life`. `cond` is the shared
# `equipment_condition_index`. The result is then scaled by `leak_propensity` and by
# criticality, as in V1. `days_since_service` is measured from the end of the asset's last
# `Maintenance` interval in `fact_asset_state`. Before the first one in the history, it
# assumes the asset was mid-cycle at the history start, half an inspection interval since
# service.
#
# **Operating state.** An onset drawn while the asset is in `Maintenance` or `Down` is
# dropped. It is not moved: shifting it to the next Running interval would bunch onsets at
# every restart. The draw happens anyway, so the random sequence is unaffected. **An episode
# under way when its asset enters `Maintenance` or `Down` is truncated at that state change
# and ends there.** It does not resume on restart. A stop is where a leak gets found and fixed,
# and a Corrective or Scheduled PM visit is exactly that. `Standby`, `Startup` and
# `Shutdown` still emit: the asset is pressurised.
#
# **Root cause** is weighted by equipment type, and only causes whose 02b signature touches
# at least one of that type's tags are allowed. Tank flashing is on storage tanks only,
# unlit flare on flares only, compressor blowby on compressors only. Valve and Pipeline
# Segment carry no tags, so their episodes are never corroborated by telemetry, whatever the
# cause.
#
# **Rates are a two-component log-normal mixture**, explained in the calibration note
# below.

# CELL ********************

# ---- 03a episode model (pure: tools/harness/harness_episodes.py executes this cell) ----

ROOT_CAUSES = ["Seal failure", "Valve leak", "Tank flashing", "Unlit flare", "Corrosion",
               "Pneumatic device", "Compressor blowby", "Unknown"]

# Root cause given equipment type. For the six instrumented types, a cause is listed only if
# its signature in 02b's EPISODE_SIGNATURE touches at least one tag that type carries in
# TAG_TEMPLATES; otherwise 02b's overlay could never produce the signature, and the episode
# could not be corroborated. harness_episodes.py checks that against both notebooks.
ROOT_CAUSE_WEIGHTS = {
    "Compressor":       {"Seal failure": 0.30, "Compressor blowby": 0.30, "Valve leak": 0.15,
                         "Pneumatic device": 0.10, "Corrosion": 0.05, "Unknown": 0.10},
    "Separator":        {"Valve leak": 0.40, "Pneumatic device": 0.25, "Corrosion": 0.20,
                         "Unknown": 0.15},
    "Storage Tank":     {"Tank flashing": 0.60, "Pneumatic device": 0.15, "Corrosion": 0.10,
                         "Unknown": 0.15},
    "Flare":            {"Unlit flare": 0.75, "Valve leak": 0.10, "Unknown": 0.15},
    "Pump":             {"Seal failure": 0.50, "Valve leak": 0.20, "Corrosion": 0.10,
                         "Pneumatic device": 0.05, "Unknown": 0.15},
    "Metering Station": {"Valve leak": 0.45, "Pneumatic device": 0.25, "Corrosion": 0.15,
                         "Unknown": 0.15},
    "Valve":            {"Valve leak": 0.55, "Pneumatic device": 0.25, "Corrosion": 0.10,
                         "Unknown": 0.10},
    "Pipeline Segment": {"Corrosion": 0.60, "Valve leak": 0.20, "Unknown": 0.20},
}
# Causes that physically belong to one equipment type and nowhere else.
TYPE_EXCLUSIVE_CAUSES = {"Tank flashing": "Storage Tank", "Unlit flare": "Flare",
                         "Compressor blowby": "Compressor"}

# Share of episodes that are intermittent -- short repeated venting -- rather than a
# sustained leak, by cause. Flashing and pneumatic actuation vent in bursts; corrosion and
# seal failures leak continuously.
INTERMITTENT_SHARE = {"Seal failure": 0.15, "Valve leak": 0.30, "Tank flashing": 0.70,
                      "Unlit flare": 0.30, "Corrosion": 0.05, "Pneumatic device": 0.80,
                      "Compressor blowby": 0.20, "Unknown": 0.35}

# Durations in hours, gamma(shape, scale), from V1: intermittent mean 12 h, sustained 120 h.
# Capped at EPISODE_MAX_DURATION_DAYS, which is also the incremental lookback (see below).
DURATION_GAMMA_H = {True: (2.0, 6.0), False: (2.5, 48.0)}
EPISODE_MIN_DURATION_H = 0.25
EPISODE_MAX_DURATION_DAYS = 30

# mean_rate / peak_rate. Intermittent venting spends most of its time between bursts.
MEAN_FRACTION = {True: (0.20, 0.50), False: (0.70, 0.95)}

# ---- rates: calibrated, not assumed -------------------------------------------------------
# TROPOMI's practical floor here is ~3 t/h, the catalogue minimum among wind-dependent
# plumes. The observed above-floor plumes (2026-08-16..2026-09-15) have median 19.7 t/h and
# max 71.8 t/h, n = 49 over 14 scene days. CAMS, same instrument, gives a median of 48 t/h.
#
# A single log-normal cannot meet both constraints. For the share above 3 t/h to be
# anything from 1% to 30%, AND for that subset to have a median near 20 t/h, sigma has to be
# 3.7-7.6 in log units. That puts the maximum across ~50 above-floor episodes at 5,000 t/h or
# more. The real above-floor spread is narrow: 3.2 to 71.8 t/h, about 0.7 in log units.
#
# So rates are a two-component log-normal mixture, which is also how super-emitter
# populations are usually described:
#   body           ordinary component leaks, median 25 kg/h, sigma 1.3 -- essentially never
#                  above the floor (P ~ 2e-4)
#   super-emitter  median 20 t/h, sigma 0.7 -- the above-floor population, matched to the
#                  observed median and spread
# The super-emitter probability depends on root cause. Unlit flares and tank flashing are
# the classic Permian super-emitters, compressor blowby often, and component leaks rarely.
#
# How often it fires is set by a TAIL-FREQUENCY target, not by the shape. The target is
# the number of above-floor episodes active during the detection window (2026-08-16..09-15)
# that is consistent with what TROPOMI saw: 50-100, aiming at ~70.
#   - Of the 49 plumes, 42 attribute to an estate facility, but at a median of 21 km
#     inside a 50 km search. With 150 facilities spread over the basin nearly any plume
#     finds one, so that is attribution geometry, not origin. The estate is perhaps 5-15%
#     of the basin's super-emitter-capable infrastructure, and some of the 49 are likely
#     false positives (CAMS's median is 2.4x ours). Taken literally that is a handful of
#     estate-origin detections. The demo needs the estate to carry a real share, so the
#     target is 10-20 estate-origin detections: above the literal estimate, short of 42.
#   - Detection per above-floor episode is ~0.2. Only 14 of 31 days are scene days, the
#     episode must be active at the one daily overpass, and detection near the floor is
#     poor. 45 of 47 gold_emission_sites are single-detection. A multi-day super-emitter
#     seen on most clear overpasses would make repeat sites common, so the per-overpass
#     detection probability must be ~0.15-0.3.
#   - 10-20 / 0.2 = 50-100 active above-floor episodes.
# At ~2 episodes per asset-year this makes ~14% of episodes super-emitters. That is far
# from rare, and far above any real leak population. It is the price of an estate that
# must account for a real share of a basin's satellite detections. The other lever,
# many more small episodes, would keep the share rare but leave 02b's telemetry overlay
# switched on almost constantly. The first calibration, at ~0.3%, gave 0-2 above-floor
# episodes in the window: nothing for the satellite to have seen.
# Resulting shape (harness_episodes.py, synthetic estate): above-floor median ~19-20 t/h,
# top 5% of episodes ~85% of total mass. V1's target was 50%+.
RATE_BODY_MEDIAN_KG_H = 25.0
RATE_BODY_SIGMA = 1.3
RATE_SUPER_MEDIAN_KG_H = 20000.0
RATE_SUPER_SIGMA = 0.7
SUPER_EMITTER_SHARE = {"Seal failure": 0.060, "Valve leak": 0.100, "Tank flashing": 0.420,
                       "Unlit flare": 0.700, "Corrosion": 0.050, "Pneumatic device": 0.035,
                       "Compressor blowby": 0.240, "Unknown": 0.100}
# The target above, checked in the calibration report.
ACTIVE_IN_WINDOW_TARGET = (50, 100)
TROPOMI_FLOOR_KG_H = 3000.0

# ---- hazard ------------------------------------------------------------------------------
EPISODE_WEIBULL_K = 1.8                    # V1
EPISODE_WEIBULL_LIFE_FRACTION = 0.9        # V1: lambda = 0.9 x expected life
EPISODE_COND_FLOOR = 0.05                  # a just-serviced new asset still leaks, rarely
CRITICALITY_HAZARD_WEIGHT = {"Low": 0.8, "Medium": 1.0, "High": 1.2, "Critical": 1.4}  # V1
# Scales the Weibull to an estate-wide episode rate: ~2 episodes per asset-year, so the
# median facility sees single digits per quarter and the largest sites a few dozen. It is a
# free parameter. The observed plume count does not constrain it, because detection
# probability sits between the two (see the calibration report), and it was not fitted to
# that count. At 60 the synthetic estate in harness_episodes.py gave 0.72 per asset-year,
# only ~2 episodes above the floor in 90 days, too thin to check the rate model against.
EPISODE_HAZARD_SCALE = 170.0

# Source position: the asset's own area (assets carry no coordinates -- 01b asserts it)
# plus localisation noise, ~55 m, as V1.
LOCALISATION_SD_DEG = 0.0005

# States in which an asset emits. Maintenance and Down do not: an asset that is not running
# is not leaking process gas. Standby, Startup and Shutdown do -- the asset is pressurised.
EMITTING_STATES = {"Running", "Standby", "Startup", "Shutdown"}
NON_EMITTING_STATES = {"Maintenance", "Down"}

# ---- configuration checks, at definition time ---------------------------------------------
assert EMITTING_STATES | NON_EMITTING_STATES == set(STATES), \
    "every state in STATES must be either emitting or non-emitting"
assert not EMITTING_STATES & NON_EMITTING_STATES, "a state is both emitting and non-emitting"
assert set(ROOT_CAUSE_WEIGHTS) == set(EQUIPMENT_TYPES), \
    "ROOT_CAUSE_WEIGHTS must cover exactly the equipment types in EQUIPMENT_TYPES"
for _et, _w in ROOT_CAUSE_WEIGHTS.items():
    assert set(_w) <= set(ROOT_CAUSES), f"{_et}: unknown cause(s) {set(_w) - set(ROOT_CAUSES)}"
    assert abs(sum(_w.values()) - 1.0) < 1e-9, f"{_et}: root-cause weights must sum to 1"
    assert all(v > 0 for v in _w.values()), f"{_et}: a listed cause has zero weight"
for _c, _et in TYPE_EXCLUSIVE_CAUSES.items():
    _on = [et for et, w in ROOT_CAUSE_WEIGHTS.items() if _c in w]
    assert _on == [_et], f"{_c} must occur on {_et} only; it is weighted on {_on}"
assert set(INTERMITTENT_SHARE) == set(ROOT_CAUSES) == set(SUPER_EMITTER_SHARE)
assert TROPOMI_FLOOR_KG_H == 3000.0, "above_tropomi_floor is defined as peak > 3000 kg/h"

_DAY = pd.Timedelta(days=1)
_OPEN_NS = np.iinfo("int64").max


def draw_episode(rng, equipment_type):
    """One episode's parameters, drawn from rng in a fixed order.

    Every draw is taken whether or not the episode survives the operating-state gate, so
    which later episodes exist never depends on which earlier ones were dropped.
    """
    causes = list(ROOT_CAUSE_WEIGHTS[equipment_type])
    p = [ROOT_CAUSE_WEIGHTS[equipment_type][c] for c in causes]
    offset_s = int(rng.integers(0, 86400))
    cause = str(causes[int(rng.choice(len(causes), p=p))])
    intermittent = bool(rng.random() < INTERMITTENT_SHARE[cause])
    shape, scale = DURATION_GAMMA_H[intermittent]
    dur_h = float(np.clip(rng.gamma(shape, scale), EPISODE_MIN_DURATION_H,
                          EPISODE_MAX_DURATION_DAYS * 24.0))
    is_super = bool(rng.random() < SUPER_EMITTER_SHARE[cause])
    med, sig = ((RATE_SUPER_MEDIAN_KG_H, RATE_SUPER_SIGMA) if is_super
                else (RATE_BODY_MEDIAN_KG_H, RATE_BODY_SIGMA))
    peak = float(rng.lognormal(np.log(med), sig))
    lo, hi = MEAN_FRACTION[intermittent]
    mean_frac = float(rng.uniform(lo, hi))
    dlat, dlon = (float(x) for x in rng.normal(0.0, LOCALISATION_SD_DEG, 2))
    return dict(offset_s=offset_s, root_cause=cause, is_intermittent=intermittent,
                nominal_duration_s=max(1, int(round(dur_h * 3600.0))), is_super=is_super,
                peak_rate_kg_h=peak, mean_rate_kg_h=peak * mean_frac, dlat=dlat, dlon=dlon)


def hazard_per_year(asset, day, days_since_service):
    """Expected onsets per year for one asset on one day."""
    age_years = max((day - asset["install_date"]).days / 365.25, 0.0)
    life = float(asset["expected_life_years"])
    cond = equipment_condition_index(age_years, life, days_since_service,
                                     int(asset["inspection_frequency_days"]),
                                     float(asset["reliability_index"]),
                                     float(asset["leak_propensity"]))
    cond = max(cond, EPISODE_COND_FLOOR)
    lam = EPISODE_WEIBULL_LIFE_FRACTION * life
    t_eff = cond * life
    weibull = (EPISODE_WEIBULL_K / lam) * (t_eff / lam) ** (EPISODE_WEIBULL_K - 1.0)
    return (EPISODE_HAZARD_SCALE * weibull * float(asset["leak_propensity"])
            * CRITICALITY_HAZARD_WEIGHT[asset["criticality"]])


def state_arrays(state_rows):
    """Sorted interval arrays for one asset. An open interval (end_ts null) runs to +inf."""
    s = state_rows.sort_values("start_ts", kind="mergesort")
    starts = s["start_ts"].values.astype("datetime64[ns]").astype("int64")
    ends = s["end_ts"].values.astype("datetime64[ns]")
    ends = np.where(np.isnat(ends), _OPEN_NS, ends.astype("int64"))
    states = s["state"].values
    emit = np.isin(states, list(EMITTING_STATES))
    maint = states == "Maintenance"
    closed_maint_ends = np.sort(ends[maint & (ends != _OPEN_NS)])
    stop_starts = np.sort(starts[~emit])
    return starts, ends, emit, closed_maint_ends, stop_starts


def generate_episodes(assets, state, day_lo, day_hi, history_start):
    """Episodes with onset in [day_lo, day_hi), as a DataFrame. Pure.

    assets: one row per asset with equipment_sk, equipment_id, equipment_type, install_date,
    expected_life_years, inspection_frequency_days, reliability_index, leak_propensity,
    criticality, area_sk, facility_sk, facility_id, area_lat, area_lon.
    state: fact_asset_state rows (equipment_sk, state, start_ts, end_ts; end_ts null = open).

    An episode whose nominal end lies beyond the last known state change keeps that nominal
    end. Its truncation is settled by a later run, which regenerates every onset day within
    EPISODE_MAX_DURATION_DAYS of its window.
    """
    rows = []
    by_asset = {k: g for k, g in state.groupby("equipment_sk", sort=False)}
    days = pd.date_range(day_lo, day_hi, freq="D", inclusive="left")
    for a in assets.sort_values("equipment_sk", kind="mergesort").to_dict("records"):
        g = by_asset.get(a["equipment_sk"])
        if g is None or not len(days):
            continue
        starts, ends, emit, maint_ends, stop_starts = state_arrays(g)
        insp = int(a["inspection_frequency_days"])
        for day in days:
            if day < a["install_date"]:
                continue
            day_ns = day.value
            i = int(np.searchsorted(maint_ends, day_ns, "right"))
            dss = ((day_ns - maint_ends[i - 1]) / 86400e9 if i
                   else (day - history_start) / _DAY + insp / 2.0)
            lam_day = hazard_per_year(a, day, dss) / 365.25
            rng = get_rng("episode", a["equipment_id"], day.date().isoformat())
            n = int(rng.poisson(lam_day))
            for _ in range(n):
                ep = draw_episode(rng, a["equipment_type"])
                onset_ns = day_ns + ep["offset_s"] * 1_000_000_000
                j = int(np.searchsorted(starts, onset_ns, "right")) - 1
                if j < 0 or onset_ns >= ends[j] or not emit[j]:
                    continue    # no known state, or Maintenance / Down: dropped, not moved
                nominal_end_ns = onset_ns + ep["nominal_duration_s"] * 1_000_000_000
                k = int(np.searchsorted(stop_starts, onset_ns, "right"))
                stop_ns = int(stop_starts[k]) if k < len(stop_starts) else _OPEN_NS
                end_ns = min(nominal_end_ns, stop_ns)
                start_ts, end_ts = pd.Timestamp(onset_ns), pd.Timestamp(end_ns)
                dur_h = (end_ns - onset_ns) / 3600e9
                rows.append({
                    "episode_sk": stable_key("episode", a["equipment_id"],
                                             start_ts.isoformat()),
                    "equipment_sk": int(a["equipment_sk"]),
                    "equipment_id": a["equipment_id"],
                    "equipment_type": a["equipment_type"],
                    "area_sk": int(a["area_sk"]),
                    "facility_sk": int(a["facility_sk"]),
                    "facility_id": a["facility_id"],
                    "source_lat": float(a["area_lat"]) + ep["dlat"],
                    "source_lon": float(a["area_lon"]) + ep["dlon"],
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "duration_hours": dur_h,
                    "peak_rate_kg_h": ep["peak_rate_kg_h"],
                    "mean_rate_kg_h": ep["mean_rate_kg_h"],
                    "total_mass_kg": ep["mean_rate_kg_h"] * dur_h,
                    "root_cause": ep["root_cause"],
                    "is_intermittent": ep["is_intermittent"],
                    "above_tropomi_floor": ep["peak_rate_kg_h"] > TROPOMI_FLOOR_KG_H,
                    "date_sk": int(start_ts.strftime("%Y%m%d")),
                    "is_synthetic": True,
                    # working columns, not written
                    "_is_super": ep["is_super"],
                    "_truncated": stop_ns < nominal_end_ns,
                })
    cols = ["episode_sk", "equipment_sk", "equipment_id", "equipment_type", "area_sk",
            "facility_sk", "facility_id", "source_lat", "source_lon", "start_ts", "end_ts",
            "duration_hours", "peak_rate_kg_h", "mean_rate_kg_h", "total_mass_kg",
            "root_cause", "is_intermittent", "above_tropomi_floor", "date_sk",
            "is_synthetic", "_is_super", "_truncated"]
    return pd.DataFrame(rows, columns=cols)


print("episode model defined -- a day's episodes are a pure function of "
      "(asset, day, TOPOLOGY_SEED, fact_asset_state)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run mode and window
#
# `run_mode` via `getArgument`, matching 02a, with optional `start_date` / `end_date`
# overrides. Both modes cover the same history as `fact_asset_state`: `STATE_HISTORY_DAYS`
# back from `TOPOLOGY_AS_OF`.
#
# **Incremental follows `fact_asset_state`, not a watermark of its own.** Its window is the
# last day of state history, the day after that table's latest `date_sk`. Episodes are sparse,
# a few dozen a day estate-wide, so a watermark on this table's own `max(date_sk)` could land
# on a day with no episodes and stop advancing, rerunning that day forever.
#
# **The `date_sk` question.** An episode that starts before the window and ends inside it
# lives in a `date_sk` partition outside the window. 02a handled the same thing by widening
# `replaceWhere` over every partition the run touches, then carrying the untouched rows it
# swept in. **The widening fits here; the carrying is not needed.** The only thing about an
# earlier episode that can change is its truncation, because the state that ends it may not
# have been written yet. That can only matter within `EPISODE_MAX_DURATION_DAYS` of its
# start. So every run regenerates every onset day back to `window start − 30 days` and
# replaces exactly those partitions. Onset draws are pure, so the regenerated rows are the
# old rows with their truncation brought up to date, and nothing needs carrying across.

# CELL ********************

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

AS_OF = pd.Timestamp(TOPOLOGY_AS_OF)
HISTORY_START = AS_OF - pd.Timedelta(days=STATE_HISTORY_DAYS)
LOOKBACK = pd.Timedelta(days=EPISODE_MAX_DURATION_DAYS)

assert table_exists("fact_asset_state"), "fact_asset_state does not exist -- run 02a first"
_state_wm = read_input("fact_asset_state").agg(F.max("date_sk").alias("m")).collect()[0]["m"]
assert _state_wm is not None, "fact_asset_state is empty -- run 02a first"
# The last moment fact_asset_state describes: 02a simulates each run to midnight after its
# last day, so its latest date_sk + 1 day.
STATE_HORIZON = pd.Timestamp(str(int(_state_wm))) + _DAY

if RUN_MODE == "backfill":
    WINDOW_START, WINDOW_END = HISTORY_START, AS_OF
else:
    WINDOW_END = STATE_HORIZON
    WINDOW_START = WINDOW_END - _DAY

if _start_override:
    WINDOW_START = pd.Timestamp(_start_override)
if _end_override:
    WINDOW_END = pd.Timestamp(_end_override)

assert WINDOW_START < WINDOW_END, f"empty window: {WINDOW_START} .. {WINDOW_END}"
assert WINDOW_START >= HISTORY_START, (
    f"window starts {WINDOW_START.date()}, before the {STATE_HISTORY_DAYS}-day state history "
    f"at {HISTORY_START.date()} -- there is no operating state to gate episodes on"
)
assert WINDOW_END <= STATE_HORIZON, (
    f"window ends {WINDOW_END.date()} but fact_asset_state only runs to "
    f"{STATE_HORIZON.date()}. Run 02a for the missing days first; episodes cannot be gated "
    "on state that has not been generated."
)

# Onset days regenerated: the window, plus the lookback over which an earlier episode's
# truncation can still change.
GEN_START = max(HISTORY_START, WINDOW_START - LOOKBACK) if RUN_MODE == "incremental" \
    else WINDOW_START
GEN_END = WINDOW_END
SK_LO = int(GEN_START.strftime("%Y%m%d"))
SK_HI = int((GEN_END - _DAY).strftime("%Y%m%d"))

if RUN_MODE == "incremental" and table_exists(TABLE):
    _own = spark.table(TABLE).agg(F.max("date_sk").alias("m")).collect()[0]["m"]
    assert _own is not None and pd.Timestamp(str(int(_own))) >= GEN_START - _DAY, (
        f"{TABLE} was last written through date_sk {_own}, more than the "
        f"{EPISODE_MAX_DURATION_DAYS}-day lookback before this run's window. The days in "
        "between were never generated -- run a backfill."
    )

print(f"RUN_MODE={RUN_MODE}  window={WINDOW_START.date()}..{WINDOW_END.date()}")
print(f"onset days regenerated {GEN_START.date()}..{(GEN_END - _DAY).date()}  "
      f"(replaceWhere date_sk {SK_LO}..{SK_HI})")
print(f"state history {HISTORY_START.date()} .. {STATE_HORIZON.date()}  as-of {AS_OF.date()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load the estate and its operating state

# CELL ********************

eq_pdf = read_input("dim_equipment").toPandas()
fac_pdf = read_input("dim_facility").filter("is_current = true").toPandas()
area_pdf = read_input("dim_area").filter("is_current = true").toPandas()

assert (eq_pdf["topology_seed"] == TOPOLOGY_SEED).all(), (
    "dim_equipment was built with a different TOPOLOGY_SEED; rerun 01a-01c"
)
assert "area_sk" in eq_pdf.columns, "dim_equipment has no area_sk -- run 01c first"
assert set(eq_pdf["equipment_type"]) <= set(ROOT_CAUSE_WEIGHTS), (
    f"equipment types with no root-cause weights: "
    f"{sorted(set(eq_pdf['equipment_type']) - set(ROOT_CAUSE_WEIGHTS))}"
)

assets_pdf = eq_pdf.merge(area_pdf[["area_sk", "area_lat", "area_lon"]], on="area_sk",
                          how="left")
assert assets_pdf["area_lat"].notna().all(), (
    f"{int(assets_pdf['area_lat'].isna().sum())} asset(s) have no current dim_area row"
)
assets_pdf["install_date"] = pd.to_datetime(assets_pdf["install_date"])

# State from the history start: days_since_service needs every Maintenance interval before
# the first onset day, not only the ones inside the window.
state_pdf = (read_input("fact_asset_state")
             .select("equipment_sk", "state", "start_ts", "end_ts")
             .toPandas())
state_pdf["start_ts"] = pd.to_datetime(state_pdf["start_ts"])
state_pdf["end_ts"] = pd.to_datetime(state_pdf["end_ts"])

print(f"{len(assets_pdf):,} assets, {len(area_pdf)} areas, {len(fac_pdf)} facilities")
print(f"{len(state_pdf):,} state intervals for {state_pdf['equipment_sk'].nunique():,} assets")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Generate

# CELL ********************

ep = generate_episodes(assets_pdf, state_pdf, GEN_START, GEN_END, HISTORY_START)
ep = ep.sort_values(["equipment_sk", "start_ts"], kind="mergesort").reset_index(drop=True)

# In-run determinism: the same assets regenerated from scratch must reproduce their rows
# exactly. The harness checks the stronger properties (rerun, backfill = incremental).
_probe = assets_pdf.sort_values("equipment_sk").head(200)
_again = (generate_episodes(_probe, state_pdf, GEN_START, GEN_END, HISTORY_START)
          .sort_values(["equipment_sk", "start_ts"], kind="mergesort").reset_index(drop=True))
_first = ep[ep["equipment_sk"].isin(_probe["equipment_sk"])].reset_index(drop=True)
pd.testing.assert_frame_equal(_first, _again, check_exact=True)

print(f"{len(ep):,} episodes with onset {GEN_START.date()}..{(GEN_END - _DAY).date()}")
print(f"  truncated by a Maintenance or Down interval: {int(ep['_truncated'].sum()):,}")
print(f"OK  regenerating {len(_probe)} assets reproduces their {len(_first)} rows exactly")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation — every check fails the run, none warns

# CELL ********************

# --- inputs: nothing observational was read -------------------------------------------------
assert not TABLES_READ & set(OBSERVATION_TABLES), (
    f"observation table(s) read: {sorted(TABLES_READ & set(OBSERVATION_TABLES))}"
)
assert TABLES_READ <= set(INPUT_TABLES), f"undeclared input(s): {TABLES_READ - set(INPUT_TABLES)}"

# --- keys --------------------------------------------------------------------------------------
assert ep["episode_sk"].is_unique, "episode_sk not unique -- two onsets share (asset, second)"
_unknown = set(ep["equipment_sk"]) - set(eq_pdf["equipment_sk"])
assert not _unknown, f"equipment_sk not in dim_equipment: {sorted(_unknown)[:5]}"
for c in ("episode_sk", "equipment_sk", "area_sk", "facility_sk", "start_ts", "end_ts",
          "peak_rate_kg_h", "mean_rate_kg_h", "total_mass_kg", "root_cause", "date_sk"):
    assert ep[c].notna().all(), f"nulls in {c}"

# --- timestamps, durations, mass -----------------------------------------------------------------
assert (ep["end_ts"] > ep["start_ts"]).all(), "end_ts not after start_ts"
_dur = (ep["end_ts"] - ep["start_ts"]).dt.total_seconds() / 3600.0
assert np.allclose(_dur, ep["duration_hours"], rtol=0, atol=1e-9), \
    "duration_hours disagrees with the timestamps"
assert np.allclose(ep["mean_rate_kg_h"] * ep["duration_hours"], ep["total_mass_kg"],
                   rtol=1e-12, atol=0), "total_mass_kg != mean_rate_kg_h x duration_hours"
assert (ep["peak_rate_kg_h"] >= ep["mean_rate_kg_h"]).all(), "mean rate above peak rate"
assert (ep["above_tropomi_floor"] == (ep["peak_rate_kg_h"] > 3000.0)).all(), \
    "above_tropomi_floor disagrees with peak_rate_kg_h > 3000"
assert (ep["duration_hours"] <= EPISODE_MAX_DURATION_DAYS * 24.0 + 1e-9).all(), \
    "an episode outlasts EPISODE_MAX_DURATION_DAYS, so the incremental lookback is too short"

# --- window --------------------------------------------------------------------------------------
_outside = ep[(ep["start_ts"] < GEN_START) | (ep["start_ts"] >= GEN_END)]
assert _outside.empty, f"{len(_outside)} episode(s) start outside {GEN_START}..{GEN_END}"
assert ((ep["date_sk"] >= SK_LO) & (ep["date_sk"] <= SK_HI)).all(), \
    "a row's date_sk is outside this run's replaceWhere range"
assert (ep["date_sk"] == ep["start_ts"].dt.strftime("%Y%m%d").astype("int64")).all(), \
    "date_sk is not the start_ts day"

# --- root cause consistent with equipment type ---------------------------------------------
_bad = ep[[rc not in ROOT_CAUSE_WEIGHTS[et]
           for et, rc in zip(ep["equipment_type"], ep["root_cause"])]]
assert _bad.empty, (
    f"{len(_bad)} episode(s) with a root cause not allowed on their equipment type, e.g. "
    f"{_bad[['equipment_type', 'root_cause']].drop_duplicates().head().values.tolist()}"
)
for _c, _et in TYPE_EXCLUSIVE_CAUSES.items():
    _wrong = ep[(ep["root_cause"] == _c) & (ep["equipment_type"] != _et)]
    assert _wrong.empty, f"{len(_wrong)} '{_c}' episode(s) on something other than a {_et}"

# --- no episode overlaps Maintenance or Down on its own asset -----------------------------------
_stops = state_pdf[state_pdf["state"].isin(NON_EMITTING_STATES)].copy()
_stops["end_ns"] = np.where(_stops["end_ts"].isna(), _OPEN_NS,
                            _stops["end_ts"].values.astype("datetime64[ns]").astype("int64"))
_stops["start_ns"] = _stops["start_ts"].values.astype("datetime64[ns]").astype("int64")
_by = {k: (g["start_ns"].values, g["end_ns"].values) for k, g in _stops.groupby("equipment_sk")}
_overlaps = []
for r in ep[["equipment_sk", "start_ts", "end_ts", "episode_sk"]].itertuples(index=False):
    s = _by.get(r.equipment_sk)
    if s is None:
        continue
    e0, e1 = r.start_ts.value, r.end_ts.value
    if np.any((s[0] < e1) & (s[1] > e0)):
        _overlaps.append(r.episode_sk)
assert not _overlaps, (
    f"{len(_overlaps)} episode(s) overlap a Maintenance or Down interval on their own asset, "
    f"e.g. episode_sk {_overlaps[:3]}"
)

print("OK  read only " + ", ".join(sorted(TABLES_READ)) + "; no observation table")
print("OK  episode_sk unique; every equipment_sk resolves to dim_equipment; no nulls")
print("OK  end_ts > start_ts; duration_hours and total_mass_kg agree with the rates")
print(f"OK  every start_ts inside {GEN_START.date()}..{(GEN_END - _DAY).date()}; "
      "date_sk is the start day")
print("OK  root cause allowed for its equipment type; tank flashing, unlit flare and "
      "compressor blowby only on their own type")
print(f"OK  no episode overlaps a Maintenance or Down interval ({len(ep):,} checked)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Calibration report
#
# The above-floor subset is compared with what TROPOMI saw over 2026-08-16..2026-09-15. The
# reference values below are constants measured once, for comparison only. Nothing is fitted
# to them at run time, and the observed side is never adjusted.
#
# **Not a count fit.** TROPOMI sees an episode only if it is above the floor, a cloud-free
# overpass happens while it is active, the retrieval succeeds, and the plume is not rejected
# as a striping artifact (115 of 164 clusters on this window). Those probabilities belong to
# the detection layer. The count comparison below is a rough consistency check. The realised
# subset is also small, so the model's own above-floor distribution is printed next to it
# from a large sample of the same `draw_episode`.

# CELL ********************

OBSERVED = {"floor_kg_h": 3000.0, "median_kg_h": 19700.0, "max_kg_h": 71800.0,
            "n_plumes": 49, "scene_days": 14, "window_days": 30, "cams_median_kg_h": 48000.0}
DETECTION_WINDOW = (pd.Timestamp("2026-08-16"), pd.Timestamp("2026-09-16"))


def top_share(mass, share=0.05):
    s = np.sort(np.asarray(mass))[::-1]
    return float(s[:max(1, int(len(s) * share))].sum() / s.sum()) if s.sum() > 0 else float("nan")


_win = ep[ep["start_ts"] >= WINDOW_START]
_above = _win[_win["above_tropomi_floor"]]
_days = max((WINDOW_END - WINDOW_START) / _DAY, 1.0)
_asset_years = len(assets_pdf) * _days / 365.25

print("=" * 80)
print("EPISODE CALIBRATION")
print("=" * 80)
print(f"  window                  {WINDOW_START.date()} .. {WINDOW_END.date()} ({_days:.0f} days)")
print(f"  episodes                {len(_win):,}   ({len(_win) / _asset_years:.2f} per asset-year)")
print(f"  above 3 t/h             {len(_above):,}   ({len(_above) / max(len(_win), 1):.3%} of episodes)")
if len(_above):
    print(f"    median / max          {_above['peak_rate_kg_h'].median() / 1000:.1f} / "
          f"{_above['peak_rate_kg_h'].max() / 1000:.1f} t/h")
print(f"  top 5% by mass          hold {top_share(_win['total_mass_kg']):.1%} of total mass  "
      "(V1 target 50%+)")
print(f"  intermittent            {_win['is_intermittent'].mean():.1%}")
print(f"  truncated by a stop     {_win['_truncated'].mean():.1%}")

# The model's own above-floor distribution, from the same draw_episode over the realised mix
# of equipment types. A dedicated rng, so this sample never touches an episode's draws.
_mc_rng = get_rng("episode_rate_model_check")
_types = _win["equipment_type"].value_counts(normalize=True)
_mc = [draw_episode(_mc_rng, t)["peak_rate_kg_h"]
       for t in _mc_rng.choice(_types.index.values, size=200_000, p=_types.values)]
_mc = np.array(_mc)
_mc_above = _mc[_mc > TROPOMI_FLOOR_KG_H]
print()
print(f"  model, 200,000 draws    {len(_mc_above) / len(_mc):.3%} above 3 t/h; "
      f"above-floor median {np.median(_mc_above) / 1000:.1f} t/h, "
      f"p5 {np.percentile(_mc_above, 5) / 1000:.1f}, p95 {np.percentile(_mc_above, 95) / 1000:.1f}")
_n_like = int(OBSERVED["n_plumes"])
_mc_max49 = np.median([np.max(_mc_rng.choice(_mc_above, _n_like)) for _ in range(500)])
print(f"                          typical max of {_n_like} above-floor draws "
      f"{_mc_max49 / 1000:.1f} t/h")

print()
print("  observed, TROPOMI 2026-08-16..2026-09-15 (reference, not fitted)")
print(f"    median {OBSERVED['median_kg_h'] / 1000:.1f} t/h, max {OBSERVED['max_kg_h'] / 1000:.1f} "
      f"t/h, n = {OBSERVED['n_plumes']} over {OBSERVED['scene_days']} scene days; "
      f"CAMS median {OBSERVED['cams_median_kg_h'] / 1000:.0f} t/h")
_med_ratio = np.median(_mc_above) / OBSERVED["median_kg_h"]
print(f"    model above-floor median / observed median = {_med_ratio:.2f}")
if not 0.67 <= _med_ratio <= 1.5:
    print("    DOES NOT RESEMBLE the observed catalogue on the median -- the rate model, not")
    print("    the observed side, is what needs revisiting.")

# The tail-frequency target: above-floor episodes active during the detection window, and
# the days on which at least one was active. ACTIVE_IN_WINDOW_TARGET (50-100) is derived in
# the model cell: 10-20 estate-origin detections at ~0.2 detection per above-floor episode.
# Printed, not asserted: the count is a Poisson draw on the real estate, and the target
# is a judgement, not a measurement.
if WINDOW_START <= DETECTION_WINDOW[0] and WINDOW_END >= DETECTION_WINDOW[1] - _DAY:
    _act = ep[ep["above_tropomi_floor"] & (ep["start_ts"] < DETECTION_WINDOW[1])
              & (ep["end_ts"] > DETECTION_WINDOW[0])]
    _active_days = set()
    for r in _act.itertuples():
        for d in pd.date_range(max(r.start_ts, DETECTION_WINDOW[0]).normalize(),
                               min(r.end_ts, DETECTION_WINDOW[1]), freq="D"):
            if d < DETECTION_WINDOW[1]:
                _active_days.add(d)
    _lo, _hi = ACTIVE_IN_WINDOW_TARGET
    print()
    print(f"  active 2026-08-16..09-15 {len(_act)} above-floor episode(s), on "
          f"{len(_active_days)} of 31 days   (target {_lo}-{_hi}: "
          f"{'within' if _lo <= len(_act) <= _hi else 'OUTSIDE'})")
    print(f"    at ~0.2 detection each, ~{0.2 * len(_act):.0f} estate-origin detections, against "
          f"{OBSERVED['n_plumes']} plumes basin-wide on {OBSERVED['scene_days']} scene days")
    print("    (42 attributed to the estate, but at a median 21 km: attribution geometry, "
          "not origin)")

# --- episodes per facility --------------------------------------------------------------------
_fac = (_win.groupby("facility_id").size()
        .reindex(fac_pdf["facility_id"], fill_value=0).rename("episodes").to_frame())
_fac["assets"] = eq_pdf.groupby("facility_id").size().reindex(_fac.index).fillna(0).astype(int)
_fac = _fac.sort_values("episodes", ascending=False)
print()
print(f"  episodes per facility   min {_fac['episodes'].min()}  median "
      f"{_fac['episodes'].median():.0f}  p90 {_fac['episodes'].quantile(0.9):.0f}  "
      f"max {_fac['episodes'].max()}   ({int((_fac['episodes'] == 0).sum())} with none)")
print("    top 5:  " + ", ".join(f"{fid} {r.episodes} ({r.assets} assets)"
                               for fid, r in _fac.head(5).iterrows()))

print()
print("  by root cause")
_rc = _win.groupby("root_cause").agg(n=("episode_sk", "size"),
                                     above=("above_tropomi_floor", "sum"),
                                     mass_t=("total_mass_kg", lambda s: s.sum() / 1000))
_rc["mass_share"] = _rc["mass_t"] / _rc["mass_t"].sum()
for rc, r in _rc.sort_values("n", ascending=False).iterrows():
    print(f"    {rc:<18}{int(r.n):>6,}   above floor {int(r.above):>3}   "
          f"mass {r.mass_share:>6.1%}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write `fact_emission_episode`

# CELL ********************

EPISODE_SCHEMA = ["episode_sk", "equipment_sk", "equipment_id", "area_sk", "facility_sk",
                  "facility_id", "source_lat", "source_lon", "start_ts", "end_ts",
                  "duration_hours", "peak_rate_kg_h", "mean_rate_kg_h", "total_mass_kg",
                  "root_cause", "is_intermittent", "above_tropomi_floor", "date_sk",
                  "is_synthetic"]

out = ep[EPISODE_SCHEMA].copy()
out_spark = spark.createDataFrame(
    out,
    "episode_sk long, equipment_sk long, equipment_id string, area_sk long, facility_sk long, "
    "facility_id string, source_lat double, source_lon double, start_ts timestamp, "
    "end_ts timestamp, duration_hours double, peak_rate_kg_h double, mean_rate_kg_h double, "
    "total_mass_kg double, root_cause string, is_intermittent boolean, "
    "above_tropomi_floor boolean, date_sk long, is_synthetic boolean",
)

if table_exists(TABLE):
    writer = (out_spark.write.format("delta").mode("overwrite")
              .option("replaceWhere", f"date_sk >= {SK_LO} AND date_sk <= {SK_HI}")
              .partitionBy("date_sk"))
else:
    # replaceWhere needs an existing table; the first write creates it.
    writer = (out_spark.write.format("delta").mode("overwrite")
              .option("overwriteSchema", "true").partitionBy("date_sk"))
    print(f"{TABLE} does not exist -- creating it with a plain partitioned overwrite")
writer.saveAsTable(TABLE)

_written = spark.table(TABLE).filter(f"date_sk >= {SK_LO} AND date_sk <= {SK_HI}").count()
assert _written == len(out), f"{TABLE} holds {_written} rows in {SK_LO}..{SK_HI}, wrote {len(out)}"
print(f"{TABLE}: {len(out):,} rows written (replaceWhere date_sk {SK_LO}..{SK_HI}, "
      "partitioned by date_sk)")

display(out_spark.orderBy(F.desc("peak_rate_kg_h")).limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary

# CELL ********************

print("=" * 76)
print("EMISSION EPISODES GENERATED")
print("=" * 76)
print(f"  run mode          {RUN_MODE}")
print(f"  window            {WINDOW_START.date()} .. {WINDOW_END.date()}")
print(f"  regenerated       onset days {GEN_START.date()} .. {(GEN_END - _DAY).date()}")
print(f"  episodes written  {len(out):,}")
print(f"  above 3 t/h       {int(out['above_tropomi_floor'].sum()):,}")
print()
print(f"  table written     {TABLE} (one row per episode, partitioned by date_sk)")
print(f"  read              {', '.join(sorted(TABLES_READ))}")
print("  not read          any observation table -- plumes, signatures, telemetry")
print("  not modified      every dim_* table, fact_asset_state")
print("  not generated     plumes, telemetry, work orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
