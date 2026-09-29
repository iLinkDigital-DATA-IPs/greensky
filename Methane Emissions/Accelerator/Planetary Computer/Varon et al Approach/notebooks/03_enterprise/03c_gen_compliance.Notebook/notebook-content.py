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

# # 03c — Generate Compliance Events
#
# Writes **`dim_regulation`** (the rules events are evaluated against) and
# **`fact_compliance_event`** (one row per reportable event, carrying its case status).
#
# ### What a regulator sees
# A regulator sees reports and observations, never the leak itself. So every event here comes
# from something observed: an attributed TROPOMI plume, a sustained run on a facility's own
# CH₄ detectors, or a standing alarm on a flare. **`fact_emission_episode` is never read.** It
# is the hidden ground truth. A compliance event sourced from it would make every later claim
# that a regulator acted on a real release circular. Every read goes through `read_input()`,
# which refuses the table, and `tools/harness/harness_compliance.py` checks statically that
# the name appears nowhere in the code outside the refusal list.
#
# ### Regulatory framing, checked 2026-09-29
# Every figure below is either a rule, with where it comes from, or a **modelling choice**,
# labelled as one. They were checked against public sources on 2026-09-29, not taken from V1,
# and should be re-checked before anything here is shown as current law.
#
# - **EPA Waste Emissions Charge.** The implementing rule was disapproved under the
#   Congressional Review Act (joint resolution passed February–March 2025, signed). The One Big
#   Beautiful Bill Act (signed 2025-07-04) then delayed the statutory charge itself to calendar
#   year 2034. So **no federal per-tonne fee applies** to this window, and the default fine
#   scenario is reporting-only. V1's comment also said EPA "revoked" the rule in May 2025.
#   That specific step was **not** verified and is not relied on here.
# - **"Other Large Release Events" is a GHGRP Subpart W provision, not NSPS OOOOb.** The 2024
#   Subpart W revisions (40 CFR Part 98) require reporting of releases at an instantaneous
#   methane rate of **100 kg/h** or more. EPA dropped the proposed 250 tCO2e per-event
#   alternative. It is **reporting**: an OLRE is reported, not a violation in itself. EPA
#   proposed in September 2025 to suspend Subpart W reporting until reporting year 2034. As of
#   the February 2026 deadline-extension rule that proposal was **not final**, and the RY2025
#   report deadline moved to 2026-10-30.
# - **NSPS OOOOb super-emitter program** (40 CFR 60.5371b). A super-emitter event is ≥100 kg/h
#   of methane, detected remotely, at a well site or compressor station. On an EPA
#   notification the operator must start an investigation within 5 days and report within 15.
#   An interim final rule (effective 2025-07-31, finalised 2025-11-26) **postponed the
#   program to 2027-01-22**, so it is not in force in this window. It would not fit these
#   plumes even if it were: the notification is tied to a facility within 50 m of the
#   reported location, and a TROPOMI pixel is ~5.5 km, attributed here at a median of 21 km.
# - **Texas RRC Statewide Rule 32** (16 TAC §3.32) governs venting and flaring. Gas must be
#   put to authorised use, and flaring or venting needs an exception except in listed cases
#   (for example, during drilling and up to 10 days after completion for testing). Penalties
#   are assessed under Tex. Nat. Res. Code §81.0531–81.0533. **This notebook models no
#   specific SWR 32 exception or penalty figure.** The state path uses modelling thresholds,
#   labelled as such.
#
# ### Reports are many; violations are few
# TROPOMI only sees releases of roughly 3 t/h and up, 30 times the 100 kg/h reporting threshold,
# so any real rate threshold is crossed by everything it detects. **Every attributed plume is
# a reportable OLRE**, and none is a violation for its rate alone. A plume escalates to a
# **state case** only on evidence beyond its rate:
#
# - **Repetition**: an earlier detection within 5 km of the same source location, attributed
#   to the same facility, in the trailing 30 days. A release reported once and seen again was
#   not fixed. Repetition is judged on **location**, not facility. Attributing at a median
#   21 km means one nearby facility collects plumes from several distinct sources. On the
#   estate's own geometry, 38 attributions land on 25 facilities, up to 4 at one. A
#   same-facility rule would escalate that attribution geometry, not a repeat release. 5 km
#   is 06's `persistence_match_radius_km`, so this agrees with how `gold_emission_sites`
#   groups detections.
# - **Persistence**: the facility's own CH₄ detectors have been in a continuous exceedance
#   run for at least 24 h at the moment of detection. This is on-site corroboration, not an
#   inference from a 5.5 km pixel.
#
# A state case is also opened when a flare alarm stands for 24 h: unlit (stack temperature
# below its trip) for venting, or over-range flow for flaring. Each case is then reviewed, and
# most, not all, end in a violation. The fine appears only if the review finds one.
#
# **Violation means a notice of violation issued at inspection** (7–35 days after the report,
# a modelling choice). The formal penalty process that can follow takes months and is not
# modelled. `fine_usd` is the proposed penalty on the notice. Modelling the full process would
# leave no decided case inside a 30-day retention, and every dashboard tile would read zero.
#
# ### Status progresses: design note §2.3
# The event is point-in-time; its status is not: **Reported → Under Review → Closed**, or
# **→ Violation**. The whole progression is drawn once at creation from
# `get_rng("compliance", compliance_id)`: the report delay, the review duration, the outcome,
# the fine, and whether the case stalls. Pass 1 advances cases whose time has come. A share
# of cases stall Under Review, since regulatory cases do. A stall that never exits would
# grow the pool without bound and make any open-cases KPI climb forever, so pass 1
# **cancels** a case still undecided `CE_CANCEL_AFTER_DAYS` (120) after its event: 03b's
# 60-day rule, on regulatory timescales. Cancelled cases are never counted as open, and are
# reported separately. Stalled cases are excluded from
# the steady-state checks and reported separately.
#
# ### Writes
# `dim_regulation` (whole-table overwrite; four rows) and `fact_compliance_event`, partitioned
# by `date_sk` with `replaceWhere`. `gold_plume_catalog`, every other `dim_*` table and every
# telemetry table are read and never modified.

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

# ### Inputs, and the guard against reading the ground truth

# CELL ********************

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

REG_TABLE = "dim_regulation"
CE_TABLE = "fact_compliance_event"

INPUT_TABLES = ("dim_facility", "dim_equipment", "dim_scada_tag", "dim_sensor",
                "gold_plume_catalog", "gold_multi_gas_signatures", "sensor_telemetry",
                "scada_telemetry", "fact_scada_alarm", CE_TABLE)

# The hidden ground truth. A regulator sees reports and observations, not the release itself.
GROUND_TRUTH_TABLES = ("fact_emission_episode",)
assert not set(INPUT_TABLES) & set(GROUND_TRUTH_TABLES), "an input is a ground-truth table"

TABLES_READ = set()


def read_input(name):
    """The only way this notebook reads a table."""
    assert name not in GROUND_TRUTH_TABLES, (
        f"{name} is hidden ground truth. Compliance events come from reports and observations "
        "only; reading it would make every 'the regulator acted on the release' claim circular."
    )
    assert name in INPUT_TABLES, f"{name} is not a declared input of 03c: {INPUT_TABLES}"
    TABLES_READ.add(name)
    return spark.table(name)


def table_exists(name):
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False


print(f"inputs            {', '.join(INPUT_TABLES)}")
print(f"refused           {', '.join(GROUND_TRUTH_TABLES)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### The compliance model
#
# This cell is pure: no Spark, no table, no clock. `tools/harness/harness_compliance.py`
# executes it verbatim against `01_topology_config`.
#
# **Every event is knowable at its `event_ts`** from data that exists by then:
#
# - An OLRE, at the plume's detection.
# - A state case, at the same instant, with repetition judged on earlier detections only and
#   persistence on detector readings up to it.
# - A flare case, 24 h after the alarm raised, if it has not cleared.
# - A facility exceedance, at its 24th consecutive flagged hour.
#
# That is what lets an incremental run see the same events for the same day as a backfill.

# CELL ********************

# ---- 03c compliance model (pure: tools/harness/harness_compliance.py executes this cell) ----
EVENT_TYPES = ("OLRE", "Exceedance", "Venting", "Flaring")
EVENT_ORDER = {t: i for i, t in enumerate(EVENT_TYPES)}      # tie-break at one instant
CE_STATUSES = ("Reported", "Under Review", "Closed", "Violation", "Cancelled")
CE_DECIDED = ("Closed", "Violation", "Cancelled")   # terminal: never counted as open
CE_OPEN_STATUSES = ("Reported", "Under Review")
SEVERITIES = ("Critical", "Major", "Minor")

# ---- dim_regulation ----------------------------------------------------------------------------
# threshold_basis / fine_basis say, per row, whether a number is a RULE or a MODELLING CHOICE.
# fine_min/max are modelling choices everywhere they are non-zero: V1 carried $5,000-$75,000
# for a "discrete NSPS/State violation" with no source, and no assessed-penalty figure was
# verified for either programme. Statutory maxima (CAA per-day civil penalties under 40 CFR
# Part 19; Tex. Nat. Res. Code §81.0531) are ceilings, not typical assessed penalties, and are
# deliberately not used. Treat every fine here as obviously synthetic.
MODELLED_FINE_RANGE_USD = (5_000.0, 75_000.0)
REGULATIONS = [
    {"regulation_id": "EPA_GHGRP_W_OLRE",
     "regulation_name": "GHGRP Subpart W - Other Large Release Events",
     "jurisdiction": "Federal (EPA, 40 CFR Part 98 Subpart W)",
     "threshold_value": 100.0, "threshold_unit": "kg_CH4_per_h",
     "threshold_basis": "rule: instantaneous CH4 rate >= 100 kg/h (2024 Subpart W revisions)",
     "reporting_window_hours": None,     # reported in the annual GHGRP report, not per event
     "fine_min_usd": 0.0, "fine_max_usd": 0.0,
     "fine_basis": "reporting provision: an OLRE is reported, not fined",
     "effective_from": "2025-01-01", "is_active": True,
     "status_note": "suspension to RY2034 proposed 2025-09, not final as of 2026-02; "
                    "RY2025 report due 2026-10-30"},
    {"regulation_id": "EPA_NSPS_OOOOb_SEP",
     "regulation_name": "NSPS OOOOb - Super-Emitter Program",
     "jurisdiction": "Federal (EPA, 40 CFR 60.5371b)",
     "threshold_value": 100.0, "threshold_unit": "kg_CH4_per_h",
     "threshold_basis": "rule: remotely detected >= 100 kg/h at a well site or compressor "
                        "station, within 50 m of the notified location",
     "reporting_window_hours": 360.0,    # rule: investigate within 5 days, report within 15
     "fine_min_usd": MODELLED_FINE_RANGE_USD[0], "fine_max_usd": MODELLED_FINE_RANGE_USD[1],
     "fine_basis": "modelling choice (V1 range, unsourced)",
     "effective_from": "2027-01-22", "is_active": False,
     "status_note": "postponed to 2027-01-22 by interim final rule; TROPOMI attribution at "
                    "km scale cannot meet the 50 m location test in any case"},
    {"regulation_id": "TX_RRC_SWR32",
     "regulation_name": "Texas RRC Statewide Rule 32 - venting and flaring",
     "jurisdiction": "State (Texas RRC, 16 TAC 3.32)",
     "threshold_value": 24.0, "threshold_unit": "hours_sustained",
     "threshold_basis": "modelling choice: a case opens on repetition, 24 h of on-site "
                        "persistence, or a flare condition standing 24 h; SWR 32 itself "
                        "sets no kg/h or duration trigger that was verified",
     "reporting_window_hours": 48.0,     # modelling choice
     "fine_min_usd": MODELLED_FINE_RANGE_USD[0], "fine_max_usd": MODELLED_FINE_RANGE_USD[1],
     "fine_basis": "modelling choice (V1 range, unsourced); penalties are assessed under "
                   "Tex. Nat. Res. Code 81.0531, no figure used",
     "effective_from": "2021-01-01", "is_active": True,
     "status_note": "state path models escalation to a case and a notice of violation; not "
                   "any specific SWR 32 exception, and not the formal penalty process"},
    {"regulation_id": "GS_OPERATOR_CMS",
     "regulation_name": "Operator continuous-monitoring action level (synthetic)",
     "jurisdiction": "Operator (internal programme)",
     "threshold_value": 24.0, "threshold_unit": "hours_sustained",
     "threshold_basis": "modelling choice: 24 consecutive flagged hours on a facility's "
                        "CH4 detectors; not a regulation",
     "reporting_window_hours": 24.0,     # modelling choice
     "fine_min_usd": 0.0, "fine_max_usd": 0.0,
     "fine_basis": "internal programme: no fines",
     "effective_from": "2025-01-01", "is_active": True,
     "status_note": "records on-site persistence; never a violation by itself"},
]
REG_BY_ID = {r["regulation_id"]: r for r in REGULATIONS}
REG_SK = {r["regulation_id"]: stable_key("regulation", r["regulation_id"]) for r in REGULATIONS}
REG_OF_EVENT = {"OLRE": "EPA_GHGRP_W_OLRE", "Exceedance": "GS_OPERATOR_CMS",
                "Venting": "TX_RRC_SWR32", "Flaring": "TX_RRC_SWR32"}

# ---- fine scenarios ------------------------------------------------------------------------------
# reporting_only is the default and current law: no federal per-tonne fee; discrete state
# violation fines only, from the modelled range above. The others are switchable and off.
# wec_hypothetical carries the IRA's statutory schedule ($900 / $1,200 / $1,500 per tonne CH4
# for 2024 / 2025 / 2026 emissions, for facilities reporting over 25,000 tCO2e/yr under
# Subpart W). It is a facility-year charge, not a per-event fine, so 03c records the scenario
# and leaves the calculation to the financial layer. social_cost is V1's ESG shadow price,
# $1,600/t CH4, carried from V1 and NOT re-verified.
FINE_SCENARIOS = {
    "reporting_only":   {"per_tonne_usd": None, "discrete_fine_range_usd": MODELLED_FINE_RANGE_USD},
    "wec_hypothetical": {"per_tonne_usd": {2024: 900, 2025: 1200, 2026: 1500},
                         "applicability_tco2e_per_year": 25_000,
                         "discrete_fine_range_usd": MODELLED_FINE_RANGE_USD},
    "social_cost":      {"per_tonne_usd": 1600, "discrete_fine_range_usd": MODELLED_FINE_RANGE_USD},
}
ACTIVE_FINE_SCENARIO = "reporting_only"

# ---- escalation rules: modelling choices -----------------------------------------------------------
OLRE_THRESHOLD_KG_H = REG_BY_ID["EPA_GHGRP_W_OLRE"]["threshold_value"]   # rule, 100 kg/h
REPEAT_WINDOW_DAYS = 30             # an earlier detection this recently = not fixed
REPEAT_RADIUS_KM = float(CONFIG["persistence_match_radius_km"])   # 06's site radius, 5 km
PERSISTENCE_HOURS = 24.0            # facility detectors in a run this long at detection
FLARE_STANDING_HOURS = 24.0         # a flare alarm standing this long opens a case
EXCEEDANCE_EVENT_HOURS = 24         # consecutive flagged hours for an operator event
# Flare alarms that open a case: an unlit flare vents (stack temperature below its trip),
# over-range flow is excess flaring.
FLARE_CASE_ALARMS = {("stack_temperature", "LoLo"): "Venting", ("flow", "HiHi"): "Flaring"}
# Severity of a state case: Critical on a large or repeated release, else Major. Reports and
# operator events are Minor. Modelling choice.
CRITICAL_RATE_KG_H = 50_000.0       # 50 t/h: the top of what TROPOMI has seen here (max 71.8)
CRITICAL_REPEATS = 2                # two or more earlier detections in the window

# ---- the status progression, drawn once at creation: modelling choices -------------------------------
REPORT_DELAY_H = {"OLRE": (24.0, 72.0), "Exceedance": (1.0, 24.0),
                  "Venting": (4.0, 48.0), "Flaring": (4.0, 48.0)}
REVIEW_DAYS = {"OLRE": (5.0, 20.0), "Exceedance": (2.0, 10.0),
               "Venting": (7.0, 35.0), "Flaring": (7.0, 35.0)}   # state: to notice of violation
STALL_SHARE = {"OLRE": 0.03, "Exceedance": 0.03, "Venting": 0.10, "Flaring": 0.10}
VIOLATION_AFFIRM_SHARE = 0.80       # state cases that end in a violation; the rest close
MAX_REVIEW_DAYS = 120               # no non-stalled case may sit Under Review longer
# The stalled-case exit: a case undecided this many days after its event is Cancelled by
# pass 1, with the instant fixed at creation (event_ts + this), like every other transition.
# 120 days rather than 03b's 60: a regulatory review legitimately runs for months, and a
# notice-of-violation case here decides within ~37 days, so 120 only ever catches a stall.
# With the exit, stalled-and-open converges to (stall arrivals per day) x 120 instead of
# growing without bound.
CE_CANCEL_AFTER_DAYS = 120

# ---- steady state -----------------------------------------------------------------------------------
STEADY_BAND = (0.4, 2.5)
TREND_WINDOW_DAYS = 14
TREND_MAX_RISE = 0.25
WARMUP_DAYS = 60                    # > the longest planned review, so the population has levelled

CE_COLS = ["compliance_sk", "compliance_id", "facility_sk", "facility_id", "equipment_sk",
           "regulation_sk", "event_ts", "event_type", "source_ref", "measured_value",
           "threshold_value", "threshold_unit", "is_violation", "severity", "fine_usd",
           "status", "status_ts", "reported_ts", "date_sk", "is_synthetic"]
SOURCE_COLS = ["event_ts", "event_type", "source_ref", "facility_sk", "equipment_sk",
               "measured_value", "threshold_value", "threshold_unit", "severity", "escalated"]

assert ACTIVE_FINE_SCENARIO in FINE_SCENARIOS
assert set(REG_OF_EVENT) == set(EVENT_TYPES) and set(REG_OF_EVENT.values()) <= set(REG_BY_ID)
assert len(set(REG_SK.values())) == len(REG_SK), "regulation_sk collision"
for _t in EVENT_TYPES:
    assert REVIEW_DAYS[_t][1] + REPORT_DELAY_H[_t][1] / 24.0 < WARMUP_DAYS < MAX_REVIEW_DAYS
    assert 0.0 <= STALL_SHARE[_t] < 0.5
    assert CE_CANCEL_AFTER_DAYS > REVIEW_DAYS[_t][1] + REPORT_DELAY_H[_t][1] / 24.0, (
        f"{_t}: the stalled exit would cut into ordinary reviews")
for _r in REGULATIONS:
    assert 0.0 <= _r["fine_min_usd"] <= _r["fine_max_usd"]

_H = pd.Timedelta(hours=1)
_DAY = pd.Timedelta(days=1)


def _typed(rows):
    """Candidate frame with exact dtypes: source_ref is a string (a plume_id is hex text),
    equipment_sk a nullable 63-bit key, which must never pass through float64."""
    df = pd.DataFrame(rows, columns=SOURCE_COLS)
    df["event_ts"] = pd.to_datetime(pd.Series([r[0] for r in rows], dtype="datetime64[ns]"))
    df["facility_sk"] = pd.Series([int(r[3]) for r in rows], dtype="int64")
    df["equipment_sk"] = pd.array([None if r[4] is None else int(r[4]) for r in rows],
                                  dtype="Int64")
    for c in ("measured_value", "threshold_value"):
        df[c] = df[c].astype("float64")
    df["escalated"] = df["escalated"].astype(bool)
    return df


def facility_runs(flags, sensors, cadence_s):
    """Facility-level CH4 exceedance runs: an hour is flagged at a facility when any of its
    detectors is flagged, and a run breaks on a missing hour -- 02e's run definition, lifted
    to the facility. flags: (sensor_id, reading_ts) of flagged readings."""
    cols = ["facility_sk", "run_start", "run_end", "run_len", "nth_ts", "lead_equipment_sk",
            "run_key"]
    if flags.empty:
        return pd.DataFrame(columns=cols)
    s = sensors[["sensor_id", "facility_sk", "equipment_sk"]]
    f = flags.merge(s, on="sensor_id", how="inner")
    hrs = (f.groupby(["facility_sk", "reading_ts"]).size().reset_index()[["facility_sk",
                                                                          "reading_ts"]]
           .sort_values(["facility_sk", "reading_ts"], kind="mergesort").reset_index(drop=True))
    prev = hrs.groupby("facility_sk")["reading_ts"].shift(1)
    hrs["run"] = (prev.isna() | ((hrs["reading_ts"] - prev) != pd.Timedelta(seconds=cadence_s))
                  ).cumsum()
    hrs["pos"] = hrs.groupby("run").cumcount() + 1
    g = hrs.groupby("run")
    runs = pd.DataFrame({"facility_sk": g["facility_sk"].first(),
                         "run_start": g["reading_ts"].first(),
                         "run_end": g["reading_ts"].last() + pd.Timedelta(seconds=cadence_s),
                         "run_len": g.size()})
    nth = hrs[hrs["pos"] == EXCEEDANCE_EVENT_HOURS].set_index("run")["reading_ts"]
    runs["nth_ts"] = nth.reindex(runs.index)
    # the asset whose detector carried the most flagged hours up to the event -- observable
    lead = {}
    fr = f.merge(hrs[["facility_sk", "reading_ts", "run"]], on=["facility_sk", "reading_ts"])
    for k, gg in fr.groupby("run"):
        cut = runs.at[k, "nth_ts"]
        sub = gg if pd.isna(cut) else gg[gg["reading_ts"] <= cut]
        top = (sub.groupby(["sensor_id", "equipment_sk"]).size().reset_index(name="n")
               .sort_values(["n", "sensor_id"], ascending=[False, True], kind="mergesort"))
        lead[k] = int(top["equipment_sk"].iloc[0])
    runs["lead_equipment_sk"] = pd.Series(lead)
    runs["run_key"] = [str(stable_key("facility_ch4_run", int(fs), t.isoformat()))
                       for fs, t in zip(runs["facility_sk"], runs["run_start"])]
    return runs.reset_index(drop=True)[cols]


def plume_sources(plumes, runs, facility_sk_of):
    """OLRE reports for every attributed plume at or over the reporting threshold, and a state
    case for each one escalated by repetition or on-site persistence.

    plumes: (plume_id, detect_ts, emission_rate_kg_h, source_lat, source_lon,
    attributed_facility_id, signature).
    Returns (candidates, skipped) where skipped counts plumes with no operator to cite."""
    att = plumes[plumes["attributed_facility_id"].notna()].copy()
    skipped = {"unattributed": int(plumes["attributed_facility_id"].isna().sum()),
               "below_threshold": int((att["emission_rate_kg_h"] < OLRE_THRESHOLD_KG_H).sum()),
               "escalated_repeat": 0, "escalated_persistence": 0, "escalated_both": 0}
    att = att[att["emission_rate_kg_h"] >= OLRE_THRESHOLD_KG_H]
    att["facility_sk"] = att["attributed_facility_id"].map(facility_sk_of)
    assert att["facility_sk"].notna().all(), "a plume attributed to a facility not in dim_facility"
    att = att.sort_values(["detect_ts", "plume_id"], kind="mergesort")
    win = pd.Timedelta(days=REPEAT_WINDOW_DAYS)
    rows = []
    for p in att.to_dict("records"):
        fs, t, rate = int(p["facility_sk"]), pd.Timestamp(p["detect_ts"]), float(p["emission_rate_kg_h"])
        same = att[(att["facility_sk"] == fs) & (att["detect_ts"] < t) & (att["detect_ts"] >= t - win)]
        km = haversine_km(float(p["source_lat"]), float(p["source_lon"]),
                          same["source_lat"].values, same["source_lon"].values)
        repeats = int((np.asarray(km) <= REPEAT_RADIUS_KM).sum())
        r = runs[runs["facility_sk"] == fs] if len(runs) else runs
        persistent = bool(len(r) and ((r["run_start"] <= t - pd.Timedelta(hours=PERSISTENCE_HOURS))
                                      & (r["run_end"] > t)).any())
        rows.append((t, "OLRE", str(p["plume_id"]), fs, None, rate, OLRE_THRESHOLD_KG_H,
                     "kg_CH4_per_h", "Minor", False))
        if repeats >= 1 or persistent:
            skipped["escalated_" + ("both" if repeats >= 1 and persistent
                                    else "repeat" if repeats >= 1 else "persistence")] += 1
            kind = "Flaring" if p.get("signature") == "Incomplete Combustion" else "Venting"
            sev = ("Critical" if rate >= CRITICAL_RATE_KG_H or repeats >= CRITICAL_REPEATS
                   else "Major")
            rows.append((t, kind, str(p["plume_id"]), fs, None, rate, OLRE_THRESHOLD_KG_H,
                         "kg_CH4_per_h", sev, True))
    return _typed(rows), skipped


def flare_sources(alarms, horizon):
    """State cases from flare alarms still standing FLARE_STANDING_HOURS after they raised.
    alarms: fact_scada_alarm rows joined to the tag (alarm_sk, equipment_sk, facility_sk,
    equipment_type, tag_name, alarm_type, raised_ts, cleared_ts)."""
    a = alarms[alarms["equipment_type"] == "Flare"].copy()
    a["kind"] = [FLARE_CASE_ALARMS.get((tn, at)) for tn, at in zip(a["tag_name"], a["alarm_type"])]
    a = a[a["kind"].notna()]
    stand = pd.Timedelta(hours=FLARE_STANDING_HOURS)
    a["event_ts"] = a["raised_ts"] + stand
    a = a[(a["event_ts"] < horizon) & (a["cleared_ts"].isna() | (a["cleared_ts"] > a["event_ts"]))]
    rows = []
    for r in a.to_dict("records"):
        rows.append((r["event_ts"], r["kind"], str(int(r["alarm_sk"])), int(r["facility_sk"]),
                     int(r["equipment_sk"]), float(FLARE_STANDING_HOURS), FLARE_STANDING_HOURS,
                     "hours_sustained", "Major", True))
    return _typed(rows)


def exceedance_sources(runs):
    """Operator events for facility runs reaching EXCEEDANCE_EVENT_HOURS consecutive hours."""
    r = runs[runs["run_len"] >= EXCEEDANCE_EVENT_HOURS] if len(runs) else runs
    rows = [(x["nth_ts"], "Exceedance", x["run_key"], int(x["facility_sk"]),
             int(x["lead_equipment_sk"]), float(EXCEEDANCE_EVENT_HOURS),
             float(EXCEEDANCE_EVENT_HOURS), "hours_sustained", "Minor", False)
            for x in r.to_dict("records")]
    return _typed(rows)


def build_sources(plumes, runs, alarms, facility_sk_of, horizon):
    """Every event knowable by the horizon, in one deterministic order."""
    p, skipped = plume_sources(plumes, runs, facility_sk_of)
    parts = [x for x in (p, flare_sources(alarms, horizon), exceedance_sources(runs)) if len(x)]
    s = pd.concat(parts, ignore_index=True) if parts else _typed([])
    s = s[s["event_ts"] < horizon].copy()
    s["_o"] = s["event_type"].map(EVENT_ORDER)
    s = (s.sort_values(["event_ts", "_o", "source_ref"], kind="mergesort")
          .drop(columns="_o").reset_index(drop=True))
    assert not s.duplicated(["event_type", "source_ref"]).any(), "one source raised two events"
    return s[SOURCE_COLS], skipped


def event_plan(compliance_id, event_type, escalated, severity):
    """The whole future of a case, drawn once in a fixed order and never re-drawn."""
    rng = get_rng("compliance", compliance_id)
    stalled = bool(rng.random() < STALL_SHARE[event_type])
    d_lo, d_hi = REPORT_DELAY_H[event_type]
    report_s = int(round(float(rng.uniform(d_lo, d_hi)) * 3600.0))
    r_lo, r_hi = REVIEW_DAYS[event_type]
    review_s = int(round(float(rng.uniform(r_lo, r_hi)) * 86400.0))
    affirm = bool(rng.random() < VIOLATION_AFFIRM_SHARE)
    u_fine = float(rng.random())
    outcome = "Violation" if (escalated and affirm) else "Closed"
    reg = REG_BY_ID[REG_OF_EVENT[event_type]]
    lo, hi = reg["fine_min_usd"], reg["fine_max_usd"]
    # Critical fines in the upper half of the modelled range, Major in the lower half
    band = (lo + 0.5 * (hi - lo), hi) if severity == "Critical" else (lo, lo + 0.5 * (hi - lo))
    fine = round(band[0] + u_fine * (band[1] - band[0]), 2) if outcome == "Violation" else 0.0
    return {"is_stalled": stalled, "report_s": report_s, "review_s": review_s,
            "outcome": outcome, "fine_usd": fine}


def make_event(number, s):
    ce_id = f"CE-{number:06d}"
    plan = event_plan(ce_id, s["event_type"], bool(s["escalated"]), s["severity"])
    return attach_plan({
        "compliance_sk": stable_key("compliance", s["event_type"], s["source_ref"]),
        "compliance_id": ce_id, "number": number,
        "facility_sk": int(s["facility_sk"]),
        "equipment_sk": None if pd.isna(s["equipment_sk"]) else int(s["equipment_sk"]),
        "regulation_sk": REG_SK[REG_OF_EVENT[s["event_type"]]],
        "event_ts": pd.Timestamp(s["event_ts"]), "event_type": s["event_type"],
        "source_ref": s["source_ref"], "measured_value": float(s["measured_value"]),
        "threshold_value": float(s["threshold_value"]), "threshold_unit": s["threshold_unit"],
        "severity": s["severity"], "escalated": bool(s["escalated"]),
    }, plan)


def attach_plan(e, plan):
    e = dict(e)
    e["plan"] = plan
    e["reported_ts"] = e["event_ts"] + pd.Timedelta(seconds=plan["report_s"])
    e["decision_ts"] = (None if plan["is_stalled"]
                        else e["reported_ts"] + pd.Timedelta(seconds=plan["review_s"]))
    e["cancel_ts"] = e["event_ts"] + pd.Timedelta(days=CE_CANCEL_AFTER_DAYS)
    if e["decision_ts"] is not None and e["decision_ts"] >= e["cancel_ts"]:
        e["decision_ts"] = None     # a review past the exit is cancelled like a stall
    return e


def status_as_of(e, horizon):
    """(status, status_ts) at the horizon. The only place status is decided."""
    if e["decision_ts"] is not None and e["decision_ts"] < horizon:
        return e["plan"]["outcome"], e["decision_ts"]
    if e["decision_ts"] is None and e["cancel_ts"] < horizon:
        return "Cancelled", e["cancel_ts"]
    if e["reported_ts"] < horizon:
        return "Under Review", e["reported_ts"]
    return "Reported", e["event_ts"]


def row_as_of(e, horizon, facility_id_of):
    status, sts = status_as_of(e, horizon)
    viol = status == "Violation"
    return {"compliance_sk": e["compliance_sk"], "compliance_id": e["compliance_id"],
            "facility_sk": e["facility_sk"], "facility_id": facility_id_of[e["facility_sk"]],
            "equipment_sk": e["equipment_sk"], "regulation_sk": e["regulation_sk"],
            "event_ts": e["event_ts"], "event_type": e["event_type"],
            "source_ref": e["source_ref"], "measured_value": e["measured_value"],
            "threshold_value": e["threshold_value"], "threshold_unit": e["threshold_unit"],
            "is_violation": viol, "severity": e["severity"],
            "fine_usd": e["plan"]["fine_usd"] if viol else 0.0,
            "status": status, "status_ts": sts, "reported_ts": e["reported_ts"],
            "date_sk": int(e["event_ts"].strftime("%Y%m%d")), "is_synthetic": True}


def changes_in(e, lo, hi):
    inst = [e["event_ts"], e["reported_ts"],
            e["decision_ts"] if e["decision_ts"] is not None else e["cancel_ts"]]
    return any(lo <= x < hi for x in inst)


def prior_from_table(ce_rows, window_start):
    """Pass 1's read: cases created before the window and not decided before it. Selected on
    timestamps, not on status, because after a rerun the stored rows are as of a later
    horizon (03b's lesson). Each plan is re-derived from compliance_id and checked."""
    before = ce_rows[ce_rows["event_ts"] < window_start]
    nums = before["compliance_id"].str.slice(3).astype(int)
    next_number = int(nums.max()) + 1 if len(nums) else 1
    decided = before["status"].isin(CE_DECIDED) & (before["status_ts"] < window_start)
    events = []
    for r in before[~decided].to_dict("records"):
        s = {k: r[k] for k in ("event_type", "source_ref", "measured_value", "threshold_value",
                               "threshold_unit", "severity")}
        s.update(event_ts=pd.Timestamp(r["event_ts"]), facility_sk=int(r["facility_sk"]),
                 equipment_sk=None if pd.isna(r["equipment_sk"]) else int(r["equipment_sk"]),
                 escalated=REG_OF_EVENT[r["event_type"]] == "TX_RRC_SWR32")
        e = make_event(int(r["compliance_id"][3:]), s)
        assert e["compliance_sk"] == int(r["compliance_sk"]), (
            f"{r['compliance_id']}: stored compliance_sk disagrees with its source")
        assert r["status"] != "Cancelled" or e["decision_ts"] is None, (
            f"{r['compliance_id']}: stored as Cancelled but its plan decides it -- "
            "CE_CANCEL_AFTER_DAYS or the plan changed. Run a backfill.")
        assert e["reported_ts"] == pd.Timestamp(r["reported_ts"]), (
            f"{r['compliance_id']}: stored reported_ts disagrees with its plan -- a plan must "
            "never change after creation. Run a backfill.")
        events.append(e)
    return events, next_number


def run_window(prior, next_number, sources, window_start, window_end, facility_id_of):
    """Both passes, day by day, over [window_start, window_end). Pass 1 has nothing to emit
    but a changed row: statuses are functions of the fixed plan, so advancing a case is
    recomputing its row as of the day's end. Pass 2 numbers the day's new events in order."""
    events = list(prior)
    day = window_start
    while day < window_end:
        nxt = min(day + _DAY, window_end)
        todays = sources[(sources["event_ts"] >= day) & (sources["event_ts"] < nxt)]
        for s in todays.to_dict("records"):
            events.append(make_event(next_number, s))
            next_number += 1
        day = nxt
    rows = [row_as_of(e, window_end, facility_id_of) for e in events
            if changes_in(e, window_start, window_end)]
    return rows, events


def to_frame(rows):
    ce = pd.DataFrame(rows, columns=CE_COLS)
    for c in ("compliance_sk", "facility_sk", "regulation_sk", "date_sk"):
        ce[c] = ce[c].astype("int64")
    ce["equipment_sk"] = pd.array([r["equipment_sk"] for r in rows], dtype="Int64")
    for c in ("event_ts", "status_ts", "reported_ts"):
        ce[c] = pd.to_datetime(ce[c])
    return ce.sort_values(["event_ts", "compliance_id"], kind="mergesort").reset_index(drop=True)


def merge_for_write(existing, changed, window_start):
    """03b's pattern: carry rows created before the window that this run did not change."""
    keep = existing[(existing["event_ts"] < window_start)
                    & ~existing["compliance_sk"].isin(changed["compliance_sk"])]
    out = pd.concat([keep[CE_COLS], changed[CE_COLS]], ignore_index=True)
    return out.sort_values(["event_ts", "compliance_id"], kind="mergesort").reset_index(drop=True)


def open_trajectory(ce, days):
    """Open cases (Reported / Under Review) at the end of each day, from the stored rows: open
    from event_ts until a Closed / Violation / Cancelled status_ts. Pass it the rows to count."""
    out = []
    for d in days:
        h = d + _DAY
        decided = ce["status"].isin(CE_DECIDED) & (ce["status_ts"] < h)
        out.append(int(((ce["event_ts"] < h) & ~decided).sum()))
    return np.array(out)


def cancelled_by(ce, days):
    """Cumulative Cancelled cases at the end of each day."""
    at = ce.loc[ce["status"] == "Cancelled", "status_ts"]
    return np.array([int((at < d + _DAY).sum()) for d in days])


print("compliance model defined -- a case's whole progression is a pure function of "
      "(compliance_id, event_type, severity, TOPOLOGY_SEED)")
print(f"fine scenario: {ACTIVE_FINE_SCENARIO}  (per-tonne charge: "
      f"{FINE_SCENARIOS[ACTIVE_FINE_SCENARIO]['per_tonne_usd']})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run mode and window
#
# As 03b: the window follows the sources. A backfill runs from `SOURCE_START`. An incremental
# run takes the last source day, and rerunning it replaces that day. Sources are built over
# the whole retention in both modes. Repetition needs the trailing 30 days of plumes, and
# persistence needs detector runs that began before the window.

# CELL ********************

RUN_MODE = "backfill"
try:
    RUN_MODE = getArgument("run_mode", "backfill")
except Exception:
    pass
RUN_MODE = (str(RUN_MODE) or "backfill").lower()
assert RUN_MODE in ("backfill", "incremental"), f"run_mode must be backfill or incremental"
try:
    _start_override = getArgument("start_date", "")
    _end_override = getArgument("end_date", "")
except Exception:
    _start_override, _end_override = "", ""

for _t in ("gold_plume_catalog", "sensor_telemetry", "fact_scada_alarm", "scada_telemetry"):
    assert table_exists(_t), f"{_t} does not exist -- 03c derives events from it"


def _span(name):
    r = read_input(name).agg(F.min("date_sk").alias("lo"), F.max("date_sk").alias("hi")).first()
    assert r["lo"] is not None, f"{name} is empty"
    return pd.Timestamp(str(int(r["lo"]))), pd.Timestamp(str(int(r["hi"]))) + _DAY


_tel, _ch4 = _span("scada_telemetry"), _span("sensor_telemetry")
SOURCE_START, SOURCE_END = max(_tel[0], _ch4[0]), min(_tel[1], _ch4[1])
assert SOURCE_START < SOURCE_END

if RUN_MODE == "backfill":
    WINDOW_START, WINDOW_END = SOURCE_START, SOURCE_END
else:
    WINDOW_END = SOURCE_END
    WINDOW_START = WINDOW_END - _DAY
if _start_override:
    WINDOW_START = pd.Timestamp(_start_override)
if _end_override:
    WINDOW_END = pd.Timestamp(_end_override)
WINDOW_START, WINDOW_END = WINDOW_START.normalize(), WINDOW_END.normalize()
assert WINDOW_START < WINDOW_END and SOURCE_START <= WINDOW_START and WINDOW_END <= SOURCE_END

if RUN_MODE == "incremental":
    assert table_exists(CE_TABLE), f"{CE_TABLE} does not exist -- run a backfill first"
    _hi = read_input(CE_TABLE).agg(F.max("event_ts").alias("m")).first()["m"]
    assert _hi is None or pd.Timestamp(_hi) < WINDOW_END, (
        f"{CE_TABLE} already holds events to {_hi}, past this window's end. Rerun through the "
        "latest day, or run a backfill.")

print(f"RUN_MODE={RUN_MODE}  window={WINDOW_START.date()}..{WINDOW_END.date()}")
print(f"sources span {SOURCE_START.date()}..{SOURCE_END.date()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load the observations

# CELL ********************

fac_pdf = read_input("dim_facility").filter("is_current = true").toPandas()
eq_pdf = read_input("dim_equipment").toPandas()
tag_pdf = read_input("dim_scada_tag").filter("is_current = true").toPandas()
sen_pdf = read_input("dim_sensor").filter("is_current = true").toPandas()
FAC_SK_OF = dict(zip(fac_pdf["facility_id"], fac_pdf["facility_sk"].astype(int)))
FAC_ID_OF = dict(zip(fac_pdf["facility_sk"].astype(int), fac_pdf["facility_id"]))
CH4_CADENCE_S = int(CH4_INTERVAL_HOURS * 3600)

_cat = read_input("gold_plume_catalog")
_need = {"plume_id", "detection_date", "emission_rate_kg_h", "source_lat", "source_lon",
         "attributed_facility_id", "attribution_probability", "attribution_distance_km"}
assert _need <= set(_cat.columns), f"gold_plume_catalog lacks {_need - set(_cat.columns)}"
_cols = sorted(_need | ({"emission_rate_p5_kg_h", "emission_rate_p95_kg_h"} & set(_cat.columns)))
plume_pdf = _cat.select(*_cols).toPandas()
plume_pdf["detect_ts"] = pd.to_datetime(plume_pdf["detection_date"])
if table_exists("gold_multi_gas_signatures"):
    _sig = read_input("gold_multi_gas_signatures").select("plume_id", "emission_signature").toPandas()
    plume_pdf = plume_pdf.merge(_sig.rename(columns={"emission_signature": "signature"}),
                                on="plume_id", how="left")
else:
    plume_pdf["signature"] = None
plume_pdf["signature"] = plume_pdf["signature"].fillna("Undetermined")
# Plumes before SOURCE_START are kept: repetition looks back 30 days, and a detection just
# before the span is still an earlier detection. Only events inside the span are written.
_in = (plume_pdf["detect_ts"] >= SOURCE_START) & (plume_pdf["detect_ts"] < SOURCE_END)
n_plumes_outside = int((~_in).sum())
plume_pdf = plume_pdf[plume_pdf["detect_ts"] < SOURCE_END].reset_index(drop=True)

flag_pdf = (read_input("sensor_telemetry").filter("exceedance_flag = true")
            .select("sensor_id", "reading_ts").toPandas())
flag_pdf["reading_ts"] = pd.to_datetime(flag_pdf["reading_ts"])

alarm_pdf = (read_input("fact_scada_alarm")
             .select("alarm_sk", "tag_sk", "equipment_sk", "facility_sk", "alarm_type",
                     "raised_ts", "cleared_ts").toPandas())
for _c in ("raised_ts", "cleared_ts"):
    alarm_pdf[_c] = pd.to_datetime(alarm_pdf[_c])
alarm_pdf = (alarm_pdf.merge(tag_pdf[["tag_sk", "tag_name"]], on="tag_sk", how="left")
             .merge(eq_pdf[["equipment_sk", "equipment_type"]], on="equipment_sk", how="left"))

runs_pdf = facility_runs(flag_pdf, sen_pdf, CH4_CADENCE_S)
sources, SKIPPED = build_sources(plume_pdf, runs_pdf, alarm_pdf, FAC_SK_OF, SOURCE_END)
sources = sources[sources["event_ts"] >= SOURCE_START].reset_index(drop=True)

print(f"plumes in the source span {int(_in.sum())}  ({n_plumes_outside} outside it: earlier "
      "ones count only as repeats, later ones are ignored)")
print(f"  attributed {int(plume_pdf['attributed_facility_id'].notna().sum())}, "
      f"UNATTRIBUTED {SKIPPED['unattributed']} -- no operator to cite, so no event; "
      f"below the {OLRE_THRESHOLD_KG_H:.0f} kg/h threshold {SKIPPED['below_threshold']}")
print(f"facility CH4 runs {len(runs_pdf):,} ({int((runs_pdf['run_len'] >= EXCEEDANCE_EVENT_HOURS).sum()) if len(runs_pdf) else 0}"
      f" reach {EXCEEDANCE_EVENT_HOURS} h)   flare alarms {int((alarm_pdf['equipment_type'] == 'Flare').sum()):,}")
print(f"candidate events: {len(sources):,}  (plume escalations over the whole catalogue: "
      f"repetition {SKIPPED['escalated_repeat']}, on-site persistence "
      f"{SKIPPED['escalated_persistence']}, both {SKIPPED['escalated_both']})")
for (_t, _e), _n in sources.groupby(["event_type", "escalated"]).size().items():
    print(f"  {_t:<11}{'state case' if _e else 'report':<12}{_n:>5}")
print("  NSPS OOOOb super-emitter program: 0 events -- not in force until 2027-01-22, and a "
      "km-scale attribution cannot meet its 50 m test")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### The threshold question, measured
#
# V1 treated every plume above 100 kg/h as a federal violation. The table below is why that
# cannot be right: TROPOMI's floor is ~3 t/h, so any threshold at or below it is crossed by
# every detection. A rate cut **inside** the detected range, say 25 t/h, would be a line
# drawn through the measurement noise. The IME rate's own p5–p95 spread is printed beside it,
# and CAMS's median on the same instrument is 2.4× ours. Rate decides that a release is
# reportable. It cannot decide that a release is a violation.

# CELL ********************

_span_pl = plume_pdf[plume_pdf["detect_ts"] >= SOURCE_START]
_r = _span_pl["emission_rate_kg_h"]
_att = _span_pl[_span_pl["attributed_facility_id"].notna()]["emission_rate_kg_h"]
print(f"emission_rate_kg_h over {len(_r)} plumes ({len(_att)} attributed)")
if len(_r):
    print("  " + "  ".join(f"p{int(q * 100)} {_r.quantile(q) / 1000:.1f}" for q in
                           (0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0)) + "  (t/h)")
    for _lo, _hi in ((0, 3e3), (3e3, 1e4), (1e4, 2.5e4), (2.5e4, 5e4), (5e4, 1e9)):
        _n = int(((_r >= _lo) & (_r < _hi)).sum())
        print(f"  {_lo / 1000:>5.0f}-{min(_hi, 1e5) / 1000:<5.0f} t/h {_n:>4}  {'#' * _n}")
    print(f"  {'candidate threshold':<44}{'share breaching':>16}{'attributed breaching':>22}")
    for _t, _why in ((100, "Subpart W OLRE / OOOOb SEP (rule)"), (1_000, "modelling"),
                     (3_000, "~TROPOMI floor"), (10_000, "modelling"), (25_000, "modelling"),
                     (50_000, "modelling")):
        print(f"  {_t / 1000:>6.1f} t/h  {_why:<34}{(_r >= _t).mean():>16.0%}"
              f"{int((_att >= _t).sum()):>16} of {len(_att)}")
    if {"emission_rate_p5_kg_h", "emission_rate_p95_kg_h"} <= set(_span_pl.columns):
        _spread = _span_pl["emission_rate_p95_kg_h"] / _span_pl["emission_rate_p5_kg_h"]
        print(f"  IME p95/p5 ratio: median {_spread.median():.1f}x -- a rate cut inside the "
              "detected range sits inside the measurement uncertainty")
print(f"chosen: every attributed plume >= {OLRE_THRESHOLD_KG_H:.0f} kg/h is REPORTED (rule); "
      "a state case needs repetition or on-site persistence (modelling)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Pass 1 and pass 2, and the write
#
# Pass 1 reads the cases undecided when the window began, selected on timestamps as in 03b.
# A case's row lives in its event day's partition, so a decision reached today rewrites a row
# up to ~95 days old. The `replaceWhere` range widens back to it, and `merge_for_write()`
# carries the untouched rows in that range.

# CELL ********************

if RUN_MODE == "incremental":
    stored_pdf = read_input(CE_TABLE).fillna({"equipment_sk": -1}).toPandas()
    stored_pdf["equipment_sk"] = pd.array([None if v < 0 else int(v)
                                           for v in stored_pdf["equipment_sk"]], dtype="Int64")
    for _c in ("event_ts", "status_ts", "reported_ts"):
        stored_pdf[_c] = pd.to_datetime(stored_pdf[_c])
    prior, next_number = prior_from_table(stored_pdf, WINDOW_START)
else:
    stored_pdf = pd.DataFrame(columns=CE_COLS)
    prior, next_number = [], 1

rows, all_events = run_window(prior, next_number, sources, WINDOW_START, WINDOW_END, FAC_ID_OF)
changed_pdf = to_frame(rows)
_c2 = to_frame(run_window(prior, next_number, sources, WINDOW_START, WINDOW_END, FAC_ID_OF)[0])
pd.testing.assert_frame_equal(changed_pdf, _c2, check_exact=True)

WS_SK = int(WINDOW_START.strftime("%Y%m%d"))
WE_SK = int((WINDOW_END - _DAY).strftime("%Y%m%d"))
if RUN_MODE == "incremental":
    RANGE_LO = min([WS_SK] + changed_pdf["date_sk"].tolist())
    _ex = stored_pdf[(stored_pdf["date_sk"] >= RANGE_LO) & (stored_pdf["date_sk"] <= WE_SK)]
    ce_write = merge_for_write(_ex, changed_pdf, WINDOW_START)
else:
    RANGE_LO, ce_write = WS_SK, changed_pdf

print(f"pass 1: {len(prior):,} undecided case(s) carried in at {WINDOW_START.date()}")
print(f"pass 2: {sum(1 for e in all_events if e['event_ts'] >= WINDOW_START):,} event(s) created")
print(f"write: {len(changed_pdf):,} changed + {len(ce_write) - len(changed_pdf):,} carried, "
      f"replaceWhere date_sk {RANGE_LO}..{WE_SK}")
print("OK  rerunning the window from the same prior state reproduces it exactly")


def _py(v):
    if v is None or v is pd.NA or v is pd.NaT or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, np.generic):
        return v.item()
    return v


def _spark_frame(pdf, schema):
    rows_ = [tuple(_py(v) for v in r) for r in pdf.itertuples(index=False, name=None)]
    return spark.createDataFrame(rows_, schema)


REG_COLS = ["regulation_sk", "regulation_id", "regulation_name", "jurisdiction",
            "threshold_value", "threshold_unit", "threshold_basis", "reporting_window_hours",
            "fine_min_usd", "fine_max_usd", "fine_basis", "effective_from", "is_active",
            "status_note", "is_synthetic"]
REG_SCHEMA = ("regulation_sk long, regulation_id string, regulation_name string, "
              "jurisdiction string, threshold_value double, threshold_unit string, "
              "threshold_basis string, reporting_window_hours double, fine_min_usd double, "
              "fine_max_usd double, fine_basis string, effective_from timestamp, "
              "is_active boolean, status_note string, is_synthetic boolean")
reg_pdf = pd.DataFrame([dict(r, regulation_sk=REG_SK[r["regulation_id"]],
                             effective_from=pd.Timestamp(r["effective_from"]),
                             is_synthetic=r["regulation_id"].startswith("GS_"))
                        for r in REGULATIONS])[REG_COLS]
(_spark_frame(reg_pdf, REG_SCHEMA).write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(REG_TABLE))
print(f"{REG_TABLE}: {len(reg_pdf)} rows (whole-table overwrite)")

CE_SCHEMA = ("compliance_sk long, compliance_id string, facility_sk long, facility_id string, "
             "equipment_sk long, regulation_sk long, event_ts timestamp, event_type string, "
             "source_ref string, measured_value double, threshold_value double, "
             "threshold_unit string, is_violation boolean, severity string, fine_usd double, "
             "status string, status_ts timestamp, reported_ts timestamp, date_sk long, "
             "is_synthetic boolean")
_sdf = _spark_frame(ce_write[CE_COLS], CE_SCHEMA)
if RUN_MODE == "backfill" or not table_exists(CE_TABLE):
    (_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
         .partitionBy("date_sk").saveAsTable(CE_TABLE))
else:
    (_sdf.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"date_sk >= {RANGE_LO} AND date_sk <= {WE_SK}")
         .partitionBy("date_sk").saveAsTable(CE_TABLE))
_n = spark.table(CE_TABLE).filter(f"date_sk >= {RANGE_LO} AND date_sk <= {WE_SK}").count()
assert _n == len(ce_write), f"{CE_TABLE} holds {_n} rows in range, wrote {len(ce_write)}"
print(f"{CE_TABLE}: {len(ce_write):,} rows written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation — every check fails the run, none warns

# CELL ********************

ce = read_input(CE_TABLE).fillna({"equipment_sk": -1}).toPandas()
ce["equipment_sk"] = pd.array([None if v < 0 else int(v) for v in ce["equipment_sk"]], dtype="Int64")
for _c in ("event_ts", "status_ts", "reported_ts"):
    ce[_c] = pd.to_datetime(ce[_c])
HORIZON = WINDOW_END
assert (ce["event_ts"] < HORIZON).all(), "an event after the run's horizon"

# --- inputs ---------------------------------------------------------------------------------------
assert not TABLES_READ & set(GROUND_TRUTH_TABLES), "a ground-truth table was read"
assert TABLES_READ <= set(INPUT_TABLES), f"undeclared input(s): {TABLES_READ - set(INPUT_TABLES)}"

# --- keys and FKs -----------------------------------------------------------------------------------
assert ce["compliance_sk"].is_unique and ce["compliance_id"].is_unique, "duplicate key"
assert ce["compliance_id"].str.fullmatch(r"CE-\d{6}").all(), "compliance_id not CE-nnnnnn"
assert set(ce["facility_sk"]) <= set(fac_pdf["facility_sk"]), "facility_sk unresolved"
assert set(int(v) for v in ce["equipment_sk"].dropna()) <= set(eq_pdf["equipment_sk"]), \
    "equipment_sk unresolved"
assert set(ce["regulation_sk"]) <= set(reg_pdf["regulation_sk"]), "regulation_sk unresolved"
assert (ce["facility_id"] == ce["facility_sk"].map(FAC_ID_OF)).all(), "facility_id != facility_sk"
_inactive = set(reg_pdf.loc[~reg_pdf["is_active"], "regulation_sk"])
assert not set(ce["regulation_sk"]) & _inactive, "an event under a regulation not in force"
assert set(ce["event_type"]) <= set(EVENT_TYPES) and set(ce["status"]) <= set(CE_STATUSES)

# --- derivation: every source_ref resolves to a real upstream row -------------------------------------
_in_ret = ce["event_ts"] >= SOURCE_START
_plumes = plume_pdf.set_index(plume_pdf["plume_id"].astype(str))
_miss = set(ce.loc[_in_ret & (ce["event_type"] == "OLRE"), "source_ref"]) - set(_plumes.index)
assert not _miss, f"{len(_miss)} OLRE event(s) whose plume is not in gold_plume_catalog"
_pl = ce[_in_ret & ce["source_ref"].isin(_plumes.index)]
_pp = _plumes.loc[_pl["source_ref"]]
assert _pp["attributed_facility_id"].notna().all(), "an event references an unattributed plume"
assert (_pp["attributed_facility_id"].map(FAC_SK_OF).values == _pl["facility_sk"].values).all(), \
    "a plume event is not at the plume's attributed facility"
assert np.allclose(_pp["emission_rate_kg_h"].values, _pl["measured_value"].values), \
    "a plume event's measured_value is not the plume's rate"
_rk = set(runs_pdf["run_key"]) if len(runs_pdf) else set()
_miss = set(ce.loc[_in_ret & (ce["event_type"] == "Exceedance"), "source_ref"]) - _rk
assert not _miss, f"{len(_miss)} Exceedance event(s) with no facility run in sensor_telemetry"
_al = set(alarm_pdf["alarm_sk"].astype(str))
_fl = ce[_in_ret & ce["event_type"].isin(["Venting", "Flaring"]) & ~ce["source_ref"].isin(_plumes.index)]
assert set(_fl["source_ref"]) <= _al, "a flare case whose alarm is not in fact_scada_alarm"
_d = sources.set_index(["event_type", "source_ref"])
_k = list(zip(ce.loc[_in_ret, "event_type"], ce.loc[_in_ret, "source_ref"]))
_miss = [k for k in _k if k not in _d.index]
assert not _miss, f"{len(_miss)} event(s) the rules do not derive, e.g. {_miss[:3]}"
assert (_d.loc[_k, "event_ts"].values == ce.loc[_in_ret, "event_ts"].values).all(), \
    "an event's event_ts is not its source's instant"

# --- violations and fines ------------------------------------------------------------------------------
_v = ce[ce["is_violation"]]
assert (ce["is_violation"] == (ce["status"] == "Violation")).all(), "is_violation != status Violation"
assert (ce.loc[~ce["is_violation"], "fine_usd"] == 0).all(), "a fine on a non-violation"
_rg = reg_pdf.set_index("regulation_sk")
assert ((_v["fine_usd"].values >= _rg.loc[_v["regulation_sk"], "fine_min_usd"].values)
        & (_v["fine_usd"].values <= _rg.loc[_v["regulation_sk"], "fine_max_usd"].values)).all(), \
    "a fine outside its regulation's range"
assert (_v["measured_value"] >= _v["threshold_value"]).all(), "a violation below its threshold"
assert _v["event_type"].isin(["Venting", "Flaring"]).all(), "a violation that is not a state case"
if len(_v):
    assert (_v["fine_usd"] > 0).all(), "a violation with no fine under the active scenario"

# --- status progression -----------------------------------------------------------------------------------
assert (ce["reported_ts"] > ce["event_ts"]).all(), "reported_ts not after event_ts"
_s = ce["status"]
assert (ce.loc[_s == "Reported", "status_ts"] == ce.loc[_s == "Reported", "event_ts"]).all()
assert (ce.loc[_s == "Reported", "reported_ts"] >= HORIZON).all(), "Reported past its report time"
assert (ce.loc[_s == "Under Review", "status_ts"] == ce.loc[_s == "Under Review", "reported_ts"]).all()
_dec = ce[_s.isin(["Closed", "Violation"])]
assert (_dec["status_ts"] > _dec["reported_ts"]).all(), "a decision before the report"
_exit = pd.Timedelta(days=CE_CANCEL_AFTER_DAYS)
_cx = ce[_s == "Cancelled"]
assert (_cx["status_ts"] == _cx["event_ts"] + _exit).all(), "a case not cancelled at its exit"
assert (_cx["status_ts"] < HORIZON).all() and not _cx["is_violation"].any() \
    and (_cx["fine_usd"] == 0).all(), "a Cancelled case with a violation or a fine"
_op = ce[_s.isin(CE_OPEN_STATUSES)]
assert (_op["event_ts"] + _exit >= HORIZON).all(), (
    f"{int((_op['event_ts'] + _exit < HORIZON).sum())} case(s) open past the "
    f"{CE_CANCEL_AFTER_DAYS}-day exit -- the stalled exit did not fire")
_plans = {r["compliance_id"]: event_plan(r["compliance_id"], r["event_type"],
                                         REG_OF_EVENT[r["event_type"]] == "TX_RRC_SWR32",
                                         r["severity"]) for r in ce.to_dict("records")}
ce["_stalled"] = ce["compliance_id"].map(lambda c: _plans[c]["is_stalled"])
assert not ce.loc[ce["_stalled"], "status"].isin(["Closed", "Violation"]).any(), "a stalled case decided"
assert ce.loc[_s == "Cancelled", "_stalled"].all(), "a non-stalled case cancelled"
_ur = ce[(_s == "Under Review") & ~ce["_stalled"]]
_age = (HORIZON - _ur["reported_ts"]).dt.total_seconds() / 86400.0
assert (_age <= MAX_REVIEW_DAYS).all(), f"{int((_age > MAX_REVIEW_DAYS).sum())} case(s) stuck"

print("OK  read no ground-truth table")
print("OK  keys unique; facility, equipment and regulation resolve; nothing under an inactive rule")
print(f"OK  every event derives from a real plume, facility CH4 run or flare alarm "
      f"({int(_in_ret.sum()):,} checked); no event references an unattributed plume "
      f"({SKIPPED['unattributed']} skipped for that reason)")
print("OK  fines zero unless a violation, inside the regulation's range when one; every "
      "violation above its threshold and a state case")
print(f"OK  status progression valid; no non-stalled case Under Review past {MAX_REVIEW_DAYS} days")
print(f"OK  no case open past the {CE_CANCEL_AFTER_DAYS}-day exit; every Cancelled case stalled, "
      "cancelled exactly at it, with no violation or fine")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Steady state and distributions
#
# Three populations, reported apart and never summed:
#
# - **Open**: Reported or Under Review, not stalled. This is the KPI, and it converges to
#   `λ × T`.
# - **Stalled open**: stalled and not yet at the exit. It converges to
#   `stall arrivals/day × 120` once 120 days of history exist.
# - **Cancelled**: stalled cases that reached the 120-day exit. It is cumulative, and never open.
#
# Open cases, Reported or Under Review and not stalled, converge to `λ × T`, with T the
# mean planned time to decision. State cases take up to ~37 days to decide, so the population
# needs about 60 days to level. On a 30-day retention the check is printed and **not
# asserted**.
# `harness_compliance.py` asserts it over 300 days.

# CELL ********************

DAYS = pd.date_range(SOURCE_START, HORIZON - _DAY, freq="D")
_ns = ce[~ce["_stalled"]]
traj = open_trajectory(_ns, DAYS)
stalled_open = open_trajectory(ce[ce["_stalled"]], DAYS)
cancelled_cum = cancelled_by(ce, DAYS)
_tw = DAYS[-TREND_WINDOW_DAYS:]
LAMBDA = int(((_ns["event_ts"] >= _tw[0]) & (_ns["event_ts"] < HORIZON)).sum()) / len(_tw)
T_DAYS = float(np.mean([(_plans[c]["report_s"] + _plans[c]["review_s"]) / 86400.0
                        for c in _ns["compliance_id"]])) if len(_ns) else 0.0
REALISED = float(traj[-TREND_WINDOW_DAYS:].mean()) if len(traj) else 0.0
print(f"open cases (non-stalled) by day: " + " ".join(str(v) for v in traj))
print(f"arrivals {LAMBDA:.2f}/day, mean time to decision {T_DAYS:.1f} d, predicted open "
      f"{LAMBDA * T_DAYS:.1f}, realised {REALISED:.1f}")
_st_rate = int((ce["_stalled"] & (ce["event_ts"] >= _tw[0])).sum()) / len(_tw)
_cx = ce[ce["status"] == "Cancelled"]
print("CASE POPULATIONS AT THE HORIZON -- reported apart, never summed")
print(f"  open (KPI)        {int(traj[-1]):>5}   Reported / Under Review, not stalled")
print(f"  stalled open      {int(stalled_open[-1]):>5}   heading for ~{_st_rate * CE_CANCEL_AFTER_DAYS:.0f} "
      f"({_st_rate:.2f}/day x {CE_CANCEL_AFTER_DAYS} days)")
print(f"  cancelled         {len(_cx):>5}   at the {CE_CANCEL_AFTER_DAYS}-day exit, never open"
      + ("" if len(DAYS) > CE_CANCEL_AFTER_DAYS
         else f" -- none can exist until {CE_CANCEL_AFTER_DAYS} days of history")
      + ("   by type: " + ", ".join(f"{k} {v}" for k, v in _cx["event_type"].value_counts().items())
         if len(_cx) else ""))
if len(DAYS) >= WARMUP_DAYS + TREND_WINDOW_DAYS:
    assert STEADY_BAND[0] * LAMBDA * T_DAYS <= REALISED <= STEADY_BAND[1] * LAMBDA * T_DAYS
    _rise = np.polyfit(np.arange(len(_tw), dtype=float), traj[-len(_tw):].astype(float), 1)[0] * (len(_tw) - 1)
    assert _rise <= TREND_MAX_RISE * REALISED, "open cases trending upward"
    print("OK  open cases inside the steady-state band and not trending upward")
else:
    print(f"NOTE  {len(DAYS)} days of history; the steady-state check needs "
          f"{WARMUP_DAYS + TREND_WINDOW_DAYS}. NOT asserted this run.")

print()
print("events by type and regulation")
_rid = dict(zip(reg_pdf["regulation_sk"], reg_pdf["regulation_id"]))
print(ce.assign(regulation=ce["regulation_sk"].map(_rid))
        .pivot_table(index="event_type", columns="regulation", values="compliance_id",
                     aggfunc="count", fill_value=0).to_string())
print()
print(f"violations {len(_v):,} of {len(ce):,} events ({len(_v) / max(len(ce), 1):.1%}); "
      f"state cases {int(ce['event_type'].isin(['Venting', 'Flaring']).sum()):,}")
print(f"fines total ${ce['fine_usd'].sum():,.0f} under '{ACTIVE_FINE_SCENARIO}'  "
      + "  ".join(f"{k} ${v:,.0f}" for k, v in _v.groupby("severity")["fine_usd"].sum().items()))
print("status: " + "   ".join(f"{k} {v:,}" for k, v in ce["status"].value_counts().items())
      + f"   (stalled {int(ce['_stalled'].sum())})")
_epf = ce.groupby("facility_sk").size().reindex(fac_pdf["facility_sk"], fill_value=0)
print(f"events per facility: median {_epf.median():.0f}  p90 {_epf.quantile(0.9):.0f}  max "
      f"{_epf.max()}  ({int((_epf == 0).sum())} facilities with none)")
print(f"plumes skipped as unattributed: {SKIPPED['unattributed']} -- releases the estate "
      "could not be tied to; no operator was cited")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary

# CELL ********************

print("=" * 76)
print("COMPLIANCE EVENTS GENERATED")
print("=" * 76)
print(f"  run mode          {RUN_MODE}   window {WINDOW_START.date()} .. {WINDOW_END.date()}")
print(f"  events            {len(ce):,}; violations {len(_v):,}; fines ${ce['fine_usd'].sum():,.0f}")
print(f"  cases             {int(traj[-1])} open, {int(stalled_open[-1])} stalled open, "
      f"{len(_cx)} cancelled at the {CE_CANCEL_AFTER_DAYS}-day exit")
print(f"  fine scenario     {ACTIVE_FINE_SCENARIO} (no federal per-tonne charge)")
print(f"  tables written    {REG_TABLE}, {CE_TABLE}")
print(f"  read              {', '.join(sorted(TABLES_READ))}")
print(f"  not read          {', '.join(GROUND_TRUTH_TABLES)} -- a regulator sees observations only")
print("  not modified      gold_plume_catalog, every other dim_* table, every telemetry table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
