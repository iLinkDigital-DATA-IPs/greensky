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

# # 03e — Generate Production, Financial Impact and Facility Daily Snapshots
#
# Writes three tables. **`fact_production_daily`** is facility × day volumes. It exists mainly
# as the denominator of the dashboard's Methane Intensity %, which divides emissions by
# `gross_gas_mcf`. **`fact_financial_impact`** holds what emissions and violations cost.
# **`fact_facility_daily_snapshot`** is one row per facility per day, and the Executive
# Overview binds to it. The first two feed the third. Every other table is read and never
# modified.
#
# ### V1's convention, and what is kept
# There is no archived DAX in this repo. The V1 semantic model's measures were never
# exported. The convention exists in three places: `gen_financial`'s header, which says
# "attributed_kg = emission_rate_kg_s * 3600 * allocation_factor"; `build_snapshots`, which
# computes `total_emissions_kg = SUM(rate x allocation) x 3600`; and the archived KQL
# dashboard, which reads `total_emissions_kg`. **The shape is kept:
# `attributed_kg = emission_rate_kg_h x duration_h x allocation_factor`.** Two inputs change.
#
# - **Duration.** V1's `x 3600` was an implicit one-hour duration. It was not measured. Here
#   `duration_h` is the plume's own mixing time, `t_mix_s / 3600`, from 04. 04 computes
#   `rate = IME / t_mix`, so rate × duration is exactly the IME: **the methane the satellite
#   actually saw in the plume at overpass.** That is a floor, not an estimate of the whole
#   release. TROPOMI sees one instant a day and cannot say how long a release ran. The IME
#   does not depend on the plume-length or wind assumptions behind the rate, so the floor is
#   the most directly observed mass available.
# - **Allocation.** V1 split a plume across facilities through
#   `bridge_plume_facility_attribution`. 05 attributes each plume to **one** facility, so
#   `allocation_factor = 1.0`. The whole plume is charged to that facility, which is how 03c
#   reports it. `attribution_probability` is not used as a split: the unassigned remainder
#   would belong to no facility, and the plume would stop reconciling. Attribution
#   uncertainty is therefore **not** in the band below. The run prints its distribution.
#
# All three factors are columns on every emission row, so any measure can recompute
# `attributed_emissions_kg` from them.
#
# ### Uncertainty: 04's Monte Carlo, not a confidence string
# V1 invented a band. It turned `emission_rate_confidence` ('high' / 'medium' / 'low') into a
# lognormal sigma and wrote P10/P90 as `total x exp(±1.2816 sigma)`. **That is gone.** 04
# propagates retrieval noise and wind uncertainty through 500 samples, seeded per plume, and
# writes `emission_rate_p5/p50/p95_kg_h`. Lost-gas value and social cost are both
# increasing linear functions of the rate. A fine does not depend on the rate. A percentile
# of the rate is therefore exactly the same percentile of the impact. No sampling is
# repeated and nothing is assumed.
#
# **The columns are `total_impact_usd_p5 / _p50 / _p95`, because that is what they hold.** 04
# writes the 5th and 95th percentiles, and a P10/P90 label on them would claim an 80%
# interval that is really a 90% one. The V1 dashboard labels its tiles P10/P50/P90. Change
# the labels to P5/P50/P95. Do not rerun 04 at 10/90: the detection layer's own
# `uncertainty_ratio` and `confidence` are built on p95, so one interval should run from
# detection to dollars. Note also that p50 is the **median of the samples**, not 04's point
# estimate `emission_rate_kg_h`. The component columns (`lost_gas_value_usd` and the rest)
# use the point estimate, as `attributed_emissions_kg` does, so `total_impact_usd_p50` is not
# their sum. The run prints the gap.
#
# ### Money: components, each with its source
# - **Lost gas value**: the methane itself, as mcf, at the gas price. This is a real
#   operational cost.
# - **Fines**: `fact_compliance_event.fine_usd`, on its own row, **dated at the notice of
#   violation (`status_ts`), not at the plume**. A fine is decided weeks after the detection
#   it cites. Put on the plume's row, it would rewrite a past day's impact whenever a case
#   decided. Flare-alarm cases cite no plume at all. Dating each fine at its notice makes
#   every row final when written, and every violation in the window appears exactly once.
#   Few violations, and so a small, sporadic total, is correct. Nothing is scaled up.
# - **CO₂e tonnes**: for reporting, not a cost.
# - **Social cost**: a shadow price, **off by default**. Switched on, it is added to the
#   totals, and `fine_scenario` says so on every row.
#
# The constants cell labels every number as a **rule**, a **physical constant**, a **market
# observation** (a dated price that moves), a **modelling choice**, or a **shadow price**,
# as 03c does for regulations.
#
# ### Snapshots are point-in-time, so a past day never changes
# A snapshot row is the facility **as it stood at the end of that day**. It is computed from
# event timestamps only, never from a current `status` column: created and closed times,
# notice times, raised and cleared times, reading times, maintenance times, survey times.
# So a ticket that closes tomorrow does not alter yesterday. Yesterday it was open, and
# yesterday's MTTR counts only closures up to yesterday. **Recomputing any past day
# reproduces it exactly, with no trailing recompute window.** The run asserts this on a
# sample of days. Two things can still change a past row: an upstream notebook rewriting its
# own history (04/05 re-deriving `gold_plume_catalog`, a whole-table overwrite, is the known
# case), or a generator re-backfilled with new parameters. A 03e backfill is the remedy for
# either. Where V1's semantics were not point-in-time, they change, and the column list
# below says how.
#
# ### Writes
# All three tables are partitioned by `date_sk` and written with `replaceWhere` over the
# run's window, **in both run modes**. A backfill replaces its window, not the whole table.
# The sources keep a finite retention, and snapshot days that have aged out of it can no
# longer be recomputed. A whole-table overwrite would delete them.
#
# ### Pipeline order
# 03c → 03b → 03d → **03e**. 03e reads each one's output, and 03d must have run through the
# sources' last day.

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

PROD_TABLE = "fact_production_daily"
FIN_TABLE = "fact_financial_impact"
SNAP_TABLE = "fact_facility_daily_snapshot"

INPUT_TABLES = ("dim_facility", "dim_equipment", "dim_sensor", "dim_regulation",
                "fact_asset_state", "gold_plume_catalog", "fact_compliance_event",
                "fact_work_order", "fact_work_order_event", "fact_maintenance",
                "fact_pm_schedule", "fact_ldar_survey", "fact_scada_alarm", "sensor_telemetry",
                PROD_TABLE, FIN_TABLE, SNAP_TABLE)

# The hidden ground truth. A cost is charged on what was observed, never on what was there.
GROUND_TRUTH_TABLES = ("fact_emission_episode",)
assert not set(INPUT_TABLES) & set(GROUND_TRUTH_TABLES), "an input is a ground-truth table"

TABLES_READ = set()


def read_input(name):
    """The only way this notebook reads a table."""
    assert name not in GROUND_TRUTH_TABLES, (
        f"{name} is hidden ground truth. Financial impact is charged on observed plumes and "
        "issued notices; reading it would price releases nobody detected."
    )
    assert name in INPUT_TABLES, f"{name} is not a declared input of 03e: {INPUT_TABLES}"
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

# ### The model
#
# This cell is pure: no Spark, no table, no clock. `tools/harness/harness_financial.py`
# executes it verbatim against `01_topology_config`. It also asserts that every definition
# copied from 03b, 03c and 03d is identical to its source's text.
#
# **The risk score.** It ranks facilities, and a customer will ask how. It is 0–100: the sum
# of five components, each a weight times a score in [0, 1] that saturates at a stated level.
# Each component's points are stored on the row, so every score can be taken apart.
#
# | component | weight | score in [0, 1] | why |
# |---|---|---|---|
# | condition | 30 | mean `equipment_condition_index` of the facility's assets | the shared degradation model: the asset 03a's hazard treats as likely to leak |
# | emissions | 25 | attributed plumes in the trailing 30 days, saturating at 3 | 3 = one detection plus 03c's `CRITICAL_REPEATS` of 2 earlier ones: a repeat emitter |
# | compliance | 20 | violation notices in the trailing 30 days, plus 0.5 per open state case, saturating at 1 | a notice is the regulator's finding; an open state case is one pending |
# | maintenance | 15 | half the overdue-PM share (saturating at 25%, about twice the estate's ~11%), half LDAR leaks outstanding (saturating at 5) | a slipping backlog |
# | open work | 10 | active open plus stalled-backlog tickets, saturating at 5 | work already raised against the site |
#
# V1 weighted health 40, 8 per open ticket, 12 per violation and 5 per active plume on
# **that day**, capped at 100. A score driven by one day's plume count jumps with each
# overpass. The trailing windows smooth that, and they are point-in-time like everything
# else. OLRE reports are not in the compliance component, because every attributed plume
# files one and the emissions component already counts it. **The score is per facility.**
# The "High-Risk Equipment List" ranks equipment within the facilities this score ranks.
# Every weight and saturation level is a modelling choice.

# CELL ********************

# ---- 03e financial model (pure: tools/harness/harness_financial.py executes this cell) ----
_DAY = pd.Timedelta(days=1)
_H = pd.Timedelta(hours=1)

# ---- money and physics ---------------------------------------------------------------------------
# Every constant carries its kind and its source in CONSTANT_BASIS, printed by the run.
# Checked against the named sources on 2026-09-30; re-check before showing a figure as current.
GAS_PRICE_USD_PER_MCF = 2.75
KG_CH4_PER_MCF = 19.26
GWP_CH4_100 = 28.0
SOCIAL_COST_USD_PER_T_CH4 = 1600.0
SOCIAL_COST_ON = False              # the switch: a shadow price, never an incurred cost
FINE_SCENARIO = "reporting_only"    # 03c's ACTIVE_FINE_SCENARIO; the harness asserts they agree
ALLOCATION_FACTOR = 1.0
CONSTANT_BASIS = {
    "GAS_PRICE_USD_PER_MCF": (
        "market observation",
        "V1's config value, undated and unsourced ('Henry Hub-ish'), kept so the figures stay "
        "comparable with V1. Checked 2026-09-30: EIA reports the Henry Hub spot averaged "
        "$2.93/MMBtu over June-August 2026 (Today in Energy, 2026-09-25), about $3.04/mcf at "
        "~1.037 MMBtu/mcf, so this reads ~10% low for the window. Henry Hub prices pipeline "
        "gas; an mcf of methane is valued as an mcf of gas (a modelling choice)."),
    "KG_CH4_PER_MCF": (
        "physical constant",
        "V1's value, and the divisor the dashboard's Methane Intensity tile hard-codes. "
        "GHGRP Subpart W uses 0.0192 kg/scf (19.2 kg/mcf) at 60 F and 14.7 psia "
        "(40 CFR 98.233); the 0.3% difference is kept so this table and that tile agree."),
    "GWP_CH4_100": (
        "rule",
        "IPCC AR5 100-year GWP for methane, as adopted in GHGRP Table A-1 (40 CFR Part 98 "
        "Subpart A) from reporting year 2024 (final rule 89 FR 31802, 2024-04-25). AR6 gives "
        "29.8 for fossil methane; the reporting rule's value is used."),
    "SOCIAL_COST_USD_PER_T_CH4": (
        "shadow price",
        "EPA's December 2023 Report on the Social Cost of Greenhouse Gases: $1,600 per tonne "
        "CH4 for 2020 emissions at a 2.0% near-term discount rate, in 2020 dollars. The "
        "federal estimates were withdrawn by Executive Order 14154 (2025-01-20). An ESG "
        "shadow price, not a cost anyone is charged. V1 multiplied CO2e by a CO2 price "
        "instead; the report's methane-specific figure is used directly."),
    "FINE_SCENARIO": (
        "rule",
        "03c's default: no federal per-tonne charge applies to this window (the Waste "
        "Emissions Charge rule was disapproved in 2025 and the charge delayed to 2034); fines "
        "are 03c's discrete notices, whose amounts 03c labels a modelling choice."),
    "ALLOCATION_FACTOR": (
        "modelling choice",
        "05 attributes each plume to one facility; the whole plume is charged to it, as 03c "
        "reports it."),
    "duration_h": (
        "modelling choice",
        "the plume's own mixing time t_mix_s / 3600 from 04, so rate x duration = IME: the "
        "mass observed at overpass, a floor on the release. V1 used an implicit 1 hour."),
}

# ---- production (modelling choices; fact_production_daily is synthetic) ---------------------------
# Base gross gas per facility type, V1's values: 6,000 mcf/d for a processing plant, 3,500 for a
# compression station, 2,200 otherwise. Unsourced, and small for real Permian plants; they set
# the scale of the intensity denominator only. Oil and water ratios are V1's too.
PROD_BASE_GAS_MCF_D = {"Gas Processing Plant": 6000.0, "Compression Station": 3500.0,
                       "Gathering System": 2200.0, "Tank Battery": 2200.0,
                       "Central Delivery Point": 2200.0}
PROD_DAILY_SPREAD = 0.10            # +-10% day to day; V1 drew x0.6-1.4, a +-40% daily swing
OIL_BBL_PER_MCF = (0.05, 0.15)      # V1
WATER_BBL_PER_MCF = (0.20, 0.50)    # V1
# Production follows fact_asset_state: the share of the facility's asset-time that was Running.
# Standby is available but idle for commercial reasons, so it produces nothing. Every asset
# counts equally. downtime_flag marks a day the facility was largely Down or in Maintenance.
PRODUCING_STATES = ("Running",)
DOWNTIME_STATES = ("Down", "Maintenance")
DOWNTIME_FLAG_SHARE = 0.5

# ---- snapshot (modelling choices) ------------------------------------------------------------------
MTTR_WINDOW_DAYS = 30               # MTTR over tickets closed in the trailing 30 days
RISK_TRAILING_DAYS = 30
OFFLINE_KPI_HOURS = 8               # 02e's KPI_HOURS: no reading in 8 h = offline on the dashboard
STATE_CASE_TYPES = ("Venting", "Flaring")    # 03c's state cases (TX_RRC_SWR32)
RISK_WEIGHTS = {"condition": 30.0, "emissions": 25.0, "compliance": 20.0, "maintenance": 15.0,
                "open_work": 10.0}
RISK_PLUMES_AT_MAX = 3.0
RISK_OPEN_CASE_WEIGHT = 0.5
RISK_OVERDUE_SHARE_AT_MAX = 0.25
RISK_LDAR_AT_MAX = 5.0
RISK_TICKETS_AT_MAX = 5.0
RISK_RANGE = (0.0, 100.0)

assert abs(sum(RISK_WEIGHTS.values()) - RISK_RANGE[1]) < 1e-9, "risk weights must sum to 100"
assert set(PROD_BASE_GAS_MCF_D) == set(FACILITY_TYPES), "a facility type has no base volume"
assert FINE_SCENARIO == "reporting_only", "only reporting_only fines exist upstream"
assert 0.0 < ALLOCATION_FACTOR <= 1.0

# ---- copied verbatim from 03b: the dashboard's work-order measures -------------------------------
# harness_financial.py asserts these are character-for-character 03b's.
WO_OPEN_STATUSES = ("Open", "In Progress")
WO_CANCEL_AFTER_DAYS = 60


def open_measures(wo):
    """(active_open, stalled_backlog) -- the dashboard's observable masks over fact_work_order."""
    is_open = wo["status"].isin(WO_OPEN_STATUSES)
    past_sla = wo["is_breached"].astype(bool)
    return is_open & ~past_sla, is_open & past_sla


def observable_trajectory(wo, days):
    """(active open, stalled backlog) at the end of each day, as open_measures() would count
    them on that day: open then, and within or past created_ts + sla_hours."""
    cancel_at = wo["created_ts"] + pd.Timedelta(days=WO_CANCEL_AFTER_DAYS)
    is_c = wo["status"] == "Cancelled"
    due = wo["created_ts"] + pd.to_timedelta(wo["sla_hours"], unit="h")
    act, bkl = [], []
    for d in days:
        h = d + _DAY
        is_open = ((wo["created_ts"] < h)
                   & (wo["closed_ts"].isna() | (wo["closed_ts"] >= h))
                   & ~(is_c & (cancel_at < h)))
        past = due < h
        act.append(int((is_open & ~past).sum()))
        bkl.append(int((is_open & past).sum()))
    return np.array(act), np.array(bkl)


# ---- copied verbatim from 03c: open compliance cases ------------------------------------------------
CE_DECIDED = ("Closed", "Violation", "Cancelled")


def open_trajectory(ce, days):
    """Open cases (Reported / Under Review) at the end of each day, from the stored rows: open
    from event_ts until a Closed / Violation / Cancelled status_ts. Pass it the rows to count."""
    out = []
    for d in days:
        h = d + _DAY
        decided = ce["status"].isin(CE_DECIDED) & (ce["status_ts"] < h)
        out.append(int(((ce["event_ts"] < h) & ~decided).sum()))
    return np.array(out)


# ---- copied verbatim from 03d: the PM state, asset condition and LDAR repair constants -------------
# fact_pm_schedule is a snapshot at 03d's horizon, and fact_ldar_survey carries repairs as of
# it. 03d rebuilds any past day's overdue count from fact_maintenance with overdue_flags(), and
# these copies do the same here. harness_financial.py asserts they are 03d's text, and the run
# asserts that at the horizon they reproduce fact_pm_schedule and fact_ldar_survey exactly.
PM_ON_TIME_COMPLETION = 0.85
PM_CATCHUP_SHARE = 0.70
PM_SEED_CYCLES = 12
SEED_CALIBRATION_DAYS = 60
LDAR_CADENCE_DAYS = 90
LDAR_EPOCH = pd.Timestamp("2026-01-01")
LDAR_REPAIR_MEDIAN_DAYS, LDAR_REPAIR_SIGMA = 7.0, 0.8
LDAR_REPAIR_MAX_DAYS = 45.0
LDAR_DELAY_SHARE = 0.05
LDAR_DELAY_DAYS = (45.0, 120.0)
LDAR_REPAIR_COST = (400.0, 1800.0)
FEDERAL_FROM = pd.Timestamp("2015-09-18")


def pm_phase_days(equipment_id, insp_days):
    """Per-asset offset into the PM cycle, so PMs are not all due on the same day."""
    return float(get_rng("pm_phase", equipment_id).uniform(0.0, insp_days))


def asset_views(eq, fac):
    """One dict per asset with everything the model needs, including both calendars."""
    f = fac.set_index("facility_sk")
    out = {}
    for r in eq.to_dict("records"):
        freq = int(r["inspection_frequency_days"])
        inst = pd.Timestamp(r["install_date"])
        fr = f.loc[r["facility_sk"]]
        out[int(r["equipment_sk"])] = dict(
            equipment_sk=int(r["equipment_sk"]), equipment_id=r["equipment_id"],
            equipment_type=r["equipment_type"], facility_sk=int(r["facility_sk"]),
            area_sk=int(r["area_sk"]), install=inst, freq=freq,
            life=float(r["expected_life_years"]), criticality=r["criticality"],
            reliability=float(r["reliability_index"]), leak_propensity=float(r["leak_propensity"]),
            sub_basin=fr["sub_basin"],
            program=ldar_program(fr),
            # Both calendars are floored to the second. A fractional-day phase gives a
            # nanosecond anchor, and Delta stores microseconds, so an unfloored next_due_ts
            # or inspection_ts would not survive a write and read back equal. That would
            # break the next_due check and every follow-up re-derived from a stored row.
            # pm_k() rounds to the nearest calendar index, so flooring cannot move a PM
            # visit off its 02a stop.
            pm_anchor=(inst + pd.Timedelta(days=pm_phase_days(r["equipment_id"], freq))).floor("s"),
            insp_anchor=(inst + pd.Timedelta(days=float(
                get_rng("insp_phase", r["equipment_id"]).uniform(0.0, freq)))).floor("s"))
    return out


def ldar_program(fac_row):
    if pd.Timestamp(fac_row["commission_date"]) >= FEDERAL_FROM:
        return "Federal"
    return "State" if fac_row["facility_type"] in ("Tank Battery", "Gathering System") else "Voluntary"


def state_index(state):
    """Per asset: sorted interval arrays (start_ns, end_ns with +inf for open, state, cause)."""
    idx = {}
    s = state.sort_values(["equipment_sk", "start_ts"], kind="mergesort")
    for k, g in s.groupby("equipment_sk", sort=False):
        e = g["end_ts"].values.astype("datetime64[ns]")
        idx[int(k)] = (g["start_ts"].values.astype("datetime64[ns]").astype("int64"),
                       np.where(np.isnat(e), np.iinfo("int64").max, e.astype("int64")),
                       g["state"].values, g["cause"].values)
    return idx


def days_since_service(si, a, t, history_start):
    """03a's definition: from the end of the last Maintenance interval before t; before the
    first one, the asset is assumed mid-cycle at the history start."""
    x = si.get(a["equipment_sk"])
    if x is not None:
        m = (x[2] == "Maintenance") & (x[1] <= t.value)
        if m.any():
            return (t.value - int(x[1][m].max())) / 86400e9
    return (t - history_start) / _DAY + a["freq"] / 2.0


def condition(si, a, t, history_start):
    age = max((t - a["install"]).days / 365.25, 0.0)
    return equipment_condition_index(age, a["life"], days_since_service(si, a, t, history_start),
                                     a["freq"], a["reliability"], a["leak_propensity"])


def pm_completes(a, k):
    return bool(get_rng("pm_complete", a["equipment_id"], k).random() < PM_ON_TIME_COMPLETION)


def seed_calibration(assets, state, history_start):
    """Per equipment type, measured on [history_start, + SEED_CALIBRATION_DAYS):
    skip_share -- calendar PMs with no Scheduled PM stop starting within 2 h of the due
    instant (02a skips a PM when the asset is down then); stop_rate -- non-PM Maintenance
    stops per asset-day. Both feed pm_seed, so the pre-history pool matches what the history
    itself then does. The first version of the seed modelled incomplete visits only, and the
    overdue pool climbed ~13% over four months as skips accumulated that the seed never had."""
    hi = history_start + pd.Timedelta(days=SEED_CALIBRATION_DAYS)
    m = state[(state["state"] == "Maintenance") & (state["start_ts"] >= history_start)
              & (state["start_ts"] < hi)]
    pm_starts = {}
    for k, g in m[m["cause"] == "Scheduled PM"].groupby("equipment_sk"):
        pm_starts[int(k)] = np.sort(g["start_ts"].values.astype("datetime64[ns]").astype("int64"))
    other = m[m["cause"] != "Scheduled PM"].groupby("equipment_sk").size()
    due, skipped, stops, days = {}, {}, {}, {}
    for k, a in assets.items():
        t = a["equipment_type"]
        lo = max(history_start, a["install"])
        if lo >= hi:
            continue
        days[t] = days.get(t, 0.0) + (hi - lo) / _DAY
        stops[t] = stops.get(t, 0) + int(other.get(k, 0))
        span = pd.Timedelta(days=a["freq"])
        k_lo = int(np.ceil((lo - a["pm_anchor"]) / span))
        for j in range(max(k_lo, 0), int(np.floor((hi - a["pm_anchor"]) / span)) + 1):
            g = a["pm_anchor"] + span * j
            if not (lo <= g < hi - pd.Timedelta(hours=2)):
                continue
            due[t] = due.get(t, 0) + 1
            st = pm_starts.get(k)
            hit = st is not None and bool(((st >= g.value) & (st <= (g + pd.Timedelta(hours=2)).value)).any())
            skipped[t] = skipped.get(t, 0) + int(not hit)
    return {t: {"skip_share": skipped.get(t, 0) / due[t] if due.get(t) else 0.0,
                "stop_rate": stops.get(t, 0) / days[t] if days.get(t) else 0.0}
            for t in set(due) | set(days)}


def pm_seed(a, history_start, calib):
    """last_completed at the history start, drawn from the same process the history then
    runs, so the overdue pool starts at its steady level rather than drifting to it.

    Walking back over calendar PMs k0, k0-1, ...: visit j happened with probability
    1 - skip_share and completed with PM_ON_TIME_COMPLETION (the completion drawn with the
    history's own pm_completes key). If it did not, a catch-up in the gap after it (to the
    next visit, or to the history start) happened with probability
    1 - exp(-PM_CATCHUP_SHARE x stop_rate x gap_days), at a drawn point in that gap. The
    first of these found is the last completion. None for an asset whose calendar starts
    inside the history."""
    if a["pm_anchor"] >= history_start:
        return None
    c = calib.get(a["equipment_type"], {"skip_share": 0.0, "stop_rate": 0.0})
    span = pd.Timedelta(days=a["freq"])
    k0 = int(np.floor((history_start - a["pm_anchor"]) / span))
    for k in range(k0, k0 - PM_SEED_CYCLES, -1):
        if k < 0:
            # Every calendar PM since install failed or was skipped: the asset has never had a
            # PM, so it is due at its first calendar point (None). The first version fell
            # through to the fallback below and treated that first visit as COMPLETED, which
            # started young-but-established assets not overdue when they were. The overdue
            # pool then climbed for weeks as the history "discovered" them.
            return None
        g = a["pm_anchor"] + span * k
        r = get_rng("pm_seed", a["equipment_id"], k)
        if r.random() >= c["skip_share"] and pm_completes(a, k):
            return g
        gap = min(g + span, history_start) - g
        if r.random() < 1.0 - np.exp(-PM_CATCHUP_SHARE * c["stop_rate"] * (gap / _DAY)):
            return (g + gap * float(r.random())).floor("s")
    return a["pm_anchor"] + span * max(k0 - PM_SEED_CYCLES + 1, 0)


def next_due(a, last_completed):
    if last_completed is None:
        return a["pm_anchor"]
    return last_completed + pd.Timedelta(days=a["freq"])


def overdue_flags(assets, maint_rows, days, history_start, calib):
    """(assets x days) booleans: whether each asset's PM was overdue at the end of each day,
    from completed preventive records plus the seed -- the same answer in either run mode,
    because it reads the records, not a snapshot."""
    comp = {}
    if len(maint_rows):
        d = maint_rows[(maint_rows["maintenance_type"] == "Preventive")
                       & maint_rows["is_completed"].astype(bool)]
        for k, g in d.groupby("equipment_sk"):
            # datetime64[ns] explicitly. Spark's toPandas gives datetime64[us] in Fabric, and
            # .astype("int64") on that yields MICROSECONDS, compared below against a
            # nanosecond day boundary. Every completion then read as "before" every day and
            # came back as a 1970 date, so every asset with a completed PM counted as overdue
            # on every day: a series climbing 1,527 -> 1,773 beside a correct snapshot of 299.
            comp[int(k)] = np.sort(pd.to_datetime(g["maintenance_ts"]).values
                                   .astype("datetime64[ns]").astype("int64"))
    seed = {k: pm_seed(a, history_start, calib) for k, a in assets.items()}
    out = np.zeros((len(assets), len(days)), dtype=bool)
    for t, day in enumerate(days):
        h = day + _DAY
        for r, (k, a) in enumerate(assets.items()):
            if a["install"] >= h:
                continue
            c = comp.get(k)
            j = int(np.searchsorted(c, h.value, "left")) if c is not None else 0
            lc = pd.Timestamp(int(c[j - 1])) if j else seed[k]
            out[r, t] = next_due(a, lc) < h
    return out


# ---- 03e's own: LDAR repairs re-derived from the stored survey rows ----------------------------------
def ldar_repairs(facility_id, survey_sk, survey_ts, leaks_detected):
    """[(repair_ts, cost_usd)] for one stored survey, re-derived from 03d's own keys: the
    calendar index k is recovered by matching survey_sk, and each leak's delay and cost are
    drawn from get_rng("ldar_leak", facility_id, k, i) in 03d's order. The run asserts that at
    the horizon these reproduce every stored leaks_repaired and repair_cost_usd exactly."""
    anchor = LDAR_EPOCH + pd.Timedelta(
        days=float(get_rng("ldar_phase", facility_id).uniform(0.0, LDAR_CADENCE_DAYS)))
    span = pd.Timedelta(days=LDAR_CADENCE_DAYS)
    k0 = int(round((survey_ts - pd.Timedelta(hours=8) - anchor) / span))
    ks = [k for k in (k0 - 1, k0, k0 + 1) if stable_key("ldar_survey", facility_id, k) == survey_sk]
    assert len(ks) == 1, (f"survey {survey_sk} at {facility_id}: its calendar index cannot be "
                          "recovered -- 03d's survey calendar changed")
    out = []
    for i in range(int(leaks_detected)):
        lr = get_rng("ldar_leak", facility_id, ks[0], i)
        if lr.random() < LDAR_DELAY_SHARE:
            d = float(lr.uniform(*LDAR_DELAY_DAYS))
        else:
            d = float(min(LDAR_REPAIR_MAX_DAYS,
                          lr.lognormal(np.log(LDAR_REPAIR_MEDIAN_DAYS), LDAR_REPAIR_SIGMA)))
        out.append((survey_ts + pd.Timedelta(seconds=int(round(max(d, 0.25) * 86400))),
                    round(float(lr.uniform(*LDAR_REPAIR_COST)), 2)))
    return out


# ---- financial impact -------------------------------------------------------------------------------
def _lost_gas_usd(kg):
    return kg / KG_CH4_PER_MCF * GAS_PRICE_USD_PER_MCF


def _social_usd(kg):
    return kg / 1000.0 * SOCIAL_COST_USD_PER_T_CH4 if SOCIAL_COST_ON else 0.0


FIN_COLS = ["financial_sk", "facility_sk", "facility_id", "plume_id", "compliance_sk",
            "regulation_sk", "impact_type", "detect_ts", "period_month", "emission_rate_kg_h",
            "duration_h", "allocation_factor", "attributed_emissions_kg", "gwp_co2e_tonnes",
            "lost_gas_value_usd", "violation_fine_usd", "social_cost_usd",
            "total_impact_usd_p5", "total_impact_usd_p50", "total_impact_usd_p95",
            "fine_scenario", "date_sk", "is_synthetic"]


def emission_rows(plumes, lo, hi, fac_sk_of, fac_id_of, olre_of):
    """One row per attributed plume detected in [lo, hi). plumes: gold_plume_catalog with
    detect_ts. olre_of: plume_id -> (compliance_sk, regulation_sk) of its OLRE report."""
    p = plumes[(plumes["detect_ts"] >= lo) & (plumes["detect_ts"] < hi)
               & plumes["attributed_facility_id"].notna()]
    scenario = "social_cost" if SOCIAL_COST_ON else FINE_SCENARIO
    rows = []
    for r in p.sort_values(["detect_ts", "plume_id"], kind="mergesort").to_dict("records"):
        fsk = int(fac_sk_of[r["attributed_facility_id"]])
        dur_h = float(r["t_mix_s"]) / 3600.0
        kg = float(r["emission_rate_kg_h"]) * dur_h * ALLOCATION_FACTOR
        kq = [float(r[c]) * dur_h * ALLOCATION_FACTOR
              for c in ("emission_rate_p5_kg_h", "emission_rate_p50_kg_h", "emission_rate_p95_kg_h")]
        ce_sk, reg_sk = olre_of.get(str(r["plume_id"]), (None, None))
        t = pd.Timestamp(r["detect_ts"])
        rows.append({
            "financial_sk": stable_key("financial", "emission", str(r["plume_id"])),
            "facility_sk": fsk, "facility_id": fac_id_of[fsk], "plume_id": str(r["plume_id"]),
            "compliance_sk": ce_sk, "regulation_sk": reg_sk, "impact_type": "emission",
            "detect_ts": t, "period_month": t.strftime("%Y-%m"),
            "emission_rate_kg_h": float(r["emission_rate_kg_h"]), "duration_h": round(dur_h, 6),
            "allocation_factor": ALLOCATION_FACTOR,
            "attributed_emissions_kg": round(kg, 3),
            "gwp_co2e_tonnes": round(kg / 1000.0 * GWP_CH4_100, 4),
            "lost_gas_value_usd": round(_lost_gas_usd(kg), 2), "violation_fine_usd": 0.0,
            "social_cost_usd": round(_social_usd(kg), 2),
            "total_impact_usd_p5": round(_lost_gas_usd(kq[0]) + _social_usd(kq[0]), 2),
            "total_impact_usd_p50": round(_lost_gas_usd(kq[1]) + _social_usd(kq[1]), 2),
            "total_impact_usd_p95": round(_lost_gas_usd(kq[2]) + _social_usd(kq[2]), 2),
            "fine_scenario": scenario, "date_sk": int(t.strftime("%Y%m%d")), "is_synthetic": True})
    return rows


def fine_rows(ce, lo, hi, plume_ids, fac_id_of):
    """One row per notice of violation issued in [lo, hi), dated at the notice. plume_id is
    the cited plume when a plume escalated the case, NULL for a flare-alarm case."""
    v = ce[(ce["status"] == "Violation") & (ce["status_ts"] >= lo) & (ce["status_ts"] < hi)]
    scenario = "social_cost" if SOCIAL_COST_ON else FINE_SCENARIO
    rows = []
    for r in v.sort_values(["status_ts", "compliance_sk"], kind="mergesort").to_dict("records"):
        t = pd.Timestamp(r["status_ts"])
        fine = round(float(r["fine_usd"]), 2)
        ref = str(r["source_ref"])
        fsk = int(r["facility_sk"])
        rows.append({
            "financial_sk": stable_key("financial", "fine", int(r["compliance_sk"])),
            "facility_sk": fsk, "facility_id": fac_id_of[fsk],
            "plume_id": ref if ref in plume_ids else None,
            "compliance_sk": int(r["compliance_sk"]), "regulation_sk": int(r["regulation_sk"]),
            "impact_type": "violation_fine", "detect_ts": t, "period_month": t.strftime("%Y-%m"),
            "emission_rate_kg_h": None, "duration_h": None, "allocation_factor": None,
            "attributed_emissions_kg": 0.0, "gwp_co2e_tonnes": 0.0, "lost_gas_value_usd": 0.0,
            "violation_fine_usd": fine, "social_cost_usd": 0.0,
            "total_impact_usd_p5": fine, "total_impact_usd_p50": fine, "total_impact_usd_p95": fine,
            "fine_scenario": scenario, "date_sk": int(t.strftime("%Y%m%d")), "is_synthetic": True})
    return rows


# ---- production ---------------------------------------------------------------------------------------
PROD_COLS = ["facility_sk", "facility_id", "date_sk", "prod_date", "gross_gas_mcf", "oil_bbl",
             "water_bbl", "operating_hours", "downtime_flag", "is_synthetic"]


def state_seconds(state, fac_of_asset, fac_pos, days):
    """(producing, downtime, covered) asset-seconds per (facility, day), arrays [n_fac, n_days].
    An interval still open at the horizon runs on past every day it could be asked about."""
    s = state[state["equipment_sk"].isin(list(fac_of_asset))]
    a = s["start_ts"].values.astype("datetime64[ns]").astype("int64")
    e = s["end_ts"].values.astype("datetime64[ns]")
    b = np.where(np.isnat(e), np.iinfo("int64").max, e.astype("int64"))
    f = np.array([fac_pos[fac_of_asset[int(k)]] for k in s["equipment_sk"]], dtype=int)
    prod_m = s["state"].isin(PRODUCING_STATES).values
    down_m = s["state"].isin(DOWNTIME_STATES).values
    out = np.zeros((3, len(fac_pos), len(days)))
    for j, d in enumerate(days):
        lo, hi = d.value, (d + _DAY).value
        ov = np.clip(np.minimum(b, hi) - np.maximum(a, lo), 0, None) / 1e9
        for i, m in enumerate((prod_m, down_m, np.ones(len(ov), dtype=bool))):
            np.add.at(out[i, :, j], f[m], ov[m])
    return out[0], out[1], out[2]


def production_rows(fac, days, secs):
    prod_s, down_s, cov_s = secs
    rows = []
    for i, fr in enumerate(fac.to_dict("records")):
        for j, d in enumerate(days):
            share = prod_s[i, j] / cov_s[i, j] if cov_s[i, j] > 0 else 0.0
            down = down_s[i, j] / cov_s[i, j] if cov_s[i, j] > 0 else 0.0
            dsk = int(d.strftime("%Y%m%d"))
            r = get_rng("production", fr["facility_id"], dsk)
            gas = round(PROD_BASE_GAS_MCF_D[fr["facility_type"]]
                        * float(r.uniform(1.0 - PROD_DAILY_SPREAD, 1.0 + PROD_DAILY_SPREAD)) * share, 1)
            oil_r, water_r = float(r.uniform(*OIL_BBL_PER_MCF)), float(r.uniform(*WATER_BBL_PER_MCF))
            rows.append({"facility_sk": int(fr["facility_sk"]), "facility_id": fr["facility_id"],
                         "date_sk": dsk, "prod_date": d, "gross_gas_mcf": gas,
                         "oil_bbl": round(gas * oil_r, 1), "water_bbl": round(gas * water_r, 1),
                         "operating_hours": round(24.0 * share, 2),
                         "downtime_flag": bool(down >= DOWNTIME_FLAG_SHARE), "is_synthetic": True})
    return rows


# ---- the snapshot ---------------------------------------------------------------------------------------
RISK_COMPONENTS = ("condition", "emissions", "compliance", "maintenance", "open_work")
SNAP_COLS = (["date_sk", "facility_sk", "facility_id", "snapshot_date", "active_plumes",
              "total_emissions_kg", "emission_rate_kg_s_sum", "daily_financial_impact",
              "open_tickets", "stalled_backlog", "mean_time_to_repair_hr", "compliance_violations",
              "open_compliance_cases", "overdue_pms", "ldar_leaks_outstanding",
              "active_exceedances", "sensors_offline", "alarms_raised", "alarms_active",
              "equipment_health_score", "risk_score"]
             + [f"risk_pts_{c}" for c in RISK_COMPONENTS] + ["is_synthetic"])


def risk_points(cond, plumes_30d, violations_30d, open_state_cases, overdue_share, ldar_out,
                tickets):
    """{component: points}; the points sum to the risk score."""
    s = {"condition": min(max(cond, 0.0), 1.0),
         "emissions": min(plumes_30d / RISK_PLUMES_AT_MAX, 1.0),
         "compliance": min(violations_30d + RISK_OPEN_CASE_WEIGHT * open_state_cases, 1.0),
         "maintenance": 0.5 * min(overdue_share / RISK_OVERDUE_SHARE_AT_MAX, 1.0)
                        + 0.5 * min(ldar_out / RISK_LDAR_AT_MAX, 1.0),
         "open_work": min(tickets / RISK_TICKETS_AT_MAX, 1.0)}
    return {c: round(RISK_WEIGHTS[c] * s[c], 2) for c in RISK_COMPONENTS}


def _per_fac(frame, fac_col, fac_pos):
    """{position: sub-frame} for the facilities that have rows."""
    return {fac_pos[int(k)]: g for k, g in frame.groupby(fac_col) if int(k) in fac_pos}


def compute_days(days, I):
    """(production, financial, snapshot) rows for exactly these days. Every value for day d is
    a function of rows timestamped before d's end, so a day computed alone, in any batch, on
    any later run, is the same day."""
    days = [pd.Timestamp(d).normalize() for d in days]
    fac, assets, fac_pos = I["fac"], I["assets"], I["fac_pos"]
    nF, nD = len(fac), len(days)
    fin = []
    for d in days:
        fin += emission_rows(I["plumes"], d, d + _DAY, I["fac_sk_of"], I["fac_id_of"], I["olre_of"])
        fin += fine_rows(I["ce"], d, d + _DAY, I["plume_ids"], I["fac_id_of"])
    secs = state_seconds(I["state"], I["fac_of_asset"], fac_pos, days)
    prod = production_rows(fac, days, secs)

    z = lambda: np.zeros((nF, nD))   # noqa: E731
    act, bkl, ncase, nstate = z(), z(), z(), z()
    for p, g in _per_fac(I["wo"], "facility_sk", fac_pos).items():
        act[p], bkl[p] = observable_trajectory(g, days)
    for p, g in _per_fac(I["ce"], "facility_sk", fac_pos).items():
        ncase[p] = open_trajectory(g, days)
        nstate[p] = open_trajectory(g[g["event_type"].isin(STATE_CASE_TYPES)], days)
    flags = overdue_flags(assets, I["maint"], days, I["history_start"], I["calib"])
    arow = np.array([fac_pos[a["facility_sk"]] for a in assets.values()], dtype=int)
    overdue, n_assets, cond_sum = z(), z(), z()
    for j, d in enumerate(days):
        h = d + _DAY
        live = np.array([a["install"] < h for a in assets.values()])
        np.add.at(overdue[:, j], arow[live], flags[live, j])
        np.add.at(n_assets[:, j], arow[live], 1.0)
        c = np.array([condition(I["si"], a, h, I["history_start"]) if a["install"] < h else 0.0
                      for a in assets.values()])
        np.add.at(cond_sum[:, j], arow[live], c[live])

    pl, al, cev, wo, lr = I["plumes_att"], I["alarms"], I["ce"], I["wo"], I["ldar"]
    tel, sen = I["tel"], I["sensors"]
    tel_ns = tel["reading_ts"].values.astype("datetime64[ns]").astype("int64")
    out = []
    fin_df = pd.DataFrame(fin, columns=FIN_COLS)
    for j, d in enumerate(days):
        h, dsk = d + _DAY, int(d.strftime("%Y%m%d"))
        # The last OFFLINE_KPI_HOURS before the day's end, [h - 8 h, h): 02e's horizon, but ending
        # strictly before h. A reading stamped exactly at midnight belongs to the next day, and
        # does not exist yet when this day is first computed. The first version took (h - 8 h, h],
        # and the recompute check caught sensors_offline changing on a past day.
        k8 = h - pd.Timedelta(hours=OFFLINE_KPI_HOURS)
        w = (tel_ns >= k8.value) & (tel_ns < h.value)
        last = tel[w].sort_values(["sensor_id", "reading_ts"], kind="mergesort") \
            .drop_duplicates("sensor_id", keep="last")
        exc = last[last["exceedance_flag"].astype(bool)].groupby("facility_sk").size()
        due = sen[sen["install_ts"] <= k8]
        off = due[~due["sensor_id"].isin(set(last["sensor_id"]))].groupby("facility_sk").size()
        today = pl[(pl["detect_ts"] >= d) & (pl["detect_ts"] < h)]
        n_pl = today.groupby("facility_sk").size()
        rate = (today["emission_rate_kg_s"] * ALLOCATION_FACTOR).groupby(today["facility_sk"]).sum()
        p30 = pl[(pl["detect_ts"] >= h - pd.Timedelta(days=RISK_TRAILING_DAYS)) & (pl["detect_ts"] < h)] \
            .groupby("facility_sk").size()
        fd = fin_df[fin_df["date_sk"] == dsk]
        kg = fd.groupby("facility_sk")["attributed_emissions_kg"].sum()
        usd = (fd["lost_gas_value_usd"] + fd["violation_fine_usd"] + fd["social_cost_usd"]) \
            .groupby(fd["facility_sk"]).sum()
        viol = cev[cev["status"] == "Violation"]
        v_today = viol[(viol["status_ts"] >= d) & (viol["status_ts"] < h)].groupby("facility_sk").size()
        v_30 = viol[(viol["status_ts"] >= h - pd.Timedelta(days=RISK_TRAILING_DAYS))
                    & (viol["status_ts"] < h)].groupby("facility_sk").size()
        closed = wo[(wo["status"] == "Closed") & (wo["closed_ts"] < h)
                    & (wo["closed_ts"] >= h - pd.Timedelta(days=MTTR_WINDOW_DAYS))]
        mttr = ((closed["closed_ts"] - closed["created_ts"]) / _H).groupby(closed["facility_sk"]).mean()
        raised = al[(al["raised_ts"] >= d) & (al["raised_ts"] < h)].groupby("facility_sk").size()
        standing = al[(al["raised_ts"] < h) & (al["cleared_ts"].isna() | (al["cleared_ts"] >= h))] \
            .groupby("facility_sk").size()
        det = lr[lr["survey_ts"] < h].groupby("facility_sk")["leaks_detected"].sum()
        rep = lr.assign(n=[sum(1 for ts, _ in x if ts < h) for x in lr["_repairs"]]) \
            .groupby("facility_sk")["n"].sum()
        for i, fr in enumerate(fac.to_dict("records")):
            fsk = int(fr["facility_sk"])
            g = lambda s, v=0: s.get(fsk, v)   # noqa: E731
            cond = cond_sum[i, j] / n_assets[i, j] if n_assets[i, j] else 0.0
            share = overdue[i, j] / n_assets[i, j] if n_assets[i, j] else 0.0
            ldar_out = int(g(det)) - int(g(rep))
            pts = risk_points(cond, int(g(p30)), int(g(v_30)), int(nstate[i, j]), share, ldar_out,
                              int(act[i, j] + bkl[i, j]))
            m = g(mttr, None)
            row = {"date_sk": dsk, "facility_sk": fsk, "facility_id": fr["facility_id"],
                   "snapshot_date": d, "active_plumes": int(g(n_pl)),
                   "total_emissions_kg": round(float(g(kg, 0.0)), 3),
                   "emission_rate_kg_s_sum": float(g(rate, 0.0)),
                   "daily_financial_impact": round(float(g(usd, 0.0)), 2),
                   "open_tickets": int(act[i, j]), "stalled_backlog": int(bkl[i, j]),
                   "mean_time_to_repair_hr": None if m is None else round(float(m), 3),
                   "compliance_violations": int(g(v_today)),
                   "open_compliance_cases": int(ncase[i, j]), "overdue_pms": int(overdue[i, j]),
                   "ldar_leaks_outstanding": ldar_out, "active_exceedances": int(g(exc)),
                   "sensors_offline": int(g(off)), "alarms_raised": int(g(raised)),
                   "alarms_active": int(g(standing)),
                   "equipment_health_score": round(1.0 - cond, 6),
                   "risk_score": round(sum(pts.values()), 2), "is_synthetic": True}
            row.update({f"risk_pts_{c}": pts[c] for c in RISK_COMPONENTS})
            out.append(row)
    return prod, fin, out


print("03e model defined -- every value for a day is a function of rows timestamped before its end")
print(f"  lost gas at ${GAS_PRICE_USD_PER_MCF:.2f}/mcf, {KG_CH4_PER_MCF} kg CH4/mcf; "
      f"CO2e at GWP100 {GWP_CH4_100:g}; social cost {'ON' if SOCIAL_COST_ON else 'off'} "
      f"(${SOCIAL_COST_USD_PER_T_CH4:,.0f}/t CH4, shadow price); fines: {FINE_SCENARIO}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Every constant, and where it came from

# CELL ********************

for _name, (_kind, _src) in CONSTANT_BASIS.items():
    _val = globals().get(_name, "per plume")
    print(f"{_name:<27} {str(_val):<16} [{_kind}]")
    print(f"{'':<27} {_src}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run mode and window
#
# **The window follows the sources**, as in 03b: `sensor_telemetry` (the exceedance and offline
# columns need it), `fact_work_order_event` (tickets exist only from 03b's first day) and
# `fact_asset_state`. A backfill covers the whole overlap, and an incremental run takes its last
# day. 03d must have run through that last day, because the PM rebuild is checked against
# `fact_pm_schedule` at its horizon.

# CELL ********************

RUN_MODE = "backfill"
try:
    RUN_MODE = getArgument("run_mode", "backfill")
except Exception:
    pass
RUN_MODE = (str(RUN_MODE) or "backfill").lower()
assert RUN_MODE in ("backfill", "incremental"), "run_mode must be backfill or incremental"
try:
    _start_override = getArgument("start_date", "")
    _end_override = getArgument("end_date", "")
except Exception:
    _start_override, _end_override = "", ""

for _t in ("gold_plume_catalog", "fact_compliance_event", "fact_work_order", "fact_work_order_event",
           "fact_maintenance", "fact_pm_schedule", "fact_ldar_survey", "fact_scada_alarm",
           "sensor_telemetry", "fact_asset_state", "dim_regulation"):
    assert table_exists(_t), f"{_t} does not exist -- run 02a-02e, 04, 05, 03c, 03b and 03d first"


def _span(name):
    r = read_input(name).agg(F.min("date_sk").alias("lo"), F.max("date_sk").alias("hi")).first()
    assert r["lo"] is not None, f"{name} is empty"
    return pd.Timestamp(str(int(r["lo"]))), pd.Timestamp(str(int(r["hi"]))) + _DAY


_ch4 = _span("sensor_telemetry")
_wev = _span("fact_work_order_event")
_state = _span("fact_asset_state")
SOURCE_START = max(_ch4[0], _wev[0])
SOURCE_END = min(_ch4[1], _state[1])
assert SOURCE_START < SOURCE_END, f"the sources do not overlap: {_ch4}, {_wev}, {_state}"
assert _wev[1] >= SOURCE_END - 2 * _DAY, (
    f"fact_work_order_event ends at {(_wev[1] - _DAY).date()}, before the sources' last day "
    f"{(SOURCE_END - _DAY).date()} -- run 03b first")
_pm_as_of = read_input("fact_pm_schedule").agg(F.max("as_of_ts").alias("m")).first()["m"]
assert _pm_as_of is not None and pd.Timestamp(_pm_as_of) == SOURCE_END, (
    f"fact_pm_schedule is as of {_pm_as_of}, not the sources' horizon {SOURCE_END} -- run 03d "
    "through the last day first")

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
assert SOURCE_START <= WINDOW_START < WINDOW_END <= SOURCE_END, (
    f"window {WINDOW_START.date()}..{WINDOW_END.date()} outside the sources' span "
    f"{SOURCE_START.date()}..{SOURCE_END.date()}")
if RUN_MODE == "incremental":
    assert table_exists(SNAP_TABLE), f"{SNAP_TABLE} does not exist -- run a backfill first"
    _sh = read_input(SNAP_TABLE).agg(F.max("date_sk").alias("m")).first()["m"]
    assert _sh is not None and pd.Timestamp(str(int(_sh))) >= WINDOW_START - _DAY, (
        f"{SNAP_TABLE} was last written through date_sk {_sh}; the days before this window "
        "were never processed -- pass start_date to cover them, or run a backfill")

AS_OF = pd.Timestamp(TOPOLOGY_AS_OF)
HISTORY_START = AS_OF - pd.Timedelta(days=STATE_HISTORY_DAYS)     # 03d's, for the PM seed
DAYS = list(pd.date_range(WINDOW_START, WINDOW_END - _DAY, freq="D"))
WS_SK, WE_SK = int(WINDOW_START.strftime("%Y%m%d")), int((WINDOW_END - _DAY).strftime("%Y%m%d"))
print(f"RUN_MODE={RUN_MODE}  window={WINDOW_START.date()}..{WINDOW_END.date()} ({len(DAYS)} day(s))")
print(f"sources span {SOURCE_START.date()}..{SOURCE_END.date()}  (sensor_telemetry "
      f"{_ch4[0].date()}..{_ch4[1].date()}, work order events from {_wev[0].date()}, state to "
      f"{_state[1].date()}); 03d's PM snapshot as of {pd.Timestamp(_pm_as_of)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load everything the model reads
#
# Every table is read whole. The trailing windows (30-day MTTR and risk, the 8-hour offline
# lookback) and the carried state (open tickets, open cases, overdue PMs, LDAR repairs) reach
# back before the window. All of it is small: ~1k tickets, ~100 compliance events, ~3k
# maintenance records, ~7k alarms, and the ~400k CH₄ readings, which are three columns each.

# CELL ********************

fac_pdf = (read_input("dim_facility").filter("is_current = true").toPandas()
           .sort_values("facility_sk", kind="mergesort").reset_index(drop=True))
eq_pdf = read_input("dim_equipment").toPandas()
sen_pdf = read_input("dim_sensor").filter("is_current = true").toPandas()
reg_pdf = read_input("dim_regulation").toPandas()
for _lbl, _f in (("dim_facility", fac_pdf), ("dim_equipment", eq_pdf), ("dim_sensor", sen_pdf)):
    assert (_f["topology_seed"] == TOPOLOGY_SEED).all(), f"{_lbl} built with a different seed"
assert (pd.to_datetime(fac_pdf["commission_date"]) < WINDOW_END).all(), \
    "a facility commissioned after the window has no days to snapshot"
FAC_SK_OF = dict(zip(fac_pdf["facility_id"], fac_pdf["facility_sk"].astype(int)))
FAC_ID_OF = dict(zip(fac_pdf["facility_sk"].astype(int), fac_pdf["facility_id"]))
FAC_POS = {int(k): i for i, k in enumerate(fac_pdf["facility_sk"])}
ASSETS = asset_views(eq_pdf, fac_pdf)
FAC_OF_ASSET = {k: a["facility_sk"] for k, a in ASSETS.items()}
assert all(n > 0 for n in pd.Series(list(FAC_OF_ASSET.values())).value_counts()
           .reindex(list(FAC_POS), fill_value=0)), "a facility with no assets cannot produce"

state_pdf = (read_input("fact_asset_state")
             .select("equipment_sk", "state", "cause", "start_ts", "end_ts").toPandas())
for _c in ("start_ts", "end_ts"):
    state_pdf[_c] = pd.to_datetime(state_pdf[_c])
state_pdf["cause"] = state_pdf["cause"].where(state_pdf["cause"].notna(), None)
SI = state_index(state_pdf)
CALIB = seed_calibration(ASSETS, state_pdf, HISTORY_START)

# --- plumes: the real Monte Carlo percentiles, never a band from a confidence string ------------------
_PLUME_COLS = ["plume_id", "detection_date", "emission_rate_kg_h", "emission_rate_kg_s",
               "emission_rate_p5_kg_h", "emission_rate_p50_kg_h", "emission_rate_p95_kg_h",
               "t_mix_s", "ime_kg", "attributed_facility_id", "attribution_probability"]
_cat = read_input("gold_plume_catalog")
assert set(_PLUME_COLS) <= set(_cat.columns), \
    f"gold_plume_catalog lacks {sorted(set(_PLUME_COLS) - set(_cat.columns))} -- run 04 and 05"
plume_pdf = _cat.select(*_PLUME_COLS).toPandas()
plume_pdf["plume_id"] = plume_pdf["plume_id"].astype(str)
plume_pdf["detect_ts"] = pd.to_datetime(plume_pdf["detection_date"])
plume_pdf["attributed_facility_id"] = plume_pdf["attributed_facility_id"].where(
    plume_pdf["attributed_facility_id"].notna(), None)
_att = plume_pdf[plume_pdf["attributed_facility_id"].notna()]
assert _att["attributed_facility_id"].isin(set(FAC_SK_OF)).all(), \
    "a plume attributed to a facility not in dim_facility"
for _c in ("emission_rate_kg_h", "emission_rate_p5_kg_h", "emission_rate_p50_kg_h",
           "emission_rate_p95_kg_h", "t_mix_s", "ime_kg"):
    assert _att[_c].notna().all() and np.isfinite(_att[_c].astype(float)).all(), \
        f"an attributed plume has no finite {_c}"
assert (_att["t_mix_s"] > 0).all(), "a plume with a non-positive mixing time"
assert ((_att["emission_rate_p5_kg_h"] <= _att["emission_rate_p50_kg_h"])
        & (_att["emission_rate_p50_kg_h"] <= _att["emission_rate_p95_kg_h"])).all(), \
    "04's Monte Carlo percentiles are out of order"
# rate x duration must be the IME exactly: 04 computes rate = IME / t_mix
_ime = _att["emission_rate_kg_h"] * _att["t_mix_s"] / 3600.0
assert np.allclose(_ime, _att["ime_kg"], rtol=1e-9, atol=1e-6), \
    "emission_rate_kg_h x t_mix_s != ime_kg -- 04's rate is no longer IME / t_mix"
plume_att = _att.assign(facility_sk=_att["attributed_facility_id"].map(FAC_SK_OF).astype("int64"))
PLUME_IDS = set(plume_pdf["plume_id"])

# --- compliance: OLRE reports for the plume rows, notices for the fine rows --------------------------
ce_pdf = (read_input("fact_compliance_event")
          .select("compliance_sk", "compliance_id", "facility_sk", "regulation_sk", "event_ts",
                  "event_type", "source_ref", "is_violation", "fine_usd", "status", "status_ts")
          .toPandas())
for _c in ("event_ts", "status_ts"):
    ce_pdf[_c] = pd.to_datetime(ce_pdf[_c])
ce_pdf["source_ref"] = ce_pdf["source_ref"].astype(str)
_olre = ce_pdf[ce_pdf["event_type"] == "OLRE"]
assert not _olre["source_ref"].duplicated().any(), "a plume with two OLRE reports"
OLRE_OF = {r["source_ref"]: (int(r["compliance_sk"]), int(r["regulation_sk"]))
           for r in _olre.to_dict("records")}

# --- work orders and their event log ---------------------------------------------------------------------
wo_pdf = (read_input("fact_work_order")
          .select("work_order_id", "facility_sk", "status", "created_ts", "closed_ts", "sla_hours",
                  "is_breached", "is_stalled").toPandas())
for _c in ("created_ts", "closed_ts"):
    wo_pdf[_c] = pd.to_datetime(wo_pdf[_c])
wev_pdf = (read_input("fact_work_order_event")
           .select("work_order_id", "event_ts", "from_status", "to_status").toPandas())
wev_pdf["event_ts"] = pd.to_datetime(wev_pdf["event_ts"])

# --- maintenance, the PM snapshot and LDAR ----------------------------------------------------------------
maint_pdf = (read_input("fact_maintenance")
             .select("equipment_sk", "maintenance_ts", "maintenance_type", "is_completed").toPandas())
maint_pdf["maintenance_ts"] = pd.to_datetime(maint_pdf["maintenance_ts"])
pm_pdf = (read_input("fact_pm_schedule")
          .select("equipment_sk", "facility_sk", "last_completed_ts", "next_due_ts", "is_overdue",
                  "as_of_ts").toPandas())
for _c in ("last_completed_ts", "next_due_ts", "as_of_ts"):
    pm_pdf[_c] = pd.to_datetime(pm_pdf[_c])
ldar_pdf = (read_input("fact_ldar_survey")
            .select("survey_sk", "facility_sk", "survey_ts", "leaks_detected", "leaks_repaired",
                    "repair_cost_usd").toPandas())
ldar_pdf["survey_ts"] = pd.to_datetime(ldar_pdf["survey_ts"])
ldar_pdf["_repairs"] = [ldar_repairs(FAC_ID_OF[int(f)], int(s), pd.Timestamp(t), int(n))
                        for f, s, t, n in zip(ldar_pdf["facility_sk"], ldar_pdf["survey_sk"],
                                              ldar_pdf["survey_ts"], ldar_pdf["leaks_detected"])]

# --- alarms (facility through the alarm's asset) and the CH4 detectors ---------------------------------
alarm_pdf = (read_input("fact_scada_alarm")
             .select("alarm_sk", "tag_sk", "equipment_sk", "raised_ts", "cleared_ts").toPandas())
for _c in ("raised_ts", "cleared_ts"):
    alarm_pdf[_c] = pd.to_datetime(alarm_pdf[_c])
alarm_pdf["facility_sk"] = alarm_pdf["equipment_sk"].astype("int64").map(FAC_OF_ASSET)
assert alarm_pdf["facility_sk"].notna().all(), "an alarm on an asset not in dim_equipment"
alarm_pdf["facility_sk"] = alarm_pdf["facility_sk"].astype("int64")
tel_pdf = (read_input("sensor_telemetry")
           .select("sensor_id", "facility_sk", "reading_ts", "exceedance_flag").toPandas())
tel_pdf["reading_ts"] = pd.to_datetime(tel_pdf["reading_ts"])
tel_pdf = tel_pdf.sort_values(["reading_ts", "sensor_id"], kind="mergesort").reset_index(drop=True)
sen_pdf["install_ts"] = pd.to_datetime(sen_pdf["install_date"])    # 02e's own derivation

INPUTS = {"fac": fac_pdf, "assets": ASSETS, "fac_pos": FAC_POS, "fac_of_asset": FAC_OF_ASSET,
          "fac_sk_of": FAC_SK_OF, "fac_id_of": FAC_ID_OF, "state": state_pdf, "si": SI,
          "calib": CALIB, "history_start": HISTORY_START, "plumes": plume_pdf,
          "plumes_att": plume_att, "plume_ids": PLUME_IDS, "olre_of": OLRE_OF, "ce": ce_pdf,
          "wo": wo_pdf, "maint": maint_pdf, "ldar": ldar_pdf, "alarms": alarm_pdf,
          "tel": tel_pdf, "sensors": sen_pdf[["sensor_id", "facility_sk", "install_ts"]]}

_win = plume_pdf[(plume_pdf["detect_ts"] >= WINDOW_START) & (plume_pdf["detect_ts"] < WINDOW_END)]
SKIPPED_UNATTRIBUTED = int(_win["attributed_facility_id"].isna().sum())
print(f"{len(fac_pdf)} facilities, {len(ASSETS):,} assets, {len(sen_pdf)} CH4 sensors, "
      f"{len(state_pdf):,} state intervals")
print(f"plumes in window: {len(_win)}; attributed {len(_win) - SKIPPED_UNATTRIBUTED}; "
      f"SKIPPED {SKIPPED_UNATTRIBUTED} unattributed (no facility to charge)")
_ap = _win.loc[_win["attributed_facility_id"].notna(), "attribution_probability"].dropna()
if len(_ap):
    print(f"attribution_probability of the charged plumes (not in the band): median "
          f"{_ap.median():.2f}, range {_ap.min():.2f}-{_ap.max():.2f}")
print(f"{len(ce_pdf)} compliance events ({int((ce_pdf['status'] == 'Violation').sum())} violations), "
      f"{len(wo_pdf):,} work orders, {len(maint_pdf):,} maintenance records, {len(ldar_pdf)} surveys, "
      f"{len(alarm_pdf):,} alarms, {len(tel_pdf):,} CH4 readings")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Compute the window, and check the copies against their owners at the horizon
#
# The copies of 03d's PM state and LDAR repairs are checked against 03d's own snapshots. At
# 03d's horizon, the rebuilt overdue flag and next-due instant must equal `fact_pm_schedule`
# for every asset. The re-derived repairs must equal every survey's stored `leaks_repaired`
# and `repair_cost_usd`. The copy of 03b's measures is checked against `open_measures()` on the
# stored rows at the same horizon.

# CELL ********************

prod_rows, fin_rows, snap_rows = compute_days(DAYS, INPUTS)
_again = compute_days(DAYS, INPUTS)
assert _again == (prod_rows, fin_rows, snap_rows), "computing the window twice gave two answers"

# --- at the horizon, the copies reproduce their owners' snapshots exactly ----------------------------------
_HZ = SOURCE_END
_last = [_HZ - _DAY]
_f = overdue_flags(ASSETS, maint_pdf, _last, HISTORY_START, CALIB)[:, 0]
_pmx = pm_pdf.set_index("equipment_sk")
_ks = [k for k, a in ASSETS.items() if a["install"] < _HZ]
assert set(_pmx.index) == set(_ks), "fact_pm_schedule's assets are not the installed estate"
_pos = {k: i for i, k in enumerate(ASSETS)}
_bad = [k for k in _ks if bool(_f[_pos[k]]) != bool(_pmx.at[k, "is_overdue"])]
assert not _bad, (f"the overdue rebuild disagrees with fact_pm_schedule on {len(_bad)} asset(s), "
                  f"e.g. {_bad[:3]} -- the copy of 03d's PM model has drifted")
_comp = maint_pdf[(maint_pdf["maintenance_type"] == "Preventive") & maint_pdf["is_completed"].astype(bool)
                  & (maint_pdf["maintenance_ts"] < _HZ)].groupby("equipment_sk")["maintenance_ts"].max()
_nd_bad = 0
for k in _ks:
    lc = _comp.get(k)
    lc = pd.Timestamp(lc) if lc is not None else pm_seed(ASSETS[k], HISTORY_START, CALIB)
    _nd_bad += int(next_due(ASSETS[k], lc) != pd.Timestamp(_pmx.at[k, "next_due_ts"]))
assert _nd_bad == 0, f"next_due_ts rebuilt differs from fact_pm_schedule on {_nd_bad} asset(s)"
_rep = [sum(1 for ts, _ in x if ts < _HZ) for x in ldar_pdf["_repairs"]]
_cost = [round(sum(c for ts, c in x if ts < _HZ), 2) for x in ldar_pdf["_repairs"]]
assert list(ldar_pdf["leaks_repaired"].astype(int)) == _rep, \
    "re-derived LDAR repairs disagree with fact_ldar_survey.leaks_repaired"
assert np.allclose(ldar_pdf["repair_cost_usd"].astype(float), _cost, atol=0.005), \
    "re-derived LDAR repair costs disagree with fact_ldar_survey.repair_cost_usd"
_act_h, _bkl_h = open_measures(wo_pdf)
for _fsk, _g in wo_pdf.groupby("facility_sk"):
    _a, _b = observable_trajectory(_g, _last)
    assert (_a[0], _b[0]) == (int(_act_h[_g.index].sum()), int(_bkl_h[_g.index].sum())), (
        f"facility {_fsk}: the as-of-day measures disagree with open_measures() at the horizon")

print(f"window: {len(prod_rows):,} production rows, {len(fin_rows)} financial rows "
      f"({sum(r['impact_type'] == 'emission' for r in fin_rows)} emission, "
      f"{sum(r['impact_type'] == 'violation_fine' for r in fin_rows)} fine), "
      f"{len(snap_rows):,} snapshot rows")
print("OK  computing the window twice gives the same rows")
print(f"OK  at the horizon: the overdue rebuild equals fact_pm_schedule on all {len(_ks):,} assets "
      f"({int(_f.sum())} overdue, next_due_ts identical); re-derived repairs equal fact_ldar_survey "
      f"on all {len(ldar_pdf)} surveys; the as-of-day ticket measures equal open_measures()")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write the three tables
#
# Every table is written from Python rows against an explicit schema, so a missing value is a
# real NULL, never the string "NaN" (05's and 06's lesson). `replaceWhere` covers exactly the
# window's days in both run modes. A table that does not exist yet is created, partitioned by
# `date_sk`.

# CELL ********************

SCHEMAS = {
    PROD_TABLE: ("facility_sk long, facility_id string, date_sk long, prod_date timestamp, "
                 "gross_gas_mcf double, oil_bbl double, water_bbl double, operating_hours double, "
                 "downtime_flag boolean, is_synthetic boolean"),
    FIN_TABLE: ("financial_sk long, facility_sk long, facility_id string, plume_id string, "
                "compliance_sk long, regulation_sk long, impact_type string, detect_ts timestamp, "
                "period_month string, emission_rate_kg_h double, duration_h double, "
                "allocation_factor double, attributed_emissions_kg double, gwp_co2e_tonnes double, "
                "lost_gas_value_usd double, violation_fine_usd double, social_cost_usd double, "
                "total_impact_usd_p5 double, total_impact_usd_p50 double, "
                "total_impact_usd_p95 double, fine_scenario string, date_sk long, "
                "is_synthetic boolean"),
    SNAP_TABLE: ("date_sk long, facility_sk long, facility_id string, snapshot_date timestamp, "
                 "active_plumes long, total_emissions_kg double, emission_rate_kg_s_sum double, "
                 "daily_financial_impact double, open_tickets long, stalled_backlog long, "
                 "mean_time_to_repair_hr double, compliance_violations long, "
                 "open_compliance_cases long, overdue_pms long, ldar_leaks_outstanding long, "
                 "active_exceedances long, sensors_offline long, alarms_raised long, "
                 "alarms_active long, equipment_health_score double, risk_score double, "
                 + ", ".join(f"risk_pts_{c} double" for c in RISK_COMPONENTS)
                 + ", is_synthetic boolean"),
}
COLS = {PROD_TABLE: PROD_COLS, FIN_TABLE: FIN_COLS, SNAP_TABLE: SNAP_COLS}
for _t in SCHEMAS:
    assert [p.split()[0] for p in SCHEMAS[_t].split(", ")] == COLS[_t], f"{_t}: schema != columns"


def _py(v):
    if v is None or v is pd.NA or v is pd.NaT or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, np.generic):
        return v.item()
    return v


def write_table(rows, table):
    cols = COLS[table]
    data = [tuple(_py(r[c]) for c in cols) for r in rows]
    sdf = spark.createDataFrame(data, SCHEMAS[table])
    w = sdf.write.format("delta").mode("overwrite")
    if not table_exists(table):
        w.option("overwriteSchema", "true").partitionBy("date_sk").saveAsTable(table)
        print(f"{table}: {len(rows):,} rows (created)")
    else:
        (w.option("replaceWhere", f"date_sk >= {WS_SK} AND date_sk <= {WE_SK}")
          .partitionBy("date_sk").saveAsTable(table))
        print(f"{table}: {len(rows):,} rows (replaceWhere date_sk {WS_SK}..{WE_SK})")


write_table(prod_rows, PROD_TABLE)
write_table(fin_rows, FIN_TABLE)
write_table(snap_rows, SNAP_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation — every check fails the run, none warns
#
# The checks read the tables back and compare them with the inputs. Where a check recomputes a
# measure, it does so independently. Open tickets are replayed from 03b's event log, which is
# a different record from the `created_ts` / `closed_ts` the snapshot counted from.

# CELL ********************

prod = read_input(PROD_TABLE).toPandas()
# A nullable bigint with nulls reaches pandas as float64, which rounds every 63-bit key above
# 2**53. Fill in Spark, then restore the NULLs from exact ints (03d's contractor_sk pattern).
fin = read_input(FIN_TABLE).fillna({"compliance_sk": -1, "regulation_sk": -1}).toPandas()
for _c in ("compliance_sk", "regulation_sk"):
    fin[_c] = pd.array([None if v < 0 else int(v) for v in fin[_c]], dtype="Int64")
snap = read_input(SNAP_TABLE).toPandas()
_in = lambda f: f[(f["date_sk"] >= WS_SK) & (f["date_sk"] <= WE_SK)]   # noqa: E731
wprod, wfin, wsnap = _in(prod), _in(fin), _in(snap)

# --- the ground truth was never read -------------------------------------------------------------------
assert not TABLES_READ & set(GROUND_TRUTH_TABLES), "a ground-truth table was read"
assert TABLES_READ <= set(INPUT_TABLES), f"undeclared input(s): {TABLES_READ - set(INPUT_TABLES)}"

# --- keys unique, every foreign key resolves ------------------------------------------------------------
assert not fin["financial_sk"].duplicated().any(), "financial_sk not unique"
assert not prod.duplicated(["facility_sk", "date_sk"]).any(), "production facility-day not unique"
assert not snap.duplicated(["facility_sk", "date_sk"]).any(), "snapshot facility-day not unique"
for _lbl, _t in (("production", prod), ("financial", fin), ("snapshot", snap)):
    assert set(_t["facility_sk"]) <= set(FAC_POS), f"{_lbl}: facility_sk unresolved"
    assert (_t["facility_id"] == _t["facility_sk"].map(FAC_ID_OF)).all(), f"{_lbl}: facility_id != facility_sk"
_regs = fin["regulation_sk"].dropna().astype("int64")
assert set(_regs) <= set(reg_pdf["regulation_sk"].astype("int64")), "regulation_sk unresolved"
assert set(fin["plume_id"].dropna()) <= PLUME_IDS, "plume_id not in gold_plume_catalog"
assert set(fin["compliance_sk"].dropna().astype("int64")) <= set(ce_pdf["compliance_sk"].astype("int64")), \
    "compliance_sk not in fact_compliance_event"
assert set(fin["impact_type"]) <= {"emission", "violation_fine"}

# --- every facility-day has a row, including facilities where nothing happened ----------------------------
_want = {(f, int(d.strftime("%Y%m%d"))) for f in FAC_POS for d in DAYS}
for _lbl, _t in (("snapshot", wsnap), ("production", wprod)):
    _got = set(zip(_t["facility_sk"].astype(int), _t["date_sk"].astype(int)))
    assert _got == _want, (f"{_lbl}: {len(_want - _got)} facility-day(s) missing, "
                           f"{len(_got - _want)} unexpected")

# --- financial rows: only attributed plumes, every notice once, the band in order ----------------------
_em = wfin[wfin["impact_type"] == "emission"]
_fn = wfin[wfin["impact_type"] == "violation_fine"]
_wplumes = _win[_win["attributed_facility_id"].notna()]
assert sorted(_em["plume_id"]) == sorted(_wplumes["plume_id"]), \
    "emission rows are not exactly the window's attributed plumes"
_viol = ce_pdf[(ce_pdf["status"] == "Violation") & (ce_pdf["status_ts"] >= WINDOW_START)
               & (ce_pdf["status_ts"] < WINDOW_END)]
assert sorted(_fn["compliance_sk"].astype("int64")) == sorted(_viol["compliance_sk"].astype("int64")), \
    "fine rows are not exactly the window's notices of violation"
assert np.isclose(_fn["violation_fine_usd"].sum(), _viol["fine_usd"].sum(), atol=0.01), \
    "fines do not reconcile with fact_compliance_event"
assert ((wfin["total_impact_usd_p5"] <= wfin["total_impact_usd_p50"])
        & (wfin["total_impact_usd_p50"] <= wfin["total_impact_usd_p95"])).all(), \
    "total_impact_usd p5 <= p50 <= p95 violated"
_kg = _em["emission_rate_kg_h"] * _em["duration_h"] * _em["allocation_factor"]
assert np.allclose(_kg, _em["attributed_emissions_kg"], atol=0.001), \
    "attributed_emissions_kg != emission_rate_kg_h x duration_h x allocation_factor"
assert (_em["violation_fine_usd"] == 0).all() and (_fn["attributed_emissions_kg"] == 0).all()
assert SOCIAL_COST_ON or (wfin["social_cost_usd"] == 0).all(), "a social cost with the switch off"

# --- the snapshot reconciles with the financial table ------------------------------------------------------
_kg_fd = wfin.groupby(["facility_sk", "date_sk"])["attributed_emissions_kg"].sum()
_s = wsnap.set_index(["facility_sk", "date_sk"])
_j = _s["total_emissions_kg"].to_frame().join(_kg_fd.rename("fin_kg"), how="left").fillna({"fin_kg": 0.0})
assert np.allclose(_j["total_emissions_kg"], _j["fin_kg"], atol=0.001), \
    "total_emissions_kg does not reconcile with fact_financial_impact.attributed_emissions_kg"
_usd_fd = (wfin["lost_gas_value_usd"] + wfin["violation_fine_usd"] + wfin["social_cost_usd"]) \
    .groupby([wfin["facility_sk"], wfin["date_sk"]]).sum()
_j = _s["daily_financial_impact"].to_frame().join(_usd_fd.rename("fin_usd"), how="left").fillna({"fin_usd": 0.0})
assert np.allclose(_j["daily_financial_impact"], _j["fin_usd"], atol=0.01), \
    "daily_financial_impact does not reconcile with fact_financial_impact"
_nv = _fn.groupby(["facility_sk", "date_sk"]).size()
_j = _s["compliance_violations"].to_frame().join(_nv.rename("n"), how="left").fillna({"n": 0})
assert (_j["compliance_violations"] == _j["n"]).all(), "compliance_violations != notices issued that day"

# --- open_tickets against 03b's active-open, computed independently from the event log -----------------
_ev = wev_pdf.assign(_o=wev_pdf["to_status"].map({"Open": 0, "In Progress": 1, "Closed": 2,
                                                   "Cancelled": 2}))
assert _ev["_o"].notna().all(), "an unknown status in fact_work_order_event"
_ev = _ev.sort_values(["work_order_id", "event_ts", "_o"], kind="mergesort")
_meta = wo_pdf.set_index("work_order_id")[["facility_sk", "created_ts", "sla_hours"]]
assert set(_ev["work_order_id"]) == set(_meta.index), "event log and fact_work_order disagree on tickets"
_bad = 0
for d in DAYS:
    h = d + _DAY
    st = _ev[_ev["event_ts"] < h].drop_duplicates("work_order_id", keep="last").set_index("work_order_id")
    m = _meta.loc[st.index]
    live = st["to_status"].isin(("Open", "In Progress"))
    within = (m["created_ts"] + pd.to_timedelta(m["sla_hours"], unit="h")) >= h
    a_ = m[live & within].groupby("facility_sk").size()
    b_ = m[live & ~within].groupby("facility_sk").size()
    s_ = _s.xs(int(d.strftime("%Y%m%d")), level="date_sk")
    _bad += int((s_["open_tickets"] != a_.reindex(s_.index, fill_value=0)).sum())
    _bad += int((s_["stalled_backlog"] != b_.reindex(s_.index, fill_value=0)).sum())
assert _bad == 0, f"open_tickets / stalled_backlog disagree with the event-log replay on {_bad} facility-day(s)"

# --- ranges -------------------------------------------------------------------------------------------------
_pts = wsnap[[f"risk_pts_{c}" for c in RISK_COMPONENTS]]
assert wsnap["risk_score"].between(*RISK_RANGE).all(), f"risk_score outside {RISK_RANGE}"
assert np.allclose(_pts.sum(axis=1), wsnap["risk_score"], atol=0.011), "risk points do not sum to the score"
for c in RISK_COMPONENTS:
    assert wsnap[f"risk_pts_{c}"].between(0, RISK_WEIGHTS[c]).all(), f"risk_pts_{c} outside its weight"
assert wsnap["equipment_health_score"].between(0, 1).all(), "equipment_health_score outside [0, 1]"
assert (wsnap["ldar_leaks_outstanding"] >= 0).all(), "negative LDAR outstanding"
assert (wprod["gross_gas_mcf"] >= 0).all() and wprod["operating_hours"].between(0, 24).all()
assert not (wprod["downtime_flag"] & (wprod["operating_hours"] > 24 * (1 - DOWNTIME_FLAG_SHARE))).any(), \
    "a downtime day that still ran most of the day"

print("OK  read no ground-truth table; keys unique; facility, regulation, plume and compliance keys resolve")
print(f"OK  every facility-day has a snapshot and a production row: {len(FAC_POS)} x {len(DAYS)} = "
      f"{len(_want):,}")
print(f"OK  emission rows are exactly the {len(_em)} attributed plumes ({SKIPPED_UNATTRIBUTED} "
      f"unattributed skipped); fine rows are exactly the {len(_fn)} notices, ${_fn['violation_fine_usd'].sum():,.2f}")
print("OK  attributed_emissions_kg = rate x duration x allocation; p5 <= p50 <= p95 on every row")
print("OK  total_emissions_kg, daily_financial_impact and compliance_violations reconcile with "
      "fact_financial_impact")
print("OK  open_tickets and stalled_backlog equal an independent replay of fact_work_order_event")
print(f"OK  risk_score inside {RISK_RANGE} and equal to the sum of its points")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Recomputing past days reproduces them exactly
#
# A sample of stored days is recomputed on its own, from the inputs as they stand now, and
# compared with the stored rows, value for value. In an incremental run the sample is taken
# from days **earlier runs** wrote, against upstream tables that have moved on since. That is
# the real test of the point-in-time claim. In a backfill every day is this run's, so the
# check proves only that a day computed alone equals the same day computed in a batch.

# CELL ********************

_stored_days = sorted(set(snap["date_sk"].astype(int)))
_prior = [d for d in _stored_days if d < WS_SK]
_pool = _prior if _prior else _stored_days
_pick = sorted(set(_pool[i] for i in np.linspace(0, len(_pool) - 1, min(5, len(_pool))).round().astype(int)))
_rp, _rf, _rs = compute_days([pd.Timestamp(str(d)) for d in _pick], INPUTS)


def _norm(rows_or_df, cols, key):
    df = rows_or_df if isinstance(rows_or_df, pd.DataFrame) else pd.DataFrame(rows_or_df, columns=cols)
    df = df[cols].copy()
    for c in df.columns:
        if str(df[c].dtype).startswith("datetime64"):
            df[c] = df[c].astype("datetime64[ns]")
    df = df.astype(object).where(df.notna(), None)
    return df.sort_values(key, kind="mergesort").reset_index(drop=True)


for _lbl, _rows, _tbl, _cols, _key in (
        ("production", _rp, prod, PROD_COLS, ["facility_sk", "date_sk"]),
        ("financial", _rf, fin, FIN_COLS, ["financial_sk"]),
        ("snapshot", _rs, snap, SNAP_COLS, ["facility_sk", "date_sk"])):
    _st = _tbl[_tbl["date_sk"].isin(_pick)]
    a, b = _norm(_rows, _cols, _key), _norm(_st, _cols, _key)
    assert len(a) == len(b), f"{_lbl}: recomputed {len(a)} rows, stored {len(b)}"
    for c in _cols:
        x, y = a[c].tolist(), b[c].tolist()
        if c in ("date_sk", "facility_sk", "financial_sk", "compliance_sk", "regulation_sk"):
            x = [None if v is None else int(v) for v in x]
            y = [None if v is None else int(v) for v in y]
        elif c in ("detect_ts", "snapshot_date", "prod_date"):
            x = [None if v is None else pd.Timestamp(v) for v in x]
            y = [None if v is None else pd.Timestamp(v) for v in y]
        assert x == y, f"{_lbl}.{c}: a recomputed past day differs from what was stored"
print(f"OK  recomputed {len(_pick)} stored day(s) {_pick} "
      f"({'written by earlier runs' if _prior else 'this run, computed one at a time'}): "
      "production, financial and snapshot rows identical")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### What the dashboard will show

# CELL ********************

_emw = wfin[wfin["impact_type"] == "emission"]
_tot = {"lost gas (point estimate)": wfin["lost_gas_value_usd"].sum(),
        "fines (notices of violation)": wfin["violation_fine_usd"].sum(),
        "social cost (shadow price)": wfin["social_cost_usd"].sum()}
print(f"FINANCIAL IMPACT, {WINDOW_START.date()}..{(WINDOW_END - _DAY).date()} (fine scenario "
      f"'{FINE_SCENARIO}', social cost {'ON' if SOCIAL_COST_ON else 'off'}):")
for k, v in _tot.items():
    print(f"  {k:<30} ${v:>14,.2f}")
print(f"  {'total, point estimate':<30} ${sum(_tot.values()):>14,.2f}")
print(f"  total by 04's Monte Carlo       p5 ${wfin['total_impact_usd_p5'].sum():,.0f}   "
      f"p50 ${wfin['total_impact_usd_p50'].sum():,.0f}   p95 ${wfin['total_impact_usd_p95'].sum():,.0f}")
print(f"  p50 against the point estimate on the emission rows: "
      f"{(_emw['total_impact_usd_p50'].sum() / max(_emw['lost_gas_value_usd'].sum() + _emw['social_cost_usd'].sum(), 1e-9) - 1):+.1%} "
      "(p50 is the Monte Carlo median, not 04's point rate)")
print(f"  methane {_emw['attributed_emissions_kg'].sum() / 1000:,.1f} t observed ({len(_emw)} plumes), "
      f"CO2e {_emw['gwp_co2e_tonnes'].sum():,.0f} t at GWP100 {GWP_CH4_100:g}")
_gas = wprod["gross_gas_mcf"].sum()
print(f"  methane intensity: {_emw['attributed_emissions_kg'].sum() / KG_CH4_PER_MCF:,.0f} mcf CH4 "
      f"over {_gas:,.0f} mcf gross gas = {_emw['attributed_emissions_kg'].sum() / KG_CH4_PER_MCF / max(_gas, 1e-9):.3%} "
      "(the observed-mass floor)")

_top = (wfin.assign(usd=wfin["lost_gas_value_usd"] + wfin["violation_fine_usd"] + wfin["social_cost_usd"])
        .groupby("facility_id").agg(usd=("usd", "sum"), p50=("total_impact_usd_p50", "sum"),
                                    plumes=("impact_type", lambda s: int((s == "emission").sum())),
                                    notices=("impact_type", lambda s: int((s == "violation_fine").sum())))
        .sort_values("usd", ascending=False).head(10))
print("\nTOP 10 FACILITIES BY IMPACT:")
print(_top.to_string(formatters={"usd": "${:,.0f}".format, "p50": "${:,.0f}".format}))

print(f"\nRISK SCORE over {len(wsnap):,} facility-days (range {RISK_RANGE}):")
print("  " + "  ".join(f"p{q} {np.percentile(wsnap['risk_score'], q):.1f}" for q in (5, 25, 50, 75, 95, 99))
      + f"  max {wsnap['risk_score'].max():.1f}")
_h = np.histogram(wsnap["risk_score"], bins=[0, 10, 20, 30, 40, 50, 60, 80, 100.001])
for lo_, hi_, n_ in zip(_h[1][:-1], _h[1][1:], _h[0]):
    print(f"  {lo_:>5.0f}-{min(hi_, 100):<5.0f} {n_:>6,}  {'#' * int(60 * n_ / max(_h[0].max(), 1))}")
print("  mean points by component: " + "  ".join(
    f"{c} {wsnap[f'risk_pts_{c}'].mean():.1f}/{RISK_WEIGHTS[c]:g}" for c in RISK_COMPONENTS))
_last_day = wsnap[wsnap["date_sk"] == WE_SK].sort_values("risk_score", ascending=False).head(5)
print("  highest on the last day: " + "; ".join(
    f"{r.facility_id} {r.risk_score:.1f}" for r in _last_day.itertuples()))

_activity = ["active_plumes", "daily_financial_impact", "open_tickets", "stalled_backlog",
             "compliance_violations", "alarms_raised", "alarms_active", "active_exceedances"]
_zero = (wsnap[_activity] == 0).all(axis=1)
print(f"\nZERO-ACTIVITY facility-days: {int(_zero.sum()):,} of {len(wsnap):,} ({_zero.mean():.1%}) -- "
      "no plume, no cost, no open ticket, no notice, no alarm, no exceedance. They are stored as rows "
      "of zeros, not left missing. Overdue PMs, LDAR leaks and offline sensors are standing "
      "conditions, not activity, and are not part of this test.")
_last_snap = wsnap[wsnap["date_sk"] == WE_SK]
print(f"on the last day: {int(_last_snap['open_tickets'].sum())} active open tickets, "
      f"{int(_last_snap['stalled_backlog'].sum())} stalled backlog, {int(_last_snap['overdue_pms'].sum())} "
      f"overdue PMs, {int(_last_snap['ldar_leaks_outstanding'].sum())} LDAR leaks outstanding, "
      f"{int(_last_snap['active_exceedances'].sum())} active exceedances, "
      f"{int(_last_snap['sensors_offline'].sum())} sensors offline, "
      f"{int(_last_snap['alarms_active'].sum())} alarms standing")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
