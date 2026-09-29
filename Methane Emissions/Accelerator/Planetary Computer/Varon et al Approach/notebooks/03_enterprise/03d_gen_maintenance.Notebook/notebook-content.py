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

# # 03d — Generate Maintenance, Inspections and LDAR
#
# Writes four tables. **`fact_pm_schedule`** is what V1 never had: without a `next_due_ts`
# there is nothing to be overdue against. The other three are **`fact_maintenance`**,
# **`fact_inspection`** and **`fact_ldar_survey`**. V1 generated completed events only, so no
# work ever fell due without being done. Here every backlog has an inflow and an outflow
# (design note §2.3).
#
# ### Maintenance is aligned to `fact_asset_state` — the reconciliation chosen
# 02a already decides when an asset is down for maintenance. It runs each asset to the next
# point on its PM calendar (`pm_phase_days`, copied verbatim below), then Shutdown →
# Maintenance(Scheduled PM) → Startup. After a trip it also takes the asset into
# Maintenance(Corrective). **This notebook does not invent maintenance downtime.**
#
# - A **PM visit** is a 02a `Scheduled PM` Maintenance interval. Its record is stamped at the
#   interval's end, and its downtime is the interval's duration. Completion is uncertain:
#   the visit completes the PM with probability `PM_ON_TIME_COMPLETION`. An incomplete visit
#   is still recorded, with `is_completed = false`, and `next_due_ts` does not advance.
# - An **overdue PM is caught up opportunistically**, during the asset's next Maintenance
#   interval of any cause, with probability `PM_CATCHUP_SHARE`. Otherwise it waits for the
#   next calendar visit. 02a also skips a calendar PM when the asset is down at its due
#   instant, and that asset becomes overdue too.
# - A **corrective record** comes from a **closed work order**, read from `fact_work_order`,
#   never re-derived from alarms. Its `downtime_hours` is the work order's own: the overlap
#   of the asset's Down and Maintenance time with the ticket's life, computed by 03b from
#   the same `fact_asset_state`. A corrective record with **zero downtime** is work done
#   on-line, with the asset running. It is legitimate, and the run counts it. It is never
#   presented as downtime.
#
# So every preventive record coincides exactly with a Maintenance interval, and every
# corrective record's downtime is time the asset was actually down. The validation checks
# both against `fact_asset_state`. What a user comparing the tables will still see: some
# 02a `Corrective` Maintenance intervals have no maintenance record. That is a trip whose
# repair no work order covered, or whose ticket has not closed yet. The run counts them.
#
# ### Inspections and LDAR read condition, never the ground truth
# The leak-found probability rises with `equipment_condition_index`, the model shared with
# 03a's hazard. It is evaluated with days-since-service measured from the asset's last
# Maintenance interval in `fact_asset_state`, exactly as 03a does. **`fact_emission_episode`
# is never read.** An inspector finds what the asset's condition makes likely, not what the
# hidden ground truth says is there.
#
# ### LDAR carries a real backlog
# Leaks found at a survey are repaired over the following days. Each leak's repair delay is
# drawn once at detection. `leaks_repaired` and `leaks_outstanding` on a survey row are as
# of the run's horizon, so detected minus repaired is a genuine outstanding count with a
# genuine lag, not V1's within-survey ratio.
#
# ### Pipeline order
# 03c → 03b → **03d**. 03d reads closed work orders, so 03b runs first each day.

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

PM_TABLE = "fact_pm_schedule"
MAINT_TABLE = "fact_maintenance"
INSP_TABLE = "fact_inspection"
LDAR_TABLE = "fact_ldar_survey"

INPUT_TABLES = ("dim_facility", "dim_equipment", "dim_sensor", "fact_asset_state",
                "fact_work_order", "fact_work_order_event", PM_TABLE, MAINT_TABLE,
                INSP_TABLE, LDAR_TABLE)

# The hidden ground truth. An inspector finds what condition makes likely, not what is there.
GROUND_TRUTH_TABLES = ("fact_emission_episode",)
assert not set(INPUT_TABLES) & set(GROUND_TRUTH_TABLES), "an input is a ground-truth table"

TABLES_READ = set()


def read_input(name):
    """The only way this notebook reads a table."""
    assert name not in GROUND_TRUTH_TABLES, (
        f"{name} is hidden ground truth. Inspections and LDAR find what asset condition makes "
        "likely; reading it would make every 'the inspection found the leak' claim circular."
    )
    assert name in INPUT_TABLES, f"{name} is not a declared input of 03d: {INPUT_TABLES}"
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

# ### The maintenance model
#
# This cell is pure: no Spark, no table, no clock. `tools/harness/harness_maintenance.py`
# executes it verbatim against `01_topology_config`.
#
# Every event is **knowable at its timestamp** from data that exists by then:
#
# - A PM record, at its Maintenance interval's end.
# - A corrective record, at its work order's close.
# - An inspection, at its calendar instant, or, if the asset is stopped then, when it
#   restarts.
# - A survey, at its calendar instant.
# - A leak repair, at the delay drawn when the leak was found.
#
# Every draw is keyed on the event's own identity: (asset, calendar index), (work order),
# (facility, survey index), (survey, leak). Nothing depends on how the run window was sliced.

# CELL ********************

# ---- 03d maintenance model (pure: tools/harness/harness_maintenance.py executes this cell) ----
MAINT_TYPES = ("Preventive", "Corrective")
TRIGGERS = ("Scheduled", "WorkOrder")
INSP_TYPES = ("Routine", "Regulatory", "FollowUp")
INSP_METHODS = ("OGI", "Method21", "AVO", "CMS")
INSP_RESULTS = ("Pass", "Leak Found", "Repair Needed")
EMITTING = ("Running", "Standby", "Startup", "Shutdown")    # an asset that can be inspected

# ---- PM: completion is uncertain (modelling choices) --------------------------------------------
PM_ON_TIME_COMPLETION = 0.85    # share of calendar PM visits that complete the PM
PM_CATCHUP_SHARE = 0.70         # an overdue PM completed during the next Maintenance stop
PM_GRACE_DAYS = 7               # a PM completed within this of its due date is on time
PM_SEED_CYCLES = 12             # how far back the pre-history seed looks for a completion
PM_LABOR_H = (2.0, 9.0)         # V1's range
PM_PARTS_GAMMA = (2.0, 220.0)   # V1's gamma(shape, scale), USD
LABOR_RATE_USD_H = 120.0        # blended crew rate, modelling choice
CONTRACTOR_SHARE = 0.25         # V1
CONTRACTORS = tuple(f"Contractor {c}" for c in "ABCDEF")      # synthetic roster, no dim exists
CONTRACTOR_SK = {stable_key("contractor", c): c for c in CONTRACTORS}

# ---- corrective: from closed work orders ----------------------------------------------------------
# Root cause from the source that raised the ticket -- what the crew was sent to fix.
WO_ROOT_CAUSE = {"Alarm": "Process excursion", "Exceedance": "Methane leak",
                 "SensorStatus": "Instrument fault", "Compliance": "Regulatory corrective action"}
WO_PARTS_SHARE = (0.25, 0.65)   # share of the work order's cost that was parts

# ---- inspections (V1's leak model; modelling choices) ----------------------------------------------
INSP_LEAK_BASE, INSP_LEAK_SLOPE, INSP_LEAK_MAX = 0.04, 0.55, 0.60      # V1: min(0.6, .04+.55c)
INSP_REPAIR_NEEDED_SHARE = 0.05                                        # V1: non-leak defect
FOLLOWUP_DAYS = (14.0, 30.0)    # a Leak Found is re-inspected to verify the repair
FOLLOWUP_LEAK_FACTOR = 0.30     # the repair did not hold
COMPONENTS_BY_TYPE = {"Compressor": 60, "Separator": 40, "Storage Tank": 30, "Valve": 8,
                      "Pipeline Segment": 12, "Pump": 25, "Metering Station": 20, "Flare": 15}
METHOD_BY_TYPE = {"Compressor": "OGI", "Separator": "OGI", "Storage Tank": "OGI",
                  "Flare": "OGI", "Valve": "Method21", "Pipeline Segment": "Method21",
                  "Pump": "AVO", "Metering Station": "Method21"}
REGULATORY_TYPES = ("Compressor", "Storage Tank")   # "Regulatory" at Federal-programme sites

# ---- LDAR (modelling choices) --------------------------------------------------------------------------
LDAR_CADENCE_DAYS = 90          # roughly quarterly, per facility
LDAR_EPOCH = pd.Timestamp("2026-01-01")             # the survey calendar's fixed origin
LDAR_LEAK_RATE = (0.002, 0.020) # per component: base + slope x condition
LDAR_REPAIR_MEDIAN_DAYS, LDAR_REPAIR_SIGMA = 7.0, 0.8
LDAR_REPAIR_MAX_DAYS = 45.0     # ordinary repairs are done inside this
LDAR_DELAY_SHARE = 0.05         # delay of repair: waits for a shutdown
LDAR_DELAY_DAYS = (45.0, 120.0)
LDAR_REPAIR_COST = (400.0, 1800.0)                  # V1's per-leak range, USD
LDAR_METHODS = {"OGI": 0.60, "Method21": 0.25, "Aerial": 0.15}      # V1
# regulatory_program: a SYNTHETIC assignment. Federal where the facility was commissioned on
# or after 2015-09-18, the NSPS OOOOa applicability date (not re-verified here). State for
# tank batteries and gathering systems older than that; Voluntary for the rest. No specific
# rule's applicability is modelled.
FEDERAL_FROM = pd.Timestamp("2015-09-18")

# ---- steady state ---------------------------------------------------------------------------------------
TREND_WINDOW_DAYS, TREND_MAX_RISE = 14, 0.25
WARMUP_DAYS = 45                # LDAR repairs run to 45 days; the overdue pool is seeded stationary
LDAR_REPAIR_LAG_DAYS = 30       # cumulative repaired must reach ...
LDAR_REPAIR_BAND = (0.70, 1.00) # ... this share of cumulative detected LAG days earlier

# ---- teams: the same roster and key as 03b ------------------------------------------------------------
TEAM_DISCIPLINES = ("Mechanical", "Operations", "Instrumentation & Controls",
                    "Environmental & LDAR")
PM_DISCIPLINE = {"Compressor": "Mechanical", "Pump": "Mechanical", "Separator": "Operations",
                 "Storage Tank": "Operations", "Flare": "Operations",
                 "Metering Station": "Instrumentation & Controls", "Valve": "Operations",
                 "Pipeline Segment": "Operations"}
TEAM_ROSTER = {stable_key("team", b, d): f"{b} {d}" for b in ANCHORS for d in TEAM_DISCIPLINES}

assert set(PM_DISCIPLINE) == set(EQUIPMENT_TYPES) == set(COMPONENTS_BY_TYPE) == set(METHOD_BY_TYPE)
assert set(WO_ROOT_CAUSE) == {"Alarm", "Exceedance", "SensorStatus", "Compliance"}
assert 0 < PM_ON_TIME_COMPLETION < 1 and 0 <= PM_CATCHUP_SHARE <= 1
assert abs(sum(LDAR_METHODS.values()) - 1) < 1e-9

_DAY = pd.Timedelta(days=1)
_H = pd.Timedelta(hours=1)


# 02a's own PM calendar, copied verbatim so PM due dates agree with 02a's Maintenance
# intervals. harness_maintenance.py asserts the copy is identical to 02a's text.
def pm_phase_days(equipment_id, insp_days):
    """Per-asset offset into the PM cycle, so PMs are not all due on the same day."""
    return float(get_rng("pm_phase", equipment_id).uniform(0.0, insp_days))


def team_sk(sub_basin, discipline):
    return stable_key("team", sub_basin, discipline)


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


def resume_time(si, a, t, horizon):
    """The first instant at or after t that the asset is in an inspectable state, if known
    before the horizon; None if it is still stopped at the horizon (the inspection waits)."""
    x = si.get(a["equipment_sk"])
    if x is None:
        return None
    starts, ends, states = x[0], x[1], x[2]
    j = int(np.searchsorted(starts, t.value, "right")) - 1
    if j < 0:
        return None
    while j < len(starts):
        if states[j] in EMITTING:
            ts = max(t.value, int(starts[j]))
            return pd.Timestamp(ts) if ts < horizon.value else None
        j += 1
    return None


# ---- PM ------------------------------------------------------------------------------------------------
def pm_k(a, ts):
    """The PM calendar index nearest ts."""
    return int(round((ts - a["pm_anchor"]) / pd.Timedelta(days=a["freq"])))


def pm_completes(a, k):
    return bool(get_rng("pm_complete", a["equipment_id"], k).random() < PM_ON_TIME_COMPLETION)


def pm_seed(a, history_start):
    """last_completed at the history start: the latest calendar PM before it that completed,
    drawn with the same completion model, so the overdue pool starts stationary rather than
    at zero. None for an asset whose calendar starts inside the history."""
    if a["pm_anchor"] >= history_start:
        return None
    k0 = int(np.floor((history_start - a["pm_anchor"]) / pd.Timedelta(days=a["freq"])))
    for k in range(k0, k0 - PM_SEED_CYCLES, -1):
        if k < 0:
            break
        if pm_completes(a, k):
            return a["pm_anchor"] + pd.Timedelta(days=a["freq"] * k)
    return a["pm_anchor"] + pd.Timedelta(days=a["freq"] * max(k0 - PM_SEED_CYCLES + 1, 0))


def next_due(a, last_completed):
    if last_completed is None:
        return a["pm_anchor"]
    return last_completed + pd.Timedelta(days=a["freq"])


def _costs(rng, labor_lo_hi, parts_scale=1.0):
    labor = float(rng.uniform(*labor_lo_hi))
    parts = float(rng.gamma(*PM_PARTS_GAMMA)) * parts_scale
    return labor, parts


def pm_record(a, start, end, kind, completed):
    """A preventive record at the end of a Maintenance interval. kind: 'calendar' | 'catchup'."""
    key = ("pm", a["equipment_id"], start.isoformat())
    rng = get_rng("maint", *key)
    labor, parts = _costs(rng, PM_LABOR_H, 1.0 if completed else 0.0)
    if not completed:
        labor *= 0.5
    contractor = sorted(CONTRACTOR_SK)[int(rng.integers(0, len(CONTRACTOR_SK)))] \
        if rng.random() < CONTRACTOR_SHARE else None
    return {"maintenance_sk": stable_key("maintenance", *key),
            "equipment_sk": a["equipment_sk"], "facility_sk": a["facility_sk"],
            "area_sk": a["area_sk"],
            "team_sk": team_sk(a["sub_basin"], PM_DISCIPLINE[a["equipment_type"]]),
            "contractor_sk": contractor, "maintenance_ts": end,
            "maintenance_type": "Preventive", "trigger": "Scheduled", "work_order_id": None,
            "downtime_hours": (end - start) / _H, "labor_hours": round(labor, 2),
            "parts_cost_usd": round(parts, 2),
            "total_cost_usd": round(labor * LABOR_RATE_USD_H + parts, 2),
            "root_cause": None, "is_completed": completed,
            "date_sk": int(end.strftime("%Y%m%d")), "is_synthetic": True, "_kind": kind}


def wo_record(w, a):
    """A corrective record for a closed work order, at its close."""
    rng = get_rng("maint", "wo", w["work_order_id"])
    total = float(w["cost_usd"])
    parts = round(total * float(rng.uniform(*WO_PARTS_SHARE)), 2)
    return {"maintenance_sk": stable_key("maintenance", "wo", w["work_order_id"]),
            "equipment_sk": a["equipment_sk"], "facility_sk": a["facility_sk"],
            "area_sk": a["area_sk"], "team_sk": int(w["assigned_team_sk"]),
            "contractor_sk": None, "maintenance_ts": pd.Timestamp(w["closed_ts"]),
            "maintenance_type": "Corrective", "trigger": "WorkOrder",
            "work_order_id": w["work_order_id"], "downtime_hours": float(w["downtime_hours"]),
            "labor_hours": round((total - parts) / LABOR_RATE_USD_H, 2),
            "parts_cost_usd": parts, "total_cost_usd": round(total, 2),
            "root_cause": WO_ROOT_CAUSE[w["source"]], "is_completed": True,
            "date_sk": int(pd.Timestamp(w["closed_ts"]).strftime("%Y%m%d")),
            "is_synthetic": True, "_kind": "workorder"}


# ---- inspections ---------------------------------------------------------------------------------------
def followup_due(inspection_sk, inspection_ts):
    """When a Leak Found is re-inspected. Keyed on the parent's inspection_sk, which is stored,
    so pass 1 can re-derive a pending follow-up from the table alone."""
    d = float(get_rng("followup", int(inspection_sk)).uniform(*FOLLOWUP_DAYS))
    return pd.Timestamp(inspection_ts) + pd.Timedelta(seconds=int(round(d * 86400)))


def inspection_event(si, a, kind, key_k, t, history_start, sensors_cms):
    """An inspection at t (already resumed): its draws keyed on (asset, kind, key_k)."""
    rng = get_rng("inspection", a["equipment_id"], kind, key_k)
    cond = condition(si, a, t, history_start)
    p = min(INSP_LEAK_MAX, INSP_LEAK_BASE + INSP_LEAK_SLOPE * cond)
    if kind == "FollowUp":
        p *= FOLLOWUP_LEAK_FACTOR
    n_comp = max(1, int(round(COMPONENTS_BY_TYPE[a["equipment_type"]] * rng.uniform(0.8, 1.2))))
    leak = bool(rng.random() < p)
    n_leaks = int(rng.integers(1, max(2, int(n_comp * 0.15)))) if leak else 0
    defect = bool(rng.random() < INSP_REPAIR_NEEDED_SHARE)
    minutes = float(rng.uniform(1.5, 3.0))
    result = "Leak Found" if n_leaks else ("Repair Needed" if defect else "Pass")
    itype = kind if kind == "FollowUp" else (
        "Regulatory" if a["program"] == "Federal" and a["equipment_type"] in REGULATORY_TYPES
        else "Routine")
    method = "CMS" if a["equipment_sk"] in sensors_cms else METHOD_BY_TYPE[a["equipment_type"]]
    return {"inspection_sk": stable_key("inspection", a["equipment_id"], kind, key_k),
            "equipment_sk": a["equipment_sk"], "facility_sk": a["facility_sk"],
            "team_sk": team_sk(a["sub_basin"], "Environmental & LDAR"),
            "inspection_ts": t, "inspection_type": itype, "method": method,
            "components_checked": n_comp, "leaks_found": n_leaks, "result": result,
            "duration_hours": round(0.25 + n_comp * minutes / 60.0, 2),
            "date_sk": int(t.strftime("%Y%m%d")), "is_synthetic": True, "_cond": cond}


def inspections_in(si, assets, lo, hi, horizon, history_start, sensors_cms, parents):
    """Every inspection with inspection_ts in [lo, hi): routine ones on each asset's calendar,
    deferred to a restart if the asset is stopped, and follow-ups of earlier Leak Found
    results. parents: Leak Found inspections (inspection_sk, equipment_sk, inspection_ts)
    whose follow-up may fall in the window. Calendar instants are looked for up to one cycle
    before lo, because a deferral can carry one forward."""
    out = []
    for a in assets.values():
        if a["install"] >= hi:
            continue
        span = pd.Timedelta(days=a["freq"])
        k_lo = max(0, int(np.floor((lo - span - a["insp_anchor"]) / span)))
        k_hi = int(np.floor((hi - a["insp_anchor"]) / span))
        for k in range(k_lo, k_hi + 1):
            g = a["insp_anchor"] + span * k
            if g < a["install"] or g < history_start:
                continue
            t = resume_time(si, a, g, horizon)
            if t is not None and lo <= t < hi:
                out.append(inspection_event(si, a, "Routine", k, t, history_start, sensors_cms))
    queue = list(parents) + [e for e in out if e["result"] == "Leak Found"]
    seen = set()
    while queue:
        p = queue.pop(0)
        if p["inspection_sk"] in seen:
            continue
        seen.add(p["inspection_sk"])
        a = assets[int(p["equipment_sk"])]
        t = resume_time(si, a, followup_due(p["inspection_sk"], p["inspection_ts"]), horizon)
        if t is not None and lo <= t < hi:
            e = inspection_event(si, a, "FollowUp", int(p["inspection_sk"]), t, history_start,
                                 sensors_cms)
            out.append(e)
            if e["result"] == "Leak Found":
                queue.append(e)
    return sorted(out, key=lambda e: (e["inspection_ts"], e["inspection_sk"]))


# ---- LDAR ----------------------------------------------------------------------------------------------
def ldar_surveys_in(si, assets_by_fac, fac, lo, hi, history_start):
    """Surveys with survey_ts in [lo, hi) on each facility's quarterly calendar."""
    out = []
    span = pd.Timedelta(days=LDAR_CADENCE_DAYS)
    for fr in fac.sort_values("facility_sk").to_dict("records"):
        fid = fr["facility_id"]
        anchor = LDAR_EPOCH + pd.Timedelta(
            days=float(get_rng("ldar_phase", fid).uniform(0.0, LDAR_CADENCE_DAYS)))
        for k in range(int(np.floor((lo - anchor) / span)) - 1, int(np.floor((hi - anchor) / span)) + 1):
            t = (anchor + span * k).floor("D") + pd.Timedelta(hours=8)
            if not (lo <= t < hi) or t < history_start:
                continue
            out.append(ldar_survey(si, assets_by_fac.get(int(fr["facility_sk"]), []), fr, k, t,
                                   history_start))
    return out


def ldar_survey(si, fac_assets, fr, k, t, history_start):
    rng = get_rng("ldar", fr["facility_id"], k)
    live = [a for a in fac_assets if a["install"] <= t]
    comps = sum(COMPONENTS_BY_TYPE[a["equipment_type"]] for a in live)
    lam = sum(COMPONENTS_BY_TYPE[a["equipment_type"]]
              * (LDAR_LEAK_RATE[0] + LDAR_LEAK_RATE[1] * condition(si, a, t, history_start))
              for a in live)
    detected = int(min(comps, rng.poisson(lam)))
    methods = list(LDAR_METHODS)
    method = str(methods[int(rng.choice(len(methods), p=list(LDAR_METHODS.values())))])
    repairs = []
    for i in range(detected):
        lr = get_rng("ldar_leak", fr["facility_id"], k, i)
        if lr.random() < LDAR_DELAY_SHARE:
            d = float(lr.uniform(*LDAR_DELAY_DAYS))
        else:
            d = float(min(LDAR_REPAIR_MAX_DAYS,
                          lr.lognormal(np.log(LDAR_REPAIR_MEDIAN_DAYS), LDAR_REPAIR_SIGMA)))
        repairs.append((t + pd.Timedelta(seconds=int(round(max(d, 0.25) * 86400))),
                        round(float(lr.uniform(*LDAR_REPAIR_COST)), 2)))
    return {"survey_sk": stable_key("ldar_survey", fr["facility_id"], k),
            "facility_sk": int(fr["facility_sk"]),
            "team_sk": team_sk(fr["sub_basin"], "Environmental & LDAR"), "survey_ts": t,
            "survey_method": method, "regulatory_program": ldar_program(fr),
            "components_surveyed": int(comps), "leaks_detected": detected,
            "duration_hours": round(2.0 + comps / 120.0, 2),
            "date_sk": int(t.strftime("%Y%m%d")), "_repairs": repairs}


LDAR_COLS = ["survey_sk", "facility_sk", "team_sk", "survey_ts", "survey_method",
             "regulatory_program", "components_surveyed", "leaks_detected", "leaks_repaired",
             "leaks_outstanding", "repair_cost_usd", "duration_hours", "date_sk", "is_synthetic"]


def ldar_row(s, horizon):
    done = [c for ts, c in s["_repairs"] if ts < horizon]
    r = {k: s[k] for k in ("survey_sk", "facility_sk", "team_sk", "survey_ts", "survey_method",
                           "regulatory_program", "components_surveyed", "leaks_detected",
                           "duration_hours", "date_sk")}
    r.update(leaks_repaired=len(done), leaks_outstanding=s["leaks_detected"] - len(done),
             repair_cost_usd=round(sum(done), 2), is_synthetic=True)
    return {k: r[k] for k in LDAR_COLS}


def ldar_changes_in(s, lo, hi):
    return lo <= s["survey_ts"] < hi or any(lo <= ts < hi for ts, _ in s["_repairs"])


LDAR_LOOKBACK = pd.Timedelta(days=LDAR_DELAY_DAYS[1] + 1)   # a survey's repairs end inside this


# ---- pass 1's read -----------------------------------------------------------------------------------------
def prior_state(assets, fac, si, maint_rows, insp_rows, window_start, history_start):
    """What pass 1 carries in at window_start, selected on timestamps (03b's lesson: after a
    rerun the stored rows are as of a later horizon, so status or snapshot columns would
    describe the wrong instant).

    - last_completed: the latest completed preventive record before window_start, else the
      pre-history seed.
    - parents: Leak Found inspections before window_start whose follow-up may still fall
      due (re-derivable from inspection_sk alone).
    - surveys: plans of surveys dated in the LDAR lookback, regenerated from the calendar,
      so their repairs can keep completing. Their stored rows are checked against these.
    """
    last = {k: pm_seed(a, history_start) for k, a in assets.items()}
    if len(maint_rows):
        done = maint_rows[(maint_rows["maintenance_type"] == "Preventive")
                          & maint_rows["is_completed"].astype(bool)
                          & (maint_rows["maintenance_ts"] < window_start)]
        for k, ts in done.groupby("equipment_sk")["maintenance_ts"].max().items():
            if int(k) in last:
                last[int(k)] = pd.Timestamp(ts)
    parents = []
    if len(insp_rows):
        lf = insp_rows[(insp_rows["result"] == "Leak Found")
                       & (insp_rows["inspection_ts"] < window_start)
                       & (insp_rows["inspection_ts"] >= window_start - pd.Timedelta(days=120))]
        parents = lf[["inspection_sk", "equipment_sk", "inspection_ts"]].to_dict("records")
    by_fac = {}
    for a in assets.values():
        by_fac.setdefault(a["facility_sk"], []).append(a)
    surveys = ldar_surveys_in(si, by_fac, fac, max(history_start, window_start - LDAR_LOOKBACK),
                              window_start, history_start)
    return {"last_completed": last, "parents": parents, "surveys": surveys}


# ---- the two passes ---------------------------------------------------------------------------------------
def run_window(assets, fac, si, state, wos, sensors_cms, prior, lo, hi, history_start):
    """Both passes, day by day, over [lo, hi). Returns (maintenance rows, inspection rows,
    LDAR rows changed in the window as of hi, last_completed at hi)."""
    last = dict(prior["last_completed"])
    maint = state[(state["state"] == "Maintenance") & state["end_ts"].notna()]
    maint = maint[(maint["end_ts"] >= lo) & (maint["end_ts"] < hi)].sort_values(
        ["end_ts", "equipment_sk"], kind="mergesort")
    wo = wos[(wos["status"] == "Closed") & (wos["closed_ts"] >= lo) & (wos["closed_ts"] < hi)]
    by_fac = {}
    for a in assets.values():
        by_fac.setdefault(a["facility_sk"], []).append(a)
    m_rows, i_rows, surveys = [], [], list(prior["surveys"])
    parents = list(prior["parents"])
    day = lo
    while day < hi:
        nxt = min(day + _DAY, hi)
        # pass 1 -- advance: PM visits falling due and completing (or not), overdue PMs caught
        # up in the next stop, work orders closing into corrective records. Leak repairs
        # advance through ldar_row, as of the horizon.
        for r in maint[(maint["end_ts"] >= day) & (maint["end_ts"] < nxt)].to_dict("records"):
            a = assets.get(int(r["equipment_sk"]))
            if a is None:
                continue
            start, end = pd.Timestamp(r["start_ts"]), pd.Timestamp(r["end_ts"])
            due = next_due(a, last.get(a["equipment_sk"]))
            done = False
            if r["cause"] == "Scheduled PM":
                done = pm_completes(a, pm_k(a, start))
                m_rows.append(pm_record(a, start, end, "calendar", done))
            elif due < start and get_rng("pm_catchup", a["equipment_id"],
                                         start.isoformat()).random() < PM_CATCHUP_SHARE:
                done = True
                m_rows.append(pm_record(a, start, end, "catchup", True))
            if done:
                last[a["equipment_sk"]] = end
        for w in wo[(wo["closed_ts"] >= day) & (wo["closed_ts"] < nxt)].sort_values(
                ["closed_ts", "work_order_id"], kind="mergesort").to_dict("records"):
            m_rows.append(wo_record(w, assets[int(w["equipment_sk"])]))
        # pass 2 -- create: the day's inspections, follow-ups and surveys
        new_i = inspections_in(si, assets, day, nxt, hi, history_start, sensors_cms, parents)
        i_rows += new_i
        parents = parents + [{k: e[k] for k in ("inspection_sk", "equipment_sk", "inspection_ts")}
                             for e in new_i if e["result"] == "Leak Found"]
        surveys += ldar_surveys_in(si, by_fac, fac, day, nxt, history_start)
        day = nxt
    l_rows = [ldar_row(s, hi) for s in surveys if ldar_changes_in(s, lo, hi)]
    return m_rows, i_rows, l_rows, last


PM_COLS = ["pm_schedule_sk", "equipment_sk", "facility_sk", "area_sk", "pm_type",
           "frequency_days", "last_completed_ts", "next_due_ts", "is_overdue", "days_overdue",
           "criticality", "assigned_team_sk", "as_of_ts", "date_sk", "is_synthetic"]
MAINT_COLS = ["maintenance_sk", "equipment_sk", "facility_sk", "area_sk", "team_sk",
              "contractor_sk", "maintenance_ts", "maintenance_type", "trigger", "work_order_id",
              "downtime_hours", "labor_hours", "parts_cost_usd", "total_cost_usd", "root_cause",
              "is_completed", "date_sk", "is_synthetic"]
INSP_COLS = ["inspection_sk", "equipment_sk", "facility_sk", "team_sk", "inspection_ts",
             "inspection_type", "method", "components_checked", "leaks_found", "result",
             "duration_hours", "date_sk", "is_synthetic"]


def pm_snapshot(assets, last, horizon):
    rows = []
    for a in sorted(assets.values(), key=lambda x: x["equipment_sk"]):
        if a["install"] >= horizon:
            continue
        lc = last.get(a["equipment_sk"])
        due = next_due(a, lc)
        od = bool(due < horizon)
        rows.append({"pm_schedule_sk": stable_key("pm_schedule", a["equipment_id"],
                                                  f"{a['equipment_type']} PM"),
                     "equipment_sk": a["equipment_sk"], "facility_sk": a["facility_sk"],
                     "area_sk": a["area_sk"], "pm_type": f"{a['equipment_type']} PM",
                     "frequency_days": a["freq"], "last_completed_ts": lc, "next_due_ts": due,
                     "is_overdue": od,
                     "days_overdue": round((horizon - due) / _DAY, 4) if od else 0.0,
                     "criticality": a["criticality"],
                     "assigned_team_sk": team_sk(a["sub_basin"], PM_DISCIPLINE[a["equipment_type"]]),
                     "as_of_ts": horizon, "date_sk": int((horizon - _DAY).strftime("%Y%m%d")),
                     "is_synthetic": True})
    return rows


def overdue_trajectory(assets, maint_rows, days, history_start):
    """Overdue PMs at the end of each day, from completed preventive records plus the seed --
    the same answer in either run mode, because it reads the records, not a snapshot."""
    comp = {}
    if len(maint_rows):
        d = maint_rows[(maint_rows["maintenance_type"] == "Preventive")
                       & maint_rows["is_completed"].astype(bool)]
        for k, g in d.groupby("equipment_sk"):
            comp[int(k)] = np.sort(pd.to_datetime(g["maintenance_ts"]).values.astype("int64"))
    seed = {k: pm_seed(a, history_start) for k, a in assets.items()}
    out = []
    for day in days:
        h = day + _DAY
        n = 0
        for k, a in assets.items():
            if a["install"] >= h:
                continue
            c = comp.get(k)
            j = int(np.searchsorted(c, h.value, "left")) if c is not None else 0
            lc = pd.Timestamp(int(c[j - 1])) if j else seed[k]
            n += int(next_due(a, lc) < h)
        out.append(n)
    return np.array(out)


def ldar_cumulative(surveys, days):
    """Cumulative leaks detected and repaired, and outstanding, at the end of each day."""
    det, rep = [], []
    for day in days:
        h = day + _DAY
        det.append(sum(s["leaks_detected"] for s in surveys if s["survey_ts"] < h))
        rep.append(sum(1 for s in surveys for ts, _ in s["_repairs"] if ts < h))
    det, rep = np.array(det), np.array(rep)
    return det, rep, det - rep


def to_frame(rows, cols, int_cols=(), nullable_int=()):
    df = pd.DataFrame(rows, columns=cols)
    for c in int_cols:
        df[c] = df[c].astype("int64")
    for c in nullable_int:
        df[c] = pd.array([r[c] for r in rows], dtype="Int64")
    return df


def merge_for_write(existing, changed, key, ts_col, window_start, cols):
    """03b's pattern: carry rows dated before the window that this run did not change."""
    keep = existing[(existing[ts_col] < window_start) & ~existing[key].isin(changed[key])]
    out = pd.concat([keep[cols], changed[cols]], ignore_index=True)
    return out.sort_values([ts_col, key], kind="mergesort").reset_index(drop=True)


print("03d model defined -- PM visits are 02a's Scheduled PM intervals; completion, inspections, "
      "surveys and repairs are pure functions of (asset | facility, calendar index, seed)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run mode and window
#
# **The window follows `fact_asset_state`**, as in 03a, because PM visits are its
# Maintenance intervals. A backfill covers the whole state history (`STATE_HISTORY_DAYS`)
# and runs both passes day by day. An incremental run takes the last state day, and
# rerunning it replaces that day. Corrective records need closed work orders, which exist
# only over 03b's retention, so days before it carry preventive work only. That is stated,
# not hidden. 03b must have run through the window's last day.

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

for _t in ("fact_asset_state", "fact_work_order", "fact_work_order_event"):
    assert table_exists(_t), f"{_t} does not exist -- run 02a and 03b first"

AS_OF = pd.Timestamp(TOPOLOGY_AS_OF)
HISTORY_START = AS_OF - pd.Timedelta(days=STATE_HISTORY_DAYS)
_st = read_input("fact_asset_state").agg(F.max("date_sk").alias("m")).first()["m"]
assert _st is not None, "fact_asset_state is empty -- run 02a first"
STATE_HORIZON = pd.Timestamp(str(int(_st))) + _DAY

if RUN_MODE == "backfill":
    WINDOW_START, WINDOW_END = HISTORY_START, STATE_HORIZON
else:
    WINDOW_END = STATE_HORIZON
    WINDOW_START = WINDOW_END - _DAY
if _start_override:
    WINDOW_START = pd.Timestamp(_start_override)
if _end_override:
    WINDOW_END = pd.Timestamp(_end_override)
WINDOW_START, WINDOW_END = WINDOW_START.normalize(), WINDOW_END.normalize()
assert HISTORY_START <= WINDOW_START < WINDOW_END <= STATE_HORIZON

_ev = read_input("fact_work_order_event").agg(F.max("date_sk").alias("m")).first()["m"]
assert _ev is not None and pd.Timestamp(str(int(_ev))) >= WINDOW_END - 2 * _DAY, (
    f"fact_work_order_event ends at date_sk {_ev}, before this window's end "
    f"{WINDOW_END.date()} -- run 03b first; 03d reads its closed work orders")
if RUN_MODE == "incremental":
    assert table_exists(MAINT_TABLE), f"{MAINT_TABLE} does not exist -- run a backfill first"
    _mh = read_input(MAINT_TABLE).agg(F.max("maintenance_ts").alias("m")).first()["m"]
    assert _mh is None or pd.Timestamp(_mh) < WINDOW_END, (
        f"{MAINT_TABLE} already holds records to {_mh}, past this window. Rerun through the "
        "latest day, or run a backfill.")

print(f"RUN_MODE={RUN_MODE}  window={WINDOW_START.date()}..{WINDOW_END.date()}  "
      f"(state history {HISTORY_START.date()}..{STATE_HORIZON.date()})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load the estate, its operating state and the closed work orders

# CELL ********************

fac_pdf = read_input("dim_facility").filter("is_current = true").toPandas()
eq_pdf = read_input("dim_equipment").toPandas()
sen_pdf = read_input("dim_sensor").filter("is_current = true").toPandas()
for _lbl, _f in (("dim_facility", fac_pdf), ("dim_equipment", eq_pdf), ("dim_sensor", sen_pdf)):
    assert (_f["topology_seed"] == TOPOLOGY_SEED).all(), f"{_lbl} built with a different seed"
ASSETS = asset_views(eq_pdf, fac_pdf)
SENSORS_CMS = set(sen_pdf.loc[sen_pdf["sensor_type"] == "CMS", "equipment_sk"].astype(int))

state_pdf = (read_input("fact_asset_state")
             .select("equipment_sk", "state", "cause", "start_ts", "end_ts").toPandas())
for _c in ("start_ts", "end_ts"):
    state_pdf[_c] = pd.to_datetime(state_pdf[_c])
state_pdf["cause"] = state_pdf["cause"].where(state_pdf["cause"].notna(), None)
SI = state_index(state_pdf)

wo_pdf = (read_input("fact_work_order")
          .select("work_order_id", "equipment_sk", "source", "status", "created_ts", "closed_ts",
                  "downtime_hours", "cost_usd", "assigned_team_sk").toPandas())
for _c in ("created_ts", "closed_ts"):
    wo_pdf[_c] = pd.to_datetime(wo_pdf[_c])
WO_START = wo_pdf["created_ts"].min().normalize() if len(wo_pdf) else WINDOW_END

print(f"{len(ASSETS):,} assets, {len(fac_pdf)} facilities, {len(SENSORS_CMS)} with a CMS detector")
print(f"{len(state_pdf):,} state intervals; "
      f"{int(((state_pdf['state'] == 'Maintenance') & (state_pdf['cause'] == 'Scheduled PM')).sum()):,} "
      f"Scheduled PM and "
      f"{int(((state_pdf['state'] == 'Maintenance') & (state_pdf['cause'] != 'Scheduled PM')).sum()):,} "
      "other Maintenance intervals")
print(f"{len(wo_pdf):,} work orders from {WO_START.date()} ({int((wo_pdf['status'] == 'Closed').sum()):,} "
      "closed); corrective records exist only from there")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Pass 1 and pass 2
#
# Pass 1's carried state is read on timestamps, as of the window start, never from a snapshot
# or a status column. After a rerun those describe a later horizon. The PM state comes from
# `fact_maintenance`. Pending follow-ups come from `fact_inspection`, re-derivable from
# `inspection_sk` alone. Survey plans are regenerated from the calendar and checked against
# the stored `fact_ldar_survey` rows.

# CELL ********************

def _read(table, ts_cols):
    if RUN_MODE != "incremental" or not table_exists(table):
        return pd.DataFrame()
    pdf = read_input(table).toPandas()
    for c in ts_cols:
        pdf[c] = pd.to_datetime(pdf[c])
    return pdf


stored_maint = _read(MAINT_TABLE, ["maintenance_ts"])
stored_insp = _read(INSP_TABLE, ["inspection_ts"])
stored_ldar = _read(LDAR_TABLE, ["survey_ts"])
if RUN_MODE == "incremental":
    prior = prior_state(ASSETS, fac_pdf, SI, stored_maint, stored_insp, WINDOW_START, HISTORY_START)
    if len(stored_ldar):
        _chk = stored_ldar.set_index("survey_sk")
        for _s in prior["surveys"]:
            if _s["survey_sk"] in _chk.index:
                assert int(_chk.at[_s["survey_sk"], "leaks_detected"]) == _s["leaks_detected"], (
                    f"survey {_s['survey_sk']}: stored leaks_detected disagrees with its plan -- a "
                    "survey's plan must never change. Run a backfill.")
else:
    prior = {"last_completed": {k: pm_seed(a, HISTORY_START) for k, a in ASSETS.items()},
             "parents": [], "surveys": []}

m_rows, i_rows, l_rows, last_completed = run_window(
    ASSETS, fac_pdf, SI, state_pdf, wo_pdf, SENSORS_CMS, prior, WINDOW_START, WINDOW_END,
    HISTORY_START)
pm_rows = pm_snapshot(ASSETS, last_completed, WINDOW_END)

maint_new = to_frame(m_rows, MAINT_COLS + ["_kind"],
                     int_cols=("maintenance_sk", "equipment_sk", "facility_sk", "area_sk",
                               "team_sk", "date_sk"), nullable_int=("contractor_sk",))
insp_new = to_frame(i_rows, INSP_COLS + ["_cond"],
                    int_cols=("inspection_sk", "equipment_sk", "facility_sk", "team_sk",
                              "components_checked", "leaks_found", "date_sk"))
ldar_changed = to_frame(l_rows, LDAR_COLS, int_cols=("survey_sk", "facility_sk", "team_sk",
                                                      "components_surveyed", "leaks_detected",
                                                      "leaks_repaired", "leaks_outstanding",
                                                      "date_sk"))
pm_pdf = to_frame(pm_rows, PM_COLS, int_cols=("pm_schedule_sk", "equipment_sk", "facility_sk",
                                               "area_sk", "frequency_days", "assigned_team_sk",
                                               "date_sk"))

# in-run determinism
_again = run_window(ASSETS, fac_pdf, SI, state_pdf, wo_pdf, SENSORS_CMS, prior, WINDOW_START,
                    WINDOW_END, HISTORY_START)
assert [r["maintenance_sk"] for r in _again[0]] == list(maint_new["maintenance_sk"])
assert [r["inspection_sk"] for r in _again[1]] == list(insp_new["inspection_sk"])
assert _again[2] == l_rows and _again[3] == last_completed

WS_SK = int(WINDOW_START.strftime("%Y%m%d"))
WE_SK = int((WINDOW_END - _DAY).strftime("%Y%m%d"))
if RUN_MODE == "incremental":
    LDAR_LO = min([WS_SK] + ldar_changed["date_sk"].tolist())
    _ex = stored_ldar[(stored_ldar["date_sk"] >= LDAR_LO) & (stored_ldar["date_sk"] <= WE_SK)] \
        if len(stored_ldar) else pd.DataFrame(columns=LDAR_COLS)
    ldar_write = merge_for_write(_ex, ldar_changed, "survey_sk", "survey_ts", WINDOW_START, LDAR_COLS)
else:
    LDAR_LO, ldar_write = WS_SK, ldar_changed

print(f"pass 1: last_completed for {len(prior['last_completed']):,} assets, "
      f"{len(prior['parents'])} pending follow-up(s), {len(prior['surveys'])} survey(s) still repairing")
print(f"pass 2: {len(maint_new):,} maintenance record(s), {len(insp_new):,} inspection(s), "
      f"{int((ldar_changed['survey_ts'] >= WINDOW_START).sum()):,} new survey(s); "
      f"{len(ldar_changed):,} survey row(s) changed")
print("OK  rerunning the window from the same prior state reproduces it exactly")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write the four tables
#
# `fact_maintenance` and `fact_inspection` hold events that never change once written, so
# `replaceWhere` covers exactly the window's days. `fact_ldar_survey` rows change as repairs
# complete, so, as in 03b, the range widens back to the oldest changed survey, and untouched
# rows in it are carried. **`fact_pm_schedule` is a snapshot, overwritten in full** each run,
# with `as_of_ts` (and a single `date_sk`) set to the run's horizon. Its history is not lost:
# `fact_maintenance` holds every completion, and `overdue_trajectory()` rebuilds the overdue
# count for any past day from it. A daily-snapshot table would store the same thing again.
# Every table is written from Python rows against an explicit schema, so a missing value is a
# real NULL, never the string "NaN". That is 05's and 06's lesson.

# CELL ********************

from pyspark.sql import types as T

SCHEMAS = {
    MAINT_TABLE: ("maintenance_sk long, equipment_sk long, facility_sk long, area_sk long, "
                  "team_sk long, contractor_sk long, maintenance_ts timestamp, "
                  "maintenance_type string, trigger string, work_order_id string, "
                  "downtime_hours double, labor_hours double, parts_cost_usd double, "
                  "total_cost_usd double, root_cause string, is_completed boolean, "
                  "date_sk long, is_synthetic boolean"),
    INSP_TABLE: ("inspection_sk long, equipment_sk long, facility_sk long, team_sk long, "
                 "inspection_ts timestamp, inspection_type string, method string, "
                 "components_checked long, leaks_found long, result string, "
                 "duration_hours double, date_sk long, is_synthetic boolean"),
    LDAR_TABLE: ("survey_sk long, facility_sk long, team_sk long, survey_ts timestamp, "
                 "survey_method string, regulatory_program string, components_surveyed long, "
                 "leaks_detected long, leaks_repaired long, leaks_outstanding long, "
                 "repair_cost_usd double, duration_hours double, date_sk long, "
                 "is_synthetic boolean"),
    PM_TABLE: ("pm_schedule_sk long, equipment_sk long, facility_sk long, area_sk long, "
               "pm_type string, frequency_days long, last_completed_ts timestamp, "
               "next_due_ts timestamp, is_overdue boolean, days_overdue double, "
               "criticality string, assigned_team_sk long, as_of_ts timestamp, date_sk long, "
               "is_synthetic boolean"),
}


def _py(v):
    if v is None or v is pd.NA or v is pd.NaT or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, np.generic):
        return v.item()
    return v


def write_table(pdf, table, cols, lo_sk=None, hi_sk=None, full=False):
    rows = [tuple(_py(v) for v in r) for r in pdf[cols].itertuples(index=False, name=None)]
    sdf = spark.createDataFrame(rows, SCHEMAS[table])
    w = sdf.write.format("delta").mode("overwrite")
    if full or RUN_MODE == "backfill" or not table_exists(table):
        w.option("overwriteSchema", "true").partitionBy("date_sk").saveAsTable(table)
        print(f"{table}: {len(pdf):,} rows (whole-table overwrite)")
    else:
        (w.option("replaceWhere", f"date_sk >= {lo_sk} AND date_sk <= {hi_sk}")
          .partitionBy("date_sk").saveAsTable(table))
        print(f"{table}: {len(pdf):,} rows (replaceWhere date_sk {lo_sk}..{hi_sk})")


write_table(maint_new, MAINT_TABLE, MAINT_COLS, WS_SK, WE_SK)
write_table(insp_new, INSP_TABLE, INSP_COLS, WS_SK, WE_SK)
write_table(ldar_write, LDAR_TABLE, LDAR_COLS, LDAR_LO, WE_SK)
write_table(pm_pdf, PM_TABLE, PM_COLS, full=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation — every check fails the run, none warns
#
# **The consistency check chosen, in place of "no maintenance on an asset Running
# throughout":**
#
# - Every **preventive** record ends exactly at a Maintenance interval on its asset in
#   `fact_asset_state`, with `downtime_hours` equal to that interval's length.
# - Every **corrective** record's `downtime_hours` equals the asset's actual Down and
#   Maintenance time over its work order's life, recomputed here from `fact_asset_state`.
#
# A corrective record with zero downtime is on-line work, and it is counted. So no record
# claims downtime the asset did not have. A record can still exist with the asset running,
# but only as on-line work, with zero downtime.

# CELL ********************

maint = read_input(MAINT_TABLE).fillna({"contractor_sk": -1}).toPandas()
maint["contractor_sk"] = pd.array([None if v < 0 else int(v) for v in maint["contractor_sk"]],
                                  dtype="Int64")
insp = read_input(INSP_TABLE).toPandas()
ldar = read_input(LDAR_TABLE).toPandas()
pm = read_input(PM_TABLE).toPandas()
for _df, _cs in ((maint, ["maintenance_ts"]), (insp, ["inspection_ts"]), (ldar, ["survey_ts"]),
                 (pm, ["last_completed_ts", "next_due_ts", "as_of_ts"])):
    for _c in _cs:
        _df[_c] = pd.to_datetime(_df[_c])
HORIZON = WINDOW_END

# --- inputs and fake nulls ---------------------------------------------------------------------------
assert not TABLES_READ & set(GROUND_TRUTH_TABLES), "a ground-truth table was read"
assert TABLES_READ <= set(INPUT_TABLES), f"undeclared input(s): {TABLES_READ - set(INPUT_TABLES)}"
for _tbl in (MAINT_TABLE, INSP_TABLE, LDAR_TABLE, PM_TABLE):
    _df = spark.table(_tbl)
    _bad = {}
    for f in _df.schema.fields:
        if isinstance(f.dataType, T.StringType):
            _n = _df.filter(F.lower(F.trim(F.col(f.name))).isin("nan", "none", "null")).count()
        elif isinstance(f.dataType, T.DoubleType):
            _n = _df.filter(F.isnan(F.col(f.name))).count()
        else:
            _n = 0
        if _n:
            _bad[f.name] = _n
    assert not _bad, f"{_tbl}: null-like strings or NaN doubles instead of NULL: {_bad}"

# --- keys and FKs ---------------------------------------------------------------------------------------
for _df, _k in ((maint, "maintenance_sk"), (insp, "inspection_sk"), (ldar, "survey_sk"),
                (pm, "pm_schedule_sk")):
    assert _df[_k].is_unique, f"{_k} not unique"
_eq = eq_pdf.set_index("equipment_sk")
for _df in (maint, insp, pm):
    assert set(_df["equipment_sk"]) <= set(_eq.index), "equipment_sk unresolved"
    assert (_df["facility_sk"].values == _eq.loc[_df["equipment_sk"], "facility_sk"].values).all(), \
        "facility_sk is not the asset's facility"
for _df in (maint, pm):
    assert (_df["area_sk"].values == _eq.loc[_df["equipment_sk"], "area_sk"].values).all()
assert set(ldar["facility_sk"]) <= set(fac_pdf["facility_sk"]), "survey facility unresolved"
for _df, _c in ((maint, "team_sk"), (insp, "team_sk"), (ldar, "team_sk"), (pm, "assigned_team_sk")):
    assert set(_df[_c]) <= set(TEAM_ROSTER), f"{_c} not in the team roster"
assert set(int(v) for v in maint["contractor_sk"].dropna()) <= set(CONTRACTOR_SK), "contractor unresolved"
assert set(maint["maintenance_type"]) <= set(MAINT_TYPES) and set(maint["trigger"]) <= set(TRIGGERS)
assert set(insp["inspection_type"]) <= set(INSP_TYPES) and set(insp["result"]) <= set(INSP_RESULTS)
assert set(insp["method"]) <= set(INSP_METHODS)

# --- corrective: every record traces to a closed work order, and vice versa -----------------------------
_wo = wo_pdf.set_index("work_order_id")
_c = maint[maint["maintenance_type"] == "Corrective"]
assert (_c["trigger"] == "WorkOrder").all() and _c["work_order_id"].notna().all()
assert _c["work_order_id"].is_unique, "one work order produced two corrective records"
_miss = set(_c["work_order_id"]) - set(_wo.index)
assert not _miss, f"corrective record(s) with no work order: {sorted(_miss)[:5]}"
_w = _wo.loc[_c["work_order_id"]]
assert (_w["status"] == "Closed").all(), "a corrective record for a work order that is not Closed"
assert (_w["closed_ts"].values == _c["maintenance_ts"].values).all(), "not at the ticket's close"
assert (_w["equipment_sk"].values == _c["equipment_sk"].values).all(), "not on the ticket's asset"
assert np.allclose(_w["downtime_hours"].values, _c["downtime_hours"].values), "downtime is not the ticket's"
_closed_in = wo_pdf[(wo_pdf["status"] == "Closed") & (wo_pdf["closed_ts"] >= HISTORY_START)
                    & (wo_pdf["closed_ts"] < HORIZON)]
assert set(_closed_in["work_order_id"]) <= set(_c["work_order_id"]), (
    f"{len(set(_closed_in['work_order_id']) - set(_c['work_order_id']))} closed work order(s) "
    "with no corrective record")
_p = maint[maint["maintenance_type"] == "Preventive"]
assert (_p["trigger"] == "Scheduled").all() and _p["work_order_id"].isna().all()

# --- alignment with fact_asset_state -------------------------------------------------------------------
_mi = state_pdf[(state_pdf["state"] == "Maintenance") & state_pdf["end_ts"].notna()]
_mi = _mi.assign(_h=(_mi["end_ts"] - _mi["start_ts"]).dt.total_seconds() / 3600.0)
_j = _p.merge(_mi, left_on=["equipment_sk", "maintenance_ts"], right_on=["equipment_sk", "end_ts"],
              how="left")
assert _j["end_ts"].notna().all(), (f"{int(_j['end_ts'].isna().sum())} preventive record(s) not at "
                                    "the end of a Maintenance interval on their asset")
assert np.allclose(_j["downtime_hours"], _j["_h"]), "preventive downtime != the interval's length"
assert _j.loc[_j["cause"] != "Scheduled PM", "is_completed"].astype(bool).all(), \
    "a catch-up PM (in a non-PM stop) recorded as not completed"
_sched = _mi[(_mi["cause"] == "Scheduled PM") & (_mi["end_ts"] >= HISTORY_START)
             & (_mi["end_ts"] < HORIZON)]
_k = set(zip(_p["equipment_sk"], _p["maintenance_ts"]))
_unrec = [1 for e, t in zip(_sched["equipment_sk"], _sched["end_ts"]) if (e, t) not in _k]
assert not _unrec, f"{len(_unrec)} Scheduled PM interval(s) with no preventive record"


def _down_hours(esk, lo, hi):
    x = SI.get(int(esk))
    if x is None:
        return 0.0
    m = np.isin(x[2], ["Down", "Maintenance"])
    a = np.maximum(x[0][m], lo.value)
    b = np.minimum(x[1][m], hi.value)
    return float(np.clip(b - a, 0, None).sum() / 3600e9)


_rec = [_down_hours(e, cr, cl) for e, cr, cl in
        zip(_w["equipment_sk"], _w["created_ts"], _w["closed_ts"])]
assert np.allclose(_rec, _c["downtime_hours"].values, atol=1e-6), (
    "a corrective record claims downtime fact_asset_state does not show")
N_ONLINE = int((_c["downtime_hours"] == 0).sum())
_corr_iv = _mi[(_mi["cause"] != "Scheduled PM") & (_mi["end_ts"] >= WO_START) & (_mi["end_ts"] < HORIZON)]
_covered = 0
for _r in _corr_iv.itertuples():
    _t = _c[(_c["equipment_sk"] == _r.equipment_sk)]
    if len(_t) and ((_w.loc[_t["work_order_id"], "created_ts"].values <= np.datetime64(_r.end_ts))
                    & (_t["maintenance_ts"].values >= np.datetime64(_r.start_ts))).any():
        _covered += 1
N_CORR_IV, N_CORR_UNCOVERED = len(_corr_iv), len(_corr_iv) - _covered

# --- PM schedule -----------------------------------------------------------------------------------------
_both = pm[pm["last_completed_ts"].notna()]
assert (_both["next_due_ts"] == _both["last_completed_ts"]
        + pd.to_timedelta(_both["frequency_days"], unit="D")).all(), \
    "next_due_ts != last_completed_ts + frequency_days"
_none = pm[pm["last_completed_ts"].isna()]
assert all(ASSETS[int(e)]["pm_anchor"] == d for e, d in zip(_none["equipment_sk"], _none["next_due_ts"])), \
    "an asset with no completion is not due at its first calendar PM"
assert (pm["is_overdue"] == (pm["next_due_ts"] < HORIZON)).all()
assert (pm["as_of_ts"] == HORIZON).all() and (pm["days_overdue"][~pm["is_overdue"]] == 0).all()

# --- inspections ----------------------------------------------------------------------------------------
assert ((insp["leaks_found"] > 0) == (insp["result"] == "Leak Found")).all()
assert (insp["leaks_found"] <= insp["components_checked"]).all()
assert int((insp["inspection_type"] == "FollowUp").sum()) <= int((insp["result"] == "Leak Found").sum()) \
    + len(prior["parents"]), "more follow-ups than Leak Found inspections to follow up"

# --- LDAR --------------------------------------------------------------------------------------------------
assert (ldar["leaks_repaired"] + ldar["leaks_outstanding"] == ldar["leaks_detected"]).all()
assert (ldar["leaks_repaired"] >= 0).all() and (ldar["leaks_outstanding"] >= 0).all()
assert (ldar["leaks_detected"] <= ldar["components_surveyed"]).all()

print("OK  read no ground-truth table; no null-like string or NaN double in any of the four tables")
print("OK  keys unique; asset, facility, area, team and contractor resolve")
print(f"OK  every corrective record traces to a closed work order ({len(_c):,}), and every closed "
      "work order has one")
print(f"OK  every preventive record is a Maintenance interval in fact_asset_state ({len(_p):,}); "
      "every Scheduled PM interval is recorded")
print(f"OK  corrective downtime is the asset's real Down/Maintenance time; {N_ONLINE} on-line "
      "record(s) with zero downtime")
print("OK  next_due_ts = last_completed_ts + frequency_days wherever both exist")
print("OK  inspection results consistent; LDAR repaired + outstanding = detected")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Backlogs — the trajectory, not a day
#
# Two backlogs. **Overdue PMs**: the pre-history seed starts the pool stationary, so the
# trend is judged over the last `TREND_WINDOW_DAYS`. **Outstanding LDAR leaks**: they start
# from zero at the first survey, so both checks wait out `WARMUP_DAYS`. Cumulative repaired
# must reach `LDAR_REPAIR_BAND[0]` of cumulative detected `LDAR_REPAIR_LAG_DAYS` earlier,
# the design note's "≥70% with lag".

# CELL ********************

DAYS = pd.date_range(HISTORY_START, HORIZON - _DAY, freq="D")
overdue_traj = overdue_trajectory(ASSETS, maint, DAYS, HISTORY_START)
_by_fac = {}
for _a in ASSETS.values():
    _by_fac.setdefault(_a["facility_sk"], []).append(_a)
ALL_SURVEYS = ldar_surveys_in(SI, _by_fac, fac_pdf, HISTORY_START, HORIZON, HISTORY_START)
cum_det, cum_rep, outstanding = ldar_cumulative(ALL_SURVEYS, DAYS)
assert sorted(s["survey_sk"] for s in ALL_SURVEYS) == sorted(ldar["survey_sk"]), \
    "fact_ldar_survey does not hold exactly the calendar's surveys"

_enough = len(DAYS) >= WARMUP_DAYS + TREND_WINDOW_DAYS
_x = np.arange(TREND_WINDOW_DAYS, dtype=float)


def _rise(v):
    return float(np.polyfit(_x, v[-TREND_WINDOW_DAYS:].astype(float), 1)[0]) * (TREND_WINDOW_DAYS - 1)


_od = pm[pm["is_overdue"]]
_sched_w = _p[_p.merge(_mi, left_on=["equipment_sk", "maintenance_ts"],
                       right_on=["equipment_sk", "end_ts"], how="left")["cause"].values == "Scheduled PM"]
PM_COMPLETION = float(_sched_w["is_completed"].mean()) if len(_sched_w) else float("nan")
print(f"PM visits (02a Scheduled PM intervals): {len(_sched_w):,}; completed {PM_COMPLETION:.1%} "
      f"(target {PM_ON_TIME_COMPLETION:.0%}); catch-ups in other stops "
      f"{int((_p['maintenance_ts'].isin(_mi.loc[_mi['cause'] != 'Scheduled PM', 'end_ts'])).sum())}")
print(f"overdue PMs now: {len(_od):,} of {len(pm):,} ({len(_od) / max(len(pm), 1):.1%}); days overdue "
      + ("  ".join(f"p{q} {np.percentile(_od['days_overdue'], q):.0f}" for q in (25, 50, 75, 90))
         + f"  max {_od['days_overdue'].max():.0f}" if len(_od) else "-"))
print("overdue PMs by week: " + "  ".join(str(int(overdue_traj[i:i + 7].mean()))
                                          for i in range(0, len(overdue_traj), 7)))
print(f"LDAR: {len(ALL_SURVEYS)} surveys, cumulative detected {cum_det[-1]:,}, repaired "
      f"{cum_rep[-1]:,}, outstanding {outstanding[-1]:,}")
print("LDAR outstanding by week: " + "  ".join(str(int(outstanding[i:i + 7].mean()))
                                               for i in range(0, len(outstanding), 7)))
if _enough:
    _r = _rise(overdue_traj)
    assert _r <= TREND_MAX_RISE * max(overdue_traj[-TREND_WINDOW_DAYS:].mean(), 1.0), (
        f"overdue PMs rose {_r:+.1f} over the last {TREND_WINDOW_DAYS} days -- the overdue "
        "population is growing, which is the V1 defect")
    _lag = LDAR_REPAIR_LAG_DAYS
    _ok = [cum_rep[i] >= LDAR_REPAIR_BAND[0] * cum_det[i - _lag] for i in range(WARMUP_DAYS, len(DAYS))]
    assert all(_ok) and (cum_rep <= cum_det).all(), (
        f"cumulative repaired fell below {LDAR_REPAIR_BAND[0]:.0%} of cumulative detected "
        f"{_lag} days earlier on {len(_ok) - sum(_ok)} day(s)")
    _ro = _rise(outstanding)
    assert _ro <= TREND_MAX_RISE * max(outstanding[-TREND_WINDOW_DAYS:].mean(), 1.0), (
        f"LDAR outstanding rose {_ro:+.1f} over the last {TREND_WINDOW_DAYS} days")
    print(f"OK  overdue PMs not trending ({_r:+.1f}); LDAR repaired >= {LDAR_REPAIR_BAND[0]:.0%} of "
          f"detected {_lag} d earlier on every day; outstanding not trending ({_ro:+.1f})")
else:
    print(f"NOTE  {len(DAYS)} days of history; the backlog checks need "
          f"{WARMUP_DAYS + TREND_WINDOW_DAYS}. NOT asserted this run.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Distributions

# CELL ********************

print("maintenance by type and trigger")
print(maint.pivot_table(index="maintenance_type", columns="trigger", values="maintenance_sk",
                        aggfunc="count", fill_value=0).to_string())
print(f"  preventive not completed: {int((~_p['is_completed'].astype(bool)).sum()):,}; "
      f"corrective on-line (zero downtime): {N_ONLINE:,}")
print(f"  02a Corrective/other Maintenance intervals since {WO_START.date()}: {N_CORR_IV:,}, of which "
      f"{N_CORR_UNCOVERED:,} have no corrective record -- a stop no closed work order covers")
print(f"  total cost ${maint['total_cost_usd'].sum():,.0f}")

if len(insp_new):
    _d = insp_new.assign(decile=pd.qcut(insp_new["_cond"].rank(method="first"), 10, labels=False) + 1)
    print("inspection Leak Found rate by condition decile (this run's inspections)")
    for _q, _g in _d.groupby("decile"):
        print(f"  decile {_q:>2}  condition {_g['_cond'].min():.2f}-{_g['_cond'].max():.2f}  "
              f"{(_g['result'] == 'Leak Found').mean():>6.1%}  of {len(_g)}")
print("inspections: " + "   ".join(f"{k} {v:,}" for k, v in insp["result"].value_counts().items())
      + "   by type: " + ", ".join(f"{k} {v}" for k, v in insp["inspection_type"].value_counts().items()))
_lf_days = max((HORIZON - HISTORY_START) / _DAY, 1)
print(f"Leak Found inspections: {int((insp['result'] == 'Leak Found').sum()):,} "
      f"({(insp['result'] == 'Leak Found').sum() / _lf_days:.1f}/day) -- what an Inspection source in "
      "03b would receive before dedup; NOT wired (see the summary)")

print("LDAR cumulative (every 15 days): " + "  ".join(
    f"{DAYS[i].date()} {cum_det[i]}/{cum_rep[i]}" for i in range(0, len(DAYS), 15)) + "  (detected/repaired)")

for _lbl, _df in (("maintenance records", maint), ("inspections", insp), ("LDAR surveys", ldar),
                  ("overdue PMs", pm[pm["is_overdue"]])):
    _v = _df.groupby("facility_sk").size().reindex(fac_pdf["facility_sk"], fill_value=0)
    print(f"{_lbl:<20} per facility: median {_v.median():.0f}  p90 {_v.quantile(0.9):.0f}  "
          f"max {_v.max()}  ({int((_v == 0).sum())} with none)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary

# CELL ********************

print("=" * 76)
print("MAINTENANCE, INSPECTIONS AND LDAR GENERATED")
print("=" * 76)
print(f"  run mode          {RUN_MODE}   window {WINDOW_START.date()} .. {WINDOW_END.date()}")
print(f"  PM schedule       {len(pm):,} assets, {len(_od):,} overdue as of {HORIZON.date()}")
print(f"  maintenance       {len(maint):,} records ({len(_p):,} preventive, {len(_c):,} corrective)")
print(f"  inspections       {len(insp):,}; Leak Found {int((insp['result'] == 'Leak Found').sum()):,}")
print(f"  LDAR              {len(ldar):,} surveys; outstanding leaks {outstanding[-1]:,}")
print(f"  tables written    {PM_TABLE}, {MAINT_TABLE}, {INSP_TABLE}, {LDAR_TABLE}")
print(f"  read              {', '.join(sorted(TABLES_READ))}")
print(f"  not read          {', '.join(GROUND_TRUTH_TABLES)} -- condition decides what is found")
print("  not modified      every dim_* table, fact_asset_state, every 03b/03c table")
print("  NOT wired         inspection Leak Found -> 03b: 03d reads 03b's closed work orders, so "
      "03b reading 03d's inspections would make the pipeline circular")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
