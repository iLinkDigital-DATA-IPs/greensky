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

# # 03b — Generate Work Orders
#
# Writes **`fact_work_order`** (one row per ticket) and **`fact_work_order_event`** (one row
# per status transition). The first backlog in the estate, and the first notebook to
# implement `DESIGN_NOTE_incremental_facts.md` §2.3 as its main job rather than as a detail.
#
# ### Tickets come from symptoms, never from the leak
# An operator cannot see a leak. They see what it does to the instruments, so every ticket
# here comes from something observable:
#
# | source | raises a ticket when | priority |
# |---|---|---|
# | `Alarm` | P1 alarms, directly. P2 when it **recurs** (3 raises in 24 h on the tag) or **stands** 4 h, since a single warning does not dispatch a crew (`ALARM_P2_MODE`). P3 only when it recurs on the same tag (see the rule below). P4 never | the alarm's own |
# | `Exceedance` | a CH₄ detector's `exceedance_flag` holds for `EXCEEDANCE_MIN_READINGS` consecutive readings. A single flagged reading does not | P2 |
# | `Compliance` | a compliance case reaching **Violation** in `fact_compliance_event` (03c), at the instant of the finding. Reports and undecided cases raise nothing | P1 Critical, P2 Major |
# | `SensorStatus` | a SCADA tag stays offline for `OFFLINE_TICKET_HOURS` with no Maintenance on its asset | P3 |
#
# **`fact_emission_episode` is never read.** It is the hidden ground truth. A ticket sourced
# from an episode would make every downstream claim that "the work order responded to the
# leak" circular. The generator would only be finding what it copied in. Every read goes
# through `read_input()`, which refuses the table, and the validation fails the run if it was
# read. `tools/harness/harness_work_orders.py` also checks statically that the name appears
# nowhere in the code outside the refusal list.
#
# **So some real leaks get no ticket at all.** That is correct, not a defect. A leak on a
# valve, a pipeline segment or any other uninstrumented asset raises no alarm, no detector
# covers it, and no operator knows it is there. That coverage gap is what the platform exists
# to close. The satellite layer sees what the SCADA layer cannot, so any "the ticket
# responded to the leak" story has to be told only where the symptoms were observable.
#
# ### Two passes, and closure fixed at creation
# Pass 1 advances the tickets that are not yet closed. Pass 2 creates the window's new ones.
# A ticket's resolution time, response time, stall flag and cost are drawn **once**, from
# `get_rng("work_order", work_order_id)`, and never again. Whether a ticket closes on a given
# day is therefore a function of elapsed time only, and a backfill equals the sequence of
# incremental days. This notebook exists to avoid V1's defect, which decided open versus
# closed at creation and never revisited it.
#
# ### Writes
# `fact_work_order` and `fact_work_order_event`, both partitioned by `date_sk` and written with
# `replaceWhere`. No `dim_*` table, no alarm or telemetry table, and no state table is modified.

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
#
# Every table read goes through `read_input()`. It refuses `fact_emission_episode` outright
# and anything not in `INPUT_TABLES`. The validation cell fails the run if anything outside
# the declared inputs was read. `fact_compliance_event` is written by 03c, which must run
# before 03b in the daily pipeline so its statuses are as of the same horizon.

# CELL ********************

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

WO_TABLE = "fact_work_order"
EVENT_TABLE = "fact_work_order_event"
COMPLIANCE_TABLE = "fact_compliance_event"

INPUT_TABLES = ("dim_facility", "dim_area", "dim_equipment", "dim_scada_tag", "dim_sensor",
                "fact_asset_state", "fact_scada_alarm", "fact_sensor_status_event",
                "sensor_telemetry", "scada_telemetry", COMPLIANCE_TABLE,
                WO_TABLE, EVENT_TABLE)

# The hidden ground truth. An operator sees a leak's symptoms (alarms, detector readings,
# a plume in a compliance case) and never the leak itself. A ticket sourced from here would
# make every later "the work order responded to the leak" claim circular.
GROUND_TRUTH_TABLES = ("fact_emission_episode",)
assert not set(INPUT_TABLES) & set(GROUND_TRUTH_TABLES), "an input is a ground-truth table"

TABLES_READ = set()


def read_input(name):
    """The only way this notebook reads a table."""
    assert name not in GROUND_TRUTH_TABLES, (
        f"{name} is hidden ground truth. Work orders come from observable symptoms only; "
        "reading it would make every 'the ticket responded to the leak' claim circular."
    )
    assert name in INPUT_TABLES, f"{name} is not a declared input of 03b: {INPUT_TABLES}"
    TABLES_READ.add(name)
    return spark.table(name)


def table_exists(name):
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False


def read_work_orders():
    """fact_work_order as pandas, with tag_sk exact.

    tag_sk is a nullable 63-bit key: null on Exceedance tickets, which come from a CH4
    detector rather than a SCADA tag. Spark's toPandas turns a nullable long column into
    float64, and every key above 2**53 comes back as a different number. That would fail the
    FK check, and pass 1 would write the wrong tag_sk onto every carried row. So nulls are
    filled with -1 in Spark, where the column is still exact, and restored after.
    """
    pdf = read_input(WO_TABLE).fillna({"tag_sk": -1}).toPandas()
    assert str(pdf["tag_sk"].dtype) == "int64", f"tag_sk arrived as {pdf['tag_sk'].dtype}"
    pdf["tag_sk"] = pd.array([None if v < 0 else int(v) for v in pdf["tag_sk"]], dtype="Int64")
    for c in ("created_ts", "closed_ts"):
        pdf[c] = pd.to_datetime(pdf[c])
    return pdf


print(f"inputs            {', '.join(INPUT_TABLES)}")
print(f"refused           {', '.join(GROUND_TRUTH_TABLES)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### The work-order model
#
# This cell is pure: no Spark, no table, no clock. `tools/harness/harness_work_orders.py`
# executes it verbatim against `01_topology_config`, so the harness checks this code, not a
# copy of it.
#
# **Sources.** `build_sources()` turns the upstream tables into candidate tickets, each with
# the instant an operator would raise it (`trigger_ts`). Every trigger is knowable at that
# instant from data that already exists by then. An alarm raises on its Nth breaching
# reading. An exceedance run triggers on its Nth consecutive flagged reading. A tag has been
# offline for `OFFLINE_TICKET_HOURS` when that many hours pass with no reading. That is what
# lets an incremental run and a backfill see the same sources for the same day.
#
# **The P3 recurrence rule.** A P3 alarm raises a ticket when it is the
# `P3_RECURRENCE_COUNT`-th P3 raise on the **same tag** within the trailing
# `P3_RECURRENCE_HOURS`, counting itself. So the rule is three within 24 h. A single repeat of a
# warning inside a day happens by chance on a tag that sits near its limit. Three is a
# pattern worth a technician. On the offline source stream three-in-24 h passes 18 P3 alarms
# a month and two-in-24 h passes 247, so the count is the sensitive knob.
#
# **Dedup.** A candidate does not open a second ticket on an asset that already has one open
# at the same or higher urgency. It is absorbed, and the run counts it. A P1 trip arriving
# while a P3 ticket is open on the asset still gets its own P1 ticket. `DEDUP_COOLDOWN_HOURS`
# optionally extends "open" past closure, so a repeat alarm soon after a fix is treated as the
# same unresolved problem. It defaults to 0; the calibration below says what each setting
# buys.
#
# **The plan.** `ticket_plan()` draws, in a fixed order from
# `get_rng("work_order", work_order_id)`: the stall flag, the resolution duration (log-normal
# per priority, solved so its mean and SLA-breach share hit the design-note targets), the
# response delay as a share of it, and the cost. It is drawn once and never re-drawn. Pass 1
# re-derives it from the stored `work_order_id` and asserts it matches the stored row.
#
# **Status as of an instant.** A ticket is `Open` from `created_ts`, `In Progress` from
# `created_ts + respond`, and `Closed` at `created_ts + resolution`. A stalled ticket goes In
# Progress and never closes. Pass 1 instead **cancels** it at `created_ts +
# WO_CANCEL_AFTER_DAYS` (60), with a note, so the stalled population is bounded rather than
# growing for ever. Every ticket ends exactly once, Closed or Cancelled. `row_as_of()` is the only place status is decided, and it depends
# on nothing but those fixed instants and the horizon.

# CELL ********************

# ---- 03b work-order model (pure: tools/harness/harness_work_orders.py executes this cell) ----
from collections import defaultdict
from statistics import NormalDist

WO_SOURCES = ("Alarm", "Exceedance", "Compliance", "SensorStatus")
SOURCE_ORDER = {s: i for i, s in enumerate(WO_SOURCES)}      # tie-break at one instant
WO_PRIORITIES = ("P1", "P2", "P3", "P4")
PRIORITY_RANK = {p: i for i, p in enumerate(WO_PRIORITIES)}  # lower = more urgent
WO_STATUSES = ("Open", "In Progress", "Closed", "Cancelled")
WO_OPEN_STATUSES = ("Open", "In Progress")

# ---- sources ------------------------------------------------------------------------------
ALARM_DIRECT_PRIORITIES = ("P1", "P2")   # raise a ticket on every alarm
P3_RECURRENCE_COUNT = 3                  # ...a P3 raises on its 3rd raise on one tag
P3_RECURRENCE_HOURS = 24.0               # ...within this trailing window
# P2 handling. "gated": a P2 alarm is a candidate only when it is the P2_RECURRENCE_COUNT-th
# P2 raise on its tag within P2_RECURRENCE_HOURS, or when it is still standing
# P2_STANDING_HOURS after it raised. A single P2 warning does not dispatch a crew -- a
# repeating or standing condition is when an operator actually acts. "direct" makes every P2
# alarm a candidate, and is kept switchable. Measured on the offline stream (30 days, one open
# ticket per asset, no cooldown; open = non-stalled mean over the last 14 days):
#   "direct"  53 tickets/day, 125 open (lambda x T predicted 124), 0.83 open per facility
#   "gated"   18 tickets/day,  60 open (lambda x T predicted  58), 0.40 open per facility
# Only "gated" lands in the 10-20 a day / 40-100 open target; see the calibration note.
ALARM_P2_MODE = "gated"
P2_RECURRENCE_COUNT = 3
P2_RECURRENCE_HOURS = 24.0
P2_STANDING_HOURS = 4.0

EXCEEDANCE_MIN_READINGS = 6              # consecutive flagged hourly readings: a sustained run
EXCEEDANCE_PRIORITY = "P2"
OFFLINE_TICKET_HOURS = 24.0              # tag with no reading this long raises a ticket
SENSOR_STATUS_PRIORITY = "P3"
# 02d's SENSOR_OFFLINE_MULTIPLE: a gap longer than this many cadences is an outage.
# harness_work_orders.py asserts it equals 02d's value.
SENSOR_OFFLINE_MULTIPLE = 2.0
# How far back the ongoing-outage scan looks for a tag's last reading. An outage is capped at
# 02b's TELEMETRY_OUTAGE_MAX_H (120 h, the same as CH4_OUTAGE_MAX_H), so a tag silent for
# longer than this is not in an outage -- it is decommissioned or not yet installed.
OFFLINE_SCAN_DAYS = 7
assert OFFLINE_SCAN_DAYS * 24.0 > CH4_OUTAGE_MAX_H + OFFLINE_TICKET_HOURS

# ---- dedup ------------------------------------------------------------------------------------
DEDUP_COOLDOWN_HOURS = 0.0

# ---- stalled-ticket exit ----------------------------------------------------------------------
# A ticket still open this many days after it was raised is Cancelled by pass 1, with a note.
# Stalled work exists, but a stall that never exits makes the stalled population grow without
# bound, and any open-work-order KPI then climbs for ever -- the failure the design note was
# written to prevent. With the exit, stalled-and-open converges to
# (stall arrivals per day) x WO_CANCEL_AFTER_DAYS. The instant is created_ts + this, fixed at
# creation like everything else, so cancellation is as deterministic as closure.
# Kept at 60 deliberately. A 30-day exit would halve the stalled backlog and flatter the
# number, but 30 days is too soon to write a ticket off as abandoned.
WO_CANCEL_AFTER_DAYS = 60

# ---- SLA and resolution: DESIGN_NOTE_incremental_facts.md section 3 --------------------------
WO_SLA_HOURS = {"P1": 24.0, "P2": 72.0, "P3": 168.0, "P4": 336.0}
WO_MEAN_RESOLUTION_HOURS = {"P1": 18.0, "P2": 55.0, "P3": 140.0, "P4": 300.0}
WO_BREACH_TARGET = {"P1": 0.20, "P2": 0.20, "P3": 0.25, "P4": 0.30}
# Asserted on tickets created in the evaluation period, where a priority has enough tickets
# to measure. If nothing breaches, is_breached is decoration; if everything does, it is noise.
WO_BREACH_BAND = {"P1": (0.10, 0.32), "P2": (0.10, 0.32), "P3": (0.13, 0.38),
                  "P4": (0.15, 0.45)}
WO_BREACH_MIN_TICKETS = 40
WO_STALL_SHARE = 0.05                    # permanently unresolved, fixed at creation
WO_RESPOND_FRACTION = (0.05, 0.25)       # Open -> In Progress, as a share of the resolution
WO_MIN_RESOLUTION_S = 900


def _lognormal_for(sla_h, mean_h, breach):
    """(mu, sigma) of a log-normal with the given mean and P(X > sla) = breach.

    exp(mu + s^2/2) = mean and (ln sla - mu) / s = z give s^2/2 - z s + ln(sla/mean) = 0.
    Of the two roots the smaller is taken: it keeps the tail realistic (P1's p99 is about
    49 h against 144 h for the other root), and both hit the mean and the breach share.
    """
    z = NormalDist().inv_cdf(1.0 - breach)
    disc = z * z - 2.0 * np.log(sla_h / mean_h)
    assert disc >= 0, (f"no log-normal has mean {mean_h} h and {breach:.0%} above {sla_h} h; "
                       "the target is inconsistent")
    s = z - np.sqrt(disc)
    return float(np.log(mean_h) - 0.5 * s * s), float(s)


WO_RESOLUTION_LOGNORMAL = {p: _lognormal_for(WO_SLA_HOURS[p], WO_MEAN_RESOLUTION_HOURS[p],
                                             WO_BREACH_TARGET[p]) for p in WO_PRIORITIES}

# ---- steady state: design note sections 3 and 7 ----------------------------------------------
STEADY_BAND = (0.4, 2.5)                 # realised open / (arrival rate x mean resolution)
TREND_WINDOW_DAYS = 14                   # the trajectory is judged over the last 14 days
TREND_MAX_RISE = 0.25                    # fitted rise over that window, as a share of its mean
# Days of history before the trend window starts. The open population converges once the
# longest-lived ticket at the busiest priority has had time to close. P3's p99 is ~13 days.
WARMUP_DAYS = 14
# The operational target, reported against and never asserted. See the calibration note.
TARGET_ARRIVALS_PER_DAY = (10.0, 20.0)
TARGET_OPEN = (40.0, 100.0)

# ---- teams: no dim_team exists yet -------------------------------------------------------------
# assigned_team_sk is stable_key("team", sub_basin, discipline) over this roster. FKs are
# checked against the roster, since there is no team dimension to check them against.
TEAM_DISCIPLINES = ("Mechanical", "Operations", "Instrumentation & Controls",
                    "Environmental & LDAR")
ALARM_DISCIPLINE = {"Compressor": "Mechanical", "Pump": "Mechanical",
                    "Separator": "Operations", "Storage Tank": "Operations",
                    "Flare": "Operations", "Metering Station": "Instrumentation & Controls",
                    "Valve": "Operations", "Pipeline Segment": "Operations"}
SOURCE_DISCIPLINE = {"Exceedance": "Environmental & LDAR",
                     "Compliance": "Environmental & LDAR",
                     "SensorStatus": "Instrumentation & Controls"}
TEAM_ROSTER = {stable_key("team", b, d): f"{b} {d}" for b in ANCHORS for d in TEAM_DISCIPLINES}

# ---- cost, revealed at closure ------------------------------------------------------------------
CREW_RATE_USD_H = {"Mechanical": 145.0, "Operations": 95.0,
                   "Instrumentation & Controls": 130.0, "Environmental & LDAR": 120.0}
# Most tickets are a warning alarm investigated and cleared, not a rebuild, so the typical
# ticket is a few crew-hours and a small part; the log-normal tail carries the real repairs.
# Not calibrated against any cost data -- a plausible order of magnitude (~$2.5k mean).
WO_HANDS_ON_SHARE = (0.05, 0.20)         # share of the elapsed resolution a crew is on the job
PARTS_MEDIAN_USD = {"Compressor": 1200.0, "Pump": 600.0, "Separator": 450.0,
                    "Storage Tank": 400.0, "Flare": 500.0, "Metering Station": 300.0,
                    "Valve": 200.0, "Pipeline Segment": 800.0}
SOURCE_PARTS_FACTOR = {"Alarm": 1.0, "Exceedance": 0.8, "Compliance": 1.0,
                       "SensorStatus": 0.25}  # a transmitter or radio, not the machine
PARTS_SIGMA = 1.1

# ---- configuration checks ----------------------------------------------------------------------
assert set(WO_SLA_HOURS) == set(WO_MEAN_RESOLUTION_HOURS) == set(WO_BREACH_TARGET) \
    == set(WO_BREACH_BAND) == set(WO_PRIORITIES)
for _p in WO_PRIORITIES:
    assert WO_MEAN_RESOLUTION_HOURS[_p] < WO_SLA_HOURS[_p]
    assert WO_BREACH_BAND[_p][0] < WO_BREACH_TARGET[_p] < WO_BREACH_BAND[_p][1]
assert set(ALARM_DIRECT_PRIORITIES) <= {"P1", "P2"}, "P3 and P4 never raise directly"
assert ALARM_P2_MODE in ("direct", "gated")
assert set(ALARM_DISCIPLINE) == set(EQUIPMENT_TYPES) == set(PARTS_MEDIAN_USD)
assert set(SOURCE_DISCIPLINE) | {"Alarm"} == set(WO_SOURCES) == set(SOURCE_PARTS_FACTOR)
assert set(CREW_RATE_USD_H) == set(TEAM_DISCIPLINES)
# The exit must sit well beyond any real resolution, so it only ever catches stalled work:
# 1.5x the slowest priority's p99.9 (P4, ~733 h, so ~1,100 h against 1,440 h) leaves a
# vanishing share of non-stalled tails to it.
assert WO_CANCEL_AFTER_DAYS * 24.0 > 1.5 * max(
    np.exp(_mu + 3.09 * _sg) for _mu, _sg in WO_RESOLUTION_LOGNORMAL.values())
assert 0.0 < WO_STALL_SHARE < 0.2 and 0.0 < WO_RESPOND_FRACTION[0] < WO_RESPOND_FRACTION[1] < 1.0
assert len(TEAM_ROSTER) == len(ANCHORS) * len(TEAM_DISCIPLINES), "team_sk collision"

_H = pd.Timedelta(hours=1)
_S = pd.Timedelta(seconds=1)
_DAY = pd.Timedelta(days=1)
SOURCE_COLS = ["trigger_ts", "source", "source_ref", "priority", "equipment_sk", "tag_sk",
               "detail"]


def _candidates(trigger_ts, source, source_ref, priority, equipment_sk, tag_sk, detail):
    """A typed candidate frame. Every part goes through here: surrogate keys are 63-bit, and
    one empty part in a concat silently upcasts source_ref to float64, losing precision and
    changing the key. That happened in the harness on a day with no exceedance run."""
    n = len(source_ref)
    return pd.DataFrame({
        "trigger_ts": pd.to_datetime(pd.Series(list(trigger_ts), dtype="datetime64[ns]")),
        "source": pd.Series([source] * n, dtype=object),
        "source_ref": pd.Series(np.asarray(list(source_ref), dtype="int64")),
        "priority": pd.Series(list(priority) if not isinstance(priority, str)
                              else [priority] * n, dtype=object),
        "equipment_sk": pd.Series(np.asarray(list(equipment_sk), dtype="int64")),
        "tag_sk": pd.array(list(tag_sk), dtype="Int64"),
        "detail": pd.Series(list(detail), dtype=object),
    })


def discipline_for(source, equipment_type):
    return ALARM_DISCIPLINE[equipment_type] if source == "Alarm" else SOURCE_DISCIPLINE[source]


def _recurring(al, count, hours):
    """Rows of al (one priority) that are at least the count-th raise on their tag within
    the trailing window, counting themselves."""
    if al.empty:
        return al.iloc[0:0]
    a = al.sort_values(["tag_sk", "raised_ts", "alarm_sk"], kind="mergesort")
    keep = np.zeros(len(a), dtype=bool)
    ts = a["raised_ts"].values
    tag = a["tag_sk"].values
    w = np.timedelta64(int(hours * 3600), "s")
    for i in range(count - 1, len(a)):
        j = i - count + 1
        keep[i] = tag[j] == tag[i] and ts[i] - ts[j] <= w
    return a[keep]


def alarm_sources(alarms):
    """Alarm candidates. alarms: fact_scada_alarm rows (alarm_sk, tag_sk, tag_id,
    equipment_sk, alarm_type, priority, raised_ts, cleared_ts)."""
    a = alarms.copy()
    a["trigger_ts"] = a["raised_ts"]
    parts = [a[a["priority"] == "P1"]]
    p2 = a[a["priority"] == "P2"]
    if ALARM_P2_MODE == "direct":
        parts.append(p2)
    else:
        rec = _recurring(p2, P2_RECURRENCE_COUNT, P2_RECURRENCE_HOURS)
        stand_at = p2["raised_ts"] + pd.Timedelta(hours=P2_STANDING_HOURS)
        standing = p2[p2["cleared_ts"].isna() | (p2["cleared_ts"] > stand_at)].copy()
        standing["trigger_ts"] = standing["raised_ts"] + pd.Timedelta(hours=P2_STANDING_HOURS)
        both = pd.concat([rec, standing]).sort_values("trigger_ts", kind="mergesort")
        parts.append(both.drop_duplicates("alarm_sk", keep="first"))
    parts.append(_recurring(a[a["priority"] == "P3"], P3_RECURRENCE_COUNT, P3_RECURRENCE_HOURS))
    s = pd.concat([p for p in parts if len(p)] or [a.iloc[0:0]], ignore_index=True)
    return _candidates(s["trigger_ts"], "Alarm", s["alarm_sk"], s["priority"],
                       s["equipment_sk"], s["tag_sk"],
                       [f"{t} alarm on {g}" for t, g in zip(s["alarm_type"], s["tag_id"])])


def exceedance_runs(flags, cadence_s):
    """Runs of consecutive flagged readings per sensor, as 02e's validation cell defines them:
    a new run starts wherever the previous flagged reading is not exactly one cadence earlier.
    flags: sensor_telemetry rows with exceedance_flag = true (sensor_id, reading_ts)."""
    if flags.empty:
        return pd.DataFrame(columns=["sensor_id", "run_start", "run_len", "nth_ts", "run_key"])
    f = flags.sort_values(["sensor_id", "reading_ts"], kind="mergesort").reset_index(drop=True)
    prev_ts = f.groupby("sensor_id")["reading_ts"].shift(1)
    new = prev_ts.isna() | ((f["reading_ts"] - prev_ts) != pd.Timedelta(seconds=cadence_s))
    f["run"] = new.cumsum()
    f["pos"] = f.groupby("run").cumcount() + 1
    g = f.groupby("run")
    runs = pd.DataFrame({"sensor_id": g["sensor_id"].first(), "run_start": g["reading_ts"].first(),
                         "run_len": g.size()})
    nth = f[f["pos"] == EXCEEDANCE_MIN_READINGS].set_index("run")["reading_ts"]
    runs["nth_ts"] = nth.reindex(runs.index)
    runs["run_key"] = [stable_key("ch4_exceedance_run", s, t.isoformat())
                       for s, t in zip(runs["sensor_id"], runs["run_start"])]
    return runs.reset_index(drop=True)


def exceedance_sources(flags, sensors, cadence_s):
    r = exceedance_runs(flags, cadence_s)
    r = r[r["run_len"] >= EXCEEDANCE_MIN_READINGS]
    eq_of = sensors.set_index("sensor_id")["equipment_sk"]
    return _candidates(r["nth_ts"], "Exceedance", r["run_key"], EXCEEDANCE_PRIORITY,
                       r["sensor_id"].map(eq_of), [pd.NA] * len(r),
                       [f"CH4 above threshold {EXCEEDANCE_MIN_READINGS} h on {s}"
                        for s in r["sensor_id"]])


def offline_sources(leaves, ongoing, maint, horizon):
    """SensorStatus candidates.

    leaves: fact_sensor_status_event rows leaving Online (tag_sk, tag_id, equipment_sk,
    event_ts, duration_hours) -- outages that have ENDED, because 02d derives a gap from the
    reading that closes it. ongoing: outages still running at the horizon, from each tag's
    last reading in scada_telemetry (tag_sk, tag_id, equipment_sk, off_ts). Together they are
    every outage an operator can see by the horizon, so an outage 24 h old raises its ticket
    at 24 h whether or not the tag has since come back.

    maint: Maintenance intervals (equipment_sk, start_ts, end_ts; null end = open). An outage
    with Maintenance on its asset at any point before the trigger is planned work, not a fault.
    """
    thr = pd.Timedelta(hours=OFFLINE_TICKET_HOURS)
    done = leaves[leaves["duration_hours"] >= OFFLINE_TICKET_HOURS][
        ["tag_sk", "tag_id", "equipment_sk", "event_ts"]].rename(columns={"event_ts": "off_ts"})
    still = ongoing[ongoing["off_ts"] + thr <= horizon][["tag_sk", "tag_id", "equipment_sk",
                                                         "off_ts"]]
    o = pd.concat([done, still], ignore_index=True)
    o["trigger_ts"] = o["off_ts"] + thr
    if len(o) and len(maint):
        m = maint.copy()
        m["end_ts"] = m["end_ts"].fillna(pd.Timestamp.max)
        j = o.reset_index().merge(m, on="equipment_sk", how="inner")
        hit = j[(j["start_ts"] < j["trigger_ts"]) & (j["end_ts"] > j["off_ts"])]["index"]
        o = o.drop(index=hit.unique())
    return _candidates(o["trigger_ts"], "SensorStatus",
                       [stable_key("tag_offline", t, pd.Timestamp(s).isoformat())
                        for t, s in zip(o["tag_id"], o["off_ts"])],
                       SENSOR_STATUS_PRIORITY, o["equipment_sk"], o["tag_sk"],
                       [f"tag {t} offline {OFFLINE_TICKET_HOURS:.0f} h" for t in o["tag_id"]])


# Compliance: a VIOLATION raises a ticket; a report, an operator event or an undecided case
# does not. The ticket is the regulatory response -- corrective action and the filing -- at
# the finding. The field response to the release itself is already raised by the Alarm and
# Exceedance sources. Severity maps to priority; Minor never reaches Violation in 03c, and
# the mapping says so rather than guessing a priority for it.
COMPLIANCE_PRIORITY = {"Critical": "P1", "Major": "P2"}


def compliance_sources(ce, responsible_asset, horizon):
    """Candidates from fact_compliance_event rows (compliance_sk, compliance_id, facility_sk,
    equipment_sk, event_type, severity, status, status_ts).

    A plume-derived case carries no equipment_sk: a satellite plume cannot resolve an asset.
    The ticket still needs one, so it goes to responsible_asset[facility_sk], a MODELLING
    CHOICE made in the notebook and labelled there, never to an asset the plume implicates.
    """
    v = ce[(ce["status"] == "Violation") & (ce["status_ts"] < horizon)]
    bad = set(v["severity"]) - set(COMPLIANCE_PRIORITY)
    assert not bad, f"a violation of severity {bad} has no work-order priority"
    eq = [int(e) if pd.notna(e) else int(responsible_asset[int(f)])
          for e, f in zip(v["equipment_sk"], v["facility_sk"])]
    return _candidates(v["status_ts"], "Compliance", v["compliance_sk"],
                       v["severity"].map(COMPLIANCE_PRIORITY), eq, [pd.NA] * len(v),
                       [f"{t} violation {c} ({sv})" for t, c, sv in
                        zip(v["event_type"], v["compliance_id"], v["severity"])])


def build_sources(alarms, flags, sensors, ch4_cadence_s, leaves, ongoing, maint, horizon,
                  compliance=None):
    """Every candidate ticket knowable by the horizon, in one deterministic order."""
    parts = [alarm_sources(alarms), exceedance_sources(flags, sensors, ch4_cadence_s),
             offline_sources(leaves, ongoing, maint, horizon)]
    if compliance is not None and len(compliance):
        parts.append(compliance[SOURCE_COLS])
    s = pd.concat([p for p in parts if len(p)] or [parts[0]], ignore_index=True)
    assert s["source_ref"].dtype == "int64" and s["equipment_sk"].dtype == "int64",         "a candidate key lost its int64 dtype -- 63-bit keys do not survive float64"
    s = s[s["trigger_ts"] < horizon]
    s["_o"] = s["source"].map(SOURCE_ORDER)
    s = (s.sort_values(["trigger_ts", "_o", "source_ref"], kind="mergesort")
          .drop(columns="_o").reset_index(drop=True))
    assert not s.duplicated(["source", "source_ref"]).any(), "a source raised two candidates"
    return s[SOURCE_COLS]


def ticket_plan(work_order_id, priority, source, equipment_type):
    """Everything about a ticket's future, drawn once in a fixed order and never re-drawn."""
    rng = get_rng("work_order", work_order_id)
    stalled = bool(rng.random() < WO_STALL_SHARE)
    mu, sig = WO_RESOLUTION_LOGNORMAL[priority]
    res_s = max(WO_MIN_RESOLUTION_S, int(round(float(rng.lognormal(mu, sig)) * 3600.0)))
    respond_s = max(1, int(round(res_s * float(rng.uniform(*WO_RESPOND_FRACTION)))))
    hands = float(rng.uniform(*WO_HANDS_ON_SHARE))
    parts = float(rng.lognormal(np.log(PARTS_MEDIAN_USD[equipment_type]
                                       * SOURCE_PARTS_FACTOR[source]), PARTS_SIGMA))
    labour = CREW_RATE_USD_H[discipline_for(source, equipment_type)] * res_s / 3600.0 * hands
    return {"is_stalled": stalled, "resolution_s": res_s, "respond_s": respond_s,
            "cost_usd": round(labour + parts, 2)}


def make_ticket(number, source_row, ctx):
    """A new ticket from a candidate. ctx maps equipment_sk -> equipment attributes."""
    e = ctx["equipment"][int(source_row["equipment_sk"])]
    wo_id = f"WO-{number:06d}"
    plan = ticket_plan(wo_id, source_row["priority"], source_row["source"], e["equipment_type"])
    return attach_plan({
        "work_order_sk": stable_key("work_order", source_row["source"],
                                    int(source_row["source_ref"])),
        "work_order_id": wo_id, "number": number,
        "facility_sk": e["facility_sk"], "facility_id": e["facility_id"],
        "area_sk": e["area_sk"], "equipment_sk": int(source_row["equipment_sk"]),
        "equipment_type": e["equipment_type"],
        "tag_sk": (None if pd.isna(source_row["tag_sk"]) else int(source_row["tag_sk"])),
        "source": source_row["source"], "source_ref": int(source_row["source_ref"]),
        "priority": source_row["priority"], "created_ts": pd.Timestamp(source_row["trigger_ts"]),
        "sla_hours": WO_SLA_HOURS[source_row["priority"]],
        "assigned_team_sk": stable_key("team", e["sub_basin"],
                                       discipline_for(source_row["source"], e["equipment_type"])),
        "detail": source_row["detail"],
    }, plan)


def attach_plan(t, plan):
    t = dict(t)
    t["is_stalled"] = plan["is_stalled"]
    t["planned_resolution_hours"] = plan["resolution_s"] / 3600.0
    t["respond_ts"] = t["created_ts"] + plan["respond_s"] * _S
    # plan_close_ts exists for a stalled ticket too: it is when the job SHOULD have finished,
    # and it bounds how long the ticket absorbs new candidates (see is_active).
    t["plan_close_ts"] = t["created_ts"] + plan["resolution_s"] * _S
    t["cancel_ts"] = t["created_ts"] + pd.Timedelta(days=WO_CANCEL_AFTER_DAYS)
    t["close_ts"] = None if plan["is_stalled"] else t["plan_close_ts"]
    if t["close_ts"] is not None and t["close_ts"] >= t["cancel_ts"]:
        t["close_ts"] = None    # a resolution tail past the exit is cancelled like a stall
    # Every ticket ends exactly once: Closed at close_ts, or Cancelled at cancel_ts.
    t["end_ts"] = t["close_ts"] if t["close_ts"] is not None else t["cancel_ts"]
    t["breach_ts"] = t["created_ts"] + pd.Timedelta(hours=t["sla_hours"])
    t["planned_cost_usd"] = plan["cost_usd"]
    return t


def is_active(t, ts):
    """Does ticket t absorb a candidate raised at ts? Open, or within the cooldown after.

    Judged on the PLANNED close for every ticket, stalled or not. A stalled ticket that
    absorbed candidates for ever would turn each noisy asset into a permanent sink once one of
    its tickets stalled. Arrivals would then fall month on month as stalls accumulated, a
    drift rather than a steady state. Measured: the harness's 60-day open count fell 30% from
    week 2 to week 9 that way. Past its planned close, a stalled job is a known-stuck ticket,
    and a new symptom on the asset gets a new one.
    """
    if t["created_ts"] > ts:
        return False
    return ts < min(t["plan_close_ts"], t["end_ts"]) + pd.Timedelta(hours=DEDUP_COOLDOWN_HOURS)


def ticket_events(t, lo, hi, team_name):
    """Transitions of t with lo <= event_ts < hi."""
    ev = [(t["created_ts"], None, "Open", "system",
           f"raised from {t['source']}: {t['detail']} (source_ref {t['source_ref']})")]
    started = t["respond_ts"] < t["end_ts"]
    if started:
        ev.append((t["respond_ts"], "Open", "In Progress", team_name, "crew dispatched"))
    if t["close_ts"] is not None:
        res_h = (t["close_ts"] - t["created_ts"]) / _H
        ev.append((t["close_ts"], "In Progress", "Closed", team_name,
                   f"resolved in {res_h:.1f} h against a {t['sla_hours']:.0f} h SLA"
                   + (" (breached)" if res_h > t["sla_hours"] else "")))
    else:
        ev.append((t["cancel_ts"], "In Progress" if started else "Open", "Cancelled", "system",
                   f"cancelled: still unresolved {WO_CANCEL_AFTER_DAYS} days after it was "
                   f"raised, {WO_CANCEL_AFTER_DAYS * 24 / t['sla_hours']:.0f}x its "
                   f"{t['sla_hours']:.0f} h SLA; closed out as stalled work"))
    return [(t["work_order_id"], ts, a, b, actor, note) for ts, a, b, actor, note in ev
            if lo <= ts < hi]


def changes_in(t, lo, hi):
    """Does t's row as of hi differ from its row as of lo?"""
    inst = [t["created_ts"], t["end_ts"]]     # end_ts: the close, or the cancellation
    if t["respond_ts"] < t["end_ts"]:
        inst.append(t["respond_ts"])
    if t["breach_ts"] < t["end_ts"]:
        inst.append(t["breach_ts"])           # an open ticket's is_breached flips here
    return any(lo <= x < hi for x in inst)


def downtime_hours(stops, equipment_sk, lo, hi):
    """Hours the asset spent Down or in Maintenance between lo and hi."""
    s = stops.get(equipment_sk)
    if s is None:
        return 0.0
    a = np.maximum(s[0], lo.value)
    b = np.minimum(s[1], hi.value)
    return float(np.clip(b - a, 0, None).sum() / 3600e9)


def row_as_of(t, horizon, stops):
    """The fact_work_order row for t as of the horizon. The only place status is decided."""
    closed = t["close_ts"] is not None and t["close_ts"] < horizon
    cancelled = t["close_ts"] is None and t["cancel_ts"] < horizon
    status = ("Closed" if closed else "Cancelled" if cancelled
              else "In Progress" if t["respond_ts"] < horizon else "Open")
    res_h = (t["close_ts"] - t["created_ts"]) / _H if closed else None
    # breached if the SLA passed before the ticket ended, or before the horizon if it has not
    breached = ((res_h > t["sla_hours"]) if closed
                else bool(t["breach_ts"] < min(horizon, t["end_ts"])))
    return {
        "work_order_sk": t["work_order_sk"], "work_order_id": t["work_order_id"],
        "facility_sk": t["facility_sk"], "facility_id": t["facility_id"],
        "area_sk": t["area_sk"], "equipment_sk": t["equipment_sk"], "tag_sk": t["tag_sk"],
        "source": t["source"], "source_ref": t["source_ref"], "priority": t["priority"],
        "status": status, "created_ts": t["created_ts"],
        "closed_ts": t["close_ts"] if closed else None,
        "sla_hours": t["sla_hours"], "resolution_hours": res_h,
        "is_breached": breached,
        "is_stalled": t["is_stalled"], "assigned_team_sk": t["assigned_team_sk"],
        "downtime_hours": (downtime_hours(stops, t["equipment_sk"], t["created_ts"],
                                          t["close_ts"]) if closed else None),
        "cost_usd": t["planned_cost_usd"] if closed else None,
        "date_sk": int(t["created_ts"].strftime("%Y%m%d")),
        "is_synthetic": True,
    }


def prior_from_table(wo_rows, window_start, ctx):
    """Pass 1's read: the tickets that can still change or absorb a candidate at window_start.

    wo_rows is fact_work_order as stored, which after a rerun is AS OF A LATER HORIZON than
    window_start. So the filter is not "status != 'Closed'": a ticket that closed during the
    window being rerun is Closed in the table but was open when the window began. Instead:
    created before window_start, and not ENDED before window_start (less the dedup cooldown),
    where a ticket ends at its closed_ts or, if Cancelled, at created_ts + the exit. A
    Cancelled ticket is dropped once its exit has passed, so pass 1 does not carry every
    cancellation for ever.
    Rows created at or after window_start are this window's own output and are regenerated.
    Each ticket's plan is re-derived from its work_order_id and checked against the row.

    Returns (tickets, next_number).
    """
    before = wo_rows[wo_rows["created_ts"] < window_start]
    nums = before["work_order_id"].str.slice(3).astype(int)
    next_number = int(nums.max()) + 1 if len(nums) else 1
    cut = window_start - pd.Timedelta(hours=DEDUP_COOLDOWN_HOURS)
    # A stalled ticket's planned close is not stored, so every open row is carried; is_active
    # then judges each against its re-derived plan.
    ended = before["closed_ts"].where(
        before["status"] != "Cancelled",
        before["created_ts"] + pd.Timedelta(days=WO_CANCEL_AFTER_DAYS))
    live = before[ended.isna() | (ended >= cut)]
    tickets = []
    for r in live.to_dict("records"):
        e = ctx["equipment"][int(r["equipment_sk"])]
        plan = ticket_plan(r["work_order_id"], r["priority"], r["source"], e["equipment_type"])
        assert plan["is_stalled"] == bool(r["is_stalled"]), (
            f"{r['work_order_id']}: stored is_stalled disagrees with its plan -- the seed, the "
            "priority or the resolution model changed since it was written. Run a backfill.")
        t = {k: r[k] for k in ("work_order_id", "facility_id", "source", "priority")}
        t.update({k: int(r[k]) for k in ("work_order_sk", "facility_sk", "area_sk",
                                         "equipment_sk", "source_ref", "assigned_team_sk")})
        t.update(number=int(r["work_order_id"][3:]), equipment_type=e["equipment_type"],
                 tag_sk=None if pd.isna(r["tag_sk"]) else int(r["tag_sk"]),
                 created_ts=pd.Timestamp(r["created_ts"]), sla_hours=float(r["sla_hours"]),
                 detail="(carried)")
        t = attach_plan(t, plan)
        if r["status"] == "Cancelled":
            assert t["close_ts"] is None, (
                f"{r['work_order_id']}: stored as Cancelled but its plan closes it at "
                f"{t['close_ts']} -- WO_CANCEL_AFTER_DAYS or the plan changed. Run a backfill.")
        if pd.notna(r["closed_ts"]):
            assert t["close_ts"] == pd.Timestamp(r["closed_ts"]), (
                f"{r['work_order_id']}: stored closed_ts {r['closed_ts']} disagrees with its "
                f"plan ({t['close_ts']}). A plan must never change after creation.")
        tickets.append(t)
    return tickets, next_number


def run_window(prior, next_number, sources, window_start, window_end, ctx, stops):
    """Both passes, day by day, over [window_start, window_end).

    prior: tickets from prior_from_table() (empty for a backfill). sources: build_sources()
    over any span covering the window. Returns (rows, events, absorbed, tickets): rows is every
    ticket whose row changed in the window, as of window_end; events every transition in the
    window; absorbed every candidate dedup folded into an existing ticket.
    """
    tickets = list(prior)
    by_eq = defaultdict(list)
    for t in tickets:
        by_eq[t["equipment_sk"]].append(t)
    team = ctx["team_name"]
    events, absorbed = [], []
    day = window_start
    while day < window_end:
        nxt = min(day + _DAY, window_end)
        # pass 1 -- advance: every ticket that existed when the day began
        for t in tickets:
            if t["created_ts"] < day:
                events.extend(ticket_events(t, day, nxt, team[t["assigned_team_sk"]]))
        # pass 2 -- create: the day's candidates, in trigger order
        todays = sources[(sources["trigger_ts"] >= day) & (sources["trigger_ts"] < nxt)]
        for s in todays.to_dict("records"):
            live = by_eq[int(s["equipment_sk"])]
            hit = next((t for t in live if is_active(t, s["trigger_ts"])
                        and PRIORITY_RANK[t["priority"]] <= PRIORITY_RANK[s["priority"]]), None)
            if hit is not None:
                absorbed.append((s["source"], s["priority"], int(s["source_ref"]),
                                 hit["work_order_id"], s["trigger_ts"]))
                continue
            t = make_ticket(next_number, s, ctx)
            next_number += 1
            tickets.append(t)
            live.append(t)
            events.extend(ticket_events(t, day, nxt, team[t["assigned_team_sk"]]))
        day = nxt
    rows = [row_as_of(t, window_end, stops) for t in tickets
            if changes_in(t, window_start, window_end)]
    return rows, events, absorbed, tickets


WO_COLS = ["work_order_sk", "work_order_id", "facility_sk", "facility_id", "area_sk",
           "equipment_sk", "tag_sk", "source", "source_ref", "priority", "status", "created_ts",
           "closed_ts", "sla_hours", "resolution_hours", "is_breached", "is_stalled",
           "assigned_team_sk", "downtime_hours", "cost_usd", "date_sk", "is_synthetic"]
EVENT_COLS = ["work_order_id", "event_ts", "from_status", "to_status", "actor", "note",
              "date_sk"]


def to_frames(rows, events):
    wo = pd.DataFrame(rows, columns=WO_COLS)
    for c in ("work_order_sk", "facility_sk", "area_sk", "equipment_sk", "source_ref",
              "assigned_team_sk", "date_sk"):
        wo[c] = wo[c].astype("int64")          # never float64: these are 63-bit keys
    # tag_sk is null on Exceedance tickets, and a DataFrame built from dicts infers float64
    # for ints mixed with None, silently rounding every key above 2**53. Set it from the
    # Python values directly.
    wo["tag_sk"] = pd.array([r["tag_sk"] for r in rows], dtype="Int64")
    ev = pd.DataFrame(events, columns=EVENT_COLS[:-1])
    ev["date_sk"] = ev["event_ts"].dt.strftime("%Y%m%d").astype("int64") if len(ev) else []
    return (wo.sort_values(["created_ts", "work_order_id"], kind="mergesort")
              .reset_index(drop=True),
            ev.sort_values(["event_ts", "work_order_id", "to_status"], kind="mergesort")
              .reset_index(drop=True))


def merge_for_write(existing, changed, window_start):
    """fact_work_order rows to write over the widened replaceWhere range.

    existing: the table's rows inside the range. Rows created before window_start that this
    run did not change are carried unchanged; everything created at or after window_start is
    this run's own output and replaces whatever a previous run of the window wrote. Skipping
    the carry would silently delete every ticket in the range that the run did not touch.
    """
    keep = existing[(existing["created_ts"] < window_start)
                    & ~existing["work_order_sk"].isin(changed["work_order_sk"])]
    out = pd.concat([keep[WO_COLS], changed[WO_COLS]], ignore_index=True)
    return out.sort_values(["created_ts", "work_order_id"], kind="mergesort").reset_index(drop=True)


def open_trajectory(wo, days, stalled=False):
    """Tickets open (Open or In Progress) at the end of each day, from the stored rows alone.
    Non-stalled by default; stalled=True counts the stalled ones instead. A Cancelled ticket
    is open until created_ts + WO_CANCEL_AFTER_DAYS, and never after."""
    live = wo[wo["is_stalled"].astype(bool) == stalled]
    cancel_at = live["created_ts"] + pd.Timedelta(days=WO_CANCEL_AFTER_DAYS)
    is_c = live["status"] == "Cancelled"
    out = []
    for d in days:
        h = d + _DAY
        out.append(int(((live["created_ts"] < h)
                        & (live["closed_ts"].isna() | (live["closed_ts"] >= h))
                        & ~(is_c & (cancel_at < h))).sum()))
    return np.array(out)


# ---- the two dashboard measures: observable, reported apart, never summed -------------------------
# They mean different things operationally, and adding them together hides the second:
#   ACTIVE OPEN      Open / In Progress AND within SLA (NOT is_breached) -- work a planner will
#                    schedule; the KPI
#   STALLED BACKLOG  Open / In Progress AND past SLA (is_breached) -- work that needs escalation
# Both are OBSERVABLE: an operator sees a ticket's status and its age against its SLA.
# is_stalled is not observable -- it is a generator-side fact, fixed at creation, and a stalled
# ticket raised an hour ago looks like any other. It stays on the row because pass 1 uses it
# to decide a ticket never closes and the 60-day cancel keys on it, but the KPI does not split
# on it. Cancelled tickets are in neither measure. The dashboard computes both from
# fact_work_order with exactly these predicates, as two separate tiles.
#
# THE OBSERVABLE SPLIT IS NOT THE STALLED SPLIT, in two directions:
#   - a ticket can be past SLA and still resolve normally: ~20% of P1/P2 and ~25% of P3
#     breach by design, so the backlog holds breaching tickets that will close
#   - a stalled ticket sits in active open until its SLA passes (24 h for P1, 72 h for P2,
#     168 h for P3), and only then moves to the backlog, where it stays until the exit
# The two effects roughly cancel, and the backlog is NOT reliably larger than the stalled
# count. ~20% of tickets breaching is a share of TICKETS, but the backlog counts
# ticket-TIME past SLA. A normal breach overshoots its SLA by hours (P1's log-normal puts
# the p99 at 49 h against a 24 h SLA), so few are past SLA at any moment. A stalled ticket
# sits past SLA for weeks, until the 60-day exit. Measured on the offline stream at the
# 30-day horizon: backlog 22 against 24 stalled open (19 stalled past SLA, 5 stalled still
# within it, 3 normal breaches). The 120-day harness gives 215 against 216, and stalled
# tickets dominate the backlog once the 60-day pool has filled. generator_split() gives the
# is_stalled view, and the run prints both with a cross-tab, so the gap is visible rather
# than confusing.
def open_measures(wo):
    """(active_open, stalled_backlog) -- the dashboard's observable masks over fact_work_order."""
    is_open = wo["status"].isin(WO_OPEN_STATUSES)
    past_sla = wo["is_breached"].astype(bool)
    return is_open & ~past_sla, is_open & past_sla


def generator_split(wo):
    """(non-stalled open, stalled open) -- the generator-side view, for comparison only."""
    is_open = wo["status"].isin(WO_OPEN_STATUSES)
    stalled = wo["is_stalled"].astype(bool)
    return is_open & ~stalled, is_open & stalled


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


def cancelled_by(wo, days):
    """Cumulative Cancelled tickets at the end of each day."""
    at = (wo.loc[wo["status"] == "Cancelled", "created_ts"]
          + pd.Timedelta(days=WO_CANCEL_AFTER_DAYS))
    return np.array([int((at < d + _DAY).sum()) for d in days])


print("work-order model defined -- a ticket's whole future is a pure function of "
      "(work_order_id, priority, source, TOPOLOGY_SEED)")
for _p in WO_PRIORITIES:
    _mu, _s = WO_RESOLUTION_LOGNORMAL[_p]
    print(f"  {_p}  SLA {WO_SLA_HOURS[_p]:>5.0f} h   resolution log-normal median "
          f"{np.exp(_mu):6.1f} h, sigma {_s:.3f}  -> mean {WO_MEAN_RESOLUTION_HOURS[_p]:.0f} h, "
          f"{WO_BREACH_TARGET[_p]:.0%} breaching, p99 {np.exp(_mu + 2.326 * _s):.0f} h")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Calibration: the arrival rate, measured before choosing anything
#
# Per §3 of the design note, the open population converges to `λ × T`. Both are measurable.
#
# **The raw arrival rate is far above the target.** On this window `fact_scada_alarm` holds
# 7,415 alarms, and 1,043 P1 plus 3,099 P2 raise directly. That is **138 candidates a day from
# alarms alone**, before exceedances, offline tags and compliance. At the design note's mean
# resolutions, weighted by that P1:P2 mix, T is about 1.9 days, so raw candidates would hold
# roughly **260 tickets open**. The target is 40-100.
#
# **Dedup does most of the work, but not enough.** Measured by running this notebook's model
# cell over an offline source stream built from the real 5,042-tag estate, 02a's state
# machine, 02b's value model and 02d's debounce, plus 02e's CH₄ model and 02b's outage grid.
# 30 days; "open" is the non-stalled mean over the last 14:
#
# | rule | tickets/day | open | predicted λ×T |
# |---|---|---|---|
# | no dedup (raw candidates) | 108 | ~260 | — |
# | **one open ticket per asset, every P2 raises (the default)** | **53** | **125** | 124 |
# | + 72 h cooldown after the planned close | 39 | 94 | 95 |
# | + 168 h cooldown | 30 | 73 | 72, still rising at day 30 |
# | P2 gated: 3 raises in 24 h on the tag, or standing 4 h | 18 | 60 | 58 |
# | P2 gated + 72 h cooldown | 17 | 53 | 54 |
#
# That stream omits 02b's episode overlay. It reproduces P2 closely (2,622 against 3,099), but
# gives 105 P1 against 1,043: nearly all real trips come from episode signatures. Episode
# trips cluster on one asset for the episode's duration, so dedup collapses most of them, but
# they will still add to every row above in Fabric.
#
# **The binding constraint is P2 itself.** 245 critical assets raise ~11 P2 warnings each a
# month. With every P2 a candidate, one open ticket per asset still leaves ~39 P2 alarm
# tickets a day on their own, twice the whole 10-20 budget. A cooldown is a dedup rule, and even a week of
# it leaves 30 a day. **Only gating P2 reaches the target, and it is applied**
# (`ALARM_P2_MODE = "gated"`). A single P2 warning should not dispatch a crew. Three raises in
# 24 hours on one tag, or a condition standing four hours, is a better model of when an
# operator acts. `"direct"` stays one constant away. The run prints the realised figures against the target. The steady-state assertion
# uses the measured `λ × T`, never the target.

# MARKDOWN ********************

# ### Run mode and window
#
# `run_mode` via `getArgument`, with optional `start_date` / `end_date` overrides, as 02a and
# 03a do.
#
# **The window follows the sources.** Work orders can only be derived where the alarms, the
# CH₄ readings, the tag telemetry and the operating state all exist:
# `SOURCE_START..SOURCE_END` is the overlap of `scada_telemetry`, `sensor_telemetry` and
# `fact_asset_state`. A backfill runs both passes day by day across all of it, starting with no
# tickets. An incremental run takes the last source day, like 03a. Rerunning it replaces that
# day and changes nothing else.
#
# **Sources are always built over the whole retention**, in both modes. That covers the P3
# recurrence window, an exceedance run that started before the window, and the ongoing-outage
# scan. They are small: a few thousand alarms, ~5k flagged CH₄ readings, a few thousand status
# events. The window only selects which triggers pass 2 acts on.

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

for _t in ("scada_telemetry", "sensor_telemetry", "fact_asset_state", "fact_scada_alarm",
           "fact_sensor_status_event"):
    assert table_exists(_t), f"{_t} does not exist -- 03b derives tickets from it; run 02a-02e"


def _span(name):
    r = read_input(name).agg(F.min("date_sk").alias("lo"), F.max("date_sk").alias("hi")).first()
    assert r["lo"] is not None, f"{name} is empty"
    return pd.Timestamp(str(int(r["lo"]))), pd.Timestamp(str(int(r["hi"]))) + _DAY


_tel = _span("scada_telemetry")
_ch4 = _span("sensor_telemetry")
_state = _span("fact_asset_state")
SOURCE_START = max(_tel[0], _ch4[0])
SOURCE_END = min(_tel[1], _ch4[1], _state[1])
assert SOURCE_START < SOURCE_END, f"the source tables do not overlap: {_tel}, {_ch4}, {_state}"

# fact_scada_alarm and fact_sensor_status_event are sparse, so their own max(date_sk) cannot
# prove they reach SOURCE_END. They are derived by 02d from the telemetry, so the check is that
# 02d has run since the telemetry last grew: alarms are raised at ~250 a day, and a table that
# stops more than two days short was not rebuilt.
_al_hi = read_input("fact_scada_alarm").agg(F.max("date_sk").alias("m")).first()["m"]
assert _al_hi is not None and pd.Timestamp(str(int(_al_hi))) >= SOURCE_END - 2 * _DAY, (
    f"fact_scada_alarm ends at date_sk {_al_hi} but the telemetry runs to "
    f"{(SOURCE_END - _DAY).date()} -- run 02d first")

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

assert WINDOW_START < WINDOW_END, f"empty window: {WINDOW_START} .. {WINDOW_END}"
assert SOURCE_START <= WINDOW_START and WINDOW_END <= SOURCE_END, (
    f"window {WINDOW_START.date()}..{WINDOW_END.date()} is outside the sources' span "
    f"{SOURCE_START.date()}..{SOURCE_END.date()}")

if RUN_MODE == "incremental":
    assert table_exists(WO_TABLE) and table_exists(EVENT_TABLE), (
        f"{WO_TABLE} does not exist -- run a backfill first; an incremental run needs the "
        "open tickets to advance")
    _ev_hi = read_input(EVENT_TABLE).agg(F.max("date_sk").alias("m")).first()["m"]
    # ~50 transitions a day estate-wide, so a missing day in the event table is a skipped run
    assert _ev_hi is not None and pd.Timestamp(str(int(_ev_hi))) >= WINDOW_START - _DAY, (
        f"{EVENT_TABLE} was last written through date_sk {_ev_hi}, before this run's window "
        f"starts on {WINDOW_START.date()}. The days in between were never processed -- pass "
        "start_date to cover them, or run a backfill.")
    # A window may be rerun, but it must reach the table's own horizon. Rerunning an earlier
    # day alone would leave later days' rows built on history this run may have changed.
    assert pd.Timestamp(str(int(_ev_hi))) < WINDOW_END, (
        f"{EVENT_TABLE} already runs to date_sk {_ev_hi}, past this window's end "
        f"{WINDOW_END.date()}. Rerun through the latest day, or run a backfill.")

print(f"RUN_MODE={RUN_MODE}  window={WINDOW_START.date()}..{WINDOW_END.date()} "
      f"({(WINDOW_END - WINDOW_START).days} day(s))")
print(f"sources span {SOURCE_START.date()}..{SOURCE_END.date()}  (scada_telemetry "
      f"{_tel[0].date()}..{_tel[1].date()}, sensor_telemetry {_ch4[0].date()}..{_ch4[1].date()}, "
      f"state to {_state[1].date()})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load the estate and build the sources
#
# The ongoing-outage scan is the one read of `scada_telemetry`: each live tag's last reading in
# the final `OFFLINE_SCAN_DAYS`. It is one aggregate over partition-pruned days. A tag whose
# last reading is more than `SENSOR_OFFLINE_MULTIPLE` cadences before the horizon is in an
# outage that `fact_sensor_status_event` cannot show yet, because 02d derives an outage from
# the reading that ends it.

# CELL ********************

fac_pdf = read_input("dim_facility").filter("is_current = true").toPandas()
area_pdf = read_input("dim_area").filter("is_current = true").toPandas()
eq_pdf = read_input("dim_equipment").toPandas()
tag_pdf = read_input("dim_scada_tag").filter("is_current = true").toPandas()
sen_pdf = read_input("dim_sensor").filter("is_current = true").toPandas()

for _label, _frame in (("dim_facility", fac_pdf), ("dim_equipment", eq_pdf),
                       ("dim_scada_tag", tag_pdf), ("dim_sensor", sen_pdf)):
    assert (_frame["topology_seed"] == TOPOLOGY_SEED).all(), (
        f"{_label} was built with a different TOPOLOGY_SEED; rerun 01a-01d")
_cad = sen_pdf["reading_interval_hours"].unique()
assert list(_cad) == [CH4_INTERVAL_HOURS], f"dim_sensor cadence {_cad} != {CH4_INTERVAL_HOURS} h"
CH4_CADENCE_S = int(CH4_INTERVAL_HOURS * 3600)

_eqx = eq_pdf.merge(fac_pdf[["facility_sk", "sub_basin"]], on="facility_sk", how="left")
assert _eqx["sub_basin"].notna().all(), "an asset's facility has no current dim_facility row"
assert set(_eqx["sub_basin"]) <= set(ANCHORS), "a sub_basin outside ANCHORS has no team roster"
CTX = {
    "equipment": {int(r["equipment_sk"]): {"facility_sk": int(r["facility_sk"]),
                                           "facility_id": r["facility_id"],
                                           "area_sk": int(r["area_sk"]),
                                           "equipment_type": r["equipment_type"],
                                           "sub_basin": r["sub_basin"]}
                  for r in _eqx.to_dict("records")},
    "team_name": TEAM_ROSTER,
}

# --- alarms --------------------------------------------------------------------------------------
alarm_pdf = (read_input("fact_scada_alarm")
             .select("alarm_sk", "tag_sk", "tag_id", "equipment_sk", "alarm_type", "priority",
                     "raised_ts", "cleared_ts")
             .toPandas())

# --- CH4 exceedance: flagged readings only, ~1% of sensor_telemetry ------------------------------
flag_pdf = (read_input("sensor_telemetry").filter("exceedance_flag = true")
            .select("sensor_id", "reading_ts").toPandas())

# --- tag outages ---------------------------------------------------------------------------------
leave_pdf = (read_input("fact_sensor_status_event")
             .filter("from_status = 'Online' AND to_status <> 'Decommissioned'")
             .select("tag_sk", "tag_id", "equipment_sk", "event_ts", "duration_hours")
             .toPandas())

_scan_lo = int((SOURCE_END - pd.Timedelta(days=OFFLINE_SCAN_DAYS)).strftime("%Y%m%d"))
_last = (read_input("scada_telemetry").filter(f"date_sk >= {_scan_lo}")
         .groupBy("tag_sk").agg(F.max("reading_ts").alias("last_ts")).toPandas())
_live = tag_pdf[(tag_pdf["status"] != "Decommissioned")
                & (pd.to_datetime(tag_pdf["install_date"]) < SOURCE_END)]
_og = _live.merge(_last, on="tag_sk", how="left")
_silent = _og[_og["last_ts"].isna()]
_og = _og[_og["last_ts"].notna()].copy()
_og["off_ts"] = _og["last_ts"] + pd.to_timedelta(_og["sampling_interval_seconds"], unit="s")
_og = _og[(SOURCE_END - _og["last_ts"]).dt.total_seconds()
          > SENSOR_OFFLINE_MULTIPLE * _og["sampling_interval_seconds"]]
ongoing_pdf = _og[["tag_sk", "tag_id", "equipment_sk", "off_ts"]].reset_index(drop=True)

maint_pdf = (read_input("fact_asset_state").filter("state = 'Maintenance'")
             .select("equipment_sk", "start_ts", "end_ts").toPandas())
stop_pdf = (read_input("fact_asset_state").filter("state IN ('Down', 'Maintenance')")
            .select("equipment_sk", "start_ts", "end_ts").toPandas())
_OPEN_NS = np.iinfo("int64").max
STOPS = {}
for _k, _g in stop_pdf.groupby("equipment_sk"):
    _e = _g["end_ts"].values.astype("datetime64[ns]")
    STOPS[int(_k)] = (_g["start_ts"].values.astype("datetime64[ns]").astype("int64"),
                      np.where(np.isnat(_e), _OPEN_NS, _e.astype("int64")))

# --- compliance: violations from 03c ----------------------------------------------------------------
# Responsible asset for a case with no equipment_sk (a plume cannot name one): the facility's
# asset with the highest leak_propensity, then the most critical, then the lowest key. A
# MODELLING CHOICE -- the asset a compliance team would open the job against, not a claim
# about which asset leaked.
_crit = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
_resp = (eq_pdf.assign(_c=eq_pdf["criticality"].map(_crit))
         .sort_values(["facility_sk", "leak_propensity", "_c", "equipment_sk"],
                      ascending=[True, False, True, True], kind="mergesort")
         .drop_duplicates("facility_sk"))
RESPONSIBLE_ASSET = dict(zip(_resp["facility_sk"].astype(int), _resp["equipment_sk"].astype(int)))
if table_exists(COMPLIANCE_TABLE):
    ce_pdf = (read_input(COMPLIANCE_TABLE).fillna({"equipment_sk": -1})
              .select("compliance_sk", "compliance_id", "facility_sk", "equipment_sk",
                      "event_type", "severity", "status", "status_ts").toPandas())
    ce_pdf["equipment_sk"] = pd.array([None if v < 0 else int(v) for v in ce_pdf["equipment_sk"]],
                                      dtype="Int64")
    ce_pdf["status_ts"] = pd.to_datetime(ce_pdf["status_ts"])
    compliance_pdf = compliance_sources(ce_pdf, RESPONSIBLE_ASSET, SOURCE_END)
    COMPLIANCE_NOTE = (f"{len(compliance_pdf)} violation(s) of {len(ce_pdf):,} compliance events; "
                       "reports and undecided cases raise nothing")
else:
    ce_pdf = pd.DataFrame(columns=["compliance_sk", "status", "status_ts"])
    compliance_pdf = pd.DataFrame(columns=SOURCE_COLS)
    COMPLIANCE_NOTE = f"{COMPLIANCE_TABLE} does not exist yet (run 03c first) -- 0 candidates"

for _c in ("raised_ts", "cleared_ts"):
    alarm_pdf[_c] = pd.to_datetime(alarm_pdf[_c])
flag_pdf["reading_ts"] = pd.to_datetime(flag_pdf["reading_ts"])
leave_pdf["event_ts"] = pd.to_datetime(leave_pdf["event_ts"])
for _f in (maint_pdf,):
    _f["start_ts"], _f["end_ts"] = pd.to_datetime(_f["start_ts"]), pd.to_datetime(_f["end_ts"])

sources = build_sources(alarm_pdf, flag_pdf, sen_pdf, CH4_CADENCE_S, leave_pdf, ongoing_pdf,
                        maint_pdf, SOURCE_END, compliance_pdf)
sources = sources[sources["trigger_ts"] >= SOURCE_START].reset_index(drop=True)

print(f"alarms {len(alarm_pdf):,}   flagged CH4 readings {len(flag_pdf):,}   "
      f"completed outages {len(leave_pdf):,}   ongoing at the horizon {len(ongoing_pdf):,}")
if len(_silent):
    print(f"NOTE  {len(_silent)} live tag(s) with no reading in the last {OFFLINE_SCAN_DAYS} "
          "days -- longer than any outage 02b generates; not treated as offline")
print(f"candidates over the retention: {len(sources):,}")
for (_s, _p), _n in sources.groupby(["source", "priority"]).size().items():
    print(f"  {_s:<14}{_p}  {_n:>6,}   ({_n / max((SOURCE_END - SOURCE_START).days, 1):.1f}/day)")
print(f"  Compliance: {COMPLIANCE_NOTE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Pass 1 and pass 2
#
# **Pass 1's read.** Pass 1 needs the tickets that were not closed when the window began.
# After a rerun, the stored table is as of a later horizon than that. A plain
# `status != 'Closed'` would miss every ticket the window itself closed. It would then fail to
# re-emit those closures, and the event table's `replaceWhere` would delete them.
# `prior_from_table()` therefore selects on timestamps: created before the window, and not
# closed before it. `harness_work_orders.py` carries the `status != 'Closed'` version as a
# negative control and shows it breaks a rerun.
#
# **The write, and whether 02a's widening fits.** It does. A ticket's row lives in the
# partition of the day it was created. Pass 1 changes rows created before the window, when
# they go In Progress, close or cross their SLA. So the `replaceWhere` range is widened back to
# the oldest partition holding a changed row. Inside that range, rows the run did not change
# are read back and carried unchanged by `merge_for_write()`, 02a's pattern. Only changed rows
# widen the range. A stalled ticket changes three times (In Progress, breach, and its
# cancellation 60 days on) and then never again. On its cancellation day the range reaches
# back 60 days to its partition, and no further.
#
# `fact_work_order_event` needs no widening. An event's `date_sk` is its own day. Every event
# in the window is a transition this run emits, so `replaceWhere` over exactly the window's
# days replaces them all.
#
# A backfill overwrites both tables whole. It renumbers from `WO-000001`, and any older ticket
# would collide with that.

# CELL ********************

if RUN_MODE == "incremental":
    stored_pdf = read_work_orders()
    prior, next_number = prior_from_table(stored_pdf, WINDOW_START, CTX)
else:
    stored_pdf = pd.DataFrame(columns=WO_COLS)
    prior, next_number = [], 1

rows, events, absorbed, all_tickets = run_window(prior, next_number, sources, WINDOW_START,
                                                 WINDOW_END, CTX, STOPS)
changed_pdf, event_pdf = to_frames(rows, events)

# In-run determinism: the same window from the same prior state reproduces itself exactly.
_rows2, _ev2, _, _ = run_window(prior, next_number, sources, WINDOW_START, WINDOW_END, CTX, STOPS)
_c2, _e2 = to_frames(_rows2, _ev2)
pd.testing.assert_frame_equal(changed_pdf, _c2, check_exact=True)
pd.testing.assert_frame_equal(event_pdf, _e2, check_exact=True)

WS_SK = int(WINDOW_START.strftime("%Y%m%d"))
WE_SK = int((WINDOW_END - _DAY).strftime("%Y%m%d"))
if RUN_MODE == "incremental":
    RANGE_LO = min([WS_SK] + changed_pdf["date_sk"].tolist())
    _existing = stored_pdf[(stored_pdf["date_sk"] >= RANGE_LO) & (stored_pdf["date_sk"] <= WE_SK)]
    wo_write = merge_for_write(_existing, changed_pdf, WINDOW_START)
    n_carried = len(wo_write) - len(changed_pdf)
else:
    RANGE_LO, wo_write, n_carried = WS_SK, changed_pdf, 0

_new = [t for t in all_tickets if t["created_ts"] >= WINDOW_START]
print(f"pass 1: {len(prior):,} ticket(s) carried in open (or inside the dedup cooldown) "
      f"at {WINDOW_START.date()}")
print(f"pass 2: {len(_new):,} ticket(s) created; {len(absorbed):,} candidate(s) absorbed into "
      "an existing ticket on the same asset")
print(f"transitions emitted: {len(event_pdf):,}")
print(f"fact_work_order write: {len(changed_pdf):,} changed row(s) + {n_carried:,} carried, "
      f"replaceWhere date_sk {RANGE_LO}..{WE_SK}"
      + (f"  (widened back from {WS_SK})" if RANGE_LO < WS_SK else ""))
print(f"OK  rerunning the window from the same prior state reproduces {len(changed_pdf):,} rows "
      f"and {len(event_pdf):,} events exactly")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write both tables

# CELL ********************

WO_SCHEMA = ("work_order_sk long, work_order_id string, facility_sk long, facility_id string, "
             "area_sk long, equipment_sk long, tag_sk long, source string, source_ref long, "
             "priority string, status string, created_ts timestamp, closed_ts timestamp, "
             "sla_hours double, resolution_hours double, is_breached boolean, "
             "is_stalled boolean, assigned_team_sk long, downtime_hours double, "
             "cost_usd double, date_sk long, is_synthetic boolean")
EVENT_SCHEMA = ("work_order_id string, event_ts timestamp, from_status string, "
                "to_status string, actor string, note string, date_sk long")


def _py(v):
    """A value PySpark's type verifier accepts. numpy.int64 is not an int to it, and a NaN,
    NaT or pd.NA must arrive as None."""
    if v is None or v is pd.NA or v is pd.NaT or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, np.generic):
        return v.item()
    return v


def _spark_frame(pdf, schema):
    # Built from Python rows, not from the pandas frame: a nullable 63-bit key must not pass
    # through float64 on the way in, and schema inference must never see a mixed column.
    rows = [tuple(_py(v) for v in r) for r in pdf.itertuples(index=False, name=None)]
    return spark.createDataFrame(rows, schema)


def write_fact(pdf, schema, table, lo_sk, hi_sk):
    sdf = _spark_frame(pdf, schema)
    if RUN_MODE == "backfill" or not table_exists(table):
        (sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .partitionBy("date_sk").saveAsTable(table))
        print(f"{table}: {len(pdf):,} rows (whole-table overwrite, partitioned by date_sk)")
    else:
        (sdf.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"date_sk >= {lo_sk} AND date_sk <= {hi_sk}")
            .partitionBy("date_sk").saveAsTable(table))
        print(f"{table}: {len(pdf):,} rows (replaceWhere date_sk {lo_sk}..{hi_sk})")
    n = spark.table(table).filter(f"date_sk >= {lo_sk} AND date_sk <= {hi_sk}").count()
    assert n == len(pdf), f"{table} holds {n} rows in {lo_sk}..{hi_sk}, wrote {len(pdf)}"


write_fact(wo_write[WO_COLS], WO_SCHEMA, WO_TABLE, RANGE_LO, WE_SK)
write_fact(event_pdf[EVENT_COLS], EVENT_SCHEMA, EVENT_TABLE, WS_SK, WE_SK)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation — every check fails the run, none warns
#
# Run over the **whole** tables as written, not only this run's rows. The steady-state checks
# only mean something over the trajectory, and an incremental run's single day is one point
# on it.

# CELL ********************

wo = read_work_orders()
ev = read_input(EVENT_TABLE).toPandas()
ev["event_ts"] = pd.to_datetime(ev["event_ts"])
HORIZON = WINDOW_END      # every stored row is as of this instant (see the window guard)
assert (wo["created_ts"] < HORIZON).all(), "a ticket created after the run's horizon"

# --- inputs: nothing hidden was read ---------------------------------------------------------------
assert not TABLES_READ & set(GROUND_TRUTH_TABLES), (
    f"ground-truth table(s) read: {sorted(TABLES_READ & set(GROUND_TRUTH_TABLES))}")
assert TABLES_READ <= set(INPUT_TABLES), f"undeclared input(s): {TABLES_READ - set(INPUT_TABLES)}"
assert set(wo["source"]) <= set(WO_SOURCES), f"unknown source(s): {set(wo['source']) - set(WO_SOURCES)}"
assert "Episode" not in set(wo["source"]), "a ticket is sourced from an episode"

# --- keys and FKs ------------------------------------------------------------------------------------
assert wo["work_order_sk"].is_unique, "work_order_sk not unique"
assert wo["work_order_id"].is_unique, "work_order_id not unique"
assert wo["work_order_id"].str.fullmatch(r"WO-\d{6}").all(), "work_order_id not WO-nnnnnn"
assert not wo.duplicated(["source", "source_ref"]).any(), "one source raised two tickets"
for _col, _dim, _key in (("facility_sk", fac_pdf, "facility_sk"), ("area_sk", area_pdf, "area_sk"),
                         ("equipment_sk", eq_pdf, "equipment_sk")):
    _bad = set(wo[_col]) - set(_dim[_key])
    assert not _bad, f"{_col} not in its dimension: {sorted(_bad)[:5]}"
_bad = set(int(v) for v in wo["tag_sk"].dropna()) - set(tag_pdf["tag_sk"])
assert not _bad, f"tag_sk not in dim_scada_tag: {sorted(_bad)[:5]}"
_bad = set(wo["assigned_team_sk"]) - set(TEAM_ROSTER)
assert not _bad, f"assigned_team_sk not in the team roster: {sorted(_bad)[:5]}"
_fe = eq_pdf.set_index("equipment_sk")
assert (wo["facility_sk"].values == _fe.loc[wo["equipment_sk"], "facility_sk"].values).all(), \
    "a ticket's facility is not its asset's facility"
assert (wo["area_sk"].values == _fe.loc[wo["equipment_sk"], "area_sk"].values).all(), \
    "a ticket's area is not its asset's area"

# --- every ticket is DERIVED: its source_ref resolves to a real upstream row --------------------------
# The core check. Resolved against the upstream tables directly, not against build_sources(),
# which would be circular. A ticket older than the sources' current retention cannot be
# re-resolved; those are counted, not excused silently.
_in_ret = wo["created_ts"] >= SOURCE_START
_al = alarm_pdf.set_index("alarm_sk")
_a = wo[_in_ret & (wo["source"] == "Alarm")]
_miss = set(_a["source_ref"]) - set(_al.index)
assert not _miss, f"{len(_miss)} Alarm ticket(s) whose alarm_sk is not in fact_scada_alarm: {sorted(_miss)[:5]}"
_x = _al.loc[_a["source_ref"]]
assert (_x["equipment_sk"].values == _a["equipment_sk"].values).all(), "Alarm ticket on the wrong asset"
assert (_x["priority"].values == _a["priority"].values).all(), "Alarm ticket priority is not its alarm's"
assert (_x["priority"] != "P4").all(), "a P4 alarm raised a ticket"
assert (_a["created_ts"].values >= _x["raised_ts"].values).all(), "a ticket predates its alarm"

_runs = exceedance_runs(flag_pdf, CH4_CADENCE_S).set_index("run_key")
_x = wo[_in_ret & (wo["source"] == "Exceedance")]
_miss = set(_x["source_ref"]) - set(_runs.index)
assert not _miss, f"{len(_miss)} Exceedance ticket(s) with no matching run in sensor_telemetry"
_r = _runs.loc[_x["source_ref"]]
assert (_r["run_len"] >= EXCEEDANCE_MIN_READINGS).all(), "an Exceedance ticket from a short run"
assert (_r["nth_ts"].values == _x["created_ts"].values).all(), \
    f"an Exceedance ticket not raised on the run's {EXCEEDANCE_MIN_READINGS}th reading"

_x = wo[_in_ret & (wo["source"] == "SensorStatus")]
_seen = {stable_key("tag_offline", t, s.isoformat())
         for t, s in zip(leave_pdf["tag_id"], leave_pdf["event_ts"])}
_seen |= {stable_key("tag_offline", t, s.isoformat())
          for t, s in zip(ongoing_pdf["tag_id"], ongoing_pdf["off_ts"])}
_miss = set(_x["source_ref"]) - _seen
assert not _miss, (f"{len(_miss)} SensorStatus ticket(s) matching neither a status event nor an "
                   "ongoing gap in scada_telemetry")

_x = wo[_in_ret & (wo["source"] == "Compliance")]
if len(_x):
    _cev = ce_pdf.set_index("compliance_sk")
    _miss = set(_x["source_ref"]) - set(_cev.index)
    assert not _miss, f"{len(_miss)} Compliance ticket(s) with no event in {COMPLIANCE_TABLE}"
    _cx = _cev.loc[_x["source_ref"]]
    assert (_cx["status"] == "Violation").all(), "a Compliance ticket from a case that is not a violation"
    assert (_cx["status_ts"].values == _x["created_ts"].values).all(), \
        "a Compliance ticket not raised at its violation finding"

# ...and each ticket is exactly what the rules derive from those rows, at the same instant
_d = sources.set_index(["source", "source_ref"])
_k = list(zip(wo.loc[_in_ret, "source"], wo.loc[_in_ret, "source_ref"]))
_miss = [k for k in _k if k not in _d.index]
assert not _miss, f"{len(_miss)} ticket(s) the source rules do not derive, e.g. {_miss[:3]}"
_dd = _d.loc[_k]
assert (_dd["trigger_ts"].values == wo.loc[_in_ret, "created_ts"].values).all(), \
    "a ticket's created_ts is not its source's trigger instant"
assert (_dd["priority"].values == wo.loc[_in_ret, "priority"].values).all(), \
    "a ticket's priority is not its source's"
n_old = int((~_in_ret).sum())

# --- timestamps, resolution, breach -------------------------------------------------------------------
assert set(wo["status"]) <= set(WO_STATUSES), f"unknown status: {set(wo['status']) - set(WO_STATUSES)}"
_c = wo[wo["status"] == "Closed"]
_o = wo[wo["status"].isin(WO_OPEN_STATUSES)]
_x = wo[wo["status"] == "Cancelled"]
_exit = pd.Timedelta(days=WO_CANCEL_AFTER_DAYS)
assert _c["closed_ts"].notna().all() and _o["closed_ts"].isna().all() \
    and _x["closed_ts"].isna().all(), "closed_ts disagrees with status"
assert (_c["closed_ts"] > _c["created_ts"]).all(), "closed_ts not after created_ts"
assert np.allclose((_c["closed_ts"] - _c["created_ts"]).dt.total_seconds() / 3600.0,
                   _c["resolution_hours"], rtol=0, atol=1e-9), "resolution_hours disagrees"
assert _o["resolution_hours"].isna().all() and _x["resolution_hours"].isna().all(), \
    "an open or cancelled ticket carries a resolution"
# the exit: nothing open past it, and nothing cancelled before it
assert (_o["created_ts"] + _exit >= HORIZON).all(), (
    f"{int((_o['created_ts'] + _exit < HORIZON).sum())} ticket(s) still open more than "
    f"{WO_CANCEL_AFTER_DAYS} days after they were raised -- the stalled exit did not fire")
assert (_x["created_ts"] + _exit < HORIZON).all(), "a ticket cancelled before its exit"
assert _x["is_breached"].all(), "a cancelled ticket not breached (it outlived every SLA)"
assert _x["cost_usd"].isna().all() and _x["downtime_hours"].isna().all(), \
    "a cancelled ticket carries a cost or downtime -- it was never resolved"
assert (_c["is_breached"] == (_c["resolution_hours"] > _c["sla_hours"])).all(), \
    "is_breached != resolution_hours > sla_hours on a closed ticket"
_el = (HORIZON - _o["created_ts"]).dt.total_seconds() / 3600.0
assert (_o["is_breached"] == (_el > _o["sla_hours"])).all(), \
    "is_breached on an open ticket != elapsed > sla_hours at the horizon"
assert not _c["is_stalled"].any(), "a stalled ticket closed"
assert (wo["sla_hours"] == wo["priority"].map(WO_SLA_HOURS)).all(), "sla_hours is not the priority's"
assert (wo["date_sk"] == wo["created_ts"].dt.strftime("%Y%m%d").astype("int64")).all(), \
    "date_sk is not the created day"
assert _c["downtime_hours"].notna().all() and _c["cost_usd"].notna().all(), \
    "a closed ticket lacks downtime or cost"
assert (_c["downtime_hours"] <= _c["resolution_hours"] + 1e-9).all(), "downtime exceeds the ticket"

# --- events: every ticket's history is consistent with its row ---------------------------------------
assert not ev.duplicated(["work_order_id", "to_status"]).any(), "a ticket entered a status twice"
assert (ev["date_sk"] == ev["event_ts"].dt.strftime("%Y%m%d").astype("int64")).all()
_legal = {(None, "Open"), ("Open", "In Progress"), ("In Progress", "Closed"),
          ("In Progress", "Cancelled"), ("Open", "Cancelled")}
_pairs = set(zip(ev["from_status"].where(ev["from_status"].notna(), None), ev["to_status"]))
assert _pairs <= _legal, f"illegal transition(s): {_pairs - _legal}"
_evw = ev.pivot(index="work_order_id", columns="to_status", values="event_ts")
_w = wo.set_index("work_order_id")
_w_in = _w[_w["created_ts"] >= SOURCE_START]
assert set(_w_in.index) <= set(_evw.index), "a ticket with no Created event"
assert (_evw.loc[_w_in.index, "Open"].values == _w_in["created_ts"].values).all(), \
    "Created event not at created_ts"
_cl = _w_in[_w_in["status"] == "Closed"]
assert (_evw.loc[_cl.index, "Closed"].values == _cl["closed_ts"].values).all(), \
    "Closed event not at closed_ts"
_nc = _w_in[_w_in["status"] != "Closed"].index
assert _evw.loc[_nc, "Closed"].isna().all() if "Closed" in _evw else True, "an open ticket has a Closed event"
_cx = _w_in[_w_in["status"] == "Cancelled"]
if len(_cx):
    assert (_evw.loc[_cx.index, "Cancelled"].values
            == (_cx["created_ts"] + _exit).values).all(), "Cancelled event not at the exit"
_nx = _w_in[_w_in["status"] != "Cancelled"].index
assert _evw.loc[_nx, "Cancelled"].isna().all() if "Cancelled" in _evw else True, \
    "a ticket that is not Cancelled has a Cancelled event"

print("OK  read no ground-truth table; every source in " + ", ".join(WO_SOURCES))
print(f"OK  work_order_sk and work_order_id unique; facility, area, asset, tag and team resolve")
print(f"OK  every ticket's source_ref resolves to a real alarm, exceedance run or outage, and "
      f"the rules derive it at its created_ts ({int(_in_ret.sum()):,} checked"
      + (f"; {n_old:,} older than the sources' retention, not re-checkable" if n_old else "") + ")")
print("OK  closed_ts > created_ts; resolution_hours agrees; is_breached matches its rule on "
      "closed and open tickets; no stalled ticket closed")
print(f"OK  no ticket open beyond the {WO_CANCEL_AFTER_DAYS}-day exit; every Cancelled ticket "
      "cancelled exactly at it, breached, with no cost")
print("OK  every ticket's events agree with its row; only Open -> In Progress -> Closed, or "
      "-> Cancelled at the exit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Steady state — the trajectory, not a day
#
# `expected = λ × T`. λ is non-stalled tickets created per day over the trend window. T is the
# mean **planned** resolution of non-stalled tickets, re-derived from each plan. Using
# realised closures instead would be censored: a long ticket still open at the horizon has no
# resolution yet, so T would read short. Stalled tickets are excluded from both sides. They
# never close, so their drain is the 60-day exit rather than the resolution model, and
# counting them would mix the two.
#
# **Two dashboard measures, observable and never summed** (`open_measures()`):
#
# - **Active open**: Open or In Progress, and within SLA. This is work a planner will
#   schedule, and it is the KPI.
# - **Stalled backlog**: Open or In Progress, and past SLA. This is work that needs
#   escalation.
#
# Adding the two together would hide the second. A growing escalation backlog would look like
# a slightly busy planner. Cancelled tickets are in neither.
#
# **The steady-state checks stay generator-side.** λ×T is a property of the resolution model,
# so it is judged on non-stalled tickets, as above. The generator-side stalled count converges
# to `stall arrivals/day × WO_CANCEL_AFTER_DAYS`, and only reaches that after 60 days of
# history. The run prints the observable split, the generator split and their cross-tab
# side by side. See the note at `open_measures()` for why the two disagree.
#
# The warm-up is excluded. A backfill starts with no tickets, so the population rises for
# about one P3 lifetime before it levels. The trend is fitted over the final
# `TREND_WINDOW_DAYS` only, and only once `WARMUP_DAYS` of history precede them.

# CELL ********************

_first = wo["created_ts"].min().normalize()
DAYS = pd.date_range(max(_first, SOURCE_START), HORIZON - _DAY, freq="D")
traj = open_trajectory(wo, DAYS)
stalled_open = open_trajectory(wo, DAYS, stalled=True)
cancelled_cum = cancelled_by(wo, DAYS)
obs_active, obs_backlog = observable_trajectory(wo, DAYS)

_plans = [ticket_plan(r["work_order_id"], r["priority"], r["source"],
                      CTX["equipment"][int(r["equipment_sk"])]["equipment_type"])
          for r in wo.to_dict("records")]
wo["planned_resolution_hours"] = [p["resolution_s"] / 3600.0 for p in _plans]
assert (np.array([p["is_stalled"] for p in _plans]) == wo["is_stalled"].astype(bool).values).all(), \
    "a stored is_stalled disagrees with its plan"
_cp = wo["status"] == "Closed"
assert np.allclose(wo.loc[_cp, "planned_resolution_hours"], wo.loc[_cp, "resolution_hours"],
                   rtol=0, atol=1e-9), "a closed ticket's resolution is not its planned resolution"

print("open tickets at the end of each day")
print(f"  {'':<12}{'-- dashboard (observable) --':>30}   {'-- generator-side --':>26}")
print(f"  {'day':<12}{'active open':>13}{'backlog':>9}  {'':<30}"
      f"{'non-stalled':>12}{'stalled':>9}{'cancelled':>11}")
for _d, _a, _b, _n, _s, _k in zip(DAYS, obs_active, obs_backlog, traj, stalled_open,
                                  cancelled_cum):
    print(f"  {str(_d.date()):<12}{_a:>13}{_b:>9}  "
          f"{'#' * int(round(_a / max(obs_active.max(), 1) * 30)):<30}{_n:>12}{_s:>9}{_k:>11}")

_enough = len(DAYS) >= WARMUP_DAYS + TREND_WINDOW_DAYS
_tw = DAYS[-TREND_WINDOW_DAYS:]
_live = wo[~wo["is_stalled"].astype(bool)]
_arr = _live[(_live["created_ts"] >= _tw[0]) & (_live["created_ts"] < _tw[-1] + _DAY)]
LAMBDA = len(_arr) / len(_tw)
T_DAYS = float(_live["planned_resolution_hours"].mean() / 24.0)
EXPECTED = LAMBDA * T_DAYS
REALISED = float(traj[-TREND_WINDOW_DAYS:].mean())
_x = np.arange(min(TREND_WINDOW_DAYS, len(traj)), dtype=float)
_slope = float(np.polyfit(_x, traj[-len(_x):].astype(float), 1)[0]) if len(_x) > 1 else 0.0
RISE = _slope * (len(_x) - 1)
ALL_PER_DAY = len(wo[(wo["created_ts"] >= _tw[0]) & (wo["created_ts"] < _tw[-1] + _DAY)]) / len(_tw)

print()
print(f"arrival rate      {LAMBDA:.1f} non-stalled tickets/day over {_tw[0].date()}..{_tw[-1].date()}"
      f"   ({ALL_PER_DAY:.1f}/day including stalled)")
print(f"mean resolution   {T_DAYS:.2f} days (planned, non-stalled)")
print(f"predicted open    {EXPECTED:.1f}   (lambda x T)")
print(f"realised open     {REALISED:.1f}   (mean of the last {len(_x)} days; ratio "
      f"{REALISED / max(EXPECTED, 1e-9):.2f}, band {STEADY_BAND[0]}-{STEADY_BAND[1]})")
print(f"fitted rise       {RISE:+.1f} over the last {len(_x)} days "
      f"({RISE / max(REALISED, 1e-9):+.1%} of the level; max +{TREND_MAX_RISE:.0%})")
_stall_rate = int(wo.loc[wo["created_ts"] >= _tw[0], "is_stalled"].sum()) / len(_tw)
print(f"stalled open      {int(stalled_open[-1])} at the horizon, "
      f"{wo['is_stalled'].mean():.1%} of all tickets (target {WO_STALL_SHARE:.0%}), excluded above;"
      f" converges to ~{_stall_rate * WO_CANCEL_AFTER_DAYS:.0f} "
      f"({_stall_rate:.1f}/day x {WO_CANCEL_AFTER_DAYS} days)")
_cx = wo[wo["status"] == "Cancelled"]
print(f"cancelled         {len(_cx):,} to date at the {WO_CANCEL_AFTER_DAYS}-day exit "
      f"({int(_cx['is_stalled'].sum()):,} stalled, {int((~_cx['is_stalled'].astype(bool)).sum()):,}"
      " resolution tails); never counted as open"
      + ("" if len(DAYS) > WO_CANCEL_AFTER_DAYS
         else f" -- none can exist until {WO_CANCEL_AFTER_DAYS} days of history"))
_act, _bkl = open_measures(wo)
_gns, _gst = generator_split(wo)
assert not (_act & _bkl).any() and ((_act | _bkl) == (_gns | _gst)).all(), \
    "the observable and generator splits do not partition the same open tickets"
assert int(_act.sum()) == int(obs_active[-1]) and int(_bkl.sum()) == int(obs_backlog[-1]), \
    "open_measures() disagrees with the observable trajectory at the horizon"
assert int(_gns.sum()) == int(traj[-1]) and int(_gst.sum()) == int(stalled_open[-1]), \
    "generator_split() disagrees with the trajectory at the horizon"
_nf = len(fac_pdf)
print()
print("OPEN-TICKET MEASURES AT THE HORIZON -- the dashboard's two tiles, never one sum")
print(f"  active open       {int(_act.sum()):>5}   {_act.sum() / _nf:.2f} per facility   "
      "within SLA: work a planner will schedule (the KPI)")
print(f"  stalled backlog   {int(_bkl.sum()):>5}   {_bkl.sum() / _nf:.2f} per facility   "
      "past SLA: needs escalation")
print(f"  last-14-day mean  active {obs_active[-TREND_WINDOW_DAYS:].mean():.1f}, "
      f"backlog {obs_backlog[-TREND_WINDOW_DAYS:].mean():.1f}")
print("generator-side, for comparison (not a dashboard measure)")
print(f"  non-stalled open  {int(_gns.sum()):>5}   stalled open {int(_gst.sum()):>5}   heading for "
      f"~{_stall_rate * WO_CANCEL_AFTER_DAYS:.0f} at the {WO_CANCEL_AFTER_DAYS}-day exit")
print("cross-tab of the open tickets")
print(f"  {'':<22}{'not stalled':>12}{'stalled':>9}")
for _lbl, _m in (("within SLA (active)", _act), ("past SLA (backlog)", _bkl)):
    print(f"  {_lbl:<22}{int((_m & _gns).sum()):>12}{int((_m & _gst).sum()):>9}")
print("  within SLA & stalled: stuck, but not yet visible as stuck. Past SLA & not stalled: "
      "breaching normally, and will still close.")
print()
KPI_ACTIVE = float(obs_active[-TREND_WINDOW_DAYS:].mean())
print(f"against the operational target: {ALL_PER_DAY:.1f} new/day (target "
      f"{TARGET_ARRIVALS_PER_DAY[0]:.0f}-{TARGET_ARRIVALS_PER_DAY[1]:.0f}), {KPI_ACTIVE:.0f} active open "
      f"(target {TARGET_OPEN[0]:.0f}-{TARGET_OPEN[1]:.0f}) -- "
      + ("within" if (TARGET_ARRIVALS_PER_DAY[0] <= ALL_PER_DAY <= TARGET_ARRIVALS_PER_DAY[1]
                      and TARGET_OPEN[0] <= KPI_ACTIVE <= TARGET_OPEN[1]) else
         "OUTSIDE; see the calibration note for what moves it") + ". Reported, not asserted.")

if _enough:
    assert STEADY_BAND[0] * EXPECTED <= REALISED <= STEADY_BAND[1] * EXPECTED, (
        f"realised open {REALISED:.1f} is outside [{STEADY_BAND[0]} x, {STEADY_BAND[1]} x] "
        f"the predicted {EXPECTED:.1f}. The drain does not match the arrival rate.")
    assert RISE <= TREND_MAX_RISE * REALISED, (
        f"the open count rose {RISE:+.1f} over the last {len(_x)} days, more than "
        f"{TREND_MAX_RISE:.0%} of its level. The backlog is growing, which is the V1 defect.")
    print("OK  open count inside the steady-state band and not trending upward")
else:
    print(f"NOTE  only {len(DAYS)} day(s) of history; the steady-state checks need "
          f"{WARMUP_DAYS + TREND_WINDOW_DAYS}. NOT asserted this run.")

# --- breach share, on planned resolution, by priority --------------------------------------------------
print()
print(f"  {'priority':<10}{'tickets':>8}{'mean res h':>12}{'target':>8}{'breach (planned)':>18}"
      f"{'band':>14}{'closed so far':>15}")
for _p in WO_PRIORITIES:
    _t = _live[_live["priority"] == _p]
    _t = _t[_t["created_ts"] < HORIZON - pd.Timedelta(hours=WO_SLA_HOURS[_p])] if _enough else _t
    if _t.empty:
        print(f"  {_p:<10}{0:>8}   no tickets at this priority -- band not checked")
        continue
    _share = float((_t["planned_resolution_hours"] > _t["sla_hours"]).mean())
    _cls = wo[(wo["priority"] == _p) & (wo["status"] == "Closed")]
    _lo, _hi = WO_BREACH_BAND[_p]
    print(f"  {_p:<10}{len(_t):>8}{_t['planned_resolution_hours'].mean():>12.1f}"
          f"{WO_MEAN_RESOLUTION_HOURS[_p]:>8.0f}{_share:>18.1%}{f'{_lo:.0%}-{_hi:.0%}':>14}"
          f"{(_cls['is_breached'].mean() if len(_cls) else float('nan')):>15.1%}")
    if len(_t) >= WO_BREACH_MIN_TICKETS:
        assert _lo <= _share <= _hi, (f"{_p} breach share {_share:.1%} outside {_lo:.0%}-{_hi:.0%}; "
                                      "is_breached would be decoration or noise")
print("  (breach on planned resolution avoids censoring; 'closed so far' is biased low, since "
      "a long ticket is likelier still open)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Distributions

# CELL ********************

print("tickets by source and priority")
_tab = wo.pivot_table(index="source", columns="priority", values="work_order_id",
                      aggfunc="count", fill_value=0).reindex(index=list(WO_SOURCES),
                                                             columns=list(WO_PRIORITIES),
                                                             fill_value=0)
_tab["total"] = _tab.sum(axis=1)
print(_tab.to_string())
print(f"  Compliance: {COMPLIANCE_NOTE}")

if absorbed:
    _ab = pd.DataFrame(absorbed, columns=["source", "priority", "source_ref", "into", "ts"])
    print()
    print(f"candidates absorbed by dedup this run: {len(_ab):,}")
    for (_s, _p), _n in _ab.groupby(["source", "priority"]).size().items():
        print(f"  {_s:<14}{_p}  {_n:>6,}")

print()
print("status now:  " + "   ".join(f"{k} {v:,}" for k, v in wo["status"].value_counts().items()))
_cl = wo[wo["status"] == "Closed"]
print(f"closed: mean downtime {_cl['downtime_hours'].mean():.1f} h, mean cost "
      f"${_cl['cost_usd'].mean():,.0f}, total ${_cl['cost_usd'].sum():,.0f}")

_tpf = (wo.groupby("facility_sk").size().reindex(fac_pdf["facility_sk"], fill_value=0))
print()
print(f"tickets per facility: min {_tpf.min()}  median {_tpf.median():.0f}  p90 "
      f"{_tpf.quantile(0.9):.0f}  max {_tpf.max()}   ({int((_tpf == 0).sum())} facilities with none)")
_act, _bkl = open_measures(wo)
_by_fac = lambda m: (wo[m].groupby("facility_sk").size()                    # noqa: E731
                     .reindex(fac_pdf["facility_sk"], fill_value=0).values)
_top = (fac_pdf.set_index("facility_sk")[["facility_id", "facility_name", "facility_type"]]
        .assign(tickets=_tpf, active=_by_fac(_act), backlog=_by_fac(_bkl))
        .sort_values(["tickets", "facility_id"], ascending=[False, True]).head(10))
print("top 10 facilities by tickets:")
for r in _top.itertuples():
    print(f"  {r.facility_id}  {r.facility_name:<34}{r.facility_type:<24}"
          f"{r.tickets:>5} tickets  {r.active:>3} active open  {r.backlog:>3} backlog")
print(f"active open per facility now: {_act.sum() / len(fac_pdf):.2f} (target ~0.25-0.5, one per "
      f"2-4 sites);  stalled backlog per facility: {_bkl.sum() / len(fac_pdf):.2f}, reported apart")
if len(_cx):
    print()
    print("cancelled by priority:  " + "   ".join(
        f"{k} {v:,}" for k, v in _cx["priority"].value_counts().sort_index().items()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary

# CELL ********************

print("=" * 76)
print("WORK ORDERS GENERATED")
print("=" * 76)
print(f"  run mode          {RUN_MODE}")
print(f"  window            {WINDOW_START.date()} .. {WINDOW_END.date()}")
print(f"  tickets           {len(wo):,} in {WO_TABLE}; {len(ev):,} transitions in {EVENT_TABLE}")
print(f"  arrivals          {ALL_PER_DAY:.1f} per day")
print(f"  active open       {int(obs_active[-1])} at the horizon ({obs_active[-1] / len(fac_pdf):.2f} "
      "per facility) -- within SLA, the dashboard KPI")
print(f"  stalled backlog   {int(obs_backlog[-1])} at the horizon ({obs_backlog[-1] / len(fac_pdf):.2f} "
      "per facility) -- past SLA, a separate tile, never added to active open")
print(f"  generator-side    {int(traj[-1])} non-stalled open ({REALISED:.0f} last-14-day mean against "
      f"{EXPECTED:.0f} predicted), {int(stalled_open[-1])} stalled open")
print(f"  cancelled         {len(_cx):,} at the {WO_CANCEL_AFTER_DAYS}-day exit")
print(f"  P2 mode           {ALARM_P2_MODE}")
print(f"  compliance        {COMPLIANCE_NOTE}")
print()
print(f"  tables written    {WO_TABLE}, {EVENT_TABLE} (partitioned by date_sk)")
print(f"  read              {', '.join(sorted(TABLES_READ))}")
print(f"  not read          {', '.join(GROUND_TRUTH_TABLES)} -- tickets come from symptoms only")
print("  not modified      every dim_* table, fact_scada_alarm, every telemetry table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
