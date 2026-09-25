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

# # 02d — Derive SCADA Alarms and Sensor Status
#
# Writes **`fact_scada_alarm`** and **`fact_sensor_status_event`**, both derived entirely from
# `scada_telemetry`. This notebook **modifies nothing** — not the telemetry, not
# `fact_asset_state`, not any `dim_*` table.
#
# ### Alarms are derived, never generated
#
# Every alarm here comes from scanning `value_num` against that tag's own limits in
# `dim_scada_tag`. There is **no independent random alarm stream**. That is the whole point:
# the dashboard shows alarms and trends side by side, and a user who drills from an alarm into
# the trend has to find the excursion sitting there in the data. An alarm generated beside the
# telemetry rather than from it would be a lie that the demo's own chart disproves.
#
# The same property makes the table trivially idempotent — see the boundary note below.
#
# ### The four suppressions, and why each exists
#
# **Debounce.** A breach raises only after `ALARM_DEBOUNCE_SAMPLES` consecutive readings. A
# single spike is noise. Without it the 0.5% Bad-quality rail values alone would raise tens of
# thousands of alarms — 02b puts those deliberately out of range, which is exactly what a
# naive scan would call an excursion.
#
# **Deadband.** An alarm clears only once the value is back inside the limit by
# `ALARM_DEADBAND_PCT` of the normal span, held for the same count. Without hysteresis a value
# sitting on the threshold produces chatter.
#
# **Quality.** `Bad` readings never raise — a rail value is an instrument fault, not a process
# excursion — and neither do `Substituted` ones, which are Maintenance substitutions.
# `Uncertain` readings neither raise nor clear: they extend whatever state is current.
#
# **State.** No alarm on an asset that is `Down` or `Maintenance`. A stopped compressor reads
# zero flow, which is roughly 40 sigma below `normal_min` **by design** — alarming on it would
# bury every real signal under state artefacts. `Standby` additionally suppresses `flow` and
# `rpm`, which go to zero there for the same reason.
#
# **Startup and Shutdown are suppressed too — chosen, not assumed.** A ramp legitimately
# crosses limits on its way between the stopped and running profiles: 02b interpolates the two
# profiles with a smoothstep across the interval, so a compressor coming up passes through
# every value between 250 psig and 1000 psig. Those crossings are the transition working
# correctly, not an excursion, and an operator does not want an alarm per start. The cost is
# that a genuine failure *during* a start is invisible to this notebook; that is the right
# trade at this fidelity, and the place to revisit it is a start-permissive alarm group.
#
# ### What this notebook does NOT do
# No rollups — that is `02c`. No alarm is invented, and no `dim_*` or upstream fact table is
# touched.

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

# ### Run mode, and the boundary problem
#
# `run_mode` follows 02b: `getArgument` in a try/except, with `start_date` / `end_date`
# overrides, and an incremental window taken from the table's own watermark rather than
# `utcnow()`.
#
# **The boundary problem.** An alarm raised before the window and cleared inside it has a
# `raised_ts` outside the window, so its partition is not in the run's `replaceWhere` scope.
# The same applies to an outage spanning midnight in `fact_sensor_status_event`.
#
# `02a` solved this by widening the `replaceWhere` predicate to every partition the run
# touches, then **reading back** the rows it did not regenerate and writing them out unchanged
# — necessary there because a state interval's future depends on a chain that was decided when
# it opened, and cannot be recovered from anything else.
#
# **Neither 02a's approach nor a lookback window fits here.** The obvious idea — scan a few
# days before the window so any straddling alarm is recomputed — is wrong, and it is worth
# saying exactly why, because it looks right:
#
# > The debounce machine raises on the **Nth consecutive** breaching reading. Its output
# > therefore depends on where each *run* of breaches began, not merely on whether an alarm was
# > open. A scan whose first reading lands in the middle of a breach run sees a shorter run and
# > raises late, or never. With a run of 8 breaches starting at slot 10 and a debounce of 3, a
# > full scan raises at slot 12; scans beginning at slots 11, 12 and 13 raise at 13, 14 and 15.
# > No alarm is open at any of those edges — the run simply started earlier than the scan.
# > **No lookback length fixes this in general, because any edge can land mid-run.**
#
# Verified in the offline harness: with a 7-day lookback, 119 of 400 simulated months
# disagreed with their backfill.
#
# **Chosen: always derive from the full telemetry retention, in both run modes, and rewrite
# every alarm partition.** `scada_telemetry` keeps `TELEMETRY_RAW_DAYS` (30) of history, so
# there is no edge to straddle — the scan starts where the data starts. Backfill and
# incremental become *identical by construction* rather than by argument, which is a much
# stronger property than a lookback can offer.
#
# The cost is negligible and the volumes are why this is the right call rather than a
# concession: `fact_scada_alarm` is hundreds of rows, `fact_sensor_status_event` a few
# thousand, and the validation below has to re-read the telemetry anyway. Partial
# recomputation of a derived table this small buys nothing and costs correctness.
#
# **What `run_mode` still controls** is `WINDOW_END`, the "now" anchor for acknowledgement —
# and that is the §2.3 drain. Each daily run acknowledges the alarms whose fixed delay has
# elapsed by its own window end, so the acknowledged population advances day by day while the
# derivation underneath it stays constant.
#
# The alternative that *would* allow a bounded scan is a stored sessioniser checkpoint per
# `(tag, alarm_type)` — last action, run length, in-alarm flag — carried between runs. That is
# real streaming state, it would make the table no longer a pure function of the telemetry, and
# at 30 days of retention it is not worth the machinery.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window

spark.conf.set("spark.sql.session.timeZone", "UTC")

ALARM_TABLE = "fact_scada_alarm"
STATUS_TABLE = "fact_sensor_status_event"
TELEMETRY_TABLE = "scada_telemetry"
STATE_TABLE = "fact_asset_state"

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


def table_exists(name):
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False


assert table_exists(TELEMETRY_TABLE), (
    f"{TELEMETRY_TABLE} does not exist. 02d derives every row it writes from the telemetry; "
    "run 02b_gen_scada_telemetry first."
)

AS_OF = pd.Timestamp(TOPOLOGY_AS_OF)

# The telemetry's own extent bounds everything here.
_tel_span = spark.sql(
    f"SELECT min(date_sk) AS lo, max(date_sk) AS hi FROM {TELEMETRY_TABLE}").collect()[0]
assert _tel_span["lo"] is not None, f"{TELEMETRY_TABLE} is empty"
TEL_START = pd.Timestamp(str(int(_tel_span["lo"])))
TEL_END = pd.Timestamp(str(int(_tel_span["hi"]))) + pd.Timedelta(days=1)

if RUN_MODE == "backfill":
    WINDOW_START, WINDOW_END = TEL_START, TEL_END
else:
    wm = None
    if table_exists(ALARM_TABLE):
        wm = spark.sql(f"SELECT max(date_sk) AS m FROM {ALARM_TABLE}").collect()[0]["m"]
    if wm is None:
        WINDOW_START, WINDOW_END = TEL_END - pd.Timedelta(days=1), TEL_END
        print(f"{ALARM_TABLE} is empty or absent -- incremental falls back to a one-day "
              "window. Run a backfill first for a populated history.")
    else:
        WINDOW_START = pd.Timestamp(str(int(wm))) + pd.Timedelta(days=1)
        WINDOW_END = min(WINDOW_START + pd.Timedelta(days=1), TEL_END)

if _start_override:
    WINDOW_START = pd.Timestamp(_start_override)
if _end_override:
    WINDOW_END = pd.Timestamp(_end_override)

WINDOW_START = pd.Timestamp(WINDOW_START).normalize()
WINDOW_END = pd.Timestamp(WINDOW_END).normalize()
assert WINDOW_START < WINDOW_END, f"empty window: {WINDOW_START} .. {WINDOW_END}"
assert WINDOW_END <= TEL_END, (
    f"window ends {WINDOW_END.date()} but the telemetry stops at {TEL_END.date()}"
)

WINDOW_DAYS = int((WINDOW_END - WINDOW_START).days)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Alarm model configuration

# CELL ********************

ALARM_DEBOUNCE_SAMPLES = 3       # consecutive breaching readings before a raise
ALARM_DEADBAND_PCT = 0.02        # return inside the limit by this share of the normal span
ALARM_CHATTER_COUNT = 5          # raises within the window before a tag is called chattering
ALARM_CHATTER_WINDOW_HOURS = 4
ALARM_CHATTER_MAX_SHARE = 0.05   # validated
ALARM_FROZEN_SAMPLES = 8         # identical consecutive values before a Frozen alarm
# No lookback constant: a bounded scan cannot reproduce a backfill for a debounce machine.
# See the boundary note above, and harness_alarm_incr.py, which measures the disagreement.

ALARM_TYPES = ("HiHi", "Hi", "Lo", "LoLo", "BadQuality", "Frozen")
PRIORITIES = ("P1", "P2", "P3", "P4")

# Priority from the breached limit. P1-P4 is the work-order SLA vocabulary, deliberately --
# an alarm that becomes a ticket must not change priority scheme on the way.
ALARM_TRIP_PRIORITY = {"HiHi": "P1", "LoLo": "P1"}          # trip limits
ALARM_WARN_PRIORITY = {"critical": "P2", "other": "P3"}     # warning limits, by asset
ALARM_DIAGNOSTIC_PRIORITY = "P4"                            # quality and rate-of-change

# Acknowledgement delay, in minutes (lo, hi), drawn once per alarm from (alarm_sk, seed) and
# never re-drawn -- see DESIGN_NOTE_incremental_facts.md section 2.3.
ALARM_ACK_DELAY_MINUTES = {
    "P1": (2.0, 25.0),        # someone is looking at a trip within the half hour
    "P2": (10.0, 120.0),
    "P3": (30.0, 600.0),
    "P4": (120.0, 2880.0),    # two days is a normal life for a diagnostic alarm
}
# Share never acknowledged at all. Stalled work exists; it is excluded from the steady-state
# assertion so a permanently-open tail does not look like a broken drain.
ALARM_NEVER_ACK_SHARE = {"P1": 0.00, "P2": 0.01, "P3": 0.03, "P4": 0.12}

# Suppression
ALARM_SUPPRESSED_STATES = ("Down", "Maintenance", "Startup", "Shutdown")
ALARM_STANDBY_SUPPRESSED_TYPES = ("flow", "rpm")   # measurement types, not alarm types
ALARM_NEUTRAL_QUALITY = ("Bad", "Substituted", "Uncertain")
# Frozen detection is meaningless on a tag whose value is binary by construction: a flare
# pilot that stayed lit for 30 days is a working pilot, not a stuck transmitter.
FROZEN_EXCLUDED_TYPES = ("pilot_flame",)

# Sensor status
SENSOR_OFFLINE_MULTIPLE = 2.0    # gap longer than this many cadences is an outage
SENSOR_STATUSES = ("Online", "Offline", "Faulty", "Calibration", "Decommissioned")
SENSOR_CAUSES = ("Comms", "Power", "Hardware", "Calibration", "Planned", "Decommissioned")
SENSOR_UNPLANNED_CAUSE = {"Comms": 0.55, "Power": 0.25, "Hardware": 0.20}

# Validation bands
ALARM_RATE_PER_FACILITY_MONTH = (10.0, 60.0)

# --- validation -------------------------------------------------------------------------------
assert ALARM_DEBOUNCE_SAMPLES >= 1, "debounce must be at least one sample"
assert 0.0 < ALARM_DEADBAND_PCT < 0.5, "deadband must be a small share of the normal span"
assert set(ALARM_ACK_DELAY_MINUTES) == set(PRIORITIES) == set(ALARM_NEVER_ACK_SHARE)
for _p, (_lo, _hi) in ALARM_ACK_DELAY_MINUTES.items():
    assert 0 < _lo <= _hi, f"{_p}: bad acknowledgement delay range"
assert abs(sum(SENSOR_UNPLANNED_CAUSE.values()) - 1.0) < 1e-9
assert ALARM_FROZEN_SAMPLES >= ALARM_DEBOUNCE_SAMPLES, (
    "a Frozen alarm should need at least as much evidence as a process alarm"
)
assert set(SENSOR_UNPLANNED_CAUSE) <= set(SENSOR_CAUSES)
assert set(ALARM_SUPPRESSED_STATES) <= set(STATES)
assert set(ALARM_TRIP_PRIORITY) <= set(ALARM_TYPES)

# The scan is the WHOLE telemetry retention in both run modes -- see the boundary note above.
# A bounded lookback cannot reproduce a backfill, because the debounce machine's output
# depends on where each breach RUN began and any scan edge can land mid-run.
SCAN_START, SCAN_END = TEL_START, TEL_END
assert SCAN_START < SCAN_END

# run_mode controls the acknowledgement clock, not the scan.
ACK_AS_OF = WINDOW_END

print(f"RUN_MODE={RUN_MODE}  window={WINDOW_START.date()}..{WINDOW_END.date()} "
      f"({WINDOW_DAYS} days)")
print(f"telemetry available {TEL_START.date()}..{TEL_END.date()}")
print(f"scan {SCAN_START.date()}..{SCAN_END.date()} "
      f"({(SCAN_END - SCAN_START).days} days -- the full retention, in both run modes)")
print(f"acknowledgement clock anchored at {ACK_AS_OF.date()} (from run_mode, not utcnow)")
print(f"replaceWhere will cover date_sk {int(SCAN_START.strftime('%Y%m%d'))} .. "
      f"{int((SCAN_END - pd.Timedelta(days=1)).strftime('%Y%m%d'))} on both tables")
print()
print(f"debounce {ALARM_DEBOUNCE_SAMPLES} samples   deadband {ALARM_DEADBAND_PCT:.0%} of span"
      f"   chatter {ALARM_CHATTER_COUNT} raises / {ALARM_CHATTER_WINDOW_HOURS}h")
print(f"suppressed states: {', '.join(ALARM_SUPPRESSED_STATES)}"
      f"   Standby also suppresses {', '.join(ALARM_STANDBY_SUPPRESSED_TYPES)}")
print(f"neutral quality (neither raises nor clears): {', '.join(ALARM_NEUTRAL_QUALITY)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### The tag limit table
#
# One row per `(tag, alarm_type)` that the tag actually has a limit for. `alarm_lo` and
# friends are nullable per tag — vibration has no low trip, a flare header has no low-flow
# trip — and a null limit must produce no rows rather than a comparison against null.

# CELL ********************

tag_pdf = spark.table("dim_scada_tag").filter("is_current = true").toPandas()
eq_pdf = spark.table("dim_equipment").select(
    "equipment_sk", "equipment_id", "equipment_type", "criticality").toPandas()

assert (tag_pdf["topology_seed"] == TOPOLOGY_SEED).all(), (
    "dim_scada_tag was built with a different TOPOLOGY_SEED; rerun 01d_build_scada_tags"
)
tag_pdf = tag_pdf.merge(eq_pdf[["equipment_sk", "criticality", "equipment_type"]],
                        on="equipment_sk", how="left")
assert tag_pdf["criticality"].notna().all(), "a tag has no equipment criticality"

tag_pdf["span"] = tag_pdf["normal_max"] - tag_pdf["normal_min"]
tag_pdf["deadband"] = ALARM_DEADBAND_PCT * tag_pdf["span"]
tag_pdf["is_critical"] = tag_pdf["criticality"] == "Critical"

LIMIT_SPEC = [("HiHi", "alarm_hihi", 1), ("Hi", "alarm_hi", 1),
              ("Lo", "alarm_lo", 0), ("LoLo", "alarm_lolo", 0)]

rows = []
for _t, _col, _up in LIMIT_SPEC:
    sub = tag_pdf[tag_pdf[_col].notna()]
    for r in sub.itertuples():
        rows.append({
            "tag_sk": int(r.tag_sk), "alarm_type": _t,
            "limit_value": float(getattr(r, _col)), "is_upper": bool(_up),
            "priority": (ALARM_TRIP_PRIORITY.get(_t)
                         or (ALARM_WARN_PRIORITY["critical"] if r.is_critical
                             else ALARM_WARN_PRIORITY["other"])),
        })
limit_pdf = pd.DataFrame(rows)
limit_dim = F.broadcast(spark.createDataFrame(limit_pdf))

print(f"{len(tag_pdf):,} tags  ->  {len(limit_pdf):,} (tag, alarm_type) limit pairs")
for _t, _, _ in LIMIT_SPEC:
    n = int((limit_pdf["alarm_type"] == _t).sum())
    print(f"  {_t:<8}{n:>7,} tags carry this limit "
          f"({n/len(tag_pdf):.0%})")
print(f"  criticality mix: "
      + "  ".join(f"{k} {v:,}" for k, v in tag_pdf["criticality"].value_counts().items()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Where the alarms can come from — read this before trusting the rate
#
# Alarms are derived, so the achievable rate is a property of **the telemetry and the limits**,
# not of anything this notebook chooses. The table below is the diagnostic for that: it prints
# how far each limit sits from the tag's centre in units of the process standard deviation
# 02b actually generates (`PROCESS_SD_FRACTION x half_band`, currently `0.25 x half_band`).
#
# A limit at 3 sigma is breached by roughly 1 reading in 740. A limit at 7 sigma is breached by
# roughly 1 in 3e11 — which, against 15.5M readings, is never. If the printed median sits above
# about 4 sigma, essentially no alarm in the table comes from a process excursion, and the
# alarm rate is being set by state anchors and episodes instead. The validation below fails on
# the rate, and this cell is where to look when it does.

# CELL ********************

PROCESS_SD_FRACTION = TELEMETRY_PROCESS_SD_FRACTION   # from 01_topology_config, not 02b

diag = tag_pdf.copy()
diag["centre"] = 0.5 * (diag["normal_min"] + diag["normal_max"])
diag["sigma"] = PROCESS_SD_FRACTION * 0.5 * diag["span"]
for _t, _col, _up in LIMIT_SPEC:
    diag[f"z_{_t}"] = np.where(
        diag["sigma"] > 0,
        (diag[_col] - diag["centre"]) / diag["sigma"].replace(0, np.nan) * (1 if _up else -1),
        np.nan)

zcols = [f"z_{t}" for t, _, _ in LIMIT_SPEC]
print("alarm limits in units of the generated process sd, by equipment type and tag")
print(f"  {'equipment_type':<18}{'tag_name':<24}"
      + "".join(f"{t:>9}" for t, _, _ in LIMIT_SPEC))
print("  " + "-" * 78)
for (et, tn), g in diag.groupby(["equipment_type", "tag_name"], sort=True):
    vals = [g[c].mean() for c in zcols]
    print(f"  {et:<18}{tn:<24}"
          + "".join(f"{v:>9.1f}" if pd.notna(v) else f"{'-':>9}" for v in vals))

allz = diag[zcols].to_numpy().ravel()
allz = allz[~np.isnan(allz)]
Z_MEDIAN = float(np.median(allz))
print("  " + "-" * 78)
print(f"  limits sit {allz.min():.1f} to {allz.max():.1f} sigma out, median {Z_MEDIAN:.1f}")
if Z_MEDIAN > 4.0:
    print()
    print("  NOTE  at this distance the baseline process never reaches the limits. Every")
    print("        alarm below therefore comes from a STATE ANCHOR (a Standby compressor's")
    print("        discharge pressure relaxing to line pressure and tripping its Lo limits)")
    print("        or from an episode overlay -- not from process variance. If the alarm")
    print("        rate fails its band, the knob is the alarm limits in TAG_TEMPLATES")
    print("        (01_topology_config), not anything in this notebook.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Classify every reading
#
# Each reading gets one of three actions per `(tag, alarm_type)`:
#
# | action | meaning |
# |---|---|
# | `+1` breach | value is past the limit, and the reading is eligible to raise |
# | `-1` clear | value is back inside the limit by the deadband |
# | `0` neutral | inside the limit but within the deadband, or a quality/state suppression |
#
# Suppression by **state** is a `-1`, not a `0`: an asset going `Down` is a legitimate reason
# to drop an open alarm, and leaving it open through a maintenance outage is exactly the stale
# alarm that makes a real alarm list useless. Suppression by **quality** is a `0`: a Bad
# reading tells you nothing about the process either way, so it must not clear an alarm any
# more than it may raise one.
#
# The scan is narrowed to tags that breach *something*, which is what keeps the window
# functions off 15.5M rows. A tag that never crosses a limit cannot produce an alarm, so
# excluding it early changes no result.

# CELL ********************

tel = (spark.table(TELEMETRY_TABLE)
       .filter((F.col("reading_ts") >= F.lit(SCAN_START))
               & (F.col("reading_ts") < F.lit(SCAN_END)))
       .select("tag_sk", "tag_id", "equipment_sk", "area_sk", "facility_sk",
               "reading_ts", "value_num", "quality_code", "operating_state"))

tag_attr = F.broadcast(spark.createDataFrame(
    tag_pdf[["tag_sk", "measurement_type", "deadband", "sampling_interval_seconds",
             "status", "normal_min", "normal_max"]]))

tel = tel.join(tag_attr, "tag_sk", "inner")

# Eligibility is a property of the reading, before any limit is considered.
state_suppressed = F.col("operating_state").isin(*ALARM_SUPPRESSED_STATES)
standby_suppressed = ((F.col("operating_state") == "Standby")
                      & F.col("measurement_type").isin(*ALARM_STANDBY_SUPPRESSED_TYPES))
quality_neutral = F.col("quality_code").isin(*ALARM_NEUTRAL_QUALITY)

tel = (tel
       .withColumn("suppressed", state_suppressed | standby_suppressed)
       .withColumn("neutral", quality_neutral))

# --- process alarms: one row per (reading, alarm_type the tag carries) ------------------------
breach_up = F.col("value_num") > F.col("limit_value")
breach_dn = F.col("value_num") < F.col("limit_value")
clear_up = F.col("value_num") <= (F.col("limit_value") - F.col("deadband"))
clear_dn = F.col("value_num") >= (F.col("limit_value") + F.col("deadband"))

scan = (tel.join(limit_dim, "tag_sk", "inner")
        .withColumn("breach",
                    F.when(F.col("is_upper"), breach_up).otherwise(breach_dn))
        .withColumn("clear",
                    F.when(F.col("is_upper"), clear_up).otherwise(clear_dn))
        .withColumn("action",
                    F.when(F.col("suppressed"), F.lit(-1))
                     .when(F.col("neutral"), F.lit(0))
                     .when(F.col("breach"), F.lit(1))
                     .when(F.col("clear"), F.lit(-1))
                     .otherwise(F.lit(0))))

# Only tags that breach something can produce an alarm.
live_pairs = (scan.filter("action = 1").select("tag_sk", "alarm_type").distinct()).persist()
n_pairs = live_pairs.count()
print(f"(tag, alarm_type) pairs with at least one eligible breach: {n_pairs:,} "
      f"of {len(limit_pdf):,}")

scan = scan.join(F.broadcast(live_pairs), ["tag_sk", "alarm_type"], "inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Sessionise: debounce, deadband, and collapsing to alternating raise/clear
#
# Neutral readings are dropped — they neither raise nor clear, so removing them from the
# sequence is exactly what "extend whatever state is current" means. What remains alternates
# between runs of breach and runs of clear.
#
# A run of `ALARM_DEBOUNCE_SAMPLES` breaches produces a **raise candidate**, at the timestamp
# of the Nth reading. A run of that many clears produces a **clear candidate**. Candidates can
# repeat — a short clear run that never reaches the debounce count leaves the alarm open, and
# the next long breach run produces a second raise candidate while one is already open.
#
# Collapsing to the alternating subsequence fixes that in one window pass: keep a candidate
# only when it differs in kind from the previous one. That is the whole state machine, with no
# iteration and no ordering dependence beyond the window itself.

# CELL ********************

def sessionise(df, parts, debounce):
    """Raise/clear interval pairs from a +1/-1/0 action sequence.

    Returns one row per alarm with raised_ts and a nullable cleared_ts. Pure window
    functions -- the same input always gives the same output, which is what makes the
    lookback rescan reproduce stored rows exactly.
    """
    w = Window.partitionBy(*parts).orderBy("reading_ts")
    d = (df.filter("action <> 0")
           .withColumn("prev_action", F.lag("action").over(w))
           .withColumn("is_new_run",
                       (F.col("prev_action").isNull())
                       | (F.col("prev_action") != F.col("action")))
           .withColumn("run_id", F.sum(F.col("is_new_run").cast("int")).over(w)))

    wr = Window.partitionBy(*parts, "run_id").orderBy("reading_ts")
    cand = (d.withColumn("rn", F.row_number().over(wr))
             .filter(F.col("rn") == F.lit(debounce))
             .select(*parts, "reading_ts", "action"))

    wc = Window.partitionBy(*parts).orderBy("reading_ts")
    kept = (cand.withColumn("prev_kind", F.lag("action").over(wc))
                .filter(F.col("prev_kind").isNull()
                        | (F.col("prev_kind") != F.col("action"))))

    # every kept +1 pairs with the next kept -1; a trailing +1 is an open alarm
    return (kept.withColumn("next_ts", F.lead("reading_ts").over(wc))
                .withColumn("next_kind", F.lead("action").over(wc))
                .filter("action = 1")
                .withColumn("cleared_ts",
                            F.when(F.col("next_kind") == -1, F.col("next_ts")))
                .withColumnRenamed("reading_ts", "raised_ts")
                .select(*parts, "raised_ts", "cleared_ts"))


proc_alarms = sessionise(scan, ["tag_sk", "alarm_type"], ALARM_DEBOUNCE_SAMPLES).persist()
print(f"process alarms (Hi/HiHi/Lo/LoLo): {proc_alarms.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Quality and frozen alarms
#
# `BadQuality` uses the same machine with `Bad` as the breach condition. With 02b drawing its
# 0.5% Bad readings independently per row, `ALARM_DEBOUNCE_SAMPLES` consecutive Bad readings is
# about a 1-in-8-million event, so this type is expected to be **near empty**. That is reported
# rather than tuned away: it becomes a real population the moment 02b grows a correlated
# instrument fault — a transmitter stuck at the rail for an hour — and lowering the threshold
# now would only manufacture alarms out of independent noise.
#
# `Frozen` catches a tag holding one value, which 02b produces deliberately for ~0.3% of tags.
# Binary tags are excluded: a flare pilot reading 1 for thirty days is a working pilot, and
# only `Running` readings count, because a stopped asset's zero flow is constant on purpose.

# CELL ********************

wv = Window.partitionBy("tag_sk").orderBy("reading_ts")

qual = (tel.withColumn("action",
                       F.when(F.col("quality_code") == "Bad", F.lit(1))
                        .when(F.col("quality_code") == "Good", F.lit(-1))
                        .otherwise(F.lit(0)))
           .select("tag_sk", "reading_ts", "action"))
bad_alarms = (sessionise(qual, ["tag_sk"], ALARM_DEBOUNCE_SAMPLES)
              .withColumn("alarm_type", F.lit("BadQuality")))

frozen_src = (tel
              .filter(F.col("operating_state") == "Running")
              .filter(~F.col("measurement_type").isin(*FROZEN_EXCLUDED_TYPES))
              .withColumn("prev_v", F.lag("value_num").over(wv))
              .withColumn("action",
                          F.when(F.col("prev_v").isNull(), F.lit(0))
                           .when(F.col("value_num") == F.col("prev_v"), F.lit(1))
                           .otherwise(F.lit(-1)))
              .select("tag_sk", "reading_ts", "action"))
frozen_alarms = (sessionise(frozen_src, ["tag_sk"], ALARM_FROZEN_SAMPLES)
                 .withColumn("alarm_type", F.lit("Frozen")))

diag_alarms = bad_alarms.unionByName(frozen_alarms)
print(f"BadQuality alarms: {bad_alarms.count():,}   Frozen alarms: {frozen_alarms.count():,}")

all_alarms = (proc_alarms.select("tag_sk", "alarm_type", "raised_ts", "cleared_ts")
              .unionByName(diag_alarms.select("tag_sk", "alarm_type",
                                              "raised_ts", "cleared_ts"))).persist()
N_RAW_ALARMS = all_alarms.count()
print(f"total alarms before enrichment: {N_RAW_ALARMS:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Peak value, priority, keys, chattering and acknowledgement
#
# `alarm_sk` is `stable_key("alarm", tag_id, alarm_type, raised_ts)` — the same SHA-256
# construction `01_topology_config` uses, computed in Spark so the telemetry is never collected.
# Golden vectors guard the two implementations against drifting apart, as in 02b.
#
# **Acknowledgement is the one thing here that is not intrinsic to the telemetry**, and it is
# where §2.3 of `DESIGN_NOTE_incremental_facts.md` applies. The delay is drawn **once, from
# `(alarm_sk, seed)`, keyed on priority**, and never re-drawn; the run then acknowledges any
# alarm whose delay has elapsed against the window end. Re-running a day therefore produces
# identical acknowledgements, and a backfill matches thirty incremental runs, because nothing
# depends on when the notebook ran.

# CELL ********************

# 2**63, as a STRING cast to decimal -- never F.lit(9223372036854775808).
#
# stable_key in 01_topology_config reduces its 64-bit digest with `& 0x7FFF_FFFF_FFFF_FFFF`,
# and for a non-negative value that mask IS `% 2**63`. But 2**63 is exactly one past
# Long.MAX_VALUE, so Spark cannot build it as a long literal at all: F.lit(2**63) raises
# NumberFormatException while the expression is being constructed, before any data moves.
# Routing the constant through a decimal keeps the modulus exact and in a type that survives
# the round trip. conv() on 16 hex characters yields up to 20 digits, which decimal(20,0)
# holds exactly, and the result of the modulus is below 2**63 so the cast to long is safe.
SK_MODULUS = F.lit("9223372036854775808").cast("decimal(20,0)")


def sk_from_sha(*parts):
    """stable_key(*parts) from 01_topology_config, expressed in Spark.

    There is no existing Spark formulation to reuse: stable_key itself is pure Python, and
    01c/01d/02a call it in pandas. 02b's golden check passes because it compares HEX DIGESTS
    and its hash_uniform only ever takes 8 hex characters, which fit a double -- it never
    builds a 63-bit key in Spark, so its formulation does not transfer to a table whose
    surrogate key has to be derived in the engine.
    """
    s = F.concat_ws("|", F.lit(str(TOPOLOGY_SEED)), *parts)
    h = F.substring(F.sha2(s, 256), 1, 16)
    return (F.conv(h, 16, 10).cast("decimal(20,0)") % SK_MODULUS).cast("long")


def alarm_key(tag_id_col, alarm_type_col, ts_col):
    """stable_key("alarm", tag_id, alarm_type, raised_ts), in Spark.

    alarm_type is IN the key because the table's grain includes it. Hi and HiHi are
    independent debounce machines on the same tag -- validation section 4 asserts one open
    alarm per (tag, alarm_type), not per tag -- so a value that crosses both limits between
    one reading and the next raises two alarms at the SAME raised_ts. Without alarm_type the
    two collide and one is lost.

    That is not a corner case here. The dominant alarm source is a Standby interval, where
    02b relaxes a compressor's discharge pressure from ~1000 psig to 0.45 x centre in a
    single sample, straight past alarm_lo and alarm_lolo together.
    """
    return sk_from_sha(F.lit("alarm"), tag_id_col, alarm_type_col,
                       F.date_format(ts_col, "yyyy-MM-dd HH:mm:ss"))


_g = [("GS-0001.A1.PT-101", "Hi", "2026-08-20 13:45:00"),
      ("GS-0001.A1.PT-101", "HiHi", "2026-08-20 13:45:00"),   # same tag, same instant,
      ("GS-0042.A3.FT-104", "LoLo", "2026-09-01 00:00:00")]   # different type -> different key

# The vectors must EXERCISE the modulus, not just pass around it. A 16-hex-character digest
# is below 2**63 about half the time, and a set of vectors that all happened to land there
# would verify nothing about the reduction -- the broken F.lit(2**63) form would still have
# failed at construction, but a silently wrong modulus would not. So at least one vector is
# required to have a raw digest at or above 2**63, asserted here rather than hoped for.
_raw = [int(hashlib.sha256(f"{TOPOLOGY_SEED}|alarm|{t}|{k}|{s}".encode()).hexdigest()[:16], 16)
        for t, k, s in _g]
assert max(_raw) >= 2 ** 63, (
    "no golden vector has a digest at or above 2**63, so the 63-bit reduction in sk_from_sha "
    "is never exercised. Add a (tag_id, alarm_type, timestamp) vector whose first 16 hex "
    "digits start with 8-f."
)

# --- layer 1: the DIGEST ----------------------------------------------------------------------
# Checked before the numeric key, because the two fail for different reasons and want
# different fixes. A digest mismatch means Spark and Python are hashing DIFFERENT STRINGS --
# a null in concat_ws, a timestamp rendered differently, a changed seed. A digest match with
# a key mismatch means the string is right and the 63-bit REDUCTION is wrong, which is local
# arithmetic. Running them in this order turns "the key is wrong" into one of those two
# sentences instead of leaving both open.
#
# This layer is also the more robust of the two by construction: comparing hex does no
# arithmetic, so it cannot itself break the way the numeric check did when its modulus was
# expressed as a long literal Spark could not build.
_dig = [(t, k, s, hashlib.sha256(f"{TOPOLOGY_SEED}|alarm|{t}|{k}|{s}".encode()).hexdigest()[:16])
        for t, k, s in _g]
_bad_d = (spark.createDataFrame(_dig, "tag_id string, k string, ts string, expected string")
          .withColumn("actual", F.substring(F.sha2(F.concat_ws(
              "|", F.lit(str(TOPOLOGY_SEED)), F.lit("alarm"), F.col("tag_id"), F.col("k"),
              F.date_format(F.to_timestamp("ts"), "yyyy-MM-dd HH:mm:ss")), 256), 1, 16))
          .filter("actual <> expected").collect())
assert not _bad_d, (
    f"sha2 DIGEST mismatch: {_bad_d[0]}\n"
    "  Spark and Python are hashing different strings. The reduction is NOT implicated -- "
    "the numeric check below has not run. Look at concat_ws for a null argument (concat_ws "
    "skips nulls, so a null tag_id silently shortens the string), the timestamp format, and "
    "TOPOLOGY_SEED."
)
print("OK  sha2 digests agree -- Spark and Python hash the same string")

# --- layer 2: the 63-bit reduction --------------------------------------------------------------
_golden = [(t, k, s, stable_key("alarm", t, k, s)) for t, k, s in _g]
_chk = (spark.createDataFrame(_golden, "tag_id string, k string, ts string, expected long")
        .withColumn("actual", alarm_key(F.col("tag_id"), F.col("k"), F.to_timestamp("ts"))))
_bad = _chk.filter("actual <> expected").collect()
# the first two vectors differ only by alarm_type: if they collide, the key ignores it
assert _chk.select("actual").distinct().count() == len(_g), (
    "two golden vectors that differ only in alarm_type produced the same alarm_sk -- "
    "alarm_type is not reaching the key"
)
assert not _bad, (
    f"63-bit REDUCTION mismatch: {_bad[0]}\n"
    "  The digests already agreed, so the string being hashed is correct and only the "
    "reduction is wrong. stable_key uses `& 0x7FFF_FFFF_FFFF_FFFF`; sk_from_sha uses "
    "`% 2**63`, which is the same thing for a non-negative value. Check SK_MODULUS is still "
    "a decimal and not a long literal, and that conv() is reading all 16 hex characters."
)

# Same helper for event_sk. Both keys are now four-part, so this no longer adds arity
# coverage; it covers the other prefix and a timestamp passed in as a string, not formatted.
_ge = [("GS-0001.A1.PT-101", "2026-08-20 13:45:00", "Offline"),
       ("GS-0042.A3.FT-104", "2026-09-01 00:00:00", "Online")]
_golden_e = [(t, s, st, stable_key("sensor_event", t, s, st)) for t, s, st in _ge]
_bad_e = (spark.createDataFrame(
    _golden_e, "tag_id string, ts string, st string, expected long")
    .withColumn("actual", sk_from_sha(F.lit("sensor_event"), F.col("tag_id"),
                                      F.col("ts"), F.col("st")))
    .filter("actual <> expected").collect())
assert not _bad_e, (
    f"Spark's sk_from_sha disagrees with stable_key on the four-part event key: {_bad_e[0]}"
)

print(f"OK  sk_from_sha matches stable_key on {len(_golden) + len(_golden_e)} golden vectors "
      f"(4-part alarm_sk and 4-part event_sk)")
print(f"    max raw digest {max(_raw)} >= 2**63, so the 63-bit reduction is exercised")
print("    checked in two layers: digest first (is the string right?), then the reduction")

# --- peak value over each alarm's own interval ---------------------------------------------
# Joined on the tag and bounded by the alarm's own timestamps. The alarm table is small, so
# this is a broadcast join against a narrow projection of the scan rather than a range join
# over the whole telemetry.
open_end = F.coalesce(F.col("cleared_ts"), F.lit(SCAN_END))
peaks = (F.broadcast(all_alarms)
         .join(tel.select("tag_sk", "reading_ts", "value_num").alias("t"),
               (F.col("t.tag_sk") == all_alarms["tag_sk"])
               & (F.col("t.reading_ts") >= F.col("raised_ts"))
               & (F.col("t.reading_ts") < open_end), "left")
         .groupBy(all_alarms["tag_sk"], "alarm_type", "raised_ts")
         .agg(F.max("value_num").alias("v_max"), F.min("value_num").alias("v_min")))

alarms = (all_alarms.join(peaks, ["tag_sk", "alarm_type", "raised_ts"], "left")
          .join(F.broadcast(spark.createDataFrame(
              tag_pdf[["tag_sk", "tag_id", "equipment_sk", "area_sk", "facility_sk",
                       "is_critical"]])), "tag_sk", "inner")
          .join(limit_dim.select("tag_sk", "alarm_type", "limit_value", "is_upper",
                                 F.col("priority").alias("limit_priority")),
                ["tag_sk", "alarm_type"], "left"))

alarms = (alarms
          .withColumn("priority",
                      F.coalesce(F.col("limit_priority"), F.lit(ALARM_DIAGNOSTIC_PRIORITY)))
          .withColumn("peak_value",
                      F.when(F.col("is_upper"), F.col("v_max")).otherwise(F.col("v_min")))
          .withColumn("peak_value", F.coalesce(F.col("peak_value"), F.col("v_max")))
          .withColumn("threshold_value", F.col("limit_value"))
          .withColumn("alarm_sk", alarm_key(F.col("tag_id"), F.col("alarm_type"),
                                            F.col("raised_ts")))
          .withColumn("is_open", F.col("cleared_ts").isNull())
          .withColumn("duration_minutes",
                      F.when(F.col("cleared_ts").isNotNull(),
                             (F.unix_timestamp("cleared_ts")
                              - F.unix_timestamp("raised_ts")) / 60.0))
          .withColumn("date_sk", F.date_format("raised_ts", "yyyyMMdd").cast("long"))
          .withColumn("is_synthetic", F.lit(True)))

# --- chattering ---------------------------------------------------------------------------
# Counted both ways round the alarm, so every member of a burst is flagged, not just the
# ones that happen to come after the threshold is crossed.
_cw = ALARM_CHATTER_WINDOW_HOURS * 3600
alarms = alarms.withColumn("raised_unix", F.unix_timestamp("raised_ts"))
w_back = (Window.partitionBy("tag_sk").orderBy("raised_unix").rangeBetween(-_cw, 0))
w_fwd = (Window.partitionBy("tag_sk").orderBy("raised_unix").rangeBetween(0, _cw))
alarms = (alarms
          .withColumn("n_back", F.count("*").over(w_back))
          .withColumn("n_fwd", F.count("*").over(w_fwd))
          .withColumn("is_chattering",
                      (F.greatest(F.col("n_back"), F.col("n_fwd"))
                       > F.lit(ALARM_CHATTER_COUNT)))
          .drop("n_back", "n_fwd", "raised_unix"))

# --- acknowledgement, fixed at raise time ---------------------------------------------------
def _ack_u(chunk):
    h = F.sha2(F.concat_ws("|", F.lit(str(TOPOLOGY_SEED)), F.lit("ack"),
                           F.col("alarm_sk").cast("string")), 256)
    return (F.conv(F.substring(h, 8 * chunk + 1, 8), 16, 10).cast("double")
            + F.lit(0.5)) / F.lit(4294967296.0)


delay_lo = F.create_map(*[x for p in PRIORITIES
                          for x in (F.lit(p), F.lit(ALARM_ACK_DELAY_MINUTES[p][0]))])
delay_hi = F.create_map(*[x for p in PRIORITIES
                          for x in (F.lit(p), F.lit(ALARM_ACK_DELAY_MINUTES[p][1]))])
never_share = F.create_map(*[x for p in PRIORITIES
                             for x in (F.lit(p), F.lit(ALARM_NEVER_ACK_SHARE[p]))])

alarms = (alarms
          .withColumn("ack_delay_min",
                      delay_lo[F.col("priority")]
                      + (delay_hi[F.col("priority")] - delay_lo[F.col("priority")])
                      * _ack_u(0))
          .withColumn("never_ack", _ack_u(1) < never_share[F.col("priority")])
          .withColumn("ack_due_ts",
                      (F.unix_timestamp("raised_ts")
                       + F.col("ack_delay_min") * 60.0).cast("timestamp"))
          .withColumn("acknowledged_ts",
                      F.when(~F.col("never_ack")
                             & (F.col("ack_due_ts") <= F.lit(ACK_AS_OF)),
                             F.col("ack_due_ts"))))

ALARM_SCHEMA = ["alarm_sk", "tag_sk", "tag_id", "equipment_sk", "area_sk", "facility_sk",
                "alarm_type", "priority", "raised_ts", "cleared_ts", "acknowledged_ts",
                "peak_value", "threshold_value", "duration_minutes", "is_open",
                "is_chattering", "date_sk", "is_synthetic"]
alarm_out = alarms.select(*ALARM_SCHEMA).persist()
N_ALARMS = alarm_out.count()
print(f"alarms enriched: {N_ALARMS:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### `fact_sensor_status_event` — derived from gaps, so it agrees by construction
#
# A tag with no reading for more than `SENSOR_OFFLINE_MULTIPLE` cadences has gone offline; its
# next reading brings it back. Because both come from the telemetry itself, the "Sensors
# Offline (no data in last 8 hours)" tile is computable and **non-zero** — V1's generator
# emitted a row for every sensor at every slot, which made that KPI permanently zero and the
# whole panel decorative.
#
# The status a gap maps to is derived where it can be: a gap that coincides with a
# `Maintenance` interval on the asset is `Calibration`, a gap on a tag whose registry status is
# `Faulty` is `Faulty`, and everything else is `Offline`. The *cause* of an unplanned gap is
# the one thing the telemetry cannot tell us — a missing reading looks the same whether the
# radio or the power failed — so it is hash-derived from `(seed, tag_id, event_ts)`, which is
# a label rather than a fact and is reproducible like everything else here.

# CELL ********************

cad_dim = F.broadcast(spark.createDataFrame(
    tag_pdf[["tag_sk", "sampling_interval_seconds"]].rename(
        columns={"sampling_interval_seconds": "cad"})))

wg = Window.partitionBy("tag_sk").orderBy("reading_ts")
gaps = (spark.table(TELEMETRY_TABLE)
        .filter((F.col("reading_ts") >= F.lit(SCAN_START))
                & (F.col("reading_ts") < F.lit(SCAN_END)))
        .select("tag_sk", "tag_id", "equipment_sk", "facility_sk", "reading_ts")
        .withColumn("prev_ts", F.lag("reading_ts").over(wg))
        .filter(F.col("prev_ts").isNotNull())
        .join(cad_dim, "tag_sk")
        .withColumn("gap_s", F.unix_timestamp("reading_ts") - F.unix_timestamp("prev_ts"))
        .filter(F.col("gap_s") > F.lit(SENSOR_OFFLINE_MULTIPLE) * F.col("cad"))
        # the tag went dark one cadence after its last good reading
        .withColumn("off_ts", (F.unix_timestamp("prev_ts") + F.col("cad")).cast("timestamp"))
        .withColumn("back_ts", F.col("reading_ts"))
        .withColumn("off_hours",
                    (F.unix_timestamp("back_ts") - F.unix_timestamp("off_ts")) / 3600.0))

maint = F.broadcast(spark.table(STATE_TABLE)
                    .filter("state = 'Maintenance'")
                    .select(F.col("equipment_sk").alias("m_eq"),
                            F.col("start_ts").alias("m_start"),
                            F.coalesce(F.col("end_ts"), F.lit(AS_OF)).alias("m_end")))

status_dim = F.broadcast(spark.createDataFrame(
    tag_pdf[["tag_sk", "status"]].rename(columns={"status": "tag_status"})))

gaps = (gaps.join(status_dim, "tag_sk")
        .join(maint, (F.col("equipment_sk") == F.col("m_eq"))
              & (F.col("m_start") < F.col("back_ts"))
              & (F.col("m_end") > F.col("off_ts")), "left")
        .withColumn("in_maint", F.col("m_eq").isNotNull())
        .drop("m_eq", "m_start", "m_end")
        .dropDuplicates(["tag_sk", "off_ts"]))

_ch = F.sha2(F.concat_ws("|", F.lit(str(TOPOLOGY_SEED)), F.lit("sensor_cause"),
                         F.col("tag_id"), F.col("off_ts").cast("string")), 256)
_u = (F.conv(F.substring(_ch, 1, 8), 16, 10).cast("double") + F.lit(0.5)) / F.lit(4294967296.0)
_c1, _c2 = SENSOR_UNPLANNED_CAUSE["Comms"], SENSOR_UNPLANNED_CAUSE["Power"]

gaps = (gaps
        .withColumn("to_status",
                    F.when(F.col("in_maint"), F.lit("Calibration"))
                     .when(F.col("tag_status") == "Faulty", F.lit("Faulty"))
                     .otherwise(F.lit("Offline")))
        .withColumn("cause",
                    F.when(F.col("in_maint"), F.lit("Calibration"))
                     .when(F.col("tag_status") == "Faulty", F.lit("Hardware"))
                     .when(_u < F.lit(_c1), F.lit("Comms"))
                     .when(_u < F.lit(_c1 + _c2), F.lit("Power"))
                     .otherwise(F.lit("Hardware"))))

# one row leaving Online, one returning to it
leave = (gaps.select("tag_sk", "tag_id", "equipment_sk", "facility_sk",
                     F.col("off_ts").alias("event_ts"), F.lit("Online").alias("from_status"),
                     "to_status", "cause", F.col("off_hours").alias("duration_hours")))
back = (gaps.select("tag_sk", "tag_id", "equipment_sk", "facility_sk",
                    F.col("back_ts").alias("event_ts"),
                    F.col("to_status").alias("from_status"),
                    F.lit("Online").alias("to_status"), "cause",
                    F.lit(None).cast("double").alias("duration_hours")))

# Decommissioned tags emit nothing at all, so they have no gap to derive from. One event
# each, stamped at the scan start: dim_scada_tag carries no decommission date, and inventing
# one would be worse than saying "already gone when this window opened".
dec_pdf = tag_pdf[tag_pdf["status"] == "Decommissioned"][
    ["tag_sk", "tag_id", "equipment_sk", "facility_sk"]].copy()
dec_pdf["event_ts"] = SCAN_START
dec_pdf["from_status"] = "Online"
dec_pdf["to_status"] = "Decommissioned"
dec_pdf["cause"] = "Decommissioned"
dec_pdf["duration_hours"] = np.nan
decom = spark.createDataFrame(dec_pdf)

status_ev = (leave.unionByName(back).unionByName(decom)
             .withColumn("event_sk",
                         sk_from_sha(F.lit("sensor_event"), F.col("tag_id"),
                                     F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss"),
                                     F.col("to_status")))
             .withColumn("date_sk", F.date_format("event_ts", "yyyyMMdd").cast("long"))
             .withColumn("is_synthetic", F.lit(True)))

STATUS_SCHEMA = ["event_sk", "tag_sk", "tag_id", "equipment_sk", "facility_sk", "event_ts",
                 "from_status", "to_status", "cause", "duration_hours", "date_sk",
                 "is_synthetic"]
status_out = status_ev.select(*STATUS_SCHEMA).persist()
N_STATUS = status_out.count()
print(f"sensor status events: {N_STATUS:,}")
for r in (status_out.groupBy("to_status").count().orderBy(F.desc("count")).collect()):
    print(f"  -> {r['to_status']:<16}{r['count']:>8,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write both tables
#
# `replaceWhere` covers the whole retention, because the scan does. Every row in the range is
# regenerated from telemetry that this run did not modify, so the rewrite reproduces the
# previous content exactly wherever nothing upstream changed -- no read-back merge, and no
# partition left holding a row the current derivation would not produce.

# CELL ********************

SCAN_LO_SK = int(SCAN_START.strftime("%Y%m%d"))
SCAN_HI_SK = int((SCAN_END - pd.Timedelta(days=1)).strftime("%Y%m%d"))
PREDICATE = f"date_sk >= {SCAN_LO_SK} AND date_sk <= {SCAN_HI_SK}"


def write_fact(df, table, n_rows):
    if not table_exists(table):
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
           .partitionBy("date_sk").saveAsTable(table))
        print(f"{table} did not exist -- created with a partitioned overwrite "
              f"({n_rows:,} rows)")
        return
    (df.write.format("delta").mode("overwrite").option("replaceWhere", PREDICATE)
       .partitionBy("date_sk").saveAsTable(table))
    print(f"{table}: {n_rows:,} rows written (replaceWhere {PREDICATE})")


# --- surrogate keys must be unique AT THE SOURCE ------------------------------------------
# Checked here, before the write, rather than three sections into validation. A duplicate key
# is a defect in the key's definition, not in the data, and the diagnosis is far easier while
# the columns that should have been in the key are still to hand.
#
# The grain of each key has to match the grain of its table:
#   alarm_sk        (tag_id, alarm_type, raised_ts)  -- Hi and HiHi are independent machines
#                   on one tag and can raise on the same reading
#   event_sk        (tag_id, event_ts, to_status)    -- a tag leaves Online at one instant and
#                   returns at another; the two carry different to_status
def assert_key_unique(df, key, grain, table):
    n, d = df.count(), df.select(key).distinct().count()
    if n == d:
        return
    dupes = (df.groupBy(key).count().filter("count > 1")
             .join(df, key).orderBy(key).limit(6).collect())
    raise AssertionError(
        f"{table}: {n - d} duplicate {key} across {n:,} rows.\n"
        f"  The key's grain must match the table's, which is {grain}.\n"
        f"  First colliding rows:\n"
        + "\n".join(f"    {r.asDict()}" for r in dupes)
    )


assert_key_unique(alarm_out, "alarm_sk", "(tag_id, alarm_type, raised_ts)", ALARM_TABLE)
assert_key_unique(status_out, "event_sk", "(tag_id, event_ts, to_status)", STATUS_TABLE)

# ...and the natural keys behind them, so a collision is attributable to the hash rather than
# to two rows genuinely sharing a grain they should not.
for _df, _cols, _tbl in ((alarm_out, ["tag_id", "alarm_type", "raised_ts"], ALARM_TABLE),
                         (status_out, ["tag_id", "event_ts", "to_status"], STATUS_TABLE)):
    _n = _df.count()
    _d = _df.select(*_cols).distinct().count()
    assert _n == _d, (
        f"{_tbl}: {_n - _d} rows share a natural key {tuple(_cols)}. The generator is "
        "emitting two rows at one grain point -- this is upstream of the surrogate key."
    )
print(f"OK  alarm_sk and event_sk unique, and so are the natural keys behind them")

# Nothing outside the scan may be written, or replaceWhere rejects the batch.
_oos = alarm_out.filter(f"NOT ({PREDICATE})").count()
assert _oos == 0, f"{_oos} alarm(s) carry a date_sk outside the replaceWhere predicate"
_oos = status_out.filter(f"NOT ({PREDICATE})").count()
assert _oos == 0, f"{_oos} status event(s) carry a date_sk outside the replaceWhere predicate"

write_fact(alarm_out, ALARM_TABLE, N_ALARMS)
write_fact(status_out, STATUS_TABLE, N_STATUS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation — every check fails the run, none warns
#
# The first check is the one that matters: it re-reads the telemetry and proves that every
# alarm has the breaching readings behind it. That is what separates a derived alarm table
# from an invented one, and no other check here substitutes for it.

# CELL ********************

alarm_tbl = spark.table(ALARM_TABLE).filter(PREDICATE)
status_tbl = spark.table(STATUS_TABLE).filter(PREDICATE)

# --- 1. every alarm is backed by breaching readings -------------------------------------------
proc_only = alarm_tbl.filter(F.col("alarm_type").isin("HiHi", "Hi", "Lo", "LoLo"))
# Both sides of this join, and of the one in section 2, carry tag_sk. An unqualified tag_sk
# anywhere in the condition throws AMBIGUOUS_REFERENCE on Spark, so each side is aliased --
# a for the alarm, t for the telemetry -- and every column reference names its side.
backing = (F.broadcast(proc_only.select("alarm_sk", "tag_sk", "alarm_type", "raised_ts"))
           .join(limit_dim, ["tag_sk", "alarm_type"]).alias("a")
           .join(tel.select("tag_sk", F.col("reading_ts").alias("rts"), "value_num",
                            "quality_code", "operating_state", "suppressed", "neutral")
                 .alias("t"),
                 # BOTH time bounds belong in the join condition. A bound applied as a
                 # filter after the left join discards the unmatched row -- the alarm with
                 # no readings behind it, which is the failure this check exists to catch --
                 # and the check then passes on exactly that case.
                 (F.col("t.tag_sk") == F.col("a.tag_sk"))
                 & (F.col("t.rts") <= F.col("a.raised_ts"))
                 & (F.col("t.rts") >= F.expr(
                     f"a.raised_ts - INTERVAL {ALARM_DEBOUNCE_SAMPLES * 2} HOURS")), "left")
           .withColumn("breaching",
                       F.when(F.col("a.is_upper"),
                              F.col("t.value_num") > F.col("a.limit_value"))
                        .otherwise(F.col("t.value_num") < F.col("a.limit_value"))
                       & ~F.col("t.suppressed") & ~F.col("t.neutral"))
           .groupBy(F.col("a.alarm_sk").alias("alarm_sk"))
           # an unmatched alarm's only row is all-null, and sum() over nulls is null, not 0;
           # without the coalesce `n_breach < N` is null and the filter below drops it anyway
           .agg(F.coalesce(F.sum(F.col("breaching").cast("int")), F.lit(0)).alias("n_breach")))

short = backing.filter(F.col("n_breach") < ALARM_DEBOUNCE_SAMPLES).limit(5).collect()
assert not short, (
    f"{len(short)} alarm(s) have fewer than {ALARM_DEBOUNCE_SAMPLES} breaching readings "
    f"behind their raised_ts (alarm_sk, n_breach): "
    f"{[(r['alarm_sk'], r['n_breach']) for r in short]}. Alarms must be DERIVED from "
    "the telemetry -- a user drilling from the alarm into the trend has to find the excursion."
)
print(f"OK  every process alarm has >= {ALARM_DEBOUNCE_SAMPLES} breaching readings at or "
      "before raised_ts")

# --- 2. no alarm raised from a suppressed state or a non-raising quality -----------------------
raise_rows = (F.broadcast(alarm_tbl.select("alarm_sk", "tag_sk", "raised_ts", "alarm_type"))
              .alias("a")
              .join(tel.select("tag_sk", F.col("reading_ts").alias("rts"), "quality_code",
                               "operating_state").alias("t"),
                    (F.col("a.tag_sk") == F.col("t.tag_sk"))
                    & (F.col("t.rts") == F.col("a.raised_ts")),
                    "inner")
              # one copy of each column, so the checks below cannot hit the duplicate tag_sk
              .select(F.col("a.alarm_sk").alias("alarm_sk"),
                      F.col("a.alarm_type").alias("alarm_type"),
                      F.col("t.quality_code").alias("quality_code"),
                      F.col("t.operating_state").alias("operating_state")))
bad_state = raise_rows.filter(
    F.col("operating_state").isin("Down", "Maintenance")).limit(5).collect()
assert not bad_state, (
    f"alarm(s) raised on a Down or Maintenance reading: "
    f"{[(r['alarm_sk'], r['operating_state']) for r in bad_state]}"
)
bad_qual = raise_rows.filter(
    (F.col("quality_code").isin("Bad", "Substituted"))
    & (F.col("alarm_type") != "BadQuality")).limit(5).collect()
assert not bad_qual, (
    f"alarm(s) raised from a Bad or Substituted reading: "
    f"{[(r['alarm_sk'], r['quality_code']) for r in bad_qual]}"
)
print("OK  no alarm raised on a Down/Maintenance reading, or from Bad/Substituted quality")

# --- 3. timestamps, durations, keys -------------------------------------------------------------
assert alarm_tbl.filter("cleared_ts IS NOT NULL AND cleared_ts <= raised_ts").count() == 0, \
    "cleared_ts is not after raised_ts"
bad_dur = alarm_tbl.filter(
    "cleared_ts IS NOT NULL AND abs(duration_minutes - "
    "(unix_timestamp(cleared_ts) - unix_timestamp(raised_ts)) / 60.0) > 1e-6").count()
assert bad_dur == 0, f"{bad_dur} alarm(s) where duration_minutes disagrees with the timestamps"
assert alarm_tbl.filter("is_open AND duration_minutes IS NOT NULL").count() == 0, \
    "an open alarm carries a duration"
assert alarm_tbl.filter("NOT is_open AND cleared_ts IS NULL").count() == 0, \
    "a closed alarm has no cleared_ts"
dup = alarm_tbl.groupBy("alarm_sk").count().filter("count > 1").limit(5).collect()
assert not dup, f"alarm_sk is not unique: {[r['alarm_sk'] for r in dup]}"

known_tags = F.broadcast(spark.table("dim_scada_tag").select("tag_sk").distinct())
orphan = alarm_tbl.join(known_tags, "tag_sk", "left_anti").limit(5).collect()
assert not orphan, f"alarm tag_sk not in dim_scada_tag: {[r['tag_sk'] for r in orphan]}"
orphan = status_tbl.join(known_tags, "tag_sk", "left_anti").limit(5).collect()
assert not orphan, f"status tag_sk not in dim_scada_tag: {[r['tag_sk'] for r in orphan]}"

# --- 4. at most one open alarm per (tag, alarm_type) at any instant -----------------------------
wov = Window.partitionBy("tag_sk", "alarm_type").orderBy("raised_ts")
overlap = (alarm_tbl
           .withColumn("prev_end",
                       F.lag(F.coalesce(F.col("cleared_ts"), F.lit(SCAN_END))).over(wov))
           .filter(F.col("prev_end").isNotNull() & (F.col("raised_ts") < F.col("prev_end")))
           .limit(5).collect())
assert not overlap, (
    f"{len(overlap)} overlapping alarm(s) on the same (tag, alarm_type): "
    f"{[(r['tag_sk'], r['alarm_type'], str(r['raised_ts'])) for r in overlap]}"
)
print("OK  cleared_ts after raised_ts, duration agrees, alarm_sk unique, FKs resolve")
print("OK  never more than one open alarm per (tag, alarm_type) at any instant")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- 5. chattering share -----------------------------------------------------------------------
n_chat = alarm_tbl.filter("is_chattering").count()
chat_share = n_chat / max(N_ALARMS, 1)
assert chat_share < ALARM_CHATTER_MAX_SHARE, (
    f"{chat_share:.2%} of alarms are chattering, over the {ALARM_CHATTER_MAX_SHARE:.0%} cap. "
    f"Raise ALARM_DEBOUNCE_SAMPLES (now {ALARM_DEBOUNCE_SAMPLES}) or ALARM_DEADBAND_PCT "
    f"(now {ALARM_DEADBAND_PCT:.0%}) -- chatter means the hysteresis is too narrow for the "
    "noise on those tags."
)
print(f"OK  chattering share {chat_share:.2%} ({n_chat:,} alarms), under the "
      f"{ALARM_CHATTER_MAX_SHARE:.0%} cap")

# --- 6. sensor status agrees with the telemetry -------------------------------------------------
n_gaps = gaps.count()
n_off_events = status_tbl.filter("to_status <> 'Decommissioned' AND from_status = 'Online'").count()
assert n_off_events == n_gaps, (
    f"{n_off_events:,} offline-type events against {n_gaps:,} telemetry gaps over "
    f"{SENSOR_OFFLINE_MULTIPLE}x cadence -- every gap must produce exactly one event and "
    "every event must come from a gap"
)
n_return = status_tbl.filter("to_status = 'Online'").count()
assert n_return == n_gaps, (
    f"{n_return:,} return-to-Online events against {n_gaps:,} gaps -- each gap must close"
)
n_dec = status_tbl.filter("to_status = 'Decommissioned'").count()
assert n_dec == int((tag_pdf["status"] == "Decommissioned").sum()), \
    "decommissioned tag count does not match dim_scada_tag"
assert status_tbl.filter(~F.col("to_status").isin(*SENSOR_STATUSES)).count() == 0, \
    "unknown to_status"
assert status_tbl.filter(~F.col("cause").isin(*SENSOR_CAUSES)).count() == 0, "unknown cause"
print(f"OK  {n_gaps:,} telemetry gaps -> {n_off_events:,} offline events and "
      f"{n_return:,} returns; {n_dec} decommissioned")

# --- 7. the offline KPI is actually computable --------------------------------------------------
# The check V1 failed: sample instants across the window and count tags whose most recent
# reading is more than 8 hours old. If this is zero everywhere, "Sensors Offline" is
# decoration again.
KPI_HOURS = 8
probes = [SCAN_START + (SCAN_END - SCAN_START) * f for f in (0.15, 0.35, 0.55, 0.75, 0.95)]
offline_at = []
for p in probes:
    n = (status_tbl.filter((F.col("event_ts") <= F.lit(p))
                           & (F.col("to_status") != "Online"))
         .groupBy("tag_sk").agg(F.max("event_ts").alias("last_off"))
         .join(status_tbl.filter((F.col("event_ts") <= F.lit(p))
                                 & (F.col("to_status") == "Online"))
               .groupBy("tag_sk").agg(F.max("event_ts").alias("last_on")), "tag_sk", "left")
         .filter(F.col("last_on").isNull() | (F.col("last_on") < F.col("last_off")))
         .count())
    offline_at.append((p, n))
print(f"OK  tags showing offline at sampled instants (KPI horizon {KPI_HOURS}h):")
for p, n in offline_at:
    print(f"    {p:%Y-%m-%d %H:%M}   {n:>5,} tags   {n/len(tag_pdf):>7.3%} of the registry")
assert max(n for _, n in offline_at) > 0, (
    "no tag is ever offline at any sampled instant -- the Sensors Offline KPI would be "
    "permanently zero, which is the exact V1 defect this table exists to fix"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- 8. alarm rate -------------------------------------------------------------------------------
# Printed either way, asserted after. Raised alarms only -- open ones from the lookback are
# already counted in the partition they were raised in.
in_window = alarm_tbl.filter((F.col("raised_ts") >= F.lit(WINDOW_START))
                             & (F.col("raised_ts") < F.lit(WINDOW_END)))
n_win = in_window.count()
n_fac = int(tag_pdf["facility_sk"].nunique())
rate = n_win / max(n_fac, 1) * 30.0 / max(WINDOW_DAYS, 1)
lo_r, hi_r = ALARM_RATE_PER_FACILITY_MONTH

print(f"alarm rate: {n_win:,} alarms raised in {WINDOW_DAYS} day(s) across {n_fac} facilities")
print(f"            = {rate:.1f} per facility-month   (band {lo_r:.0f} - {hi_r:.0f})")
if not (lo_r <= rate <= hi_r):
    print()
    print("  The rate is out of band. This is a property of the INPUTS, not of 02d:")
    print(f"    - alarm limits sit a median {Z_MEDIAN:.1f} sigma from centre "
          "(see the diagnostic cell)")
    print("    - at over ~4 sigma the baseline process never reaches them, so alarms come")
    print("      only from state anchors and episode overlays")
    print("    - knobs, in order of directness:")
    print("        TAG_TEMPLATES alarm_lo/hi/lolo/hihi in 01_topology_config (then rerun 01d, 02b)")
    print("        PROCESS_SD_FRACTION in 02b (widens the process; breaks 02b's in-band check)")
    print("        the episode generator, which does not exist yet -- fact_emission_episode")
assert lo_r <= rate <= hi_r, (
    f"alarm rate {rate:.1f} per facility-month is outside the {lo_r:.0f}-{hi_r:.0f} band. "
    "See the printed diagnosis directly above for the cause and the knobs."
)
print(f"OK  alarm rate {rate:.1f} per facility-month is inside the band")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Distributions

# CELL ********************

print("alarms by type and priority:")
piv = (alarm_tbl.groupBy("alarm_type").pivot("priority", list(PRIORITIES))
       .count().toPandas().fillna(0).set_index("alarm_type"))
print(f"  {'alarm_type':<14}" + "".join(f"{p:>9}" for p in PRIORITIES) + f"{'total':>9}")
print("  " + "-" * 62)
for t in ALARM_TYPES:
    if t not in piv.index:
        continue
    row = [int(piv.loc[t, p]) if p in piv.columns else 0 for p in PRIORITIES]
    print(f"  {t:<14}" + "".join(f"{v:>9,}" for v in row) + f"{sum(row):>9,}")

print()
print("mean duration and acknowledgement lag by priority:")
stats = (alarm_tbl.groupBy("priority")
         .agg(F.count("*").alias("n"),
              F.avg("duration_minutes").alias("mean_dur"),
              F.sum(F.col("is_open").cast("int")).alias("open"),
              F.sum(F.when(F.col("acknowledged_ts").isNotNull(), 1).otherwise(0)).alias("ackd"),
              F.avg(F.when(F.col("acknowledged_ts").isNotNull(),
                           (F.unix_timestamp("acknowledged_ts")
                            - F.unix_timestamp("raised_ts")) / 60.0)).alias("mean_ack"))
         .toPandas().set_index("priority"))
print(f"  {'priority':<10}{'alarms':>9}{'open':>8}{'mean dur (min)':>17}"
      f"{'ack%':>8}{'mean ack lag (min)':>21}")
print("  " + "-" * 73)
for p in PRIORITIES:
    if p not in stats.index:
        continue
    r = stats.loc[p]
    md = r["mean_dur"] if pd.notna(r["mean_dur"]) else 0.0
    ma = r["mean_ack"] if pd.notna(r["mean_ack"]) else 0.0
    print(f"  {p:<10}{int(r['n']):>9,}{int(r['open']):>8,}{md:>17,.1f}"
          f"{r['ackd']/max(r['n'],1):>8.0%}{ma:>21,.1f}")

print()
print("top 10 facilities by alarm count:")
top = (alarm_tbl.groupBy("facility_sk").agg(F.count("*").alias("n"))
       .orderBy(F.desc("n")).limit(10).toPandas())
fac_name = tag_pdf.drop_duplicates("facility_sk").set_index("facility_sk")["facility_id"].to_dict()
print(f"  {'facility':<14}{'alarms':>9}{'per month':>12}")
for r in top.itertuples():
    print(f"  {fac_name.get(int(r.facility_sk), r.facility_sk):<14}{int(r.n):>9,}"
          f"{int(r.n) * 30.0 / max(WINDOW_DAYS, 1):>12,.1f}")

print()
print("alarms per facility (distribution):")
per_fac = (alarm_tbl.groupBy("facility_sk").count().toPandas()["count"]
           if N_ALARMS else pd.Series(dtype="int64"))
if len(per_fac):
    print(f"  facilities with at least one alarm: {len(per_fac):,} of {n_fac:,}")
    print(f"  min {per_fac.min()}   median {int(per_fac.median())}   "
          f"mean {per_fac.mean():.1f}   max {per_fac.max()}")

print()
print("sensor status events by cause:")
for r in status_tbl.groupBy("cause").count().orderBy(F.desc("count")).collect():
    print(f"  {r['cause']:<16}{r['count']:>8,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(alarm_tbl.orderBy(F.desc("raised_ts")).limit(25))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary

# CELL ********************

print("=" * 76)
print("SCADA ALARMS AND SENSOR STATUS DERIVED")
print("=" * 76)
print(f"  run mode          {RUN_MODE}")
print(f"  window            {WINDOW_START.date()} .. {WINDOW_END.date()} ({WINDOW_DAYS} days)")
print(f"  scan              {SCAN_START.date()} .. {SCAN_END.date()} "
      f"(full retention, both run modes)")
print(f"  ack clock         {ACK_AS_OF.date()}   (what run_mode actually controls)")
print(f"  alarms written    {N_ALARMS:,}   ({n_win:,} raised inside the window)")
print(f"  alarm rate        {rate:.1f} per facility-month  "
      f"(band {lo_r:.0f}-{hi_r:.0f})")
print(f"  chattering        {chat_share:.2%}  (cap {ALARM_CHATTER_MAX_SHARE:.0%})")
print(f"  open alarms       {alarm_tbl.filter('is_open').count():,}")
print(f"  acknowledged      {alarm_tbl.filter('acknowledged_ts IS NOT NULL').count():,}")
print(f"  status events     {N_STATUS:,}   from {n_gaps:,} telemetry gaps")
print(f"  partitions        date_sk {SCAN_LO_SK} .. {SCAN_HI_SK} on both tables")
print()
print("  tables written    fact_scada_alarm, fact_sensor_status_event")
print("  not modified      scada_telemetry, fact_asset_state, every dim_* table")
print("  not computed      rollups -- that is 02c")
print()
print("  every alarm is derived: each one has at least "
      f"{ALARM_DEBOUNCE_SAMPLES} breaching readings behind it")
print("  in scada_telemetry, asserted above. There is no independent alarm stream.")
print()
print("  determinism       alarms are a pure function of the telemetry, and the")
print("                    acknowledgement delay is fixed at raise time from (alarm_sk,")
print("                    seed) -- so a rerun, and a backfill against 30 incremental")
print("                    runs, produce identical tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
