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

# # 02a — Build Asset Operating State
#
# Writes **`fact_asset_state`**: a **sparse interval table**, one row per state change per
# asset. Never one row per timestamp — an asset that runs untouched for three weeks is one
# row, not 2,016.
#
# This is the first notebook governed by `DESIGN_NOTE_incremental_facts.md`, and the
# two-pass structure in §2.3 is a requirement here, not a suggestion.
#
# ### State is generated from asset characteristics only
# Type, age against expected life, leak propensity, inspection interval. This notebook does
# **not** read `fact_emission_episode`, `gold_plume_catalog` or any detection-layer table.
# The telemetry generator overlays episode effects separately. Keeping the two independent
# is what stops the demo becoming circular — otherwise the operational data "discovers" an
# episode that was written into it in the first place.
#
# ### The state machine
# ```
# Running --PM due-----> Shutdown(Scheduled PM) -> Maintenance(Scheduled PM) -> Startup -> Running
# Running --corrective-> Shutdown(Corrective)   -> Maintenance(Corrective)   -> Startup -> Running
# Running --market-----> Shutdown(Market)       -> Standby(Market)           -> Startup -> Running
# Running --trip-------> Down(Trip)             -> Maintenance(Corrective)   -> Startup -> Running
#                                               \-> Startup -> Running          (spurious trip)
# ```
# A trip goes straight to `Down` with no `Shutdown` — that is what distinguishes it from a
# planned stop. Every chain returns through `Startup`, so `Startup` only ever precedes
# `Running` and `Shutdown` only ever follows it.
#
# ### Why re-running a day is safe
# Every interval's duration is drawn **once, at the moment it opens**, from
# `get_rng("state", equipment_id, start_ts)`. Closure is then a pure function of elapsed time
# against that fixed duration — never a fresh draw on the day. The whole chain for an asset
# is therefore determined by its first interval, which is why a backfill and the equivalent
# sequence of incremental runs produce byte-identical tables.
#
# ### Writes
# `fact_asset_state` only, partitioned by `date_sk`, with `replaceWhere` scoped to the
# affected partitions. No `dim_*` table is modified. No telemetry, alarms or maintenance
# records — those are later notebooks.

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

# ### Run mode
#
# `run_mode` follows the convention in the archived V1 notebooks: `getArgument` wrapped in a
# try/except so the notebook also runs interactively, with optional `start_date` / `end_date`
# overrides that take precedence.
#
# What differs from V1: the incremental window is **not** anchored to `utcnow()`. V1 used the
# wall clock, which makes a rerun on a different day produce a different window and breaks
# reproducibility. Here the window comes from the table's own watermark — the day after the
# latest `date_sk` already written — so a rerun lands on the same window regardless of when
# it happens.

# CELL ********************

TABLE = "fact_asset_state"

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


def table_exists(name):
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False


if RUN_MODE == "backfill":
    WINDOW_START, WINDOW_END = HISTORY_START, AS_OF
else:
    # Watermark: resume the day after the last day already written. Falls back to a
    # one-day window ending at the as-of date when the table is empty or absent.
    wm = None
    if table_exists(TABLE):
        row = spark.sql(f"SELECT max(date_sk) AS m FROM {TABLE}").collect()[0]
        wm = row["m"]
    if wm is None:
        WINDOW_START, WINDOW_END = AS_OF - pd.Timedelta(days=1), AS_OF
        print(f"{TABLE} is empty or absent -- incremental falls back to a one-day window. "
              "Run a backfill first for a populated history.")
    else:
        WINDOW_START = pd.Timestamp(str(int(wm))) + pd.Timedelta(days=1)
        WINDOW_END = WINDOW_START + pd.Timedelta(days=1)

if _start_override:
    WINDOW_START = pd.Timestamp(_start_override)
if _end_override:
    WINDOW_END = pd.Timestamp(_end_override)

assert WINDOW_START <= WINDOW_END, f"empty window: {WINDOW_START} .. {WINDOW_END}"
assert WINDOW_START >= HISTORY_START, (
    f"window starts {WINDOW_START.date()}, before the {STATE_HISTORY_DAYS}-day history at "
    f"{HISTORY_START.date()}. State before that point was never generated, so intervals "
    "would not tile continuously."
)

WINDOW_DAYS = max(1, (WINDOW_END - WINDOW_START).days)

print(f"RUN_MODE={RUN_MODE}  window={WINDOW_START.date()}..{WINDOW_END.date()} "
      f"({WINDOW_DAYS} days)")
print(f"history anchor {HISTORY_START.date()}  as-of {AS_OF.date()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eq_pdf = (spark.table("dim_equipment").toPandas()
               .sort_values("equipment_sk").reset_index(drop=True))
area_pdf = spark.table("dim_area").filter("is_current = true").toPandas()
fac_pdf = spark.table("dim_facility").filter("is_current = true").toPandas()

assert (eq_pdf["topology_seed"] == TOPOLOGY_SEED).all(), (
    "dim_equipment was built with a different TOPOLOGY_SEED; rerun 01a-01c"
)
assert "area_sk" in eq_pdf.columns, "dim_equipment has no area_sk -- run 01c first"

eq_pdf["install_date"] = pd.to_datetime(eq_pdf["install_date"])
eq_pdf["age_years_at_asof"] = (AS_OF - eq_pdf["install_date"]).dt.days / 365.25

print(f"{len(eq_pdf):,} assets, {len(area_pdf)} areas, {len(fac_pdf)} facilities")
print(f"asset age at as-of: min {eq_pdf['age_years_at_asof'].min():.1f}y  "
      f"median {eq_pdf['age_years_at_asof'].median():.1f}y  "
      f"max {eq_pdf['age_years_at_asof'].max():.1f}y")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### The state machine
#
# `step()` is the whole model. Given an asset and the interval currently open, it returns
# that interval's duration and what follows. It is a **pure function** of
# `(asset, state, cause, start_ts, seed)` — call it twice with the same arguments and it
# returns the same answer, which is the property the whole idempotency argument rests on.

# CELL ********************

def pm_phase_days(equipment_id, insp_days):
    """Per-asset offset into the PM cycle, so PMs are not all due on the same day."""
    return float(get_rng("pm_phase", equipment_id).uniform(0.0, insp_days))


def next_pm_after(ts, install_ts, insp_days, phase_days):
    """First scheduled PM strictly after ts, on the asset's own inspection calendar."""
    anchor = install_ts + pd.Timedelta(days=phase_days)
    if ts < anchor:
        return anchor
    elapsed = (ts - anchor) / pd.Timedelta(days=insp_days)
    return anchor + pd.Timedelta(days=insp_days * (np.floor(elapsed) + 1))


def step(asset, state, cause, start_ts):
    """Duration of the interval opening now, and the state/cause that follows it.

    Pure: same inputs, same outputs, always. The RNG is keyed on the interval's own
    identity, so a duration decided when the interval opened is recoverable on any later
    run without being re-drawn.
    """
    rng = get_rng("state", asset.equipment_id, state, start_ts.isoformat())

    if state == "Running":
        # Two competing clocks: the PM calendar, and an exponential failure draw.
        # Whichever comes first ends the Running interval, and decides the cause.
        insp = int(asset.inspection_frequency_days)
        t_pm = next_pm_after(start_ts, asset.install_date, insp, asset.pm_phase)
        mtbf = mtbf_days(asset.equipment_type, asset.age_years)
        t_fail = start_ts + pd.Timedelta(days=float(rng.exponential(mtbf)))

        if t_pm <= t_fail:
            return (t_pm - start_ts), "Shutdown", "Scheduled PM"

        names = list(UNPLANNED_CAUSE_WEIGHTS)
        picked = str(rng.choice(names, p=[UNPLANNED_CAUSE_WEIGHTS[n] for n in names]))
        # A trip is unplanned and instantaneous -- it goes straight to Down, with no
        # Shutdown. Everything else is a controlled stop and gets one.
        nxt = "Down" if picked == "Trip" else "Shutdown"
        return (t_fail - start_ts), nxt, picked

    if state == "Shutdown":
        lo, hi = STATE_DWELL_HOURS["Shutdown"]
        dur = pd.Timedelta(hours=float(rng.uniform(lo, hi)))
        nxt = "Standby" if cause == "Market" else "Maintenance"
        return dur, nxt, cause

    if state == "Down":
        lo, hi = STATE_DWELL_HOURS["Down"]
        dur = pd.Timedelta(hours=float(rng.uniform(lo, hi)))
        # Some trips clear on inspection with no repair -- straight back to Startup.
        if rng.random() < SPURIOUS_TRIP_SHARE:
            return dur, "Startup", cause
        return dur, "Maintenance", "Corrective"

    if state == "Maintenance":
        lo, hi = MAINTENANCE_DWELL_HOURS.get(cause, MAINTENANCE_DWELL_HOURS["Corrective"])
        return pd.Timedelta(hours=float(rng.uniform(lo, hi))), "Startup", cause

    if state == "Standby":
        lo, hi = STATE_DWELL_HOURS["Standby"]
        return pd.Timedelta(hours=float(rng.uniform(lo, hi))), "Startup", cause

    if state == "Startup":
        lo, hi = STATE_DWELL_HOURS["Startup"]
        # Startup always lands in Running, and Running carries no cause.
        return pd.Timedelta(hours=float(rng.uniform(lo, hi))), "Running", None

    raise ValueError(f"unknown state {state!r}")


def simulate(asset, state, cause, start_ts, until_ts):
    """Walk the chain from (state, start_ts) until an interval spans until_ts.

    Returns closed intervals plus the single open one. The open interval is the one whose
    start is before until_ts and whose end is at or after it.
    """
    out = []
    while True:
        dur, nxt_state, nxt_cause = step(asset, state, cause, start_ts)
        end_ts = start_ts + dur
        if end_ts >= until_ts:
            out.append((state, cause, start_ts, None))      # open
            return out
        out.append((state, cause, start_ts, end_ts))
        state, cause, start_ts = nxt_state, nxt_cause, end_ts


print("state machine defined -- step() is pure in (asset, state, cause, start_ts, seed)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Pass 1 — advance, and Pass 2 — create
#
# **Backfill** seeds every asset at `Running` from the history anchor and walks forward to
# the window end.
#
# **Incremental** reads the open interval per asset, recomputes its fixed duration, closes it
# if it has elapsed, and continues the chain — then opens whatever the window calls for. The
# two produce the same rows because `step()` gives the same answer either way.

# CELL ********************

class AssetView:
    """The asset fields step() needs, as attributes, so the state machine never touches a
    DataFrame row and cannot accidentally depend on column order."""
    __slots__ = ("equipment_id", "equipment_sk", "equipment_type", "area_sk", "facility_sk",
                 "install_date", "inspection_frequency_days", "age_years", "pm_phase")

    def __init__(self, r):
        self.equipment_id = r.equipment_id
        self.equipment_sk = int(r.equipment_sk)
        self.equipment_type = r.equipment_type
        self.area_sk = int(r.area_sk)
        self.facility_sk = int(r.facility_sk)
        self.install_date = pd.Timestamp(r.install_date)
        self.inspection_frequency_days = int(r.inspection_frequency_days)
        self.age_years = float(r.age_years_at_asof)
        self.pm_phase = pm_phase_days(r.equipment_id, int(r.inspection_frequency_days))


assets = [AssetView(r) for r in eq_pdf.itertuples()]

# --- Pass 1: what is already open? --------------------------------------------------------
open_by_asset = {}
if RUN_MODE == "incremental" and table_exists(TABLE):
    open_pdf = (spark.table(TABLE).filter("is_open = true").toPandas())
    for r in open_pdf.itertuples():
        open_by_asset[r.equipment_id] = (r.state, r.cause, pd.Timestamp(r.start_ts))
    print(f"pass 1: {len(open_by_asset):,} open intervals read from {TABLE}")
else:
    print(f"pass 1: no prior state -- every asset seeds at Running from "
          f"{HISTORY_START.date()}")

rows = []
for a in assets:
    if a.equipment_id in open_by_asset:
        state, cause, start_ts = open_by_asset[a.equipment_id]
        cause = None if (cause is None or pd.isna(cause)) else cause
    else:
        # Seed. An asset installed after the history anchor starts at its install date.
        state, cause = "Running", None
        start_ts = max(HISTORY_START, a.install_date)

    for st, cs, s_ts, e_ts in simulate(a, state, cause, start_ts, WINDOW_END):
        rows.append({
            "equipment_sk": a.equipment_sk,
            "equipment_id": a.equipment_id,
            "area_sk": a.area_sk,
            "facility_sk": a.facility_sk,
            "state": st,
            "cause": cs,
            "start_ts": s_ts,
            "end_ts": e_ts,
        })

state_pdf = pd.DataFrame(rows)
state_pdf["state_sk"] = [
    stable_key("state", e, s.isoformat())
    for e, s in zip(state_pdf["equipment_id"], state_pdf["start_ts"])
]
state_pdf["is_open"] = state_pdf["end_ts"].isna()
state_pdf["duration_hours"] = (
    (state_pdf["end_ts"] - state_pdf["start_ts"]).dt.total_seconds() / 3600.0
)
state_pdf["date_sk"] = state_pdf["start_ts"].dt.strftime("%Y%m%d").astype("int64")
state_pdf["is_synthetic"] = True

print(f"pass 2: {len(state_pdf):,} intervals generated "
      f"({int(state_pdf['is_open'].sum()):,} open, "
      f"{int((~state_pdf['is_open']).sum()):,} closed)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### The `date_sk` wrinkle
#
# `date_sk` is the day the interval **started**. An interval that started before the window
# and closes inside it therefore lives in a partition *outside* the window — and this run
# rewrites that row, because `end_ts`, `duration_hours` and `is_open` all change.
#
# **Chosen: widen the `replaceWhere` predicate to cover every partition this run touches**,
# rather than adding a `close_date_sk` column.
#
# Why. `replaceWhere` can only be scoped on partition columns, so a `close_date_sk` would
# only help if the table were partitioned by it too — and partitioning by two date columns
# for one interval means every row lands in a partition that does not contain its own start.
# One partition column with one obvious meaning is easier to reason about and easier to
# query: "intervals that began on day X" is the natural grain.
#
# The cost is that `replaceWhere` must cover data the run did not change, so those rows have
# to be re-read and re-written unchanged. That is handled explicitly below: existing rows in
# the affected range are read, the ones this run supersedes are dropped by `state_sk`, and
# the union is written back. Skipping that step would silently delete them.

# CELL ********************

new_sks = set(state_pdf["state_sk"])
affected_lo = int(state_pdf["date_sk"].min())
affected_hi = int(WINDOW_END.strftime("%Y%m%d"))

carried = 0
if RUN_MODE == "incremental" and table_exists(TABLE):
    existing = (spark.table(TABLE)
                     .filter(f"date_sk >= {affected_lo} AND date_sk <= {affected_hi}")
                     .toPandas())
    if len(existing):
        keep = existing[~existing["state_sk"].isin(new_sks)]
        carried = len(keep)
        if carried:
            state_pdf = pd.concat([keep, state_pdf], ignore_index=True)

print(f"replaceWhere range: date_sk {affected_lo} .. {affected_hi}")
print(f"  rows this run generated : {len(new_sks):,}")
print(f"  rows carried unchanged  : {carried:,}")
print(f"  rows to write           : {len(state_pdf):,}")
if affected_lo < int(WINDOW_START.strftime("%Y%m%d")):
    print(f"  NOTE  the range reaches back past the window start "
          f"({WINDOW_START.strftime('%Y%m%d')}) because open intervals began earlier.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation — every check fails the run, none warns

# CELL ********************

sdf = state_pdf.sort_values(["equipment_id", "start_ts"], kind="mergesort").reset_index(drop=True)

# --- keys and nulls ---------------------------------------------------------------------------
assert sdf["state_sk"].is_unique, (
    "state_sk is not unique -- two intervals share (equipment_id, start_ts)"
)
for c in ("equipment_sk", "equipment_id", "area_sk", "facility_sk", "state", "start_ts",
          "date_sk", "is_open", "is_synthetic"):
    assert sdf[c].notna().all(), f"nulls in {c}"
assert set(sdf["state"]) <= set(STATES), f"unknown state(s): {set(sdf['state']) - set(STATES)}"
causes = set(sdf["cause"].dropna())
assert causes <= set(STATE_CAUSES), f"unknown cause(s): {causes - set(STATE_CAUSES)}"
assert sdf[sdf["state"] == "Running"]["cause"].isna().all(), "Running intervals carry a cause"

# --- FK ---------------------------------------------------------------------------------------
unknown = set(sdf["equipment_sk"]) - set(eq_pdf["equipment_sk"])
assert not unknown, f"equipment_sk not in dim_equipment: {sorted(unknown)[:5]}"

# --- timestamps ---------------------------------------------------------------------------------
closed = sdf[sdf["end_ts"].notna()]
assert (closed["end_ts"] > closed["start_ts"]).all(), "end_ts not after start_ts"
recomputed = (closed["end_ts"] - closed["start_ts"]).dt.total_seconds() / 3600.0
assert np.allclose(recomputed.values, closed["duration_hours"].values, atol=1e-9), \
    "duration_hours disagrees with the timestamps"
assert sdf[sdf["is_open"]]["duration_hours"].isna().all(), "open intervals carry a duration"
assert sdf[sdf["is_open"]]["end_ts"].isna().all(), "open intervals carry an end_ts"
assert (sdf["start_ts"] <= AS_OF).all(), (
    f"{int((sdf['start_ts'] > AS_OF).sum())} interval(s) start after the as-of date"
)

# --- tiling: no overlaps, no gaps ------------------------------------------------------------------
gaps, overlaps, multi_open = [], [], []
for eid, g in sdf.groupby("equipment_id", sort=False):
    g = g.sort_values("start_ts", kind="mergesort")
    ends = g["end_ts"].values[:-1]
    starts = g["start_ts"].values[1:]
    if len(g) > 1:
        d = (pd.to_datetime(starts) - pd.to_datetime(ends)).total_seconds()
        if np.any(d > 1e-6):
            gaps.append((eid, int((d > 1e-6).sum())))
        if np.any(d < -1e-6):
            overlaps.append((eid, int((d < -1e-6).sum())))
    n_open = int(g["is_open"].sum())
    if n_open != 1:
        multi_open.append((eid, n_open))

assert not overlaps, f"overlapping intervals for {len(overlaps)} asset(s): {overlaps[:5]}"
assert not gaps, f"gaps between intervals for {len(gaps)} asset(s): {gaps[:5]}"
assert not multi_open, (
    f"{len(multi_open)} asset(s) without exactly one open interval: {multi_open[:5]}"
)
assert sdf.groupby("equipment_id")["is_open"].sum().eq(1).all(), \
    "not every asset has exactly one open interval"

# --- transitional states -------------------------------------------------------------------------
bad_startup, bad_shutdown, freestanding = [], [], []
for eid, g in sdf.groupby("equipment_id", sort=False):
    seq = g.sort_values("start_ts", kind="mergesort")["state"].tolist()
    for i, st in enumerate(seq):
        nxt = seq[i + 1] if i + 1 < len(seq) else None
        prv = seq[i - 1] if i > 0 else None
        if st == "Startup":
            if nxt is not None and nxt != "Running":
                bad_startup.append((eid, i, nxt))
            if prv is None and len(seq) > 1:
                freestanding.append((eid, "Startup at sequence head"))
        if st == "Shutdown":
            if prv is not None and prv != "Running":
                bad_shutdown.append((eid, i, prv))
            if nxt is None and len(seq) > 1:
                freestanding.append((eid, "Shutdown at sequence tail with no successor"))

assert not bad_startup, f"Startup followed by something other than Running: {bad_startup[:5]}"
assert not bad_shutdown, f"Shutdown preceded by something other than Running: {bad_shutdown[:5]}"

# --- volume -----------------------------------------------------------------------------------------
# Churn is intervals OPENED IN THE WINDOW, not rows written. On an incremental run the write
# also carries rows from earlier partitions that the widened replaceWhere range sweeps in
# (see the date_sk note above); counting those would scale a one-day run's carried history
# up by 30 and report churn that is not happening.
opened_in_window = sdf[sdf["start_ts"] >= WINDOW_START]
rows_per_30d = len(opened_in_window) * 30.0 / max(WINDOW_DAYS, 1)
assert rows_per_30d < MAX_STATE_ROWS_PER_30D, (
    f"{rows_per_30d:,.0f} intervals opened per 30 days, over the "
    f"{MAX_STATE_ROWS_PER_30D:,} cap. The state model is too churny -- raise "
    "BASE_MTBF_DAYS or lengthen the dwell times in STATE_DWELL_HOURS."
)

print("OK  state_sk unique, no nulls in key columns, Running carries no cause")
print("OK  every equipment_sk resolves to dim_equipment")
print("OK  end_ts after start_ts; duration_hours agrees with the timestamps")
print("OK  no start_ts after the as-of date")
print(f"OK  intervals tile continuously for all {sdf['equipment_id'].nunique():,} assets "
      "-- no gaps, no overlaps")
print("OK  exactly one open interval per asset")
print("OK  Startup only precedes Running; Shutdown only follows it")
print(f"OK  {len(opened_in_window):,} intervals opened in {WINDOW_DAYS} day(s) = "
      f"{rows_per_30d:,.0f} per 30 days, under the {MAX_STATE_ROWS_PER_30D:,} cap")
print(f"    ({len(sdf):,} rows written in total, including "
      f"{len(sdf) - len(opened_in_window):,} carried from earlier partitions)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Distributions

# CELL ********************

# Hours are clipped to the window so availability is measured over the window, not over
# whatever an open interval will eventually become.
w = sdf.copy()
w["eff_end"] = w["end_ts"].fillna(WINDOW_END)
w["clip_start"] = w["start_ts"].clip(lower=WINDOW_START)
w["clip_end"] = w["eff_end"].clip(upper=WINDOW_END)
w["hours"] = ((w["clip_end"] - w["clip_start"]).dt.total_seconds() / 3600.0).clip(lower=0)

total_hours = w["hours"].sum()
print("state distribution:")
print(f"  {'state':<14}{'intervals':>11}{'share':>8}{'hours':>14}{'share':>8}{'mean dwell h':>14}")
print("  " + "-" * 69)
for st in STATES:
    sub = w[w["state"] == st]
    if sub.empty:
        continue
    mean_dwell = sdf[(sdf["state"] == st) & sdf["end_ts"].notna()]["duration_hours"].mean()
    print(f"  {st:<14}{len(sub):>11,}{len(sub)/len(w):>8.1%}{sub['hours'].sum():>14,.0f}"
          f"{sub['hours'].sum()/total_hours:>8.2%}"
          f"{(mean_dwell if pd.notna(mean_dwell) else 0):>14.2f}")

print()
print("cause distribution (excluding Running):")
for c, n in sdf["cause"].value_counts().items():
    print(f"  {c:<16}{n:>8,}  ({n/sdf['cause'].notna().sum():>5.1%} of caused intervals)")

# --- availability by equipment type ----------------------------------------------------------
wa = w.merge(eq_pdf[["equipment_id", "equipment_type"]], on="equipment_id")
wa["avail_h"] = np.where(wa["state"].isin(AVAILABLE_STATES), wa["hours"], 0.0)

print()
print("availability by equipment type (Running + Standby):")
print(f"  {'equipment_type':<20}{'assets':>8}{'avail %':>10}{'MTBF mid-life':>15}"
      f"{'intervals/asset/30d':>21}")
print("  " + "-" * 74)
off_band = []
for et in sorted(EQUIPMENT_TYPES, key=lambda e: -STATE_DUTY_FACTOR[e]):
    sub = wa[wa["equipment_type"] == et]
    if sub.empty:
        continue
    avail = sub["avail_h"].sum() / sub["hours"].sum()
    n_assets = sub["equipment_id"].nunique()
    per_asset = len(sub) / n_assets * 30.0 / max(WINDOW_DAYS, 1)
    flag = "" if AVAILABILITY_EXPECTED_BAND[0] <= avail <= AVAILABILITY_EXPECTED_BAND[1] else "  *"
    if flag:
        off_band.append((et, avail))
    print(f"  {et:<20}{n_assets:>8,}{avail:>10.2%}"
          f"{mtbf_days(et, EQUIPMENT_TYPES[et]['life'] * 0.5):>15.0f}{per_asset:>21.1f}{flag}")
    assert AVAILABILITY_HARD_BAND[0] <= avail <= AVAILABILITY_HARD_BAND[1], (
        f"{et} availability {avail:.2%} is outside the hard band "
        f"{AVAILABILITY_HARD_BAND[0]:.0%}-{AVAILABILITY_HARD_BAND[1]:.2%}"
    )

overall = wa["avail_h"].sum() / wa["hours"].sum()
print("  " + "-" * 74)
print(f"  {'all':<20}{wa['equipment_id'].nunique():>8,}{overall:>10.2%}")
if off_band:
    print()
    print(f"  * outside the expected band "
          f"{AVAILABILITY_EXPECTED_BAND[0]:.0%}-{AVAILABILITY_EXPECTED_BAND[1]:.0%}: "
          + ", ".join(f"{e} {a:.1%}" for e, a in off_band))
    print("    Static equipment sitting near 100% is expected -- a storage tank with a")
    print("    365-day inspection interval and duty 0.20 barely stops. Reported, not failed.")

# --- churn per day ------------------------------------------------------------------------------
print()
print("intervals opened per day (churn):")
per_day = sdf[sdf["start_ts"] >= WINDOW_START].groupby(
    sdf["start_ts"].dt.date).size()
if len(per_day):
    print(f"  days {len(per_day)}   min {per_day.min()}   median {int(per_day.median())}   "
          f"max {per_day.max()}   mean {per_day.mean():.0f}")
    print(f"  transitions per asset per 30 days: "
          f"{len(sdf)/len(eq_pdf)*30.0/max(WINDOW_DAYS,1):.1f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write `fact_asset_state`

# CELL ********************

STATE_SCHEMA = ["state_sk", "equipment_sk", "equipment_id", "area_sk", "facility_sk",
                "state", "cause", "start_ts", "end_ts", "duration_hours", "is_open",
                "date_sk", "is_synthetic"]

out = sdf[STATE_SCHEMA].copy()
out["cause"] = out["cause"].astype(object).where(out["cause"].notna(), None)

sdf_spark = spark.createDataFrame(out)

writer = (sdf_spark.write.format("delta").mode("overwrite")
          .option("replaceWhere", f"date_sk >= {affected_lo} AND date_sk <= {affected_hi}")
          .partitionBy("date_sk"))
if not table_exists(TABLE):
    # replaceWhere needs an existing table with a matching schema; the first write creates it.
    writer = (sdf_spark.write.format("delta").mode("overwrite")
              .option("overwriteSchema", "true").partitionBy("date_sk"))
    print(f"{TABLE} does not exist -- creating it with a plain partitioned overwrite")

writer.saveAsTable(TABLE)
print(f"{TABLE}: {len(out):,} rows written "
      f"(replaceWhere date_sk {affected_lo}..{affected_hi}, partitioned by date_sk)")

display(sdf_spark.orderBy("equipment_id", "start_ts").limit(25))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary

# CELL ********************

print("=" * 76)
print("ASSET STATE BUILT")
print("=" * 76)
print(f"  run mode          {RUN_MODE}")
print(f"  window            {WINDOW_START.date()} .. {WINDOW_END.date()} ({WINDOW_DAYS} days)")
print(f"  assets            {len(eq_pdf):,}")
print(f"  rows written      {len(out):,}")
print(f"  opened in window  {len(opened_in_window):,}   ({rows_per_30d:,.0f} per 30 days, "
      f"cap {MAX_STATE_ROWS_PER_30D:,})")
print(f"  open intervals    {int(out['is_open'].sum()):,}  (one per asset)")
print(f"  availability      {overall:.2%} Running or Standby")
print(f"  partitions        date_sk {affected_lo} .. {affected_hi}")
print()
print("  table written     fact_asset_state (interval table, one row per state change)")
print("  not modified      every dim_* table")
print("  not read          fact_emission_episode, gold_plume_catalog, any detection table")
print()
print("  next              telemetry generation, overlaying episode effects separately")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
