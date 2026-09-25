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

# # 02e — Generate CH₄ Detector Telemetry
#
# Writes **`sensor_telemetry`**: hourly readings from the 600 CH₄ detectors in `dim_sensor`,
# over the same 30-day window as `scada_telemetry`. Reads `dim_sensor`, `dim_equipment`,
# `dim_facility` and `fact_asset_state`; **modifies nothing else**.
#
# `dim_sensor` and `dim_scada_tag` stay separate registries — a methane detector is not a
# process instrument — and this table keeps **V1's `gold.sensor_telemetry` contract exactly**,
# because `build_snapshots` selects its columns by name:
#
# `sensor_id, equipment_sk, facility_sk, reading_ts, ch4_ppm, baseline_ppm, exceedance_flag,
# wind_speed_ms, wind_dir_deg, sensor_status, ingest_ts`, plus `date_sk` (partition) and
# `is_synthetic`.
#
# ### Cadence
#
# One hour (`CH4_INTERVAL_HOURS`). V1's 4 hours put two readings inside the dashboard's
# 8-hour offline horizon, so one missed reading nearly tripped the KPI.
# `dim_sensor.reading_interval_hours` now carries the same value, written by 01b and asserted
# equal here.
#
# ### The value model
#
# `ch4_ppm = baseline_ppm + enhancement`, every term a pure function of `(sensor_id or
# facility_id, interval_index)` built from **02b's spectral synthesis, copied verbatim** — the
# helper cell below names its source, and `tools/harness/harness_ch4.py` asserts the copy is
# identical to 02b's text, so the two cannot drift into two formulations.
#
# **Baseline** — regional ambient, V1's 1.9–2.1 ppm scale: a pre-dawn diurnal peak (the
# nocturnal boundary layer traps surface emissions), a basin-wide slow series, a small
# per-facility one, and a ~10 ppb/yr trend. Its envelope is bounded analytically in
# `01_topology_config` and sits inside 1.8–2.2 ppm by construction.
#
# **Enhancement** — `bg + leak`, both ≥ 0:
#
# - `bg` is a ≤ 0.02 ppm wander, so `ch4_ppm` is never exactly the baseline.
# - `leak = sigma_ppm × max(0, z − onset)`, where `z` is the sensor's own spectral series with
#   **time stretched ×8** (periods 13 h to 52 days) and `onset` is lowered by the sensor's
#   relative risk and by its asset's state.
#
# ### Making exceedances real — the mechanism chosen
#
# `exceedance_flag = ch4_ppm > dim_sensor.exceedance_threshold_ppm`. The threshold sits
# `sigma_ppm` above ambient, so flagging needs the latent about one unit past the onset.
#
# - **Clustered in time.** A flag is the latent crossing a level, and the latent is smooth.
#   Unstretched, 02b's shortest harmonics (1.63 h, 4.37 h) are faster than the hourly cadence
#   can follow: measured offline, the median exceedance run was **1 reading**, 65% of runs
#   lasting one hour. Stretching the time argument ×8 — the same helper, a different clock,
#   not a second formulation — gives a median run of **6 h**, p90 ~20 h: a leak persists.
# - **Concentrated on risky assets.** `risk = leak_propensity × (1 + age / expected_life)`,
#   divided by the estate's own mean risk, so the shift averages zero whatever the equipment
#   mix and the overall rate is set by `CH4_LEAK_ONSET` alone. `CH4_RISK_SHIFT = 2` makes a
#   compressor exceed tens of times as often as a metering station. In the harness about a
#   third of sensors exceed at all in 30 days and the top 10% hold ~75% of exceedances.
# - **Venting states.** Standby, which lasts hours, lowers `onset` by 0.6: a plateau.
#   Startup and Shutdown last about an hour, so they multiply a leak already under way by
#   1.5 instead. Lowering the onset for one hour made isolated one-reading spikes, which
#   pulled a quiet sensor's lag-1 autocorrelation to ~0.4 in the harness; purging amplifies
#   an emission rather than inventing one.
# - **Maintenance suppresses** the leak term — the asset is isolated and depressurised — and
#   the row reads `sensor_status = 'Calibration'`.
#
# The realised rate is asserted inside 0.5–2% over the 30-day table (a single day swings with
# whichever few sensors are mid-leak — 0.5% to 1.7% in the harness) and printed either way,
# with its breakdown by state, risk and sensor type, so a miss says which lever moved.
#
# ### Gaps and status
#
# As 02b: Poisson outages on a fixed 45-day grid anchored at `TELEMETRY_EPOCH` (so an
# incremental day sees the outages a backfill would), lognormal durations with a 6 h median,
# 8× the rate on a Faulty sensor, and scattered 0.2% dropouts. Rows are **absent, not null**.
# Decommissioned sensors emit nothing. Outages go through 02b's `merge_intervals` and
# `assert_disjoint`. `sensor_status` is `Calibration` during Maintenance on the sensor's asset,
# `Fault` on a Faulty sensor, otherwise `OK`.
#
# ### Wind
#
# Per facility, from the facility's own spectral series mixed with a basin-wide one, so wind
# varies across the basin and over time. The detection pipeline's weather tables are
# **deliberately not read**: they sit on a 0.5° grid at a different cadence, and joining them
# would couple the SCADA layer to the ingest layer for nothing a demo needs.
#
# ### Run modes
#
# As 02c: `run_mode` via `getArgument`. Backfill is the 30-day raw window; incremental resumes
# the day after the table's watermark, re-generating the last day when nothing is newer.
# `replaceWhere` on `date_sk`. Every value is a function of the reading's own index, so
# incremental reproduces backfill exactly (`ingest_ts` excepted — it records the load).

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

# ### Run mode and window

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window

spark.conf.set("spark.sql.session.timeZone", "UTC")

TABLE = "sensor_telemetry"
STATE_TABLE = "fact_asset_state"

# V1's gold.sensor_telemetry contract, in V1's order. build_snapshots selects these by name.
V1_COLUMNS = ["sensor_id", "equipment_sk", "facility_sk", "reading_ts", "ch4_ppm",
              "baseline_ppm", "exceedance_flag", "wind_speed_ms", "wind_dir_deg",
              "sensor_status", "ingest_ts"]
SCHEMA = V1_COLUMNS + ["date_sk", "is_synthetic"]
SENSOR_STATUSES = ("OK", "Fault", "Calibration")
CADENCE_S = CH4_INTERVAL_HOURS * 3600

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


def sk(ts):
    return int(pd.Timestamp(ts).strftime("%Y%m%d"))


AS_OF = pd.Timestamp(TOPOLOGY_AS_OF)
RAW_START, RAW_END = AS_OF - pd.Timedelta(days=TELEMETRY_RAW_DAYS), AS_OF

# An existing table of another shape is someone else's; replaceWhere would fail on it, or
# worse, succeed. Refuse rather than overwrite it.
if table_exists(TABLE):
    _cols = spark.table(TABLE).columns
    assert set(_cols) == set(SCHEMA), (
        f"{TABLE} exists with columns {_cols}, not this notebook's {SCHEMA}. 02e will not "
        "overwrite a table it did not create -- drop or rename it deliberately first."
    )

if RUN_MODE == "backfill":
    WINDOW_START, WINDOW_END = RAW_START, RAW_END
else:
    wm = (spark.sql(f"SELECT max(date_sk) AS m FROM {TABLE}").collect()[0]["m"]
          if table_exists(TABLE) else None)
    if wm is None:
        WINDOW_START, WINDOW_END = RAW_START, RAW_END
        print(f"{TABLE} is empty or absent -- incremental generates the whole window.")
    else:
        WINDOW_START = min(pd.Timestamp(str(int(wm))) + pd.Timedelta(days=1),
                           RAW_END - pd.Timedelta(days=1))
        WINDOW_END = min(WINDOW_START + pd.Timedelta(days=1), RAW_END)

if _start_override:
    WINDOW_START = pd.Timestamp(_start_override)
if _end_override:
    WINDOW_END = pd.Timestamp(_end_override)

WINDOW_START = pd.Timestamp(WINDOW_START).normalize()
WINDOW_END = pd.Timestamp(WINDOW_END).normalize()
assert RAW_START <= WINDOW_START < WINDOW_END <= RAW_END, (
    f"window {WINDOW_START.date()} .. {WINDOW_END.date()} is empty or outside the raw window "
    f"{RAW_START.date()} .. {RAW_END.date()} that scada_telemetry covers"
)
PREDICATE = f"date_sk >= {sk(WINDOW_START)} AND date_sk <= {sk(WINDOW_END - pd.Timedelta(days=1))}"
WINDOW_DAYS = int((WINDOW_END - WINDOW_START).days)
print(f"RUN_MODE={RUN_MODE}  window={WINDOW_START.date()}..{WINDOW_END.date()} "
      f"({WINDOW_DAYS} days)  cadence {CH4_INTERVAL_HOURS} h")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Shared with 02b — copied verbatim, not reformulated
#
# Every definition in the next cell is copied **character for character** from
# `02b_gen_scada_telemetry`: the SHA-256 row hash and its golden vectors, the spectral
# synthesis, the slot grid, `merge_intervals` / `assert_disjoint`, and the hour-bucketed
# half-open state join. Fabric notebooks cannot import from one another, so the choice was a
# copy or a shared `%run` target; a copy keeps 02b untouched. `tools/harness/harness_ch4.py`
# parses both notebooks and fails if any of these definitions differ, so a change to 02b that
# is not carried here is caught offline, before a sync.

# CELL ********************

# ---- copied verbatim from 02b_gen_scada_telemetry; harness_ch4.py asserts they are identical ----
HASH_CHUNKS = 6
HASH_DIVISOR = 4294967296.0
TELEMETRY_HASH_GOLDEN = [
    ("GS-0001.A1.PT-101", 0,       "e1f097e9638c51cf"),
    ("GS-0001.A1.PT-101", 1782432, "0a55bd6cb273d60d"),
    ("GS-0042.A3.FT-104", 5347296, "7e183b7652c53a54"),
]


def row_hash_col(tag_id_col, idx_col):
    """One sha2 per row over (TOPOLOGY_SEED, tag_id, interval_index)."""
    return F.sha2(F.concat_ws("|", F.lit(str(TOPOLOGY_SEED)), tag_id_col,
                              idx_col.cast("string")), 256)



def hash_uniform(h_col, chunk):
    """Chunk `chunk` of a sha2 digest as a uniform on (0, 1)."""
    assert 0 <= chunk < HASH_CHUNKS
    raw = F.conv(F.substring(h_col, 8 * chunk + 1, 8), 16, 10).cast("double")
    return (raw + F.lit(0.5)) / F.lit(HASH_DIVISOR)


TELEMETRY_HARMONICS = [
    (1.63, 0.35),     # short process wander
    (4.37, 0.60),
    (11.90, 0.85),
    (23.93, 1.00),    # near-diurnal, deliberately not exactly 24 h so it beats against
    (61.70, 0.80),    # the explicit diurnal term instead of reinforcing it
    (157.00, 0.60),   # ~6.5 days
]
TELEMETRY_AMP_JITTER = 0.40
HARMONIC_OMEGA = [2.0 * np.pi / (p * 3600.0) for p, _ in TELEMETRY_HARMONICS]


def series_params(kind, series_id):
    """Unit-variance spectral parameters for one series.

    Amplitude and phase are hash-derived from (TOPOLOGY_SEED, series_id, harmonic_index);
    the frequencies are fixed. Normalising by sqrt(sum(a^2)/2) makes every series have
    variance 1 exactly, so the mixing weights below mean what they say.
    """
    amps, phases = [], []
    for h, (_period, weight) in enumerate(TELEMETRY_HARMONICS):
        rng = get_rng("tel_spectral", kind, series_id, h)
        u, v = rng.random(), rng.random()
        amps.append(weight * (1.0 - TELEMETRY_AMP_JITTER + 2.0 * TELEMETRY_AMP_JITTER * u))
        phases.append(2.0 * np.pi * v)
    amps = np.asarray(amps)
    return amps / np.sqrt((amps ** 2).sum() / 2.0), np.asarray(phases)



def spectral_expr(prefix, t_sec_col):
    """The spectral sum as a Spark expression: sum a_h * sin(w_h * t + p_h)."""
    term = None
    for h, w in enumerate(HARMONIC_OMEGA):
        t = F.col(f"{prefix}_a{h}") * F.sin(F.lit(w) * t_sec_col + F.col(f"{prefix}_p{h}"))
        term = t if term is None else term + t
    return term



def series_frame(kind, ids, prefix, key_name):
    """A small DataFrame of spectral parameters, one row per series."""
    rows = []
    for sid in ids:
        a, p = series_params(kind, sid)
        r = {key_name: sid}
        for h in range(len(TELEMETRY_HARMONICS)):
            r[f"{prefix}_a{h}"] = float(a[h])
            r[f"{prefix}_p{h}"] = float(p[h])
        rows.append(r)
    return pd.DataFrame(rows)


TELEMETRY_EPOCH = pd.Timestamp("2026-01-01")
_EPOCH_TS = pd.Timestamp("1970-01-01")


def slot_ceil(ts, cadence_s):
    """First slot index at or after ts, on the global grid.

    Every slot-index boundary in this notebook goes through here, because the one time it
    did not the two ends of a range were rounded differently. The count of slots with
    `lo <= k*cadence < hi` is `slot_ceil(hi) - slot_ceil(lo)` -- CEIL at BOTH ends.

    Using floor(hi/cadence) for the upper end is equivalent only when hi lands exactly on a
    slot boundary. It does for every whole-window and whole-day call, since midnight is
    divisible by both 300s and 900s -- which is precisely why the asymmetry stayed invisible
    until outage intervals, whose lognormal end times land nowhere in particular, were
    counted with the same helper. It undercounted by exactly one slot per interval.
    """
    return int(np.ceil((pd.Timestamp(ts) - _EPOCH_TS).total_seconds() / int(cadence_s)))



def window_slots(install_date, cadence_s, lo=None, hi=None):
    """Grid slots in [max(lo, install_date), hi) for one tag.

    This is the denominator of every rate in this notebook, and it is clipped to the tag's
    install date rather than starting at WINDOW_START for everybody.

    The estate is YOUNG. Asset ages come from a triangular draw in 01b whose mode sits at
    30% of the facility's own age, so the median asset is about 18 months old, not 7 years;
    tag install dates then sit up to a year after that. Roughly 12% of tags are therefore
    installed part-way through a 30-day window and legitimately emit only a fraction of it.
    Counting those absent slots as offline made the realised offline share ~9.4% against a
    2% target -- not a transmitter that is dark, but a tag that did not exist yet.

    A tag installed at or after WINDOW_END scores zero and drops out of both sides of every
    ratio. A DECOMMISSIONED tag keeps its slots: it existed, it is in the registry, and
    anything computing staleness from MAX(reading_ts) will rightly call it offline.
    """
    lo = WINDOW_START if lo is None else lo
    hi = WINDOW_END if hi is None else hi
    start = max(pd.Timestamp(install_date), pd.Timestamp(lo))
    hi = pd.Timestamp(hi)
    if start >= hi:
        return 0
    return max(0, slot_ceil(hi, cadence_s) - slot_ceil(start, cadence_s))



def _slot_range(slot_s, lo_ts, hi_ts):
    k0 = int(np.floor((lo_ts - TELEMETRY_EPOCH).total_seconds() / slot_s)) - 1
    k1 = int(np.floor((hi_ts - TELEMETRY_EPOCH).total_seconds() / slot_s))
    return range(k0, k1 + 1)



def merge_intervals(pdf, key, start, end):
    """Collapse overlapping or abutting intervals per key into their union.

    Two outages that overlap in time are physically ONE outage, and they have to be stored
    that way. The suppression join below is a LEFT join on a half-open range predicate, so a
    reading covered by two intervals matches twice and is emitted twice. That does not
    corrupt the written table -- both copies are dropped as outage rows -- but it inflates
    the pre-filter slot count the per-day assertion checks, and it double-counts offline
    seconds in the volume projection and the offline-share expectation.

    Overlaps are not exotic here. A Faulty tag draws Poisson(8) arrivals per 45-day slot with
    a mean duration near 9 h, so a handful of the ~80 faulty tags will have two outages that
    run into each other in any given month. Merging at the source fixes the count, fixes the
    arithmetic, and is what the physics says anyway: a transmitter that drops out again
    before it came back was never back.
    """
    if not len(pdf):
        return pdf
    out, cur = [], None
    for r in pdf.sort_values([key, start], kind="mergesort").to_dict("records"):
        if cur is not None and r[key] == cur[key] and r[start] <= cur[end]:
            cur[end] = max(cur[end], r[end])
            continue
        if cur is not None:
            out.append(cur)
        cur = dict(r)
    out.append(cur)
    return pd.DataFrame(out).reset_index(drop=True)



def assert_disjoint(pdf, key, start, end, what):
    """No two intervals for the same key may overlap, or the join stops being one-to-one."""
    if len(pdf) < 2:
        return
    g = pdf.sort_values([key, start], kind="mergesort").reset_index(drop=True)
    same = g[key].eq(g[key].shift(1))
    bad = int((same & (g[start] < g[end].shift(1))).sum())
    assert bad == 0, (
        f"{bad} overlapping {what} interval pair(s) survived merge_intervals. A reading "
        "inside the overlap would match both and be emitted twice."
    )


HOUR_S = 3600


def _ramp_phi():
    span = F.unix_timestamp("eff_end") - F.unix_timestamp("start_ts")
    phi = F.when(span > 0,
                 (F.unix_timestamp("reading_ts") - F.unix_timestamp("start_ts"))
                 / span.cast("double")).otherwise(F.lit(0.0))
    return F.least(F.greatest(phi, F.lit(0.0)), F.lit(1.0))



def state_buckets(lo_ts, hi_ts):
    """Intervals overlapping [lo, hi), exploded to the hour buckets inside that range.

    Both ends are clamped to the range. Clamping the LOW end is what keeps this bounded: a
    Running interval that opened three weeks ago and is still open would otherwise explode
    into 500 buckets, and there is one such interval per asset. The range predicate in
    attach_state still decides membership, so clamping loses nothing.
    """
    lo_u = int(pd.Timestamp(lo_ts).timestamp())
    hi_u = int(pd.Timestamp(hi_ts).timestamp())
    sub = state_all.filter(
        (F.col("start_ts") < F.lit(hi_ts)) & (F.col("eff_end") > F.lit(lo_ts)))
    lo_b = F.floor(F.greatest(F.unix_timestamp("start_ts"), F.lit(lo_u)) / F.lit(HOUR_S))
    hi_b = F.floor(F.least(F.unix_timestamp("eff_end"), F.lit(hi_u)) / F.lit(HOUR_S))
    return (sub
            .withColumn("hour_bucket", F.explode(F.sequence(lo_b, hi_b)))
            .select("equipment_sk", "state", "start_ts", "eff_end", "hour_bucket"))



def attach_state(df, buckets):
    """One state row per reading, plus the ramp fraction for Startup/Shutdown."""
    j = (df.withColumn("hour_bucket", F.floor(F.unix_timestamp("reading_ts") / F.lit(HOUR_S)))
           .join(F.broadcast(buckets), ["equipment_sk", "hour_bucket"], "inner")
           .filter((F.col("reading_ts") >= F.col("start_ts"))
                   & (F.col("reading_ts") < F.col("eff_end"))))
    return (j.withColumn("ramp_phi", _ramp_phi())
             .drop("hour_bucket", "start_ts", "eff_end"))

_golden = spark.createDataFrame(
    [(t, int(i), g) for t, i, g in TELEMETRY_HASH_GOLDEN],
    "tag_id string, interval_index bigint, expected string",
).withColumn("actual", F.substring(row_hash_col(F.col("tag_id"), F.col("interval_index")), 1, 16))
_bad = _golden.filter("actual <> expected").collect()
assert not _bad, f"Spark's sha2 disagrees with 02b's golden vectors: {_bad[0]}"
print(f"OK  sha2 matches 02b's {len(TELEMETRY_HASH_GOLDEN)} golden vectors")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### The registry, and each sensor's risk

# CELL ********************

sen_pdf = spark.table("dim_sensor").filter("is_current = true").toPandas()
_need = {"exceedance_threshold_ppm", "sigma_ppm", "tier", "reading_interval_hours", "status",
         "install_date", "sensor_type", "equipment_sk", "facility_sk", "facility_id"}
assert _need <= set(sen_pdf.columns), (
    f"dim_sensor lacks {sorted(_need - set(sen_pdf.columns))} -- rerun 01b_build_asset_topology, "
    "which writes the exceedance threshold, sigma and tier"
)
assert (sen_pdf["topology_seed"] == TOPOLOGY_SEED).all(), \
    "dim_sensor was built with a different TOPOLOGY_SEED; rerun 01b"
assert sen_pdf["sensor_id"].is_unique, "sensor_id is not unique among current dim_sensor rows"
assert (sen_pdf["reading_interval_hours"] == CH4_INTERVAL_HOURS).all(), (
    f"dim_sensor.reading_interval_hours is not CH4_INTERVAL_HOURS ({CH4_INTERVAL_HOURS}) for "
    "every sensor -- the registry and the generator disagree on cadence; rerun 01b"
)
assert set(CH4_STATE_SHIFT) == set(STATES) == set(CH4_STATE_LEAK_MULT), \
    "CH4_STATE_SHIFT and CH4_STATE_LEAK_MULT must cover every state"

eq_pdf = spark.table("dim_equipment").select(
    "equipment_sk", "equipment_type", "leak_propensity", "expected_life_years",
    F.col("install_date").alias("eq_install")).toPandas()
fac_ids = set(spark.table("dim_facility").select("facility_id").toPandas()["facility_id"])
assert set(sen_pdf["facility_id"]) <= fac_ids, "a sensor's facility is not in dim_facility"
sen_pdf = sen_pdf.merge(eq_pdf, on="equipment_sk", how="left")
assert sen_pdf["leak_propensity"].notna().all(), "a sensor's equipment is not in dim_equipment"

sen_pdf["install_ts"] = pd.to_datetime(sen_pdf["install_date"])
_age = (AS_OF - pd.to_datetime(sen_pdf["eq_install"])).dt.days / 365.25
sen_pdf["age_ratio"] = np.clip(_age / sen_pdf["expected_life_years"], 0.0, 1.0)
sen_pdf["risk"] = sen_pdf["leak_propensity"] * (1.0 + CH4_AGE_WEIGHT * sen_pdf["age_ratio"])
# relative to the estate's own mean: the shift averages zero whatever the equipment mix, so
# CH4_LEAK_ONSET alone sets the overall rate. A registry property, not an outcome -- it does
# not depend on the window.
sen_pdf["risk_rel"] = sen_pdf["risk"] / sen_pdf["risk"].mean()
sen_pdf["is_faulty"] = sen_pdf["status"] == "Faulty"

live = sen_pdf[sen_pdf["status"] != "Decommissioned"].copy()
live["potential_slots"] = [window_slots(d, CADENCE_S) for d in live["install_ts"]]
sen_pdf["potential_slots"] = [window_slots(d, CADENCE_S) for d in sen_pdf["install_ts"]]
ALL_SLOTS = int(sen_pdf["potential_slots"].sum())
LIVE_SLOTS = int(live["potential_slots"].sum())
DARK_SLOTS = ALL_SLOTS - LIVE_SLOTS

print(f"{len(sen_pdf):,} sensors: " + "  ".join(
    f"{k} {v:,}" for k, v in sen_pdf["status"].value_counts().items()))
print(f"relative risk {sen_pdf['risk_rel'].min():.2f} .. {sen_pdf['risk_rel'].max():.2f}; "
      f"exceedance_threshold_ppm {sen_pdf['exceedance_threshold_ppm'].min():.3f} .. "
      f"{sen_pdf['exceedance_threshold_ppm'].max():.3f}")
print(f"slots: {ALL_SLOTS:,} potential ({LIVE_SLOTS:,} on live sensors, {DARK_SLOTS:,} dark)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Outages

# CELL ********************

outage_rows = []
for r in live.itertuples():
    lam = CH4_FAULTY_OUTAGE_MULT if r.is_faulty else 1.0
    for k in _slot_range(CH4_OUTAGE_MEAN_DAYS * 86400.0, WINDOW_START, WINDOW_END):
        rng = get_rng("ch4_outage", r.sensor_id, k)
        n = int(rng.poisson(lam))
        if not n:
            continue
        offs = rng.random(n) * CH4_OUTAGE_MEAN_DAYS * 86400.0
        durs = np.minimum(CH4_OUTAGE_MEDIAN_H * np.exp(CH4_OUTAGE_SIGMA * rng.standard_normal(n)),
                          CH4_OUTAGE_MAX_H)
        for o, d in zip(offs, durs):
            s = TELEMETRY_EPOCH + pd.Timedelta(seconds=float(k * CH4_OUTAGE_MEAN_DAYS * 86400.0 + o))
            e = s + pd.Timedelta(hours=float(d))
            if e > WINDOW_START and s < WINDOW_END:
                outage_rows.append({"sensor_id": r.sensor_id, "out_start": s, "out_end": e})

outage_pdf = pd.DataFrame(outage_rows, columns=["sensor_id", "out_start", "out_end"])
_n_raw = len(outage_pdf)
outage_pdf = merge_intervals(outage_pdf, "sensor_id", "out_start", "out_end")
assert_disjoint(outage_pdf, "sensor_id", "out_start", "out_end", "outage")

_inst = live.set_index("sensor_id")["install_ts"].to_dict()
OUTAGE_SLOTS = int(sum(
    window_slots(_inst[r.sensor_id], CADENCE_S,
                 lo=max(r.out_start, WINDOW_START), hi=min(r.out_end, WINDOW_END))
    for r in outage_pdf.itertuples())) if len(outage_pdf) else 0
OFFLINE_EXPECTED = (DARK_SLOTS + OUTAGE_SLOTS) / max(ALL_SLOTS, 1)
print(f"{len(outage_pdf):,} outage intervals ({_n_raw - len(outage_pdf)} merged), "
      f"{OUTAGE_SLOTS:,} slots; expected offline share {OFFLINE_EXPECTED:.2%}")

outage_dim = F.broadcast(spark.createDataFrame(
    outage_pdf if len(outage_pdf) else
    pd.DataFrame({"sensor_id": pd.Series(dtype="object"),
                  "out_start": pd.Series(dtype="datetime64[ns]"),
                  "out_end": pd.Series(dtype="datetime64[ns]")})))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Generate
#
# One row per live sensor per hour from its install, conditioned on its asset's state through
# 02b's bucketed half-open join, outages and dropouts removed, then the value model.

# CELL ********************

SENSOR_EQ = sorted(int(e) for e in live["equipment_sk"].unique())
state_all = (spark.table(STATE_TABLE)
             .select("equipment_sk", "state", "start_ts", "end_ts")
             .filter(F.col("equipment_sk").isin(SENSOR_EQ))
             .withColumn("eff_end", F.coalesce(F.col("end_ts"),
                                               F.lit(AS_OF + pd.Timedelta(days=3650))))
             .drop("end_ts"))


def spectral_dim(kind, ids, prefix, key):
    return F.broadcast(spark.createDataFrame(series_frame(kind, ids, prefix, key)))


fac_list = sorted(live["facility_id"].unique())
REGION = "PERMIAN"
sens_dim = F.broadcast(spark.createDataFrame(live[[
    "sensor_id", "equipment_sk", "facility_sk", "facility_id", "sensor_type", "tier",
    "exceedance_threshold_ppm", "sigma_ppm", "risk_rel", "is_faulty", "install_ts"]]))

k0 = slot_ceil(WINDOW_START, CADENCE_S)
k1 = slot_ceil(WINDOW_END, CADENCE_S)
grid = (spark.range(k0, k1).withColumnRenamed("id", "interval_index")
        .withColumn("reading_ts", F.timestamp_seconds(F.col("interval_index") * F.lit(CADENCE_S))))

base = (grid.crossJoin(sens_dim)
        .filter(F.col("reading_ts") >= F.col("install_ts")).drop("install_ts"))
N_SLOTS = base.count()
assert N_SLOTS == LIVE_SLOTS, (
    f"{N_SLOTS:,} grid slots against {LIVE_SLOTS:,} from window_slots -- the install-date clip "
    "and the slot grid disagree")

# exactly one state per reading, or the join has duplicated or dropped a slot
rows = attach_state(base, state_buckets(WINDOW_START, WINDOW_END)).drop("ramp_phi")
N_STATED = rows.count()
assert N_STATED == N_SLOTS, (
    f"{N_STATED:,} readings after the fact_asset_state join against {N_SLOTS:,} slots -- a "
    "sensor's asset has a gap or an overlap in its state intervals over the window")

o = outage_dim.select(F.col("sensor_id").alias("o_id"), "out_start", "out_end")
rows = (rows.join(o, (F.col("sensor_id") == F.col("o_id"))
                  & (F.col("reading_ts") >= F.col("out_start"))
                  & (F.col("reading_ts") < F.col("out_end")), "left")
        .filter(F.col("o_id").isNull()).drop("o_id", "out_start", "out_end"))
rows = rows.withColumn("_h", row_hash_col(F.col("sensor_id"), F.col("interval_index")))
N_AFTER_OUTAGE = rows.count()
rows = rows.filter(hash_uniform(F.col("_h"), 0) >= F.lit(CH4_DROPOUT_RATE))

# --- the spectral series ------------------------------------------------------------------------
rows = (rows
        .join(spectral_dim("ch4_leak", list(live["sensor_id"]), "lk", "sensor_id"), "sensor_id")
        .join(spectral_dim("ch4_site", fac_list, "st", "facility_id"), "facility_id")
        .join(spectral_dim("ch4_wind_speed", fac_list, "wf", "facility_id"), "facility_id")
        .join(spectral_dim("ch4_wind_dir", fac_list, "df", "facility_id"), "facility_id")
        .crossJoin(spectral_dim("ch4_region", [REGION], "rg", "rg_id").drop("rg_id"))
        .crossJoin(spectral_dim("ch4_wind_speed", [REGION], "wr", "wr_id").drop("wr_id"))
        .crossJoin(spectral_dim("ch4_wind_dir", [REGION], "dr", "dr_id").drop("dr_id")))

t = (F.col("interval_index") * F.lit(CADENCE_S)).cast("double")
epoch_s = float((TELEMETRY_EPOCH - _EPOCH_TS).total_seconds())
assert abs((WINDOW_END - TELEMETRY_EPOCH).days) <= 366, \
    "the baseline trend bound in 01_topology_config assumes the window is within a year of TELEMETRY_EPOCH"

local_h = F.pmod(t / F.lit(3600.0) + F.lit(CH4_LOCAL_UTC_OFFSET_H), F.lit(24.0))
baseline = (F.lit(CH4_AMBIENT_REF_PPM)
            + F.lit(CH4_DIURNAL_PPM) * F.cos(F.lit(2 * np.pi / 24.0)
                                             * (local_h - F.lit(CH4_DIURNAL_PEAK_LOCAL_H)))
            + F.lit(CH4_TREND_PPM_PER_YEAR) * (t - F.lit(epoch_s)) / F.lit(365.25 * 86400.0)
            + F.lit(CH4_REGIONAL_PPM) * spectral_expr("rg", t / F.lit(CH4_BASELINE_TIME_STRETCH))
            + F.lit(CH4_SITE_PPM) * spectral_expr("st", t / F.lit(CH4_BASELINE_TIME_STRETCH)))

state_shift = F.create_map(*[x for s, v in CH4_STATE_SHIFT.items() for x in (F.lit(s), F.lit(v))])
state_mult = F.create_map(*[x for s, v in CH4_STATE_LEAK_MULT.items()
                            for x in (F.lit(s), F.lit(v))])
z = spectral_expr("lk", t / F.lit(CH4_LEAK_TIME_STRETCH))
onset = (F.lit(CH4_LEAK_ONSET) - F.lit(CH4_RISK_SHIFT) * (F.col("risk_rel") - F.lit(1.0))
         - state_shift[F.col("state")])
suppressed = F.col("state").isin(*CH4_SUPPRESSED_STATES)
leak = F.when(suppressed, F.lit(0.0)).otherwise(
    F.col("sigma_ppm") * state_mult[F.col("state")]
    * F.greatest(F.lit(0.0), F.col("_z") - F.col("_onset")))
bg = F.lit(CH4_BG_PPM) * F.least(F.lit(1.0), F.greatest(F.lit(0.0), (
    F.col("_z") + F.lit(CH4_SPECTRAL_BOUND)) / F.lit(2 * CH4_SPECTRAL_BOUND)))

w_speed = 0.6 * spectral_expr("wr", t) + 0.8 * spectral_expr("wf", t)
w_dir = 0.6 * spectral_expr("dr", t) + 0.8 * spectral_expr("df", t)

gen = (rows
       .withColumn("_z", z).withColumn("_onset", onset)
       .withColumn("_base", baseline).withColumn("_enh", bg + leak)
       .withColumn("baseline_ppm", F.round(F.col("_base"), 4))
       .withColumn("ch4_ppm", F.round(F.col("_base") + F.col("_enh"), 4))
       # the flag is computed on the stored value, so ch4_ppm > threshold holds in the table
       .withColumn("exceedance_flag", F.col("ch4_ppm") > F.col("exceedance_threshold_ppm"))
       .withColumn("wind_speed_ms", F.round(F.lit(CH4_WIND_MEAN_MS) * F.exp(
           F.lit(CH4_WIND_LOG_SD) * w_speed - F.lit(CH4_WIND_LOG_SD ** 2 / 2.0)), 2))
       .withColumn("wind_dir_deg", F.pmod(F.round(F.lit(CH4_WIND_DIR_PREVAILING)
                                                  + F.lit(CH4_WIND_DIR_SPREAD) * w_dir, 1),
                                          F.lit(360.0)))
       .withColumn("sensor_status",
                   F.when(suppressed, F.lit("Calibration"))
                    .when(F.col("is_faulty"), F.lit("Fault")).otherwise(F.lit("OK")))
       .withColumn("date_sk", F.date_format("reading_ts", "yyyyMMdd").cast("long"))
       .withColumn("is_synthetic", F.lit(True))
       .withColumn("ingest_ts", F.current_timestamp()))

DIAG_COLS = ["state", "sensor_type", "tier", "risk_rel", "facility_id", "exceedance_threshold_ppm",
             "_enh"]
gen = gen.select(*SCHEMA, *DIAG_COLS).persist()
N_ROWS = gen.count()
N_DROPOUT = N_AFTER_OUTAGE - N_ROWS
print(f"generated {N_ROWS:,} readings ({N_SLOTS - N_AFTER_OUTAGE:,} slots in outages, "
      f"{N_DROPOUT:,} dropouts)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write

# CELL ********************

out = gen.select(*SCHEMA)
_oos = out.filter(f"NOT ({PREDICATE})").count()
assert _oos == 0, f"{_oos} row(s) outside the replaceWhere predicate {PREDICATE}"
w = out.write.format("delta").mode("overwrite").partitionBy("date_sk")
if table_exists(TABLE):
    w.option("replaceWhere", PREDICATE).saveAsTable(TABLE)
else:
    w.option("overwriteSchema", "true").saveAsTable(TABLE)
print(f"{TABLE}: {N_ROWS:,} rows written (replaceWhere {PREDICATE})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation — every check fails the run, none warns

# CELL ********************

tbl = spark.table(TABLE).filter(PREDICATE).persist()
N_TBL = tbl.count()

# --- volume -------------------------------------------------------------------------------------
PROJECTED = (LIVE_SLOTS - OUTAGE_SLOTS) * (1.0 - CH4_DROPOUT_RATE)
assert N_TBL == N_ROWS, f"{N_TBL:,} rows read back, {N_ROWS:,} written"
assert abs(N_TBL - PROJECTED) <= 0.10 * PROJECTED, (
    f"{N_TBL:,} rows, {abs(N_TBL / PROJECTED - 1):.1%} from the projection {PROJECTED:,.0f}")
assert N_SLOTS - N_AFTER_OUTAGE == OUTAGE_SLOTS, (
    f"{N_SLOTS - N_AFTER_OUTAGE:,} slots removed by outages, {OUTAGE_SLOTS:,} expected -- "
    "an outage matched a reading twice, or not at all")
print(f"OK  {N_TBL:,} rows, {N_TBL / PROJECTED - 1:+.2%} against the projection "
      f"{PROJECTED:,.0f}; outage slots removed exactly as counted")

# --- keys, FKs, dates ---------------------------------------------------------------------------
_dup = tbl.groupBy("sensor_id", "reading_ts").count().filter("count > 1").limit(5).collect()
assert not _dup, f"(sensor_id, reading_ts) not unique: {[tuple(r)[:2] for r in _dup]}"
_known = F.broadcast(spark.table("dim_sensor").select("sensor_id").distinct())
_orph = tbl.join(_known, "sensor_id", "left_anti").select("sensor_id").distinct().limit(5).collect()
assert not _orph, f"sensor_id not in dim_sensor: {[r['sensor_id'] for r in _orph]}"
_dec = sorted(sen_pdf.loc[sen_pdf["status"] == "Decommissioned", "sensor_id"])
if _dec:
    assert tbl.filter(F.col("sensor_id").isin(*_dec)).count() == 0, \
        "a Decommissioned sensor emitted a reading"
assert tbl.filter("date_sk <> CAST(date_format(reading_ts, 'yyyyMMdd') AS BIGINT)").count() == 0, \
    "date_sk disagrees with reading_ts"
assert tbl.filter((F.col("reading_ts") < F.lit(WINDOW_START))
                  | (F.col("reading_ts") >= F.lit(WINDOW_END))).count() == 0, \
    "a reading outside the window, or at/after the as-of date"
assert tbl.filter(~F.col("sensor_status").isin(*SENSOR_STATUSES)).count() == 0, \
    "sensor_status outside OK / Fault / Calibration"
print("OK  (sensor_id, reading_ts) unique; every sensor resolves; no decommissioned emission; "
      "date_sk agrees; nothing outside the window")

# --- values -------------------------------------------------------------------------------------
_v = tbl.agg(F.min("baseline_ppm").alias("bmin"), F.max("baseline_ppm").alias("bmax"),
             F.sum(F.when(F.col("ch4_ppm") < F.col("baseline_ppm"), 1).otherwise(0)).alias("neg"),
             F.min("wind_speed_ms").alias("wmin"), F.max("wind_dir_deg").alias("dmax")).first()
lo_b, hi_b = CH4_BASELINE_BOUNDS
assert lo_b <= _v["bmin"] and _v["bmax"] <= hi_b, \
    f"baseline_ppm {_v['bmin']}..{_v['bmax']} outside {CH4_BASELINE_BOUNDS}"
assert _v["neg"] == 0, f"{_v['neg']} reading(s) with ch4_ppm below baseline_ppm"
assert _v["wmin"] >= 0 and _v["dmax"] < 360, "wind speed negative or direction not in [0, 360)"
_fl = gen.filter(F.col("exceedance_flag") != (F.col("ch4_ppm") > F.col("exceedance_threshold_ppm"))).count()
assert _fl == 0, "exceedance_flag disagrees with ch4_ppm > exceedance_threshold_ppm"
print(f"OK  baseline {_v['bmin']:.3f}..{_v['bmax']:.3f} ppm; ch4_ppm >= baseline_ppm "
      "everywhere; flag = ch4_ppm > threshold")

# --- the headline check: exceedance rate ---------------------------------------------------------
# Asserted over the whole table once it covers the full raw window, not over one incremental
# day: exceedances cluster by design, so a single day's rate swings with whichever few
# sensors are mid-leak, and a band on it would fail runs for doing what they should.
full = spark.table(TABLE)
FULL = full.select("date_sk").distinct().count() == TELEMETRY_RAW_DAYS
scope = full if FULL else tbl
SCOPE = "whole table (30 days)" if FULL else "run window"
N_EXC = tbl.filter("exceedance_flag").count()
EXC_RATE = N_EXC / max(N_TBL, 1)
_s = scope.agg(F.count("*").alias("n"), F.sum(F.col("exceedance_flag").cast("int")).alias("x")).first()
EXC_RATE_SCOPE = (_s["x"] or 0) / max(_s["n"], 1)
print(f"\nexceedance rate {EXC_RATE_SCOPE:.2%} over the {SCOPE}; {EXC_RATE:.2%} in this run's "
      f"window ({N_EXC:,} of {N_TBL:,}); band {CH4_EXCEEDANCE_BAND[0]:.1%}-"
      f"{CH4_EXCEEDANCE_BAND[1]:.1%}")
for _k, _lbl in (("state", "by state"), ("sensor_type", "by sensor type"), ("tier", "by tier")):
    _g = (gen.groupBy(_k).agg(F.count("*").alias("n"),
                              F.avg(F.col("exceedance_flag").cast("double")).alias("r"))
          .orderBy(_k).collect())
    print(f"  {_lbl:<16}" + "   ".join(f"{r[_k]} {r['r']:.2%} (n={r['n']:,})" for r in _g))
_q = (gen.withColumn("rq", F.when(F.col("risk_rel") < 0.8, "<0.8")
                     .when(F.col("risk_rel") < 1.0, "0.8-1.0")
                     .when(F.col("risk_rel") < 1.2, "1.0-1.2").otherwise(">=1.2"))
      .groupBy("rq").agg(F.avg(F.col("exceedance_flag").cast("double")).alias("r"),
                         F.countDistinct("sensor_id").alias("s")).orderBy("rq").collect())
print("  by relative risk " + "   ".join(f"{r['rq']} {r['r']:.2%} ({r['s']} sensors)" for r in _q))
assert CH4_EXCEEDANCE_BAND[0] <= EXC_RATE_SCOPE <= CH4_EXCEEDANCE_BAND[1], (
    f"exceedance rate {EXC_RATE_SCOPE:.2%} over the {SCOPE} outside {CH4_EXCEEDANCE_BAND}. "
    "Zero is the V1 defect "
    "back in a new form; far above and the KPI is noise. The lever is CH4_LEAK_ONSET in "
    "01_topology_config (about x1.5 in rate per 0.2 lower); the breakdown above says whether "
    "state, type or risk moved it."
)
_maint = gen.filter(F.col("state").isin(*CH4_SUPPRESSED_STATES) & F.col("exceedance_flag")).count()
assert _maint == 0, f"{_maint} exceedance(s) during Maintenance, which suppresses the leak term"
print("OK  exceedance rate inside the band; none during Maintenance")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- exceedance runs: clustered, not scattered ----------------------------------------------------
ws = Window.partitionBy("sensor_id").orderBy("reading_ts")
runs = (tbl.filter("exceedance_flag").select("sensor_id", "reading_ts")
        .withColumn("prev", F.lag("reading_ts").over(ws))
        .withColumn("new", F.col("prev").isNull()
                    | ((F.unix_timestamp("reading_ts") - F.unix_timestamp("prev")) != CADENCE_S))
        .withColumn("run", F.sum(F.col("new").cast("int")).over(ws))
        .groupBy("sensor_id", "run").count())
_rl = runs.select("count").toPandas()["count"]
if len(_rl):
    _bins = [(1, 1), (2, 3), (4, 6), (7, 12), (13, 24), (25, 10 ** 9)]
    print(f"exceedance runs: {len(_rl):,}, median {int(_rl.median())} h, p90 "
          f"{int(_rl.quantile(0.9))} h, longest {int(_rl.max())} h")
    for a, b in _bins:
        n = int(((_rl >= a) & (_rl <= b)).sum())
        label = f"{a} h" if a == b else (f"{a}-{b} h" if b < 10 ** 9 else f">={a} h")
        print(f"  {label:>8}  {n:>6,} runs  {n / len(_rl):>6.1%}")
    assert _rl.median() >= 3, (
        f"median exceedance run is {_rl.median():.0f} reading(s) -- flags are scattered, not "
        "clustered. CH4_LEAK_TIME_STRETCH sets how long a crossing lasts.")
    print("OK  exceedances cluster: median run >= 3 consecutive readings")

# --- lag-1 autocorrelation on 50 sensors ----------------------------------------------------------
AC_N, AC_MIN = 50, 0.70
_pick = sorted(live["sensor_id"])[:: max(1, len(live) // AC_N)][:AC_N]
_ac = (tbl.filter(F.col("sensor_id").isin(*_pick)).select("sensor_id", "reading_ts", "ch4_ppm")
       .withColumn("pv", F.lag("ch4_ppm").over(ws)).withColumn("pt", F.lag("reading_ts").over(ws))
       .filter((F.unix_timestamp("reading_ts") - F.unix_timestamp("pt")) == CADENCE_S)
       .groupBy("sensor_id").agg(F.corr("ch4_ppm", "pv").alias("r1"), F.count("*").alias("n"))
       .filter("n >= 20 AND r1 IS NOT NULL").toPandas())
assert len(_ac) >= 0.8 * len(_pick), f"only {len(_ac)} of {len(_pick)} sampled sensors measurable"
assert (_ac["r1"] > AC_MIN).all(), (
    f"{int((_ac['r1'] <= AC_MIN).sum())} sampled sensor(s) with lag-1 autocorrelation <= "
    f"{AC_MIN}:\n{_ac.sort_values('r1').head(5).to_string(index=False)}")
print(f"OK  lag-1 autocorrelation on {len(_ac)} sensors: min {_ac['r1'].min():.3f}, "
      f"median {_ac['r1'].median():.3f}")

# --- offline share, and the KPI the dashboard shows -------------------------------------------------
OFFLINE_REALISED = (ALL_SLOTS - N_TBL - N_DROPOUT) / max(ALL_SLOTS, 1)
print(f"\noffline share {OFFLINE_REALISED:.2%} of sensor-hours (expected {OFFLINE_EXPECTED:.2%}, "
      f"band {CH4_OFFLINE_BAND[0]:.0%}-{CH4_OFFLINE_BAND[1]:.0%}; dropouts are single missed "
      "readings, not offline)")
assert abs(OFFLINE_REALISED - OFFLINE_EXPECTED) < 1e-9, "realised offline share != expected"
# The band, like the exceedance rate, over the whole table once it is complete. Missing
# sensor-hours there include dropouts (0.2%), since the full table's cannot be separated out.
if FULL:
    _all_full = sum(window_slots(d, CADENCE_S, lo=RAW_START, hi=RAW_END)
                    for d in sen_pdf["install_ts"])
    OFFLINE_SCOPE = 1.0 - full.count() / max(_all_full, 1)
else:
    OFFLINE_SCOPE = OFFLINE_REALISED
print(f"offline share over the {SCOPE}: {OFFLINE_SCOPE:.2%}")
assert CH4_OFFLINE_BAND[0] <= OFFLINE_SCOPE <= CH4_OFFLINE_BAND[1], (
    f"offline share {OFFLINE_SCOPE:.2%} over the {SCOPE} outside {CH4_OFFLINE_BAND}")

KPI_HOURS = 8
probes = [WINDOW_START + (WINDOW_END - WINDOW_START) * f for f in (0.15, 0.35, 0.55, 0.75, 0.95)]
# the whole table, not the run window: an 8-hour lookback crosses into the previous day
last = spark.table(TABLE).select("sensor_id", "reading_ts")
kpi = []
for p in probes:
    p = pd.Timestamp(p).floor("h")
    seen = set(r["sensor_id"] for r in
               last.filter((F.col("reading_ts") <= F.lit(p))
                           & (F.col("reading_ts") > F.lit(p - pd.Timedelta(hours=KPI_HOURS))))
               .select("sensor_id").distinct().collect())
    exist = sen_pdf[sen_pdf["install_ts"] <= p - pd.Timedelta(hours=KPI_HOURS)]
    off = exist[~exist["sensor_id"].isin(seen)]
    n_dec = int((off["status"] == "Decommissioned").sum())
    kpi.append((p, len(off), n_dec, len(exist)))
print(f"sensors showing offline under the {KPI_HOURS}-hour horizon (what the dashboard displays):")
for p, n, d, e in kpi:
    print(f"  {p:%Y-%m-%d %H:%M}   {n:>3} of {e}  ({n / max(e, 1):.2%})   "
          f"{d} decommissioned, {n - d} live sensors in an outage")
assert min(n for _, n, _, _ in kpi) > 0, (
    "the 8-hour offline KPI is zero at a sampled instant -- the V1 defect this table exists "
    "to fix")
print("OK  offline share inside the band; the 8-hour KPI is non-zero at every sampled instant")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Distributions and summary

# CELL ********************

_per = tbl.groupBy("sensor_id").count().toPandas()["count"]
_fac = (gen.groupBy("facility_id").agg(F.sum(F.col("exceedance_flag").cast("int")).alias("n"))
        .toPandas().sort_values("n", ascending=False))
_mix = {r["sensor_status"]: r["count"] for r in tbl.groupBy("sensor_status").count().collect()}
_any = int(gen.filter("exceedance_flag").select("sensor_id").distinct().count())
_top = (gen.filter("exceedance_flag").groupBy("sensor_id").count().toPandas()["count"]
        .sort_values(ascending=False))
_top_share = _top.head(max(1, len(live) // 10)).sum() / max(_top.sum(), 1)

print("=" * 76)
print("CH4 DETECTOR TELEMETRY GENERATED")
print("=" * 76)
print(f"  run mode          {RUN_MODE}")
print(f"  window            {WINDOW_START.date()} .. {WINDOW_END.date()} ({WINDOW_DAYS} days), "
      f"{CH4_INTERVAL_HOURS} h cadence")
print(f"  rows              {N_TBL:,}")
print(f"  readings/sensor   min {_per.min():,}  median {int(_per.median()):,}  max {_per.max():,}")
print(f"  exceedances       {N_EXC:,}  ({EXC_RATE:.2%} of readings)")
print(f"                    on {_any} of {len(live)} live sensors; the top 10% of sensors hold "
      f"{_top_share:.0%}")
print(f"  per facility      median {int(_fac['n'].median())}, max {int(_fac['n'].max())}, "
      f"{int((_fac['n'] == 0).sum())} of {len(_fac)} facilities with none")
print("  top 10 facilities by exceedance count")
for r in _fac.head(10).itertuples():
    print(f"    {r.facility_id:<12}{r.n:>6,}")
print("  sensor_status     " + "   ".join(f"{k} {v / N_TBL:.1%}" for k, v in sorted(_mix.items())))
print(f"  offline share     {OFFLINE_REALISED:.2%}")
print()
print("  table written     sensor_telemetry (V1 contract + date_sk, is_synthetic)")
print("  not modified      dim_sensor, dim_equipment, dim_facility, fact_asset_state, "
      "scada_telemetry")
print("  not read          the detection pipeline's weather tables")
print("  determinism       every value is a function of (sensor or facility, interval_index);")
print("                    a rerun and a backfill against 30 incremental days agree, ingest_ts")
print("                    aside")

gen.unpersist()
tbl.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
