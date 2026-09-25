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

# # 02c — Roll Up SCADA Telemetry
#
# Writes **`scada_telemetry_hourly`** and **`scada_telemetry_daily`** from `scada_telemetry`.
# This notebook **modifies nothing** — not the telemetry, not any `dim_*` table.
#
# ### Window
#
# Both rollups cover **exactly** the window `scada_telemetry` covers, set by
# `ROLLUP_MATCH_RAW_WINDOW` in `01_topology_config` (the reasoning is recorded there). The
# run fails at the start if the raw table is absent or covers any other range.
#
# ### Values come from Good readings only; counts come from every reading
#
# `value_avg`, `value_min`, `value_max` and `value_stddev` are over `quality_code = 'Good'`
# readings. A Bad reading is a rail value by construction in 02b — `normal_max + rail_span` or
# below `normal_min` — and a single one would drag an hour's average far off. Substituted
# (Maintenance) and Uncertain readings are excluded too: neither is a measurement of the
# process. `sample_count` and the four quality counts cover **every** row, so `pct_good` is
# meaningful.
#
# **Nulls are intentional, and asserted.** A bucket with no Good readings still has a row —
# its readings exist, they are just not trustworthy — with `sample_count` populated and every
# value column null. `value_stddev` is also null for a single Good reading, where a sample
# standard deviation is undefined. The validation asserts both rules exactly, so a null that
# arrived any other way (an overflow, a join miss) fails the run rather than hiding in a chart.
#
# ### Missing hours produce no row
#
# An hour with no readings at all produces **no row**. Nothing is padded or fabricated; the
# absence is the signal. `expected_count` versus `sample_count` makes partial hours visible:
# 4 of 12 means the tag was mostly offline.
#
# ### Day grain is built from the hourly table, and weighted
#
# The daily rollup reads the **written** hourly table, never raw, so the two cannot disagree.
# The hourly table carries the sums behind each average — `value_sum_e6` and
# `value_sumsq_e12`, the Good values and their squares in units of 1e-6 and 1e-12 — and the day
# is computed from **summed sums**: `value_avg = Σ sum_h / Σ good_count_h`. That *is* the mean of
# hourly averages weighted by `good_count`, so an hour with 2 Good readings counts one sixth as
# much as one with 12. A flat mean of hourly averages is the usual way a two-level rollup goes
# quietly wrong, and the validation includes a check chosen specifically to catch it.
#
# **Standard deviation** cannot be recovered from hourly standard deviations, so it is computed
# from the carried sums: `n·Σx² − (Σx)²`, over `n(n−1)`. Two things make that safe here:
#
# - the sums are **exact integers** (values scaled by 1e6 — finer than the 0.001 finest tag
#   resolution, asserted — so the long and `decimal(38,0)` sums round nothing). The textbook
#   objection to this formula is cancellation in floating point; in exact arithmetic there is
#   none, and a frozen tag's standard deviation comes out as exactly 0. (In the offline
#   harness the double form gives ~3e-5 for a frozen day, and NaN wherever rounding pushes
#   the variance below zero.)
# - integer sums are **order-independent**. A double `sum()` in Spark depends on the order
#   partial aggregates are merged, which varies with file layout and task timing, so two runs
#   can differ in the last bit. These cannot. That is what makes the determinism claim below
#   literally true, not true to a tolerance.
#
# ### Run modes and the boundary
#
# `run_mode` follows 02d. **Backfill** rolls up the whole raw window. **Incremental** recomputes
# whole days from the rollup's watermark: the **last day already rolled, re-rolled**, plus
# anything after it. Buckets are always recomputed from raw, never updated.
#
# Hour buckets never cross midnight, and 02b writes raw one whole UTC day at a time with a
# `replaceWhere` on that `date_sk`, which is atomic. So every hour of a day is as complete as
# it will ever be once its day exists. The final hour of the window is complete in raw on the
# same terms — if a tag's last hour is short, it is short because the tag was offline, and it
# is reported short, not padded. Re-rolling the last rolled day is the belt to that brace: if
# the final day was ever rolled while incomplete, the next run recomputes it whole and
# produces the row a backfill would. An **older** day rewritten upstream is not detected;
# re-roll it with `start_date` / `end_date`, or backfill.

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

spark.conf.set("spark.sql.session.timeZone", "UTC")

TELEMETRY_TABLE = "scada_telemetry"
HOURLY_TABLE = "scada_telemetry_hourly"
DAILY_TABLE = "scada_telemetry_daily"

# Good values are summed as integers in units of 1/VALUE_SCALE. See the markdown above.
VALUE_SCALE = 10 ** 6
# Comparisons against raw allow one unit of that scale. Values are quantised to at least
# 0.001, so the scaled sums are exact and the real disagreement is a few ulps.
VALUE_TOL = 1.0 / VALUE_SCALE

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


def day_predicate(lo, hi):
    """date_sk over the midnights [lo, hi)."""
    return f"date_sk >= {sk(lo)} AND date_sk <= {sk(hi - pd.Timedelta(days=1))}"


assert ROLLUP_MATCH_RAW_WINDOW, (
    "02c implements only ROLLUP_MATCH_RAW_WINDOW = True -- rollups over exactly the raw window. "
    "Anything else would need a source for buckets the raw table does not hold."
)

AS_OF = pd.Timestamp(TOPOLOGY_AS_OF)
RAW_START, RAW_END = AS_OF - pd.Timedelta(days=TELEMETRY_RAW_DAYS), AS_OF
RAW_PREDICATE = day_predicate(RAW_START, RAW_END)
RAW_DAYS = [sk(RAW_START + pd.Timedelta(days=i)) for i in range(TELEMETRY_RAW_DAYS)]

assert table_exists(TELEMETRY_TABLE), (
    f"{TELEMETRY_TABLE} does not exist. 02c rolls up nothing else -- run "
    "02b_gen_scada_telemetry first."
)
_have = sorted(int(r["date_sk"]) for r in
               spark.table(TELEMETRY_TABLE).select("date_sk").distinct().collect())
if _have != RAW_DAYS:
    _miss = sorted(set(RAW_DAYS) - set(_have))
    _extra = sorted(set(_have) - set(RAW_DAYS))
    raise AssertionError(
        f"{TELEMETRY_TABLE} does not cover the expected raw window.\n"
        f"  expected  {RAW_DAYS[0]} .. {RAW_DAYS[-1]}  ({len(RAW_DAYS)} days: "
        f"TELEMETRY_RAW_DAYS back from TOPOLOGY_AS_OF {AS_OF.date()})\n"
        f"  found     "
        + (f"{_have[0]} .. {_have[-1]}  ({len(_have)} days)" if _have else "no rows") + "\n"
        f"  missing   {_miss[:6]}{' ...' if len(_miss) > 6 else ''}\n"
        f"  outside   {_extra[:6]}{' ...' if len(_extra) > 6 else ''}\n"
        "  ROLLUP_MATCH_RAW_WINDOW ties both rollups to exactly this window, so 02c will not "
        "guess which part to roll. Rerun 02b in backfill mode, or correct TOPOLOGY_AS_OF / "
        "TELEMETRY_RAW_DAYS in 01_topology_config."
    )

if RUN_MODE == "backfill":
    WINDOW_START, WINDOW_END = RAW_START, RAW_END
else:
    wm = None
    if table_exists(HOURLY_TABLE):
        wm = spark.sql(f"SELECT max(date_sk) AS m FROM {HOURLY_TABLE}").collect()[0]["m"]
    if wm is None:
        # Unlike 02d there is no one-day fallback: a one-day rollup would not cover the raw
        # window, which is the property ROLLUP_MATCH_RAW_WINDOW exists to guarantee.
        WINDOW_START, WINDOW_END = RAW_START, RAW_END
        print(f"{HOURLY_TABLE} is empty or absent -- incremental rolls up the whole raw window.")
    else:
        # the last rolled day is re-rolled, in case it was incomplete when it was rolled
        WINDOW_START = min(max(pd.Timestamp(str(int(wm))), RAW_START),
                           RAW_END - pd.Timedelta(days=1))
        WINDOW_END = RAW_END

if _start_override:
    WINDOW_START = pd.Timestamp(_start_override)
if _end_override:
    WINDOW_END = pd.Timestamp(_end_override)

WINDOW_START = pd.Timestamp(WINDOW_START).normalize()
WINDOW_END = pd.Timestamp(WINDOW_END).normalize()
assert RAW_START <= WINDOW_START < WINDOW_END <= RAW_END, (
    f"window {WINDOW_START.date()} .. {WINDOW_END.date()} is empty or outside the raw window "
    f"{RAW_START.date()} .. {RAW_END.date()}. 02c never rolls up beyond the raw window."
)
WINDOW_PREDICATE = day_predicate(WINDOW_START, WINDOW_END)
WINDOW_DAYS = int((WINDOW_END - WINDOW_START).days)

print(f"RUN_MODE={RUN_MODE}  window={WINDOW_START.date()}..{WINDOW_END.date()} "
      f"({WINDOW_DAYS} days)")
print(f"raw window {RAW_START.date()}..{RAW_END.date()} ({TELEMETRY_RAW_DAYS} days), "
      f"present in {TELEMETRY_TABLE} in full")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Hourly rollup
#
# One row per `(tag_sk, bucket_ts)` with at least one reading. The denormalised columns are
# grouping keys rather than `first()`, so a tag whose attributes varied within the window would
# produce two rows for one bucket and fail the uniqueness check instead of silently picking one.
#
# `dominant_state` is the modal `operating_state`. Ties go to the earlier state in `STATES`
# (Running first), so the choice is deterministic. The per-state counts are carried as columns
# because the day's modal state has to be computed from them — a mode of hourly modes is not
# the day's mode. `pct_time_running` is a share of readings; readings are evenly spaced, so it
# is the share of *observed* time, not of the hour when readings are missing.

# CELL ********************

tag_dim = (spark.table("dim_scada_tag").filter("is_current = true")
           .select("tag_sk", F.col("sampling_interval_seconds").cast("int").alias("cad"),
                   "resolution"))
_td = tag_dim.agg(F.count("*").alias("n"), F.countDistinct("tag_sk").alias("d"),
                  F.min("resolution").alias("res")).first()
assert _td["n"] == _td["d"], "dim_scada_tag has more than one current row for a tag_sk"
assert _td["res"] * VALUE_SCALE >= 1, (
    f"a tag resolution of {_td['res']} is finer than 1/VALUE_SCALE, so the scaled integer sums "
    "would round the values they sum. Raise VALUE_SCALE."
)
_cads = [int(r["cad"]) for r in tag_dim.select("cad").distinct().collect()]
assert all(3600 % c == 0 for c in _cads), (
    f"cadences {_cads} must divide an hour, or expected_count is not a whole number"
)
tag_dim = F.broadcast(tag_dim.drop("resolution"))

KEYS = ["tag_sk", "tag_id", "equipment_sk", "area_sk", "facility_sk", "uom"]
STATE_COLS = {s: f"n_{s.lower()}" for s in STATES}
COUNT_COLS = (["sample_count", "good_count", "bad_count", "uncertain_count",
               "substituted_count"] + list(STATE_COLS.values()))
good = F.col("quality_code") == "Good"


def n_where(cond):
    return F.sum(F.when(cond, 1).otherwise(0)).cast("int")


def finish(df):
    """The derived columns, identical at both grains, from counts and exact sums."""
    n = F.col("good_count")
    s1 = F.col("value_sum_e6").cast("decimal(38,0)")
    # n*sum(x^2) - sum(x)^2, in exact integers: >= 0, and exactly 0 for a constant series
    num = n.cast("decimal(38,0)") * F.col("value_sumsq_e12") - s1 * s1
    return (df
            .withColumn("value_avg", F.when(n > 0, F.col("value_sum_e6").cast("double")
                                            / n / F.lit(float(VALUE_SCALE))))
            .withColumn("value_stddev", F.when(n > 1, F.sqrt(
                num.cast("double") / (n.cast("double") * (n - 1)))
                / F.lit(float(VALUE_SCALE))))
            .withColumn("pct_good", F.col("good_count") / F.col("sample_count"))
            .withColumn("pct_time_running", F.col("n_running") / F.col("sample_count"))
            .withColumn("dominant_state", F.greatest(*[
                F.struct(F.col(c).alias("n"), F.lit(-i).alias("rank"), F.lit(s).alias("state"))
                for i, (s, c) in enumerate(STATE_COLS.items())]).getField("state"))
            .withColumn("date_sk", F.date_format("bucket_ts", "yyyyMMdd").cast("long"))
            .withColumn("is_synthetic", F.lit(True)))


raw = spark.table(TELEMETRY_TABLE).filter(WINDOW_PREDICATE)
xi = F.when(good, F.round(F.col("value_num") * F.lit(float(VALUE_SCALE))).cast("long"))

hourly = (raw.withColumn("bucket_ts", F.date_trunc("hour", "reading_ts"))
          .groupBy(*KEYS, "bucket_ts")
          .agg(F.count(F.lit(1)).cast("int").alias("sample_count"),
               n_where(good).alias("good_count"),
               n_where(F.col("quality_code") == "Bad").alias("bad_count"),
               n_where(F.col("quality_code") == "Uncertain").alias("uncertain_count"),
               n_where(F.col("quality_code") == "Substituted").alias("substituted_count"),
               *[n_where(F.col("operating_state") == s).alias(c)
                 for s, c in STATE_COLS.items()],
               n_where(good & F.col("value_num").isNull()).alias("_good_null"),
               F.min(F.when(good, F.col("value_num"))).alias("value_min"),
               F.max(F.when(good, F.col("value_num"))).alias("value_max"),
               F.sum(xi).alias("value_sum_e6"),
               F.sum(xi.cast("decimal(20,0)") * xi.cast("decimal(20,0)"))
                .cast("decimal(38,0)").alias("value_sumsq_e12"))
          # left, so a reading whose tag is missing from the dimension is still counted --
          # and then fails the FK check -- rather than silently dropping out of sample_count
          .join(tag_dim, "tag_sk", "left")
          .withColumn("expected_count", (F.lit(3600) / F.col("cad")).cast("int"))
          .drop("cad"))
hourly = finish(hourly)

SHARED = ["value_avg", "value_min", "value_max", "value_stddev", "sample_count",
          "expected_count"]
TAIL = ["good_count", "bad_count", "uncertain_count", "substituted_count", "pct_good",
        "pct_time_running", "dominant_state", *STATE_COLS.values(), "value_sum_e6",
        "value_sumsq_e12", "uom", "is_synthetic"]
HEAD = ["tag_sk", "tag_id", "equipment_sk", "area_sk", "facility_sk", "bucket_ts", "date_sk"]
HOURLY_SCHEMA = HEAD + SHARED + TAIL
DAILY_SCHEMA = HEAD + SHARED + ["hours_present"] + TAIL

hourly_src = hourly.persist()
_gn = hourly_src.agg(F.sum("_good_null")).first()[0] or 0
assert _gn == 0, (
    f"{_gn} Good reading(s) carry a null value_num. good_count would then over-count the "
    "values behind value_avg; 02b must never emit that."
)
hourly_out = hourly_src.select(*HOURLY_SCHEMA)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write the hourly table, then roll the day from what was written
#
# `replaceWhere` on the run window (§2.2 of the design note): re-running a day replaces that
# day and never adds to it. The daily rollup is then built from the hourly **table**, not from
# the in-memory frame, so what the day aggregates is exactly what a reader of the hourly table
# sees. Finally, any rollup partition outside the raw window is deleted; that only happens
# after `TOPOLOGY_AS_OF` has moved, and it is what keeps "exactly the raw window" true.

# CELL ********************


def write_rollup(df, table, predicate):
    n = df.count()
    _oos = df.filter(f"NOT ({predicate})").count()
    assert _oos == 0, f"{table}: {_oos} row(s) outside the replaceWhere predicate {predicate}"
    _d = df.select("tag_sk", "bucket_ts").distinct().count()
    assert _d == n, f"{table}: {n - _d} duplicate (tag_sk, bucket_ts) before the write"
    w = (df.write.format("delta").mode("overwrite").partitionBy("date_sk"))
    if table_exists(table):
        w.option("replaceWhere", predicate).saveAsTable(table)
    else:
        w.option("overwriteSchema", "true").saveAsTable(table)
    print(f"{table}: {n:,} rows written (replaceWhere {predicate})")
    return n


N_HOURLY_WRITTEN = write_rollup(hourly_out, HOURLY_TABLE, WINDOW_PREDICATE)
hourly_src.unpersist()

daily = (spark.table(HOURLY_TABLE).filter(WINDOW_PREDICATE)
         .withColumn("bucket_ts", F.date_trunc("day", "bucket_ts"))
         .groupBy(*KEYS, "bucket_ts")
         .agg(*[F.sum(c).cast("int").alias(c) for c in COUNT_COLS],
              F.count(F.lit(1)).cast("int").alias("hours_present"),
              (F.max("expected_count") * 24).cast("int").alias("expected_count"),
              F.min("value_min").alias("value_min"),
              F.max("value_max").alias("value_max"),
              F.sum("value_sum_e6").alias("value_sum_e6"),
              F.sum("value_sumsq_e12").cast("decimal(38,0)").alias("value_sumsq_e12")))
daily_out = finish(daily).select(*DAILY_SCHEMA)
N_DAILY_WRITTEN = write_rollup(daily_out, DAILY_TABLE, WINDOW_PREDICATE)

for _t in (HOURLY_TABLE, DAILY_TABLE):
    _stale = spark.table(_t).filter(f"NOT ({RAW_PREDICATE})").count()
    if _stale:
        spark.sql(f"DELETE FROM {_t} WHERE NOT ({RAW_PREDICATE})")
        print(f"{_t}: deleted {_stale:,} row(s) outside the raw window")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation — every check fails the run, none warns

# CELL ********************

h_tbl = spark.table(HOURLY_TABLE).filter(WINDOW_PREDICATE).persist()
d_tbl = spark.table(DAILY_TABLE).filter(WINDOW_PREDICATE).persist()
N_RAW, N_H, N_D = raw.count(), h_tbl.count(), d_tbl.count()

# --- 1-2. every reading counted exactly once, at both grains ----------------------------------
h_sum = h_tbl.agg(F.sum("sample_count")).first()[0] or 0
d_sum = d_tbl.agg(F.sum("sample_count")).first()[0] or 0
assert h_sum == N_RAW, f"hourly sample_count sums to {h_sum:,}, raw has {N_RAW:,} rows"
assert d_sum == h_sum, f"daily sample_count sums to {d_sum:,}, hourly to {h_sum:,}"
print(f"OK  sample_count sums to the raw row count exactly: {N_RAW:,} (hourly and daily)")

for _name, _df in (("hourly", h_tbl), ("daily", d_tbl)):
    # --- 3. grain -------------------------------------------------------------------------------
    _dup = _df.groupBy("tag_sk", "bucket_ts").count().filter("count > 1").limit(5).collect()
    assert not _dup, f"{_name}: (tag_sk, bucket_ts) not unique: {[tuple(r) for r in _dup]}"

    # --- the counts partition sample_count -------------------------------------------------------
    _q = _df.filter("good_count + bad_count + uncertain_count + substituted_count "
                    "<> sample_count").count()
    _s = _df.filter(" + ".join(STATE_COLS.values()) + " <> sample_count").count()
    assert _q == 0, f"{_name}: {_q} bucket(s) where the quality counts do not sum to " \
                    "sample_count -- a quality_code outside Good/Bad/Uncertain/Substituted"
    assert _s == 0, f"{_name}: {_s} bucket(s) where the state counts do not sum to " \
                    "sample_count -- an operating_state outside STATES"

    # --- nulls are exactly the intended ones ---------------------------------------------------
    _n1 = _df.filter("(good_count = 0) <> (value_avg IS NULL) OR (good_count = 0) <> "
                     "(value_min IS NULL) OR (good_count = 0) <> (value_max IS NULL)").count()
    _n2 = _df.filter("(good_count < 2) <> (value_stddev IS NULL)").count()
    assert _n1 == 0, f"{_name}: {_n1} bucket(s) with a null value column that good_count " \
                     "does not explain, or a value with no Good reading behind it"
    assert _n2 == 0, f"{_name}: {_n2} bucket(s) where value_stddev is null other than for " \
                     "good_count < 2 -- check the decimal sums for overflow"

    # --- 4. min <= avg <= max -------------------------------------------------------------------
    _b = _df.filter(f"value_avg IS NOT NULL AND (value_avg < value_min - {VALUE_TOL} "
                    f"OR value_avg > value_max + {VALUE_TOL})").limit(5).collect()
    assert not _b, f"{_name}: value_avg outside [value_min, value_max]: " \
                   f"{[(r['tag_sk'], str(r['bucket_ts'])) for r in _b]}"

    # --- 7. never more readings than the cadence allows -----------------------------------------
    _o = _df.filter("sample_count > expected_count OR expected_count IS NULL").count()
    assert _o == 0, f"{_name}: {_o} bucket(s) with sample_count > expected_count, or no cadence"

# --- 8. every tag resolves ----------------------------------------------------------------------
known = F.broadcast(spark.table("dim_scada_tag").select("tag_sk").distinct())
for _name, _df in (("hourly", h_tbl), ("daily", d_tbl)):
    _orph = _df.join(known, "tag_sk", "left_anti").select("tag_sk").distinct().limit(5).collect()
    assert not _orph, f"{_name}: tag_sk not in dim_scada_tag: {[r['tag_sk'] for r in _orph]}"

# --- coverage: both tables hold exactly the raw window's days -----------------------------------
for _t in (HOURLY_TABLE, DAILY_TABLE):
    _days = sorted(int(r["date_sk"]) for r in
                   spark.table(_t).select("date_sk").distinct().collect())
    assert _days == RAW_DAYS, (
        f"{_t} covers {len(_days)} days ({_days[:1]} .. {_days[-1:]}), not the raw window "
        f"{RAW_DAYS[0]} .. {RAW_DAYS[-1]}. Run a backfill."
    )
print("OK  grain unique; counts partition sample_count; nulls only where good_count < 1 (< 2 "
      "for stddev)")
print("OK  min <= avg <= max; sample_count <= expected_count; every tag_sk resolves; both "
      "tables cover exactly the raw window")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Recompute from raw
#
# The hourly check samples 20 tag-hours at random. The daily check does not sample at random,
# because a random tag-day is one where most hours have the same `good_count`, and there a flat
# mean of hourly averages and the correct weighted mean agree — the check would pass on the
# bug it exists to catch. So it samples from tag-days where the two **differ**, selected from
# raw independently of the rollup under test, and asserts such days exist.

# CELL ********************

good_raw = raw.filter(good)

# --- 5. twenty tag-hours ------------------------------------------------------------------------
s_h = (h_tbl.filter("good_count > 0").orderBy(F.xxhash64("tag_sk", "bucket_ts")).limit(20)
       .select("tag_sk", "bucket_ts", "value_avg"))
r_h = (good_raw.withColumn("bucket_ts", F.date_trunc("hour", "reading_ts"))
       .join(F.broadcast(s_h.select("tag_sk", "bucket_ts")), ["tag_sk", "bucket_ts"])
       .groupBy("tag_sk", "bucket_ts").agg(F.avg("value_num").alias("raw_avg")))
cmp_h = s_h.join(r_h, ["tag_sk", "bucket_ts"], "left").collect()
_bad = [r for r in cmp_h if r["raw_avg"] is None or abs(r["raw_avg"] - r["value_avg"]) > VALUE_TOL]
assert len(cmp_h) == 20 and not _bad, f"hourly value_avg disagrees with raw: {_bad[:3]}"
print(f"OK  20 tag-hours: value_avg matches raw to within {VALUE_TOL:g}")

# --- 6. twenty tag-days where weighting matters -------------------------------------------------
r_d = (good_raw.withColumn("bucket_ts", F.date_trunc("day", "reading_ts"))
       .groupBy("tag_sk", "bucket_ts")
       .agg(F.avg("value_num").alias("raw_avg"), F.stddev_samp("value_num").alias("raw_sd")))
flat = (good_raw.withColumn("h", F.date_trunc("hour", "reading_ts"))
        .groupBy("tag_sk", "h").agg(F.avg("value_num").alias("h_avg"))
        .withColumn("bucket_ts", F.date_trunc("day", "h"))
        .groupBy("tag_sk", "bucket_ts").agg(F.avg("h_avg").alias("flat_avg")))
disc = (r_d.join(flat, ["tag_sk", "bucket_ts"])
        .filter(F.abs(F.col("raw_avg") - F.col("flat_avg")) > F.lit(100 * VALUE_TOL)))
N_DISC = disc.count()
assert N_DISC >= 20, (
    f"only {N_DISC} tag-day(s) where a flat mean of hourly averages differs from the weighted "
    "mean, so the weighting check below would not tell them apart"
)
s_d = (disc.orderBy(F.xxhash64("tag_sk", "bucket_ts")).limit(20)
       .join(d_tbl.select("tag_sk", "bucket_ts", "value_avg", "value_stddev"),
             ["tag_sk", "bucket_ts"], "left").collect())
_bad = [r for r in s_d if r["value_avg"] is None or r["value_stddev"] is None
        or abs(r["value_avg"] - r["raw_avg"]) > VALUE_TOL
        or abs(r["value_stddev"] - r["raw_sd"]) > VALUE_TOL + 1e-9 * r["raw_sd"]]
assert not _bad, (
    f"daily value_avg or value_stddev disagrees with raw: {_bad[:3]}. If value_avg equals "
    "flat_avg, the day is a flat mean of hourly averages -- it must be weighted by good_count."
)
_gap = max(abs(r["raw_avg"] - r["flat_avg"]) for r in s_d)
print(f"OK  20 tag-days: weighted value_avg and value_stddev match raw to within {VALUE_TOL:g};")
print(f"    on these days a flat mean would be off by up to {_gap:.4g}, so the check has teeth "
      f"({N_DISC:,} such tag-days in the window)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary

# CELL ********************


def completeness(df):
    r = F.col("sample_count") / F.col("expected_count")
    return (df.withColumn("b", F.when(r >= 1, "full")
                          .when(r >= 0.75, "75-99%").when(r >= 0.5, "50-74%")
                          .when(r >= 0.25, "25-49%").otherwise("<25%"))
            .groupBy("b").count().collect())


_order = ["full", "75-99%", "50-74%", "25-49%", "<25%"]
no_good_h = h_tbl.filter("good_count = 0").count()
no_good_d = d_tbl.filter("good_count = 0").count()
_hp = {r["hours_present"]: r["count"] for r in d_tbl.groupBy("hours_present").count().collect()}

print("=" * 76)
print("SCADA TELEMETRY ROLLED UP")
print("=" * 76)
print(f"  run mode          {RUN_MODE}")
print(f"  window            {WINDOW_START.date()} .. {WINDOW_END.date()} ({WINDOW_DAYS} days)")
print(f"  raw rows          {N_RAW:,}")
print(f"  hourly rows       {N_H:,}   compression {N_RAW / max(N_H, 1):.1f}x against raw")
print(f"  daily rows        {N_D:,}   compression {N_RAW / max(N_D, 1):.1f}x against raw")
print()
print("  bucket completeness (sample_count / expected_count)")
for _name, _df in (("hourly", h_tbl), ("daily", d_tbl)):
    _c = {r["b"]: r["count"] for r in completeness(_df)}
    _tot = max(sum(_c.values()), 1)
    print(f"    {_name:<8}" + "".join(f"{b:>8} {_c.get(b, 0) / _tot:>6.1%}" for b in _order))
print(f"  daily hours_present = 24 on {_hp.get(24, 0) / max(N_D, 1):.1%} of tag-days")
print()
print(f"  buckets with no Good reading (values null, sample_count populated)")
print(f"    hourly {no_good_h:,}   daily {no_good_d:,}")
print()
print("  tables written    scada_telemetry_hourly, scada_telemetry_daily")
print("  not modified      scada_telemetry, every dim_* table")
print("  missing hours     no row -- nothing is padded or fabricated")
print()
print("  determinism       both rollups are a pure function of the raw table, and every sum")
print("                    is an exact integer, so a rerun and a backfill against 30")
print("                    incremental days produce bitwise-identical tables")

h_tbl.unpersist()
d_tbl.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
