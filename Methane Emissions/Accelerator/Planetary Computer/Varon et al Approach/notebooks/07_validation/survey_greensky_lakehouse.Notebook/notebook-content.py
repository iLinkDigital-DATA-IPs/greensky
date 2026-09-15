# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {}
# META   }
# META }

# MARKDOWN ********************

# # ⚠ Attach `GreenSky_Lakehouse` before running
#
# **This notebook has no lakehouse bound.** It profiles `GreenSky_Lakehouse`, which lives in
# workspace `060ba34b-f1a3-4509-a6e2-36d1e736a8eb` — **a different workspace from
# Green Sky - Dev (`640876ea-6158-4ffd-8598-5eb210e088a0`)**, where this notebook syncs. That
# is why the binding is left empty: a cross-workspace lakehouse GUID in source would not
# survive Git sync cleanly.
#
# Before running, attach `GreenSky_Lakehouse` manually in the Fabric UI
# (**Explorer → Lakehouses → Add**). Without it every `spark.table(...)` call fails, though
# the notebook degrades gracefully — each table is reported as `MISSING` rather than raising.
#
# The notebook resolves tables through the `LAKEHOUSE = "GreenSky_Lakehouse"` constant in
# Cell 1, not through the default-lakehouse binding, so the name in that constant is what
# must match whatever you attach.
#
# **It writes nothing.** No `saveAsTable`, no `write`, no DDL, no temp views — every cell
# only reads and prints. It is safe to run against the live lakehouse.

# MARKDOWN ********************

# # survey_greensky_lakehouse
#
# **Read-only profiling of `GreenSky_Lakehouse`.** This notebook writes nothing — no
# `saveAsTable`, no `write`, no temp views persisted. Every cell only reads and prints.
#
# Companion to `GREENSKY_LAKEHOUSE_SURVEY.md` at the repo root, which covers the static
# analysis. This notebook answers the questions that static analysis cannot:
#
# 1. Which tables actually exist, in which schemas, and how many rows do they hold?
# 2. Is `bronze.scada_realtime` a real time series or a stub? What is its true sampling
#    interval, and how many tags and facilities does it cover?
# 3. Is `bronze.facility_master` real or synthetic?
# 4. Do the `gold` dimensions hold rows at all, given that every delta write for them is
#    commented out in `Nb_Gold`?
#
# Tables are discovered with `SHOW TABLES IN <schema>`, not hard-coded. Missing schemas and
# unreadable tables are reported and skipped, never fatal.

# CELL ********************

# CELL 1 — Configuration and helpers
#
# Nothing below writes. The only Spark actions are counts, aggregations and small collects.

from pyspark.sql import functions as F
from pyspark.sql.types import (
    NumericType, StringType, TimestampType, DateType, BooleanType, BinaryType,
    ArrayType, MapType, StructType,
)
from pyspark.sql.window import Window

LAKEHOUSE = "GreenSky_Lakehouse"

# Schemas we always probe even if discovery misses them. Discovery results are unioned in.
CANDIDATE_SCHEMAS = ["bronze", "silver", "gold", "dbo"]

TOPK = 5                      # most-common values to show per string column
APPROX_DISTINCT_RSD = 0.02    # relative standard deviation for approx_count_distinct
TOPK_SAMPLE_ROWS = 2_000_000  # above this, the top-K pass runs on a sample
MAX_TS_VALUES = 500_000       # cap on distinct timestamps used for interval analysis

# Verdict thresholds for the final summary
STUB_MAX_ROWS = 25            # <= this many rows and it is a stub, not a dataset
SPARSE_NULL_RATE = 0.50       # mean null rate above this and the table is mostly empty

# Column-name hints for "looks like a timestamp" on non-native types
TS_NAME_HINTS = ("time", "date", "_ts", "ts_", "timestamp", "_at", "datetime")


def safe(fn, default=None, label=""):
    """Run fn(); on any exception print a short note and return `default`."""
    try:
        return fn()
    except Exception as exc:
        if label:
            print(f"      !! {label}: {type(exc).__name__}: {str(exc)[:180]}")
        return default


def is_numeric(dt):
    return isinstance(dt, NumericType) and not isinstance(dt, BooleanType)


def is_temporal(dt):
    return isinstance(dt, (TimestampType, DateType))


def is_complex(dt):
    return isinstance(dt, (ArrayType, MapType, StructType, BinaryType))


def looks_like_timestamp(name, dt):
    """Native date/timestamp, or a string/int column whose name hints at time."""
    if is_temporal(dt):
        return True
    lowered = name.lower()
    if not any(hint in lowered for hint in TS_NAME_HINTS):
        return False
    return isinstance(dt, StringType) or is_numeric(dt)


def fmt(v, width=None):
    if v is None:
        return "NULL"
    if isinstance(v, float):
        s = f"{v:,.4f}".rstrip("0").rstrip(".")
    elif isinstance(v, int):
        s = f"{v:,}"
    else:
        s = str(v)
    if width and len(s) > width:
        s = s[: width - 1] + "…"
    return s


def human_seconds(secs):
    """Render a second count as the most readable unit."""
    if secs is None:
        return "n/a"
    secs = float(secs)
    if secs < 1:
        return f"{secs:.3f}s"
    if secs < 90:
        return f"{secs:.0f}s"
    if secs < 5400:
        return f"{secs / 60:.1f} min"
    if secs < 172800:
        return f"{secs / 3600:.2f} h"
    return f"{secs / 86400:.2f} d"


# Populated by the cells below; read by the summary cell at the end.
PROFILE = {}

print("Read-only survey of", LAKEHOUSE)
print("This notebook performs no writes.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 2 — Discover schemas and tables
#
# Schemas come from `SHOW SCHEMAS`, falling back through `SHOW DATABASES` and finally the
# hard-coded candidate list. Tables come from `SHOW TABLES IN <schema>` in every case.

# CELL ********************

# CELL 2 — Discovery

def discover_schemas():
    statements = [
        f"SHOW SCHEMAS IN {LAKEHOUSE}",
        f"SHOW DATABASES IN {LAKEHOUSE}",
        "SHOW SCHEMAS",
        "SHOW DATABASES",
    ]
    for stmt in statements:
        rows = safe(lambda s=stmt: spark.sql(s).collect())
        if rows:
            names = [r[0] for r in rows if r[0]]
            print(f"  discovered {len(names)} schema(s) via: {stmt}")
            return names
    print("  !! no SHOW SCHEMAS variant succeeded — falling back to the candidate list")
    return []


def discover_tables(schema):
    """Return table names in `schema`, or [] if the schema is absent/unreadable."""
    for stmt in (f"SHOW TABLES IN {LAKEHOUSE}.{schema}", f"SHOW TABLES IN {schema}"):
        rows = safe(lambda s=stmt: spark.sql(s).collect())
        if rows is not None:
            out = []
            for r in rows:
                d = r.asDict()
                # Column is tableName on Spark, table_name on some endpoints.
                name = d.get("tableName") or d.get("table_name")
                if name is None:
                    name = r[1] if len(r) > 1 else r[0]
                if d.get("isTemporary") or d.get("is_temporary"):
                    continue
                out.append(name)
            return sorted(out)
    return []


discovered = discover_schemas()
schemas = sorted(set(discovered) | set(CANDIDATE_SCHEMAS))

INVENTORY = []
MISSING_SCHEMAS = []

print()
for schema in schemas:
    tables = discover_tables(schema)
    if not tables:
        MISSING_SCHEMAS.append(schema)
        print(f"  {schema:<10} — absent or empty")
        continue
    print(f"  {schema:<10} — {len(tables)} table(s): {', '.join(tables)}")
    for t in tables:
        INVENTORY.append((schema, t))

print()
print(f"Inventory: {len(INVENTORY)} table(s) across {len(schemas) - len(MISSING_SCHEMAS)} schema(s)")
if MISSING_SCHEMAS:
    print(f"Skipped (absent or empty): {', '.join(MISSING_SCHEMAS)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 3 — Row count, column count, full schema
#
# One `count()` per table. Tables that cannot be read are recorded as `MISSING` and skipped
# by every later cell.

# CELL ********************

# CELL 3 — Shape and schema

for schema, table in INVENTORY:
    fqn = f"{LAKEHOUSE}.{schema}.{table}"
    print("=" * 78)
    print(f"{schema}.{table}")
    print("=" * 78)

    df = safe(lambda f=fqn: spark.table(f), label=f"read {fqn}")
    if df is None:
        PROFILE[fqn] = {"schema": schema, "table": table, "status": "MISSING"}
        print("  UNREADABLE — skipped for the rest of the survey")
        print()
        continue

    n_rows = safe(lambda d=df: d.count(), default=None, label="count")
    fields = df.schema.fields

    PROFILE[fqn] = {
        "schema": schema,
        "table": table,
        "status": "OK",
        "row_count": n_rows,
        "col_count": len(fields),
        "fields": [(f.name, f.dataType.simpleString()) for f in fields],
        "columns": {},
        "ts_ranges": {},
    }

    print(f"  rows: {fmt(n_rows)}    columns: {len(fields)}")
    print("  schema:")
    width = max((len(f.name) for f in fields), default=10)
    for f in fields:
        print(f"    {f.name:<{width}}  {f.dataType.simpleString()}")
    print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 4 — Per-column profile
#
# Null rate and distinct count for every column; min / median / max for numerics; the five
# most common values for strings.
#
# Distinct counts use `approx_count_distinct` (rsd 2 %) in a single aggregation pass —
# exact `countDistinct` on every column of a large table is a shuffle per column. The
# `scada_realtime` cell below re-does the counts that matter **exactly**.
#
# Complex columns (struct / array / map / binary) get null rate only.

# CELL ********************

# CELL 4 — Column profiling

for fqn, info in PROFILE.items():
    if info["status"] != "OK":
        continue

    df = safe(lambda f=fqn: spark.table(f))
    if df is None:
        continue

    n_rows = info["row_count"] or 0
    print("=" * 78)
    print(f"{info['schema']}.{info['table']}   ({fmt(n_rows)} rows)")
    print("=" * 78)

    if n_rows == 0:
        print("  empty — no column statistics")
        print()
        continue

    fields = df.schema.fields

    # --- one aggregation pass for null counts, distincts and numeric stats -------
    exprs, plan = [], []
    for i, f in enumerate(fields):
        c = F.col(f"`{f.name}`")
        exprs.append(F.count(F.when(c.isNull(), F.lit(1))).alias(f"n_{i}"))
        plan.append(("null", i, f.name))
        if not is_complex(f.dataType):
            exprs.append(F.approx_count_distinct(c, APPROX_DISTINCT_RSD).alias(f"d_{i}"))
            plan.append(("dist", i, f.name))
        if is_numeric(f.dataType):
            exprs.append(F.min(c).alias(f"mn_{i}"))
            exprs.append(F.max(c).alias(f"mx_{i}"))
            exprs.append(F.percentile_approx(c, 0.5).alias(f"md_{i}"))
            plan.extend([("min", i, f.name), ("max", i, f.name), ("med", i, f.name)])

    agg = safe(lambda d=df, e=exprs: d.agg(*e).collect()[0], label="aggregate")
    if agg is None:
        print("  aggregation failed — skipping column statistics")
        print()
        continue

    stats = {f.name: {} for f in fields}
    for kind, i, name in plan:
        key = {"null": "n_", "dist": "d_", "min": "mn_", "max": "mx_", "med": "md_"}[kind]
        stats[name][kind] = agg[f"{key}{i}"]

    # --- top-K for string columns (one small job per column) ---------------------
    string_cols = [f.name for f in fields if isinstance(f.dataType, StringType)]
    topk_src = df
    sampled = False
    if string_cols and n_rows > TOPK_SAMPLE_ROWS:
        frac = min(1.0, TOPK_SAMPLE_ROWS / float(n_rows))
        topk_src = df.sample(withReplacement=False, fraction=frac, seed=42)
        sampled = True

    top_values = {}
    for name in string_cols:
        rows = safe(
            lambda n=name, s=topk_src: (
                s.groupBy(F.col(f"`{n}`").alias("v"))
                 .count()
                 .orderBy(F.desc("count"), F.col("v"))
                 .limit(TOPK)
                 .collect()
            ),
            default=[],
        )
        top_values[name] = [(r["v"], r["count"]) for r in rows]

    # --- print -------------------------------------------------------------------
    width = max((len(f.name) for f in fields), default=10)
    for f in fields:
        s = stats[f.name]
        nulls = s.get("null") or 0
        rate = nulls / n_rows if n_rows else 0.0
        dist = s.get("dist")
        dist_s = "n/a" if dist is None else f"~{fmt(dist)}"
        print(f"  {f.name:<{width}}  {f.dataType.simpleString():<14}"
              f"  nulls {nulls:>9,} ({rate:6.1%})   distinct {dist_s}")

        if is_numeric(f.dataType):
            print(f"  {'':<{width}}    min {fmt(s.get('min'))}"
                  f"   median {fmt(s.get('med'))}"
                  f"   max {fmt(s.get('max'))}")
        elif isinstance(f.dataType, StringType):
            vals = top_values.get(f.name, [])
            if vals:
                tag = " (sampled)" if sampled else ""
                rendered = ", ".join(f"{fmt(v, 32)!r}×{c:,}" for v, c in vals)
                print(f"  {'':<{width}}    top {TOPK}{tag}: {rendered}")

        info["columns"][f.name] = {
            "type": f.dataType.simpleString(),
            "nulls": nulls,
            "null_rate": rate,
            "approx_distinct": dist,
            "min": s.get("min"),
            "median": s.get("med"),
            "max": s.get("max"),
            "top": top_values.get(f.name),
        }
    print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 5 — Timestamp columns
#
# For every column that looks like a timestamp — native `date` / `timestamp`, plus string and
# integer columns whose name hints at time and which parse — report min, max, span, and the
# **modal interval between consecutive values**.
#
# The interval is computed over *distinct* values rather than rows, so a table with many rows
# per timestamp (`scada_realtime` has one row per tag per tick) still reports the true clock
# cadence rather than zero.

# CELL ********************

# CELL 5 — Timestamp columns

def to_ts(col, dt):
    """Best-effort cast of a column to timestamp, or None if it cannot be parsed."""
    if isinstance(dt, TimestampType):
        return col
    if isinstance(dt, DateType):
        return col.cast("timestamp")
    if isinstance(dt, StringType):
        # coalesce over the two shapes seen in this lakehouse
        return F.coalesce(
            F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(col, "yyyy-MM-dd"),
            F.to_timestamp(col),
        )
    if is_numeric(dt):
        # YYYYMMDD integers, as used by date_key
        return F.to_timestamp(col.cast("string"), "yyyyMMdd")
    return None


def timestamp_profile(df, name, dt, label_prefix="  "):
    """Print min/max/span/modal-interval for one column. Returns (min, max) or None."""
    col = F.col(f"`{name}`")
    ts = to_ts(col, dt)
    if ts is None:
        return None

    parsed = safe(lambda: df.select(ts.alias("ts")).where(F.col("ts").isNotNull()))
    if parsed is None:
        return None

    # Bounds come from the FULL column — never from the capped subset below, or a wide
    # table's reported date range would be whatever rows the limit happened to return.
    bounds = safe(
        lambda p=parsed: p.agg(
            F.min("ts").alias("lo"),
            F.max("ts").alias("hi"),
            F.count("ts").alias("n"),
            F.approx_count_distinct("ts", APPROX_DISTINCT_RSD).alias("nd"),
        ).collect()[0]
    )
    if bounds is None or bounds["n"] == 0:
        print(f"{label_prefix}{name}: no parseable values")
        return None

    lo, hi, n_vals, n_distinct = bounds["lo"], bounds["hi"], bounds["n"], bounds["nd"]
    span_days = (hi - lo).total_seconds() / 86400.0 if lo and hi else 0.0
    print(f"{label_prefix}{name} [{dt.simpleString()}]")
    print(f"{label_prefix}  min {lo}   max {hi}   span {span_days:,.1f} d"
          f"   non-null {n_vals:,}   distinct ~{n_distinct:,}")

    if n_distinct < 2:
        print(f"{label_prefix}  modal interval: n/a (fewer than 2 distinct values)")
        return (lo, hi)

    # Interval analysis runs on distinct values only — a table with many rows per tick
    # would otherwise report a modal gap of zero — and is capped for cost.
    vals = parsed.distinct().limit(MAX_TS_VALUES)
    if n_distinct >= MAX_TS_VALUES:
        print(f"{label_prefix}  (interval measured on {MAX_TS_VALUES:,} distinct values)")

    w = Window.orderBy("ts")
    deltas = safe(
        lambda v=vals: (
            v.withColumn("prev", F.lag("ts").over(w))
             .where(F.col("prev").isNotNull())
             .withColumn("d", F.col("ts").cast("long") - F.col("prev").cast("long"))
             .groupBy("d").count()
             .orderBy(F.desc("count"))
             .limit(3)
             .collect()
        ),
        default=[],
        label="interval",
    )
    if deltas:
        total = sum(r["count"] for r in deltas)
        modal = deltas[0]
        print(f"{label_prefix}  modal interval: {human_seconds(modal['d'])}"
              f"  ({modal['count']:,} of the top-3 total {total:,} gaps)")
        if len(deltas) > 1:
            others = "; ".join(f"{human_seconds(r['d'])} ×{r['count']:,}" for r in deltas[1:])
            print(f"{label_prefix}  next most common: {others}")
    return (lo, hi)


for fqn, info in PROFILE.items():
    if info["status"] != "OK" or not info.get("row_count"):
        continue
    df = safe(lambda f=fqn: spark.table(f))
    if df is None:
        continue

    candidates = [(f.name, f.dataType) for f in df.schema.fields
                  if looks_like_timestamp(f.name, f.dataType)]
    if not candidates:
        continue

    print("=" * 78)
    print(f"{info['schema']}.{info['table']} — {len(candidates)} time-like column(s)")
    print("=" * 78)
    for name, dt in candidates:
        rng = timestamp_profile(df, name, dt)
        if rng:
            info["ts_ranges"][name] = rng
        print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 6 — `bronze.scada_realtime` deep dive
#
# The question this cell exists to answer: **is this a real time series or a stub?** The Data
# Agent's `userDescription` claims 15-minute SCADA intervals; nothing in the repo corroborates
# that.
#
# Counts here are **exact**, not approximate. The sampling interval is measured per series —
# one `(facility_id, equipment_tag, measurement_type)` triple — because a global interval over
# interleaved tags would understate it.

# CELL ********************

# CELL 6 — scada_realtime

SCADA_FQN = f"{LAKEHOUSE}.bronze.scada_realtime"
scada_info = PROFILE.get(SCADA_FQN)

print("=" * 78)
print("bronze.scada_realtime — real time series or stub?")
print("=" * 78)

scada = safe(lambda: spark.table(SCADA_FQN), label="read scada_realtime")

if scada is None:
    print("  table not present or unreadable — nothing to report")
elif not (scada_info and scada_info.get("row_count")):
    print("  table exists but holds 0 rows — this is an empty shell")
else:
    cols = set(scada.columns)
    n_rows = scada_info["row_count"]
    print(f"  total rows: {n_rows:,}")
    print()

    # --- exact cardinalities ------------------------------------------------------
    def exact_distinct(col_names, label):
        present = [c for c in col_names if c in cols]
        if len(present) != len(col_names):
            missing = set(col_names) - set(present)
            print(f"  {label:<44} column(s) absent: {', '.join(sorted(missing))}")
            return None
        v = safe(
            lambda: scada.select(*[F.col(f"`{c}`") for c in present]).distinct().count(),
            label=label,
        )
        if v is not None:
            print(f"  {label:<44} {v:,}")
        return v

    n_tags = exact_distinct(["equipment_tag"], "distinct equipment_tag (sensors/tags)")
    n_types = exact_distinct(["measurement_type"], "distinct measurement_type")
    n_fac = exact_distinct(["facility_id"], "distinct facility_id")
    n_series = exact_distinct(
        ["facility_id", "equipment_tag", "measurement_type"],
        "distinct series (facility x tag x type)",
    )
    exact_distinct(["source_system"], "distinct source_system")
    exact_distinct(["quality_code"], "distinct quality_code")

    # --- measurement_type breakdown ----------------------------------------------
    if "measurement_type" in cols:
        print()
        print("  rows by measurement_type:")
        rows = safe(
            lambda: (scada.groupBy("measurement_type").count()
                          .orderBy(F.desc("count")).limit(20).collect()),
            default=[],
        )
        for r in rows:
            share = r["count"] / n_rows if n_rows else 0
            print(f"    {str(r['measurement_type']):<24} {r['count']:>12,}  ({share:5.1%})")

    # --- date range ---------------------------------------------------------------
    print()
    for tcol in ("timestamp", "date"):
        if tcol in cols:
            dt = scada.schema[tcol].dataType
            print(f"  {tcol} range:")
            timestamp_profile(scada, tcol, dt, label_prefix="    ")
            print()

    # --- sampling interval, measured per series -----------------------------------
    if {"facility_id", "equipment_tag", "timestamp"} <= cols:
        print("  sampling interval, measured within each series:")
        part = ["facility_id", "equipment_tag"]
        if "measurement_type" in cols:
            part.append("measurement_type")
        w = Window.partitionBy(*[F.col(f"`{c}`") for c in part]).orderBy("timestamp")
        deltas = safe(
            lambda: (
                scada.select(*part, F.col("timestamp").cast("timestamp").alias("timestamp"))
                     .where(F.col("timestamp").isNotNull())
                     .withColumn("prev", F.lag("timestamp").over(w))
                     .where(F.col("prev").isNotNull())
                     .withColumn("d", F.col("timestamp").cast("long") - F.col("prev").cast("long"))
                     .groupBy("d").count()
                     .orderBy(F.desc("count"))
                     .limit(5)
                     .collect()
            ),
            default=[],
            label="per-series interval",
        )
        if not deltas:
            print("    no consecutive pairs — every series has a single reading")
        else:
            total = sum(r["count"] for r in deltas)
            for i, r in enumerate(deltas):
                marker = "  <-- modal" if i == 0 else ""
                print(f"    {human_seconds(r['d']):>12}  x {r['count']:>12,}"
                      f"  ({r['count'] / total:5.1%} of top 5){marker}")

    # --- readings per series, and rows per day ------------------------------------
    print()
    if n_series and n_series > 0:
        print(f"  mean readings per series: {n_rows / n_series:,.1f}")
    if "timestamp" in cols:
        per_day = safe(
            lambda: (
                scada.groupBy(F.to_date(F.col("timestamp")).alias("d")).count()
                     .agg(
                         F.count("d").alias("days"),
                         F.min("count").alias("lo"),
                         F.percentile_approx("count", 0.5).alias("med"),
                         F.max("count").alias("hi"),
                     ).collect()[0]
            )
        )
        if per_day and per_day["days"]:
            print(f"  days covered: {per_day['days']:,}"
                  f"    rows/day  min {fmt(per_day['lo'])}"
                  f"  median {fmt(per_day['med'])}  max {fmt(per_day['hi'])}")

    # --- verdict -------------------------------------------------------------------
    print()
    expected_15min = None
    if n_series and scada_info["ts_ranges"].get("timestamp"):
        lo, hi = scada_info["ts_ranges"]["timestamp"]
        span_s = (hi - lo).total_seconds()
        if span_s > 0:
            expected_15min = int(n_series * (span_s / 900.0))
            print(f"  a genuine 15-minute series over this span x {n_series:,} series"
                  f" would hold ~{expected_15min:,} rows")
            print(f"  actual: {n_rows:,}  ({n_rows / expected_15min:.2%} of that)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 7 — `bronze.facility_master` deep dive
#
# Row count, coordinate ranges, and whether the facility names look real or generated from a
# template. The synthetic-name heuristic strips trailing digits and separators from each name
# and counts how many distinct stems remain: a handful of stems covering every row means a
# template such as `"Well Pad A-01"`, `"Well Pad A-02"`, ….

# CELL ********************

# CELL 7 — facility_master

FM_FQN = f"{LAKEHOUSE}.bronze.facility_master"
fm_info = PROFILE.get(FM_FQN)

print("=" * 78)
print("bronze.facility_master — real or synthetic?")
print("=" * 78)

fm = safe(lambda: spark.table(FM_FQN), label="read facility_master")

if fm is None:
    print("  table not present or unreadable — nothing to report")
elif not (fm_info and fm_info.get("row_count")):
    print("  table exists but holds 0 rows — this is an empty shell")
else:
    cols = set(fm.columns)
    print(f"  rows: {fm_info['row_count']:,}")

    # --- coordinate ranges ---------------------------------------------------------
    if {"latitude", "longitude"} <= cols:
        box = safe(
            lambda: fm.agg(
                F.min("latitude").alias("lat_lo"), F.max("latitude").alias("lat_hi"),
                F.min("longitude").alias("lon_lo"), F.max("longitude").alias("lon_hi"),
                F.count(F.when(F.col("latitude").isNull() | F.col("longitude").isNull(), 1))
                 .alias("no_coords"),
            ).collect()[0]
        )
        if box:
            print()
            print("  coordinate range:")
            print(f"    latitude   {fmt(box['lat_lo'])} .. {fmt(box['lat_hi'])}")
            print(f"    longitude  {fmt(box['lon_lo'])} .. {fmt(box['lon_hi'])}")
            print(f"    rows missing coordinates: {box['no_coords']:,}")
            # The Data Agent documents the Permian bbox as 31.8..32.5 N, -102.0..-101.5 W
            try:
                inside = (31.8 <= float(box["lat_lo"]) and float(box["lat_hi"]) <= 32.5
                          and -102.0 <= float(box["lon_lo"]) and float(box["lon_hi"]) <= -101.5)
                print(f"    within the documented Permian bbox"
                      f" (31.8..32.5 N, -102.0..-101.5 W): {inside}")
            except (TypeError, ValueError):
                pass

    # --- names ---------------------------------------------------------------------
    if "facility_name" in cols:
        print()
        n_rows = fm_info["row_count"]
        n_names = safe(lambda: fm.select("facility_name").distinct().count())
        print(f"  distinct facility_name: {fmt(n_names)} of {n_rows:,} rows")

        stems = safe(
            lambda: (
                fm.select(
                    F.trim(
                        F.regexp_replace(F.col("facility_name"), r"[\s\-_#]*[0-9]+\s*$", "")
                    ).alias("stem")
                )
                .groupBy("stem").count()
                .orderBy(F.desc("count"))
                .limit(15)
                .collect()
            ),
            default=[],
        )
        n_stems = safe(
            lambda: (
                fm.select(
                    F.trim(
                        F.regexp_replace(F.col("facility_name"), r"[\s\-_#]*[0-9]+\s*$", "")
                    ).alias("stem")
                ).distinct().count()
            )
        )
        if stems:
            print(f"  distinct name stems (trailing digits stripped): {fmt(n_stems)}")
            for r in stems:
                print(f"    {str(r['stem']):<40} x {r['count']:,}")
            if n_stems and n_rows:
                ratio = n_stems / n_rows
                verdict = ("SYNTHETIC (template-generated)" if ratio < 0.25
                           else "PLAUSIBLY REAL (names do not collapse to a few stems)")
                print(f"  stem-to-row ratio {ratio:.2f} -> {verdict}")

        print()
        print("  sample of facility_name values:")
        for r in safe(lambda: fm.select("facility_name").limit(20).collect(), default=[]):
            print(f"    {r['facility_name']}")

    # --- ids and operators ----------------------------------------------------------
    for c in ("facility_id", "operator", "facility_type", "basin", "active_status"):
        if c in cols:
            rows = safe(
                lambda cc=c: (fm.groupBy(cc).count().orderBy(F.desc("count")).limit(10).collect()),
                default=[],
            )
            if rows:
                print()
                print(f"  {c} — top {len(rows)}:")
                for r in rows:
                    print(f"    {str(r[c]):<40} x {r['count']:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 8 — Summary
#
# One line per table: name, row count, date range where one exists, and a one-word verdict.
#
# | Verdict | Meaning |
# |---|---|
# | `MISSING` | table could not be read |
# | `EMPTY` | 0 rows |
# | `STUB` | at most `STUB_MAX_ROWS` rows — a placeholder, not a dataset |
# | `SPARSE` | mean null rate across columns above `SPARSE_NULL_RATE` |
# | `REAL` | holds substantive, populated data |

# CELL ********************

# CELL 8 — Summary

def verdict_for(info):
    if info["status"] != "OK":
        return "MISSING"
    n = info.get("row_count")
    if n is None:
        return "MISSING"
    if n == 0:
        return "EMPTY"
    if n <= STUB_MAX_ROWS:
        return "STUB"
    rates = [c["null_rate"] for c in info.get("columns", {}).values()]
    if rates and (sum(rates) / len(rates)) > SPARSE_NULL_RATE:
        return "SPARSE"
    return "REAL"


def date_range_for(info):
    ranges = info.get("ts_ranges") or {}
    if not ranges:
        return ""
    # Prefer the widest span, so the column that actually carries the history wins.
    best_name, best_span = None, -1.0
    for name, (lo, hi) in ranges.items():
        if lo is None or hi is None:
            continue
        span = (hi - lo).total_seconds()
        if span > best_span:
            best_name, best_span = name, span
    if best_name is None:
        return ""
    lo, hi = ranges[best_name]
    return f"{lo.date()} .. {hi.date()} [{best_name}]"


print("=" * 110)
print(f"SURVEY SUMMARY — {LAKEHOUSE}")
print("=" * 110)
print(f"{'table':<44}{'rows':>14}  {'date range':<34}verdict")
print("-" * 110)

counts = {}
for fqn in sorted(PROFILE):
    info = PROFILE[fqn]
    name = f"{info['schema']}.{info['table']}"
    v = verdict_for(info)
    counts[v] = counts.get(v, 0) + 1
    rows = "-" if info.get("row_count") is None else f"{info['row_count']:,}"
    print(f"{name:<44}{rows:>14}  {date_range_for(info):<34}{v}")

print("-" * 110)
print("  ".join(f"{k}: {v}" for k, v in sorted(counts.items())))
if MISSING_SCHEMAS:
    print(f"schemas absent or empty: {', '.join(MISSING_SCHEMAS)}")
print()
print("No tables were written. This notebook is read-only.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
