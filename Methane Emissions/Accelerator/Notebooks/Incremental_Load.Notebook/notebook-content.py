# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "427f0431-b084-4858-82dd-1bfa55380658",
# META       "default_lakehouse_name": "GreenSky_Lakehouse",
# META       "default_lakehouse_workspace_id": "060ba34b-f1a3-4509-a6e2-36d1e736a8eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "427f0431-b084-4858-82dd-1bfa55380658"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM GreenSky_Lakehouse.dbo.gold_emission_events_extra LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM GreenSky_Lakehouse.gold.dim_date")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM GreenSky_Lakehouse.gold.dim_equipment")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM GreenSky_Lakehouse.gold.dim_facility")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fix for SparkSQL ParseException caused by incorrect clause order.
# In SparkSQL, ORDER BY must come before LIMIT.
# The corrected query places ORDER BY before LIMIT so the most recent events are returned.

df = spark.sql("SELECT * FROM GreenSky_Lakehouse.gold.gold_emission_events ORDER BY event_date DESC LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM GreenSky_Lakehouse.dbo.gold_emission_events_extra ORDER BY event_date DESC LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
=============================================================================
PySpark Date Shift Transformation — Microsoft Fabric
=============================================================================

PURPOSE
-------
Automatically detect ALL date / timestamp / date-like columns in a DataFrame
and shift the entire timeline forward so that:
    new_earliest_date = original_max_date + 1 day

The relative gap between every date is preserved exactly.

STRATEGY
--------
1. Classify columns into four buckets:
      a) DateType          — native Spark date columns
      b) TimestampType     — native Spark timestamp columns
      c) Integer YYYYMMDD  — integer columns whose values encode dates
      d) String date/ts    — string columns matching ISO date or datetime patterns

2. Find the GLOBAL min and max across ALL detected columns (cast to timestamp
   for a fair comparison) in ONE aggregation pass.

3. Compute the shift offset:
      offset = (max_ts + 1 day) - min_ts          (expressed in seconds)

4. Apply the offset to each detected column while preserving the original
   data type and column format.

5. Handle NULLs safely — any null value stays null after transformation.

COLUMNS DETECTED IN THIS SAMPLE DATASET
-----------------------------------------
  • event_id              — string  "EVENT-YYYYMMDD-CM-YYYYMMDD-NNNN"  ← two embedded dates
  • event_date            — string  "YYYY-MM-DD"
  • detection_timestamp   — string  "YYYY-MM-DD HH:MM:SS"
  • date_key              — integer YYYYMMDD  (e.g. 20260119)

=============================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, TimestampType, IntegerType, LongType, StringType
)
from typing import Dict, Tuple
import re


# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

_DATE_PATTERN     = r"^\d{4}-\d{2}-\d{2}$"                            # 2025-08-17
_TS_PATTERN       = r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?$"      # 2025-08-17 15:00:00
_INT_DATE_PATTERN = r"^\d{8}$"                                         # 20250817

# Matches IDs with two embedded YYYYMMDD dates, e.g. EVENT-20260119-CM-20260119-0009
# Capture groups: (1) prefix  (2) date1  (3) middle-segment  (4) date2  (5) suffix
_EVENT_ID_PATTERN = r"^([A-Z]+-?)(\d{8})(-[A-Z]+-?)(\d{8})(-\d+)$"

_SAMPLE_ROWS      = 50          # rows sampled when probing string/int columns


# ===========================================================================
# SECTION 1 — COLUMN DETECTION
# ===========================================================================

def _is_integer_date_column(df: DataFrame, col_name: str) -> bool:
    """
    Return True when an IntegerType / LongType column holds YYYYMMDD values.

    Heuristic: at least 95 % of sampled non-null values are 8-digit integers
    that fall in a plausible date range (year 1900–2100).
    Only _SAMPLE_ROWS rows are collected — no full scan.
    """
    sample = (
        df.select(col_name)
          .where(F.col(col_name).isNotNull())
          .limit(_SAMPLE_ROWS)
          .collect()
    )
    if not sample:
        return False

    def valid(v):
        s = str(v)
        if not re.fullmatch(_INT_DATE_PATTERN, s):
            return False
        year = int(s[:4])
        return 1900 <= year <= 2100

    hits = sum(1 for row in sample if row[0] is not None and valid(row[0]))
    return (hits / len(sample)) >= 0.95


def _sniff_string_col(df: DataFrame, col_name: str) -> str | None:
    """
    Sample a StringType column and return:
        'str_date'      → values look like "YYYY-MM-DD"
        'str_timestamp' → values look like "YYYY-MM-DD HH:MM:SS"
        None            → not a date column
    Only _SAMPLE_ROWS rows are collected.
    """
    sample = (
        df.select(col_name)
          .where(F.col(col_name).isNotNull())
          .limit(_SAMPLE_ROWS)
          .collect()
    )
    if not sample:
        return None

    n = len(sample)
    ts_hits      = sum(1 for r in sample if r[0] and re.fullmatch(_TS_PATTERN,       str(r[0]).strip()))
    date_hits    = sum(1 for r in sample if r[0] and re.fullmatch(_DATE_PATTERN,     str(r[0]).strip()))
    eventid_hits = sum(1 for r in sample if r[0] and re.fullmatch(_EVENT_ID_PATTERN, str(r[0]).strip()))

    if ts_hits      / n >= 0.90:  return "str_timestamp"
    if date_hits    / n >= 0.90:  return "str_date"
    if eventid_hits / n >= 0.90:  return "str_event_id"
    return None


def detect_date_columns(df: DataFrame) -> Dict[str, str]:
    """
    Walk every column in `df` and classify it.

    Returns
    -------
    dict  { column_name : classification_string }

    Classifications
    ---------------
    'native_date'      → DateType
    'native_timestamp' → TimestampType
    'int_date'         → IntegerType / LongType encoded as YYYYMMDD
    'str_date'         → StringType "YYYY-MM-DD"
    'str_timestamp'    → StringType "YYYY-MM-DD HH:MM:SS"
    'str_event_id'     → StringType with embedded YYYYMMDD dates e.g. "EVENT-20260119-CM-20260119-0009"
    """
    result: Dict[str, str] = {}

    for field in df.schema.fields:
        name  = field.name
        dtype = field.dataType

        if isinstance(dtype, DateType):
            result[name] = "native_date"

        elif isinstance(dtype, TimestampType):
            result[name] = "native_timestamp"

        elif isinstance(dtype, (IntegerType, LongType)):
            if _is_integer_date_column(df, name):
                result[name] = "int_date"

        elif isinstance(dtype, StringType):
            kind = _sniff_string_col(df, name)
            if kind:
                result[name] = kind

    return result


# ===========================================================================
# SECTION 2 — GLOBAL MIN / MAX  (single aggregation pass)
# ===========================================================================

def _to_ts_expr(col_name: str, cls: str):
    """
    Build a Spark Column expression that casts any detected column type to
    TimestampType so that all date columns are on the same scale.
    """
    c = F.col(col_name)
    if cls == "native_date":
        return c.cast(TimestampType())
    if cls == "native_timestamp":
        return c
    if cls == "int_date":
        # 20250817  →  cast to string  →  parse as "yyyyMMdd"  →  timestamp
        return F.to_timestamp(c.cast(StringType()), "yyyyMMdd")
    if cls == "str_date":
        return F.to_timestamp(c, "yyyy-MM-dd")
    if cls == "str_timestamp":
        return F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss")
    if cls == "str_event_id":
        # Extract the first embedded YYYYMMDD (group 2) to represent this column
        # in the global min/max calculation
        return F.to_timestamp(F.regexp_extract(c, _EVENT_ID_PATTERN, 2), "yyyyMMdd")
    raise ValueError(f"Unsupported classification: {cls}")


def find_global_min_max(
    df: DataFrame,
    date_cols: Dict[str, str],
) -> Tuple:
    """
    Compute the single global (min, max) across ALL detected date columns
    in ONE Spark job using F.least() / F.greatest() row-wise, then MIN/MAX
    across rows.

    Returns
    -------
    (global_min, global_max) as Python datetime objects.
    Only ONE row is ever collected back to the driver.
    """
    ts_exprs   = [_to_ts_expr(c, k).alias(f"_ts_{c}") for c, k in date_cols.items()]
    ts_aliases = [f"_ts_{c}" for c in date_cols]

    temp = df.select(*ts_exprs)

    row_min = F.least(   *[F.col(a) for a in ts_aliases]).alias("_row_min")
    row_max = F.greatest(*[F.col(a) for a in ts_aliases]).alias("_row_max")

    agg_row = temp.select(row_min, row_max).agg(
        F.min("_row_min").alias("global_min"),
        F.max("_row_max").alias("global_max"),
    ).collect()[0]                    # ← single scalar pair — safe to collect

    return agg_row["global_min"], agg_row["global_max"]


# ===========================================================================
# SECTION 3 — APPLY THE SHIFT
# ===========================================================================

def shift_date_columns(
    df: DataFrame,
    date_cols: Dict[str, str],
    offset_seconds: float,
) -> DataFrame:
    """
    Add `offset_seconds` to every detected date column.

    Approach
    --------
    1. Cast the column to epoch-seconds (long).
    2. Add the integer offset.
    3. Cast back to TimestampType.
    4. Re-format to the original type / string pattern.
    5. Nulls pass through unchanged via F.when(...).otherwise(...).

    Parameters
    ----------
    df             : Source DataFrame
    date_cols      : Output of detect_date_columns()
    offset_seconds : Seconds to add to every date value

    Returns
    -------
    New DataFrame with all date columns shifted forward.
    """
    result = df
    offset_lit = F.lit(int(offset_seconds))

    for col_name, cls in date_cols.items():
        ts_expr = _to_ts_expr(col_name, cls)

        # Shift in epoch-seconds, then back to timestamp
        shifted_ts = F.when(
            F.col(col_name).isNull(),
            F.lit(None).cast(TimestampType()),
        ).otherwise(
            (ts_expr.cast("long") + offset_lit).cast(TimestampType())
        )

        # ── Restore to original type / format ──────────────────────────
        if cls == "native_date":
            new_col = shifted_ts.cast(DateType())

        elif cls == "native_timestamp":
            new_col = shifted_ts

        elif cls == "int_date":
            # Back to YYYYMMDD integer; preserve original integer sub-type
            new_col = F.date_format(shifted_ts, "yyyyMMdd").cast(
                result.schema[col_name].dataType
            )

        elif cls == "str_date":
            new_col = F.date_format(shifted_ts, "yyyy-MM-dd")

        elif cls == "str_timestamp":
            # Preserve original time component (HH:mm:ss)
            new_col = F.date_format(shifted_ts, "yyyy-MM-dd HH:mm:ss")

        elif cls == "str_event_id":
            # Both embedded YYYYMMDD dates must be shifted independently,
            # then the full ID string is reconstructed from its five parts.
            #
            # Input  →  EVENT-20260119-CM-20260119-0009
            # Groups →  (EVENT-)(20260119)(-CM-)(20260119)(-0009)
            #
            p = _EVENT_ID_PATTERN
            g1 = F.regexp_extract(F.col(col_name), p, 1)   # "EVENT-"
            d1 = F.regexp_extract(F.col(col_name), p, 2)   # "20260119"  ← date 1
            g3 = F.regexp_extract(F.col(col_name), p, 3)   # "-CM-"
            d2 = F.regexp_extract(F.col(col_name), p, 4)   # "20260119"  ← date 2
            g5 = F.regexp_extract(F.col(col_name), p, 5)   # "-0009"

            def _shift_yyyymmdd(date_str_col):
                """Parse a YYYYMMDD string column, add offset, return new YYYYMMDD string."""
                ts = F.to_timestamp(date_str_col, "yyyyMMdd")
                shifted = (ts.cast("long") + offset_lit).cast(TimestampType())
                return F.date_format(shifted, "yyyyMMdd")

            new_col = F.when(
                F.col(col_name).isNull(),
                F.lit(None).cast(StringType()),
            ).otherwise(
                F.concat(g1, _shift_yyyymmdd(d1), g3, _shift_yyyymmdd(d2), g5)
            )

        else:
            raise ValueError(f"Unknown classification: {cls}")

        result = result.withColumn(col_name, new_col)

    return result


# ===========================================================================
# SECTION 4 — DETERMINISTIC ROW ORDERING  (window function, no shuffle)
# ===========================================================================

def add_row_order(df: DataFrame, order_col: str = "_row_order") -> DataFrame:
    """
    Attach a deterministic row number to the DataFrame.

    Uses monotonically_increasing_id() as the ordering key inside a Window
    function so that:
      • Relative row order is preserved (no global sort required).
      • The result is deterministic within a single Spark execution.
      • No shuffle is triggered when used with an unbounded Window.

    The column is named `_row_order` by default (drop it later if not needed).
    """
    from pyspark.sql.window import Window
    w = Window.orderBy(F.monotonically_increasing_id())
    return df.withColumn(order_col, F.row_number().over(w))


# ===========================================================================
# SECTION 5 — MAIN PUBLIC FUNCTION
# ===========================================================================

def apply_date_shift(df: DataFrame, verbose: bool = True) -> DataFrame:
    """
    End-to-end date-shift pipeline.

    Steps
    -----
    1. Detect all date / timestamp / date-like columns automatically.
    2. Find global min and max (single Spark aggregation).
    3. Compute offset: new_min = old_max + 1 day.
    4. Shift all detected columns by that offset.
    5. Return the transformed DataFrame.

    Parameters
    ----------
    df      : Input PySpark DataFrame
    verbose : Print detection / date-range summary to stdout

    Returns
    -------
    Transformed PySpark DataFrame — same schema, all dates shifted forward.
    """
    # ── 1. Detect ──────────────────────────────────────────────────────────
    date_cols = detect_date_columns(df)

    if not date_cols:
        print("⚠  No date / timestamp columns detected. Returning original DataFrame.")
        return df

    if verbose:
        print("=" * 62)
        print("  DETECTED DATE / TIMESTAMP COLUMNS")
        print("=" * 62)
        for col, cls in date_cols.items():
            print(f"  {col:<30}  [{cls}]")
        print()

    # ── 2. Global min / max ─────────────────────────────────────────────────
    global_min, global_max = find_global_min_max(df, date_cols)

    if verbose:
        print(f"  Global MIN  →  {global_min}")
        print(f"  Global MAX  →  {global_max}")

    # ── 3. Offset calculation ───────────────────────────────────────────────
    one_day            = 86_400                                   # seconds
    offset_seconds     = (global_max.timestamp() + one_day) - global_min.timestamp()
    offset_days        = offset_seconds / one_day

    from datetime import timedelta
    new_min = global_max + timedelta(days=1)

    if verbose:
        print(f"  New MIN     →  {new_min}  (old MAX + 1 day)")
        print(f"  Offset      →  {offset_seconds:,.0f} s  ({offset_days:.1f} days)")
        print("=" * 62)
        print()

    # ── 4. Apply shift ──────────────────────────────────────────────────────
    df_shifted = shift_date_columns(df, date_cols, offset_seconds)

    return df_shifted


# ===========================================================================
# SECTION 6 — USAGE  (Microsoft Fabric — GreenSky Lakehouse)
# ===========================================================================

# In Microsoft Fabric the SparkSession is already available as `spark`.
# No SparkSession.builder call is needed inside a Fabric notebook.

# ── Load from Lakehouse ────────────────────────────────────────────────────
df = spark.sql("SELECT * FROM GreenSky_Lakehouse.gold.gold_emission_events")

# ── Preview original dates ─────────────────────────────────────────────────
print("── BEFORE ──────────────────────────────────────────────────────────")
df.select("event_id", "date_key", "event_date", "detection_timestamp").show(5, truncate=False)

# ── Apply date shift transformation ───────────────────────────────────────
df_out = apply_date_shift(df, verbose=True)

# ── Preview shifted dates ──────────────────────────────────────────────────
print("── AFTER ───────────────────────────────────────────────────────────")
df_out.select("event_id", "date_key", "event_date", "detection_timestamp").show(5, truncate=False)

# ── (Optional) Write back to Lakehouse ────────────────────────────────────
# df_out.write.mode("overwrite").saveAsTable("GreenSky_Lakehouse.gold.gold_emission_events_shifted")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_out.write.mode("overwrite").saveAsTable("GreenSky_Lakehouse.dbo.gold_emission_events_shifted")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.sql("SELECT * FROM GreenSky_Lakehouse.dbo.gold_emission_events_shifted")

df_filtered = df.filter(F.col("event_date") <= F.current_date())

df_filtered.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_filtered.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("GreenSky_Lakehouse.gold.gold_emission_events")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
