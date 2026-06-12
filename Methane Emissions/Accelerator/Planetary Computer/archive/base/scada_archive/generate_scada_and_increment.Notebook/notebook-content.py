# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "11ca0f84-52dc-44fc-9270-538fcda8f1ad",
# META       "default_lakehouse_name": "Operations_LH",
# META       "default_lakehouse_workspace_id": "060ba34b-f1a3-4509-a6e2-36d1e736a8eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "11ca0f84-52dc-44fc-9270-538fcda8f1ad"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# =============================================================================
# Purpose: Generate (or append) 5-min SCADA for the rolling window. On the
# first run produces the full 30-day window; on subsequent daily runs produces
# only the increment since the last watermark.
#
# Generation model per sensor type:
#   - Ornstein-Uhlenbeck process for continuous signals (auto-correlated noise)
#   - Sinusoidal diurnal modulation for flow
#   - Sawtooth for tank levels
#   - Discrete state machine for valve_state and leak_alarm
#
# Anomaly overlay is applied AFTER the base signal, indexed by operational
# events from fact_operational_event. Anomaly injection for new events
# triggered by plumes happens in notebook 04, then notebook 02 is re-run for
# the affected window — or, simpler, notebook 04 patches the SCADA in-place.
# We choose the latter.
# =============================================================================


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ./config_and_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import hashlib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pipeline parameters
force_full_regen = False    # rewrite the entire 30-day window
window_override_start = None  # ISO string to override start, e.g. "2026-04-22T00:00:00Z"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Watermark management
 
 
CONTROL_TABLE = CONFIG["paths"]["control_table"]
 
 
def ensure_control_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
            run_id           STRING,
            run_kind         STRING,
            start_time_utc   TIMESTAMP,
            end_time_utc     TIMESTAMP,
            window_start     TIMESTAMP,
            window_end       TIMESTAMP,
            rows_written     BIGINT,
            status           STRING,
            details          STRING
        ) USING DELTA
    """)
 
 
def get_scada_watermark():
    """
    Last successful SCADA window_end, or None.
    """
    ensure_control_table()
    df = spark.sql(f"""
        SELECT MAX(window_end) AS wm
        FROM {CONTROL_TABLE}
        WHERE run_kind = 'scada_generate' AND status = 'success'
    """)
    row = df.collect()[0]
    return row.wm
 
 
def log_run(run_kind, window_start, window_end, rows, status, details=""):
    run_id = deterministic_id("RUN", run_kind, datetime.utcnow().isoformat())
    spark.createDataFrame(
        [(
            run_id, run_kind,
            datetime.utcnow(), datetime.utcnow(),
            window_start, window_end,
            int(rows), status, details,
        )],
        schema="run_id STRING, run_kind STRING, start_time_utc TIMESTAMP, "
               "end_time_utc TIMESTAMP, window_start TIMESTAMP, window_end TIMESTAMP, "
               "rows_written BIGINT, status STRING, details STRING",
    ).write.mode("append").saveAsTable(CONTROL_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Determine the window to generate
 
 
def determine_scada_window():
    start_target, end_target = scada_window_bounds()
 
    if force_full_regen:
        return start_target, end_target, "full"
 
    if window_override_start is not None:
        s = datetime.fromisoformat(window_override_start.replace("Z", "+00:00"))
        return s, end_target, "override"
 
    wm = get_scada_watermark()
    if wm is None:
        return start_target, end_target, "first_run"
 
    # Append from the watermark forward, but no earlier than start_target
    # (rolling window).
    s = max(wm.replace(tzinfo=timezone.utc), start_target)
    if s >= end_target:
        return None, None, "up_to_date"
    return s, end_target, "increment"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Per-sensor-type generators
 
 
def ou_step_series(n, theta, mu, sigma, x0, rng):
    """
    Discrete Ornstein-Uhlenbeck process, dt = 1 step.
    theta : mean-reversion strength
    mu    : long-run mean
    sigma : noise scale per step
    """
    x = np.empty(n, dtype=np.float64)
    x[0] = x0
    eps = rng.standard_normal(n)
    for i in range(1, n):
        x[i] = x[i-1] + theta * (mu - x[i-1]) + sigma * eps[i]
    return x
 
 
def generate_pressure(n, nominal, std, rng):
    return ou_step_series(n, theta=0.08, mu=nominal, sigma=std * 0.4,
                          x0=nominal + rng.normal(0, std), rng=rng)
 
 
def generate_flow(n, nominal, std, rng, freq_minutes):
    base = ou_step_series(n, theta=0.05, mu=nominal, sigma=std * 0.5,
                          x0=nominal, rng=rng)
    # Diurnal sinusoid, 5% amplitude, period = 1 day
    steps_per_day = (24 * 60) // freq_minutes
    phase = rng.uniform(0, 2 * np.pi)
    diurnal = 0.05 * nominal * np.sin(2 * np.pi * np.arange(n) / steps_per_day + phase)
    return base + diurnal
 
 
def generate_valve_state(n, rng):
    # Mostly open (1), occasional scheduled close
    x = np.ones(n, dtype=np.float64)
    n_closures = max(1, n // 2880)   # ~1 closure per 10 days
    for _ in range(n_closures):
        start = int(rng.integers(0, max(n - 50, 1)))
        dur = int(rng.integers(2, 12))
        x[start:start + dur] = 0.0
    return x
 
 
def generate_tank_level(n, nominal, std, rng):
    # Sawtooth between 20% and 90%, period ~ 8h, with noise
    period_steps = int(8 * 60 / CONFIG["temporal"]["scada_freq_minutes"])
    if period_steps < 4:
        period_steps = 4
    phase = int(rng.integers(0, period_steps))
    t = (np.arange(n) + phase) % period_steps
    triangle = 20.0 + 70.0 * (t / period_steps)
    noise = rng.normal(0, std * 0.3, size=n)
    return np.clip(triangle + noise, 0.0, 100.0)
 
 
def generate_temperature(n, nominal, std, rng, freq_minutes):
    base = ou_step_series(n, theta=0.04, mu=nominal, sigma=std * 0.3,
                          x0=nominal, rng=rng)
    steps_per_day = (24 * 60) // freq_minutes
    diurnal = 6.0 * np.sin(2 * np.pi * np.arange(n) / steps_per_day - np.pi / 2)
    return base + diurnal
 
 
def generate_compressor_util(n, nominal, std, rng):
    x = ou_step_series(n, theta=0.10, mu=nominal, sigma=std * 0.4,
                       x0=nominal, rng=rng)
    return np.clip(x, 0.0, 100.0)
 
 
def generate_leak_alarm(n, rng):
    # Default false; flips true very rarely as base rate
    x = np.zeros(n, dtype=np.float64)
    n_alarms = rng.poisson(0.02 * (n / 288))   # very rare random false positives
    for _ in range(int(n_alarms)):
        idx = int(rng.integers(0, n))
        x[idx] = 1.0
    return x
 
 
SENSOR_GENERATORS = {
    "pressure":        lambda n, nv, ns, rng, fm: generate_pressure(n, nv, ns, rng),
    "flow":            lambda n, nv, ns, rng, fm: generate_flow(n, nv, ns, rng, fm),
    "valve_state":     lambda n, nv, ns, rng, fm: generate_valve_state(n, rng),
    "tank_level":      lambda n, nv, ns, rng, fm: generate_tank_level(n, nv, ns, rng),
    "temperature":     lambda n, nv, ns, rng, fm: generate_temperature(n, nv, ns, rng, fm),
    "compressor_util": lambda n, nv, ns, rng, fm: generate_compressor_util(n, nv, ns, rng),
    "leak_alarm":      lambda n, nv, ns, rng, fm: generate_leak_alarm(n, rng),
}
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Batched SCADA generation. Loops over sensors in Python but writes one big
# Spark DataFrame. At demo scale this is fine; for larger runs convert to a
# mapPartitions / pandas_udf pattern.
 
 
def generate_scada_batch(window_start, window_end):
    freq = int(CONFIG["temporal"]["scada_freq_minutes"])
    ticks = scada_tick_grid(window_start, window_end, freq)
    n = len(ticks)
    if n == 0:
        return spark.createDataFrame([], SCHEMA_SCADA)
 
    print(f"  ticks: {n} per sensor")
 
    # Load dimensions
    sensors = spark.table(fqn("silver", "dim_sensor")).toPandas()
    pads = spark.table(fqn("silver", "dim_well_pad")).select(
        "pad_id", "operator_id"
    ).toPandas()
    stations = spark.table(fqn("silver", "dim_compressor_station")).select(
        "station_id", "operator_id"
    ).toPandas()
    plants = spark.table(fqn("silver", "dim_processing_plant")).select(
        "plant_id", "operator_id"
    ).toPandas()
 
    operator_lookup = {}
    for _, r in pads.iterrows():
        operator_lookup[r["pad_id"]] = r["operator_id"]
    for _, r in stations.iterrows():
        operator_lookup[r["station_id"]] = r["operator_id"]
    for _, r in plants.iterrows():
        operator_lookup[r["plant_id"]] = r["operator_id"]
 
    # Each sensor gets its own stream, deterministically seeded from its ID
    # plus the window_start (so increments are reproducible too).
    # plus the window_start (so increments are reproducible too).
    win_key = int(window_start.timestamp())
 
    ingestion_ts = datetime.utcnow()
 
    all_blocks = []
    for sensor_row in sensors.itertuples(index=False):
        # Build a deterministic per-sensor seed
        h = hashlib.sha256(f"{sensor_row.sensor_id}|{win_key}".encode()).hexdigest()
        seed = int(h[:8], 16)
        rng = np.random.default_rng(seed)
 
        gen = SENSOR_GENERATORS.get(sensor_row.sensor_type)
        if gen is None:
            continue
        values = gen(n, sensor_row.nominal_value, sensor_row.nominal_std, rng, freq)
 
        op_id = operator_lookup.get(sensor_row.parent_facility_id, "OP_UNKNOWN")
 
        block = {
            "event_time":             ticks,
            "sensor_id":              np.repeat(sensor_row.sensor_id, n),
            "parent_facility_id":     np.repeat(sensor_row.parent_facility_id, n),
            "parent_facility_type":   np.repeat(sensor_row.parent_facility_type, n),
            "operator_id":            np.repeat(op_id, n),
            "sensor_type":            np.repeat(sensor_row.sensor_type, n),
            "value":                  values.astype(np.float64),
        }
        all_blocks.append(block)
 
    if not all_blocks:
        return spark.createDataFrame([], SCHEMA_SCADA)
 
    # Stitch into a single pandas DataFrame, then to Spark
    pdf = pd.concat([pd.DataFrame(b) for b in all_blocks], ignore_index=True)
    pdf["event_time"] = pd.to_datetime(pdf["event_time"], utc=True)
    pdf["event_date"] = pdf["event_time"].dt.date
    pdf["ingestion_time"] = ingestion_ts
    pdf["quality_code"] = "good"
    pdf["is_anomalous"] = False
    pdf["anomaly_source_event_id"] = None
 
    # Reorder to match schema
    pdf = pdf[[
        "event_time", "event_date", "ingestion_time",
        "sensor_id", "parent_facility_id", "parent_facility_type",
        "operator_id", "sensor_type", "value",
        "quality_code", "is_anomalous", "anomaly_source_event_id",
    ]]
 
    sdf = spark.createDataFrame(pdf, schema=SCHEMA_SCADA)
    sdf._pdf_row_count = len(pdf)   # stash count to avoid post-write scan
    return sdf
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write SCADA. Partition by event_date and parent_facility_type for query
# efficiency, especially for the dashboard which filters on facility type.
 
 
def write_scada(df: "DataFrame", mode: str, row_count: int) -> int:
    """Write SCADA DataFrame. row_count passed in to avoid a costly post-write scan."""
    table = fqn("silver", "fact_scada_5min")
    writer = (df.write.format("delta")
                .partitionBy("event_date", "parent_facility_type")
                .mode(mode))
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(table)
    return row_count
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Main
 
 
def main_scada():
    win_start, win_end, mode_label = determine_scada_window()
    print(f"SCADA window decision: {mode_label}")
    if win_start is None:
        print("  Up to date, nothing to generate.")
        return
 
    print(f"  start = {win_start}, end = {win_end}")
 
    try:
        df = generate_scada_batch(win_start, win_end)
        write_mode = "overwrite" if mode_label in ("full", "first_run") else "append"
        row_count = df._pdf_row_count if hasattr(df, "_pdf_row_count") else df.count()
        rows = write_scada(df, write_mode, row_count)
        log_run("scada_generate", win_start, win_end, rows, "success",
                f"mode={mode_label}")
        print(f"  wrote {rows} rows in mode={write_mode}")
    except Exception as e:
        log_run("scada_generate", win_start, win_end, 0, "failed", str(e))
        raise
 
 
main_scada()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
