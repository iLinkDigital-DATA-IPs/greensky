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
# Purpose:
#   1. For each new plume within the rolling 30-day SCADA window, with
#      probability `plume_scada_coupling_rate`, select the best-candidate
#      facility from fact_plume_attribution.
#   2. Generate an operational event at that facility, backdated by a sampled
#      lead time before the plume's observation timestamp.
#   3. Insert into fact_operational_event.
#   4. Back-patch fact_scada_5min: flip is_anomalous=true and overlay the
#      event's anomaly signature on affected sensors during [start, end].
#   5. Update fact_plume_attribution.coupled_event_id on the chosen row.
#
# Also generates background (non-plume-coupled) events at the configured base
# rates so the SCADA stream contains negative examples too.
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

import re
import math
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType,
)
 
# Parameters
generate_background_events = True
incremental_coupling = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Helper: derive plume observation time from scene_id.
# The user mentioned TROPOMI; scene_ids in their pipeline typically encode a
# date (often YYYYMMDD or YYYY-MM-DD). This helper tries common patterns and
# falls back to the SCADA window midpoint if no date can be extracted.
 
 
def plume_observation_time(scene_id: str) -> datetime:
    if scene_id is None:
        return None
    s = str(scene_id)
    # Try YYYYMMDD or YYYY-MM-DD or YYYYMMDDTHHMMSS embedded anywhere
    patterns = [
        r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})[T_-]?(\d{2})?(\d{2})?(\d{2})?",
    ]
    for pat in patterns:
        m = re.search(pat, s)
        if m:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            hh = int(m.group(4)) if m.group(4) else 13
            mm = int(m.group(5)) if m.group(5) else 30
            ss = int(m.group(6)) if m.group(6) else 0
            try:
                return datetime(y, mo, d, hh, mm, ss, tzinfo=timezone.utc)
            except ValueError:
                pass
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Choose event type appropriate to the facility type
 
 
EVENT_TYPE_BY_FACILITY = {
    "pad":                  ["tank_venting", "intermittent_leak", "valve_failure",
                             "persistent_super_emitter"],
    "compressor_station":   ["compressor_malfunction", "valve_failure",
                             "intermittent_leak"],
    "processing_plant":     ["intermittent_leak", "valve_failure",
                             "compressor_malfunction"],
    "pipeline_segment":     ["pipeline_rupture", "intermittent_leak",
                             "valve_failure"],
}
 
EVENT_TYPE_WEIGHTS = {
    "pad":                  [0.40, 0.35, 0.15, 0.10],
    "compressor_station":   [0.50, 0.30, 0.20],
    "processing_plant":     [0.50, 0.30, 0.20],
    "pipeline_segment":     [0.10, 0.60, 0.30],
}
 
 
SEVERITY_BY_TYPE = {
    "valve_failure":            ("medium",   50,   500),
    "compressor_malfunction":   ("high",     200, 3000),
    "pipeline_rupture":         ("critical", 1000, 50000),
    "tank_venting":             ("low",      20,   200),
    "scheduled_maintenance":    ("low",      0,    0),
    "intermittent_leak":        ("medium",   5,    100),
    "persistent_super_emitter": ("high",     500,  5000),
}
 
 
def sample_event_type(facility_type: str, rng) -> str:
    choices = EVENT_TYPE_BY_FACILITY[facility_type]
    weights = EVENT_TYPE_WEIGHTS[facility_type]
    return str(rng.choice(choices, p=weights))
 
 
def sample_event_duration(event_type: str, rng) -> timedelta:
    table = {
        "valve_failure":            (2, 24),
        "compressor_malfunction":   (4, 48),
        "pipeline_rupture":         (1, 12),
        "tank_venting":             (0.5, 6),
        "scheduled_maintenance":    (4, 24),
        "intermittent_leak":        (1, 6),
        "persistent_super_emitter": (24 * 3, 24 * 14),
    }
    lo, hi = table[event_type]
    h = rng.uniform(lo, hi)
    return timedelta(hours=h)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Plume-coupled event injection
 
 
def inject_plume_coupled_events():
    plume_table = CONFIG["paths"]["plume_catalog_table"]
    attr_table = fqn("silver", "fact_plume_attribution")
    event_table = fqn("silver", "fact_operational_event")
 
    win_start, win_end = scada_window_bounds()
    print(f"SCADA window: {win_start} .. {win_end}")
 
    plumes = spark.table(plume_table).toPandas()
    plumes["obs_time"] = plumes["scene_id"].apply(plume_observation_time)
 
    # Only couple plumes whose observation time falls in the SCADA window
    in_window = plumes["obs_time"].apply(
        lambda t: t is not None and win_start <= t < win_end
    )
    plumes_w = plumes[in_window].copy()
    print(f"{len(plumes_w)} plumes fall in the SCADA window.")
    if plumes_w.empty:
        return pd.DataFrame()
 
    if incremental_coupling:
        already_coupled = spark.sql(
            f"SELECT DISTINCT triggered_by_plume_id AS pid "
            f"FROM {event_table} WHERE triggered_by_plume_id IS NOT NULL"
        ).toPandas()["pid"].tolist() if spark.catalog.tableExists(event_table) else []
        plumes_w = plumes_w[~plumes_w["plume_id"].isin(already_coupled)]
        print(f"{len(plumes_w)} plumes remain after de-duplication.")
 
    if plumes_w.empty:
        return pd.DataFrame()
 
    # Load best candidates only
    attr = spark.table(attr_table).filter(F.col("is_best_candidate")).toPandas()
    attr_by_plume = attr.set_index("plume_id")
 
    rng = make_rng("events_offset")
    coupling_rate = float(CONFIG["coupling"]["plume_scada_coupling_rate"])
    lead_lo = float(CONFIG["coupling"]["scada_lead_time_hours_min"])
    lead_hi = float(CONFIG["coupling"]["scada_lead_time_hours_max"])
 
    rows = []
    attribution_updates = []
 
    for _, p in plumes_w.iterrows():
        if rng.random() > coupling_rate:
            continue
        pid = int(p["plume_id"])
        if pid not in attr_by_plume.index:
            continue
        cand = attr_by_plume.loc[pid]
        if isinstance(cand, pd.DataFrame):
            cand = cand.iloc[0]
 
        facility_type = cand["candidate_facility_type"]
        if facility_type not in EVENT_TYPE_BY_FACILITY:
            continue
 
        event_type = sample_event_type(facility_type, rng)
        severity, ch4_lo, ch4_hi = SEVERITY_BY_TYPE[event_type]
        # Scale ch4 with plume ime_kg when available; cap at ch4_hi
        plume_ime = float(p.get("ime_kg", 0.0) or 0.0)
        expected_ch4 = float(max(min(plume_ime, ch4_hi), ch4_lo))
 
        lead_h = float(rng.uniform(lead_lo, lead_hi))
        plume_time = p["obs_time"]
        start = plume_time - timedelta(hours=lead_h)
        end = start + sample_event_duration(event_type, rng)
        # Clamp end to window end so SCADA patch is bounded
        end = min(end, win_end.replace(tzinfo=timezone.utc))
 
        event_id = deterministic_id("EVT", pid, cand["candidate_facility_id"])
 
        rows.append({
            "event_id":              event_id,
            "event_type":            event_type,
            "facility_id":           cand["candidate_facility_id"],
            "facility_type":         facility_type,
            "operator_id":           cand["candidate_operator_id"],
            "start_time":            start,
            "end_time":              end,
            "severity":              severity,
            "expected_ch4_kg":       expected_ch4,
            "triggered_by_plume_id": pid,
            "is_synthetic":          True,
        })
        attribution_updates.append((cand["attribution_id"], event_id))
 
    if not rows:
        return pd.DataFrame()
 
    events_pdf = pd.DataFrame(rows)
    write_events(events_pdf)
 
    # Update attribution rows with coupled_event_id
    update_attribution_coupling(attribution_updates)
 
    return events_pdf
 
 
def write_events(pdf: pd.DataFrame):
    event_table = fqn("silver", "fact_operational_event")
    sdf = spark.createDataFrame(pdf, schema=SCHEMA_EVENT)
    if not spark.catalog.tableExists(event_table):
        (sdf.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true").saveAsTable(event_table))
    else:
        sdf.createOrReplaceTempView("staging_events")
        spark.sql(f"""
            MERGE INTO {event_table} t
            USING staging_events s
            ON t.event_id = s.event_id
            WHEN NOT MATCHED THEN INSERT *
        """)
    print(f"  wrote {len(pdf)} events.")
 
 
def update_attribution_coupling(updates):
    if not updates:
        return
    attr_table = fqn("silver", "fact_plume_attribution")
    pdf = pd.DataFrame(updates, columns=["attribution_id", "coupled_event_id_new"])
    sdf = spark.createDataFrame(pdf)
    sdf.createOrReplaceTempView("staging_couple")
    spark.sql(f"""
        MERGE INTO {attr_table} t
        USING staging_couple s
        ON t.attribution_id = s.attribution_id
        WHEN MATCHED THEN UPDATE SET t.coupled_event_id = s.coupled_event_id_new
    """)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Background event generation — independent, base-rate-driven
 
 
def generate_background_events_fn():
    """
    Sample background events using the configured per-facility-per-day rates
    over the SCADA window. These are NOT triggered by plumes (triggered_by_plume_id
    stays NULL) and serve as negative examples for downstream ML.
    """
    rng = np.random.default_rng(int(CONFIG["seeds"]["master"]) + int(CONFIG["seeds"]["events_offset"]) * 7 + 11)
    win_start, win_end = scada_window_bounds()
    window_days = (win_end - win_start).total_seconds() / 86400.0
 
    # Load facility universe with operator
    facilities = spark.sql(f"""
        SELECT pad_id AS facility_id, 'pad' AS facility_type, operator_id
        FROM   {fqn("silver","dim_well_pad")}
        UNION ALL
        SELECT station_id, 'compressor_station', operator_id
        FROM   {fqn("silver","dim_compressor_station")}
        UNION ALL
        SELECT plant_id, 'processing_plant', operator_id
        FROM   {fqn("silver","dim_processing_plant")}
        UNION ALL
        SELECT segment_id, 'pipeline_segment', 'OP_UNKNOWN'
        FROM   {fqn("silver","dim_pipeline_segment")}
    """).toPandas()
 
    rates = CONFIG["event_rates"]
    rows = []
    for ev_type, daily_rate in rates.items():
        expected = float(daily_rate) * len(facilities) * window_days
        if expected <= 0:
            continue
        n = rng.poisson(expected)
        if n == 0:
            continue
        # Sample facilities
        idx = rng.integers(0, len(facilities), size=n)
        for k, i in enumerate(idx):
            f = facilities.iloc[int(i)]
            ftype = f["facility_type"]
            if ev_type not in [e for sub in EVENT_TYPE_BY_FACILITY.values() for e in sub]:
                continue
            # If this event type doesn't match this facility, skip
            if ev_type not in EVENT_TYPE_BY_FACILITY.get(ftype, []) and ev_type != "scheduled_maintenance":
                continue
 
            sev, lo, hi = SEVERITY_BY_TYPE[ev_type]
            start_offset = rng.uniform(0, window_days * 86400)
            start = win_start + timedelta(seconds=float(start_offset))
            dur = sample_event_duration(ev_type, rng)
            end = min(start + dur, win_end.replace(tzinfo=timezone.utc))
            ch4 = float(rng.uniform(lo, hi))
 
            ev_id = deterministic_id("EVT", f["facility_id"], ev_type,
                                     int(start.timestamp()))
            rows.append({
                "event_id":              ev_id,
                "event_type":            ev_type,
                "facility_id":           f["facility_id"],
                "facility_type":         ftype,
                "operator_id":           f["operator_id"],
                "start_time":            start,
                "end_time":              end,
                "severity":              sev,
                "expected_ch4_kg":       ch4,
                "triggered_by_plume_id": None,
                "is_synthetic":          True,
            })
 
    if not rows:
        print("  No background events sampled.")
        return
    pdf = pd.DataFrame(rows)
    write_events(pdf)
    print(f"  generated {len(pdf)} background events.")
 
 
# CELL 6 -----------------------------------------------------------------------
# Back-patch SCADA for all events touching the SCADA window.
 
 
# Anomaly signatures per (sensor_type, event_type). Multiplicative on the
# baseline value (1.0 = no effect). For valve_state and leak_alarm we use
# absolute overrides.
 
ANOMALY_PROFILES = {
    # (sensor_type, event_type): callable(value) -> patched_value
    ("pressure", "pipeline_rupture"):       lambda v: v * 0.55,
    ("pressure", "valve_failure"):          lambda v: v * 0.85,
    ("pressure", "compressor_malfunction"): lambda v: v * 0.90,
    ("pressure", "intermittent_leak"):      lambda v: v * 0.96,
    ("flow", "pipeline_rupture"):           lambda v: v * 0.30,
    ("flow", "valve_failure"):              lambda v: v * 0.70,
    ("flow", "compressor_malfunction"):     lambda v: v * 0.60,
    ("flow", "intermittent_leak"):          lambda v: v * 0.95,
    ("tank_level", "tank_venting"):         lambda v: max(v - 25.0, 0.0),
    ("temperature", "compressor_malfunction"): lambda v: v + 12.0,
    ("compressor_util", "compressor_malfunction"): lambda v: 0.0,
    ("compressor_util", "persistent_super_emitter"): lambda v: min(v + 10.0, 100.0),
    ("valve_state", "valve_failure"):       lambda v: 1.0,   # stuck open
    ("leak_alarm", "intermittent_leak"):    lambda v: 1.0,
    ("leak_alarm", "pipeline_rupture"):     lambda v: 1.0,
    ("leak_alarm", "persistent_super_emitter"): lambda v: 1.0,
}
 
 
def back_patch_scada():
    """
    For every event whose window overlaps the SCADA window, identify affected
    sensors and update fact_scada_5min in-place via MERGE.
    """
    scada_table = fqn("silver", "fact_scada_5min")
    event_table = fqn("silver", "fact_operational_event")
    if not spark.catalog.tableExists(scada_table) or not spark.catalog.tableExists(event_table):
        return
 
    win_start, win_end = scada_window_bounds()
    events = spark.sql(f"""
        SELECT event_id, event_type, facility_id, start_time, end_time
        FROM {event_table}
        WHERE end_time > TIMESTAMP '{win_start.strftime('%Y-%m-%d %H:%M:%S')}'
          AND start_time < TIMESTAMP '{win_end.strftime('%Y-%m-%d %H:%M:%S')}'
    """).toPandas()
    if events.empty:
        print("  No events overlap the SCADA window.")
        return
 
    print(f"  back-patching SCADA for {len(events)} events...")
    patches_total = 0
 
    for _, ev in events.iterrows():
        ev_start = pd.Timestamp(ev["start_time"]).to_pydatetime()
        ev_end = pd.Timestamp(ev["end_time"]).to_pydatetime()
        if ev_start.tzinfo is None:
            ev_start = ev_start.replace(tzinfo=timezone.utc)
        if ev_end.tzinfo is None:
            ev_end = ev_end.replace(tzinfo=timezone.utc)
 
        # Pull SCADA rows for this facility in the event window
        slice_df = spark.sql(f"""
            SELECT *
            FROM {scada_table}
            WHERE parent_facility_id = '{ev["facility_id"]}'
              AND event_time >= TIMESTAMP '{ev_start.strftime('%Y-%m-%d %H:%M:%S')}'
              AND event_time <  TIMESTAMP '{ev_end.strftime('%Y-%m-%d %H:%M:%S')}'
        """)
        pdf = slice_df.toPandas()
        if pdf.empty:
            continue
 
        ev_type = ev["event_type"]
        for sensor_type, profile in [(t[0], t[1]) for t in ANOMALY_PROFILES.items()
                                     if t[0][1] == ev_type]:
            mask = pdf["sensor_type"] == sensor_type
            if not mask.any():
                continue
            fn = profile
            pdf.loc[mask, "value"] = pdf.loc[mask, "value"].apply(fn)
            pdf.loc[mask, "is_anomalous"] = True
            pdf.loc[mask, "anomaly_source_event_id"] = ev["event_id"]
            pdf.loc[mask, "quality_code"] = "suspect"
            patches_total += int(mask.sum())
 
        if pdf["is_anomalous"].any():
            sdf = spark.createDataFrame(pdf, schema=SCHEMA_SCADA)
            sdf.createOrReplaceTempView("staging_scada_patch")
            spark.sql(f"""
                MERGE INTO {scada_table} t
                USING staging_scada_patch s
                ON t.sensor_id = s.sensor_id AND t.event_time = s.event_time
                WHEN MATCHED THEN UPDATE SET
                    t.value                   = s.value,
                    t.is_anomalous            = s.is_anomalous,
                    t.anomaly_source_event_id = s.anomaly_source_event_id,
                    t.quality_code            = s.quality_code
            """)
 
    print(f"  patched {patches_total} SCADA rows.")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Main
 
 
def main_coupling():
    print("Step 1: plume-coupled events...")
    inject_plume_coupled_events()
 
    if generate_background_events:
        print("Step 2: background events...")
        generate_background_events_fn()
 
    print("Step 3: back-patching SCADA for all events in window...")
    back_patch_scada()
 
 
main_coupling()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
