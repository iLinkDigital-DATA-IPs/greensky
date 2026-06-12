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
# Purpose: Define the gold layer views the RTI dashboard consumes directly via
# the queryset (no semantic model). All gold artifacts are SQL views over
# silver Delta tables — fast to refresh, no storage duplication.
#
# Views produced:
#   vw_plume_root_cause          - one row per plume with best-candidate detail
#   vw_operator_emission_rollup  - operator-level emission + attribution metrics
#   vw_facility_health           - latest sensor state + active anomalies
#   vw_daily_summary             - one-row-per-day KPI strip for the dashboard
#
# vw_financial_impact and vw_operator_financial_rollup are published by nb_05.
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

 
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# vw_plume_root_cause
 
 
def publish_plume_root_cause():
    plume_table = CONFIG["paths"]["plume_catalog_table"]
    attr_table  = fqn("silver", "fact_plume_attribution")
    event_table = fqn("silver", "fact_operational_event")
 
    spark.sql(f"""
        CREATE OR REPLACE VIEW {fqn("gold", "vw_plume_root_cause")} AS
        WITH best AS (
            SELECT * FROM {attr_table} WHERE is_best_candidate = true
        )
        SELECT
            p.plume_id,
            p.scene_id,
            p.centroid_lat,
            p.centroid_lon,
            p.source_lat,
            p.source_lon,
            p.ime_kg,
            p.emission_rate_kg_s,
            p.emission_rate_confidence,
            p.wind_speed_ms,
            p.wind_dir_deg,
            p.wind_aligned,
            b.candidate_facility_id    AS root_cause_facility_id,
            b.candidate_facility_type  AS root_cause_facility_type,
            b.candidate_operator_id    AS root_cause_operator_id,
            b.tier                     AS attribution_tier,
            b.distance_km,
            b.confidence_score,
            b.confidence_tier,
            b.coupled_event_id,
            e.event_type               AS root_cause_event_type,
            e.severity                 AS root_cause_event_severity,
            e.start_time               AS root_cause_event_start,
            e.end_time                 AS root_cause_event_end,
            e.expected_ch4_kg          AS root_cause_expected_ch4_kg
        FROM       {plume_table} p
        LEFT JOIN  best              b ON b.plume_id = p.plume_id
        LEFT JOIN  {event_table}     e ON e.event_id = b.coupled_event_id
    """)
    print(f"  view {fqn('gold','vw_plume_root_cause')} refreshed.")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# vw_operator_emission_rollup — emission tonnage and plume counts by operator,
# using confidence weights so attribution uncertainty propagates
 
 
def publish_operator_emission_rollup():
    plume_table = CONFIG["paths"]["plume_catalog_table"]
    attr_table  = fqn("silver", "fact_plume_attribution")
 
    spark.sql(f"""
        CREATE OR REPLACE VIEW {fqn("gold", "vw_operator_emission_rollup")} AS
        WITH conf_sum AS (
            SELECT plume_id, SUM(confidence_score) AS sum_conf
            FROM   {attr_table}
            GROUP BY plume_id
        ),
        weighted AS (
            SELECT
                a.candidate_operator_id AS operator_id,
                a.plume_id,
                CASE WHEN c.sum_conf > 0
                     THEN a.confidence_score / c.sum_conf
                     ELSE 0 END         AS w,
                p.ime_kg,
                p.emission_rate_kg_s
            FROM       {attr_table} a
            JOIN       conf_sum     c USING (plume_id)
            JOIN       {plume_table} p USING (plume_id)
        )
        SELECT
            operator_id,
            COUNT(DISTINCT plume_id)              AS attributed_plume_count,
            SUM(ime_kg * w)                       AS expected_ime_kg,
            SUM(emission_rate_kg_s * w)           AS expected_emission_rate_kg_s,
            AVG(w)                                AS avg_attribution_weight,
            MAX(w)                                AS max_attribution_weight
        FROM weighted
        GROUP BY operator_id
    """)
    print(f"  view {fqn('gold','vw_operator_emission_rollup')} refreshed.")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# vw_facility_health — latest sensor value per (facility, sensor_type), with
# active-anomaly flag
 
 
def publish_facility_health():
    scada_table = fqn("silver", "fact_scada_5min")
    event_table = fqn("silver", "fact_operational_event")
 
    spark.sql(f"""
        CREATE OR REPLACE VIEW {fqn("gold", "vw_facility_health")} AS
        WITH latest_per_sensor AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY sensor_id ORDER BY event_time DESC
                   ) AS rn
            FROM   {scada_table}
        ),
        latest AS (
            SELECT
                parent_facility_id, parent_facility_type, operator_id,
                sensor_id, sensor_type, value, event_time, is_anomalous,
                anomaly_source_event_id, quality_code
            FROM latest_per_sensor
            WHERE rn = 1
        ),
        active_events AS (
            SELECT facility_id,
                   COUNT(*) AS active_event_count,
                   MAX(severity) AS max_active_severity,
                   COLLECT_SET(event_type) AS active_event_types
            FROM {event_table}
            WHERE start_time <= current_timestamp()
              AND (end_time IS NULL OR end_time >= current_timestamp())
            GROUP BY facility_id
        )
        SELECT
            l.parent_facility_id   AS facility_id,
            l.parent_facility_type AS facility_type,
            l.operator_id,
            l.sensor_id,
            l.sensor_type,
            l.value                AS latest_value,
            l.event_time           AS latest_value_at,
            l.is_anomalous,
            l.quality_code,
            l.anomaly_source_event_id,
            COALESCE(ae.active_event_count, 0) AS active_event_count,
            ae.max_active_severity,
            ae.active_event_types
        FROM       latest        l
        LEFT JOIN  active_events ae ON ae.facility_id = l.parent_facility_id
    """)
    print(f"  view {fqn('gold','vw_facility_health')} refreshed.")
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# vw_daily_summary — KPI strip
 
 
def publish_daily_summary():
    plume_table = CONFIG["paths"]["plume_catalog_table"]
    event_table = fqn("silver", "fact_operational_event")
    fin_table   = fqn("silver", "fact_plume_financial_impact")
 
    spark.sql(f"""
        CREATE OR REPLACE VIEW {fqn("gold", "vw_daily_summary")} AS
        WITH plume_daily AS (
            SELECT
                TO_DATE(SUBSTRING(scene_id, 1, 10)) AS day,   -- best-effort
                COUNT(*)         AS plumes_detected,
                SUM(ime_kg)      AS total_ime_kg,
                AVG(emission_rate_kg_s) AS avg_emission_rate_kg_s
            FROM {plume_table}
            GROUP BY TO_DATE(SUBSTRING(scene_id, 1, 10))
        ),
        events_daily AS (
            SELECT
                TO_DATE(start_time) AS day,
                COUNT(*) AS events_started,
                SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_events
            FROM {event_table}
            GROUP BY TO_DATE(start_time)
        ),
        fin_daily AS (
            SELECT
                TO_DATE(SUBSTRING(scene_id, 1, 10)) AS day,
                SUM(total_impact_usd) AS total_financial_impact_usd
            FROM {fin_table}
            GROUP BY TO_DATE(SUBSTRING(scene_id, 1, 10))
        )
        SELECT
            COALESCE(p.day, e.day, f.day) AS day,
            COALESCE(p.plumes_detected, 0)      AS plumes_detected,
            COALESCE(p.total_ime_kg, 0)         AS total_ime_kg,
            COALESCE(p.avg_emission_rate_kg_s, 0) AS avg_emission_rate_kg_s,
            COALESCE(e.events_started, 0)       AS events_started,
            COALESCE(e.critical_events, 0)      AS critical_events,
            COALESCE(f.total_financial_impact_usd, 0) AS total_financial_impact_usd
        FROM      plume_daily p
        FULL JOIN events_daily e ON e.day = p.day
        FULL JOIN fin_daily    f ON f.day = COALESCE(p.day, e.day)
        ORDER BY day
    """)
    print(f"  view {fqn('gold','vw_daily_summary')} refreshed.")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Main
 
publish_plume_root_cause()
publish_operator_emission_rollup()
publish_facility_health()
publish_daily_summary()
 
print("Gold views published.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
