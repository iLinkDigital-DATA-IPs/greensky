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
# Purpose: For each attributed plume, compute lost gas value, WEC regulatory
# exposure, deferred production loss, and maintenance cost. Roll up to the
# operator level using confidence-weighted attribution.
#
# Writes:
#   silver/fact_plume_financial_impact   (per plume, total)
#   gold/vw_financial_impact             (denormalized view for dashboard)
#   gold/vw_operator_financial_rollup    (operator-level totals)
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
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType,
)
 
force_full_recompute = False
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Synthetic price curves
 
 
def synthetic_prices_for_window(plume_obs_times):
    """
    Generate synthetic Henry Hub gas and oil prices for the dates of interest.
    Flat-with-noise so the data is obviously synthetic.
    """
    rng = make_rng("financial_offset")
    econ = CONFIG["economics"]
 
    unique_dates = pd.to_datetime(pd.Series(plume_obs_times)).dt.date.unique()
    gas_map, oil_map = {}, {}
    for d in unique_dates:
        gas_map[d] = float(rng.normal(econ["henry_hub_usd_per_mcf_mean"],
                                       econ["henry_hub_usd_per_mcf_std"]))
        oil_map[d] = float(rng.normal(econ["oil_price_usd_per_bbl_mean"],
                                       econ["oil_price_usd_per_bbl_std"]))
    return gas_map, oil_map
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Per-plume financial computation
 
SCHEMA_FIN = T.StructType([
    T.StructField("plume_id",             T.LongType(),   False),
    T.StructField("scene_id",             T.StringType(), False),
    T.StructField("lost_gas_kg",          T.DoubleType(), False),
    T.StructField("lost_gas_mcf",         T.DoubleType(), False),
    T.StructField("gas_price_usd_per_mcf",T.DoubleType(), False),
    T.StructField("gas_value_usd",        T.DoubleType(), False),
    T.StructField("co2e_tons",            T.DoubleType(), False),
    T.StructField("wec_exposure_usd",     T.DoubleType(), False),
    T.StructField("deferred_prod_bbl",    T.DoubleType(), False),
    T.StructField("oil_price_usd_per_bbl",T.DoubleType(), False),
    T.StructField("deferred_value_usd",   T.DoubleType(), False),
    T.StructField("maintenance_usd",      T.DoubleType(), False),
    T.StructField("total_impact_usd",     T.DoubleType(), False),
    T.StructField("computed_at",          T.TimestampType(), False),
])
 
 
def plume_observation_time(scene_id: str):
    if scene_id is None:
        return None
    m = re.search(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})", str(scene_id))
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
        except ValueError:
            return None
    return None
 
 
def compute_financial_impact():
    econ = CONFIG["economics"]
    plumes = spark.table(CONFIG["paths"]["plume_catalog_table"]).toPandas()
    attr_table = fqn("silver", "fact_plume_attribution")
    event_table = fqn("silver", "fact_operational_event")
 
    attr_df = (spark.table(attr_table)
                 .filter(F.col("is_best_candidate"))
                 .toPandas()) if spark.catalog.tableExists(attr_table) else pd.DataFrame()
    events_df = (spark.table(event_table).toPandas()
                  if spark.catalog.tableExists(event_table) else pd.DataFrame())
 
    # Map coupled events by plume_id
    coupled_by_plume = {}
    if not events_df.empty:
        for _, ev in events_df.iterrows():
            pid = ev.get("triggered_by_plume_id")
            if pid is not None and not pd.isna(pid):
                coupled_by_plume[int(pid)] = ev
 
    plumes["obs_time"] = plumes["scene_id"].apply(plume_observation_time)
    gas_map, oil_map = synthetic_prices_for_window(plumes["obs_time"].dropna())
 
    rng = make_rng("financial_offset")
 
    rows = []
    now = datetime.utcnow()
    for _, p in plumes.iterrows():
        pid = int(p["plume_id"])
        ime_kg = float(p.get("ime_kg") or 0.0)
        lost_mcf = ime_kg / econ["ch4_kg_per_mcf"]
        obs_date = p["obs_time"].date() if p["obs_time"] is not None else None
        gas_price = gas_map.get(obs_date, econ["henry_hub_usd_per_mcf_mean"])
        gas_val   = lost_mcf * gas_price
 
        co2e_tons = (ime_kg / 1000.0) * econ["ch4_to_co2e_gwp100"]
        wec_usd   = co2e_tons * econ["waste_emissions_charge_usd_per_ton_co2e"]
 
        # Deferred production and maintenance only if coupled to an event
        if pid in coupled_by_plume:
            deferred_bbl = float(max(rng.normal(econ["deferred_production_bbl_per_event_mean"],
                                                econ["deferred_production_bbl_per_event_std"]), 0.0))
            oil_price = oil_map.get(obs_date, econ["oil_price_usd_per_bbl_mean"])
            deferred_val = deferred_bbl * oil_price
            maint = float(max(rng.normal(econ["maintenance_cost_usd_event_mean"],
                                          econ["maintenance_cost_usd_event_std"]), 0.0))
        else:
            deferred_bbl = 0.0
            oil_price = oil_map.get(obs_date, econ["oil_price_usd_per_bbl_mean"])
            deferred_val = 0.0
            maint = 0.0
 
        total = gas_val + wec_usd + deferred_val + maint
 
        rows.append({
            "plume_id":              pid,
            "scene_id":              str(p["scene_id"]),
            "lost_gas_kg":           round(ime_kg, 3),
            "lost_gas_mcf":          round(lost_mcf, 3),
            "gas_price_usd_per_mcf": round(gas_price, 3),
            "gas_value_usd":         round(gas_val, 2),
            "co2e_tons":             round(co2e_tons, 3),
            "wec_exposure_usd":      round(wec_usd, 2),
            "deferred_prod_bbl":     round(deferred_bbl, 2),
            "oil_price_usd_per_bbl": round(oil_price, 2),
            "deferred_value_usd":    round(deferred_val, 2),
            "maintenance_usd":       round(maint, 2),
            "total_impact_usd":      round(total, 2),
            "computed_at":           now,
        })
 
    pdf = pd.DataFrame(rows)
    sdf = spark.createDataFrame(pdf, schema=SCHEMA_FIN)
 
    fin_table = fqn("silver", "fact_plume_financial_impact")
    (sdf.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(fin_table))
    print(f"Wrote {len(pdf)} rows to {fin_table}.")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold views — confidence-weighted operator rollup
 
 
def publish_financial_gold():
    fin_table = fqn("silver", "fact_plume_financial_impact")
    attr_table = fqn("silver", "fact_plume_attribution")
 
    # Per-plume × per-candidate-operator with confidence weighting.
    # Weight = confidence_score / sum_confidence_per_plume.
    spark.sql(f"""
        CREATE OR REPLACE VIEW {fqn("gold", "vw_financial_impact")} AS
        WITH conf_sum AS (
            SELECT plume_id, SUM(confidence_score) AS sum_conf
            FROM   {attr_table}
            GROUP BY plume_id
        ),
        weighted AS (
            SELECT
                a.plume_id,
                a.candidate_operator_id     AS operator_id,
                a.candidate_facility_id     AS facility_id,
                a.candidate_facility_type   AS facility_type,
                a.confidence_score,
                a.confidence_tier,
                a.is_best_candidate,
                a.tier,
                a.distance_km,
                CASE WHEN c.sum_conf > 0
                     THEN a.confidence_score / c.sum_conf
                     ELSE 0 END             AS attribution_weight,
                f.lost_gas_kg, f.lost_gas_mcf, f.gas_value_usd,
                f.co2e_tons, f.wec_exposure_usd,
                f.deferred_prod_bbl, f.deferred_value_usd,
                f.maintenance_usd, f.total_impact_usd
            FROM       {attr_table} a
            JOIN       {fin_table}  f USING (plume_id)
            LEFT JOIN  conf_sum     c USING (plume_id)
        )
        SELECT
            plume_id, operator_id, facility_id, facility_type,
            confidence_score, confidence_tier, is_best_candidate, tier, distance_km,
            attribution_weight,
            lost_gas_kg,
            lost_gas_mcf,
            gas_value_usd,
            co2e_tons,
            wec_exposure_usd,
            deferred_prod_bbl,
            deferred_value_usd,
            maintenance_usd,
            total_impact_usd,
            total_impact_usd * attribution_weight AS expected_impact_usd
        FROM weighted
    """)
    print(f"  view {fqn('gold','vw_financial_impact')} refreshed.")
 
    spark.sql(f"""
        CREATE OR REPLACE VIEW {fqn("gold", "vw_operator_financial_rollup")} AS
        SELECT
            operator_id,
            COUNT(DISTINCT plume_id)                     AS plumes_attributed,
            SUM(lost_gas_kg * attribution_weight)        AS expected_lost_gas_kg,
            SUM(co2e_tons   * attribution_weight)        AS expected_co2e_tons,
            SUM(gas_value_usd      * attribution_weight) AS expected_gas_value_usd,
            SUM(wec_exposure_usd   * attribution_weight) AS expected_wec_usd,
            SUM(deferred_value_usd * attribution_weight) AS expected_deferred_usd,
            SUM(maintenance_usd    * attribution_weight) AS expected_maintenance_usd,
            SUM(expected_impact_usd)                     AS expected_total_impact_usd
        FROM {fqn("gold", "vw_financial_impact")}
        GROUP BY operator_id
    """)
    print(f"  view {fqn('gold','vw_operator_financial_rollup')} refreshed.")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Main
 
compute_financial_impact()
publish_financial_gold()
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
