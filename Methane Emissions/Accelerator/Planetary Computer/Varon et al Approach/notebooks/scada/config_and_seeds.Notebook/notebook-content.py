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
# META     },
# META     "environment": {
# META       "environmentId": "cf70e84c-e5f3-9589-4218-88cc1ae7b47d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Fabric notebook: 00_config_and_seeds
# =====================================================================
# Purpose: central configuration, deterministic RNG, and calibration of
# synthetic distributions against the REAL plume month (2 May–2 Jun).
# All other notebooks `%run 00_config_and_seeds` to inherit CONFIG + helpers.
#
# Engine: PySpark on Microsoft Fabric, schema-enabled Lakehouse Operations_LH.
# Generation strategy: entity-scale data (facilities ~350, equipment ~10k,
# episodes/plumes ~10^4-10^5) is generated on the driver with seeded numpy
# for determinism, then parallelized to Spark and written to Delta. The only
# Spark-native (distributed) generator is sensor telemetry (~15M rows, notebook 60).
# =====================================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, hashlib
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd
from pyspark.sql import functions as F, types as T

MASTER_SEED = 20260602  # change to regenerate the entire synthetic universe

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Temporal window --------------------------------------------------
REAL_PLUME_START = date(2025, 5, 10)     # real feed begins here (is_synthetic=False)
REAL_PLUME_END   = date(2026, 6, 2)
HISTORY_YEARS    = 5
SYNTH_PLUME_END  = REAL_PLUME_START - timedelta(days=1)          # 2025-05-09
HISTORY_START    = SYNTH_PLUME_END  - timedelta(days=365 * HISTORY_YEARS)  # 2020-05-09

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Geography: two anchors + 80/15/5 clustering ----------------------
ANCHORS = {
    "Permian Basin": {"lat": 31.5, "lon": -102.0, "weight": 0.6},
    "Texas Site A":  {"lat": 30.2, "lon": -101.5, "weight": 0.4},
}
# radii (deg ~ 0.01 deg ≈ 1.1 km). in/perimeter/out bands → attribution confidence tiers
REGION_RADIUS_DEG   = 0.30   # ~33 km core
PERIMETER_RADIUS_DEG = 0.55  # outer band
FACILITY_SPLIT = {"inside": 0.80, "perimeter": 0.15, "outside": 0.05}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Scale targets ----------------------------------------------------
N_FACILITIES        = 350
EQUIP_PER_FACILITY  = (10, 50)
N_OPERATORS         = 12
SENSORS_PER_FACILITY = 4
SENSOR_INTERVAL_HOURS = 4

# ---- Financial / regulatory scenarios ---------------------------------
# Defaults reflect current law: the EPA Waste Emissions Charge implementing
# rule (40 CFR Part 99) was disapproved under the Congressional Review Act
# (Mar 2025) and revoked by EPA (May 2025); the statutory charge is not
# scheduled to apply until 2034. So reporting_only carries NO federal per-ton
# fee. Sources cited inline.
GAS_PRICE_USD_PER_MCF = 2.75          # Henry Hub-ish; methane ~19.2 kg per mcf
KG_CH4_PER_MCF        = 19.26
GWP_OPTIONS  = {"AR5_100": 28, "AR4_100": 25, "AR6_fossil_100": 29.8, "GWP20": 84}
GWP_DEFAULT  = "AR5_100"              # carbon_equivalent_tonnes = ch4_t * GWP

SOCIAL_COST_CH4_USD_T = 1600          # EPA 2023 final, 2.0% discount (range ~1300-2300)
SOCIAL_COST_CO2_USD_T = 190

FINE_SCENARIOS = {
    "reporting_only": {                # DEFAULT — current law, no active WEC
        "wec_rate_usd_t": 0.0,
        "wec_threshold_tco2e": None,
        "use_social_cost": False,
        "nsps_state_violation_fine_usd": (5_000, 75_000),  # discrete modeled violations
    },
    "wec_hypothetical": {              # IRA escalator, applied above threshold
        "wec_rate_by_year": {2024: 900, 2025: 1200, 2026: 1500},  # USD/tonne CH4
        "wec_threshold_tco2e": 25_000,
        "use_social_cost": False,
        "nsps_state_violation_fine_usd": (5_000, 75_000),
    },
    "social_cost": {                   # ESG shadow price
        "wec_rate_usd_t": 0.0,
        "use_social_cost": True,
        "nsps_state_violation_fine_usd": (5_000, 75_000),
    },
}
ACTIVE_FINE_SCENARIO = "reporting_only"

# ---- Equipment taxonomy (leak propensity drives episode hazard) -------
EQUIPMENT_TYPES = {
    "Compressor":        {"life": 20, "insp_days": 90,  "leak_propensity": 1.00, "crit_bias": 0.8},
    "Valve":             {"life": 25, "insp_days": 180, "leak_propensity": 0.70, "crit_bias": 0.4},
    "Separator":         {"life": 25, "insp_days": 180, "leak_propensity": 0.55, "crit_bias": 0.5},
    "Storage Tank":      {"life": 30, "insp_days": 365, "leak_propensity": 0.85, "crit_bias": 0.6},
    "Flare":             {"life": 20, "insp_days": 180, "leak_propensity": 0.60, "crit_bias": 0.7},
    "Pipeline Segment":  {"life": 40, "insp_days": 365, "leak_propensity": 0.65, "crit_bias": 0.6},
    "Pump":              {"life": 15, "insp_days": 120, "leak_propensity": 0.75, "crit_bias": 0.5},
    "Metering Station":  {"life": 20, "insp_days": 180, "leak_propensity": 0.45, "crit_bias": 0.4},
}
MANUFACTURERS = {  # reliability_index < 1 = more reliable (lowers hazard)
    "Ariel": 0.85, "Caterpillar": 0.90, "Waukesha": 1.00, "Cameron": 0.95,
    "Baker Hughes": 0.92, "Emerson": 0.88, "Honeywell": 0.90, "Flowserve": 1.05,
}
# Permian-region place names → realistic facility names
PLACES = ["Midland","Odessa","Pecos","Monahans","Kermit","Wink","Crane","Andrews",
          "Stanton","Big Spring","Fort Stockton","Reeves County","Loving","Mentone",
          "Orla","Sand Hills","Goldsmith","Notrees","Coyanosa","Imperial"]
FAC_DESCRIPTORS = {
    "Compression Station","Gathering Hub","Processing Plant","Tank Battery",
    "Collection Facility","Central Delivery Point","Booster Station","Treating Facility",
}
FACILITY_TYPES = ["Compression Station","Gathering System","Gas Processing Plant",
                  "Tank Battery","Central Delivery Point"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Deterministic RNG: get_rng("episodes", facility_id) is reproducible across reruns.
def _seed(*parts) -> int:
    h = hashlib.sha256(("|".join(map(str, (MASTER_SEED, *parts)))).encode()).hexdigest()
    return int(h[:16], 16) % (2**32)

def get_rng(*parts) -> np.random.Generator:
    return np.random.default_rng(_seed(*parts))

# Shared equipment-condition model — used by BOTH episode hazard and the
# downstream maintenance generator so the two stay consistent.
def equipment_condition_index(age_years, life_years, days_since_service,
                              insp_days, reliability_index, leak_propensity):
    """0 (pristine) → 1 (degraded). Drives Weibull hazard scaling. No labels used."""
    wear = np.clip(age_years / max(life_years, 1), 0, 1.4)
    service_gap = np.clip(days_since_service / (insp_days * 2.0), 0, 1.5)
    cond = (0.55 * wear + 0.30 * service_gap) * reliability_index * (0.6 + 0.4 * leak_propensity)
    return float(np.clip(cond, 0, 1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def sample_facility_location(rng):
    band = rng.choice(["inside","perimeter","outside"],
                      p=[FACILITY_SPLIT["inside"], FACILITY_SPLIT["perimeter"], FACILITY_SPLIT["outside"]])
    anchor_name = rng.choice(list(ANCHORS), p=[a["weight"] for a in ANCHORS.values()])
    a = ANCHORS[anchor_name]
    if band == "inside":
        r = rng.uniform(0, REGION_RADIUS_DEG)
    elif band == "perimeter":
        r = rng.uniform(REGION_RADIUS_DEG, PERIMETER_RADIUS_DEG)
    else:
        r = rng.uniform(PERIMETER_RADIUS_DEG, PERIMETER_RADIUS_DEG + 0.4)
    theta = rng.uniform(0, 2*np.pi)
    lat = a["lat"] + r * np.cos(theta)
    lon = a["lon"] + r * np.sin(theta) / np.cos(np.radians(a["lat"]))
    return float(lat), float(lon), anchor_name, band

def haversine_km(lat1, lon1, lat2, lon2):
    """Vectorised — works for both scalar and array inputs."""
    R    = 6371.0
    p1   = np.radians(lat1); p2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1); dl = np.radians(lon2 - lon1)
    h    = np.sin(dphi/2)**2 + np.cos(p1)*np.cos(p2)*np.sin(dl/2)**2
    return 2 * R * np.arcsin(np.sqrt(np.clip(h, 0, 1)))  # clip guards fp rounding noise

def bearing_deg(lat1, lon1, lat2, lon2):
    """Vectorised bearing in degrees [0, 360)."""
    y = np.sin(np.radians(lon2-lon1)) * np.cos(np.radians(lat2))
    x = (np.cos(np.radians(lat1)) * np.sin(np.radians(lat2)) -
         np.sin(np.radians(lat1)) * np.cos(np.radians(lat2)) *
         np.cos(np.radians(lon2-lon1)))
    return (np.degrees(np.arctan2(y, x)) + 360) % 360

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calibrate_from_real_plumes():
    cal = {"lognorm_mu": np.log(0.05), "lognorm_sigma": 1.1,
           "daily_detections": 35, "wind_speed_mean": 4.5, "wind_speed_sd": 2.0}
    try:
        df = spark.table("bronze.plume_raw").select("emission_rate_kg_s", "wind_speed_ms")
        if df.count() > 50:
            pdf = df.toPandas().dropna()
            rates = pdf["emission_rate_kg_s"].clip(lower=1e-4)
            cal["lognorm_mu"]        = float(np.log(rates).mean())
            cal["lognorm_sigma"]     = float(np.log(rates).std())
            days = max(1, (REAL_PLUME_END - REAL_PLUME_START).days)
            cal["daily_detections"]  = float(len(pdf) / days)
            cal["wind_speed_mean"]   = float(pdf["wind_speed_ms"].mean())
            cal["wind_speed_sd"]     = float(pdf["wind_speed_ms"].std())
            print(f"calibration from real plumes: {len(pdf)} rows, "
                  f"lognorm_mu={cal['lognorm_mu']:.3f}, "
                  f"daily_detections={cal['daily_detections']:.1f}")
    except Exception as e:
        print("calibration fallback to priors:", e)
    return cal

CALIBRATION = calibrate_from_real_plumes()

_cfg_blob = {
    "master_seed": MASTER_SEED, "history_start": HISTORY_START.isoformat(),
    "real_plume_start": REAL_PLUME_START.isoformat(),
    "active_fine_scenario": ACTIVE_FINE_SCENARIO, "gwp_default": GWP_DEFAULT,
    "calibration": CALIBRATION,
}
try:
    mssparkutils.fs.put("Files/config/run_config.json", json.dumps(_cfg_blob, indent=2), True)
except Exception:
    pass
print("CONFIG ready:", _cfg_blob)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
