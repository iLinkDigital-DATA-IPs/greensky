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
# Purpose: Build dim_operator, dim_well_pad, dim_well, dim_compressor_station,
# dim_processing_plant, dim_pipeline_segment, dim_sensor.
# Idempotent: if tables already exist with expected row counts, this notebook
# is a no-op. Force regen with the parameter `force_rebuild = True`.
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

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DoubleType,
    BooleanType, DateType,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

force_rebuild = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Operators
 
 
def generate_operators() -> pd.DataFrame:
    rng = make_rng("operators_offset")
    n = CONFIG["scale"]["n_operators"]
 
    # Pareto-distributed market share, normalized to 100%.
    raw_share = rng.pareto(a=1.5, size=n) + 1.0
    share = 100.0 * raw_share / raw_share.sum()
    share = np.sort(share)[::-1]   # descending
 
    tiers = []
    for s in share:
        if s >= 12.0:
            tiers.append("major")
        elif s >= 5.0:
            tiers.append("large_independent")
        elif s >= 2.0:
            tiers.append("mid")
        else:
            tiers.append("small")
 
    founded_years = rng.integers(1950, 2021, size=n)
 
    rows = []
    for i in range(n):
        op_id = f"OP_{i+1:03d}"
        rows.append((
            op_id,
            f"Operator {i+1:03d}",
            tiers[i],
            float(round(share[i], 4)),
            "TX",
            int(founded_years[i]),
            True,
        ))
 
    cols = [f.name for f in SCHEMA_OPERATOR.fields]
    return pd.DataFrame(rows, columns=cols)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Allocate facility counts to operators by market share
 
 
def allocate_by_share(total: int, shares: np.ndarray, rng) -> np.ndarray:
    """
    Multinomial allocation of `total` units across operators, with a minimum
    of 1 per operator so small operators are not empty.
    """
    p = shares / shares.sum()
    counts = rng.multinomial(total - len(shares), p) + 1
    return counts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Well pads
 
 
def generate_well_pads(operators_pdf: pd.DataFrame) -> pd.DataFrame:
    rng = make_rng("facilities_offset")
    scale = CONFIG["scale"]
    spatial = CONFIG["spatial"]
    n_pads = scale["n_well_pads"]
 
    # operators_pdf is already a pandas DataFrame — no .toPandas() needed
    ops = operators_pdf.sort_values("operator_id").reset_index(drop=True)
    counts = allocate_by_share(n_pads, ops["market_share_pct"].to_numpy(), rng)
 
    centers = [spatial["delaware_center"], spatial["midland_center"]]
    weights = [0.55, 0.45]
    lats, lons = sample_clustered_points(
        n_pads, centers, spatial["cluster_std_deg"], weights, rng
    )
    lats, lons = clip_to_envelope(lats, lons)
 
    sub_basin = np.where(
        haversine_km(lats, lons, *spatial["delaware_center"])
        < haversine_km(lats, lons, *spatial["midland_center"]),
        "delaware",
        "midland",
    )
 
    n_wells = rng.integers(scale["wells_per_pad_min"], scale["wells_per_pad_max"] + 1, size=n_pads)
    install_years = rng.integers(2005, 2025, size=n_pads)
    pad_type_choices = rng.choice(
        ["oil_dominant", "gas_dominant", "mixed"], size=n_pads, p=[0.45, 0.30, 0.25]
    )
    base_prop = rng.beta(2.0, 5.0, size=n_pads)
    type_bonus = np.where(pad_type_choices == "gas_dominant", 0.10, 0.0)
    emission_propensity = np.clip(base_prop + type_bonus, 0.0, 1.0)
 
    op_ids = ops["operator_id"].to_numpy()
    assigned = np.repeat(op_ids, counts)
    rng.shuffle(assigned)
 
    rows = []
    for i in range(n_pads):
        pad_id = f"PAD_{i+1:05d}"
        rows.append((
            pad_id,
            str(assigned[i]),
            f"{assigned[i]}-{sub_basin[i][:3].upper()}-{i+1:04d}",
            str(sub_basin[i]),
            float(lats[i]),
            float(lons[i]),
            int(n_wells[i]),
            int(install_years[i]),
            str(pad_type_choices[i]),
            float(round(emission_propensity[i], 4)),
            True,
        ))
 
    cols = [f.name for f in SCHEMA_PAD.fields]
    return pd.DataFrame(rows, columns=cols)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Wells
 
 
def generate_wells(pads_pdf: pd.DataFrame) -> pd.DataFrame:
    # pads_pdf is already a pandas DataFrame — no .toPandas() needed
    rng = np.random.default_rng(int(CONFIG["seeds"]["master"]) + 22)
 
    rows = []
    well_idx = 0
    for _, pad in pads_pdf.iterrows():
        n = int(pad["n_wells"])
        for w in range(n):
            well_idx += 1
            well_id = f"WELL_{well_idx:07d}"
            api = f"42-{rng.integers(1, 500):03d}-{rng.integers(10000, 99999):05d}"
            spud_year = int(rng.integers(int(pad["install_year"]), 2025))
            spud_doy  = int(rng.integers(1, 366))
            try:
                spud = datetime(spud_year, 1, 1) + timedelta(days=spud_doy - 1)
            except Exception:
                spud = datetime(spud_year, 1, 1)
            status = str(rng.choice(["producing", "shut_in", "plugged"], p=[0.82, 0.13, 0.05]))
            product = pad["pad_type"].replace("_dominant", "")
            if product == "mixed":
                product = str(rng.choice(["oil", "gas", "mixed"], p=[0.4, 0.3, 0.3]))
            rows.append((well_id, pad["pad_id"], api, spud.date(), status, product))
 
    cols = [f.name for f in SCHEMA_WELL.fields]
    return pd.DataFrame(rows, columns=cols)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Compressor stations
 
 
def generate_compressor_stations(operators_pdf: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(int(CONFIG["seeds"]["master"]) + 33)
    n = CONFIG["scale"]["n_compressor_stations"]
    spatial = CONFIG["spatial"]
 
    ops = operators_pdf.sort_values("operator_id").reset_index(drop=True)
    w = np.sqrt(ops["market_share_pct"].to_numpy())
    counts = allocate_by_share(n, w, rng)
    assigned = np.repeat(ops["operator_id"].to_numpy(), counts)
    rng.shuffle(assigned)
 
    centers = [spatial["delaware_center"], spatial["midland_center"]]
    lats, lons = sample_clustered_points(n, centers, spatial["cluster_std_deg"] * 1.2,
                                         [0.5, 0.5], rng)
    lats, lons = clip_to_envelope(lats, lons)
 
    n_compressors = rng.integers(2, 7, size=n)
    hp_per = rng.integers(1500, 5500, size=n)
    total_hp = n_compressors * hp_per
    commissioned = rng.integers(1995, 2024, size=n)
    propensity = rng.beta(2.5, 4.0, size=n)
 
    rows = []
    for i in range(n):
        sid = f"CS_{i+1:03d}"
        rows.append((
            sid, str(assigned[i]), f"Station {i+1:03d}",
            float(lats[i]), float(lons[i]),
            int(n_compressors[i]), int(total_hp[i]),
            int(commissioned[i]),
            float(round(propensity[i], 4)),
        ))
 
    cols = [f.name for f in SCHEMA_COMPRESSOR_STATION.fields]
    return pd.DataFrame(rows, columns=cols)
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Processing plants
 
 
def generate_processing_plants(operators_pdf: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(int(CONFIG["seeds"]["master"]) + 44)
    n = CONFIG["scale"]["n_processing_plants"]
    spatial = CONFIG["spatial"]
 
    ops = operators_pdf.sort_values("operator_id").reset_index(drop=True)
    top = ops.nlargest(8, "market_share_pct")["operator_id"].to_numpy()
    assigned = rng.choice(top, size=n)
 
    centers = [spatial["delaware_center"], spatial["midland_center"]]
    lats, lons = sample_clustered_points(n, centers, spatial["cluster_std_deg"] * 1.5,
                                         [0.5, 0.5], rng)
    lats, lons = clip_to_envelope(lats, lons)
 
    capacity = rng.uniform(50.0, 800.0, size=n)
    commissioned = rng.integers(1990, 2023, size=n)
 
    rows = []
    for i in range(n):
        pid = f"PP_{i+1:02d}"
        rows.append((
            pid, str(assigned[i]), f"Plant {i+1:02d}",
            float(lats[i]), float(lons[i]),
            float(round(capacity[i], 1)), int(commissioned[i]),
        ))
 
    cols = [f.name for f in SCHEMA_PROCESSING_PLANT.fields]
    return pd.DataFrame(rows, columns=cols)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pipeline segments — connect pads to nearest compressor station, and compressor
# stations to nearest processing plant. Straight-line.
 
 
def generate_pipeline_segments(pads_pdf, stations_pdf, plants_pdf) -> pd.DataFrame:
    rng = np.random.default_rng(int(CONFIG["seeds"]["master"]) + 55)
    target_n = CONFIG["scale"]["n_pipeline_segments"]
 
    # All inputs are already pandas DataFrames — no .toPandas() needed
    pads     = pads_pdf
    stations = stations_pdf
    plants   = plants_pdf
 
    rows = []
 
    # 1) Pad → nearest compressor station.
    if len(stations) > 0 and len(pads) > 0:
        s_lat = stations["latitude"].to_numpy()
        s_lon = stations["longitude"].to_numpy()
        p_lat = pads["latitude"].to_numpy()
        p_lon = pads["longitude"].to_numpy()
 
        d = haversine_km(p_lat[:, None], p_lon[:, None], s_lat[None, :], s_lon[None, :])
        nearest = np.argmin(d, axis=1)
 
        cap = min(len(pads), target_n // 2)
        chosen_pads = rng.choice(len(pads), size=cap, replace=False)
        for pi in chosen_pads:
            si = int(nearest[pi])
            length = float(d[pi, si])
            seg_id = f"SEG_{len(rows)+1:04d}"
            mlat = float((p_lat[pi] + s_lat[si]) / 2.0)
            mlon = float((p_lon[pi] + s_lon[si]) / 2.0)
            rows.append((
                seg_id,
                str(pads.iloc[pi]["pad_id"]), "pad",
                str(stations.iloc[si]["station_id"]), "compressor_station",
                round(length, 3),
                int(rng.choice([4, 6, 8, 10, 12], p=[0.2, 0.3, 0.25, 0.15, 0.1])),
                int(rng.integers(2005, 2024)),
                mlat, mlon,
            ))
 
    # 2) Compressor station → nearest processing plant.
    if len(stations) > 0 and len(plants) > 0:
        s_lat = stations["latitude"].to_numpy()
        s_lon = stations["longitude"].to_numpy()
        pp_lat = plants["latitude"].to_numpy()
        pp_lon = plants["longitude"].to_numpy()
 
        d = haversine_km(s_lat[:, None], s_lon[:, None], pp_lat[None, :], pp_lon[None, :])
        nearest = np.argmin(d, axis=1)
 
        remaining = target_n - len(rows)
        cap = min(len(stations), remaining)
        chosen = np.arange(len(stations))[:cap]
        for si in chosen:
            pi = int(nearest[si])
            length = float(d[si, pi])
            seg_id = f"SEG_{len(rows)+1:04d}"
            mlat = float((s_lat[si] + pp_lat[pi]) / 2.0)
            mlon = float((s_lon[si] + pp_lon[pi]) / 2.0)
            rows.append((
                seg_id,
                str(stations.iloc[si]["station_id"]), "compressor_station",
                str(plants.iloc[pi]["plant_id"]), "processing_plant",
                round(length, 3),
                int(rng.choice([12, 16, 20, 24], p=[0.25, 0.35, 0.25, 0.15])),
                int(rng.integers(2000, 2023)),
                mlat, mlon,
            ))
 
    cols = [f.name for f in SCHEMA_PIPELINE_SEGMENT.fields]
    return pd.DataFrame(rows, columns=cols)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sensors
 
 
SENSOR_PROFILES = {
    # type            unit       nominal    std
    "pressure":      ("psi",     650.0,     12.0),
    "flow":          ("mcfd",   1800.0,     60.0),
    "valve_state":   ("state",     1.0,      0.0),
    "tank_level":    ("pct",      55.0,      8.0),
    "temperature":   ("degF",     95.0,      4.0),
    "compressor_util":("pct",     72.0,      6.0),
    "leak_alarm":    ("bool",      0.0,      0.0),
}
 
PAD_SENSOR_TYPES = [
    "pressure", "flow", "valve_state", "tank_level", "tank_level", "temperature",
]
COMPRESSOR_SENSOR_TYPES = [
    "pressure", "pressure", "flow", "valve_state", "compressor_util",
    "compressor_util", "temperature", "temperature", "leak_alarm", "tank_level",
]
PROCESSING_SENSOR_TYPES = (
    ["pressure"] * 5 + ["flow"] * 5 + ["valve_state"] * 4
    + ["temperature"] * 5 + ["compressor_util"] * 3 + ["leak_alarm"] * 2
    + ["tank_level"] * 1
)
 
 
def generate_sensors(pads_pdf, stations_pdf, plants_pdf) -> pd.DataFrame:
    rng = make_rng("sensors_offset")
 
    rows = []
 
    def make_sensors_for(parent_id: str, parent_type: str, install_year: int, types):
        for j, st in enumerate(types):
            unit, nominal, std = SENSOR_PROFILES[st]
            sid = f"SEN_{parent_type[:2].upper()}_{parent_id}_{j+1:02d}"
            install = datetime(install_year, int(rng.integers(1, 13)), 1).date()
            n_val = float(nominal * (1.0 + rng.normal(0.0, 0.05)))
            n_std = float(max(std * (1.0 + rng.normal(0.0, 0.10)), 0.1))
            rows.append((sid, parent_id, parent_type, st, unit, n_val, n_std, install))
 
    # All inputs are already pandas DataFrames — no .toPandas() needed
    for p in pads_pdf.itertuples(index=False):
        make_sensors_for(p.pad_id, "pad", p.install_year, PAD_SENSOR_TYPES)
 
    for s in stations_pdf.itertuples(index=False):
        make_sensors_for(s.station_id, "compressor_station",
                         s.commissioned_year, COMPRESSOR_SENSOR_TYPES)
 
    for pp in plants_pdf.itertuples(index=False):
        make_sensors_for(pp.plant_id, "processing_plant",
                         pp.commissioned_year, PROCESSING_SENSOR_TYPES)
 
    cols = [f.name for f in SCHEMA_SENSOR.fields]
    return pd.DataFrame(rows, columns=cols)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write all dimensions
# Conversion from pandas → Spark happens here and only here, so numpy dtypes
# never travel back through Arrow/pickle from a Spark DataFrame.
 
 
def write_dim(pdf: pd.DataFrame, schema, name: str) -> None:
    table = fqn("silver", name)
    df = spark.createDataFrame(pdf, schema=schema)
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table))
    print(f"  wrote {table}: {len(pdf)} rows")
 
 
def main_dimensions():
    print("Generating dimensions...")
 
    operators = generate_operators()
    pads      = generate_well_pads(operators)
    wells     = generate_wells(pads)
    stations  = generate_compressor_stations(operators)
    plants    = generate_processing_plants(operators)
    segments  = generate_pipeline_segments(pads, stations, plants)
    sensors   = generate_sensors(pads, stations, plants)
 
    write_dim(operators, SCHEMA_OPERATOR,            "dim_operator")
    write_dim(pads,      SCHEMA_PAD,                 "dim_well_pad")
    write_dim(wells,     SCHEMA_WELL,                "dim_well")
    write_dim(stations,  SCHEMA_COMPRESSOR_STATION,  "dim_compressor_station")
    write_dim(plants,    SCHEMA_PROCESSING_PLANT,    "dim_processing_plant")
    write_dim(segments,  SCHEMA_PIPELINE_SEGMENT,    "dim_pipeline_segment")
    write_dim(sensors,   SCHEMA_SENSOR,              "dim_sensor")
    print("Dimensions generated.")
 
 
def dimensions_exist() -> bool:
    required = [
        "dim_operator", "dim_well_pad", "dim_well", "dim_compressor_station",
        "dim_processing_plant", "dim_pipeline_segment", "dim_sensor",
    ]
    for name in required:
        if not spark.catalog.tableExists(fqn("silver", name)):
            return False
    return True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run
 
if force_rebuild or not dimensions_exist():
    main_dimensions()
else:
    print("Dimensions already exist; skipping. Set force_rebuild=True to regenerate.")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
