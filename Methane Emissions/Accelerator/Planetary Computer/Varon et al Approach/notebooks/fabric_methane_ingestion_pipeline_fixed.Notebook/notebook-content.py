# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "95f7f55e-9d06-4699-9483-754709a1d397",
# META       "default_lakehouse_name": "Planetary_computer_LH",
# META       "default_lakehouse_workspace_id": "060ba34b-f1a3-4509-a6e2-36d1e736a8eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "95f7f55e-9d06-4699-9483-754709a1d397"
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

# # CH₄ Ingestion Pipeline — Microsoft Fabric / Delta Lake
# Loads raw TROPOMI pixels · Joins weather · Writes silver table

# CELL ********************

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    DoubleType, StringType, TimestampType, DateType, BooleanType
)
from delta.tables import DeltaTable
import pandas as pd

print('Imports OK')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CFG = {
    'methane_table':              'Planetary_computer_LH.bronze.planetary_comp_raw_data',
    'weather_table':              'Planetary_computer_LH.bronze.weather',
    'weather_csv_path':           'Files/weather/weather.csv',   # fallback if Delta table absent
    'output_table':               'Planetary_computer_LH.silver.ch4_plume_ready',
    'qa_min':                     0.5,
    'temporal_join_window_hours': 12,
    'spatial_join_radius_deg':    5.0,
    'partition_by':               'acquisition_date',            # date partition — stac_id not guaranteed
}

# Columns required from the raw methane table.
# stac_id is intentionally excluded — it is a STAC catalog artifact,
# not a pixel property, and is not guaranteed to exist after ETL.
# Scene/overpass grouping is derived in the detection notebook via time-gap clustering.
METHANE_REQUIRED_COLS = ['latitude', 'longitude', 'ch4', 'qa_value', 'datetime']

WEATHER_SCHEMA = StructType([
    StructField('dateTime',         TimestampType(), True),
    StructField('date',             DateType(),      True),
    StructField('temperature',      DoubleType(),    True),
    StructField('relativeHumidity', DoubleType(),    True),
    StructField('pressure',         DoubleType(),    True),
    StructField('wind_speed',       DoubleType(),    True),
    StructField('wind_direction',   DoubleType(),    True),
    StructField('latitude',         DoubleType(),    False),
    StructField('longitude',        DoubleType(),    False),
    StructField('locationName',     StringType(),    True),
    StructField('source',           StringType(),    True),
])

print('Configuration OK')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_methane_data(
    table_name: str,
    qa_min: float = CFG['qa_min'],
) -> DataFrame:
    """
    Load raw TROPOMI CH4 pixels from Delta. Cast types, drop nulls, apply QA.
    No grouping, no plume constructs, no stac_id dependency.
    """
    df = spark.read.table(table_name)

    # Validate required columns exist
    missing = [c for c in METHANE_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f'Missing required columns in {table_name}: {missing}\n'
            f'Available: {sorted(df.columns)}'
        )

    # Explicit casts — never rely on inferred Delta types
    df = (
        df
        .withColumn('latitude',  F.col('latitude').cast(DoubleType()))
        .withColumn('longitude', F.col('longitude').cast(DoubleType()))
        .withColumn('ch4',       F.col('ch4').cast(DoubleType()))
        .withColumn('qa_value',  F.col('qa_value').cast(DoubleType()))
        .withColumn('datetime',  F.to_timestamp(F.col('datetime').cast(StringType())))
    )

    # Drop rows missing any essential field
    df = df.dropna(subset=METHANE_REQUIRED_COLS)

    # QA filter
    df = df.filter(F.col('qa_value') >= qa_min)

    # Derive acquisition_date — used for partitioning and temporal join
    df = df.withColumn('acquisition_date', F.to_date(F.col('datetime')))

    # Stats via single Spark job — never call len() / .min() on a Spark DataFrame
    stats = df.agg(
        F.count('*').alias('n_pixels'),
        F.min('ch4').alias('ch4_min'),
        F.max('ch4').alias('ch4_max'),
        F.min('latitude').alias('lat_min'),
        F.max('latitude').alias('lat_max'),
        F.min('datetime').alias('t_min'),
        F.max('datetime').alias('t_max'),
    ).collect()[0]

    print(f'[methane] pixels={stats["n_pixels"]:,}  '
          f'ch4={stats["ch4_min"]:.1f}–{stats["ch4_max"]:.1f} ppb  '
          f'lat={stats["lat_min"]:.2f}–{stats["lat_max"]:.2f}  '
          f't={stats["t_min"]} → {stats["t_max"]}')

    return df

print('load_methane_data() defined')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_weather_data(
    table_name: str = CFG['weather_table'],
    csv_path:   str = CFG['weather_csv_path'],
) -> DataFrame:
    """
    Load weather from bronze Delta table, falling back to CSV.
    Normalises units (km/h → m/s), decomposes wind to u/v components,
    deduplicates live vs historical overlap.
    """
    # Load — Delta first, CSV fallback
    try:
        df = spark.read.table(table_name)
        print(f'[weather] loaded from Delta: {table_name}')
    except Exception as e:
        print(f'[weather] Delta unavailable ({e}) — loading CSV: {csv_path}')
        df = (
            spark.read
            .option('header', 'true')
            .option('inferSchema', 'false')
            .schema(WEATHER_SCHEMA)
            .csv(csv_path)
        )

    # Explicit casts
    df = (
        df
        .withColumn('dateTime',       F.to_timestamp(F.col('dateTime').cast(StringType())))
        .withColumn('wind_speed',     F.col('wind_speed').cast(DoubleType()))
        .withColumn('wind_direction', F.col('wind_direction').cast(DoubleType()))
        .withColumn('latitude',       F.col('latitude').cast(DoubleType()))
        .withColumn('longitude',      F.col('longitude').cast(DoubleType()))
    )

    # Unit normalisation: Open-Meteo returns km/h; IME formula requires m/s
    df = (
        df
        .withColumn('wind_speed_ms',      F.col('wind_speed') / F.lit(3.6))
        .withColumn('wind_direction_deg', F.col('wind_direction'))
    )

    # Decompose to u/v transport vector
    # Meteorological convention: direction = FROM which wind blows
    # u = -speed * sin(dir)  (eastward component)
    # v = -speed * cos(dir)  (northward component)
    wind_valid = (
        F.col('wind_speed_ms').isNotNull() &
        F.col('wind_direction_deg').isNotNull()
    )
    df = (
        df
        .withColumn(
            'wind_u_ms',
            F.when(wind_valid,
                   -F.col('wind_speed_ms') * F.sin(F.radians(F.col('wind_direction_deg')))
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
        .withColumn(
            'wind_v_ms',
            F.when(wind_valid,
                   -F.col('wind_speed_ms') * F.cos(F.radians(F.col('wind_direction_deg')))
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
        .withColumn('is_wind_valid', wind_valid.cast(BooleanType()))
    )

    # Deduplicate: prefer 'live' over 'historical' for same timestamp+location
    w = Window.partitionBy('dateTime', 'latitude', 'longitude').orderBy(
        F.when(F.col('source') == 'live', 0).otherwise(1)
    )
    df = (
        df
        .withColumn('_rn', F.row_number().over(w))
        .filter(F.col('_rn') == 1)
        .drop('_rn', 'wind_speed', 'wind_direction')
    )

    # Final column selection
    df = df.select(
        'dateTime', 'latitude', 'longitude', 'locationName', 'source',
        'temperature', 'relativeHumidity', 'pressure',
        'wind_speed_ms', 'wind_direction_deg',
        'wind_u_ms', 'wind_v_ms', 'is_wind_valid',
    )

    stats = df.agg(
        F.count('*').alias('n'),
        F.sum(F.col('is_wind_valid').cast('int')).alias('n_wind'),
        F.countDistinct('locationName').alias('n_stations'),
    ).collect()[0]

    print(f'[weather] rows={stats["n"]:,}  valid_wind={stats["n_wind"]:,}  stations={stats["n_stations"]}')

    return df

print('load_weather_data() defined')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def join_methane_weather(
    df_methane: DataFrame,
    df_weather: DataFrame,
    temporal_window_hours: float = CFG['temporal_join_window_hours'],
    spatial_radius_deg:    float = CFG['spatial_join_radius_deg'],
) -> DataFrame:
    """
    Left-outer join: match each methane pixel to the nearest weather
    observation within the spatial radius and temporal window.
    Pixels with no match are retained with null wind columns.
    """
    window_seconds = int(temporal_window_hours * 3600)

    # Prefix weather columns to avoid collision
    w_cols = [
        'dateTime', 'latitude', 'longitude', 'locationName',
        'wind_speed_ms', 'wind_direction_deg',
        'wind_u_ms', 'wind_v_ms', 'is_wind_valid',
    ]
    df_w = df_weather.select(*[F.col(c).alias(f'_w_{c}') for c in w_cols])

    # Broadcast weather (small table) — eliminates shuffle entirely
    df_candidates = (
        df_methane.alias('m')
        .join(
            F.broadcast(df_w).alias('w'),
            on=(
                (F.abs(F.col('m.latitude')  - F.col('w._w_latitude'))  <= spatial_radius_deg) &
                (F.abs(F.col('m.longitude') - F.col('w._w_longitude')) <= spatial_radius_deg) &
                (F.abs(
                    F.col('m.datetime').cast('long') - F.col('w._w_dateTime').cast('long')
                ) <= window_seconds)
            ),
            how='left',
        )
    )

    # Exact distance + time delta for ranking
    df_candidates = (
        df_candidates
        .withColumn(
            '_dist_deg',
            F.sqrt(
                F.pow(F.col('m.latitude')  - F.col('w._w_latitude'),  2) +
                F.pow(F.col('m.longitude') - F.col('w._w_longitude'), 2)
            )
        )
        .withColumn(
            '_dt_s',
            F.abs(F.col('m.datetime').cast('long') - F.col('w._w_dateTime').cast('long'))
        )
    )

    # Pick the single closest observation per pixel
    # Partition by pixel identity — no stac_id, use lat+lon+datetime
    w_rank = Window.partitionBy(
        'm.latitude', 'm.longitude', 'm.datetime'
    ).orderBy(
        F.col('_dist_deg').asc_nulls_last(),
        F.col('_dt_s').asc_nulls_last(),
    )

    df_joined = (
        df_candidates
        .withColumn('_rank', F.row_number().over(w_rank))
        .filter(F.col('_rank') == 1)
        .drop('_rank')
    )

    # Rename weather output columns
    df_joined = (
        df_joined
        .withColumnRenamed('_w_wind_speed_ms',      'wind_speed_ms')
        .withColumnRenamed('_w_wind_direction_deg', 'wind_direction_deg')
        .withColumnRenamed('_w_wind_u_ms',          'wind_u_ms')
        .withColumnRenamed('_w_wind_v_ms',          'wind_v_ms')
        .withColumnRenamed('_w_is_wind_valid',      'is_wind_valid')
        .withColumnRenamed('_w_locationName',       'weather_station_name')
        .withColumnRenamed('_dist_deg',             'weather_station_dist_deg')
        .withColumnRenamed('_dt_s',                 'weather_obs_time_delta_s')
        .drop('_w_dateTime', '_w_latitude', '_w_longitude')
    )

    # Null-safe: pixels with no weather match get is_wind_valid = False
    df_joined = df_joined.withColumn(
        'is_wind_valid',
        F.coalesce(F.col('is_wind_valid'), F.lit(False))
    )

    stats = df_joined.agg(
        F.count('*').alias('n'),
        F.sum(F.col('is_wind_valid').cast('int')).alias('n_wind'),
        F.avg('weather_station_dist_deg').alias('avg_dist'),
        F.avg('weather_obs_time_delta_s').alias('avg_dt'),
    ).collect()[0]

    print(f'[join] pixels={stats["n"]:,}  '
          f'with_wind={stats["n_wind"]:,}  '
          f'no_wind={stats["n"]-stats["n_wind"]:,}'
          + (f'  avg_dist={stats["avg_dist"]:.2f}°  avg_dt={stats["avg_dt"]/3600:.1f}h'
             if stats['avg_dist'] else '')
    )

    return df_joined

print('join_methane_weather() defined')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def finalise_plume_ready(df: DataFrame) -> DataFrame:
    """
    Select, cast, and validate the final plume-ready column set.
    This is the contract boundary between ingestion and detection.

    Guaranteed output columns:
        latitude, longitude        DOUBLE
        ch4                        DOUBLE  [ppb]
        qa_value                   DOUBLE  [0-1]
        datetime                   TIMESTAMP UTC
        acquisition_date           DATE
        wind_speed_ms              DOUBLE  [m/s]   nullable
        wind_direction_deg         DOUBLE  [deg]   nullable
        wind_u_ms                  DOUBLE  [m/s]   nullable
        wind_v_ms                  DOUBLE  [m/s]   nullable
        is_wind_valid              BOOLEAN
    Plus pass-through metadata columns if present:
        instrument, platform, processing_level, mission_phase
        weather_station_name, weather_station_dist_deg, weather_obs_time_delta_s
    """
    REQUIRED = ['latitude', 'longitude', 'ch4', 'qa_value', 'datetime',
                'wind_speed_ms', 'wind_direction_deg']

    KEEP = [
        'latitude', 'longitude', 'ch4', 'qa_value', 'datetime', 'acquisition_date',
        'instrument', 'platform', 'processing_level', 'mission_phase',
        'wind_speed_ms', 'wind_direction_deg', 'wind_u_ms', 'wind_v_ms', 'is_wind_valid',
        'weather_station_name', 'weather_station_dist_deg', 'weather_obs_time_delta_s',
    ]

    # Select only columns that exist (graceful degradation for optional fields)
    existing = set(df.columns)
    df = df.select(*[c for c in KEEP if c in existing])

    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f'Plume-ready validation failed — missing: {missing}')

    # Final type enforcement
    df = (
        df
        .withColumn('latitude',           F.col('latitude').cast(DoubleType()))
        .withColumn('longitude',          F.col('longitude').cast(DoubleType()))
        .withColumn('ch4',                F.col('ch4').cast(DoubleType()))
        .withColumn('qa_value',           F.col('qa_value').cast(DoubleType()))
        .withColumn('wind_speed_ms',      F.col('wind_speed_ms').cast(DoubleType()))
        .withColumn('wind_direction_deg', F.col('wind_direction_deg').cast(DoubleType()))
        .withColumn('is_wind_valid',      F.coalesce(
            F.col('is_wind_valid').cast(BooleanType()), F.lit(False)
        ))
    )

    print(f'[finalise] columns: {df.columns}')
    return df

print('finalise_plume_ready() defined')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_plume_ready(
    df: DataFrame,
    output_table: str = CFG['output_table'],
    partition_by: str = CFG['partition_by'],
    mode: str = 'overwrite',
) -> None:
    (
        df.write
        .format('delta')
        .mode(mode)
        .partitionBy(partition_by)
        .option('overwriteSchema', 'true')  # CHANGE: was 'false'
        .saveAsTable(output_table)
    )
    n = spark.read.table(output_table).count()
    print(f'[write] {output_table}: {n:,} rows, partitioned by {partition_by}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('Step 1: Loading methane data...')
df_methane = load_methane_data(
    table_name = CFG['methane_table'],
    qa_min     = CFG['qa_min'],
)

print('\nStep 2: Loading weather data...')
df_weather = load_weather_data(
    table_name = CFG['weather_table'],
    csv_path   = CFG['weather_csv_path'],
)

print('\nStep 3: Joining methane + weather...')
df_joined = join_methane_weather(
    df_methane            = df_methane,
    df_weather            = df_weather,
    temporal_window_hours = CFG['temporal_join_window_hours'],
    spatial_radius_deg    = CFG['spatial_join_radius_deg'],
)

print('\nStep 4: Finalising schema...')
df_plume_ready = finalise_plume_ready(df_joined)

print('\nStep 5: Writing to silver layer...')
write_plume_ready(
    df           = df_plume_ready,
    output_table = CFG['output_table'],
    partition_by = CFG['partition_by'],
)

print('\n✓ Pipeline complete.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_plume_ready.printSchema()

qc = df_plume_ready.agg(
    F.count('*').alias('total_pixels'),
    F.sum(F.col('is_wind_valid').cast('int')).alias('pixels_with_wind'),
    F.min('ch4').alias('ch4_min'),
    F.max('ch4').alias('ch4_max'),
    F.avg('ch4').alias('ch4_mean'),
    F.avg('wind_speed_ms').alias('wind_speed_mean_ms'),
    F.sum(F.when(F.col('ch4').isNull(),         1).otherwise(0)).alias('null_ch4'),
    F.sum(F.when(F.col('wind_speed_ms').isNull(), 1).otherwise(0)).alias('null_wind'),
).collect()[0]

print('=== Quality Report ===')
print(f'  pixels       : {qc["total_pixels"]:,}')
print(f'  with wind    : {qc["pixels_with_wind"]:,}  ({100*qc["pixels_with_wind"]/qc["total_pixels"]:.1f}%)')
print(f'  CH4          : {qc["ch4_min"]:.1f} – {qc["ch4_max"]:.1f} ppb  (mean {qc["ch4_mean"]:.1f})')
wind_str = f'{qc["wind_speed_mean_ms"]:.2f} m/s' if qc['wind_speed_mean_ms'] else 'N/A'
print(f'  wind mean    : {wind_str}')
print(f'  null ch4     : {qc["null_ch4"]}')
print(f'  null wind    : {qc["null_wind"]:,}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Per-date breakdown — shows what the detection notebook will iterate over
df_plume_ready.groupBy('acquisition_date').agg(
    F.count('*').alias('n_pixels'),
    F.avg('ch4').alias('ch4_mean'),
    F.stddev('ch4').alias('ch4_std'),
    F.sum(F.col('is_wind_valid').cast('int')).alias('n_wind_valid'),
).orderBy('acquisition_date').show(truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
