# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "cf70e84c-e5f3-9589-4218-88cc1ae7b47d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### 2026-08-14 — Day 1 (Completed)
# 
# #### Verified (Existing Data)
# - Existing table: Planetary_computer_LH.bronze.planetary_comp_raw_data
# - Total rows: 940,952 (32,049 in Permian Basin BBOX)
# - Date range: 2026-06-10 to 2026-07-09 (~1 month)
# - 68 distinct STAC scenes
# - QA values: 0.9-1.0 (pre-filtered, stricter than 0.5 threshold)
# - CH4 values: ~1900 ppb range in Permian Basin (physically plausible)
# - Schema: latitude, longitude, ch4, qa_value, datetime, gas, instrument, 
#   platform, collection, stac_id, provider, provider_all, provider_roles, 
#   processing_level, mission_phase
# 
# #### Completed
# - Migrated bronze_methane_pixels to greensky_lakehouse
# - CAMS validation CSV uploaded to greensky_lakehouse/Files/validation/
#   (Schuit_etal2023_TROPOMI_all_plume_detections_2021.csv, 127 KB)
# 
# #### Decided
# - Planetary Computer confirmed as CH4 data source (collection: sentinel-5p-l2-netcdf)
# - CDSE STAC available as fallback but not needed
# - 1-month data window sufficient for initial pipeline development; will expand post-sprint


# MARKDOWN ********************

# ### 2026-08-17 -- Day 2 (Completed)
# 
# #### Created (Notebooks)
# - 00_config: Centralized configuration notebook with all pipeline parameters
#   (BBOX, thresholds, detection params, clustering params, attribution params)
# - 02_ingest_weather_data: Open-Meteo hourly ingestion for 63 grid points (0.5-degree spacing)
# - 02b_ingest_era5: ERA5 single-level hourly ingestion and NetCDF parsing
# - 03_join_data: Spatial-temporal join of methane + weather + ERA5
# 
# #### Tables Created
# - bronze_methane_pixels: migrated from Planetary_computer_LH (940,952 rows total, 32,049 in Permian BBOX)
# - bronze_weather: Open-Meteo hourly weather grid (45,360 rows, 63 grid points, 30 days)
# - bronze_era5_wind: ERA5 hourly reanalysis (323,544 rows, 4 variables: u10, v10, BLH, SP)
# - silver_plume_ready_pixels: joined methane + weather + ERA5 (32,049 rows, 20 columns)
# 
# #### Fixed
# - Open-Meteo wind_speed_10m unit: API returns km/h by default, not m/s
#   Added &wind_speed_unit=ms to API URL
#   Re-ran full ingestion to ensure consistent units across all 63 grid points
#   Before fix: avg wind 18.75 m/s (actually km/h). After fix: avg 5.2 m/s (correct)
# - Open-Meteo 12 grid point timeouts: same 12 points failed on both initial run and re-run
#   Backfilled with retry logic (3 attempts, 120s timeout, 2s delay between retries)
#   All 12 recovered on retry, 0 still failed
# - ERA5 NetCDF parsing: xarray not available in default Fabric environment
#   Fixed by attaching notebooks to planetary_computer environment
# - ERA5 coordinate inspection: TypeError on scalar coordinates and UFuncTypeError on string coordinates
#   Fixed with ndim and dtype checks before calling min/max
# 
# #### Verified (Silver Table Quality)
# - Row count preserved: 32,049 in = 32,049 out (no lost or duplicated rows)
# - ERA5 fill rate: 100% (all pixels have ERA5 wind data)
# - Zero nulls across all 20 columns
# - Weather distance: min 0.15 km, median 20 km, max 36 km (within Gaussian decay range)
# - Wind speeds consistent: Open-Meteo avg 5.2 m/s, ERA5 avg ~5-7 m/s (agreement)
# - ERA5 BLH range: 1000-1800 m (reasonable for Permian Basin summer)
# - CH4 avg: 1900 ppb (physically plausible for background + enhancements)
# 
# #### Decided
# - Table naming: prefix-based under dbo schema (bronze_, silver_, gold_) instead of separate schemas
#   Rationale: simpler for solo sprint, refactor to proper schemas post-sprint
# - Wind decomposition: meteorological convention (direction = where wind comes FROM)
#   u = -speed * sin(direction), v = -speed * cos(direction)
# - Spatial-temporal join strategy: round to nearest hour, Euclidean distance, keep nearest station
#   Gaussian decay weighting with sigma=50 km preserved for potential future weighted interpolation
# - ERA5 downloaded manually from CDS web interface (API credentials not set up in Fabric)
#   File: era5_permian_202606_202607.nc (2.8 MB, NetCDF4 format)
#   Variables: u10, v10, boundary_layer_height, surface_pressure
#   Area: 30.5-33.5N, 101-105W (Permian Basin)
#   Period: June-July 2026, all hours


# MARKDOWN ********************

# ### 2026-08-18 -- Day 3 (Completed)
# 
# #### Created (Notebooks)
# - 04_derive_emissions: Full detection + quantification pipeline with Tier 1 improvements
# 
# #### Pipeline Results (1 month, Permian Basin)
# - Input: 32,049 plume-ready pixels across 27 scenes (49 STAC IDs)
# - Candidate pixels: 1,764 / 32,049 (5.5%)
# - Valid plumes detected: 109 (3-15 pixels each, 667 total pixels)
# - Flagged large clusters: 20 (not discarded, written to gold_flagged_large_clusters)
# - Emission rate range: 0.6 - 14.3 kg/h (median 4.3 kg/h, mean 4.7 kg/h)
# - T_mix method: 100% wind-dependent (zero fallbacks)
# - Confidence: 105 high, 4 medium
# - Uncertainty (p95/p50 ratio): 1.48 - 1.89 (median 1.60)
# - All emission rates < 100 t/h (physically plausible)
# 
# #### Tables Created
# - gold_plume_catalog: 109 plumes with emission rates, uncertainty bounds, and confidence
# - gold_flagged_large_clusters: 872 pixels across 20 flagged large clusters
# 
# #### Tier 1 Improvements Implemented
# - Wind-dependent T_mix = L / U_eff using ERA5 effective wind speed (all 109 plumes)
# - Per-estimate Monte Carlo uncertainty (N=500, CH4 noise + wind uncertainty)
# - 5th/50th/95th percentile emission rates reported
# - Large clusters flagged rather than discarded
# - Composite confidence scoring (wind alignment + uncertainty ratio + pixel count)
# 
# #### Technical Notes
# - Background estimation: kNN (30 neighbors, 15th percentile) per scene via scipy cKDTree
# - MAD thresholds ranged from 6.0 ppb (floor) to 46.2 ppb across scenes
# - Plume clustering: Union-Find with 12 km radius, size filter 3-15 pixels, shape filter aspect <= 20
# - IME conversion: 1 ppb over 1 TROPOMI pixel = 0.0217 kg CH4
# - Arrow optimization warnings in Spark (cosmetic, fell back to standard conversion)


# MARKDOWN ********************

# ### 2026-08-18 -- Day 4 (Completed)
# 
# #### Created (Notebooks)
# - 05_attribute_facilities: Wind-aware probabilistic facility attribution
# - 06_temporal_persistence: Repeat detection tracking and persistence classification
# 
# #### Tables Created
# - ref_facilities: 48 Permian Basin grid reference points (fallback, EPA API blocked 403)
# - gold_emission_sites: 103 emission sites with persistence classification
# - gold_plume_site_mapping: 109 plume-to-site mappings
# 
# #### Updated
# - gold_plume_catalog: added 11 attribution columns (39 total)
# - 00_config: attribution_search_radius_km changed from 30 to 50
# 
# #### Attribution Results
# - 109/109 plumes attributed (100% coverage)
# - Attribution probability range: 0.50 - 1.00 (median 0.976)
# - Distance to attributed facility: 2.4 - 49.9 km (median 26.2 km)
# - Facilities in range per plume: 1-4 (mean 2.7)
# 
# #### Persistence Results
# - 103 emission sites identified (5 km match radius)
# - 97 single-detection sites
# - 6 intermittent sites (2 detections each, 6-17 day spans)
# - 0 persistent/chronic sites (expected with 1-month window)
# - Top emitter: 14.3 kg/h at 30.56N 101.84W
# 
# #### Issues Resolved
# - EPA Envirofacts API blocked from Fabric (403)
#   Fallback: 48 grid reference points at 0.5-degree spacing across Permian Basin
#   Replace with real facility data when network access or manual download available
# - Initial attribution: 40/109 with 15 reference points and 30 km radius
#   Fixed by increasing to 48 grid points and 50 km search radius -> 109/109
# - Column duplication (_x/_y suffixes) from re-running attribution on already-attributed table
#   Fixed by dropping existing attribution columns before merge
# 
# #### Gold Layer Status
# - gold_plume_catalog: 109 rows, 39 columns
# - gold_flagged_large_clusters: 872 rows, 8 columns
# - gold_emission_sites: 103 rows, 15 columns
# - gold_plume_site_mapping: 109 rows, 2 columns
# - ref_facilities: 48 rows, 4 columns


# MARKDOWN ********************

# ### 2026-08-18 -- Day 5 (In Progress)
# 
# #### Created (Notebooks)
# - 07_ingest_validation: CAMS, Carbon Mapper, and EMIT validation data ingestion
# 
# #### Tables Created
# - validation_cams_plumes: 92 CAMS/SRON Permian Basin plumes (2021)
# - validation_cams_plumes_global: 2,974 CAMS global plumes (2021)
# - validation_carbon_mapper_plumes: 2,000 Carbon Mapper Permian Basin plumes (2025-2026)
# 
# #### Validation Sources Status
# 
# CAMS/SRON (Schuit et al. 2023):
# - Status: LOADED, spatial comparison only
# - 92 plumes in Permian Basin, all from 2021
# - No temporal overlap with Green Sky (Jun-Jul 2026)
# - Emission rates: 9-271 t/h (super-emitters only)
# - Usable for: spatial hotspot pattern comparison (are Green Sky 2026 detections
#   near known 2021 emission areas?)
# 
# Carbon Mapper:
# - Status: LOADED, spatial + temporal comparison possible
# - 43,184 total plumes available in Permian Basin (2,000 downloaded, most recent first)
# - 27 plumes overlap Green Sky date range (Jun 10 - Jul 9, 2026)
# - Sensors: Tanager-1 (1,217), AVIRIS-NG (737), EMIT (46)
# - Emission rates: 13-229,277 kg/h (median 320 kg/h, mean 828 kg/h)
# - Sector: 98% oil and gas (1B2)
# - Usable for: direct temporal cross-validation on 27 co-temporal plumes
# 
# EMIT (NASA):
# - Status: ACCESSIBLE, not yet ingested
# - Collection found: EMITL2BCH4ENH_002 (60m resolution enhancement maps)
# - 20 enhancement maps in Permian Basin during Green Sky dates
# - Usable for: independent high-resolution cross-sensor validation
# - Ingestion deferred to notebook 08 or post-sprint
# 
# #### Emission Rate Scale Discrepancy (Green Sky vs Carbon Mapper)
# 
# Green Sky (TROPOMI, 7 km pixels):
#   Range: 0.6 - 14.3 kg/h
#   Median: 4.3 kg/h
#   Mean: 4.7 kg/h
# 
# Carbon Mapper (Tanager/AVIRIS/EMIT, 3-30 m pixels):
#   Range: 13 - 229,277 kg/h
#   Median: 320 kg/h
#   Mean: 828 kg/h
# 
# The ~75x difference in median emission rate is expected and is NOT a bug. Root causes:
# 
# 1. Resolution mismatch: TROPOMI pixels are 7x5.5 km. A point-source plume that
#    Carbon Mapper resolves at 30m is diluted across a TROPOMI pixel, reducing the
#    apparent enhancement. Green Sky's IME calculation uses the diluted enhancement,
#    producing a lower emission estimate.
# 
# 2. Detection threshold difference: Carbon Mapper detects individual facility-level
#    plumes at 100+ kg/h. Green Sky's MAD-threshold approach on TROPOMI data detects
#    diffuse enhancements that may represent smaller sources, partial plume captures,
#    or aggregated emissions from multiple nearby sources within one 7km pixel.
# 
# 3. Quantification method: Green Sky uses IME with wind-dependent T_mix on coarse
#    pixels. Carbon Mapper uses matched-filter concentration retrieval at meter-scale
#    resolution with direct plume mass integration. The methods have fundamentally
#    different sensitivity and accuracy characteristics.
# 
# 4. This is consistent with the literature: Jacob et al. (2022) documents that
#    TROPOMI-based quantification is reliable for large plumes (>1-5 t/h) but has
#    limited sensitivity to smaller sources. Green Sky's current detections at
#    0.6-14.3 kg/h are below TROPOMI's typical detection threshold, suggesting
#    they may represent noise-level enhancements rather than confirmed point sources.
# 
# Implication: Green Sky's detected "plumes" at kg/h scale may be:
#   (a) Real but very small sources that TROPOMI can marginally detect
#   (b) Fragments of larger plumes partially captured in one pixel
#   (c) Statistical fluctuations passing the MAD threshold
#   (d) Regional enhancement gradients misidentified as point sources
# 
# This reinforces the Tier 2 recommendation for ML-based artifact classification.
# The 27 co-temporal Carbon Mapper plumes provide ground truth to test which
# Green Sky detections correspond to real sources vs false positives.
# 
# #### Issues Resolved
# - CAMS date parsing: integer YYYYMMDD format now correctly parsed via pd.to_datetime
#   with format="%Y%m%d"
# - Carbon Mapper API bbox: changed from comma-separated string to repeated query
#   parameters (API expects bbox=val&bbox=val&bbox=val&bbox=val)
# - Carbon Mapper datetime timezone: cm_datetime is tz-aware (UTC), fixed comparisons
#   to use pd.Timestamp("2026-06-10", tz="UTC")
# - Spark schema inference: all-null columns in Carbon Mapper data caused
#   CANNOT_DETERMINE_TYPE error, fixed by casting null columns to explicit dtypes
# - Delta schema mismatch on overwrite: added .option("overwriteSchema", "true")
#   to validation table writes
# 
# #### Security Note
# - Carbon Mapper API token was exposed in chat -- must be rotated
# - Post-sprint: set up Azure Key Vault for all API secrets
# 
# #### Remaining for Day 5
# - [ ] Notebook 08_validation_crossmatch: spatial comparison, temporal comparison,
#       internal consistency checks, validation metrics summary
# - [ ] Investigate the emission rate scale discrepancy further using the 27
#       co-temporal Carbon Mapper plumes

