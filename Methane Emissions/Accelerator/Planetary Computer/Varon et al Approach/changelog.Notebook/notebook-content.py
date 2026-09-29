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


# MARKDOWN ********************

# ### 2026-09-10 -- Day 6 (In Progress)
#
# #### Created (Notebooks)
# - 07b_detection_diagnostics: read-only diagnostic notebook (writes no tables) testing
#   whether the 109 plumes in gold_plume_catalog are real methane point sources or
#   artefacts at the TROPOMI instrument noise floor. Cells: discarded large-cluster audit
#   (are super-emitters being cut by max_cluster_pixels?), per-plume enhancement backed
#   out from IME vs the ~10-20 ppb TROPOMI precision band, CAMS and Carbon Mapper
#   emission-rate distribution comparisons (including 5 km spatial matching against
#   Carbon Mapper, regardless of date), and a mad_sigma sensitivity sweep (2/3/4/5).
# - 07c_quantification_diagnostics: read-only diagnostic notebook (writes no tables)
#   testing where the gap against Carbon Mapper lives in the quantification path rather
#   than detection. Reconstructs plume membership from silver_plume_ready_pixels (scene
#   separation + kNN background + clustering, matched back to gold_plume_catalog by
#   (scene_id, source_lat, source_lon) since plume_id is an unstable counter), audits
#   plume geometry (pairwise / nearest-neighbour pixel distances), compares the existing
#   kNN background against an alternative annulus background (25-100 km ring), recomputes
#   emission rate under four background/L combinations, and does a single-plume visual
#   deep dive.
#
# #### Findings (07b/07c diagnostics)
# - 07b: plume enhancements are real (mean ~32 ppb, above the TROPOMI noise floor); the
#   discarded large clusters are not the missing super-emitters; emission rate is nearly
#   insensitive to mad_sigma (plume count moves ~44x across sigma 2->5, median rate only
#   ~1.6x) -- pointing at the quantification path, not the detection threshold, as the
#   source of the ~100x gap against Carbon Mapper.
# - 07c: found duplicate pixel rows in accepted plumes -- e.g. plume 88 had 13 member
#   rows across 7 unique locations, each location appearing twice with near-identical CH4
#   (1931.955 vs 1931.908 ppb at the same lat/lon).
#
# #### Root Cause: NRTI/OFFL Overlap
# 02_ingest_tropomi_ch4 ingests both NRTI and OFFL processing modes. The same orbit is
# delivered as two separate STAC items with different stac_ids, covering the same
# physical pixels with slightly different retrievals -- exactly matching the 07c finding.
# Deduplicating on stac_id (or any key that includes it) is a silent no-op here, since the
# stac_ids differ for what is physically the same detector cell.
#
# #### Fixed
# - bronze_ch4_pixels (02_ingest_tropomi_ch4): added scanline, ground_pixel,
#   n_ground_pixels, processing_mode, orbit columns. scanline/ground_pixel are TROPOMI
#   PRODUCT-group dimension coordinates and come from ds.to_dataframe().reset_index()
#   directly (an earlier attempt to construct them via np.indices collided with these
#   same-named dimension coordinates and produced a duplicate-column DataFrame, which
#   crashed pd.to_numeric() downstream -- corrected, and assertions added right after
#   reset_index() to catch any recurrence loudly). orbit is read from the
#   sat:absolute_orbit STAC property, not parsed from item.id -- verified against live
#   STAC results that Planetary Computer truncates item IDs (no collection /
#   processor-version / production-time suffix), so the full ESA filename convention does
#   not apply; no fixed digit width is assumed for orbit.
# - silver_plume_ready_pixels (03_join_data): two new diagnostic cells print total rows,
#   distinct (stac_id, scanline, ground_pixel), distinct (stac_id, latitude, longitude),
#   distinct (orbit, scanline, ground_pixel), and the row split by processing_mode --
#   once right after bronze_ch4_pixels is read, once after the weather join -- to show
#   whether duplication is already present in bronze or introduced by the join fan-out.
#   Added a deterministic dedup keyed on (orbit, scanline, ground_pixel): OFFL preferred
#   over NRTI, then highest qa_value, then weather_dist_km ascending, then latitude
#   ascending -- no arbitrary dropDuplicates. scanline, ground_pixel, n_ground_pixels,
#   processing_mode, and orbit are now carried through to silver_plume_ready_pixels.
#
# #### Documentation
# - Added ARCHITECTURE.md at the repo root: full notebook table (purpose/inputs/outputs),
#   bronze -> silver -> gold Mermaid lineage diagram, exact column lists for every table
#   as derived from the code that writes it (not from filenames or docs), a
#   dependency-ordered execution sequence with parallelism notes, and every hard-coded
#   constant found outside 00_config with file:line references.
#
# #### Backlog
# - 02_ingest_tropomi_no2 was NOT changed -- no scanline / ground_pixel /
#   n_ground_pixels / processing_mode / orbit columns yet. Deferred because NO2 is only
#   used for co-location in 04b, not detection; noted in CLAUDE.md Known Issues.
#
# #### Remaining for Day 6
# - [ ] Re-run 02_ingest_tropomi_ch4 and 03_join_data end to end and confirm the
#       duplication diagnostics + dedup actually collapse the NRTI/OFFL overlap
# - [ ] Re-run 04_derive_emissions on the deduplicated silver table and compare against
#       the 109-plume baseline (emission rates, confidence distribution)
# - [ ] 07c's quantification hypotheses (background contamination, L definition) not yet
#       evaluated against a rerun -- detection logic in 04 itself is still unchanged


# MARKDOWN ********************

# ### 2026-09-11 -- Day 7 (In Progress)
#
# #### Fixed: IME units error (10,000x understatement of every emission rate)
# DRY_AIR_COLUMN = 2.12e25 is the dry-air column in molecules per SQUARE CENTIMETRE, but
# it was commented "molecules/m^2" and multiplied by PIXEL_AREA_M2, an area in SQUARE
# METRES. Since 1 m^2 = 1e4 cm^2, every ime_kg and every emission rate produced before
# today was low by a factor of exactly 10,000. Verified two ways before changing anything:
# - From first principles the dry-air column is (101325 / 9.81) / 0.028964 * 6.022e23
#   = 2.147e29 molecules/m^2, and 2.12e25 / 2.147e29 = 9.87e-5 -- the literal is ~1e-4 of
#   the per-m^2 value, exactly the cm^2-to-m^2 ratio.
# - At the observed mean 1901 ppb, reading the literal as molecules/cm^2 gives a CH4 total
#   column of 4.0e19 molecules/cm^2 against a published TROPOMI value of ~3.8e19; reading
#   it as molecules/m^2 gives 4.0e15, four orders of magnitude too small.
# The numeric literal was deliberately left at 2.12e25 and the conversion written out as
# an explicit step (DRY_AIR_COLUMN_PER_CM2 -> DRY_AIR_COLUMN_PER_M2 = * 1e4) so the
# mistake stays visible in the code rather than disappearing into a new magic number.
# PPB_TO_KG moves from 0.021740 to 217.400332 kg per ppb per pixel.
#
# #### Changed: 00_config
# - New "Physical constants for the IME conversion" cell. PIXEL_AREA_M2, AVOGADRO, M_CH4,
#   M_AIR, DRY_AIR_COLUMN_PER_CM2, DRY_AIR_COLUMN_PER_M2 and PPB_TO_KG now live here as
#   module-level names (following the BBOX precedent), so %run 00_config supplies them and
#   no call site changed. Units are in the name or the comment for every one. Carries the
#   full unit-error derivation above as an inline note.
# - Assertion immediately after PPB_TO_KG: 100 < PPB_TO_KG < 400 kg per ppb per pixel,
#   with a message stating that a value near 0.02 means the cm^2/m^2 confusion has
#   returned. Confirmed offline that it rejects the regressed expression.
# - Records that the value is a SEA-LEVEL standard atmosphere: the Permian sits at ~800 m
#   (~92 kPa), so the true column is ~9% lower and every IME is ~9% high. Left as an
#   approximation, with surface_pressure in silver_plume_ready_pixels noted as the route
#   to a per-pixel column if wanted (units need checking -- Open-Meteo reports hPa).
# - The destripe_* and collinearity_* keys were already present and were verified, not
#   re-added.
#
# #### Changed: 04_derive_emissions -- across-track destriping (new Step 2b)
# Addresses the 07c finding that the highest-rate accepted plume was seven perfectly
# collinear, evenly spaced pixels stepping 0.049 deg in latitude (the along-track pixel
# size) -- one detector column, not a plume.
# - Runs between background estimation and candidate detection, so it operates on the
#   enhancement field rather than raw XCH4: the kNN background has already removed the
#   large-scale structure, so what survives in a column median is instrument bias.
# - Groups on (stac_id, ground_pixel), NEVER ground_pixel alone -- ground_pixel is
#   granule-relative, not orbit-relative, so the same number in two granules is two
#   different physical detector columns. Same reason the 03_join_data dedup key had to
#   move to (orbit, latitude, longitude). Recorded in a comment at the grouping site.
# - Subtracts each group's median enhancement into a new ch4_enhancement_destriped column;
#   ch4_enhancement is kept for comparison. Groups with fewer than destripe_min_scanlines
#   distinct scanlines are skipped and counted. The median-robustness assumption (a few
#   plume pixels in a column cannot move it) is commented where it is relied on.
# - ch4_enhancement_destriped is used for candidate detection, the MAD, and IME onward.
# - Diagnostics per scene: granules, (stac_id, ground_pixel) groups, groups skipped, and
#   min/median/max correction in ppb; then the overall correction distribution and the
#   count exceeding destripe_max_correction_ppb.
#
# #### Changed: 04_derive_emissions -- collinearity rejection
# - principal_axis() now returns both the axis vector and the variance-explained fraction
#   from one PCA; Step 5's inline PCA was replaced by a call to it, so plume orientation
#   and the collinearity test are the same fit read two ways.
# - Clusters are rejected when the first principal component explains more than
#   collinearity_max_r2 of the variance (above collinearity_min_pixels unique locations),
#   or when every pixel shares a single (stac_id, ground_pixel) pair at any size.
# - New table gold_rejected_collinear: gold_flagged_large_clusters shape plus
#   variance_explained, n_column_pairs, the swath indices and would_be_valid. Written
#   outside the "any valid plumes" branch on purpose, so a run where rejection removes
#   everything still leaves the evidence.
# - The test runs before the aspect-ratio filter. The accepted set is identical either
#   way; going first means striping artefacts land in gold_rejected_collinear instead of
#   being dropped silently by the shape filter.
#
# #### Changed: 04_derive_emissions -- summary and pixel area
# - Summary cell now reports plume count and median rate before/after destriping and
#   before/after collinearity rejection, all on the same size + shape basis, plus the
#   granules-per-scene distribution (per-granule destriping groups get smaller and their
#   medians noisier when scenes hold several granules).
# - The two hard-coded (5.5 * 7.0) plume_area_km2 expressions now derive from
#   PIXEL_AREA_M2 / 1e6, so the pixel area has one definition. Value bit-identical
#   (38.5 km^2). Commented that plume_area_km2 feeds L_m, L_m feeds t_mix, and t_mix
#   divides IME -- so an inconsistency there propagates into every emission rate.
#
# #### Changed: 07b_detection_diagnostics
# - Local PPB_TO_KG / PIXEL_AREA_M2 / DRY_AIR_COLUMN / AVOGADRO / M_CH4 definitions
#   removed; all come from 00_config now. The comment records that this duplication is
#   what let the cm^2/m^2 error sit in three notebooks at once.
#
# #### Changed: 07c_quantification_diagnostics
# - Constants and the three remaining (5.5 * 7.0) literals replaced by the 00_config
#   values, each site commented with the duplication history.
# - The shared reconstruction cell gained Step 2b destriping and the collinearity /
#   single-column filter, so it reproduces the corrected 04. Detection, the MAD, the
#   source-pixel argmax and everything downstream now use ch4_enhancement_destriped. Both
#   backgrounds are destriped, so Cell 3's sweep varies only the background definition.
# - New Cell 0 -- did destriping work? Distinct (stac_id, ground_pixel) pairs per plume
#   with distribution and histogram, single-column plumes flagged explicitly as artefacts
#   that survived rejection, distinct granules per plume, and the count and pixel-share of
#   multi-granule plumes (NRTI and OFFL geolocate the same ground slightly differently, so
#   a plume seen in both would carry roughly twice the pixels and twice the IME).
# - New Cell 6 -- external validation at the corrected scale. Quartiles and medians for
#   Green Sky, CAMS and Carbon Mapper in kg/h with ratios to each, plus detection density
#   per unit area per day. Markdown makes CAMS the primary benchmark (TROPOMI-derived, so
#   it shares the instrument, detection limit and physics) and explains why Carbon Mapper
#   is weaker (aircraft/EMIT detection limits far lower, matched pairs have no date
#   constraint). Density uses each source's own area -- CAMS was filtered with a 0.5 deg
#   pad in 07_ingest_validation, so it covers ~209,000 km^2 against the bbox's ~125,000 --
#   and prints caveats, chiefly that neither figure corrects for observation-day coverage.
# - New Cell 7 -- remaining known biases quantified against the actual catalogue rather
#   than in the abstract: pixel area 5.5 x 7.0 -> 5.5 x 5.5 recomputed through IME, L_m,
#   t_mix and rate, and multi-granule plumes recomputed keeping only the granule with the
#   most pixels, then the combined effect. Changes nothing in 04.
# - Cells 1-5 keep their numbering and Cells 3-4 keep their structure, so the sweep and
#   the Carbon Mapper matched pairs stay directly comparable to the pre-correction run.
#   The new cell is numbered Cell 0 to avoid renumbering them.
# - Findings cell rewritten as a blank template, with a note that the Monte Carlo in 04
#   Step 8 is unseeded, so p5/p95, uncertainty_ratio and confidence vary run to run
#   (63 high / 12 medium moved to 66 high / 9 medium across two runs on an identical plume
#   set); emission_rate_kg_h itself is deterministic.
#
# #### Catalogue state after the corrections
# - 75 plumes, median 29.4 t/h, min 3.2 t/h, max 131.5 t/h (previous median 4.3 kg/h).
#
# #### Verification performed
# All changes were exercised offline against synthetic pixels, not in Fabric. The 04
# harness built a scene containing a 60-scanline stripe, a stripe too short to destripe,
# and genuine compact plumes: destriping removed the long stripe (105 -> 20 candidates,
# 27.1 ppb correction), the short stripe was correctly skipped and then caught by the
# reject filters, and the real plume survived. A second harness ran the patched 04 to
# build a catalogue and fed it through the patched 07c: all 11 cells execute and every
# gold plume matched its reconstruction, confirming 04 and 07c stayed in sync. 07c Cell 7
# reproduces the catalogue median exactly and its pixel-area result (0.886x, -11.4%)
# matches the analytic sqrt(0.7857) = 0.8864.
#
# #### Flagged, not changed
# - CLAUDE.md Known Issues still says observed rates are "~100x below the 100 kg/h NSPS
#   OLRE threshold". With the units fix that inverts -- the catalogue now sits well above
#   it. The entry is tracked, so it was left for review rather than edited.
# - 04's plausibility check warns on emission_rate_t_h > 100. That threshold was
#   calibrated against the broken numbers and will now fire on real plumes.
# - Two hard-coded (5.5 * 7.0) expressions remain in 07b (lines 145 and 534), still
#   independent of PIXEL_AREA_M2. 07b already %runs 00_config, so the fix is the same
#   one-line substitution applied in 04 and 07c today.
# - 04's collinearity rejection was also added to 07c's reconstruction, which was not
#   requested. It can only remove clusters 04 also rejected, so it cannot cause a gold
#   plume to go unmatched; it makes n_reconstructed mean the same thing as 04's accepted
#   count.
#
# #### Backlog
# - gold_rejected_collinear added to the CLAUDE.md table list.
# - The Monte Carlo in 04 Step 8 should take a seed from CONFIG so confidence and the
#   p5/p95 bounds are reproducible.
# - Per-pixel dry-air column from surface_pressure, replacing the sea-level constant
#   (~9% high over the Permian).
#
# #### Remaining for Day 7
# - [ ] Run 00_config, 04_derive_emissions and 07c in Fabric and confirm the destriping
#       diagnostics, the PPB_TO_KG assertion and gold_rejected_collinear behave as the
#       offline harnesses predict
# - [ ] Read 07c Cell 0: did any accepted plume survive on a single detector column, and
#       do any plumes draw on more than one granule?
# - [ ] Read 07c Cell 6: how does the corrected median compare against CAMS, and is the
#       detection density believable?
# - [ ] Fill in the 07c findings cell
# - [ ] Decide whether to apply the 5.5 x 5.5 pixel-area correction in 04 on the evidence
#       from 07c Cell 7


# MARKDOWN ********************

# ### 2026-09-15 -- Day 8 (Completed)
#
# Two pieces of groundwork, both read-only or additive: a survey of the accelerator's
# enterprise model, and the design note that governs every fact generator built after it.
#
# #### Added: GREENSKY_LAKEHOUSE_SURVEY.md
# Static analysis of GreenSky_Lakehouse -- the accelerator model behind the Data Agent and
# the RTI dashboard -- ahead of designing a replacement SCADA layer. It is a DIFFERENT
# WORKSPACE (060ba34b-...) from Green Sky - Dev (640876ea-...), so it shares no tables with
# V2; both had to be understood before a third model was built beside them.
# - Access matrix for all 16 tables across bronze / silver / gold / dbo, with which of
#   Incremental_Load, Nb_Bronze_to_Silver and Nb_Gold reads or writes each.
# - Full column lists with types, and the transformation logic behind every silver and gold
#   table.
# - Findings that matter for anything built on top of it: the four CREATE TABLE statements
#   in Nb_Gold declare NARROWER tables than exist physically (dim_facility DDL says 8
#   columns, the table has 18), and IF NOT EXISTS makes the DDL a permanent no-op, so anyone
#   reading it designs against the wrong schema. Every delta write for the three gold
#   dimensions is commented out -- only Kusto appends run -- yet Nb_Gold reads those same
#   tables back to build its fact. The Data Agent's instructions and its single few-shot
#   query both target gold.fact_emission_events and four vw_* views, none of which exist.
#   Nb_Gold randomises every event_date to today or yesterday, and Incremental_Load appends
#   date-shifted rows back into the same table with no dedup.
# - Recorded three incompatible facility business keys for the same basin -- FAC-0001 (V1
#   Operations_LH), WP-001 (accelerator), PB_001 (V2 ref_facilities) -- with nothing mapping
#   between them. That is what forced the key decision in Day 9's rebuild.
#
# #### Added: survey_greensky_lakehouse notebook
# Read-only profiling to answer what static analysis cannot: row counts, null rates,
# distinct counts, timestamp ranges and modal sampling intervals, plus a deep dive on
# scada_realtime (is it a real 15-minute series or a stub?) and facility_master (are the
# names real or templated). Discovers tables via SHOW TABLES rather than a hard-coded list
# and skips absent schemas cleanly. Verified to contain no write of any kind.
# Moved into notebooks/07_validation/ so it syncs with Green Sky - Dev, and its lakehouse
# binding dropped to an empty object -- a cross-workspace GUID does not survive Git sync, so
# GreenSky_Lakehouse is attached by hand before a run. A markdown cell at the top states the
# workspace split, the manual attach step and that the notebook writes nothing.
#
# #### Added: DESIGN_NOTE_incremental_facts.md
# The agreed model for how enterprise fact generators behave on daily runs. Status: proposed.
# Three requirements: topology is a setup-only pipeline the daily run reads and never writes;
# fact writes are window-scoped and idempotent via replaceWhere on a date_sk partition; and
# every backlog drains through explicit state transitions in a two-pass generator rather than
# having its outcome fixed at creation. Calls out two blocking prerequisites -- unstable
# plume_id (already tracked in CLAUDE.md) and no fact table partitioning by date_sk.
# CLAUDE.md gained a "Design notes" section pointing at it. That file sits above the repo
# root and is not tracked, so it is in no commit.


# MARKDOWN ********************

# ### 2026-09-16 -- Day 9 (Completed)
#
# Rebuilt the enterprise facility and asset topology from scratch in notebooks/01_topology.
# The V1 model in Operations_LH is replaced, not extended, and remains reference-only.
#
# #### Why the V1 model could not be repaired
# Three defects, one of them mis-diagnosed until the V1 code was read properly:
# - dim_facility carried TWO coordinate pairs -- latitude/longitude from dim_build and
#   facility_lat/facility_lon written later by build_attribution, which overwrote the
#   dimension -- with nothing recording which was authoritative.
# - CORRECTION TO THE BRIEF: the out-of-BBOX facilities were NOT caused by
#   build_attribution's np.random.default_rng(42) reseed. That reseed draws from
#   lat 31.5-33.5 / lon -105.0 to -101.3, which is INSIDE the V2 BBOX and cannot produce
#   FAC-0157 at latitude 29.48. The real cause is structural and lives in dim_build: the
#   "Texas Site A" anchor sits at lat 30.2, below the BBOX floor of 30.5, carries 40% of the
#   estate, and its outside band reaches 0.95 deg -- so facilities land near 29.25 by design,
#   seed or no seed. The reseed is a separate defect that made facility_lat/lon in-BBOX but
#   uniform-random and unclustered. Neither pair was salvageable, which is the strongest
#   argument for a rebuild rather than a repair.
# - facility_name and facility_type were drawn independently, producing "Odessa Processing
#   Plant" typed Gathering System.
#
# #### Added: 01_topology_config
# Seeds, geography, taxonomy and helpers. TOPOLOGY_SEED = 20260915, deliberately distinct
# from V1's MASTER_SEED. get_rng(*parts) is the only route randomness takes; a bare
# default_rng anywhere in 01_topology is a bug.
# - Facility keys are GS-nnnn, NOT V1's FAC-nnnn. 350 FAC- keys still exist in Operations_LH
#   at different coordinates from a different seed, and reusing the prefix would make the two
#   estates indistinguishable in a query result.
# - Anchors are real Permian sub-basins, all inside CONFIG["bbox"].
# - Type is drawn FIRST and the name built from it via TYPE_DESCRIPTORS, so name and type
#   agree by construction. assert_descriptor_map_unique() guards the map against a later edit
#   putting one descriptor under two types.
# - TOPOLOGY_AS_OF is a fixed date(2026, 9, 15), never date.today(), with an assertion that
#   it is not in the future. NOTE: this was already a fixed literal when it was flagged as a
#   moving-date defect -- the value simply happened to equal that day's date. The assertion
#   and the comment were added; no bug was fixed.
#
# #### Added: 01a_build_facility_topology
# Writes dim_facility and ref_facilities. One coordinate pair, facility_lat/facility_lon,
# written once. 150 facilities -- not V1's 350 -- sized against the detection rate so the
# Facility Operations dashboard page is not empty for most sites.
# - Band allocation is an exact QUOTA via largest-remainder, not a per-facility draw, so the
#   realised 75/20/5 split matches the configured one instead of drifting with sampling
#   noise.
# - A coverage cell measures, against gold_plume_catalog, the distance from every plume to
#   its nearest facility, the share beyond attribution_search_radius_km, and the median
#   latitude of uncovered versus covered plumes. The latitude comparison is what makes a
#   directional gap visible rather than just a count.
#
# #### Added: 01b_build_asset_topology
# Writes dim_equipment and dim_sensor. Assets carry NO geography and reach it by joining
# facility_id -- V1's silver.equipment_geo copied facility coordinates onto equipment and the
# copy then diverged. Asserted: no lat/lon-shaped column on either table.
# - Equipment mix is weighted by facility type via TYPE_EQUIPMENT_WEIGHTS. The first version
#   drew uniformly and gave every site the same profile -- all eight types between 11.6% and
#   13.5%, so a tank battery held as many compressors as a gas processing plant.
# - Asset count follows EQUIP_COUNT_BY_TYPE per facility type; EQUIP_PER_FACILITY is now
#   DERIVED as the global min/max rather than declared, so the two cannot drift.
# - A mix assertion written as a rank test (dominant type must equal highest-weighted type)
#   was WRONG and was replaced: Gas Processing Plant has Compressor at 26% and Separator at
#   24%, two points apart over ~500 assets, so which lands on top is noise. It now checks each
#   share against 3 sigma on a binomial, plus that the dominant share clears 1.4x the uniform
#   12.5% -- the test a uniform draw actually fails.
#
# #### Changed: 05_attribute_facilities
# The EPA Envirofacts scrape, the BBOX filter, the PB_nnn grid fallback and the
# ref_facilities write were removed (-171 lines) and replaced by a read plus a guard that
# fails if PB_nnn keys are ever found in the table. Necessary, not opportunistic: with 01a
# writing ref_facilities and 05 also writing it, running 05 afterwards would have replaced
# the real topology with grid points. 05 now has no network dependency.
#
# #### Anchor and radius work, in two passes
# - A fourth anchor, Northwest Shelf, closes a northern coverage gap: 32 of 75 plumes had
#   facilities_in_range = 0, all with a nearest facility beyond 50 km, sitting at median
#   latitude 32.96 against 31.88 for attributed plumes. Weights rebalanced to
#   0.37 / 0.33 / 0.18 / 0.12, taking 0.18 proportionally from the existing three.
# - Placed at 32.90 N, not the 33.0 N scoped, because an anchor plus its perimeter radius
#   must stay under the clamp at max_lat - EDGE_INSET_DEG = 33.48. A per-anchor clearance
#   assertion was added for both axes.
# - Radii then widened 0.30/0.55 -> 0.55/0.85 to close interstitial voids between four tight
#   clusters. That assertion FAILED for Northwest Shelf, which has 0.580 deg of room and
#   needed 0.85 -- reported rather than worked around. Resolved with per-anchor radii:
#   Northwest Shelf carries 0.37/0.57, preserving the global region:perimeter ratio. BOTH
#   radii are overridden, not just the perimeter -- setting perimeter alone to 0.55 would
#   equal REGION_RADIUS_DEG and collapse the band to a ring. MIN_BAND_WIDTH_DEG guards that.
# - Measured under UNIFORM plumes: uncovered share 32.0% -> 17.3%, worst-case nearest
#   facility 159.6 km -> 79.3 km, nothing now beyond 100 km. Mean nearest-neighbour spacing
#   9.8 -> 13.1 km against a 50 km attribution radius, which confirms density was never the
#   constraint -- the problem was four islands with voids between them.
#
# #### Flagged, not changed
# - Realised band split is 113/30/7 at N=150. The +/-1 is integer rounding on 150 * 0.75,
#   not sampling noise.
# - An earlier harness shaped its synthetic plume catalogue to match a reported northern skew
#   and predicted 9 uncovered plumes where the real run gave 31. Shaping the harness to the
#   last diagnosis made it agree with that diagnosis and nothing else; it now draws
#   uniformly, which is the neutral and harder test.


# MARKDOWN ********************

# ### 2026-09-17 -- Day 10 (Completed)
#
# Added the process-area level, the SCADA tag registry, and the first fact generator governed
# by DESIGN_NOTE_incremental_facts.md.
#
# #### Added: 01c_build_area_topology
# Writes dim_area (558 areas, 16 types) and adds area_sk / area_id to dim_equipment. The
# hierarchy becomes facility -> area -> asset, which SCADA tag naming and operations triage
# both need.
# - stable_key() added to 01_topology_config. NOTE: the brief described it as "hash-derived,
#   as elsewhere", but no such helper existed -- every other V2 surrogate key is a sequential
#   1..N counter. It is new, and hash-derived on purpose: area counts vary per facility, so a
#   sequential counter would renumber every area downstream of any facility whose count
#   changed. TOPOLOGY_SEED is folded in, so GS-0001-A1 under two seeds cannot alias.
#   ent_rng() likewise does not exist; get_rng() is the same function and was used.
# - Assets keep no area geography. dim_area carries area_lat/area_lon offset 100-300 m from
#   the facility centre, asserted under 500 m. Commented at the definition that this exists
#   for plausibility and future asset-level attribution ONLY: a TROPOMI pixel is ~5.5 x 7.0
#   km, so an entire facility sits inside a fraction of one and nothing in the detection
#   layer can distinguish one area from another.
# - Assets are assigned to an area of their OWN facility respecting equipment type, with a
#   fallback to the primary area where no area accepts the type. Measured fallback 2.07%
#   against a 10% threshold.
# - AREA_TYPES diverges from the shape scoped in three places, all forced by
#   TYPE_EQUIPMENT_WEIGHTS giving every equipment type a non-zero weight everywhere:
#   Gathering System gains Field Compression and moves 2-4 -> 3-5 areas with Metering
#   mandatory (the scoped four areas left 22% of its assets homeless); Central Delivery Point
#   gains Utilities; Tank Farm accepts Flare.
#
# #### Added: 01d_build_scada_tags
# Writes dim_scada_tag -- 3,965 process-measurement tags on 662 instrumented assets. A
# SECOND registry: dim_sensor keeps its 600 CH4 detectors and its contract untouched, because
# gold.sensor_telemetry depends on its schema.
# - Instrumentation policy is the volume control. 662 of 3,135 assets (21.1%), inside the
#   600-900 target, from INSTRUMENTABLE_CRITICALITY {Critical, High} and a cap of 6 per
#   facility. Valves and pipeline segments are never instrumented. Selection is a stable
#   mergesort on (criticality_rank, equipment_type_priority, equipment_id).
# - Combustion tags -- pilot_flame, stack_temperature, air_fuel_ratio -- exist so the
#   enterprise layer can corroborate 04b's Fugitive Leak versus Incomplete Combustion split,
#   which rests on the NO2 signature. Recorded at the definition so they are not trimmed as
#   decoration.
# - Three units beyond those scoped, because the scoped list would have been wrong: bpd for
#   liquid flow, inH2O for orifice differential pressure, state for pilot flame.
# - Volume table printed before the write: 17.1M rows for a 30-day raw window against a 30M
#   assertion. The two tiers contribute almost exactly equal row counts, so HOT_TAG_SHARE and
#   the cadences are the effective levers, not tag count. Storage figures are explicitly
#   labelled an assumption, not a measurement.
#
# #### Added: 02a_build_asset_state
# Writes fact_asset_state, a sparse interval table -- one row per state change, never one row
# per timestamp. 20,264 intervals over 90 days, ~6,755 per 30 days against a 200k cap. First
# notebook to implement the design note's two-pass structure.
# - Six states, five causes. A trip goes straight to Down with no Shutdown, which is what
#   distinguishes it from a planned stop; every chain returns through Startup, so the
#   Startup/Shutdown invariants hold by construction rather than by assertion.
# - Each interval's duration is drawn ONCE at creation from get_rng("state", equipment_id,
#   start_ts). Closure is a pure function of elapsed time against that fixed duration, which
#   is what makes a backfill and 30 successive incremental runs byte-identical. Verified in
#   the harness.
# - STATE_DUTY_FACTOR added rather than overloading leak_propensity. leak_propensity
#   describes how likely something is to LEAK, not to STOP, and Storage Tank sits at 0.85 --
#   using it alone would make a static vessel trip almost as often as a compressor.
# - date_sk is the interval's START day. An interval that starts before the window and closes
#   inside it therefore lives in a partition outside the window. Chosen: widen the
#   replaceWhere predicate to cover every partition the run touches, rather than adding
#   close_date_sk -- replaceWhere can only scope on partition columns, so close_date_sk would
#   only help if the table were partitioned by it too, and then no row sits in a partition
#   containing its own start. Existing rows in the widened range are read, superseded rows
#   dropped by state_sk, and the union written back; skipping that would silently delete them.
# - The incremental window comes from the table's own watermark, max(date_sk) + 1, NOT from
#   utcnow() as the V1 notebooks use. The wall clock makes a rerun land on a different window.
#
# #### Two guards caught their own bugs
# - Churn was measured on rows WRITTEN, which on a one-day incremental includes the rows the
#   widened replaceWhere range sweeps in. Scaling those to 30 days reported 446k against a
#   200k cap. Now measured on intervals opened within the window.
# - The availability upper bound of 0.9999 failed a CORRECT run: over one day, Pipeline
#   Segment sat at exactly 100% because no static asset changed state. Bound raised to 1.0;
#   the meaningful guard was always the lower one.
#
# #### Tuned once, on evidence
# Maintenance dwell was first set at 3-14h (PM) and 6-40h (corrective) and produced 98.8%
# availability with NO equipment type inside the 92-97% target -- the event rate was right,
# each event was too short. Raised to 6-24h and 12-72h, reflecting the time to mobilise a
# crew and parts to a remote pad. Compressor now 95.2%, Pump 96.8%.
#
# #### Flagged, not changed
# - Static equipment sits above the 92-97% band -- Pipeline Segment 99.4%, Storage Tank
#   99.0%. Left there: a storage tank with a 365-day inspection interval genuinely does not
#   stop often, and forcing it into band would mean inventing downtime.
# - 14 areas hold zero assets. Mandatory areas are created whether or not the equipment draw
#   produced anything for them.
# - All 991 hot tags sit on Critical assets; the 25% share is consumed before reaching High,
#   so tier correlates perfectly with criticality rather than partially.
# - 3 facilities carry no SCADA tags at all -- they hold no instrumentable asset at Critical
#   or High.
#
# #### Remaining
# - [ ] Nothing in 01_topology or 02_scada has been run in Fabric. Every figure above is from
#       the offline harness with Spark stubbed.
# - [ ] 01a's coverage cell needs a real gold_plume_catalog to size N_FACILITIES properly;
#       the 17.3% uncovered figure is against a uniform synthetic catalogue.
# - [ ] Telemetry generation, then alarms and maintenance records.


# MARKDOWN ********************

# ### 2026-09-22 -- Day 11 (Completed)
#
# SCADA telemetry and the alarms derived from it, plus an offline tools/ directory.
#
# #### Added: 02b_gen_scada_telemetry
# Writes scada_telemetry, Spark-native, partitioned by date_sk and written one day at a time
# with replaceWhere.
# - Values are a pure function of (tag, interval_index): deterministic spectral synthesis
#   (six fixed-frequency harmonics, hash-derived amplitudes and phases), a shared per-asset
#   load series and a per-facility series, state conditioning from fact_asset_state, drift,
#   noise and quantisation. Every per-row random value comes from one sha2 per row, checked
#   against golden vectors computed offline.
# - Outages and freezes are drawn on a fixed slot grid anchored at TELEMETRY_EPOCH, not per
#   run window, so an incremental day sees exactly the outages a backfill would. Outage rows
#   are absent, not null, which is what makes "Sensors Offline" computable.
# - On Fabric: 15,479,087 rows over 30 days from 3,895 of 3,965 tags (978 hot at 300 s,
#   2,917 standard at 900 s). Quality Good 95.1%, Substituted 3.3%, Uncertain 1.1%,
#   Bad 0.5%. Offline share 2.035% from 2,966 outage intervals plus 47 decommissioned tags.
#
# #### Fixed in 02b, three accounting bugs
# - Overlapping outages matched a reading twice through the half-open interval join,
#   inflating the slot count and double-counting offline seconds. A Faulty tag draws
#   Poisson(8) arrivals per 45-day slot, so overlaps were not rare. merge_intervals now
#   collapses them at the source and assert_disjoint proves it; freezes get the same
#   treatment.
# - The offline share measured ~9.4% against a 2% target, because tags installed part-way
#   through the window (~12% of them -- the estate is young) scored their pre-install slots
#   as offline. window_slots now clips to each tag's install date.
# - The slot grid rounded its two ends differently (ceil at one, floor at the other). That
#   was invisible on whole days, where midnight divides both cadences, and undercounted by
#   exactly one slot per outage interval. slot_ceil is now used at both ends, and cadences
#   are asserted to divide 86,400.
#
# #### Added: 02d_gen_alarms
# Writes fact_scada_alarm and fact_sensor_status_event, both derived entirely from
# scada_telemetry. There is no independent alarm stream: every alarm has breaching readings
# behind it, so drilling from an alarm to its trend finds the excursion.
# - Four suppressions: debounce, deadband, quality (Bad and Substituted never raise,
#   Uncertain neither raises nor clears), and state (no alarms while Down, Maintenance,
#   Startup or Shutdown; Standby also suppresses flow and rpm).
# - Derived from the full 30-day retention in both run modes, not a lookback window. The
#   debounce raises on the Nth consecutive breach, so a scan edge landing mid-run raises late
#   or never; in the harness a 7-day lookback disagreed with the backfill on 119 of 400
#   simulated months. A full scan makes backfill and incremental identical by construction.
# - TELEMETRY_PROCESS_SD_FRACTION raised 0.25 -> 0.33 in 01_topology_config. At 0.25 the
#   normal band edge sat at 4.0 sigma, the process never crossed it, and the alarm table came
#   out effectively empty. At 0.33 the band edge is at 3.03 sigma.
# - On Fabric: 3,942 alarms and 5,447 sensor status events.
#
# #### Fixed in 02d: alarm_sk
# - The 63-bit reduction was written % F.lit(2**63). 2**63 is one past Long.MAX_VALUE, so
#   Spark could not build the literal and the golden-vector check failed at construction.
#   The modulus is now a string cast to decimal(20,0), shared with event_sk as sk_from_sha,
#   and the key check runs in two layers: digest first, then the reduction.
# - alarm_sk was stable_key("alarm", tag_id, raised_ts), omitting alarm_type, while the
#   table's grain includes it. A value crossing Lo and LoLo on the same reading raised two
#   alarms at one raised_ts, and they collided: 3 collisions in 646 alarms in the harness,
#   every one a Lo/LoLo pair from a compressor entering Standby. alarm_type is now in the
#   key, and uniqueness is asserted before the write, not three sections into validation.
# - event_sk was checked and is unique. A tag cannot leave and return at one instant, by
#   construction.
#
# #### Added: tools/
# Offline checks, none of which run in Fabric.
# - lint_lit.py flags integer literals outside the Java long range, the one Spark-side
#   defect detectable without a JVM. It catches the % F.lit(2**63) bug above.
# - check_nb.py checks Fabric cell structure and Python syntax.
# - harness/ is a NumPy mirror of 02b's value model and 02d's sessioniser.
#   harness_alarm_keys.py closes the gap that hid the collision: the existing sessioniser
#   harness exercised one (tag, alarm_type) stream at a time and never built a key.
# - The README states plainly what none of this catches: whether a Spark expression computes
#   what its Python counterpart does. Only the in-notebook golden vectors check that, and
#   only on Fabric.


# MARKDOWN ********************

# ### 2026-09-25 -- Day 12 (Completed)
#
# Two validation fixes in 02d, telemetry rollups, the CH4 detector telemetry, and stable
# plume identifiers with a reproducible Monte Carlo.
#
# #### Fixed in 02d validation
# - Sections 1 and 2 joined alarms to telemetry with tag_sk unqualified on both sides, which
#   throws AMBIGUOUS_REFERENCE on Spark. Both sides are now aliased and every column
#   reference is qualified. This had been patched by hand in the Fabric session; the repo now
#   carries the same fix, so it does not come back on the next sync.
# - Section 1 passed on the exact failure it exists to catch. It left-joined on the upper time
#   bound and applied the lower bound as a filter afterwards, which discarded an alarm with
#   no readings in its window. Both bounds are now in the join condition. n_breach is
#   coalesce(sum(...), 0), because sum() over an all-null group is null and null < N is not
#   true -- moving the bound alone would still have dropped the alarm.
#   harness_alarm_backing.py reproduces both failure modes and asserts the fix.
#
# #### Added: 02c_rollup_telemetry
# Writes scada_telemetry_hourly and scada_telemetry_daily, over exactly the raw window
# (ROLLUP_MATCH_RAW_WINDOW in 01_topology_config records why).
# - Value columns are over Good readings only; counts are over every reading. A bucket with
#   no Good reading keeps its row, with null values. An hour with no readings has no row --
#   nothing is fabricated.
# - The day grain is rolled from the written hourly table, from summed sums, so value_avg is
#   good_count-weighted by construction. The sums are exact integers (values scaled by 1e6,
#   squares in decimal(38,0)). That makes the day's stddev exact -- a frozen day is exactly 0,
#   where a double sum(x^2) formula gave ~3e-5 -- and makes every aggregate independent of
#   Spark's merge order.
# - On Fabric: scada_telemetry_hourly holds 2.56M rows.
#
# #### Added: 02e_gen_ch4_telemetry
# Writes sensor_telemetry for the 600 CH4 detectors, keeping V1's gold.sensor_telemetry
# columns exactly (build_snapshots selects them by name), plus date_sk and is_synthetic.
# - Hourly, replacing V1's 4 hours, which left two readings inside the 8-hour offline horizon.
#   CH4_INTERVAL_HOURS = 1 in 01_topology_config, and dim_sensor.reading_interval_hours now
#   carries it.
# - dim_sensor did not have exceedance_threshold_ppm, sigma_ppm or tier, although the brief
#   described them as existing; 01b now writes them. It also writes an exact 1.5% Faulty /
#   1.0% Decommissioned status mix, where every sensor had been Active.
# - 02b's hash, spectral, slot-grid, interval and state-join code is copied in verbatim, and
#   harness_ch4.py fails if the copy ever differs from 02b's text.
# - Exceedances come from the sensor's own spectral series with time stretched x8, so a
#   crossing lasts hours: the median run was 1 h unstretched, 6 h stretched. They are
#   concentrated by relative risk (leak_propensity x age / life, normalised to the estate
#   mean) and by Standby. Startup and Shutdown multiply an existing leak by 1.5 rather than
#   lowering the onset, because a one-hour onset step made isolated spikes that pulled a
#   quiet sensor's autocorrelation to ~0.4. Maintenance suppresses the leak term.
# - Harness, on a synthetic registry: exceedance rate 1.10%, a third of sensors exceed and the
#   top 10% hold 75%, runs have a median of 6 h, minimum lag-1 autocorrelation 0.80, offline
#   share 1.98%, and a non-zero 8-hour KPI at every probe. The rate and offline bands are
#   asserted over the whole 30-day table, since single days ran 0.5-1.7%.
#
# #### Fixed: plume_id and scene_id are content-derived
# Removes the "iteration-order counters" known issue. 04 overwrote gold_plume_catalog with
# counters on every run, silently invalidating gold_plume_site_mapping,
# gold_multi_gas_signatures and 05's attribution.
# - scene_id is "SCN-" plus the scene group's UTC start. plume_id is "PL-" plus 12 hex of
#   sha256(scene_id|source_lat|source_lon). Both helpers live in 00_config, because 07c
#   matches its reconstructed scenes against gold_plume_catalog by scene_id, and a leftover
#   counter there would have matched nothing.
# - No detection logic changed. The new labels sort chronologically, so every per-scene loop
#   visits scenes in the old order. A within-run cluster_idx keys the intermediate steps, and
#   plume_id is assigned where source_lat/source_lon first exist.
# - Downstream: 04b no longer mints a monotonically_increasing_id when plume_id is missing;
#   04, 04b and 06 write with overwriteSchema for the bigint -> string change; 06 breaks
#   detection_date ties on plume_id so site assignment is reproducible.
# - 04 asserts plume_id unique and never numeric, and records each run's input fingerprint
#   and plume_id set in gold_plume_id_runs. A rerun on the same input fails if the set
#   changes. That check fires only on Fabric.
#
# #### Fixed: the Monte Carlo is seeded per plume
# - Step 8 drew from the unseeded global np.random, so p5/p50/p95, uncertainty_ratio and
#   confidence moved on every run: confidence 66/9 then 65/10 on identical input, and one
#   plume's p95 26,385.68 -> 26,126.62 kg/h. gen_financial consumes those bounds directly.
# - Each plume now draws from default_rng(mc_seed(plume_id)). mc_seed follows stable_key
#   without TOPOLOGY_SEED, as plume_id does. It is per plume rather than one global seed,
#   because a shared generator hands out draws in processing order; in the harness, dropping
#   one plume shifted 7 of 9 others' bounds under a global seed and none under per-plume
#   seeds. The sampling itself is unchanged.
# - 04 re-runs the Monte Carlo for five plumes and asserts identical percentiles.
#   gold_plume_id_runs also records each plume's p50, and a rerun on the same input fails if
#   any p50 moves by more than 1e-9 relative.
#
# #### Flagged, not changed
# - 02e's harness uses a synthetic registry and state history, because the real ones live in
#   OneLake. Its rates show the mechanism works; Fabric's figures will differ somewhat.
# - 04's "same 75 plumes" check against the pre-change catalog is print-only. 04 cannot tell
#   whether silver_plume_ready_pixels changed since that run, so an assertion could fail a
#   correct run.
# - The first 04 run after the seeding change differs from the existing catalog's bounds,
#   once. It has no seeded run to compare against.
#
# #### Remaining
# - [ ] Run on Fabric: 01b, then 01c (01b rewrites dim_equipment without 01c's area columns),
#       then 02e in backfill mode.
# - [ ] Run 04 twice unchanged. The first run should print 75 of 75 against the old catalog;
#       the second exercises the rerun regression for both plume_id and p50.
# - [ ] Then 04b, 05, 06 and 07c. 07c's match count should stay 75 of 75.
# - [ ] Enterprise fact layer next, starting with gen_financial, which consumes the now
#       reproducible emission_rate_p5/p50/p95.


# MARKDOWN ********************

# ### 2026-09-28 -- Day 13 (In Progress)
#
# The single-column striping hole in 04, closed in two steps, a false alarm in 03's row
# count, and the first enterprise fact generator: 03a, the hidden emission episodes.
#
# #### Fixed: 04's single-column rejection keys on the physical column
# - The test rejected a cluster whose pixels all shared one (stac_id, ground_pixel). stac_id
#   names a granule, not a detector column, and the NRTI and OFFL granules of one orbit carry
#   the same column. A stripe drawn from both granules had two pairs and escaped. That is how
#   PL-059118a52421 became the catalogue's top emitter at 131,498 kg/h, on ground_pixel 65 of
#   orbit 45136, from two granules.
# - Now keyed on (orbit, ground_pixel). orbit is sat:absolute_orbit, so it identifies the
#   overpass. Destriping stays on (stac_id, ground_pixel): the stripe median is a per-granule
#   bias estimate, scanline is granule-relative, and NRTI and OFFL are separate retrievals.
#   Both sites say why the keys differ. The old step 2b comment claimed the same ground_pixel
#   in two granules is two physical columns; the evidence contradicts that, so it was
#   corrected.
# - The collinearity variance test did not need the same change. Duplicating a point leaves
#   variance-explained unchanged. Duplicates only inflate the location count, which can
#   make a tiny cluster get tested and rejected as collinear -- it errs toward rejecting
#   too much.
# - 07c Cell 0 counted pairs, so it reported "rejection did its job" on this very case. It
#   now reports physical columns and pairs side by side.
#
# #### Fixed: single-column rejection by dominance, not unanimity (detection v3)
# - PL-059118a52421 still survived: 14 of its 15 pixels sit on (45136, 65), stepping
#   0.049 deg in latitude over 33 km, and one stray pixel from the adjacent column defeated a
#   test that required every pixel on one column.
# - 04 now rejects when the largest (orbit, ground_pixel) group holds at least
#   single_column_dominance (0.80) of the pixels, and at least
#   single_column_min_group_pixels (3) of them. Both are new CONFIG keys in 00_config. The
#   group minimum cannot bind today, since 80% of 3 or more pixels is always 3 or more.
# - dominant_column_share is on gold_rejected_collinear. 04's accepted-plume assertion
#   applies the same rule. 04 prints the share distribution over the plumes the old rule
#   would have accepted, so the 0.80 cut can be checked against real data.
# - DETECTION_VERSION goes 2 -> 3. DETECTION_VERSION itself had been pasted into the
#   Fabric copy of 04 only; it was synced into the repo first (3997882), then built on. Four
#   scratch query cells from that Fabric session were removed. One looked up
#   PL-059118a52421 with .collect()[0] and would have raised once the fix worked.
#
# #### Fixed: 03's nearest-station count
# - 03 printed "Should match original methane count: 70,126" against 34,604.
#   bronze_ch4_pixels is cumulative across ingests while weather and ERA5 cover only
#   CONFIG's window, so the two could never match.
# - 03 now counts bronze pixels inside start_date 00:00Z .. end_date + 1 day, and asserts
#   the nearest-station count equals distinct (latitude, longitude, datetime) in that window
#   -- the key the nearest-station window partitions on, so a pixel duplicated across two
#   overlapping granules is not counted as a loss. The expansion factor now divides by the
#   in-window count too; it had understated stations per pixel by about half.
#
# #### Added: 03a_gen_emission_episodes
# Writes fact_emission_episode: one row per leak episode, the hidden ground truth that plumes
# and telemetry observe. Reads dim_equipment, dim_facility, dim_area and fact_asset_state
# only. Every read goes through read_input(), which refuses the 17 observation tables. An
# episode generator reading its own observations would make every downstream agreement
# circular.
# - Hazard: V1's Weibull (k 1.8, lambda 0.9 x life) at the condition-equivalent age, times
#   leak_propensity and criticality. It uses V1's equipment_condition_index, now in
#   01_topology_config beside mtbf_days so the maintenance generator shares it. The hazard
#   uses leak_propensity (leaking), never STATE_DUTY_FACTOR (stopping).
# - State: no onset during Maintenance or Down; such an onset is dropped, not moved. An
#   episode under way when its asset stops is truncated there and does not resume. Standby,
#   Startup and Shutdown still emit.
# - Root cause is weighted by equipment type, limited to causes whose 02b signature touches a
#   tag that type has. Valve and Pipeline Segment carry no tags, so their episodes can never
#   be corroborated by telemetry.
# - Rates: a single log-normal cannot give a narrow above-floor subset at a 20 t/h median, so
#   rates are a two-component log-normal mixture. The body is median 25 kg/h, sigma 1.3.
#   Super-emitters are median 20 t/h, sigma 0.7, with a root-cause-dependent probability.
# - Source position is the asset's dim_area position plus ~55 m noise. Assets carry no
#   coordinates, by 01b's design.
# - Idempotency: a day's onsets are a pure function of (asset, day, seed). The 02a-style
#   widened replaceWhere fits, but carrying rows does not apply: every run regenerates onset
#   days back through a 30-day lookback (the duration cap), so an earlier episode's
#   truncation catches up with state written after it. Incremental mode follows
#   fact_asset_state's last day, not a high-water mark of its own; with a few dozen
#   episodes a day, a mark could land on an empty day and stop advancing.
# - harness_episodes.py runs the notebook's model cell verbatim. It checks the static
#   guards, the root causes against 02b and TAG_TEMPLATES, the invariants, two identical
#   runs, and a backfill bitwise equal to 89 incremental days. Its negative control:
#   without the lookback, 10 episode ends differ.
#
# #### Changed: 03a's super-emitter frequency
# The first Fabric run passed every validation. The above-floor median was 18.4 t/h and the
# top 5% held 61.8% of mass. But only 2 episodes cleared 3 t/h in 90 days, and none was
# active during 2026-08-16..09-15: nothing for TROPOMI to have seen.
# - Target: 50-100 above-floor episodes active in the window. 42 of 49 plumes attribute to
#   the estate, but at a median 21 km inside a 50 km search -- attribution geometry, not
#   origin. The estate is perhaps 5-15% of the basin's super-emitter-capable
#   infrastructure, so the target is 10-20 estate-origin detections. Detection per
#   above-floor episode is ~0.2: 14 scene days in 31, one overpass a day, and 45 of 47
#   emission sites detected once, which bounds per-overpass detection at ~0.15-0.3.
# - Super-emitter probabilities are raised 35-70x: unlit flare 0.70, tank flashing 0.42,
#   compressor blowby 0.24, the rest 0.035-0.10. ACTIVE_IN_WINDOW_TARGET records the target,
#   and the calibration report prints the count against it.
# - Harness, synthetic estate: 75 active in the window, on all 31 days (was 0). Above-floor
#   median 19.4 t/h (model 20.0). Top 5% by mass 86.5%. 1,533 episodes, unchanged, because
#   the probability only decides which episodes are super-emitters.
#
# #### Flagged, not changed
# - About 14% of episodes are now above 3 t/h. That is far from rare and far above any real
#   leak population. It is the price of an estate that has to account for a share of a
#   basin's detections. The alternative, many more small episodes, would leave 02b's
#   overlay on almost constantly.
# - The model's upper tail is heavier than observed: the typical max of 49 above-floor draws
#   is ~90 t/h against 71.8.
# - Plume-episode agreement will be coincidental. Plumes are real observations and episodes
#   are synthetic, independent of them by construction.
# - 02e and 03a disagree on whether an asset in Down is leaking. 03a suppresses episodes
#   during Down and Maintenance. 02e suppresses its leak term during Maintenance only
#   (CH4_SUPPRESSED_STATES = ("Maintenance",)) and gives Down a multiplier of 1.0. 03a is
#   right: a stopped asset is not venting process gas. Not fixed now, because it means
#   regenerating sensor_telemetry and re-running 02d. Until then a CH4 detector can show an
#   exceedance while its asset is Down, with no episode to explain it.
# - The 0.80 dominance threshold is provisional until 04's distribution printout shows a clean
#   gap between stripes and plumes.
# - The unanimity rule's collinearity test should have rejected PL-059118a52421 by itself: a
#   synthetic 14 + 1 stripe scores 0.988 against the 0.98 limit. Its variance_explained in
#   gold_rejected_collinear will show why it did not.
# - 02d's alarm-band diagnosis still says the episode generator "does not exist yet".
# - 07c's header still quotes the 75-plume catalogue.
#
# #### Remaining
# - [ ] Run 03, then 04 twice. Check the dominance distribution printout and the
#       single-column rejection counts. Look up PL-059118a52421's variance_explained in
#       gold_rejected_collinear.
# - [ ] 03a backfill with the new super-emitter probabilities. Confirm the in-window
#       active count lands in 50-100, then run 02b so the episode overlay switches on,
#       then 02c and 02d.
# - [ ] 02e: suppress the leak term during Down to match 03a, then regenerate
#       sensor_telemetry and re-run 02d.
# - [ ] Refresh 07c's header and 02d's "does not exist yet" note.

# MARKDOWN ********************

# ### 2026-09-29 -- Day 14 (In Progress)
#
# The instrumentation cap raised to 12; the first backlog generator, 03b work orders; and 03c
# compliance events, where most of the work was correcting the regulatory framing V1 carried.
# Every figure below is from the offline harnesses or a whole-notebook run against a pandas
# Spark stub. Nothing here has run in Fabric yet.
#
# #### Changed: MAX_INSTRUMENTED_ASSETS_PER_FACILITY 6 -> 12 (01_topology_config)
# - 662 -> 861 instrumented assets (21.1% -> 27.5%), 3,965 -> 5,042 tags. The target was
#   1,100-1,300; it cannot be reached, because only 898 assets are both instrumentable and
#   Critical/High. The criticality bar is now the limiting factor: 130 of 150 facilities
#   hold fewer than 12 eligible assets, and only 14 are still cut by the cap (37 assets).
# - Projected 30-day raw telemetry 21.8M rows (73% of 01d's 30M guard); 02b's steady-state
#   projection 21.5M against its 25M cap. Neither guard changed.
#
# #### Added: 03b_gen_work_orders
# fact_work_order and fact_work_order_event. Four observable sources: alarms, sustained CH4
# exceedance runs, offline tags, and compliance violations. fact_emission_episode is refused
# at read time and checked statically in the harness.
# - Arrival rate measured before choosing anything. 1,043 P1 + 3,099 P2 alarms is 138
#   candidates a day. One open ticket per asset brings it to ~53 a day, still far above the
#   10-20 target, and P2 alone accounts for ~39 of them. P2 is therefore gated:
#   ALARM_P2_MODE = "gated", so a P2 alarm raises a ticket only on its 3rd raise in 24 h on
#   the tag, or once it has stood for 4 h. The offline stream then gives 18 a day and ~60 open.
#   "direct" stays switchable, with both modes' measured figures in the comment. Fabric
#   reports 30.6 a day, because the offline stream lacks 02b's episode-driven P1 trips.
# - Two passes (design note §2.3). Resolution, response, stall and cost are drawn once from
#   get_rng("work_order", work_order_id). Pass 1 reads open tickets on timestamps, not on
#   status != 'Closed': after a rerun the stored rows are as of a later horizon, and the
#   status filter loses the rerun day's closures. The harness carries it as a negative
#   control, and it loses 42 transitions.
# - Stalled exit: WO_CANCEL_AFTER_DAYS = 60 -> Cancelled, with a note. Without it the
#   stalled pool grows without bound (~2.7 a day), which is the V1 defect.
# - The dashboard KPI splits on SLA, which an operator can observe, not on is_stalled, which
#   is fixed at creation and invisible. Active open = within SLA; stalled backlog = past
#   SLA. The expected "backlog > stalled count" did NOT hold: the backlog counts ticket-time
#   past SLA, and a normal breach overshoots by hours. Offline: backlog 22 against 24
#   stalled open.
# - Bugs the whole-notebook stub found, all of which would have hit Fabric: 63-bit keys
#   upcast to float64 by an empty part in a concat, and by DataFrame(dicts) on a nullable
#   column; Spark's toPandas() turning nullable long tag_sk into float64, which would have
#   corrupted every carried row; numpy scalars reaching createDataFrame, which PySpark's
#   verifier rejects. Also a design fault: a stalled ticket absorbing every later alarm on
#   its asset, which made the open count drift down 30% over 60 days.
#
# #### Added: 03c_gen_compliance
# dim_regulation (4 rows) and fact_compliance_event. Status progresses Reported -> Under
# Review -> Closed | Violation, drawn once from get_rng("compliance", compliance_id), with a
# 120-day Cancelled exit for stalled cases. fact_emission_episode is refused.
#
# **Regulatory corrections.** Checked against public sources on 2026-09-29. These three are
# the ones most likely to be repeated as fact by someone reading the tables:
#
# 1. **OLRE is a GHGRP Subpart W reporting requirement, not an NSPS OOOOb provision.** The
#    2024 Subpart W revisions (40 CFR Part 98) require "other large release events" at an
#    instantaneous methane rate of 100 kg/h or more to be REPORTED. EPA dropped the proposed
#    250 tCO2e per-event alternative. An OLRE is a report, never a fine in itself. V1 called
#    the 100 kg/h threshold "NSPS OOOOb OLRE" and treated every crossing as a federal
#    violation. EPA proposed (Sept 2025) suspending Subpart W to RY2034; as of the
#    February 2026 deadline-extension rule that was not final.
# 2. **The NSPS OOOOb super-emitter programme is postponed to 2027-01-22, and could not apply
#    to these plumes anyway.** It is the OOOOb provision that does use 100 kg/h (40 CFR
#    60.5371b: investigate within 5 days, report within 15). An interim final rule (effective
#    2025-07-31, finalised 2025-11-26) postponed it to 2027-01-22, so it is not in force in
#    this window. It also ties a notification to a well site or compressor station within
#    50 m of the reported location. A TROPOMI pixel is ~5.5 km, and 05 attributes at a
#    median of 21 km, so no TROPOMI detection can meet that test. dim_regulation carries it
#    with is_active = false, and no event is written against it.
# 3. **The $5,000-$75,000 fine range carried from V1 has no source, and is labelled
#    synthetic.** V1's FINE_SCENARIOS comment said "sources cited inline", but no source
#    appears anywhere in V1. No assessed-penalty figure was verified for either programme.
#    Statutory maxima (CAA civil penalties under 40 CFR Part 19; Tex. Nat. Res. Code
#    81.0531) are ceilings, not typical penalties, and deliberately are not used.
#    dim_regulation's fine_basis column says so on every row.
#
# Also confirmed: the Waste Emissions Charge rule was disapproved under the CRA (2025), and
# the One Big Beautiful Bill Act (2025-07-04) delayed the statutory charge to 2034. So the
# default scenario is reporting_only, with no federal per-tonne fee, and wec_hypothetical and
# social_cost stay switchable but off. Not verified, and not relied on: V1's statement that
# EPA "revoked" the rule in May 2025. Texas RRC Statewide Rule 32 (16 TAC 3.32) is real, but
# no kg/h or duration trigger in it was verified, so every state threshold is labelled a
# modelling choice. dim_regulation records, per row, whether each threshold is a rule or a
# modelling choice, with a status note.
#
# **Design.**
# - Every attributed plume >= 100 kg/h is a Subpart W report. TROPOMI's floor is ~3 t/h, so
#   every detection crosses any rule threshold. A rate cut inside the detected range would
#   sit inside the IME uncertainty (p95/p5 ~4.8x). The notebook prints the distribution and
#   the breach share at 0.1 / 1 / 3 / 10 / 25 / 50 t/h first.
# - A plume escalates to a Texas RRC case only on repetition or on-site persistence.
#   Repetition means an earlier detection within 5 km of the same location (06's radius),
#   at the same facility, in the trailing 30 days. It is judged on location, not facility:
#   nearest-facility attribution put 38 plumes on 25 facilities, so a same-facility rule would
#   have escalated geometry. Persistence means the facility's detectors had been in a
#   continuous run for 24 h at the moment of detection.
# - A flare alarm standing 24 h also opens a case: stack temperature LoLo is Venting, flow
#   HiHi is Flaring. Facility runs of 24 h or more are operator-programme events, never
#   violations.
# - "Violation" means a notice of violation, 7-35 days after the report. The formal penalty
#   process takes months and would leave nothing decided inside a 30-day retention.
# - Unattributed plumes raise nothing, because there is no operator to cite. They are counted.
# - Offline, on the real facility geometry: 69 events (38 OLRE, 29 Exceedance, 2 Venting),
#   1 violation ($9,727), 11 unattributed plumes skipped, 0 repetition escalations. That is
#   consistent with 45 of 47 gold_emission_sites being single-detection.
#
# **Wired into 03b.** Only a Violation raises a ticket, at the finding: P1 for Critical, P2
# for Major. A plume-derived case names no asset, so its ticket goes to the facility's
# highest-leak-propensity asset, labelled a modelling choice. The effect on 03b's arrival
# rate is ~+0.03-0.2 a day. The pipeline order is 03c before 03b.
#
# #### Harness
# - harness_work_orders.py: backfill = 29 incremental days, bitwise; rerun idempotent;
#   negative controls for the status-based pass-1 read and for a re-drawn plan. Over 120
#   days the stalled pool levels off after the 60-day exit, and the compliance mapping is
#   checked.
# - harness_compliance.py: the same determinism checks, repeated with a 10-day exit. A
#   negative control shows a repetition rule with hindsight breaks backfill = incremental.
#   Over 300 days, open cases sit in the lambda x T band, and the stalled pool stays bounded
#   at stall rate x 120 (12 at most, against 20 without the exit).
#
# #### Flagged, not changed: 02d cannot raise a pilot-out alarm
# - 02d raises a low alarm only when the value is strictly below the limit
#   (`breach_dn = F.col("value_num") < F.col("limit_value")`). pilot_flame's alarm_lolo is 0.0,
#   and a pilot-out reading is exactly 0, which is not below 0. So LoLo never fires on a flare
#   pilot, however long it stays out.
# - This is the unlit-flare signature, the case 01d's combustion tags exist for: pilot_flame,
#   stack_temperature, air_fuel_ratio, so the enterprise layer can corroborate 04b's
#   Incomplete Combustion class. Its most direct indicator is undetectable. 02b's "Unlit
#   flare" signature does drive the pilot to 0; 02d just cannot see it.
# - 03c works around it for now. It detects an unlit flare as Venting from a standing
#   stack_temperature LoLo, the indirect indicator, not from the pilot.
# - Fix when 02d is next touched: `<=` rather than `<` for boolean-valued tags
#   (measurement_type pilot_flame), then re-run 02d. Expect new LoLo alarms on flares during
#   unlit-flare episodes. 03c can then add the pilot alarm as a Venting source, and 03b will
#   see new P1 tickets.
#
# #### Flagged, not changed
# - No dim_team exists. 03b's assigned_team_sk is checked against a 16-team roster defined in
#   the notebook.
# - Work-order and compliance costs are modelling choices. The mean work order is ~$2.5k.
# - When the sources' 30-day retention rolls, older tickets and cases can no longer be traced
#   to their sources. Validation counts them rather than failing.
# - 00_config's CONFIG start_date / end_date (2026-06-10..07-09) do not match the real window.
#   03b and 03c take their window from the source tables, not from CONFIG.
#
# #### Remaining
# - [ ] Run 03c then 03b in Fabric. Check 03c's rate-distribution printout, the escalation
#       counts (repetition / persistence), and 03b's arrival rate with compliance wired.
# - [ ] 02d: `<=` for boolean-valued tags so pilot-out raises LoLo; re-run 02d, then 03c, 03b.
# - [ ] Re-verify the regulatory status notes in dim_regulation before any customer demo;
#       they are dated 2026-09-29.
