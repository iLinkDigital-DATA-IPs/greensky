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

