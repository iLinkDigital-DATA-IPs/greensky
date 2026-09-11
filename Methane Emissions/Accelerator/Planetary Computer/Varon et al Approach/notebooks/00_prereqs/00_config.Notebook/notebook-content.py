# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5d5c8002-789e-4319-81d1-a60f08a77996",
# META       "default_lakehouse_name": "greensky_v2_lakehouse",
# META       "default_lakehouse_workspace_id": "640876ea-6158-4ffd-8598-5eb210e088a0",
# META       "known_lakehouses": [
# META         {
# META           "id": "5d5c8002-789e-4319-81d1-a60f08a77996"
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

# Cell 1 - 00_config
# All Green Sky pipeline parameters in one place
# Other notebooks run: %run 00_config

CONFIG = {
    # Geographic bounds (Permian Basin)
    "bbox": {
        "min_lat": 30.5,
        "max_lat": 33.5,
        "min_lon": -105.0,
        "max_lon": -101.0,
    },

    # Data quality
    "qa_threshold": 0.5,

    # Scene separation
    "scene_gap_minutes": 10,

    # Background estimation
    "background_neighbors": 30,
    "background_percentile": 0.15,

    # Plume detection
    "mad_sigma": 3,
    "enhancement_floor_ppb": 6,

    # Destriping (across-track detector-column bias correction)
    "destripe_enabled": True,
    "destripe_min_scanlines": 20,       # skip destriping for granules with too few rows
    "destripe_max_correction_ppb": 50,  # sanity cap; warn if exceeded

    # Plume clustering
    "cluster_radius_km": 12,
    "min_cluster_pixels": 3,
    "max_cluster_pixels": 15,
    "shape_threshold": 20,
    "collinearity_max_r2": 0.98,        # reject clusters whose pixels fit a line this well
    "collinearity_min_pixels": 4,       # only apply the test above this size

    # Wind alignment
    "wind_alignment_threshold_deg": 75,

    # Emission quantification
    "mixing_time_fallback_s": 172800,  # 48 hours
    "min_wind_speed_ms": 0.5,

    # Uncertainty
    "mc_samples": 500,
    "wind_uncertainty_fraction": 0.3,

    # Attribution
    "attribution_search_radius_km": 50,
    "attribution_wind_sigma_deg": 30,

    # Persistence
    "persistence_match_radius_km": 5,

    # Weather grid spacing for Open-Meteo queries (degrees)
    "weather_grid_spacing": 0.5,

    # Date range (update as needed)
    "start_date": "2026-06-10",
    "end_date": "2026-07-09",
}

# Convenience accessors
BBOX = CONFIG["bbox"]
print("Config loaded successfully")
print(f"BBOX: {BBOX}")
print(f"Date range: {CONFIG['start_date']} to {CONFIG['end_date']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Physical constants for the IME conversion:

# CELL ********************

# Cell 2 - 00_config
# Physical constants for the IME (Integrated Methane Enhancement) conversion.
#
# These are module-level names rather than CONFIG entries because they are physical
# constants, not tunable pipeline parameters, and because 04_derive_emissions,
# 07b_detection_diagnostics and 07c_quantification_diagnostics all consume them as
# bare names. They used to be copy-pasted into all three notebooks.

# ---------------------------------------------------------------------------------
# UNIT ERROR, FIXED 2026-09-11
#
# 2.12e25 is the dry-air column in molecules per SQUARE CENTIMETRE. It was named
# DRY_AIR_COLUMN, commented "molecules/m^2", and multiplied by PIXEL_AREA_M2, which is
# an area in SQUARE METRES. Since 1 m^2 = 1e4 cm^2, every ime_kg and every emission
# rate this pipeline produced before this fix is low by a factor of exactly 10,000.
#
# Verified two ways:
#   1. From first principles the dry-air column of a standard atmosphere is
#        (101325 Pa / 9.81 m s^-2) / 0.028964 kg mol^-1 * 6.022e23 mol^-1
#        = 2.147e29 molecules/m^2
#      and 2.12e25 / 2.147e29 = 9.87e-5 -- the literal is ~1e-4 of the per-m^2 value,
#      which is exactly the cm^2-to-m^2 ratio.
#   2. At the observed mean of 1901 ppb, reading the literal as molecules/cm^2 gives a
#      CH4 total column of 1901e-9 * 2.12e25 = 4.0e19 molecules/cm^2, against a
#      published TROPOMI value of ~3.8e19. Reading it as molecules/m^2 instead would
#      give 4.0e15 molecules/cm^2 -- four orders of magnitude too small.
#
# The numeric literal is deliberately left at 2.12e25 and the cm^2 -> m^2 conversion
# written out as an explicit step, so the mistake stays visible in the code instead of
# being silently absorbed into a new magic number.
# ---------------------------------------------------------------------------------

PIXEL_AREA_M2 = 5500.0 * 7000.0   # m^2 per TROPOMI ground pixel (5.5 km x 7.0 km
                                  # = 38.5 km^2; pre-Aug-2019 value, tracked separately)

AVOGADRO = 6.022e23               # molecules per mol
M_CH4 = 16.04e-3                  # kg per mol (methane)
M_AIR = 28.97e-3                  # kg per mol (dry air)

DRY_AIR_COLUMN_PER_CM2 = 2.12e25  # molecules per cm^2 (standard sea-level atmosphere)
DRY_AIR_COLUMN_PER_M2 = DRY_AIR_COLUMN_PER_CM2 * 1e4   # molecules per m^2; 1 m^2 = 1e4 cm^2

# Remaining approximation: the value above is a SEA-LEVEL standard atmosphere. The
# Permian Basin sits at roughly 800 m, where surface pressure is about 92 kPa, so the
# true dry-air column there is around 9% below this and every IME is correspondingly
# about 9% high. That is second-order next to the factor-of-10,000 error above, so it
# is left as an approximation for now. If a per-pixel column is wanted later,
# surface_pressure is already carried through to silver_plume_ready_pixels:
#     DRY_AIR_COLUMN_PER_M2 = (surface_pressure / 9.81) / M_AIR * AVOGADRO
# (check its units first -- Open-Meteo reports surface_pressure in hPa, not Pa).

# Conversion factor: 1 ppb enhancement over 1 TROPOMI pixel -> kg CH4
#   mass = delta_ppb * 1e-9 * (DRY_AIR_COLUMN_PER_M2 / AVOGADRO) * M_CH4 * PIXEL_AREA_M2
PPB_TO_KG = 1e-9 * (DRY_AIR_COLUMN_PER_M2 / AVOGADRO) * M_CH4 * PIXEL_AREA_M2

# Guard against the cm^2/m^2 confusion returning. For a 38.5 km^2 TROPOMI pixel the
# correct factor is ~217 kg per ppb; the broken per-cm^2 version was ~0.0217 kg.
assert 100.0 < PPB_TO_KG < 400.0, (
    f"PPB_TO_KG = {PPB_TO_KG} kg per ppb per pixel is outside the plausible range "
    "100-400 kg for a 38.5 km^2 TROPOMI pixel. A value near 0.02 means the cm^2/m^2 "
    "confusion has returned: DRY_AIR_COLUMN_PER_CM2 (molecules/cm^2) is being "
    "multiplied by PIXEL_AREA_M2 (m^2) without the 1e4 conversion."
)

print(f"PPB_TO_KG: {PPB_TO_KG:.3f} kg CH4 per ppb per pixel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
