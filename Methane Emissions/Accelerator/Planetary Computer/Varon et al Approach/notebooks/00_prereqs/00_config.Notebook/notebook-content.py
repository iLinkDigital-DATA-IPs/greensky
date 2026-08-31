# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5d5c8002-789e-4319-81d1-a60f08a77996",
# META       "default_lakehouse_name": "greensky_lakehouse",
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

    # Plume clustering
    "cluster_radius_km": 12,
    "min_cluster_pixels": 3,
    "max_cluster_pixels": 15,
    "shape_threshold": 20,

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
