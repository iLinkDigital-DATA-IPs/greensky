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

# MARKDOWN ********************

# ### Load config and plume catalog:

# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load the gold plume catalog:

# CELL ********************

import pandas as pd
import numpy as np

plumes = spark.table("gold_plume_catalog").toPandas()
print(f"Loaded {len(plumes)} plumes from gold_plume_catalog")
print(f"Columns: {list(plumes.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Pull facility data from EPA Envirofacts:

# CELL ********************

import requests

# EPA Envirofacts API -- get O&G facilities in Texas and New Mexico
# GHGRP subpart W (petroleum and natural gas systems)

facilities_all = []

# Query Texas O&G facilities
states = ["TX", "NM"]

for state in states:
    url = (
        f"https://data.epa.gov/efservice/"
        f"V_GHG_EMITTER_FACILITIES/"
        f"STATE_ABBR/{state}/"
        f"REPORTED_INDUSTRY_TYPES/LIKE/%25Petroleum%25/"
        f"JSON"
    )

    try:
        r = requests.get(url, timeout=120)
        print(f"{state}: status {r.status_code}, ", end="")
        if r.status_code == 200:
            data = r.json()
            print(f"{len(data)} facilities")
            facilities_all.extend(data)
        else:
            print(f"failed")
    except Exception as e:
        print(f"error: {e}")

if facilities_all:
    fac_pdf = pd.DataFrame(facilities_all)
    print(f"\nTotal facilities loaded: {len(fac_pdf)}")
    print(f"Columns: {list(fac_pdf.columns)}")
else:
    print("EPA API returned no data. We'll create a fallback.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Clean and filter facilities to Permian Basin:

# CELL ********************

# Check if EPA data was loaded successfully
try:
    epa_loaded = len(fac_pdf) > 0
except NameError:
    epa_loaded = False
    fac_pdf = pd.DataFrame()

permian_fac = pd.DataFrame()

if epa_loaded:
    # Identify lat/lon columns (EPA naming varies)
    lat_col = None
    lon_col = None
    name_col = None
    id_col = None

    for c in fac_pdf.columns:
        cl = c.lower()
        if "latitude" in cl and lat_col is None:
            lat_col = c
        elif "longitude" in cl and lon_col is None:
            lon_col = c
        elif "facility_name" in cl and name_col is None:
            name_col = c
        elif "facility_id" in cl and id_col is None:
            id_col = c

    print(f"Lat column: {lat_col}")
    print(f"Lon column: {lon_col}")
    print(f"Name column: {name_col}")
    print(f"ID column: {id_col}")

    # Convert lat/lon to numeric
    fac_pdf[lat_col] = pd.to_numeric(fac_pdf[lat_col], errors="coerce")
    fac_pdf[lon_col] = pd.to_numeric(fac_pdf[lon_col], errors="coerce")

    # Drop rows without coordinates
    fac_pdf = fac_pdf.dropna(subset=[lat_col, lon_col])

    # Filter to Permian Basin BBOX with some padding (30 km ~ 0.3 degrees)
    pad = 0.3
    permian_fac = fac_pdf[
        (fac_pdf[lat_col] >= BBOX["min_lat"] - pad) &
        (fac_pdf[lat_col] <= BBOX["max_lat"] + pad) &
        (fac_pdf[lon_col] >= BBOX["min_lon"] - pad) &
        (fac_pdf[lon_col] <= BBOX["max_lon"] + pad)
    ].copy()

    print(f"\nFacilities in Permian Basin (with padding): {len(permian_fac)}")

    if len(permian_fac) > 0:
        permian_fac = permian_fac.rename(columns={
            lat_col: "facility_lat",
            lon_col: "facility_lon",
        })
        if name_col:
            permian_fac = permian_fac.rename(columns={name_col: "facility_name"})
        if id_col:
            permian_fac = permian_fac.rename(columns={id_col: "facility_id"})

        print(permian_fac[["facility_id", "facility_name", "facility_lat", "facility_lon"]].head(10).to_string())
    else:
        print("No EPA facilities found in Permian Basin BBOX")
else:
    print("EPA data not available (API returned 403 or no data)")
    print("Proceeding to fallback in next cell")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### If EPA API fails, create reference facilities from detected plumes:

# CELL ********************

if len(permian_fac) == 0:
    print("No EPA facilities available. Generating grid reference points.")

    ref_points = []
    counter = 0
    for lat in np.arange(BBOX["min_lat"] + 0.25, BBOX["max_lat"], 0.5):
        for lon in np.arange(BBOX["min_lon"] + 0.25, BBOX["max_lon"], 0.5):
            counter += 1
            ref_points.append({
                "facility_id": f"PB_{counter:03d}",
                "facility_name": f"Permian Grid {lat:.1f}N {abs(lon):.1f}W",
                "facility_lat": round(lat, 2),
                "facility_lon": round(lon, 2),
            })

    permian_fac = pd.DataFrame(ref_points)
    print(f"Created {len(permian_fac)} grid reference points")
    print("NOTE: Grid references, not actual facility locations")
    print("Replace with real EPA data when network access is available")

print(f"\nFinal facility count for attribution: {len(permian_fac)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write facility reference table:

# CELL ********************

# Save to reference table
fac_spark = spark.createDataFrame(permian_fac)
fac_spark.write.format("delta").mode("overwrite").saveAsTable("ref_facilities")
print(f"Written {len(permian_fac)} facilities to ref_facilities")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Wind-aware probabilistic attribution:

# CELL ********************

def haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance in km."""
    R = 6371.0
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = (np.sin(dlat/2)**2 +
         np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2)
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def compute_bearing(lat1, lon1, lat2, lon2):
    """Compute bearing from point 1 to point 2 in degrees (0-360)."""
    lat1_r, lat2_r = np.radians(lat1), np.radians(lat2)
    dlon_r = np.radians(lon2 - lon1)
    x = np.sin(dlon_r) * np.cos(lat2_r)
    y = (np.cos(lat1_r) * np.sin(lat2_r) -
         np.sin(lat1_r) * np.cos(lat2_r) * np.cos(dlon_r))
    bearing = np.degrees(np.arctan2(x, y))
    return bearing % 360

def angular_difference(a1, a2):
    """Smallest angular difference between two angles in degrees."""
    diff = abs(a1 - a2) % 360
    if diff > 180:
        diff = 360 - diff
    return diff

# Attribution parameters
search_radius_km = CONFIG["attribution_search_radius_km"]  # 30 km
wind_sigma_deg = CONFIG["attribution_wind_sigma_deg"]  # 30 degrees

fac_lats = permian_fac["facility_lat"].values
fac_lons = permian_fac["facility_lon"].values
fac_ids = permian_fac["facility_id"].values
fac_names = permian_fac["facility_name"].values

attribution_results = []

for _, plume in plumes.iterrows():
    plume_lat = plume["source_lat"]
    plume_lon = plume["source_lon"]
    plume_id = plume["plume_id"]

    # Get wind direction at plume (direction wind is blowing TO)
    # ERA5 u10/v10: u=eastward, v=northward
    # Wind blows FROM upwind direction
    if "U_eff_ms" in plume and plume["U_eff_ms"] > 0:
        # Wind direction the plume came from (upwind)
        # atan2(u, v) gives the direction wind is blowing TO
        # We need where it came FROM (add 180)
        wind_to_deg = np.degrees(np.arctan2(
            plume.get("era5_u10", 0) if "era5_u10" in plume.index else 0,
            plume.get("era5_v10", 0) if "era5_v10" in plume.index else 0
        )) % 360
        upwind_dir = (wind_to_deg + 180) % 360
    else:
        upwind_dir = None

    # Score each facility
    candidates = []
    for i in range(len(fac_lats)):
        dist = haversine_km(fac_lats[i], fac_lons[i], plume_lat, plume_lon)

        if dist > search_radius_km:
            continue

        # Distance weight (Gaussian, sigma = 15 km)
        w_dist = np.exp(-dist**2 / (2.0 * 15.0**2))

        # Wind-direction weight
        if upwind_dir is not None:
            # Bearing from facility to plume
            bearing = compute_bearing(fac_lats[i], fac_lons[i], plume_lat, plume_lon)
            # Is the facility upwind of the plume?
            theta = angular_difference(bearing, upwind_dir)
            w_wind = np.exp(-theta**2 / (2.0 * wind_sigma_deg**2))
        else:
            w_wind = 1.0  # no wind info, equal weight

        score = w_dist * w_wind

        candidates.append({
            "facility_id": fac_ids[i],
            "facility_name": fac_names[i],
            "facility_lat": fac_lats[i],
            "facility_lon": fac_lons[i],
            "distance_km": dist,
            "bearing_deg": bearing if upwind_dir is not None else None,
            "upwind_dir_deg": upwind_dir,
            "angular_offset_deg": theta if upwind_dir is not None else None,
            "w_dist": w_dist,
            "w_wind": w_wind,
            "score": score,
        })

    if candidates:
        # Normalize scores to probabilities
        total_score = sum(c["score"] for c in candidates)
        for c in candidates:
            c["probability"] = c["score"] / total_score if total_score > 0 else 0

        # Sort by score descending
        candidates.sort(key=lambda x: x["score"], reverse=True)

        # Top attribution
        top = candidates[0]
        attribution_results.append({
            "plume_id": plume_id,
            "attributed_facility_id": top["facility_id"],
            "attributed_facility_name": top["facility_name"],
            "attributed_facility_lat": top["facility_lat"],
            "attributed_facility_lon": top["facility_lon"],
            "attribution_probability": top["probability"],
            "attribution_distance_km": top["distance_km"],
            "attribution_angular_offset_deg": top["angular_offset_deg"],
            "attribution_method": "wind_aware",
            "facilities_in_range": len(candidates),
            "second_facility_id": candidates[1]["facility_id"] if len(candidates) > 1 else None,
            "second_facility_probability": candidates[1]["probability"] if len(candidates) > 1 else None,
        })
    else:
        attribution_results.append({
            "plume_id": plume_id,
            "attributed_facility_id": None,
            "attributed_facility_name": "NO_FACILITY_IN_RANGE",
            "attributed_facility_lat": None,
            "attributed_facility_lon": None,
            "attribution_probability": 0.0,
            "attribution_distance_km": None,
            "attribution_angular_offset_deg": None,
            "attribution_method": "no_match",
            "facilities_in_range": 0,
            "second_facility_id": None,
            "second_facility_probability": None,
        })

attribution_df = pd.DataFrame(attribution_results)
print(f"Attribution complete for {len(attribution_df)} plumes")
print(f"Plumes with facility match: {(attribution_df['facilities_in_range'] > 0).sum()}")
print(f"Plumes with no match: {(attribution_df['facilities_in_range'] == 0).sum()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Attribution quality summary:

# CELL ********************

matched = attribution_df[attribution_df["facilities_in_range"] > 0]

if len(matched) > 0:
    print("=== Attribution Summary ===")
    print(f"Plumes attributed: {len(matched)} / {len(attribution_df)}")
    print()
    print("--- Attribution Probability ---")
    print(f"  Min:    {matched['attribution_probability'].min():.3f}")
    print(f"  Median: {matched['attribution_probability'].median():.3f}")
    print(f"  Mean:   {matched['attribution_probability'].mean():.3f}")
    print(f"  Max:    {matched['attribution_probability'].max():.3f}")
    print()
    print("--- Distance to Attributed Facility ---")
    print(f"  Min:    {matched['attribution_distance_km'].min():.1f} km")
    print(f"  Median: {matched['attribution_distance_km'].median():.1f} km")
    print(f"  Max:    {matched['attribution_distance_km'].max():.1f} km")
    print()
    print("--- Facilities in Range per Plume ---")
    print(matched["facilities_in_range"].describe().to_string())
    print()
    print("--- Top Attributed Facilities ---")
    top_fac = matched.groupby("attributed_facility_name").agg(
        plume_count=("plume_id", "count"),
        avg_probability=("attribution_probability", "mean"),
        avg_distance_km=("attribution_distance_km", "mean"),
    ).sort_values("plume_count", ascending=False)
    print(top_fac.head(15).to_string())
else:
    print("No plumes attributed to any facility")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Merge attribution into plume catalog and update Gold table:

# CELL ********************

# Drop any existing attribution columns before merging new ones
attribution_cols = [c for c in plumes.columns if c.startswith("attributed_") 
                    or c.startswith("attribution_") 
                    or c in ["facilities_in_range", "second_facility_id", "second_facility_probability"]]

if attribution_cols:
    print(f"Dropping {len(attribution_cols)} old attribution columns")
    plumes_clean = plumes.drop(columns=attribution_cols)
else:
    plumes_clean = plumes

# Merge fresh attribution
plumes_attributed = plumes_clean.merge(attribution_df, on="plume_id", how="left")

# Verify no duplicate columns
dupes = [c for c in plumes_attributed.columns if c.endswith("_x") or c.endswith("_y")]
if dupes:
    print(f"WARNING: duplicate columns found: {dupes}")
else:
    print("No duplicate columns")

# Write updated gold table
plumes_spark = spark.createDataFrame(plumes_attributed)
plumes_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_plume_catalog")

print(f"Updated gold_plume_catalog with attribution ({len(plumes_attributed)} plumes)")
print(f"Columns ({len(plumes_attributed.columns)}): {list(plumes_attributed.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
