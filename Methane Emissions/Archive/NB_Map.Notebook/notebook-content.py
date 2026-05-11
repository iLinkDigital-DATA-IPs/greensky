# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e0efee7f-4d05-4685-b178-768ed7635e44",
# META       "default_lakehouse_name": "GreenSky_LH",
# META       "default_lakehouse_workspace_id": "060ba34b-f1a3-4509-a6e2-36d1e736a8eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "e0efee7f-4d05-4685-b178-768ed7635e44"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip install folium



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


%pip install folium geopandas shapely

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **rectangle**

# CELL ********************

# ----------------------------------------
# 1️⃣ Imports
# ----------------------------------------
import folium
import pandas as pd
import numpy as np
from shapely.geometry import Polygon, mapping
from folium.plugins import MarkerCluster, HeatMap
from IPython.display import display

# ----------------------------------------
# 2️⃣ Load data from Lakehouse
# ----------------------------------------
df_plumes = spark.read.table("silver.silver_carbonmapper").toPandas()
df_facilities = spark.read.table("bronze.bronze_facilities").toPandas()

# ----------------------------------------
# 3️⃣ Build GeoJSON for all plumes
# ----------------------------------------
features = []
for _, row in df_plumes.iterrows():
    polygon = Polygon([
        (row['plume_bounds_min_lon'], row['plume_bounds_min_lat']),
        (row['plume_bounds_max_lon'], row['plume_bounds_min_lat']),
        (row['plume_bounds_max_lon'], row['plume_bounds_max_lat']),
        (row['plume_bounds_min_lon'], row['plume_bounds_max_lat']),
        (row['plume_bounds_min_lon'], row['plume_bounds_min_lat'])
    ])

    plume_id = row.get("plume_id", "N/A")
    gas = row.get("gas", "N/A")
    emission_auto = row.get("emission_auto", None)
    wind_speed = row.get("wind_speed", "N/A")
    instrument = row.get("instrument", "N/A")

    features.append({
        "type": "Feature",
        "geometry": mapping(polygon),
        "properties": {
            "plume_id": plume_id,
            "gas": gas,
            "emission_auto": emission_auto,
            "wind_speed": wind_speed,
            "instrument": instrument
        }
    })

geojson_data = {"type": "FeatureCollection", "features": features}

# ----------------------------------------
# 4️⃣ Create Base Map (centered automatically)
# ----------------------------------------
df_plumes["center_lat"] = (df_plumes["plume_bounds_min_lat"] + df_plumes["plume_bounds_max_lat"]) / 2
df_plumes["center_lon"] = (df_plumes["plume_bounds_min_lon"] + df_plumes["plume_bounds_max_lon"]) / 2

center_lat = df_plumes["center_lat"].mean()
center_lon = df_plumes["center_lon"].mean()

m = folium.Map(location=[center_lat, center_lon], zoom_start=8, tiles=None)
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attr="Esri World Imagery © Esri & Contributors",
    name="Satellite"
).add_to(m)

# ----------------------------------------
# 5️⃣ Add Plumes via GeoJSON
# ----------------------------------------
plume_layer = folium.FeatureGroup(name="Plumes")
folium.GeoJson(
    geojson_data,
    name="Plumes",
    style_function=lambda feature: {
        "color": "orange",
        "weight": 1,
        "fillColor": "orange",
        "fillOpacity": 0.3
    },
    tooltip=folium.GeoJsonTooltip(
        fields=["plume_id", "gas", "emission_auto", "wind_speed", "instrument"],
        aliases=["Plume ID:", "Gas:", "Emission Auto:", "Wind Speed:", "Instrument:"],
        sticky=True
    )
).add_to(plume_layer)
plume_layer.add_to(m)

# ----------------------------------------
# 6️⃣ Add Facilities (clustered with tooltip)
# ----------------------------------------
facility_layer = folium.FeatureGroup(name="Facilities")
cluster = MarkerCluster(name="Facility Cluster").add_to(facility_layer)

for _, row in df_facilities.iterrows():
    facility_name = row.get("facility_name", "N/A")
    facility_type = row.get("facility_type", "N/A")
    latitude = row.get("latitude")
    longitude = row.get("longitude")

    if pd.notna(latitude) and pd.notna(longitude):
        tooltip_text = f"""
        <b>Facility Name:</b> {facility_name}<br>
        <b>Facility Type:</b> {facility_type}
        """
        folium.CircleMarker(
            location=[latitude, longitude],
            radius=5,
            color="blue",
            fill=True,
            fill_color="blue",
            fill_opacity=0.7,
            tooltip=tooltip_text
        ).add_to(cluster)

facility_layer.add_to(m)

# ----------------------------------------
# 7️⃣ Prepare Data for HeatMap
# ----------------------------------------
df_plumes["emission_auto"] = pd.to_numeric(df_plumes["emission_auto"], errors="coerce")
df_plumes = df_plumes.dropna(subset=["center_lat", "center_lon", "emission_auto"])

# Normalize emission_auto for visualization
df_plumes["emission_scaled"] = (
    df_plumes["emission_auto"] / df_plumes["emission_auto"].max()
).clip(0, 1)
# ----------------------------------------
# 7️⃣ Add Emission Heatmap (Weighted by Emission)
# ----------------------------------------
df_plumes["emission_auto"] = pd.to_numeric(df_plumes["emission_auto"], errors="coerce")
df_plumes = df_plumes.dropna(subset=["center_lat", "center_lon", "emission_auto"])
df_plumes["emission_scaled"] = (
    df_plumes["emission_auto"] / df_plumes["emission_auto"].max()
).clip(0, 1)

heat_data = df_plumes[["center_lat", "center_lon", "emission_scaled"]].values.tolist()

heat_layer = folium.FeatureGroup(name="Emission Intensity Heatmap")
HeatMap(
    data=heat_data,
    radius=35,
    blur=25,
    min_opacity=0.4,
    gradient={0.3: "blue", 0.6: "lime", 0.9: "red"}
).add_to(heat_layer)
heat_layer.add_to(m)

# ----------------------------------------
# 9️⃣ Layer Control (Top-right Corner)
# ----------------------------------------
folium.LayerControl(position='topright', collapsed=False).add_to(m)

# ----------------------------------------
# 🔟 Display Map
# ----------------------------------------
display(m)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **irregular polygon**

# CELL ********************

# ============================================================
# 🌍 Methane Plumes + Facilities + Heatmap (Lakehouse → Folium)
# ============================================================
import folium
import pandas as pd
import numpy as np
from shapely.geometry import Polygon, mapping
from folium.plugins import MarkerCluster, HeatMap
from IPython.display import display

# ----------------------------------------
# 1️⃣ Load data from Lakehouse
# ----------------------------------------
df_plumes = spark.read.table("silver.silver_carbonmapper").toPandas()
df_facilities = spark.read.table("bronze.bronze_facilities").toPandas()

print(f"✅ Plumes loaded: {len(df_plumes)}")
print(f"✅ Facilities loaded: {len(df_facilities)}")

# ----------------------------------------
# 2️⃣ Create irregular (cloud-like) plume polygons
# ----------------------------------------
features = []
for _, row in df_plumes.iterrows():
    try:
        # Plume center
        center_lat = (row["plume_bounds_min_lat"] + row["plume_bounds_max_lat"]) / 2
        center_lon = (row["plume_bounds_min_lon"] + row["plume_bounds_max_lon"]) / 2
        
        # Generate 12 random points around the center to make it irregular
        num_points = 12
        angles = np.linspace(0, 2 * np.pi, num=num_points, endpoint=False)
        radius = (
            (row["plume_bounds_max_lat"] - row["plume_bounds_min_lat"]) / 2
        ) * (0.5 + np.random.rand(num_points))  # variable radius for “cloudy” edges
        
        coords = [
            (center_lon + r * np.cos(a), center_lat + r * np.sin(a))
            for r, a in zip(radius, angles)
        ]
        coords.append(coords[0])  # close the polygon

        polygon = Polygon(coords)

        # Properties
        features.append({
            "type": "Feature",
            "geometry": mapping(polygon),
            "properties": {
                "plume_id": row.get("plume_id", "N/A"),
                "gas": row.get("gas", "N/A"),
                "emission_auto": row.get("emission_auto", None),
                "wind_speed": row.get("wind_speed_avg_auto", "N/A"),
                "instrument": row.get("instrument", "N/A")
            }
        })
    except Exception as e:
        print(f"⚠️ Skipped row due to error: {e}")

geojson_data = {"type": "FeatureCollection", "features": features}

# ----------------------------------------
# 3️⃣ Compute map center
# ----------------------------------------
df_plumes["center_lat"] = (df_plumes["plume_bounds_min_lat"] + df_plumes["plume_bounds_max_lat"]) / 2
df_plumes["center_lon"] = (df_plumes["plume_bounds_min_lon"] + df_plumes["plume_bounds_max_lon"]) / 2

center_lat = df_plumes["center_lat"].mean()
center_lon = df_plumes["center_lon"].mean()

# ----------------------------------------
# 4️⃣ Create Folium Map with Satellite View
# ----------------------------------------
m = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles=None)
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attr="Esri World Imagery © Esri & Contributors",
    name="Satellite"
).add_to(m)

# ----------------------------------------
# 5️⃣ Add Plume Polygons
# ----------------------------------------
plume_layer = folium.FeatureGroup(name="Methane Plumes")
folium.GeoJson(
    geojson_data,
    name="Methane Plumes",
    style_function=lambda feature: {
        "color": "#ff6600",
        "weight": 1.5,
        "fillColor": "#ff9900",
        "fillOpacity": 0.35
    },
    tooltip=folium.GeoJsonTooltip(
        fields=["plume_id", "gas", "emission_auto", "wind_speed", "instrument"],
        aliases=["Plume ID:", "Gas:", "Emission (kg/hr):", "Wind Speed:", "Instrument:"],
        sticky=True
    )
).add_to(plume_layer)
plume_layer.add_to(m)

# ----------------------------------------
# 6️⃣ Add Facility Markers (Clustered)
# ----------------------------------------
facility_layer = folium.FeatureGroup(name="Facilities")
cluster = MarkerCluster(name="Facility Cluster").add_to(facility_layer)

for _, row in df_facilities.iterrows():
    if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
        tooltip_text = f"""
        <b>Facility Name:</b> {row.get('facility_name', 'N/A')}<br>
        <b>Facility Type:</b> {row.get('facility_type', 'N/A')}
        """
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=5,
            color="blue",
            fill=True,
            fill_color="blue",
            fill_opacity=0.7,
            tooltip=tooltip_text
        ).add_to(cluster)
facility_layer.add_to(m)

# ----------------------------------------
# 7️⃣ Add Emission Heatmap (Weighted by Emission)
# ----------------------------------------
df_plumes["emission_auto"] = pd.to_numeric(df_plumes["emission_auto"], errors="coerce")
df_plumes = df_plumes.dropna(subset=["center_lat", "center_lon", "emission_auto"])
df_plumes["emission_scaled"] = (
    df_plumes["emission_auto"] / df_plumes["emission_auto"].max()
).clip(0, 1)

heat_data = df_plumes[["center_lat", "center_lon", "emission_scaled"]].values.tolist()

heat_layer = folium.FeatureGroup(name="Emission Intensity Heatmap")
HeatMap(
    data=heat_data,
    radius=35,
    blur=25,
    min_opacity=0.4,
    gradient={0.3: "blue", 0.6: "lime", 0.9: "red"}
).add_to(heat_layer)
heat_layer.add_to(m)

# ----------------------------------------
# 8️⃣ Add Layer Control
# ----------------------------------------
folium.LayerControl(collapsed=False, position='topright').add_to(m)

# ----------------------------------------
# 9️⃣ Display Map
# ----------------------------------------
display(m)

# Save the full map
m.save("/files/tmp/methane_map_oct2025.html")  # Databricks path example
print("✅ Map saved! Open 'methane_map_oct2025.html' in a browser to view all plumes.")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------
# 1️⃣ Imports
# ----------------------------------------
import folium
import pandas as pd
import numpy as np
from shapely.geometry import Polygon, mapping
from folium.plugins import MarkerCluster, HeatMap
from IPython.display import display
from pyspark.sql.functions import col, to_timestamp

# ----------------------------------------
# 2️⃣ Load data from Lakehouse
# ----------------------------------------
df_plumes = (
    spark.read.table("silver.silver_carbonmapper")
    .withColumn("scene_timestamp", to_timestamp("scene_timestamp"))  # Convert string to timestamp
    .filter(
        (col("scene_timestamp") >= "2025-10-01") & (col("scene_timestamp") <= "2025-10-31")
    )
    .toPandas()  # Convert only the filtered data to Pandas
)

df_facilities = spark.read.table("bronze.bronze_facilities").toPandas()

# ----------------------------------------
# 3️⃣ Compute map center safely
# ----------------------------------------
if df_plumes.empty:
    print("⚠️ No plumes found for October 2025. Using default map center.")
    center_lat, center_lon = 0, 0  # Default to global view
    m = folium.Map(location=[center_lat, center_lon], zoom_start=2, tiles=None)
    geojson_data = {"type": "FeatureCollection", "features": []}  # empty plume layer
else:
    # Compute plume centers
    df_plumes["center_lat"] = (df_plumes["plume_bounds_min_lat"] + df_plumes["plume_bounds_max_lat"]) / 2
    df_plumes["center_lon"] = (df_plumes["plume_bounds_min_lon"] + df_plumes["plume_bounds_max_lon"]) / 2
    center_lat = df_plumes["center_lat"].mean()
    center_lon = df_plumes["center_lon"].mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=8, tiles=None)

    # ----------------------------------------
    # 4️⃣ Build GeoJSON for all plumes
    # ----------------------------------------
    features = []
    for _, row in df_plumes.iterrows():
        polygon = Polygon([
            (row['plume_bounds_min_lon'], row['plume_bounds_min_lat']),
            (row['plume_bounds_max_lon'], row['plume_bounds_min_lat']),
            (row['plume_bounds_max_lon'], row['plume_bounds_max_lat']),
            (row['plume_bounds_min_lon'], row['plume_bounds_max_lat']),
            (row['plume_bounds_min_lon'], row['plume_bounds_min_lat'])
        ])
        features.append({
            "type": "Feature",
            "geometry": mapping(polygon),
            "properties": {
                "plume_id": row.get("plume_id", "N/A"),
                "gas": row.get("gas", "N/A"),
                "emission_auto": row.get("emission_auto", None),
                "wind_speed": row.get("wind_speed", "N/A"),
                "instrument": row.get("instrument", "N/A")
            }
        })
    geojson_data = {"type": "FeatureCollection", "features": features}

# ----------------------------------------
# 5️⃣ Add base map layer
# ----------------------------------------
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attr="Esri World Imagery © Esri & Contributors",
    name="Satellite"
).add_to(m)

# ----------------------------------------
# 6️⃣ Add Plumes via GeoJSON (if any)
# ----------------------------------------
if not df_plumes.empty:
    plume_layer = folium.FeatureGroup(name="Plumes")
    folium.GeoJson(
        geojson_data,
        style_function=lambda feature: {
            "color": "orange",
            "weight": 1,
            "fillColor": "orange",
            "fillOpacity": 0.3
        },
        tooltip=folium.GeoJsonTooltip(
            fields=["plume_id", "gas", "emission_auto", "wind_speed", "instrument"],
            aliases=["Plume ID:", "Gas:", "Emission Auto:", "Wind Speed:", "Instrument:"],
            sticky=True
        )
    ).add_to(plume_layer)
    plume_layer.add_to(m)

# ----------------------------------------
# 7️⃣ Add Facilities (clustered with tooltip)
# ----------------------------------------
facility_layer = folium.FeatureGroup(name="Facilities")
cluster = MarkerCluster(name="Facility Cluster").add_to(facility_layer)

for _, row in df_facilities.iterrows():
    latitude = row.get("latitude")
    longitude = row.get("longitude")
    if pd.notna(latitude) and pd.notna(longitude):
        tooltip_text = f"""
        <b>Facility Name:</b> {row.get("facility_name", "N/A")}<br>
        <b>Facility Type:</b> {row.get("facility_type", "N/A")}
        """
        folium.CircleMarker(
            location=[latitude, longitude],
            radius=5,
            color="blue",
            fill=True,
            fill_color="blue",
            fill_opacity=0.7,
            tooltip=tooltip_text
        ).add_to(cluster)

facility_layer.add_to(m)

# ----------------------------------------
# 8️⃣ Add Emission Heatmap (if any)
# ----------------------------------------
if not df_plumes.empty:
    df_plumes["emission_auto"] = pd.to_numeric(df_plumes["emission_auto"], errors="coerce")
    df_plumes = df_plumes.dropna(subset=["center_lat", "center_lon", "emission_auto"])
    df_plumes["emission_scaled"] = (df_plumes["emission_auto"] / df_plumes["emission_auto"].max()).clip(0, 1)
    heat_data = df_plumes[["center_lat", "center_lon", "emission_scaled"]].values.tolist()

    heat_layer = folium.FeatureGroup(name="Emission Intensity Heatmap")
    HeatMap(
        data=heat_data,
        radius=35,
        blur=25,
        min_opacity=0.4,
        gradient={0.3: "blue", 0.6: "lime", 0.9: "red"}
    ).add_to(heat_layer)
    heat_layer.add_to(m)

# ----------------------------------------
# 9️⃣ Layer Control
# ----------------------------------------
folium.LayerControl(position='topright', collapsed=False).add_to(m)

# ----------------------------------------
# 🔟 Display Map
# ----------------------------------------
display(m)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------
# 1️⃣ Imports
# ----------------------------------------
import folium
import pandas as pd
import numpy as np
from shapely.geometry import Polygon, mapping
from folium.plugins import MarkerCluster, HeatMap
from IPython.display import display

# ----------------------------------------
# 2️⃣ Load data from Lakehouse
# ----------------------------------------
df_plumes = spark.read.table("silver.silver_carbonmapper").toPandas()
df_facilities = spark.read.table("bronze.bronze_facilities_new").toPandas()

# ----------------------------------------
# 3️⃣ Build GeoJSON for all plumes
# ----------------------------------------
features = []
for _, row in df_plumes.iterrows():
    polygon = Polygon([
        (row['plume_bounds_min_lon'], row['plume_bounds_min_lat']),
        (row['plume_bounds_max_lon'], row['plume_bounds_min_lat']),
        (row['plume_bounds_max_lon'], row['plume_bounds_max_lat']),
        (row['plume_bounds_min_lon'], row['plume_bounds_max_lat']),
        (row['plume_bounds_min_lon'], row['plume_bounds_min_lat'])
    ])

    plume_id = row.get("plume_id", "N/A")
    gas = row.get("gas", "N/A")
    emission_auto = row.get("emission_auto", None)
    wind_speed = row.get("wind_speed", "N/A")
    instrument = row.get("instrument", "N/A")

    features.append({
        "type": "Feature",
        "geometry": mapping(polygon),
        "properties": {
            "plume_id": plume_id,
            "gas": gas,
            "emission_auto": emission_auto,
            "wind_speed": wind_speed,
            "instrument": instrument
        }
    })

geojson_data = {"type": "FeatureCollection", "features": features}

# ----------------------------------------
# 4️⃣ Create Base Map (centered automatically)
# ----------------------------------------
df_plumes["center_lat"] = (df_plumes["plume_bounds_min_lat"] + df_plumes["plume_bounds_max_lat"]) / 2
df_plumes["center_lon"] = (df_plumes["plume_bounds_min_lon"] + df_plumes["plume_bounds_max_lon"]) / 2

center_lat = df_plumes["center_lat"].mean()
center_lon = df_plumes["center_lon"].mean()

m = folium.Map(location=[center_lat, center_lon], zoom_start=8, tiles=None)
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attr="Esri World Imagery © Esri & Contributors",
    name="Satellite"
).add_to(m)

# ----------------------------------------
# 5️⃣ Add Plumes via GeoJSON
# ----------------------------------------
plume_layer = folium.FeatureGroup(name="Plumes")
folium.GeoJson(
    geojson_data,
    name="Plumes",
    style_function=lambda feature: {
        "color": "orange",
        "weight": 1,
        "fillColor": "orange",
        "fillOpacity": 0.3
    },
    tooltip=folium.GeoJsonTooltip(
        fields=["plume_id", "gas", "emission_auto", "wind_speed", "instrument"],
        aliases=["Plume ID:", "Gas:", "Emission Auto:", "Wind Speed:", "Instrument:"],
        sticky=True
    )
).add_to(plume_layer)
plume_layer.add_to(m)

# ----------------------------------------
# 6️⃣ Add Facilities (clustered with tooltip)
# ----------------------------------------
facility_layer = folium.FeatureGroup(name="Facilities")
cluster = MarkerCluster(name="Facility Cluster").add_to(facility_layer)

for _, row in df_facilities.iterrows():
    facility_name = row.get("facility_name", "N/A")
    facility_type = row.get("facility_type", "N/A")
    latitude = row.get("latitude")
    longitude = row.get("longitude")

    if pd.notna(latitude) and pd.notna(longitude):
        tooltip_text = f"""
        <b>Facility Name:</b> {facility_name}<br>
        <b>Facility Type:</b> {facility_type}
        """
        folium.CircleMarker(
            location=[latitude, longitude],
            radius=5,
            color="blue",
            fill=True,
            fill_color="blue",
            fill_opacity=0.7,
            tooltip=tooltip_text
        ).add_to(cluster)

facility_layer.add_to(m)

# ----------------------------------------
# 7️⃣ Add Emission Heatmap (Weighted by Emission)
# ----------------------------------------
df_plumes["emission_auto"] = pd.to_numeric(df_plumes["emission_auto"], errors="coerce")
df_plumes = df_plumes.dropna(subset=["center_lat", "center_lon", "emission_auto"])
df_plumes["emission_scaled"] = (
    df_plumes["emission_auto"] / df_plumes["emission_auto"].max()
).clip(0, 1)

heat_data = df_plumes[["center_lat", "center_lon", "emission_scaled"]].values.tolist()

heat_layer = folium.FeatureGroup(name="Emission Intensity Heatmap")
HeatMap(
    data=heat_data,
    radius=35,
    blur=25,
    min_opacity=0.4,
    gradient={0.3: "blue", 0.6: "lime", 0.9: "red"}
).add_to(heat_layer)
heat_layer.add_to(m)

# ----------------------------------------
# 9️⃣ Layer Control (Top-right Corner)
# ----------------------------------------
folium.LayerControl(position='topright', collapsed=False).add_to(m)

# ----------------------------------------
# 🔟 Display Map
# ----------------------------------------
display(m)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
