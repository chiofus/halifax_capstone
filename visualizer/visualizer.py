
from rdflib import Graph
import geopandas as gpd
from shapely import wkt
import folium
import pandas as pd
import os

def style_function(feature, color_map: dict):
    zone_type = feature["properties"]["type"]
    return {
        "fillColor": color_map.get(zone_type, "gray"),
        "color": "black",
        "weight": 1,
        "fillOpacity": 0.6
    }

def map_polygons(polygons: dict, data: list[dict], headers: list[str], filename: str, save_to: str = """visualizer/visualized""", pl_obj_name: str = 'pl') -> str: #returns loc where map was saved
    #Note that this fn expects a clean polygons dict, already referenced to the polygon obj itself.
    
    #creating save location
    os.makedirs(save_to, exist_ok=True)

    # geoms = [wkt.loads(str(r[f"{pl_obj_name}"])) for r in polygons]
    geoms = []

    #cleaning polygon objects
    for r in polygons:
        geoms.append(wkt.loads(r['value'].split("^^")[0].strip('"')))

    gdf = gpd.GeoDataFrame(rows=data, geometry=geoms, crs="EPSG:4326")

    # Create map
    m = folium.Map(location=[44.70, -63.60], zoom_start=11)

    #popups for parcels
    popup_settings = folium.GeoJsonPopup(
        fields=headers,
        aliases=headers,
        localize=True,
        labels=True
    )

    folium.GeoJson(
        gdf,
        style_function=lambda x: {
            "fillColor": "blue",
            "color": "black",
            "weight": 1,
            "fillOpacity": 0.5
        }
    ).add_to(m)

    # Save map
    saved_to: str = f"{save_to}/{filename}.html"
    m.save(saved_to)

    return saved_to

    # /** This will only be used when we do the use zone implementation

    # zones = []
    # types = []

    # for r in polygons:
    #     # zones.append(str(r.zone))
    #     # types.append(str(r.type))
    #     geoms.append(wkt.loads(str(r.polygon)))

    # Create dataframe
    # df = pd.DataFrame({
    #     "zone": zones,
    #     "type": types
    # })

    # gdf = gpd.GeoDataFrame(df, geometry=geoms, crs="EPSG:4326")

    # Color map for zone types
    # color_map = {
    #     "Urban Reserve": "blue",
    #     "Community Facility": "green",
    #     "Provincial Park": "darkgreen",
    #     "Local Business": "orange"
    # }

    # folium.GeoJson(
    #     gdf,
    #     style_function=style_function,
    #     tooltip=folium.GeoJsonTooltip(
    #         fields=["zone", "type"],
    #         aliases=["Zone:", "Type:"]
    #     )
    # ).add_to(m)

    # **\