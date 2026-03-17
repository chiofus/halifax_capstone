
# from rdflib import Graph
# import geopandas as gpd
# from shapely import wkt
# import folium
# import pandas as pd
# import os

def style_function(feature, color_map: dict):
    zone_type = feature["properties"]["type"]
    return {
        "fillColor": color_map.get(zone_type, "gray"),
        "color": "black",
        "weight": 1,
        "fillOpacity": 0.6
    }

def get_empty_civic_addresses(json_dump_location: str = '') -> list:
    #Imports
    from query_tools.general_query import query_endpoint, convert_default_sparql_to_df
    import polars as pl
    from shapely.geometry import Point, Polygon
    from tqdm import tqdm
    import json, pickle
    from copy import deepcopy

    #Helper functions
    def into_geo_objects_list(to_iterate: list[str]) -> list:
        from shapely import wkt

        #Cleaning data
        clean_data = [wkt_literal.split("^^")[0].strip('"') for wkt_literal in to_iterate]

        #Transforming and returning
        return [wkt.loads(wkt_str) for wkt_str in clean_data]

    #General
    PARCEL_POLYGONS_QUERY: str = """
        PREFIX hp:  <http://ontology.eil.utoronto.ca/HPCDM/>
        PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>

        SELECT ?p ?pl
        WHERE {
            ?p a hp:Parcel ;
            loc:hasLocation ?locObj .

            # Ensure no building occupies this parcel #not running this for now, but check if it would help make queries faster / return the same results.
            # FILTER NOT EXISTS {
            #     ?b a hp:Building ;
            #     hp:occupies ?p .
            # }

            # Polygon
            ?locObj geo:asWKT ?pl .
        }
    """

    CIVIC_ADDRESSES_POINTS_QUERY: str = """
        PREFIX contact: <https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/Contact/>
        PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        PREFIX code: <https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/Code/>
        PREFIX genprop: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/GenericProperties/>

        SELECT ?ad ?pnt ?codeName
        WHERE {
            ?ad a contact:Address ;
                    loc:hasLocation ?loc ;
                    code:hasCode ?code .

            ?loc geo:asWKT ?pnt .

            ?code genprop:hasName ?codeName .
        }
    """
    
    #Gets all empty civid addresses (civic address points that do not belong to any parcel polygon)

    #1. Getting all polygon parcel objects
    ALL_POLYGONS: pl.DataFrame = convert_default_sparql_to_df(
        query_endpoint(PARCEL_POLYGONS_QUERY)
    )

    #2. Getting all civic address objects
    ALL_CIVIC_ADDRESSES: pl.DataFrame = convert_default_sparql_to_df(
        query_endpoint(CIVIC_ADDRESSES_POINTS_QUERY)
    )

    #3. Finding all civic addresses NOT in a parcel object
    empty_civic_addresses: list[dict] = []
    polygons_to_check: list[Polygon]  = into_geo_objects_list([pol for pol in ALL_POLYGONS["pl"].to_list()])
    civic_addresses_to_check: list[Point] = into_geo_objects_list([point for point in ALL_CIVIC_ADDRESSES["pnt"].to_list()])

    int_counter: int = 0

    for idx_point, point in enumerate(tqdm(civic_addresses_to_check)):
        contained: bool = False
        for idx_poly, curr_pol in enumerate(polygons_to_check):
            if curr_pol.contains(point):
                contained = True
                break #break out of inner loop, no need to check checking if address is contained

        if not contained:
            empty_civic_addresses.append({'point': point, 'point_str': point.wkt, 'df_index': idx_point})
        
        #dumping out every 5k points checked
        int_counter += 1
        
        if int_counter > 5000 or idx_point == len(civic_addresses_to_check)-1: #dump out results every 5k iters
            int_counter = 0

            try:
                if json_dump_location:  #if a dump location is given, list is also dumped as json
                    #will first pickle it for later use
                    with open(json_dump_location.replace('.json', '.pickle'), 'wb') as pickel_file:
                        pickle.dump(empty_civic_addresses, pickel_file)

                    #remove non json serializable objects
                    dummy_addresses: list[dict] = deepcopy(empty_civic_addresses) #save a dummy copy to remove objs from dicts & not affect the og addresses
                    for item in dummy_addresses: item.pop('point', None)
                    with open(json_dump_location, 'w') as json_file:
                        json.dump(dummy_addresses, json_file, indent=2)
                    del dummy_addresses

            except Exception as e:
                print(e)

    return empty_civic_addresses

def map_polygons(polygons: dict, rows: list[dict], filename: str, save_to: str = """visualizer/visualized""", pl_obj_name: str = 'pl') -> str: #returns loc where map was saved
    #Note that this fn expects a clean polygons dict, already referenced to the polygon obj itself.
    
    #creating save location
    os.makedirs(save_to, exist_ok=True)

    # geoms = [wkt.loads(str(r[f"{pl_obj_name}"])) for r in polygons]
    geoms = []

    #cleaning polygon objects
    for r in polygons:
        geoms.append(wkt.loads(r['value'].split("^^")[0].strip('"')))

    gdf = gpd.GeoDataFrame(data=rows, geometry=geoms, crs="EPSG:4326")

    # Create map
    m = folium.Map(location=[44.70, -63.60], zoom_start=11)

    #popups for parcels
    headers = [k for k in rows[0]] #extracting headers directly from data
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
        },
        popup=popup_settings
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