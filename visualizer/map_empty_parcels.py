from shapely.geometry import Point

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
    from objects.objects import PARCEL_POLYGONS_QUERY, CIVIC_ADDRESSES_POINTS_QUERY
    from visualizer.geo_tools import into_geo_objects_list

    #Helper functions
    def find_empty_addresses_strtree(
            civic_addresses_to_check: list[Point],
            polygons_to_check: list[Polygon],
            json_dump_location: str = ''
        ) -> list[dict]:
    
        #Imports
        from shapely.strtree import STRtree
        from os import makedirs
        from tqdm import tqdm
        import pickle, json
        from copy import deepcopy

        #Spatial index and globals
        tree = STRtree(polygons_to_check)
        empty_civic_addresses: list[dict] = []
        
        #Main loop
        print("Getting all empty civic addresses...")
        for idx_point, point in enumerate(tqdm(civic_addresses_to_check)):
            #Quickly find candidate polys and check length of matches
            intersecting_polys = tree.query(point)

            if len(intersecting_polys) == 0:
                empty_civic_addresses.append(
                    {
                        'point': point,
                        'point_str': point.wkt,
                        'df_index': idx_point
                    }
                )

        #Dumping into .json
        try:
            if json_dump_location:  #if a dump location is given, list is also dumped as json
                #will first pickle it for later use

                #Make sure dir exists
                makedirs(
                    '/'.join(json_dump_location.split('/')[:-1]), exist_ok=True
                )

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

    empty_civic_addresses = find_empty_addresses_strtree(civic_addresses_to_check, polygons_to_check, json_dump_location)

    return empty_civic_addresses

def map_empty_civ_addresses() -> int: #returns number of empty addresses mapped
    import plotly.express as px

    #Get points to map
    points: list[Point] = [item['point'] for item in get_empty_civic_addresses()]

    #Basic point info
    lats = [pt.y for pt in points]
    lons = [pt.x for pt in points]

    #Creating map
    fig = px.scatter_map(
        lat=lats,
        lon=lons,
        zoom=10,
        height= 900,
        map_style="satellite-streets"
    )

    fig.update_traces(
        marker=dict(
            size=8,
            opacity=0.7,
            color = "indianred"
        )
    )

    fig.show()

    return len(points)