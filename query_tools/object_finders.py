import polars as pl
from query_tools.general_query import convert_default_sparql_to_df, query_endpoint
from objects.objects import GENERAL_POLYGON_SEARCH, GET_POLYGON_PARCEL
from visualizer.geo_tools import into_geo_objects_list
from shapely.geometry import Polygon
from shapely.strtree import STRtree

def find_parcel_covers(parcel_object: str, search_obj_type: str) -> list[str]: #returns all obj references that have polygons containing given object
    
    #1. Getting all zoning type polygon objects
    ALL_POLYGONS: pl.DataFrame = convert_default_sparql_to_df(
        query_endpoint(GENERAL_POLYGON_SEARCH.replace("CUSTOM_PROPERTY_OBJ", search_obj_type))
    )

    BASE_POLYGONS: list[Polygon] = into_geo_objects_list([pol for pol in ALL_POLYGONS["pl"].to_list()])
    
    #2. Getting target parcel's polygon
    POLYGON_TO_MATCH = get_polygon_parcel(parcel_object)

    #3. Build tree and match first poly that the base polygon covers
    tree = STRtree(BASE_POLYGONS)

    candidates = tree.query(POLYGON_TO_MATCH)

    if candidates.size != 0:
        DF_LIST: list[str] = ALL_POLYGONS["p"].to_list()
        to_return = []
        for candidate_idx in candidates:
            to_return.append(f"cot:{DF_LIST[candidate_idx].split("#")[-1]}")

        return to_return

    return []

def get_polygon_parcel(parcel_object: str) -> Polygon:
    query_as_df: pl.DataFrame = convert_default_sparql_to_df(query_endpoint(GET_POLYGON_PARCEL.replace("CUSTOM_PROPERTY_OBJ", parcel_object)))

    return into_geo_objects_list([pol for pol in query_as_df["pl"].to_list()])[0]