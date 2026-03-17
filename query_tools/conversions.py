from datetime import datetime, date
import polars as pl
from SPARQLWrapper import SPARQLWrapper, JSON
from shapely import wkt
from copy import deepcopy
import random

def polygon_centroid(wkt_literal: str, as_link: bool) -> str:
    """
    Takes a WKT polygon string (with or without datatype suffix),
    computes centroid, and returns a Google Maps link.
    """

    # Remove datatype suffix if present
    if "^^" in wkt_literal:
        wkt_literal = wkt_literal.split("^^")[0].strip('"')

    # Load polygon
    polygon = wkt.loads(wkt_literal)

    # Compute centroid
    centroid = polygon.centroid
    lon, lat = centroid.x, centroid.y   # WKT uses (lon lat)

    # Create Google Maps link
    centroid_return: str = f'({lat},{lon})'

    if as_link: centroid_return = f"https://www.google.com/maps?q={lat},{lon}"

    return centroid_return

def sample_results(to_sample: list) -> list:
    reduced = to_sample

    if len(to_sample) > 500:
        reduced = random.sample(to_sample, min(500, int(0.10*len(to_sample)))) #if too many returns, only return max 500 (otherwise gpt blows up)
    
    return reduced #modified original obj

def add_approx_loc_to_sparql_return(results: dict, as_link: bool = False, drop_polygon: bool = True, reduce: bool = True) -> list:
    #adds a google maps approx loc, when a polygon object is present

    results_copy = deepcopy(results)

    for inx, result in enumerate(results["results"]["bindings"]):
        for key, val in result.items():

            #checking if polygon present
            try:
                if val['datatype'] == "http://www.opengis.net/ont/geosparql#wktLiteral": #polygon object
                    results_copy["results"]["bindings"][inx]['approx_loc'] = {'value': polygon_centroid(val['value'], as_link)}
                    if drop_polygon:
                        del results_copy["results"]["bindings"][inx]["pl"] #dropping polygon
            except:
                continue

    if reduce: results_copy = sample_results([r for r in results_copy["results"]["bindings"]])

    return results_copy