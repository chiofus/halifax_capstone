from datetime import datetime, date
import polars as pl
from SPARQLWrapper import SPARQLWrapper, JSON
from shapely import wkt
from copy import deepcopy
import random

XSD_INT = {
    "http://www.w3.org/2001/XMLSchema#integer",
    "http://www.w3.org/2001/XMLSchema#int",
    "http://www.w3.org/2001/XMLSchema#long",
    "http://www.w3.org/2001/XMLSchema#short",
    "http://www.w3.org/2001/XMLSchema#byte",
    "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
    "http://www.w3.org/2001/XMLSchema#nonPositiveInteger",
    "http://www.w3.org/2001/XMLSchema#positiveInteger",
    "http://www.w3.org/2001/XMLSchema#negativeInteger",
}

XSD_FLOAT = {
    "http://www.w3.org/2001/XMLSchema#decimal",
    "http://www.w3.org/2001/XMLSchema#float",
    "http://www.w3.org/2001/XMLSchema#double",
}

XSD_BOOL = {
    "http://www.w3.org/2001/XMLSchema#boolean",
}

XSD_DATE = {
    "http://www.w3.org/2001/XMLSchema#date",
}

XSD_DATETIME = {
    "http://www.w3.org/2001/XMLSchema#dateTime",
}

def cast_value(value_dict: dict):
    """Cast a SPARQL binding value to appropriate Python type."""
    value = value_dict.get("value")
    dtype = value_dict.get("datatype")
    value_type = value_dict.get("type")

    if value is None:
        return None

    # URIs remain strings
    if value_type == "uri":
        return value

    if dtype in XSD_INT:
        return int(value)

    if dtype in XSD_FLOAT:
        return float(value)

    if dtype in XSD_BOOL:
        return value.lower() == "true"

    if dtype in XSD_DATE:
        return date.fromisoformat(value)

    if dtype in XSD_DATETIME:
        # Handles timezone-aware ISO strings
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    # Fallback: string
    return value

def convert_default_sparql_to_df(results: dict):
    # vars_ = list(results["results"]["bindings"])
    bindings: dict[dict] = results["results"]["bindings"]

    rows = []
    for binding in bindings:
        row = {}
        curr_vars = list(binding.keys())
        for var in curr_vars:
            # if var in binding:
            row[var] = cast_value(binding[var])
            # else:
            #     row[var] = None
        rows.append(row)

    return pl.DataFrame(rows)

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