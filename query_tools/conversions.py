from datetime import datetime, date
import polars as pl
from SPARQLWrapper import SPARQLWrapper, JSON
from shapely import wkt

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
    vars_ = results["head"]["vars"]
    bindings = results["results"]["bindings"]

    rows = []
    for binding in bindings:
        row = {}
        for var in vars_:
            if var in binding:
                row[var] = cast_value(binding[var])
            else:
                row[var] = None
        rows.append(row)

    return pl.DataFrame(rows)

def polygon_to_google_maps_link(wkt_literal: str) -> str:
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
    maps_url = f"https://www.google.com/maps?q={lat},{lon}"

    return maps_url

def add_approx_loc_to_sparql_return(results: dict) -> dict:
    #adds a google maps approx loc, when a polygon object is present

    for result in results["results"]["bindings"]:
        for key, val in result.items():
            # print(type(val))
            # print(f"{key}: {val}\n")

            #checking if polygon present
            try:
                if val['datatype'] == "http://www.opengis.net/ont/geosparql#wktLiteral": #polygon object
                    val['approx_loc'] = polygon_to_google_maps_link(val['value'])
            except:
                continue
    
    return results #modified original obj
            