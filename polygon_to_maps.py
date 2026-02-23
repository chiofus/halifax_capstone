from shapely import wkt

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


# Example usage
wkt_polygon = '"POLYGON ((-63.790463287026 44.741862802632, -63.790566830202 44.741759951960, -63.790589414879 44.741771791773, -63.790603458118 44.741759280426, -63.790692557518 44.741804832898, -63.790617124316 44.741880859409, -63.790597042890 44.741870835077, -63.790557415084 44.741910186378, -63.790463287026 44.741862802632))"^^geo:wktLiteral'

link = polygon_to_google_maps_link(wkt_polygon)
print(link)