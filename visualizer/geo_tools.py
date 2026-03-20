def into_geo_objects_list(to_iterate: list[str]) -> list:
    from shapely import wkt

    #Cleaning data
    clean_data = [wkt_literal.split("^^")[0].strip('"') for wkt_literal in to_iterate]

    #Transforming and returning
    return [wkt.loads(wkt_str) for wkt_str in clean_data]