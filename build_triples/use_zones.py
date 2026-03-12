import orjson
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, XSD
from shapely.geometry import shape
from shapely.wkt import dumps as wkt_dumps
import polars as pl
from tqdm import tqdm

# Namespaces
HP = Namespace("http://ontology.eil.utoronto.ca/HPCDM/")
COT = Namespace("http://ontology.eil.utoronto.ca/Halifax/Halifax-DT-Capstone#")
LOC = Namespace("https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/")
CITYUNITS = Namespace("http://ontology.eil.utoronto.ca/5087/1/CityUnits/")
I72 = Namespace("http://ontology.eil.utoronto.ca/ISO21972/iso21972#")
GEO = Namespace("http://www.opengis.net/ont/geosparql#")

def fast_literal(value):
    """Return properly typed RDF literal for GraphDB performance."""
    if isinstance(value, int):
        return Literal(value, datatype=XSD.integer)
    if isinstance(value, float):
        return Literal(value, datatype=XSD.double)
    if isinstance(value, str):
        # Attempt float conversion
        try:
            parsed_float = float(value)
            return Literal(parsed_float, datatype=XSD.double)
        except Exception:
            return Literal(value)
    return Literal(str(value))

def create_triples(input_geojson: str, output_file: str, input_csv:str='', format="turtle"):
    g = Graph()
    g.bind("hp", HP)
    g.bind("cot", COT)
    g.bind("loc", LOC)
    g.bind("CityUnits", CITYUNITS)
    g.bind("i72", I72)
    g.bind("geo", GEO)

    print("Loading .geojson data...")
    with open(input_geojson, "rb") as f: #loading geojson
        data = orjson.loads(f.read())

    if input_csv:
        print("Loading .csv data...")
        csv_data = pl.read_csv(input_csv) #loading csv data

    common_data_name: str = "UseZone"

    print("Creating triples...")
    for index, feature in enumerate(tqdm(data["features"])): #Building zone uses triples
        try:
            object_id = feature["properties"].get("OBJECTID") #uses object id to create unique zone id
            zone_uri = URIRef(COT[f"UseZone{object_id}"])
            loc_uri = URIRef(COT[f"UseZoneLoc{object_id}"])
            area_uri = URIRef(COT[f"{common_data_name}Area{object_id}"])
            area_measure_uri = URIRef(COT[f"{common_data_name}AreaMeasure{object_id}"])
            perimeter_uri = URIRef(COT[f"{common_data_name}Perimeter{object_id}"])
            perimeter_measure_uri = URIRef(COT[f"{common_data_name}PerimeterMeasure{object_id}"])

            # Feature typing
            g.add((zone_uri, RDF.type, HP.UseZone))
            g.add((loc_uri, RDF.type, LOC.Location))
            g.add((area_uri, RDF.type, CITYUNITS.Area))
            g.add((area_measure_uri, RDF.type, I72.Measure))
            g.add((perimeter_uri, RDF.type, CITYUNITS.Length))
            g.add((perimeter_measure_uri, RDF.type, I72.Measure))

            #relations
            g.add((zone_uri, LOC.hasLocation, loc_uri))
            g.add((area_uri, I72.hasValue, area_measure_uri))
            g.add((perimeter_uri, I72.hasValue, perimeter_measure_uri))
            g.add((zone_uri, HP.hasArea, area_uri))
            g.add((zone_uri, HP.hasPerimeter, perimeter_uri))

            #units and values
            obj_data = csv_data.filter(pl.col("OBJECTID") == object_id) #csv data filtered to curr object
            g.add((area_measure_uri, I72.hasUnit, I72.square_metre))
            g.add((area_measure_uri, I72.hasNumericalValue, fast_literal(obj_data[0, "Shape__Area"])))
            g.add((perimeter_measure_uri, I72.hasUnit, I72.metre))
            g.add((perimeter_measure_uri, I72.hasNumericalValue, fast_literal(obj_data[0, "Shape__Length"])))

            #special values to keep
            g.add((zone_uri, HP.hasZoneType, fast_literal(feature["properties"].get("DESCRIPTION")))) #keeps the actual zone use in plain english

            # Geometry conversion and object
            geom = shape(feature["geometry"])
            wkt = wkt_dumps(geom, rounding_precision=12)
            g.add((
                loc_uri,
                GEO.asWKT,
                Literal(f"{wkt}", datatype=GEO.wktLiteral)
            ))

        except Exception as e:
            print(f"Failed to load row {index}:\n{e}")

    # For very large datasets, use "nt" (N-Triples) for fastest bulk load
    g.serialize(destination=output_file, format=format)
    print(f"✔ Done. Output written to {output_file}")


if __name__ == "__main__":
    create_triples(input_geojson="raw_data/Use_Zones/ZoningBoundaries_8799345250073700043.geojson",
                   input_csv="raw_data/Use_Zones/ZoningBoundaries_6646520888006840889.csv",
                   output_file="raw_data/Use_Zones/use_zones.ttl",
                          format="turtle")
    # For bulk load speed:
    # convert_geojson("input.geojson", "output.nt", format="nt")