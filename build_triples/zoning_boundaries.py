#Uses this source: https://data-hrm.hub.arcgis.com/datasets/HRM::zoning-boundaries/about

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
GENPROP = Namespace('https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/GenericProperties/')
TIME = Namespace('http://www.w3.org/2006/time#')
BDG = Namespace('https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/Building/')
CODE = Namespace('https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/Code/')
CONTACT = Namespace('https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/Contact/')
HALIFAX = Namespace('http://ontology.eil.utoronto.ca/Halifax/Halifax#')
CDT = Namespace('http://ontology.eil.utoronto.ca/CDT#')
OZ = Namespace('http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#')

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

def quick_triple_add(graph: Graph, to_check: str, data_row: dict, subject, object) -> None:
    #adds a triple to graph if given ref exists in given row of data
    if data_row[to_check]:
        graph.add((subject, object, fast_literal(data_row[to_check])))

def create_triples(input_geojson: str, output_file: str, input_csv:str = '', format: str ="turtle", sample: bool = False):
    g = Graph()
    g.bind("hp", HP)
    g.bind("cot", COT)
    g.bind("loc", LOC)
    g.bind("CityUnits", CITYUNITS)
    g.bind("i72", I72)
    g.bind("geo", GEO)
    g.bind("genprop", GENPROP)
    g.bind("time", TIME)
    g.bind("bdg", BDG)
    g.bind("code", CODE)
    g.bind("contact", CONTACT)
    g.bind("halifax", HALIFAX)
    g.bind("cdt", CDT)
    g.bind("oz", OZ)

    print("Loading .geojson data...")
    with open(input_geojson, "rb") as f: #loading geojson
        data = orjson.loads(f.read())

    print("Creating triples...")
    for index, feature in enumerate(tqdm(data["features"])): #Building parcel triples
        if sample and index > 5: break #exit loop is just sampling
        curr_row: dict = feature["properties"]
        try:
            zoning_uri = URIRef(COT[f"ZoningBoundary{index+1}"]) #no specific index given, use default row idx
            g.add((zoning_uri, RDF.type, HP.ZoningType))

            if curr_row["DESCRIPTION"]:
                g.add((zoning_uri, OZ.allowsUse, Literal(curr_row["DESCRIPTION"])))

            #adding geometry
            try:
                geom = shape(feature["geometry"])
                wkt = wkt_dumps(geom, rounding_precision=12)

                loc_uri = URIRef(COT[f"ZoningBoundaryLoc{index+1}"])
                g.add((loc_uri, RDF.type, LOC.Location))
                g.add((zoning_uri, LOC.hasLocation, loc_uri))
                g.add((
                    loc_uri,
                    GEO.asWKT,
                    Literal(f"{wkt}", datatype=GEO.wktLiteral)
                ))
            except:
                pass #no geometry added                

        except Exception as e:
            print(f"Failed to load row {index}:\n{e}")

    # For very large datasets, use "nt" (N-Triples) for fastest bulk load
    g.serialize(destination=output_file, format=format)
    print(f"✔ Done. Output written to {output_file}")


if __name__ == "__main__":
    create_triples(input_geojson="raw_data/zoning_boundaries.geojson",
                        #   input_csv="raw_data/building_polygons.csv",
                          output_file="sample_triples/zoning_boundaries.ttl",
                          sample= True,
                          format="ttl")