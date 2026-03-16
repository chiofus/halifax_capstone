#Uses this source: https://data-hrm.hub.arcgis.com/datasets/HRM::buildings-1/about?layer=2

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

    print("Loading .geojson data...")
    with open(input_geojson, "rb") as f: #loading geojson
        data = orjson.loads(f.read())

    print("Creating triples...")
    for index, feature in enumerate(tqdm(data["features"])): #Building parcel triples
        if sample and index > 5: break #exit loop is just sampling
        curr_row: dict = feature["properties"]
        try:
            use_id = curr_row.get("USE_ID") #reference building id directly
            building_id = curr_row.get("BL_ID") #reference building id directly
            building_uri = URIRef(COT[f"Building{building_id}"])
            use_uri = URIRef(COT[f"BuildingUse{use_id}"])

            #generating triples for additional building details
            g.add((building_uri, RDF.type, HP.Building))
            g.add((use_uri, RDF.type, BDG.BuildingUse))
            g.add((building_uri, BDG.use, use_uri))

            if curr_row["USE_"]: #add use if given
                use_code_uri = URIRef(COT[f"BuildingUseCode{use_id}"])
                g.add((use_code_uri, RDF.type, CODE.Code))
                g.add((use_uri, CODE.hasCode, use_code_uri))
                g.add((use_code_uri, GENPROP.hasName, fast_literal(curr_row["USE_"])))

            if curr_row["USE_NAME"]: #add use if given
                g.add((use_uri, GENPROP.hasName, fast_literal(curr_row["USE_NAME"])))

        except Exception as e:
            print(f"Failed to load row {index}:\n{e}")

    # For very large datasets, use "nt" (N-Triples) for fastest bulk load
    g.serialize(destination=output_file, format=format)
    print(f"✔ Done. Output written to {output_file}")


if __name__ == "__main__":
    create_triples(input_geojson="raw_data/building_uses.geojson",
                        #   input_csv="raw_data/building_polygons.csv",
                          output_file="sample_triples/building_uses.ttl",
                          sample= True,
                          format="ttl")