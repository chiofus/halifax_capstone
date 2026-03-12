import orjson
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, XSD
from shapely.geometry import shape
from shapely.wkt import dumps as wkt_dumps
import polars as pl
from tqdm import tqdm

#Globals
OWNER_DICT = {
    "CCGRD": "Canadian Coast Guard",
    "CN": "Canadian National",
    "CNDO": "Condominium Corporation",
    "CSAP": "Conseil scolaire acadien provincial",
    "DND": "Department of National Defense",
    "FED": "Federal",
    "HDBC": "Halifax‐Dartmouth Bridge Commission",
    "HIAA": "Halifax International Airport Authority",
    "HRM": "Halifax",
    "HRSB": "Halifax Regional School Board",
    "HW": "Halifax Water",
    "NA": "Not Applicable",
    "NSPI": "Nova Scotia Power",
    "NTO": "Not Taken Over",
    "PRIV": "Private Person, Business, Organization or Agency",
    "PROV": "Province of Nova Scotia",
    "UN": "Unknown"
}

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

def create_triples(input_geojson: str, output_file: str, input_csv:str ='', format="turtle"):
    #Creates triples for defining which parcel is owned by who, by linking it to a building's owners
    #Note that no info was NOT given on parcel owners, but rather on building owners.
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

    print("Creating triples...")
    for index, feature in enumerate(tqdm(data["features"])): #Getting all objects to process
        try:
            object_id = feature["properties"].get("BL_ID") #uses building id to uniquely identify objs
            building_uri = URIRef(COT[f"Building{object_id}"])

            # Feature typing
            g.add((building_uri, RDF.type, HP.Building))

            #units and values
            g.add((building_uri, HP.hasOwner, fast_literal(OWNER_DICT[feature["properties"].get("OWNER")])))

        except Exception as e:
            print(f"Failed to load row {index}:\n{e}")

    # For very large datasets, use "nt" (N-Triples) for fastest bulk load
    g.serialize(destination=output_file, format=format)
    print(f"✔ Done. Output written to {output_file}")

if __name__ == "__main__":
    create_triples(input_geojson="raw_data/buildings_owners/all_owners.geojson",
                    output_file="raw_data/buildings_owners/buildings_owners.ttl",
                    format="turtle")
    # For bulk load speed:
    # convert_geojson("input.geojson", "output.nt", format="nt")