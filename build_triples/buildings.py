#Uses this source: https://data-hrm.hub.arcgis.com/datasets/HRM::buildings-1/about?layer=1

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

#General

OWNERS_MAPPING = { #code to actual name
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

TYPE_MAPPING = { #code to actual type / use
    "ACCESS": "Accessory Structure",
    "AGRIC": "Agricultural",
    "AQUA": "Aquaculture",
    "BACK": "Dwelling ‐ Backyard Suite",
    "COMM": "Commercial",
    "INDUS": "Industrial",
    "INSGOV": "Institutional and Governmental",
    "MIXED": "Mixed Use ‐ Residential & Other Use(s)",
    "MOBILE": "Mobile Home",
    "MUD": "Dwelling ‐ Multiple Units",
    "SEASON": "Dwelling ‐ Seasonal",
    "SEMI": "Dwelling ‐ Semi‐Detached",
    "SUD": "Dwelling ‐ Single Detached",
    "TOWN": "Dwelling ‐ Townhouse",
    "BACK": "Dwelling ‐ Backyard Suite"
}

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

def create_triples(input_geojson: str, output_file: str, input_csv:str = '', format: str ="turtle"):
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

    if input_csv:
        print("Loading .csv data...")
        csv_data = pl.read_csv(input_csv) #loading csv data

    print("Creating triples...")
    for index, feature in enumerate(tqdm(data["features"])): #Building parcel triples
        curr_row: dict = feature["properties"]
        try:
            object_id = curr_row.get("BL_ID") #reference building id directly
            building_uri = URIRef(COT[f"Building{object_id}"])

            #generating triples for additional building details
            g.add((building_uri, RDF.type, HP.Building))

            if curr_row["BL_NAME"]: #add name if given
                g.add((building_uri, GENPROP.hasName, fast_literal(curr_row["BL_NAME"])))

            if curr_row["OWNER"]: #add owner if given
                g.add((building_uri, HP.ownership, fast_literal(OWNERS_MAPPING[curr_row["OWNER"]])))

            if curr_row["CONST_YEAR"]: #add year if given
                year = str(curr_row["CONST_YEAR"])
                year_uri = URIRef(COT[f"Year{year}"])

                g.add((year_uri, RDF.type, TIME.DateTimeDescription))
                g.add((building_uri, BDG.yearOfConstruction, year_uri))
                g.add((year_uri, TIME.year, Literal(curr_row["CONST_YEAR"], datatype=XSD.gYear)))

            if curr_row["STC_TYPE"]:
                building_type: str = TYPE_MAPPING[curr_row["STC_TYPE"]]
                building_type_nospace = building_type.replace(" ", "").replace("‐", "")

                building_use_uri = URIRef(COT[f"BuildingUse{building_type_nospace}"])
                building_use_code_uri = URIRef(COT[f"BuildingUseCode{building_type_nospace}"])

                g.add((building_use_uri, RDF.type, BDG.BuildingUse))
                g.add((building_use_code_uri, RDF.type, CODE.Code))

                g.add((building_uri, BDG.use, building_use_uri))

                g.add((building_use_uri, CODE.hasCode, building_use_code_uri))
                g.add((building_use_code_uri, GENPROP.hasName, fast_literal(building_type)))

            if curr_row["HEIGHT_M"]:
                building_height_uri = URIRef(COT[f"BuildingHeight{object_id}"])
                building_height_measure_uri = URIRef(COT[f"BuildingHeightMeasure{object_id}"])

                g.add((building_height_uri, RDF.type, HP.BuildingHeight))
                g.add((building_uri, HP.hasBuildingHeight, building_height_uri))

                g.add((building_height_uri, I72.hasValue, building_height_measure_uri))
                g.add((building_height_measure_uri, RDF.type, I72.Measure))
                g.add((building_height_measure_uri, I72.hasNumericalValue, fast_literal(curr_row["HEIGHT_M"])))
                g.add((building_height_measure_uri, I72.hasUnit, I72.metre))

        except Exception as e:
            print(f"Failed to load row {index}:\n{e}")

    # For very large datasets, use "nt" (N-Triples) for fastest bulk load
    g.serialize(destination=output_file, format=format)
    print(f"✔ Done. Output written to {output_file}")


if __name__ == "__main__":
    create_triples(input_geojson="raw_data/buildings.geojson",
                        #   input_csv="raw_data/building_polygons.csv",
                          output_file="triples/buildings.nt",
                          format="nt")