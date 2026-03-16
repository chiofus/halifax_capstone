#Uses this source: https://data-hrm.hub.arcgis.com/datasets/HRM::building-symbols/about

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

FCODE_MAPPING = {
    "LIDCF": "Composting Facility",
    "BLIDRE": "Recycling Facility",
    "BLIDRS": "Refuse Facility",
    "BLISAR": "Archive",
    "BLISCT": "Court House",
    "BLISDC": "Daycare",
    "BLISFS": "Fire Station",
    "BLISHO": "Hospital",
    "BLISLG": "Legislative Building",
    "BLISLB": "Library",
    "BLISCL": "Medical Clinic",
    "BLISNH": "Nursing Home",
    "BLISPB": "Paramedic Base Station",
    "BLISPW": "Place of Worship",
    "BLISPO": "Post Office",
    "BLISRS": "RCMP Station",
    "BLRCAG": "Art Gallery",
    "BLRCCC": "Community Centre",
    "BLRCEC": "Event Centre",
    "BLRCMU": "Museum",
    "BLRCRF": "Recreation Facility",
    "BLRCTR": "Theatre",
    "BLTRAT": "Airport Terminal",
    "BLTRBT": "Bus Terminal",
    "BLTRFT": "Ferry Terminal",
    "BLTRRT": "Rail Terminal",
    "BLISPSHQ": "HR Police Headquarters",
    "BLISPSCO": "HR Police Community Office",
    "BLISPSSO": "HR Police Satellite Office",
    "BLISSH": "Public School",
    "BLISSHP": "Private School",
    "BLRCOB": "Observatory",
    "BLRSRC": "Residential Care Facility",
    "BLRSCB": "Community Based Option",
    "BLRSDS": "Developmental Residence",
    "BLRSGH": "Group Home",
    "BLRSRR": "Regional Rehabilitation Centre",
    "BLRSSC": "Seniors Residential Complex",
    "BLRSSO": "Small Option",
    "BLRSSA": "Supervised Apartment",
    "BLTRPW": "Public Works Depot"
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
            use_id = curr_row.get("SYMB_ID") #reference SYMB id for use
            building_id = curr_row.get("BL_ID") #reference building id directly
            building_uri = URIRef(COT[f"Building{building_id}"])
            use_uri = URIRef(COT[f"BuildingUse{use_id}"])

            #generating triples for additional building details
            g.add((building_uri, RDF.type, HP.Building))
            g.add((use_uri, RDF.type, BDG.BuildingUse))
            g.add((building_uri, BDG.use, use_uri))

            if curr_row["FCODE"]: #add use if given
                use_code_uri = URIRef(COT[f"BuildingUseCode{use_id}"])
                g.add((use_code_uri, RDF.type, CODE.Code))
                g.add((use_uri, CODE.hasCode, use_code_uri))
                g.add((use_code_uri, GENPROP.hasName, fast_literal(FCODE_MAPPING[curr_row["FCODE"]])))

            if curr_row["LABEL"]: #add name if given
                g.add((use_uri, GENPROP.hasName, fast_literal(curr_row["LABEL"]))) #general name for site

            #Now adding point geometry
            try:
                geom = shape(feature["geometry"])
                wkt = wkt_dumps(geom, rounding_precision=12)

                loc_uri = URIRef(COT[f"BuildingUseLoc{use_id}"])
                g.add((loc_uri, RDF.type, LOC.Location))
                g.add((use_uri, LOC.hasLocation, loc_uri))
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
    create_triples(input_geojson="raw_data/building_symbols.geojson",
                        #   input_csv="raw_data/building_polygons.csv",
                          output_file="sample_triples/building_symbols.ttl",
                          sample=True,
                          format="ttl")