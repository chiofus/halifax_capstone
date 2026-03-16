#Uses this source: https://data-hrm.hub.arcgis.com/datasets/HRM::civic-addresses/about

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

#General
FCODE_MAPPING = {
    "CIV_ENV": "Environmental point (pumping station)",
    "CIV_NUM": "Valid civic number on building",
    "CIV_TBD": "Unknown civic number (zero) on building",
    "PARKLAND": "Point represents parkland",
    "REM_LAND": "Point represents remainder land",
    "CIV_TMP": "Temporary civic number, to be removed at later date"
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

    print("Loading .geojson data...")
    with open(input_geojson, "rb") as f: #loading geojson
        data = orjson.loads(f.read())

    print("Creating triples...")
    for index, feature in enumerate(tqdm(data["features"])): #Building parcel triples
        if sample and index > 5: break #exit loop is just sampling
        curr_row: dict = feature["properties"]
        try:
            civ_id = str(curr_row.get("CIV_ID")).strip()
            address_uri = URIRef(COT[f"CivicAddress{civ_id}"])
            g.add((address_uri, RDF.type, CONTACT.Address))

            elements_to_check = ["CIV_ALPHA", "CIV_POSTAL", "STR_NAME", "STR_TYPE"]
            objects_to_add = [CONTACT.hasStreetNumber, CONTACT.hasPostalCode, CONTACT.hasStreet, CONTACT.hasStreetType]

            for i, el in enumerate(elements_to_check):
                quick_triple_add(g, el, curr_row, address_uri, objects_to_add[i])

            #General info
            g.add((address_uri, CONTACT.hasCity, HALIFAX.halifax))
            g.add((address_uri, CONTACT.hasProvince, CDT.novaScotia))
            g.add((address_uri, CONTACT.hasCountry, CDT.canada))

            #Add code info
            if curr_row["FCODE"]:
                code_uri = URIRef(COT[f"CivicAddressFCode{civ_id}"])
                g.add((code_uri, RDF.type, CODE.Code))
                g.add((address_uri, CODE.hasCode, code_uri))
                g.add((code_uri, GENPROP.hasName, fast_literal(FCODE_MAPPING[curr_row["FCODE"]])))

            #Add PID info
            if curr_row["PID"]:
                pid = str(curr_row.get("PID")).strip()
                pid_code_uri = URIRef(COT[f"CivicAddressPIDCode{civ_id}"])
                g.add((pid_code_uri, RDF.type, CODE.Code))
                g.add((address_uri, CODE.hasCode, pid_code_uri))
                g.add((pid_code_uri, I72.hasValue, Literal(pid)))

            #Now adding point geometry
            try:
                geom = shape(feature["geometry"])
                wkt = wkt_dumps(geom, rounding_precision=12)

                loc_uri = URIRef(COT[f"CivicAddressLoc{civ_id}"])
                g.add((loc_uri, RDF.type, LOC.Location))
                g.add((address_uri, LOC.hasLocation, loc_uri))
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
    create_triples(input_geojson="raw_data/civic_addresses.geojson",
                        #   input_csv="raw_data/building_polygons.csv",
                          output_file="sample_triples/civic_addresses.ttl",
                          sample=True,
                          format="ttl")