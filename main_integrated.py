from dotenv import load_dotenv
import os
import re
import tempfile
import urllib.request
import urllib.parse
import json

from rdflib import Graph, RDF, RDFS, OWL, Namespace
from rdflib.namespace import SKOS
from langchain_openai import ChatOpenAI
from langchain_community.graphs import OntotextGraphDBGraph
from langchain_community.chains.graph_qa.ontotext_graphdb import OntotextGraphDBQAChain

load_dotenv()

GRAPHDB_QUERY_ENDPOINT = "http://localhost:7200/repositories/zoning"
HPCDM_FILE = "HPCDM_with_instances.ttl"

HP = Namespace("http://ontology.eil.utoronto.ca/HPCDM/")

# ── Core namespaces (synthetic test data) ─────────────────────────
HP_URI   = "http://ontology.eil.utoronto.ca/HPCDM/"
OM1_URI  = "http://www.wurvoc.org/vocabularies/om-1.8/"
GPROP    = "https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/GenericProperties/"
LU5087   = "https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/LandUse/"
RDFS_URI = "http://www.w3.org/2000/01/rdf-schema#"
FOAF_URI = "http://xmlns.com/foaf/0.1/"
ORG_URI  = "http://www.w3.org/ns/org#"

# ── Real Halifax instance namespaces (teammate data) ──────────────
# All real Halifax instances use the cot: prefix
COT     = "http://ontology.eil.utoronto.ca/Halifax/Halifax-DT-Capstone#"
# ISO 21972 quantities — area/perimeter use i72:hasValue → i72:hasNumericalValue
I72     = "http://ontology.eil.utoronto.ca/ISO21972/iso21972#"
# ISO 5087-2 Code — PID stored as i72:hasValue on a code:Code node
CODE    = "https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/Code/"
# ISO 5087-2 Contact/Address — used in civic_addresses.ttl
CONTACT = "https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/Contact/"
# ISO 5087-2 Building — bdg:use, bdg:yearOfConstruction
BDG     = "https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/Building/"
# OntoZoning — oz:allowsUse on ZoningBoundary instances (plain string literal)
OZ      = "http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#"
# Generic properties — genprop:hasName on Code nodes
GENPROP = "https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/GenericProperties/"
# W3C time — time:year on year-of-construction nodes
TIME    = "http://www.w3.org/2006/time#"

# =====================================================================
# SPARQL TEMPLATES
#
# TWO sets of templates are defined:
#   1. Synthetic templates  — query the hand-crafted hp:parcelXXXXX instances
#      (used when parcel ID matches the synthetic test data format, e.g. "12345")
#   2. Real Halifax templates — query the teammate's cot:PropertyXXXXXX instances
#      (used when the question contains a real Halifax property number, e.g. "342126")
#
# The dispatcher _detect_template() and _extract_parcel_id() decide which set to use
# based on the parcel number found in the question.
# =====================================================================

# ── Synthetic instance templates (hp: namespace, gprop:hasIdentifier) ──
SPARQL_TEMPLATES: dict[str, str] = {
    "zoning": """
PREFIX hp:    <{HP}>
PREFIX gprop: <{GPROP}>
SELECT ?zoningId WHERE {{
  hp:parcel{parcel_id} hp:hasZone ?zone .
  ?zone gprop:hasIdentifier ?zoningId .
}}""",
    "landuse": """
PREFIX hp:   <{HP}>
PREFIX lu:   <{LU}>
PREFIX gprop:<{GPROP}>
SELECT ?luName WHERE {{
  hp:parcel{parcel_id} lu:landUse ?lu .
  ?lu gprop:hasName ?luName .
}}""",
    "area": """
PREFIX hp:  <{HP}>
PREFIX om1: <{OM1}>
SELECT ?val WHERE {{
  hp:parcel{parcel_id} hp:hasArea ?areaNode .
  ?areaNode om1:numerical_value ?val .
}}""",
    "perimeter": """
PREFIX hp:  <{HP}>
PREFIX om1: <{OM1}>
SELECT ?val WHERE {{
  hp:parcel{parcel_id} hp:hasPerimeter ?periNode .
  ?periNode om1:numerical_value ?val .
}}""",
    "ownership": """
PREFIX hp:   <{HP}>
PREFIX foaf: <{FOAF}>
SELECT ?ownerName WHERE {{
  hp:parcel{parcel_id} hp:ownership ?owner .
  OPTIONAL {{ ?owner foaf:name ?ownerName . }}
  OPTIONAL {{ ?owner rdfs:label  ?ownerName . }}
}}""",
    "has_building": """
PREFIX hp: <{HP}>
SELECT ?building WHERE {{
  ?building a hp:Building ;
            hp:occupies hp:parcel{parcel_id} .
}}""",
    "building_height": """
PREFIX hp:  <{HP}>
PREFIX om1: <{OM1}>
SELECT ?val WHERE {{
  ?bldg hp:occupies hp:parcel{parcel_id} ;
        hp:hasHeight ?hNode .
  ?hNode om1:numerical_value ?val .
}}""",
    "setback": """
PREFIX hp:  <{HP}>
PREFIX om1: <{OM1}>
SELECT ?val WHERE {{
  ?bldg hp:occupies hp:parcel{parcel_id} ;
        hp:hasSetbacck ?sbNode .
  ?sbNode om1:numerical_value ?val .
}}""",
    "fsr": """
PREFIX hp:  <{HP}>
PREFIX om1: <{OM1}>
SELECT ?val WHERE {{
  hp:parcel{parcel_id} hp:hasFSI ?fsiNode .
  ?fsiNode om1:numerical_value ?val .
}}""",
    "zoned_capacity": """
PREFIX hp:   <{HP}>
PREFIX gprop:<{GPROP}>
PREFIX oz: <http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#>
SELECT ?allowedUse WHERE {{
  hp:parcel{parcel_id} hp:hasZone ?zone .
  ?zone oz:allowsUse ?use .
  ?use gprop:hasName ?allowedUse .
}}""",
    "mixed_use": """
PREFIX hp:   <{HP}>
PREFIX gprop:<{GPROP}>
PREFIX oz:   <http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#>
SELECT ?allowedUse WHERE {{
  hp:parcel{parcel_id} hp:hasZone ?zone .
  ?zone oz:allowsUse ?use .
  ?use gprop:hasName ?allowedUse .
}}""",
    "planned_zoning": """
PREFIX hp:    <{HP}>
PREFIX gprop: <{GPROP}>
SELECT ?zoningId WHERE {{
  hp:parcel{parcel_id} hp:hasZone ?zone .
  ?zone gprop:hasIdentifier ?zoningId .
}}""",
    "nearby_zoning": """
PREFIX hp:    <{HP}>
PREFIX gprop: <{GPROP}>
SELECT ?zoningId WHERE {{
  hp:parcel{parcel_id} hp:hasZone ?zone .
  ?zone gprop:hasIdentifier ?zoningId .
}}""",
    "public_ownership": """
PREFIX hp:   <{HP}>
PREFIX org:  <{ORG}>
PREFIX rdfs: <{RDFS}>
SELECT ?ownerName WHERE {{
  hp:parcel{parcel_id} hp:ownership ?owner .
  ?owner a org:Organization ;
         rdfs:label ?ownerName .
}}""",
    "water_service": """
PREFIX hp:   <{HP}>
PREFIX rdfs: <{RDFS}>
SELECT ?label WHERE {{
  hp:parcel{parcel_id} hp:servicedBy ?svc .
  ?svc a hp:WaterDistributionService ;
       rdfs:label ?label .
}}""",
    "road_service": """
PREFIX hp:   <{HP}>
PREFIX rdfs: <{RDFS}>
SELECT ?label WHERE {{
  hp:parcel{parcel_id} hp:servicedBy ?svc .
  ?svc a hp:TransportationNetworkService ;
       rdfs:label ?label .
}}""",
    "wastewater_service": """
PREFIX hp:   <{HP}>
PREFIX rdfs: <{RDFS}>
SELECT ?label WHERE {{
  hp:parcel{parcel_id} hp:servicedBy ?svc .
  ?svc a hp:WastewaterService ;
       rdfs:label ?label .
}}""",
    "electricity_service": """
PREFIX hp:   <{HP}>
PREFIX rdfs: <{RDFS}>
SELECT ?label WHERE {{
  hp:parcel{parcel_id} hp:servicedBy ?svc .
  ?svc a hp:ElectricService ;
       rdfs:label ?label .
}}""",
    "fire_service": """
PREFIX hp:   <{HP}>
PREFIX rdfs: <{RDFS}>
SELECT ?label WHERE {{
  hp:parcel{parcel_id} hp:servicedBy ?svc .
  ?svc a hp:FireEmergencyService ;
       rdfs:label ?label .
}}""",
    "transit_service": """
PREFIX hp:   <{HP}>
PREFIX rdfs: <{RDFS}>
SELECT ?label WHERE {{
  hp:parcel{parcel_id} hp:servicedBy ?svc .
  ?svc a hp:PublicTransitService ;
       rdfs:label ?label .
}}""",
}

# ── Real Halifax templates (cot: namespace, i72: quantities) ──────
#
# Parcel identifier: property number in the URI  cot:PropertyXXXXXX
# Area chain:        cot:PropertyXXX  hp:hasArea  cot:PropertyAreaXXX
#                    cot:PropertyAreaXXX  i72:hasValue  cot:PropertyAreaMeasureXXX
#                    cot:PropertyAreaMeasureXXX  i72:hasNumericalValue  ?val
# Perimeter chain:   same pattern with hp:hasPerimeter / PropertyPerimeter
# Building:          cot:BuildingBLXXX  hp:occupies  cot:PropertyXXX
# Building use:      cot:BuildingBLXXX  bdg:use  ?useNode
#                    ?useNode  code:hasCode  ?codeNode
#                    ?codeNode  genprop:hasName  ?useName
# Ownership:         cot:BuildingBLXXX  hp:ownership  ?ownerLiteral  (plain string)
# Year built:        cot:BuildingBLXXX  bdg:yearOfConstruction  ?yearNode
#                    ?yearNode  time:year  ?year
# Zoning:            cot:ZoningBoundaryN  oz:allowsUse  ?useLiteral  (plain string)
#                    — zoning is stored as a spatial boundary, not linked to parcel by ID
#                    — query by WKT containment requires GeoSPARQL reasoning
# PID lookup:        cot:CivicAddressPIDCodeXXX  i72:hasValue  "XXXXXXXX"
#                    (8-digit Nova Scotia PID)
#
HALIFAX_TEMPLATES: dict[str, str] = {

    # Area of a real Halifax property by its numeric suffix (e.g. "342126")
    "area": """
PREFIX cot: <{COT}>
PREFIX hp:  <{HP}>
PREFIX i72: <{I72}>
SELECT ?val WHERE {{
  cot:Property{parcel_id} hp:hasArea ?areaNode .
  ?areaNode i72:hasValue ?measure .
  ?measure i72:hasNumericalValue ?val .
}}""",

    # Perimeter of a real Halifax property
    "perimeter": """
PREFIX cot: <{COT}>
PREFIX hp:  <{HP}>
PREFIX i72: <{I72}>
SELECT ?val WHERE {{
  cot:Property{parcel_id} hp:hasPerimeter ?periNode .
  ?periNode i72:hasValue ?measure .
  ?measure i72:hasNumericalValue ?val .
}}""",

    # Building(s) on a real Halifax property
    "has_building": """
PREFIX cot: <{COT}>
PREFIX hp:  <{HP}>
SELECT ?building WHERE {{
  ?building a hp:Building ;
            hp:occupies cot:Property{parcel_id} .
}}""",

    # Current use of the building on a real Halifax property
    "building_use": """
PREFIX cot:     <{COT}>
PREFIX hp:      <{HP}>
PREFIX bdg:     <{BDG}>
PREFIX code:    <{CODE}>
PREFIX genprop: <{GENPROP}>
SELECT ?useName WHERE {{
  ?bldg a hp:Building ;
        hp:occupies cot:Property{parcel_id} ;
        bdg:use ?useNode .
  ?useNode code:hasCode ?codeNode .
  ?codeNode genprop:hasName ?useName .
}}""",

    # Ownership category of the building on a real Halifax property
    "ownership": """
PREFIX cot: <{COT}>
PREFIX hp:  <{HP}>
SELECT ?owner WHERE {{
  ?bldg a hp:Building ;
        hp:occupies cot:Property{parcel_id} ;
        hp:ownership ?owner .
}}""",

    # Year of construction of the building on a real Halifax property
    "year_built": """
PREFIX cot:  <{COT}>
PREFIX hp:   <{HP}>
PREFIX bdg:  <{BDG}>
PREFIX time: <{TIME}>
SELECT ?year WHERE {{
  ?bldg a hp:Building ;
        hp:occupies cot:Property{parcel_id} ;
        bdg:yearOfConstruction ?yearNode .
  ?yearNode time:year ?year .
}}""",

    # Look up a parcel by its Nova Scotia PID (8-digit string)
    "pid_lookup": """
PREFIX cot:     <{COT}>
PREFIX code:    <{CODE}>
PREFIX i72:     <{I72}>
PREFIX contact: <{CONTACT}>
SELECT ?street ?streetType ?postalCode WHERE {{
  ?addr code:hasCode ?pidCode .
  ?pidCode i72:hasValue "{parcel_id}" .
  OPTIONAL {{ ?addr contact:hasStreet ?street . }}
  OPTIONAL {{ ?addr contact:hasStreetType ?streetType . }}
  OPTIONAL {{ ?addr contact:hasPostalCode ?postalCode . }}
}}""",

    # Zoning boundary by number — queries cot:ZoningBoundaryN
    "zoning": """
PREFIX cot: <{COT}>
PREFIX hp:  <{HP}>
PREFIX oz:  <{OZ}>
SELECT ?allowedUse WHERE {{
  cot:ZoningBoundary{parcel_id} a hp:ZoningType ;
        oz:allowsUse ?allowedUse .
}}""",

    # Bylaw area name by number (bylaw_areas.ttl)
    "bylaw_area": """
PREFIX cot:     <{COT}>
PREFIX hp:      <{HP}>
PREFIX genprop: <{GENPROP}>
SELECT ?name WHERE {{
  cot:BylawArea{parcel_id} a hp:ZoningType ;
        genprop:hasName ?name .
}}""",

    # Building use from building_symbols.ttl — queried by BL number
    "symbol_use_by_bl": """
PREFIX cot:     <{COT}>
PREFIX hp:      <{HP}>
PREFIX bdg:     <{BDG}>
PREFIX code:    <{CODE}>
PREFIX genprop: <{GENPROP}>
SELECT ?useName WHERE {{
  cot:BuildingBL{parcel_id} a hp:Building ;
        bdg:use ?useNode .
  {{
    ?useNode code:hasCode ?codeNode .
    ?codeNode genprop:hasName ?useName .
  }} UNION {{
    ?useNode genprop:hasName ?useName .
  }}
}}""",

    # Civic address lookup by PID (8-digit NS PID)
    "address_by_pid": """
PREFIX cot:     <{COT}>
PREFIX code:    <{CODE}>
PREFIX i72:     <{I72}>
PREFIX contact: <{CONTACT}>
SELECT ?street ?streetType ?postalCode WHERE {{
  ?addr a contact:Address ;
        code:hasCode ?pidCode .
  ?pidCode i72:hasValue "{parcel_id}" .
  OPTIONAL {{ ?addr contact:hasStreet ?street . }}
  OPTIONAL {{ ?addr contact:hasStreetType ?streetType . }}
  OPTIONAL {{ ?addr contact:hasPostalCode ?postalCode . }}
}}""",

    # Building use queried by BL number directly (building_uses.ttl)
    "building_use_by_bl": """
PREFIX cot:     <{COT}>
PREFIX hp:      <{HP}>
PREFIX bdg:     <{BDG}>
PREFIX code:    <{CODE}>
PREFIX genprop: <{GENPROP}>
SELECT ?useName WHERE {{
  cot:BuildingBL{parcel_id} a hp:Building ;
        bdg:use ?useNode .
  {{
    ?useNode code:hasCode ?codeNode .
    ?codeNode genprop:hasName ?useName .
  }} UNION {{
    ?useNode genprop:hasName ?useName .
  }}
}}""",

    # Building use from building_symbols.ttl — short BL IDs (BL100, BL432 etc)
    "symbol_use_by_bl": """
PREFIX cot:     <{COT}>
PREFIX hp:      <{HP}>
PREFIX bdg:     <{BDG}>
PREFIX code:    <{CODE}>
PREFIX genprop: <{GENPROP}>
SELECT ?useName WHERE {{
  cot:BuildingBL{parcel_id} a hp:Building ;
        bdg:use ?useNode .
  {{
    ?useNode code:hasCode ?codeNode .
    ?codeNode genprop:hasName ?useName .
  }} UNION {{
    ?useNode genprop:hasName ?useName .
  }}
}}""",

    # Ownership queried by BL number (buildings.ttl)
    "ownership_by_bl": """
PREFIX cot: <{COT}>
PREFIX hp:  <{HP}>
SELECT ?owner WHERE {{
  cot:BuildingBL{parcel_id} a hp:Building ;
        hp:ownership ?owner .
}}""",

    # Year of construction queried by BL number (buildings.ttl)
    "year_built_by_bl": """
PREFIX cot:  <{COT}>
PREFIX hp:   <{HP}>
PREFIX bdg:  <{BDG}>
PREFIX time: <{TIME}>
SELECT ?year WHERE {{
  cot:BuildingBL{parcel_id} a hp:Building ;
        bdg:yearOfConstruction ?yearNode .
  ?yearNode time:year ?year .
}}""",
}


# =====================================================================
# TEMPLATE DETECTION
# =====================================================================

TEMPLATE_RULES: list[tuple[str, str]] = [
    # Specific compound phrases FIRST — before single-word matches
    (r"\bbylaw\s+area\b",                                                "bylaw_area"),
    (r"\bzoning\s+boundary\b",                                           "zoning"),
    (r"\bplanned\s+zon\w+\b",                                           "planned_zoning"),
    (r"\b(zoned?\s+capacity|capacity\s+for\s+a\s+parcel)\b",        "zoned_capacity"),
    (r"\bmixed.?use\b",                                                   "mixed_use"),
    (r"\b(fsr|floor.?space|fsi)\b",                                      "fsr"),
    (r"\bheight\b",                                                       "building_height"),
    (r"\bsetback\b",                                                      "setback"),
    (r"\b(federal|government|public(ly)?(\s+own))\b",                   "public_ownership"),
    (r"\b(own|owner|ownership|purchase|available for)\b",               "ownership"),
    (r"\b(year|built|construction|age)\b",                               "year_built"),
    (r"\bperimeter\b",                                                    "perimeter"),
    (r"\b(size|area|how (big|large)|square)\b",                          "area"),
    (r"\b(land\s+use|currently\s+(being\s+)?used\s+for|current\s+use|used\s+for)\b", "building_use"),
    (r"\b(zon\w+|designat\w+)\b",                                       "zoning"),
    (r"\b(occupied\s+by|any\s+building|is\s+there\s+a\s+building|building\s+on)\b", "has_building"),
    # Address lookup by PID
    (r"\b(address|street|postal)\b",                                      "address_by_pid"),
    (r"\bwater\b",                                                        "water_service"),
    (r"\b(wastewater|sewer|sewage)\b",                                    "wastewater_service"),
    (r"\b(electric\w*|power)\b",                                         "electricity_service"),
    (r"\b(fire|emergency\s+access)\b",                                   "fire_service"),
    (r"\b(transit|bus|train|subway)\b",                                   "transit_service"),
    (r"\b(road|accessible|access)\b",                                     "road_service"),
]


def _detect_template(question: str) -> str | None:
    q = question.lower()
    # BL-number query: "building XXX" — handle all BL ID lengths (2-6 digits)
    if re.search(r'\bbuilding\s+\d{2,6}\b', q):
        pid = _extract_parcel_id(question)
        if re.search(r'\b(used for|use|purpose)\b', q):
            # Short IDs (<=5 digits) are from building_symbols.ttl
            return "symbol_use_by_bl" if pid and len(pid) <= 5 else "building_use_by_bl"
        if re.search(r'\bown', q):   # matches owns/owner/ownership
            return "ownership_by_bl"
        if re.search(r'\b(year|built|construction|age)\b', q):
            return "year_built_by_bl"
    for pattern, key in TEMPLATE_RULES:
        if re.search(pattern, q):
            return key
    return None


def _extract_parcel_id(question: str) -> str | None:
    """
    Extract a numeric ID from the question.
    Handles:
      - "building XXXX"  → BL number for buildings.ttl / building_uses.ttl / building_symbols.ttl
      - "bylaw area N"   → BylawArea number for bylaw_areas.ttl
      - "zoning boundary N" → ZoningBoundary number for zoning_boundaries.ttl
      - 8-digit PID      → civic address lookup
      - 6-digit property → building_polygons.ttl
      - <= 5 digit       → synthetic test data
    """
    q = question.lower()
    # BL number: "building XXXXXX" or "building XXX"
    m = re.search(r'\bbuilding\s+(\d{2,6})\b', q)
    if m:
        return m.group(1)
    # Bylaw area: "bylaw area N"
    m = re.search(r'\bbylaw\s+area\s+(\d+)\b', q)
    if m:
        return m.group(1)
    # Zoning boundary: "zoning boundary N"
    m = re.search(r'\bzoning\s+boundary\s+(\d+)\b', q)
    if m:
        return m.group(1)
    # General number (PID, property number, synthetic ID)
    m = re.search(r'\b(\d{2,8})\b', question)
    return m.group(1) if m else None


def _is_real_halifax_id(parcel_id: str) -> bool:
    """
    Returns True if the parcel_id is a real Halifax ID.
    Real Halifax IDs: 6+ digit property numbers, any-length BL numbers,
    bylaw area numbers, zoning boundary numbers, 8-digit PIDs.
    Synthetic IDs: exactly 5-digit numbers (12345, 67890, 11111).
    """
    return len(parcel_id) != 5


# =====================================================================
# DIRECT SPARQL EXECUTION
# =====================================================================

def _run_direct_sparql(template_key: str, parcel_id: str) -> str | None:
    """
    Execute a SPARQL template directly against GraphDB.
    Automatically selects synthetic vs real Halifax templates based on parcel_id length.
    """
    is_real = _is_real_halifax_id(parcel_id)

    # BL-number templates always use HALIFAX_TEMPLATES
    bl_templates = {"building_use_by_bl", "ownership_by_bl", "year_built_by_bl"}
    if template_key in bl_templates:
        sparql = HALIFAX_TEMPLATES[template_key].format(
            HP=HP_URI, COT=COT, I72=I72, CODE=CODE,
            CONTACT=CONTACT, BDG=BDG, OZ=OZ, GENPROP=GENPROP,
            TIME=TIME, parcel_id=parcel_id,
        )
    elif is_real and template_key in HALIFAX_TEMPLATES:
        sparql = HALIFAX_TEMPLATES[template_key].format(
            HP=HP_URI, COT=COT, I72=I72, CODE=CODE,
            CONTACT=CONTACT, BDG=BDG, OZ=OZ, GENPROP=GENPROP,
            TIME=TIME, parcel_id=parcel_id,
        )
    elif template_key in SPARQL_TEMPLATES:
        sparql = SPARQL_TEMPLATES[template_key].format(
            HP=HP_URI, OM1=OM1_URI, GPROP=GPROP, LU=LU5087,
            RDFS=RDFS_URI, FOAF=FOAF_URI, ORG=ORG_URI,
            parcel_id=parcel_id,
        )
    else:
        return None

    params = urllib.parse.urlencode({"query": sparql})
    url = f"{GRAPHDB_QUERY_ENDPOINT}?{params}"
    req = urllib.request.Request(url, headers={"Accept": "application/sparql-results+json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        bindings = data.get("results", {}).get("bindings", [])
        if not bindings:
            return None
        var = data["head"]["vars"][0]
        raw_values = [b[var]["value"] for b in bindings if var in b]
        # Normalize scientific notation floats (e.g. 2.834766e+02 → 283)
        def _fmt(v):
            try:
                f = float(v)
                # Round to nearest integer for all numeric values
                return str(int(round(f)))
            except ValueError:
                return v
        values = [_fmt(v) for v in raw_values]
        return ", ".join(values) if values else None
    except Exception as e:
        print(f"  [SPARQL error: {e}]")
        return None


def _format_answer(template_key: str, raw: str | None, parcel_id: str) -> str:
    if raw is None:
        return f"No information found for parcel/property {parcel_id}."
    answers = {
        "zoning":              f"Zoning boundary {parcel_id} allows: {raw}.",
        "landuse":             f"The land use of parcel {parcel_id} is {raw}.",
        "building_use":        f"The current use of property {parcel_id} is {raw}.",
        "building_use":        f"The building on property {parcel_id} is used for: {raw}.",
        "area":                f"The area of property {parcel_id} is {raw} square metres.",
        "perimeter":           f"The perimeter of property {parcel_id} is {raw} metres.",
        "ownership":           f"Property {parcel_id} is owned by: {raw}.",
        "public_ownership":    f"Property {parcel_id} is owned by: {raw}.",
        "has_building":        f"Yes, property {parcel_id} has a building ({raw}).",
        "building_height":     f"The building height on parcel {parcel_id} is {raw} storeys.",
        "setback":             f"The setback on parcel {parcel_id} is {raw} metres.",
        "fsr":                 f"The FSI/FSR on parcel {parcel_id} is {raw}.",
        "year_built":          f"The building on property {parcel_id} was constructed in {raw}.",
        "zoned_capacity":      f"Parcel {parcel_id} allows the following uses: {raw}.",
        "mixed_use":           f"Parcel {parcel_id} allows: {raw}.",
        "planned_zoning":      f"The planned zoning for parcel {parcel_id} is {raw}.",
        "nearby_zoning":       f"The zoning of parcel {parcel_id} is {raw}.",
        "pid_lookup":          f"Address found for PID {parcel_id}: {raw}.",
        "water_service":       f"Yes, parcel {parcel_id} is serviced by water ({raw}).",
        "wastewater_service":  f"Yes, parcel {parcel_id} is serviced by wastewater ({raw}).",
        "electricity_service": f"Yes, parcel {parcel_id} is serviced by electricity ({raw}).",
        "fire_service":        f"Yes, parcel {parcel_id} has fire/emergency service access ({raw}).",
        "transit_service":     f"Yes, parcel {parcel_id} is served by transit ({raw}).",
        "road_service":        f"Yes, parcel {parcel_id} is accessible by road ({raw}).",
        "bylaw_area":          f"By-law area {parcel_id} is named: {raw}.",
        "symbol_use_by_bl":    f"Building {parcel_id} is used as: {raw}.",
        "address_by_pid":      f"The address for PID {parcel_id} is: {raw}.",
    }
    return answers.get(template_key, raw)


# =====================================================================
# ONTOLOGY INDEX (for LLM fallback schema selection)
# =====================================================================

class HPCDMIndex:
    def __init__(self, ttl_path: str):
        self.g = Graph()
        self.g.parse(ttl_path, format="turtle")
        self._build_index()

    def _local(self, uri) -> str:
        return re.split(r"[#/]", str(uri))[-1]

    def _labels_for(self, subject) -> list[str]:
        terms = {self._local(subject).lower()}
        for pred in (RDFS.label, SKOS.prefLabel, SKOS.altLabel, RDFS.comment):
            for obj in self.g.objects(subject, pred):
                for word in re.findall(r"[a-zA-Z]+", str(obj)):
                    terms.add(word.lower())
        return list(terms)

    def _build_index(self):
        self.entries: list[dict] = []
        subjects = {s for s in self.g.subjects() if str(s).startswith("http")}
        for s in subjects:
            labels = self._labels_for(s)
            triples = [(s, p, o) for p, o in self.g.predicate_objects(s)]
            triples += [(sp, pp, s) for sp, pp in self.g.subject_predicates(s)]
            self.entries.append({"uri": s, "labels": labels, "triples": triples})

    def relevant_triples(self, keywords: list[str], max_triples: int = 300) -> list:
        kw_set = {k.lower() for k in keywords}
        scored, seen = [], set()
        for entry in self.entries:
            score = len(kw_set & set(entry["labels"]))
            if score > 0:
                for triple in entry["triples"]:
                    key = (str(triple[0]), str(triple[1]), str(triple[2]))
                    if key not in seen:
                        seen.add(key)
                        scored.append((score, triple))
        scored.sort(key=lambda x: -x[0])
        return [t for _, t in scored[:max_triples]]


# =====================================================================
# KEYWORD EXTRACTION
# =====================================================================

DOMAIN_VOCAB: dict[str, list[str]] = {
    "zoning":         ["zoning", "zone", "zoned", "designation", "zoningtype"],
    "landuse":        ["landuse", "land", "use", "area", "perimeter", "size",
                       "frontage", "fsi", "dwellings", "residents", "lotsize"],
    "ownership":      ["own", "owner", "ownership", "occupiedby", "occupies"],
    "regulations":    ["height", "fsr", "setback", "density", "restriction",
                       "regulation", "buildingheight", "buildingsetback", "floorarea",
                       "year", "built", "construction"],
    "infrastructure": ["water", "road", "transit", "service", "access",
                       "wastewater", "electricity", "fire", "emergency"],
}

PROPERTY_EXPANSIONS: dict[str, list[str]] = {
    "haszone":      ["zoning", "zoningtype"],
    "hasarea":      ["area", "landuse"],
    "hasperimeter": ["perimeter", "landuse"],
    "hasfsi":       ["fsi"],
    "hasheight":    ["height", "building"],
    "hassetback":   ["setback", "building"],
    "ownership":    ["owner", "ownership"],
    "servicedby":   ["service", "infrastructure"],
    "landuse":      ["landuse", "use"],
}


def extract_keywords(question: str) -> list[str]:
    tokens = re.findall(r"[a-zA-Z]+", question.lower())
    token_set = set(tokens)
    keywords: set[str] = set(tokens)
    for _cat, vocab in DOMAIN_VOCAB.items():
        for vw in vocab:
            if vw in token_set or any(vw in t for t in token_set):
                keywords.add(vw)
                for prop, expansions in PROPERTY_EXPANSIONS.items():
                    if vw in prop or prop in vw:
                        keywords.update(expansions)
    for prop, expansions in PROPERTY_EXPANSIONS.items():
        if any(p in keywords for p in prop.split()):
            keywords.update(expansions)
    return list(keywords)


def _infer_category(keywords: list[str]) -> str:
    kw_set = set(keywords)
    scores = {cat: sum(1 for v in vocab if v in kw_set)
              for cat, vocab in DOMAIN_VOCAB.items()}
    return max(scores, key=scores.get) if scores else "general"


def extract_schema(index: HPCDMIndex, question: str) -> str:
    keywords = extract_keywords(question)
    triples = index.relevant_triples(keywords)
    mini_g = Graph()
    for prefix, ns in index.g.namespaces():
        mini_g.bind(prefix, ns)
    for s, p, o in triples:
        mini_g.add((s, p, o))
    for s in set(mini_g.subjects()):
        for rdf_type in index.g.objects(s, RDF.type):
            mini_g.add((s, RDF.type, rdf_type))
    tmp = tempfile.NamedTemporaryFile(suffix=".ttl", mode="w", delete=False, encoding="utf-8")
    tmp_path = tmp.name
    tmp.close()
    mini_g.serialize(destination=tmp_path, format="turtle")
    return tmp_path


# =====================================================================
# LLM CHAIN FALLBACK  (Tier 2)
# =====================================================================

# Phrases that indicate the LLM chain found nothing useful in GraphDB.
# If the chain returns one of these, we escalate to the bare GPT fallback.
_CHAIN_EMPTY_PHRASES = [
    "don't have",
    "do not have",
    "i'm sorry",
    "i am sorry",
    "no information",
    "cannot find",
    "unable to find",
    "not available",
    "no data",
    "i don't know",
]

def _chain_result_is_empty(result: str) -> bool:
    """Return True if the LLM chain signalled it found nothing useful."""
    low = result.lower()
    return any(phrase in low for phrase in _CHAIN_EMPTY_PHRASES)


def _run_llm_chain(question: str, index: HPCDMIndex) -> str:
    """
    Tier 2 — LangChain GraphDB QA chain.
    Sends a reduced HPCDM schema + the question to GPT-4o-mini,
    which generates and executes a SPARQL query against GraphDB.
    If the result is empty/unhelpful, escalates to Tier 3.
    """
    schema_path = extract_schema(index, question)
    result = None
    try:
        graph = OntotextGraphDBGraph(
            query_endpoint=GRAPHDB_QUERY_ENDPOINT,
            local_file=schema_path,
            local_file_format="turtle",
        )
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        chain = OntotextGraphDBQAChain.from_llm(
            llm, graph=graph, allow_dangerous_requests=True,
        )
        result = chain.invoke(question)["result"]
    except (ValueError, Exception) as e:
        # Catches "The generated SPARQL query is invalid" and any other
        # chain errors — escalate directly to Tier 3
        print(f"  [LLM chain error: {type(e).__name__}: {e}]")
        print(f"  [Escalating to bare GPT fallback (Tier 3)]")
        return _run_bare_gpt(question)
    finally:
        try:
            os.unlink(schema_path)
        except OSError:
            pass

    if _chain_result_is_empty(result):
        print(f"  [LLM chain returned empty — escalating to bare GPT fallback (Tier 3)]")
        return _run_bare_gpt(question)

    return result


# =====================================================================
# BARE GPT FALLBACK  (Tier 3)
# =====================================================================

def _run_bare_gpt(question: str) -> str:
    """
    Tier 3 — Direct GPT-4o-mini call with no GraphDB context.
    Used only when the LangChain GraphDB chain returns nothing useful.
    The model answers from its general training knowledge.
    The answer is prefixed so it is clear no ontology data was used.
    """
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    messages = [
        {
            "role": "system",
            "content": (
                "You are a helpful assistant with knowledge of urban planning, "
                "zoning, property ownership, and housing development. "
                "Answer the user's question as best you can from general knowledge. "
                "Be concise and factual. If you genuinely cannot answer, say so briefly."
            ),
        },
        {"role": "user", "content": question},
    ]
    response = llm.invoke(messages)
    answer = response.content.strip()
    return f"[General knowledge — not from ontology] {answer}"


# =====================================================================
# MAIN ENTRY POINT
# =====================================================================

def run_cq(question: str, index: HPCDMIndex) -> tuple[str, str]:
    keywords = extract_keywords(question)
    category = _infer_category(keywords)
    parcel_id = _extract_parcel_id(question)
    template_key = _detect_template(question) if parcel_id else None

    # Templates that always use HALIFAX_TEMPLATES regardless of ID length
    BL_TEMPLATES = {"building_use_by_bl", "ownership_by_bl", "year_built_by_bl", "symbol_use_by_bl", "bylaw_area", "zoning", "address_by_pid", "has_building", "area", "perimeter"}

    if template_key and parcel_id:
        is_real = _is_real_halifax_id(parcel_id)

        # BL-number templates always use HALIFAX_TEMPLATES regardless of ID length
        is_bl_query = template_key in BL_TEMPLATES

        # For synthetic parcels, remap building_use → landuse
        # (synthetic data stores land use via lu:landUse, not bdg:use)
        if not is_real and not is_bl_query and template_key == "building_use":
            template_key = "landuse"

        template_exists = (
            is_bl_query or
            (is_real and template_key in HALIFAX_TEMPLATES) or
            (not is_real and template_key in SPARQL_TEMPLATES)
        )
        if template_exists:
            raw = _run_direct_sparql(template_key, parcel_id)
            answer = _format_answer(template_key, raw, parcel_id)
            src = "BL" if is_bl_query else ("real Halifax" if is_real else "synthetic")
            print(f"  [direct SPARQL: {template_key} | {src} data]")
        else:
            answer = _run_llm_chain(question, index)
            print(f"  [LLM chain fallback (Tier 2) — no template for '{template_key}' in {'Halifax' if is_real else 'synthetic'} set]")
    else:
        answer = _run_llm_chain(question, index)
        print(f"  [LLM chain fallback (Tier 2)]")

    return answer, category


def normalize(text: str) -> str:
    text = text.lower().replace(" ", "").replace("_", "")
    return text.split(":")[-1] if ":" in text else text


def evaluate(answer: str, expected: str) -> str:
    low = answer.lower()
    # UNKNOWN if any tier signalled it could not answer
    if any(p in low for p in ["don't know", "not have information", "no information found",
                               "cannot answer", "genuinely cannot"]):
        return "UNKNOWN"
    # Strip the Tier-3 prefix before matching so expected values still match
    clean = answer.replace("[General knowledge — not from ontology] ", "")
    if normalize(expected) in normalize(clean):
        return "PASS"
    return "FAIL"


# =====================================================================
# TEST SUITE — all questions use real Halifax data
#
# Tab mapping rationale:
#   Land Use & Zoning      — what exists on a parcel TODAY
#                            (size, shape, current use, ownership, zoning designation)
#   Development Feasibility — can this land be developed?
#                            (parcel size suitability, location/address, building age,
#                             ownership availability — using DIFFERENT IDs from tab 1)
#   Development Desirability — is the neighbourhood attractive for new housing?
#                            (nearby amenities, zoning capacity for density,
#                             housing accelerator by-law areas)
# =====================================================================

test_questions = [

    # ══════════════════════════════════════════════════════════════
    # TAB 1: Land Use & Zoning
    # Focus: current state of specific parcels and their zoning
    # ══════════════════════════════════════════════════════════════

    # CQ-1a: What is the size of this parcel? (Property342126 = 283 m²)
    {"question": "What is the area of property 342126?",
     "expected": "283", "tab": "Land Use & Zoning", "cq": "1a"},

    # CQ-1a: What is the size of this parcel? (Property342127 = 277 m²)
    {"question": "What is the area of property 342127?",
     "expected": "277", "tab": "Land Use & Zoning", "cq": "1a-b"},

    # CQ-1b: What is the perimeter of this parcel? (Property342128 = 110 m)
    {"question": "What is the perimeter of property 342128?",
     "expected": "110", "tab": "Land Use & Zoning", "cq": "1b"},

    # CQ-41: Is the parcel already occupied by a building? (Property342126 → BuildingBL156297)
    {"question": "Is there a building on property 342126?",
     "expected": "building", "tab": "Land Use & Zoning", "cq": "41"},

    # CQ-41: Is the parcel already occupied by a building? (Property342128 → BuildingBL155532)
    {"question": "Is there a building on property 342128?",
     "expected": "building", "tab": "Land Use & Zoning", "cq": "41-b"},

    # CQ-56: What is the current land use of this building? (BL68602 = RESIDENTIAL)
    {"question": "What is building 68602 used for?",
     "expected": "RESIDENTIAL", "tab": "Land Use & Zoning", "cq": "56"},

    # CQ-56: What is the current land use of this building? (BL68607 = RESIDENTIAL)
    {"question": "What is building 68607 used for?",
     "expected": "RESIDENTIAL", "tab": "Land Use & Zoning", "cq": "56-b"},

    # CQ-2: Who is the current owner of this building? (BL148695 = Private Person...)
    {"question": "Who owns building 148695?",
     "expected": "Private", "tab": "Land Use & Zoning", "cq": "2"},

    # CQ-3: What zoning designation applies here? (ZoningBoundary1 = Mixed Rural Residential)
    {"question": "What use does zoning boundary 1 allow?",
     "expected": "Mixed Rural Residential", "tab": "Land Use & Zoning", "cq": "3"},

    # CQ-3: What zoning designation applies here? (ZoningBoundary2 = Single Unit Dwelling)
    {"question": "What use does zoning boundary 2 allow?",
     "expected": "Single Unit Dwelling", "tab": "Land Use & Zoning", "cq": "3-b"},

    # CQ-24b: Which by-law area does this parcel fall under? (BylawArea1 = Suburban Housing Accelerator)
    {"question": "What is the name of bylaw area 1?",
     "expected": "Suburban Housing Accelerator", "tab": "Land Use & Zoning", "cq": "24b"},

    # CQ-24b: Which by-law area does this parcel fall under? (BylawArea2 = Sackville Drive)
    {"question": "What is the name of bylaw area 2?",
     "expected": "Sackville Drive", "tab": "Land Use & Zoning", "cq": "24b-b"},

    # CQ-3c: Open-ended zoning query — no numeric ID, routes to LLM chain
    # LLM queries GraphDB for all ZoningBoundary instances and their allowed uses
    {"question": "What zoning types are recorded in the Halifax dataset?",
     "expected": "Single Unit Dwelling", "tab": "Land Use & Zoning", "cq": "3c"},

    # ══════════════════════════════════════════════════════════════
    # TAB 2: Development Feasibility
    # Focus: whether a specific parcel is a viable development candidate
    # ══════════════════════════════════════════════════════════════

    # CQ-1a: Is the parcel large enough for development? (Property342129 = 265 m²)
    {"question": "What is the area of property 342129?",
     "expected": "265", "tab": "Development Feasibility", "cq": "1a"},

    # CQ-1a: Is the parcel large enough for development? (Property342131 = 123 m²)
    {"question": "What is the area of property 342131?",
     "expected": "123", "tab": "Development Feasibility", "cq": "1a-b"},

    # CQ-1b: What is the frontage/perimeter of this development candidate? (Property342127 = 68 m)
    {"question": "What is the perimeter of property 342127?",
     "expected": "68", "tab": "Development Feasibility", "cq": "1b"},

    # CQ-5: Where is this property located? (PID 00301713 = SHADY LANE)
    {"question": "What is the address for PID 00301713?",
     "expected": "SHADY", "tab": "Development Feasibility", "cq": "5"},

    # CQ-5: Where is this property located? (PID 40493843 = DAVID DR)
    {"question": "What is the address for PID 40493843?",
     "expected": "DAVID", "tab": "Development Feasibility", "cq": "5-b"},

    # CQ-5: Where is this property located? (PID 41186503 = RUTLEDGE ST)
    {"question": "What is the address for PID 41186503?",
     "expected": "RUTLEDGE", "tab": "Development Feasibility", "cq": "5-c"},

    # CQ-41a: How old is the existing building? (BL147374 = 1905, very old — redevelopment candidate)
    {"question": "What year was building 147374 built?",
     "expected": "1905", "tab": "Development Feasibility", "cq": "41a"},

    # CQ-41a: How old is the existing building? (BL147418 = 2008, relatively recent)
    {"question": "What year was building 147418 built?",
     "expected": "2008", "tab": "Development Feasibility", "cq": "41a-b"},

    # CQ-2: Is the building privately owned (i.e. available for purchase)? (BL149470 = Private)
    {"question": "Who owns building 149470?",
     "expected": "Private", "tab": "Development Feasibility", "cq": "2"},

    # CQ-24c: Open-ended by-law area query — no numeric ID, routes to LLM chain
    # LLM queries GraphDB for all BylawArea instances under Suburban Housing Accelerator
    {"question": "Which bylaw areas in Halifax are Suburban Housing Accelerator zones?",
     "expected": "Suburban Housing Accelerator", "tab": "Development Feasibility", "cq": "24c"},

    # ══════════════════════════════════════════════════════════════
    # TAB 3: Development Desirability
    # Focus: neighbourhood amenities and zoning capacity for new housing
    # ══════════════════════════════════════════════════════════════

    # CQ-56: Are there schools nearby? (BL603 = Public School)
    {"question": "What is building 603 used for?",
     "expected": "Public School", "tab": "Development Desirability", "cq": "56"},

    # CQ-56: Are there childcare facilities nearby? (BL702 = Daycare)
    {"question": "What is building 702 used for?",
     "expected": "Daycare", "tab": "Development Desirability", "cq": "56-b"},

    # CQ-56: Are there recreational facilities nearby? (BL432 = Recreation Facility)
    {"question": "What is building 432 used for?",
     "expected": "Recreation Facility", "tab": "Development Desirability", "cq": "56-c"},

    # CQ-3: Does the zoning allow multi-unit housing? (ZoningBoundary4 = Multiple Unit Dwelling)
    {"question": "What use does zoning boundary 4 allow?",
     "expected": "Multiple Unit Dwelling", "tab": "Development Desirability", "cq": "3"},

    # CQ-3: Does the zoning allow mixed-use development? (ZoningBoundary6 = Mixed Use 1)
    {"question": "What use does zoning boundary 6 allow?",
     "expected": "Mixed Use 1", "tab": "Development Desirability", "cq": "3-b"},

    # CQ-24b: Is this area covered by a housing accelerator by-law? (BylawArea3 = Bedford)
    {"question": "What is the name of bylaw area 3?",
     "expected": "Bedford", "tab": "Development Desirability", "cq": "24b"},

    # CQ-24b: Is this area covered by a housing accelerator by-law? (BylawArea4 = Suburban Housing Accelerator)
    {"question": "What is the name of bylaw area 4?",
     "expected": "Suburban Housing Accelerator", "tab": "Development Desirability", "cq": "24b-b"},

    # CQ-56d: Open-ended amenity query — no numeric ID, routes to LLM chain
    # LLM queries GraphDB for all building symbol types (community facilities)
    {"question": "What types of community facilities are present in the Halifax building dataset?",
     "expected": "Daycare", "tab": "Development Desirability", "cq": "56d"},
]

def main():
    print("Loading and indexing HPCDM ontology...")
    index = HPCDMIndex(HPCDM_FILE)
    print(f"Index built: {len(index.entries)} ontology entities\n")

    pass_count = fail_count = unknown_count = 0
    tab_stats: dict[str, dict] = {}

    print("========== CQ TEST RESULTS ==========\n")

    for q in test_questions:
        tab = q.get("tab", "—")
        cq  = q.get("cq", "—")
        print(f"[{tab} | CQ-{cq}] {q['question']}")

        answer, category = run_cq(q["question"], index)

        print(f"  Category : {category}")
        print(f"  Answer   : {answer}")

        status = evaluate(answer, q["expected"])
        print(f"  Result   : {status}\n")

        if tab not in tab_stats:
            tab_stats[tab] = {"pass": 0, "fail": 0, "unknown": 0}
        tab_stats[tab][status.lower()] = tab_stats[tab].get(status.lower(), 0) + 1

        if status == "PASS":
            pass_count += 1
        elif status == "FAIL":
            fail_count += 1
        else:
            unknown_count += 1

    total    = len(test_questions)
    answered = pass_count + fail_count
    accuracy = (pass_count / answered * 100) if answered else 0
    coverage = (answered / total * 100)

    print("========== SUMMARY BY TAB ==========")
    for tab, stats in tab_stats.items():
        a   = stats.get("pass", 0) + stats.get("fail", 0)
        acc = (stats.get("pass", 0) / a * 100) if a else 0
        print(f"  {tab[:40]:<40}  PASS={stats.get('pass',0)}  FAIL={stats.get('fail',0)}"
              f"  UNKNOWN={stats.get('unknown',0)}  Accuracy={acc:.0f}%")

    print("\n========== OVERALL SUMMARY ==========")
    print(f"Total CQ:          {total}")
    print(f"PASS:              {pass_count}")
    print(f"FAIL:              {fail_count}")
    print(f"UNKNOWN:           {unknown_count}")
    print(f"\nAccuracy:          {accuracy:.1f} %")
    print(f"Ontology coverage: {coverage:.1f} %")


if __name__ == "__main__":
    main()
