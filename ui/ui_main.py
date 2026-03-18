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
    (r"\b(size|area|how (big|large)|square)\b",                          "area"),
    (r"\bperimeter\b",                                                    "perimeter"),
    (r"\b(year|built|construction|age)\b",                               "year_built"),
    (r"\b(federal|government|public(ly)?(\s+own))\b",                   "public_ownership"),
    (r"\b(own|owner|ownership|purchase|available for)\b",               "ownership"),
    (r"\bsetback\b",                                                      "setback"),
    (r"\bheight\b",                                                       "building_height"),
    (r"\b(fsr|floor.?space|fsi)\b",                                      "fsr"),
    (r"\bmixed.?use\b",                                                   "mixed_use"),
    (r"\b(zoned?\s+capacity|capacity\s+for\s+a\s+parcel)\b",        "zoned_capacity"),
    (r"\bplanned\s+zon\w+\b",                                           "planned_zoning"),
    # Bylaw area name — must come before general zoning
    (r"\bbylaw\s+area\b",                                                "bylaw_area"),
    # Zoning boundary allowed use
    (r"\bzoning\s+boundary\b",                                           "zoning"),
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
    # Check if this is a BL-number query (e.g. "building 68602")
    if re.search(r'\bbuilding\s+\d{4,6}\b', q):
        if re.search(r'\b(used for|use|purpose)\b', q):
            return "building_use_by_bl"
        if re.search(r'\b(own|owner|ownership)\b', q):
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
    Returns True if the parcel_id looks like a real Halifax ID (6+ digits).
    Synthetic test IDs are 5 digits or fewer (e.g. 12345).
    """
    return len(parcel_id) >= 6


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
                # If it's a whole number display as int, else 2dp
                return str(int(round(f))) if f == round(f) else f"{f:.2f}"
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
# LLM CHAIN FALLBACK
# =====================================================================

def _run_llm_chain(question: str, index: HPCDMIndex, messages: list[dir]) -> tuple[str, list[dict]]:
    #Will first try for hardcoded CQ lookup
    from main_agent.agent_logic import prompt_agent
    messages, answered_cq = prompt_agent(messages) #tries to obtain hardcoded cq response

    schema_path = extract_schema(index, question)
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
        return chain.invoke(question)["result"], messages
    finally:
        try:
            os.unlink(schema_path)
        except OSError:
            pass

# =====================================================================
# MAIN ENTRY POINT
# =====================================================================

def run_cq(question: str, index: HPCDMIndex, messages: list[dict]) -> tuple[str, str, list[dict]]:
    keywords = extract_keywords(question)
    category = _infer_category(keywords)
    parcel_id = _extract_parcel_id(question)
    template_key = _detect_template(question) if parcel_id else None

    # Templates that always use HALIFAX_TEMPLATES regardless of ID length
    BL_TEMPLATES = {"building_use_by_bl", "ownership_by_bl", "year_built_by_bl", "symbol_use_by_bl", "bylaw_area", "zoning", "address_by_pid"}

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

            #Updates messages chain
            messages.append({
                "role":     "assistant",
                "content":     answer,
                "category": category,
                "method":   method,
            })

        else:
            messages = _run_llm_chain(question, index, messages)
            print(f"  [LLM chain fallback — no template for '{template_key}' in {'Halifax' if is_real else 'synthetic'} set]")
    else:
        messages = _run_llm_chain(question, index, messages)
        print(f"  [LLM chain fallback]")

    return messages, category


def normalize(text: str) -> str:
    text = text.lower().replace(" ", "").replace("_", "")
    return text.split(":")[-1] if ":" in text else text


def evaluate(answer: str, expected: str) -> str:
    low = answer.lower()
    if "don't know" in low or "not have information" in low or "no information found" in low:
        return "UNKNOWN"
    if normalize(expected) in normalize(answer):
        return "PASS"
    return "FAIL"


# =====================================================================
# TEST SUITE  —  all questions use real Halifax data
# =====================================================================

test_questions = [

    # ── Tab 1: Land Use & Zoning ─────────────────────────────────────

    # CQ-1a: Parcel area  (building_polygons.ttl — Property342126 = 283 m²)
    {"question": "What is the area of property 342126?",
     "expected": "283", "tab": "Land Use & Zoning", "cq": "1a"},

    # CQ-1a: Parcel area  (Property342128 = 481 m²)
    {"question": "What is the area of property 342128?",
     "expected": "481", "tab": "Land Use & Zoning", "cq": "1a-b"},

    # CQ-1b: Parcel perimeter  (Property342126 = 72 m)
    {"question": "What is the perimeter of property 342126?",
     "expected": "72", "tab": "Land Use & Zoning", "cq": "1b"},

    # CQ-1b: Parcel perimeter  (Property342131 = 44 m)
    {"question": "What is the perimeter of property 342131?",
     "expected": "44", "tab": "Land Use & Zoning", "cq": "1b-b"},

    # CQ-41: Is there a building on the parcel?  (Property342126 → BuildingBL156297)
    {"question": "Is there a building on property 342126?",
     "expected": "building", "tab": "Land Use & Zoning", "cq": "41"},

    # CQ-41: Is there a building on the parcel?  (Property342128 → BuildingBL155532)
    {"question": "Is there a building on property 342128?",
     "expected": "building", "tab": "Land Use & Zoning", "cq": "41-b"},

    # CQ-56: Current use of building  (building_uses.ttl — BL68602 = RESIDENTIAL)
    {"question": "What is building 68602 used for?",
     "expected": "RESIDENTIAL", "tab": "Land Use & Zoning", "cq": "56"},

    # CQ-56: Current use of building  (BL68609 = RESIDENTIAL)
    {"question": "What is building 68609 used for?",
     "expected": "RESIDENTIAL", "tab": "Land Use & Zoning", "cq": "56-b"},

    # CQ-56: Building use from symbols  (building_symbols.ttl — BL100 = Place of Worship)
    {"question": "What is building 100 used for?",
     "expected": "Place of Worship", "tab": "Land Use & Zoning", "cq": "56-sym"},

    # CQ-56: Building use from symbols  (BL603 = Public School — via SY339)
    {"question": "What is building 603 used for?",
     "expected": "Public School", "tab": "Land Use & Zoning", "cq": "56-sym2"},

    # CQ-2: Building ownership  (buildings.ttl — BL147374 = Private Person...)
    {"question": "Who owns building 147374?",
     "expected": "Private", "tab": "Land Use & Zoning", "cq": "2"},

    # CQ-2: Building ownership  (BL149470 = Private Person...)
    {"question": "Who owns building 149470?",
     "expected": "Private", "tab": "Land Use & Zoning", "cq": "2-b"},

    # CQ-41a: Year of construction  (buildings.ttl — BL148647 = 1991)
    {"question": "What year was building 148647 built?",
     "expected": "1991", "tab": "Land Use & Zoning", "cq": "41a"},

    # CQ-41a: Year of construction  (BL148695 = 2019)
    {"question": "What year was building 148695 built?",
     "expected": "2019", "tab": "Land Use & Zoning", "cq": "41a-b"},

    # CQ-3 / zoning boundary: What use does ZoningBoundary2 allow?
    {"question": "What use does zoning boundary 2 allow?",
     "expected": "Single Unit Dwelling", "tab": "Land Use & Zoning", "cq": "3"},

    # CQ-3: ZoningBoundary6 = Mixed Use 1
    {"question": "What use does zoning boundary 6 allow?",
     "expected": "Mixed Use 1", "tab": "Land Use & Zoning", "cq": "3-b"},

    # CQ-24b / Bylaw area name  (bylaw_areas.ttl — BylawArea2 = Sackville Drive)
    {"question": "What is the name of bylaw area 2?",
     "expected": "Sackville Drive", "tab": "Land Use & Zoning", "cq": "24b"},

    # CQ-24b: BylawArea3 = Bedford
    {"question": "What is the name of bylaw area 3?",
     "expected": "Bedford", "tab": "Land Use & Zoning", "cq": "24b-b"},

    # ── Tab 2: Development Feasibility ──────────────────────────────

    # CQ-5: Civic address lookup by PID  (civic_addresses.ttl — PID 00301713 = SHADY LANE)
    {"question": "What is the address for PID 00301713?",
     "expected": "SHADY", "tab": "Development Feasibility", "cq": "5"},

    # CQ-5: PID 41186503 = RUTLEDGE ST
    {"question": "What is the address for PID 41186503?",
     "expected": "RUTLEDGE", "tab": "Development Feasibility", "cq": "5-b"},
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