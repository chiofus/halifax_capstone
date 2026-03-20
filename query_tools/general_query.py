#Global Imports
from openai import OpenAI

#General query functions
def query_endpoint(
        query: str,
        repo_id: str = "HALIFAX_DT",
        default_address: str = "http://localhost:7200/repositories/",
        return_clean: bool = False) -> list | object:
    #runs the given query at the default address
    
    #Imports
    from SPARQLWrapper import SPARQLWrapper, JSON
    
    repo_address = default_address + repo_id

    sparql = SPARQLWrapper(repo_address)

    #querying
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    if return_clean: return results["results"]["bindings"] #this returns a list with the query results only.
 
    return results

def find_parcel_string(user_input: str) -> str:
    from objects.objects import CURR_STYLE, GETTING_PARCEL_OBJECT_INSTRUCTIONS
    from main_agent.agent_logic import generate_agent_response

    messages = []

    #Appending instructions to find object
    messages.append(
        {
            "role": CURR_STYLE,
            "content": GETTING_PARCEL_OBJECT_INSTRUCTIONS
        }
    )
    
    #Appending user input
    messages.append(
        {
            "role": "user",
            "content": user_input
        }
    )

    #Agent answers if this is a CQ
    #note that we do not care about preserving this convo, simply want to know if we would be able to find a specific property object
    specific_object = generate_agent_response(messages=messages)
    
    return specific_object

def get_all_parcel_objects(specific_parcel_object: str) -> str:
    from objects.objects import ALL_DATA_PARCELS_BUILDINGS, ALL_DATA_GENERAL
    from pprint import pformat
    from query_tools.object_finders import find_parcel_covers
    from copy import deepcopy

    #The answer consists of multiple parts:
    response = ''

    #1. Getting parcel info
    #Now, run query with custom property id to get all parcel related info
    response += pformat(query_endpoint(ALL_DATA_PARCELS_BUILDINGS.replace("CUSTOM_PROPERTY_OBJ", specific_parcel_object))["results"]["bindings"])

    #2. Getting all zoning objects (bylaw, zoning) that have info on parcel
    for zoning_object in find_parcel_covers(specific_parcel_object, "hp:ZoningType"):
        partial = query_endpoint(ALL_DATA_GENERAL.replace("CUSTOM_PROPERTY_OBJ", zoning_object))["results"]["bindings"]
        to_add = deepcopy(partial)

        #Remove polys
        for idx, entry in enumerate(partial):
            for object in entry.values():
                try:
                    if "POLYGON" in object['value']: 
                        del to_add[idx]
                except:
                    pass

        response += f"\n\n{pformat(to_add)}"

    return response #return clean response

def convert_default_sparql_to_df(results: dict):
    #Given the default sparql fetch results, cleans them into a dataframe

    #Imports
    import polars as pl

    #Helper fns
    def cast_value(value_dict: dict):
        #Imports
        from datetime import date, datetime


        #Datatypes
        XSD_INT = {
            "http://www.w3.org/2001/XMLSchema#integer",
            "http://www.w3.org/2001/XMLSchema#int",
            "http://www.w3.org/2001/XMLSchema#long",
            "http://www.w3.org/2001/XMLSchema#short",
            "http://www.w3.org/2001/XMLSchema#byte",
            "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
            "http://www.w3.org/2001/XMLSchema#nonPositiveInteger",
            "http://www.w3.org/2001/XMLSchema#positiveInteger",
            "http://www.w3.org/2001/XMLSchema#negativeInteger",
        }

        XSD_FLOAT = {
            "http://www.w3.org/2001/XMLSchema#decimal",
            "http://www.w3.org/2001/XMLSchema#float",
            "http://www.w3.org/2001/XMLSchema#double",
        }

        XSD_BOOL = {
            "http://www.w3.org/2001/XMLSchema#boolean",
        }

        XSD_DATE = {
            "http://www.w3.org/2001/XMLSchema#date",
        }

        XSD_DATETIME = {
            "http://www.w3.org/2001/XMLSchema#dateTime",
        }

        """Cast a SPARQL binding value to appropriate Python type."""
        value = value_dict.get("value")
        dtype = value_dict.get("datatype")
        value_type = value_dict.get("type")

        if value is None:
            return None

        # URIs remain strings
        if value_type == "uri":
            return value

        if dtype in XSD_INT:
            return int(value)

        if dtype in XSD_FLOAT:
            return float(value)

        if dtype in XSD_BOOL:
            return value.lower() == "true"

        if dtype in XSD_DATE:
            return date.fromisoformat(value)

        if dtype in XSD_DATETIME:
            # Handles timezone-aware ISO strings
            return datetime.fromisoformat(value.replace("Z", "+00:00"))

        # Fallback: string
        return value

    bindings: dict[dict] = results["results"]["bindings"]

    rows = []
    for binding in bindings:
        row = {}
        curr_vars = list(binding.keys())
        for var in curr_vars:
            row[var] = cast_value(binding[var])
            
        rows.append(row)

    return pl.DataFrame(rows)