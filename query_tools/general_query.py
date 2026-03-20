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

def convert_default_sparql_to_df(results: dict):
    #Given the default sparql fetch results, cleans them into a dataframe

    #Imports
    import polars as pl

    #Helper fns
    def cast_value(value_dict: dict):
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