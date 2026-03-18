#Global objects

CURR_STYLE = "developer" #determines which role convention to use (developer for openai, system for groq)

INTERNAL_KEY: str = "WcKRv"

ALL_QUERIES_REF = [
    #Note that all queries have a 100 results limit for testing
    {
        "id":
        "cq_1",
        
        "original":
        """Where in the city does there exist vacant parcels of land?""",

        "query":
        """
        PREFIX hp:  <http://ontology.eil.utoronto.ca/HPCDM/>
        PREFIX i72: <http://ontology.eil.utoronto.ca/ISO21972/iso21972#>
        PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>

        SELECT ?p ?a ?aUnit ?pr ?prUnit ?pl
        WHERE {
            ?p a hp:Parcel ;
            hp:hasArea ?areaObj ;
            hp:hasPerimeter ?perObj ;
            loc:hasLocation ?locObj .

            # Ensure no building occupies this parcel
            # FILTER NOT EXISTS {
            #     ?b a hp:Building ;
            #     hp:occupies ?p .
            # }

            # Area
            ?areaObj i72:hasValue ?areaMeasure .
            ?areaMeasure i72:hasNumericalValue ?a ;
                        i72:hasUnit ?aUnit .

            # Perimeter
            ?perObj i72:hasValue ?perMeasure .
            ?perMeasure i72:hasNumericalValue ?pr ;
                        i72:hasUnit ?prUnit .

            # Polygon
            ?locObj geo:asWKT ?pl .
        }
        # ORDER BY RAND()
        # LIMIT 100
        
        """
    },

    {
        'id': 'cq_2',
        'original': 'Who owns parcel x?',
        'query':
        """
        PREFIX cot: <http://ontology.eil.utoronto.ca/Halifax/Halifax-DT-Capstone#>
        PREFIX hp:  <http://ontology.eil.utoronto.ca/HPCDM/>

        SELECT ?property ?building ?owner
        WHERE {
            VALUES ?property { CUSTOM_PROPERTY_OBJ }

            OPTIONAL {
                ?building a hp:Building ;
                        hp:occupies ?property .
                OPTIONAL { ?building hp:hasOwner ?owner . }
            }
        }
        """
    },

    {
        'id': 'cq_3',
        'original': 'What use is parcel x zoned for?',
        'query': #Note that answering this question requires a lot more external processing, so this query simply retrieves all use_zone polygons
        """
        PREFIX cot: <http://ontology.eil.utoronto.ca/Halifax/Halifax-DT-Capstone#>
        PREFIX hp: <http://ontology.eil.utoronto.ca/HPCDM/>
        PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>

        SELECT ?polygon
        WHERE {
            ?zone a hp:UseZone ;
                loc:hasLocation ?location .
            
            ?location geo:asWKT ?polygon .
        }
        """
    }
]

#Special Queries
PARCEL_POLYGONS_QUERY: str = """
    PREFIX hp:  <http://ontology.eil.utoronto.ca/HPCDM/>
    PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>

    SELECT ?p ?pl
    WHERE {
        ?p a hp:Parcel ;
        loc:hasLocation ?locObj .

        # Ensure no building occupies this parcel #not running this for now, but check if it would help make queries faster / return the same results.
        # FILTER NOT EXISTS {
        #     ?b a hp:Building ;
        #     hp:occupies ?p .
        # }

        # Polygon
        ?locObj geo:asWKT ?pl .
    }
"""

CIVIC_ADDRESSES_POINTS_QUERY: str = """
    PREFIX contact: <https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/Contact/>
    PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX code: <https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/Code/>
    PREFIX genprop: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/GenericProperties/>

    SELECT ?ad ?pnt ?codeName
    WHERE {
        ?ad a contact:Address ;
                loc:hasLocation ?loc ;
                code:hasCode ?code .

        ?loc geo:asWKT ?pnt .

        ?code genprop:hasName ?codeName .
    }
"""