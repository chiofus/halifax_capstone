#Special Queries
PARCEL_POLYGONS_QUERY: str = """
    PREFIX hp:  <http://ontology.eil.utoronto.ca/HPCDM/>
    PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>

    SELECT ?p ?pl
    WHERE {
        ?p a hp:Parcel ;
        loc:hasLocation ?locObj .

        # Polygon
        ?locObj geo:asWKT ?pl .
    }
"""

GENERAL_POLYGON_SEARCH: str = """
    PREFIX cot: <http://ontology.eil.utoronto.ca/Halifax/Halifax-DT-Capstone#>
    PREFIX hp:  <http://ontology.eil.utoronto.ca/HPCDM/>
    PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>

    SELECT ?p ?pl
    WHERE {
        #Custom object definition
        VALUES ?root { CUSTOM_PROPERTY_OBJ }

        ?p a ?root ;
        loc:hasLocation ?locObj .

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

ALL_DATA_GENERAL: str = """

PREFIX cot: <http://ontology.eil.utoronto.ca/Halifax/Halifax-DT-Capstone#>

SELECT ?s ?p ?o
WHERE {
    VALUES ?root { CUSTOM_PROPERTY_OBJ }

    {
        # Depth 0 (root)
        ?root ?p ?o .
        BIND(?root AS ?s)
    }
    UNION
    {
        # Depth 1
        ?root ?p1 ?mid1 .
        ?mid1 ?p ?o .
        BIND(?mid1 AS ?s)
    }
    UNION
    {
        # Depth 2
        ?root ?p1 ?mid1 .
        ?mid1 ?p2 ?mid2 .
        ?mid2 ?p ?o .
        BIND(?mid2 AS ?s)
    }
    UNION
    {
        # Depth 3 (downstream)
        ?root ?p1 ?mid1 .
        ?mid1 ?p2 ?mid2 .
        ?mid2 ?p3 ?mid3 .
        ?mid3 ?p ?o .
        BIND(?mid3 AS ?s)
    }
    UNION
    {
        # Upstream (depth 1)
        ?s ?p ?root .
    }
    UNION
    {
        # Upstream depth 2
        ?x1 ?p1 ?root .
        ?x1 ?p ?o .
        BIND(?x1 AS ?s)
    }
    UNION
    {
        # Upstream depth 3
        ?x2 ?p2 ?x1 .
        ?x1 ?p1 ?root .
        ?x2 ?p ?o .
        BIND(?x2 AS ?s)
    }
}

"""

ALL_DATA_PARCELS_BUILDINGS: str = """

PREFIX cot: <http://ontology.eil.utoronto.ca/Halifax/Halifax-DT-Capstone#>
PREFIX hp: <http://ontology.eil.utoronto.ca/HPCDM/>

SELECT ?s ?p ?o
WHERE {
  VALUES ?root { CUSTOM_PROPERTY_OBJ }

  {
    # Depth 0 (root)
    ?root ?p ?o .
    BIND(?root AS ?s)
  }
  UNION
  {
    # Depth 1
    ?root ?p1 ?mid1 .
    ?mid1 ?p ?o .
    BIND(?mid1 AS ?s)
  }
  UNION
  {
    # Depth 2
    ?root ?p1 ?mid1 .
    ?mid1 ?p2 ?mid2 .
    ?mid2 ?p ?o .
    BIND(?mid2 AS ?s)
  }
  UNION
  {
    # Depth 3 (downstream)
    ?root ?p1 ?mid1 .
    ?mid1 ?p2 ?mid2 .
    ?mid2 ?p3 ?mid3 .
    ?mid3 ?p ?o .
    BIND(?mid3 AS ?s)
  }
  UNION
  {
    # Upstream (depth 1)
    ?s ?p ?root .
  }
  UNION
  {
    # Upstream depth 2
    ?x1 ?p1 ?root .
    ?x1 ?p ?o .
    BIND(?x1 AS ?s)
  }
  UNION
  {
    # Upstream depth 3
    ?x2 ?p2 ?x1 .
    ?x1 ?p1 ?root .
    ?x2 ?p ?o .
    BIND(?x2 AS ?s)
  }

  # Building-specific expansions
  UNION
  {
    # Buildings (0 hop)
    ?building a hp:Building ;
              hp:occupies ?root ;
              ?p ?o .
    BIND(?building AS ?s)
  }
  UNION
  {
    # Building → 1 hop
    ?building a hp:Building ;
              hp:occupies ?root ;
              ?p1 ?mid1 .
    ?mid1 ?p ?o .
    BIND(?mid1 AS ?s)
  }
  UNION
  {
    # Building → 2 hops
    ?building a hp:Building ;
              hp:occupies ?root ;
              ?p1 ?mid1 .
    ?mid1 ?p2 ?mid2 .
    ?mid2 ?p ?o .
    BIND(?mid2 AS ?s)
  }
  UNION
  {
    # Building → 3 hops
    ?building a hp:Building ;
              hp:occupies ?root ;
              ?p1 ?mid1 .
    ?mid1 ?p2 ?mid2 .
    ?mid2 ?p3 ?mid3 .
    ?mid3 ?p ?o .
    BIND(?mid3 AS ?s)
  }
}

"""

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
        'query': ALL_DATA_PARCELS_BUILDINGS
    },

    {
        'id': 'cq_1a',
        'original': 'What is the size of parcel x?',
        'query': ALL_DATA_PARCELS_BUILDINGS
    },

    {
        'id': 'cq_1b',
        'original': 'What is the perimeter of parcel x?',
        'query': ALL_DATA_PARCELS_BUILDINGS
    },

    {
        'id': 'cq_3',
        'original': 'What use is parcel x zoned for?',
        'query': ALL_DATA_PARCELS_BUILDINGS #only gets parcel data, further processing for zoning data later in code
    }
]

#Individual object retrievals
GET_POLYGON_PARCEL = """
    PREFIX hp:  <http://ontology.eil.utoronto.ca/HPCDM/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX CityUnits: <http://ontology.eil.utoronto.ca/5087/1/CityUnits/>
    PREFIX cot: <http://ontology.eil.utoronto.ca/Halifax/Halifax-DT-Capstone#>
    PREFIX i72: <http://ontology.eil.utoronto.ca/ISO21972/iso21972#>
    PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?pl
    WHERE {
        #Custom object definition
        VALUES ?p { CUSTOM_PROPERTY_OBJ }

        ?p a hp:Parcel ;
        loc:hasLocation ?locObj .

        # Polygon
        ?locObj geo:asWKT ?pl .
    }
"""

#Instructions

GETTING_PARCEL_OBJECT_INSTRUCTIONS: str = """
Since the user is (or should be) asking about a SPECIFIC property object, please look for this property object number in their input.

If their input looks something like this: 'Who owns parcel 426324?', then we know they are looking for object '426324'.

Notice how it is supposed to be a SIX digit, integer number.

Now that you identified the correct property object number, please reply to this prompt as follows:

1. Take the identified, SIX digit propery object.
2. Put it into this format: 'cot:Property426324'
3. Notice how we have 'cot:Property' followed by the six digit number you identified.

Reply to this prompt ONLY with the identified number in the format specified above.
For example, if you found 426324 in the user's input, then simply reply this: cot:Property426324

IF YOU ARE NOT ABLE TO OBTAIN A SPECIFIC NUMBER, please simply respond -1.
"""