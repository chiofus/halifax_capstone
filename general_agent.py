from rdflib import Dataset, Graph
from groq import Groq
import os
from SPARQLWrapper import SPARQLWrapper, JSON
from typing import Tuple
from query_tools.conversions import add_approx_loc_to_sparql_return, convert_default_sparql_to_df
from polars import DataFrame

#GLOBAL OBJS
ALL_QUERIES_REF = [
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

        SELECT ?p ?a ?pr ?pl WHERE {
            ?p a hp:Parcel ;
            hp:hasArea ?areaObj ;
            hp:hasPerimeter ?perObj ;
            loc:hasLocation ?locObj .

            # Ensure no building occupies this parcel
            FILTER NOT EXISTS {
                ?b a hp:Building ;
                hp:occupies ?p .
            }

            # Area
            ?areaObj i72:hasValue ?areaMeasure .
            ?areaMeasure i72:hasNumericalValue ?a .

            # Perimeter
            ?perObj i72:hasValue ?perMeasure .
            ?perMeasure i72:hasNumericalValue ?pr .

            # Polygon
            ?locObj geo:asWKT ?pl .
        }
        ORDER BY RAND()
        LIMIT 3
        
        """
    }
]

def initialize_dataset() -> Dataset | Graph:
    d = Graph()
    try:
        d.parse("raw_data\\properties_buildings\\properties_buildings.ttl", format="ttl")
    except Exception as e:
        print(f"Error parsing file: {e}")
        return
    
    return d

#GROQ CLIENT

def generate_agent_intro(
        client: Groq,
        model_name: str
) -> list:
    print("Generating agent introduction...\n")

    messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that responds as a pirate."
            },
            {
                "role": "user",
                "content": (
                    "Give me a quick introduction of who you are and what you can do for me. "
                    "Finish by stating that you are ready to continue answering any questions "
                    "I may have, all in pirate character, of course."
                )
            }
        ]

    completion = client.chat.completions.create(
        model=model_name,
        messages=messages,
        # temperature=0.1,
        # max_tokens=256,
    )

    response = completion.choices[0].message.content

    messages.append({"role": "assistant", "content": response})

    print(response)
    return messages

def initialize_agent(
        model_name: str = "openai/gpt-oss-120b" # Change to the model you want to use here
        ):
    client = Groq(
        api_key=os.environ.get("GROQ_API_KEY")  #Make sure to setup your key first in env vars
    )

    prompt_agent(client, model_name)
    return

def evaluate_potential_cq(messages: list[dict], client: Groq, model_name: str) -> Tuple[list[dict], bool]:
    #Decides if the user is asking a CQ question, so that a query can be ran for it.

    #Appending decision question to given user prompt
    messages.append(
        {"role": "user", "content": """
         Please remember that you are an intelligent chatbot assistant ready to answer housing related questions for the city of Halifax, Canada.

         Evaluate if the user is asking a Competency Question (CQ).

         If that is the case, their queston will look something like this: 'Where in the city does there exist vacant parcels of land?'

         If this is true, please simply reply 'YES'

         If not true, please simply reply 'NO'
         
         """} #This one only holds for the first CQ, implement logic for giving context for any CQ
    )

    #Agent answers if this is a CQ
    completion = client.chat.completions.create(
            model=model_name,
            messages=messages,
            # temperature=0.1,
        )
    response = completion.choices[0].message.content

    messages.append({"role": "assistant", "content": response})

    if response == "YES":
        return (messages, True)

    return (messages, False)

def process_query_response(query: dict):
    raw_query_response = query_endpoint(query["query"])
    response: str = ''

    #Now, need to check if any special modfications are needed, based on the query answered

    #Keep building, for now just checks q1
    if query['id'] in ['cq_1']: #checking if query id is cq_1
        response = add_approx_loc_to_sparql_return(raw_query_response) #if cq1, need to add approx google map's loc

    return response

#Main agent loop
def prompt_agent(
        client: Groq,
        model_name: str
) -> None:
    """
    This will be the main loop where the user can keep prompting the agent.
    """

    messages = generate_agent_intro(client, model_name)

    while True:
        user_prompt = input(">> ")
        messages.append({"role": "user", "content": user_prompt}) #preserves conversation memory

        #evaluating if user is asking CQ question
        messages, cq_check = evaluate_potential_cq(messages, client, model_name)

        if cq_check:
            #hardcoded for now, implement logic to detect correct query once we have more queries
            identified_query = ALL_QUERIES_REF[0]

            #extra processing steps for raw query output
            query_response: str = process_query_response(identified_query)

            messages.append({"role": "user",
                            "content": f"""
                                Here is the system's output for the query of '{identified_query['original']}': 
                                {query_response}

                                Can you please reply it back to the user exactly as it is presented here?
                            """
                            })
        
        else:
            #Implement logic for transitioning into generative response, for now simply keep conversation going
            messages.append({
                "role": "user",
                "content": """
                    Since no specific competency question was asked, please simply continue as normal,
                    and generate some creative response for the original prompt
                """
            })

        completion = client.chat.completions.create(
            model=model_name,
            messages=messages,
            # temperature=0.1,
            # max_tokens=256
        )

        response = completion.choices[0].message.content
        messages.append({"role": "assistant", "content": response})

        print("\n" + response + "\n")

def query_endpoint(query: str, repo_id: str = "HALIFAX_DT", default_address: str = "http://localhost:7200/repositories/"):
    repo_address = default_address + repo_id

    sparql = SPARQLWrapper(repo_address)

    #querying
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    return results

# results = query_endpoint(ALL_QUERIES_REF[0]["query"])

# approx_loc_added = add_approx_loc_to_sparql_return(results)

# for result in approx_loc_added["results"]["bindings"]:
#         for key, val in result.items():
#             print(type(val))
#             print(f"{key}: {val}\n")
# print(convert_default_sparql_to_df(approx_loc_added))
# print(approx_loc_added["results"]["bindings"])

initialize_agent(model_name="groq/compound")

exit()