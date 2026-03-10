from rdflib import Dataset, Graph
from groq import Groq
import os
from SPARQLWrapper import SPARQLWrapper, JSON
from typing import Tuple
from query_tools.conversions import add_approx_loc_to_sparql_return, convert_default_sparql_to_df
from polars import DataFrame
from openai import OpenAI
from pprint import pformat
from visualizer.visualizer import map_polygons
import webbrowser

#GLOBAL OBJS
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
            FILTER NOT EXISTS {
                ?b a hp:Building ;
                hp:occupies ?p .
            }

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

CURR_STYLE = "developer" #determines which role convention to use (developer for openai, system for groq)

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
        client: Groq | OpenAI,
        model_name: str
) -> list:
    print("Generating agent introduction...\n")

    messages=[
            {
                "role": CURR_STYLE,
                "content": """You are a helpful assistant agent and an expert in the city of Halifax, powered by a housing Ontology all about the city of Halifax.
                When answering user prompts, please do not give a detailed breakdown of how you analyzed past inputs to answer the questions, but rather continue
                the conversation in a natural tone. Please note that you are *specifically* designed to answer housing related questions.
                
                Here is background on the project you will be used for: **star of background**

                Problem Statement: The Canadian housing market has experienced both record demand and periods of slowed growth in recent years, highlighting its unpredictable nature. As a result, investors and government agencies have become more cautious in their approach to housing development. This has created a growing need for reliable “Housing Potential” assessment tools that can guide decision-making, reduce underutilized housing, and ultimately improve housing value for Canadian residents.
                In response to this challenge, a Natural Language Querying solution, Halifax.Ai, was designed to help stakeholders assess housing potential in Halifax. The system leverages a data-rich knowledge graph combined with the latest models from OpenAI to generate reliable, truthful responses to a wide range of housing-related questions that developers and planners may have.
                **end of background**

                In this case, you are Halifax.Ai. Please internalize this problem statement and use it to better understand your role as an intelligent
                chatbot assistant.
                """
            },
            {
                "role": "user",
                "content": """
                    Give me a quick introduction of who you are and what you can do for me.
                    Finish by stating that you are ready to continue answering any questions.
                    Make sure to highlight your expertise in housing realted questions.
                """
            }
        ]

    if type(client) == Groq:
        completion = client.chat.completions.create(
            model=model_name,
            messages=messages,
            # temperature=0.1,
            # max_tokens=256,
        )

        response = completion.choices[0].message.content
    
    else:
        completion = client.responses.create(
            # reasoning={"effort": "low"},
            model=model_name,
            input=messages
        )

        response = completion.output_text

    messages.append({"role": "assistant", "content": response})

    print(response)
    return messages

def initialize_agent_groq(
        model_name: str # Change to the model you want to use here
        ):
    client = Groq(
        api_key=os.environ.get("GROQ_API_KEY")  #Make sure to setup your key first in env vars
    )

    prompt_agent(client, model_name)
    return

def evaluate_potential_cq(messages: list[dict], client: Groq | OpenAI, model_name: str) -> Tuple[list[dict], bool]:
    #Decides if the user is asking a CQ question, so that a query can be ran for it.

    #Appending decision question to given user prompt
    messages.append(
        {"role": CURR_STYLE, "content": f"""
         Evaluate if the user is asking a Competency Question (CQ).

         Below is a list of the CQs:

         {pformat(
             [query['original'] for query in [dict_obj for dict_obj in ALL_QUERIES_REF]]
         )}

         Find the question that BEST matches the user's input, if one exists, and return the index.

         For example, if the first question matches the best, return 0, if the second one matches the best, return 1, and so on.

         If this is true, please simply reply with the index of the best match.

         If not true, please simply reply 'NO'
         
         """}
    )

    #Agent answers if this is a CQ
    response = generate_agent_response(client, model_name, messages)

    messages.append({"role": "assistant", "content": response})

    if response != "No":
        return (messages, int(response))

    return (messages, False)

def generate_agent_response(client: Groq | OpenAI, model_name: str, messages: dict, temp: float = 0.1) -> str: #simply returns agent's text output
    response: str = ''

    if type(client) == Groq:
        completion = client.chat.completions.create(
            model=model_name,
            messages=messages,
            temperature=temp,
        )
        response = completion.choices[0].message.content

    else:
        completion = client.responses.create( #template for openai agent
            # reasoning={"effort": "low"},
            model=model_name,
            input=messages,
            temperature=temp,
        )

        response = completion.output_text

    return response

def process_query_response(query: dict[str, str], messages: list, model_name: str, client: Groq | OpenAI):
    response: str = ''

    #Now, need to check if any special modfications are needed, based on the query answered

    #Keep building, for now just checks q1
    query_id = query['id']
    if query_id in ['cq_1']: #checking if query id is cq_1
        raw_query = query_endpoint(query["query"])
        clean_query = add_approx_loc_to_sparql_return(raw_query) #if cq1, need to add approx loc (centroid)
        map_file = map_polygons([r['pl'] for r in raw_query["results"]["bindings"]], 'test') #note that ['results']['bindings'] will access the actual query results, ['pl'] access the polygon obj of each returned row
        webbrowser.open_new_tab(f"file://{os.path.abspath(map_file)}") #opening map for user

        #Adding extra context for agent
        messages.append({"role": CURR_STYLE, "content": 
                         f"""
                            You will find the query response in the next item.

                            Note that the user has been displayed a map with all the empty parcels that we identified in the query,
                            results for which you will find in the next item.

                            Also note that the original response had {len(raw_query["results"]["bindings"])}, you are only being passed {len(clean_query)} entries.

                            Please highlight the fact that user can now see the map with the empty parcels along with your summarization for your next response.
                            """})

    elif query_id in ['cq_2']:
        #User should have provided some propery id object identifier, so make gpt find it:
        messages.append(
            {
                "role": CURR_STYLE,
                "content":
                """
                Since the user is (or should be) asking about a SPECIFIC property object, please look for this property object number in their input.

                If their input looks something like this: 'Who owns parcel 426324?', then we know they are looking for object '426324'.

                Notice how it is supposed to be a SIX digit integer number.

                Now that you identified the correct property object number, please reply to this prompt as follows:

                1. Take the identified, SIX digit propery object.
                2. Put it into this format: 'cot:Property426324'
                3. Notice how we have 'cot:Property' followed by the six digit number you identified.
                
                Reply to this prompt ONLY with the identified number in the format specified above.
                For example, if you found 426324 in the user's input, then simply reply this: cot:Property426324

                """
            }
        )

        #Agent answers if this is a CQ
        agents_response = generate_agent_response(client, model_name, messages)
        messages.append({"role": "assistant", "content": agents_response})

        #Now, run query with custom property id
        response = query_endpoint(query["query"].replace("CUSTOM_PROPERTY_OBJ", agents_response))

        #Adding extra notes
        messages.append({"role": CURR_STYLE, "content": 
                         """
                            You will find the query response in the next item.

                            Please note that each response shows a BUILDING owner, not a parcel owner.

                            If owners are found, a building will also be found.

                            In your response, please specify that you found (a) Building(s) associated with the given property, when present, and also
                            list the owner(s) in question.
                            """})

    return response

#OpenAI agent
def initialize_agent_openai(model_name: str):
    client = OpenAI()

    prompt_agent(client, model_name)
    return

#Main agent loop
def prompt_agent(
        client: Groq | OpenAI,
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

        if cq_check != 'No':
            identified_query = ALL_QUERIES_REF[cq_check]

            #extra processing steps for raw query output
            query_response: str = process_query_response(identified_query, messages, model_name, client)

            messages.append({"role": CURR_STYLE,
                            "content": f"""
                                Here is the system's output for the query: 
                                {query_response} 

                                Can you please summarize the answer in some way and present it back to the user?
                            """
                            })
        
        else:
            #Implement logic for transitioning into generative response, for now simply keep conversation going
            messages.append({
                "role": CURR_STYLE,
                "content": """
                    Since no specific competency question was asked, please simply continue as normal,
                    and generate some creative response for the original prompt
                """
            })

        response = generate_agent_response(client, model_name, messages)
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

if __name__ == "__main__":
    initialize_agent_openai(model_name="gpt-5.4")
    # process_query_response(ALL_QUERIES_REF[0], [], 'model_name', 'client')

    exit()