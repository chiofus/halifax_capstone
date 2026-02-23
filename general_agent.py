from rdflib import Dataset, Graph
from groq import Groq
import os
from SPARQLWrapper import SPARQLWrapper, JSON

def initialize_dataset() -> Dataset | Graph:
    d = Graph()
    try:
        d.parse("raw_data\\properties_buildings\\properties_buildings.ttl", format="ttl")
    except Exception as e:
        print(f"Error parsing file: {e}")
        return
    
    return d

#GROQ CLIENT

def initialize_groq_client() -> Groq:
    return Groq(
        api_key=os.environ.get("GROQ_API_KEY")  #Make sure to setup your key first in env vars
    )

def generate_agent_intro(
        client: Groq,
        model_name: str
) -> None:
    print("Generating agent introduction...\n")

    completion = client.chat.completions.create(
        model=model_name,
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
        ],
        temperature=0.1,
        max_tokens=256,
    )

    print(completion.choices[0].message.content)
    print()
    return

def initialize_agent(
        model_name: str = "openai/gpt-oss-120b" # Change the model you want to use here
        ):
    client = initialize_groq_client()

    generate_agent_intro(client, model_name)
    initialize_agent_prompting(client, model_name)
    return

def evaluate_potential_cq(messages: list[dict], client: Groq, model_name: str):
    #Decides if the user is asking a CQ question, so that a query can be ran for it.

    #Appending decision question to given user prompt
    messages = messages.append(
        {"role": "system", "content": """
         Please remember that you are an intelligent chatbot assistant ready to answer housing related questions for the city of Halifax, Canada.

         Evaluate if the user is asking a Competency Question (CQ).

         If that is the case, their queston will look something like this: 'Where in the city does there exist vacant parcels of land?'

         If this is true, please simply reply 'YES'
         
         """}
    )

    #Agent answers if this is a CQ
    completion = client.chat.completions.create(
            model=model_name,
            messages=messages,
        )
    response = completion.choices[0].message.content

    messages.append({"role": "assistant", "content": response})

    #Evaluating response
    if response == "YES":
        pass

#Main agent loop
def initialize_agent_prompting(
        client: Groq,
        model_name: str
) -> None:
    """
    This will be the main loop where the user can keep prompting the agent.
    """

    messages = [
        {"role": "system", "content": "You are a pirate assistant."}
    ]

    while True:
        user_prompt = input(">> ")
        messages.append({"role": "user", "content": user_prompt}) #preserves conversation memory

        #evaluating if user is asking CQ question
        messages = evaluate_potential_cq(messages, client, model_name)

        completion = client.chat.completions.create(
            model=model_name,
            messages=messages,
        )

        response = completion.choices[0].message.content
        messages.append({"role": "assistant", "content": response})

        print("\n" + response + "\n")

def query_endpoint(repo_id: str, query: str, default_address: str = "http://localhost:7200/repositories/"):
    repo_address = default_address + repo_id

    sparql = SPARQLWrapper(repo_address)

    #querying
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    return results

# d = initialize_dataset()

#Randomly selects one object
q: str = """
PREFIX hp:  <http://ontology.eil.utoronto.ca/HPCDM/>
PREFIX i72: <http://ontology.eil.utoronto.ca/ISO21972/iso21972#>
PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>

SELECT ?p ?a ?pr ?pl WHERE {
    ?p a hp:Parcel ;
       hp:hasArea ?areaObj ;
       hp:hasPerimter ?perObj ;
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
LIMIT 1
 
"""

results = query_endpoint(repo_id="HALIFAX_DT", query=q)

for result in results["results"]["bindings"]:
        for key, val in result.items():
            print(type(val))
            print(f"{key}: {val}\n")

# initialize_agent()

exit()