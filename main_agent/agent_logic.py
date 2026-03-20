#Global Imports
from openai import OpenAI
from typing import Tuple

#Agent logic
def prompt_agent( #Use this function to connect w UI
        messages: list[dict],
        client: OpenAI = OpenAI(),
        model_name: str = "gpt-5.4",
        user_question: str = ''
) -> tuple[list[dict], bool]: #returns chain of messages and if it was able to get a valid CQ match or not (False for no match)
    #Imports
    from objects.objects import ALL_QUERIES_REF, CURR_STYLE
    from query_tools.general_query import find_parcel_string, get_all_parcel_objects

    #evaluating if user is asking CQ question
    messages, cq_index = evaluate_potential_cq(messages=messages,client=client, model_name=model_name)

    if cq_index != -1: #-1 represents that the user did NOT ask a CQ, so if it is not -1, process as CQ
        try:
            identified_query = ALL_QUERIES_REF[cq_index]

            #extra processing steps for raw query output
            query_response: str = process_query_response(identified_query, messages, model_name, client)

            messages.append({"role": CURR_STYLE,
                            "content": f"""
                                Here is the system's output for the query: 
                                {query_response} 

                                Can you please use this information to answer the user's question?
                            """
                            })
            
            #Answering CQ
            response = generate_agent_response(client=client, model_name = model_name, messages = messages)
            messages.append({"role": "assistant", "content": response})

            return messages, True
        except:
            #Handle as general question
            messages.append({"role": CURR_STYLE,
                                "content": f"""
                                    In this case, the query failed to execute.

                                    Please take the last user's input and answer it without any specific query search, as a general chatbot, using any information you already have in the chat's message history.
                                """
                                })
            
            #General answer
            response = generate_agent_response(client=client, model_name = model_name, messages = messages)
            messages.append({"role": "assistant", "content": response})
            
            return messages, True

    #Else, handle as general question. There are two approaches:

    #1. Try to find a specific parcel object and query for all its triples
    identified_object = find_parcel_string(user_question)

    if identified_object != '-1':
        #Generate all data triples for found object
        all_object_triples = get_all_parcel_objects(identified_object)

        messages.append({"role": CURR_STYLE,
                            "content": f"""
                                Here is all the information the system has for {identified_object}: 
                                {all_object_triples} 

                                Can you please use this information to answer the user's question?
                            """
                            })
        
        response = generate_agent_response(client=client, model_name = model_name, messages = messages)
        messages.append({"role": "assistant", "content": response})

    #2. If that fails, handle as a general query.
    else:
        messages.append({"role": CURR_STYLE,
                            "content": f"""
                                In this case, no CQ match was found and the user is not asking about a specific property object.

                                Please take the last user's input and answer it without any specific query search, as a general chatbot.
                            """
                            })
        
        #General answer
        response = generate_agent_response(client=client, model_name = model_name, messages = messages)
        messages.append({"role": "assistant", "content": response})
    
    return messages, True

def evaluate_potential_cq(messages: list[dict], client: OpenAI, model_name: str) -> Tuple[list[dict], int]:
    #Imports
    from pprint import pformat
    from objects.objects import ALL_QUERIES_REF, CURR_STYLE, INTERNAL_KEY

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
    response = generate_agent_response(client=client, model_name = model_name, messages = messages)

    messages.append({"role": "assistant",
                     "content": f"{response} **IGNORE THIS PART OF THE MESSAGE {INTERNAL_KEY}**"
                    })

    try:
        if "NO" in response:
            return (messages, int(response))
    except:
        pass

    return (messages, -1)

def process_query_response(query: dict[str, str], messages: list, model_name: str, client: OpenAI) -> list[dict]:
    response: str = ''

    #Imports
    from query_tools.general_query import query_endpoint
    from objects.objects import INTERNAL_KEY, CURR_STYLE, GETTING_PARCEL_OBJECT_INSTRUCTIONS, ALL_DATA_GENERAL
    from visualizer.map_empty_parcels import map_empty_civ_addresses
    from pprint import pformat
    from query_tools.object_finders import find_parcel_covers
    from copy import deepcopy

    #Now, need to check if any special modfications are needed, based on the query answered

    #Keep building, for now just checks q1
    query_id = query['id']
    if query_id in ['cq_1']: #checking if query id is cq_1
        #map display import
        n_empty_addresses: int = map_empty_civ_addresses()

        #Adding extra context for agent
        messages.append({"role": CURR_STYLE, "content": 
                         f"""
                            The user has been displayed a map with all the empty civic addresses that we identified in the query.

                            Also note that the original response had {n_empty_addresses}.

                            Please refer the user to the fact that they can now see the map with the empty addresses, and remark the number of empty addresses found.

                            And PLEASE do not make any promises about what you can and cannot do with the data, such as: 'I can now help you narrow these down by area, zoning, ...'
                            """})

    elif query_id in ['cq_2', 'cq_1a', 'cq_1b']:
        #User should have provided some propery id object identifier, so make gpt find it:
        messages.append(
            {
                "role": CURR_STYLE,
                "content": GETTING_PARCEL_OBJECT_INSTRUCTIONS
            }
        )

        #Agent answers if this is a CQ
        agents_response = generate_agent_response(client=client, model_name = model_name, messages = messages)
        messages.append(
            {"role": "assistant", 
             "content": f"{agents_response} **IGNORE THIS PART OF THE MESSAGE {INTERNAL_KEY}**"})

        #Now, run query with custom property id
        response = pformat(query_endpoint(query["query"].replace("CUSTOM_PROPERTY_OBJ", agents_response))["results"]["bindings"])

    elif query_id in ['cq_3']:
        #Now, need to match given parcel to a land use area
        messages.append(
            {
                "role": CURR_STYLE,
                "content": GETTING_PARCEL_OBJECT_INSTRUCTIONS
            }
        )

        #The answer consists of multiple parts:

        #1. Getting parcel info
        specific_parcel_object = generate_agent_response(client=client, model_name = model_name, messages = messages)
        messages.append(
            {"role": "assistant", 
             "content": f"{specific_parcel_object} **IGNORE THIS PART OF THE MESSAGE {INTERNAL_KEY}**"})

        #Now, run query with custom property id
        response += pformat(query_endpoint(query["query"].replace("CUSTOM_PROPERTY_OBJ", specific_parcel_object))["results"]["bindings"])

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

def generate_agent_response(messages: dict, client: OpenAI = OpenAI() , model_name: str = "gpt-5.4", temp: float = 0.1) -> str: #simply returns agent's text output
    response: str = ''

    completion = client.responses.create( #template for openai agent
        # reasoning={"effort": "low"},
        model=model_name,
        input=messages,
        temperature=temp,
    )

    response = completion.output_text

    return response