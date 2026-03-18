#Global Imports
from openai import OpenAI
from typing import Tuple

#Agent logic
def initialize_agent(model_name: str, skip_intro: bool = True):
    client = OpenAI()

    prompt_agent_continuous(client, model_name)
    return

def prompt_agent( #Use this function to connect w UI
        messages: list[dict],
        client: OpenAI = OpenAI(),
        model_name: str = "gpt-5.4"
) -> tuple[list[dict], bool]: #returns chain of messages and if it was able to get a valid CQ match or not (False for no match)
    #Imports
    from objects.objects import ALL_QUERIES_REF, CURR_STYLE

    #evaluating if user is asking CQ question
    messages, cq_index = evaluate_potential_cq(messages, client, model_name)

    if cq_index != -1: #-1 represents that the user did NOT ask a CQ, so if it is not -1, process as CQ
        identified_query = ALL_QUERIES_REF[cq_index] 

        #extra processing steps for raw query output
        query_response: str = process_query_response(identified_query, messages, model_name, client)

        messages.append({"role": CURR_STYLE,
                        "content": f"""
                            Here is the system's output for the query: 
                            {query_response} 

                            Can you please summarize the answer in some way and present it back to the user?
                        """
                        })
        
        #Answering CQ
        response = generate_agent_response(client, model_name, messages)
        messages.append({"role": "assistant", "content": response})

        return messages, True
    
    return messages, False

def prompt_agent_continuous(
        client: OpenAI,
        model_name: str
) -> None:
    """
    This will be the main loop where the user can keep prompting the agent.
    """

    #Imports
    from objects.objects import ALL_QUERIES_REF, CURR_STYLE

    messages = generate_agent_intro(client, model_name)

    while True:
        user_prompt = input(">> ")
        messages.append({"role": "user", "content": user_prompt}) #preserves conversation memory

        #evaluating if user is asking CQ question
        messages, cq_check = evaluate_potential_cq(messages, client, model_name)

        if cq_check != -1: #-1 represents that the user did NOT ask a CQ, so if it is not -1, process as CQ
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

def generate_agent_intro(
        client: OpenAI,
        model_name: str
) -> list:
    #Imports
    from objects.objects import CURR_STYLE

    print("Generating agent introduction...\n")

    #1. Initial messages, and agent intro
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
    
    #2. First agent response
    completion = client.responses.create(
        # reasoning={"effort": "low"},
        model=model_name,
        input=messages
    )

    response = completion.output_text

    messages.append({"role": "assistant", "content": response})

    print(response)
    return messages

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
    response = generate_agent_response(client, model_name, messages)

    messages.append({"role": "assistant",
                     "content": f"{response} **IGNORE THIS PART OF THE MESSAGE {INTERNAL_KEY}**"
                    })

    if response != "NO":
        return (messages, int(response))

    return (messages, -1)

def process_query_response(query: dict[str, str], messages: list, model_name: str, client: OpenAI):
    response: str = ''

    #Now, need to check if any special modfications are needed, based on the query answered

    #Keep building, for now just checks q1
    query_id = query['id']
    if query_id in ['cq_1']: #checking if query id is cq_1
        #map display import
        from visualizer.map_empty_parcels import map_empty_civ_addresses
        from objects.objects import CURR_STYLE

        n_empty_addresses: int = map_empty_civ_addresses()

        #Adding extra context for agent
        messages.append({"role": CURR_STYLE, "content": 
                         f"""
                            The user has been displayed a map with all the empty civic addresses that we identified in the query.

                            Also note that the original response had {n_empty_addresses}.

                            Please refer the user to the fact that they can now see the map with the empty addresses, and remark the number of empty addresses found.

                            And PLEASE do not make any promises about what you can and cannot do with the data, such as: 'I can now help you narrow these down by area, zoning, ...'
                            """})

    # elif query_id in ['cq_2']:
    #     #User should have provided some propery id object identifier, so make gpt find it:
    #     messages.append(
    #         {
    #             "role": CURR_STYLE,
    #             "content":
    #             """
    #             Since the user is (or should be) asking about a SPECIFIC property object, please look for this property object number in their input.

    #             If their input looks something like this: 'Who owns parcel 426324?', then we know they are looking for object '426324'.

    #             Notice how it is supposed to be a SIX digit integer number.

    #             Now that you identified the correct property object number, please reply to this prompt as follows:

    #             1. Take the identified, SIX digit propery object.
    #             2. Put it into this format: 'cot:Property426324'
    #             3. Notice how we have 'cot:Property' followed by the six digit number you identified.
                
    #             Reply to this prompt ONLY with the identified number in the format specified above.
    #             For example, if you found 426324 in the user's input, then simply reply this: cot:Property426324

    #             """
    #         }
    #     )

    #     #Agent answers if this is a CQ
    #     agents_response = generate_agent_response(client, model_name, messages)
    #     messages.append({"role": "assistant", "content": agents_response})

    #     #Now, run query with custom property id
    #     response = query_endpoint(query["query"].replace("CUSTOM_PROPERTY_OBJ", agents_response))

    #     #Adding extra notes
    #     messages.append({"role": CURR_STYLE, "content": 
    #                      """
    #                         You will find the query response in the next item.

    #                         Please note that each response shows a BUILDING owner, not a parcel owner.

    #                         If owners are found, a building will also be found.

    #                         In your response, please specify that you found (a) Building(s) associated with the given property, when present, and also
    #                         list the owner(s) in question.
    #                         """})

    return response

def generate_agent_response(client: OpenAI, model_name: str, messages: dict, temp: float = 0.1) -> str: #simply returns agent's text output
    response: str = ''

    completion = client.responses.create( #template for openai agent
        # reasoning={"effort": "low"},
        model=model_name,
        input=messages,
        temperature=temp,
    )

    response = completion.output_text

    return response