from rdflib import Dataset
from groq import Groq
import os

def initialize_dataset() -> Dataset:
    d = Dataset()
    try:
        d.parse("starwars.trig", format="trig")
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

        completion = client.chat.completions.create(
            model=model_name,
            messages=messages,
        )

        response = completion.choices[0].message.content
        messages.append({"role": "assistant", "content": response})

        print("\n" + response + "\n")

d = initialize_dataset()

#Testing a query to get han solo's height
query: str = """
SELECT ?height WHERE {
  ?character rdfs:label "Han Solo"@en .
  ?character <https://swapi.co/vocabulary/height> ?height .
}
"""

# results = d.query(query)

# for row in results:
#     print(row)

initialize_agent()

exit()