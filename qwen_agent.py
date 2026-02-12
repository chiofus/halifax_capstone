from rdflib import Dataset
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig

def initialize_dataset() -> Dataset:
    d = Dataset()
    try:
        d.parse("starwars.trig", format="trig")
    except Exception as e:
        print(f"Error parsing file: {e}")
        return
    
    return d

def initialize_agent_prompting(model, tokenizer) -> None:
    """
    This will be the main loop where the user can keep prompting the agent.
    """


    while True:
        #Get user prompt
        user_prompt: str = input(">> ")

        #Generate agent response
        prompt = [
            {"role": "system", "content": "You are a helpful assistant, that responds as a pirate. The user has asked the following question."},
            {"role": "user", "content": f"{user_prompt}"},
        ]

        # Apply Qwen chat template
        inputs = tokenizer.apply_chat_template(
            prompt,
            tokenize=True,
            add_generation_prompt=True,
            return_tensors="pt",
        )

        input_ids = inputs["input_ids"].to("cuda")
        attention_mask = inputs["attention_mask"].to("cuda")

        print("Generating response...")
        with torch.no_grad():
            outputs = model.generate(
                input_ids=input_ids,
                attention_mask=attention_mask,
                max_new_tokens=256,
                temperature=0.1,
                top_p=0.9,
                repetition_penalty=1.1,
                do_sample=False,
            )

        response = tokenizer.decode(outputs[0][input_ids.shape[1]:], skip_special_tokens=True)
        print(response)

def initialize_agent():
    model_id = "Qwen/Qwen2.5-7B-Instruct"

    quantization_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_compute_dtype=torch.float16,
        bnb_4bit_use_double_quant=True,
        bnb_4bit_quant_type="nf4",
    )

    print("Loading tokenizer...")
    tokenizer = AutoTokenizer.from_pretrained(model_id)

    print("Loading model...")
    model = AutoModelForCausalLM.from_pretrained(
        model_id,
        device_map="auto",
        quantization_config=quantization_config,
        dtype=torch.float16
    )

    prompt = [
        {"role": "system", "content": "You are a helpful assistant, that responds as a pirate."},
        {"role": "user", "content": "Give me a quick introduction of who you are and what you can do for me. Finish by stating that you are ready to continue answering any questions I may have, all in pirate character, of course."},
    ]

    # Apply Qwen chat template
    inputs = tokenizer.apply_chat_template(
        prompt,
        tokenize=True,
        add_generation_prompt=True,
        return_tensors="pt",
    )

    input_ids = inputs["input_ids"].to("cuda")
    attention_mask = inputs["attention_mask"].to("cuda")

    print("Generating agent introduction...")
    with torch.no_grad():
        outputs = model.generate(
            input_ids=input_ids,
            attention_mask=attention_mask,
            max_new_tokens=256,
            temperature=0.1,
            top_p=0.9,
            repetition_penalty=1.1,
            do_sample=False,
        )

    response = tokenizer.decode(outputs[0][input_ids.shape[1]:], skip_special_tokens=True)
    print(response)

    initialize_agent_prompting(model, tokenizer)

    print("THE AGENT HAS NOW BEEN TERMINATED")
    return

d = initialize_dataset()

#Testing a query to get han solo's height
query: str = """
SELECT ?height WHERE {
  ?character rdfs:label "Han Solo"@en .
  ?character <https://swapi.co/vocabulary/height> ?height .
}
"""

results = d.query(query)

for row in results:
    print(row)

initialize_agent()

exit()