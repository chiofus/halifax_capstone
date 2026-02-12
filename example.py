from openai import OpenAI
client = OpenAI()

response = client.responses.create(
    model="gpt-5-nano",
    input="Tell me how long until ai takes over in a simple sentence."
)

print(response.output_text)