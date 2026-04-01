import json
import os
from dotenv import load_dotenv
load_dotenv("../.env", override=True)

from smolagents import ToolCallingAgent, OpenAIServerModel, tool

@tool
def simple_search(query: str) -> str:
    """Searches the web.
    Args:
        query: The search query.
    """
    return f"Results for {query}: Mock result"

model = OpenAIServerModel(
    api_key=os.environ["OPENROUTER_API_KEY"],
    api_base="https://openrouter.ai/api/v1",
    model_id="google/gemma-3-27b-it:free"
)

agent = ToolCallingAgent(
    tools=[simple_search],
    model=model,
    max_steps=3
)

result = agent.run("Search for smolagents python package")
print("RESULT:", result)
