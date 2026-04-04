import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_PROMPT_DIR = os.path.join(os.path.dirname(__file__), '..', 'prompts')

def _load_prompt(name: str) -> str:
    with open(os.path.join(_PROMPT_DIR, name), 'r', encoding='utf-8') as f:
        return f.read()

_chat_agent_instance = None

def get_chat_agent():
    global _chat_agent_instance
    if _chat_agent_instance is None:
        from google.adk.agents import Agent
        from tools.model_config import get_model

        _chat_agent_instance = Agent(
            name="chat_agent",
            model=get_model("chat"),
            description=(
                "Conversational data analyst. Answers business questions "
                "about completed analysis results. Never runs tools or "
                "triggers new analyses."
            ),
            instruction=_load_prompt("chat_agent.md"),
            tools=[],
        )
    return _chat_agent_instance
