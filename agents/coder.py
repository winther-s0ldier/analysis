import os
import re
import sys

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..")
)

from tools.analysis_library import LIBRARY_REGISTRY
from tools.code_executor import (
    get_analysis_result,
    store_analysis_result,
    _result_store,
)
from tools.model_config import get_model

def _build_library_ref() -> str:
    lines = []
    for atype, meta in LIBRARY_REGISTRY.items():
        fn = meta.get("function", "")
        args = [a for a in meta.get("required_args", []) if a != "csv_path"]
        args_str = ", ".join(args) if args else "(no extra args)"
        lines.append(f"{atype} → {fn} → {args_str}")
    return "\n".join(lines)

_LIBRARY_REF = _build_library_ref()

_PROMPT_DIR = os.path.join(os.path.dirname(__file__), '..', 'prompts')

def _load_prompt(name: str) -> str:
    with open(os.path.join(_PROMPT_DIR, name), 'r', encoding='utf-8') as f:
        return f.read()

def _coder_after_model_callback(callback_context, llm_response):
    text = ""
    if llm_response.content and llm_response.content.parts:
        for part in llm_response.content.parts:
            text += getattr(part, "text", "") or ""

    if text and not re.search(r"```python", text, re.IGNORECASE):
        from google.adk.models.llm_response import LlmResponse
        from google.genai import types
        corrective = (
            "Your previous response did not contain a ```python ... ``` code block. "
            "You MUST respond with ONLY a single Python code block in the format:\n"
            "```python\n"
            "import pandas as pd\n\n"
            "def analyze(csv_path: str) -> dict:\n"
            "    ...\n"
            "```\n"
            "No explanations, no prose — only the code block."
        )
        return LlmResponse(
            content=types.Content(
                role="model",
                parts=[types.Part(text=corrective)],
            )
        )
    return None

_coder_agent_instance = None

def get_coder_agent():
    global _coder_agent_instance
    if _coder_agent_instance is None:
        from google.adk.agents.llm_agent import LlmAgent
        _coder_agent_instance = LlmAgent(
            name="coder_agent",
            model=get_model("coder"),
            description=(
                "Pure code writer. Takes an analysis spec "
                "and returns Python code. No tools."
            ),
            instruction=_load_prompt("coder.md").replace("{LIBRARY_REF}", _LIBRARY_REF),
            tools=[],
            after_model_callback=_coder_after_model_callback,
        )
    return _coder_agent_instance
