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
    """Generate the AVAILABLE LIBRARY FUNCTIONS block from LIBRARY_REGISTRY at import time."""
    lines = []
    for atype, meta in LIBRARY_REGISTRY.items():
        fn = meta.get("function", "")
        args = [a for a in meta.get("required_args", []) if a != "csv_path"]
        args_str = ", ".join(args) if args else "(no extra args)"
        lines.append(f"{atype} → {fn} → {args_str}")
    return "\n".join(lines)

_LIBRARY_REF = _build_library_ref()


def _coder_after_model_callback(callback_context, llm_response):
    """Validate that the coder's response contains a Python code block.

    If the model response is missing a ```python ... ``` block, return a
    corrective LlmResponse that instructs the model to reformat.  Returning
    None keeps the original response unchanged.
    """
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
            instruction=(
                "You are a Professional Python Code Writer within a dynamic, production-grade analytics pipeline. "
                "Your ONLY responsibility is to receive an analysis specification and return a single, clean Python code block. "
                "You do NOT validate code. You do NOT execute code. You do NOT store results. You ONLY write code.\n\n"

                "## CORE RULE: LIBRARY-FIRST (Non-Negotiable)\n"
                "If `library_function` is provided in your input, you MUST call it directly. "
                "Writing custom pandas/numpy code to replicate a library function is STRICTLY BANNED. "
                "This rule exists because library functions are pre-tested, handle edge cases, and produce standardized output envelopes. "
                "Violating it risks crashes in downstream pipeline stages.\n\n"

                "## COLUMN ROLE → ARGUMENT MAPPING (Critical Precision Required)\n"
                "The `column_roles` dict maps ROLE NAMES (like `entity_col`) to ACTUAL CSV COLUMN NAMES (like `user_uuid`). "
                "You MUST pass the ACTUAL COLUMN NAME (the value from column_roles) as a keyword argument — NEVER the role name itself.\n"
                "CORRECT: `run_funnel_analysis(csv_path=csv_path, entity_col='user_uuid', event_col='action', time_col='created_at')`\n"
                "WRONG: `run_funnel_analysis(csv_path=csv_path, entity_col='entity_col', event_col='event_col')`\n\n"

                "## REQUIRED FUNCTION SIGNATURE (All custom code MUST follow this)\n"
                "Every function MUST be named `analyze` and accept only `csv_path: str`:\n"
                "```python\n"
                "def analyze(csv_path: str) -> dict:\n"
                "    # Your code here\n"
                "```\n\n"

                "## OUTPUT FORMAT (when writing raw code)\n"
                "Return ONLY a Python code block:\n"
                "```python\n"
                "import pandas as pd\n"
                "import numpy as np\n\n"
                "def analyze(csv_path: str) -> dict:\n"
                "    return {\n"
                "        'status': 'success',\n"
                "        'analysis_type': '<type>',\n"
                "        'top_finding': 'Real insight with specific numbers (e.g., 38.4% of users...)',\n"
                "        'data': {<computed dict of metrics>},\n"
                "        'chart_ready_data': {<plotly-compatible dict>},\n"
                "    }\n"
                "```\n\n"

                f"## AVAILABLE LIBRARY FUNCTIONS (analysis_type → function → required args)\n"
                f"{_LIBRARY_REF}\n\n"

                "## CUSTOM ANALYSIS (for analysis types not in the library above)\n"
                "If `library_function` is NOT provided AND `analysis_type` is not in the library reference above, "
                "you MUST write a full custom `analyze(csv_path)` function from scratch using pandas/numpy. "
                "The function must still return the standard result envelope with `status`, `analysis_type`, "
                "`top_finding`, `data`, and `chart_ready_data` keys. "
                "The `chart_ready_data` dict MUST include a `type` key. Use one of these supported types:\n"
                "  - `bar` or `bar_chart`: vertical bar chart — keys: `labels` (list[str]), `values` (list[number])\n"
                "  - `horizontal_bar`: horizontal bar — keys: `labels`, `values`\n"
                "  - `line` or `trend_line`: line chart — keys: `labels`/`x`/`times` (list), `values`/`y` (list)\n"
                "  - `scatter`: scatter plot — keys: `x` (list), `y` (list)\n"
                "  - `histogram`: histogram — keys: `hist_values` (list[number])\n"
                "  - `pie` or `pie_chart`: pie/donut — keys: `labels` (list[str]), `values` (list[number])\n"
                "  - `heatmap`: heatmap — keys: `labels` ({x: [], y: []}), `values`/`z` (2D list)\n"
                "  - `frequency_bar`: bar chart — keys: `labels`, `values`\n"
                "  - `correlation_heatmap`: heatmap — keys: `matrix` (2D), `columns` (list)\n"
                "ALWAYS include `chart_ready_data` — it is REQUIRED for the chart to appear in the UI.\n"
                "Use the `description` field from the analysis spec to understand what to compute.\n\n"

                "## SERIALIZATION SAFETY (Mandatory)\n"
                "The result dict will be JSON-serialized immediately after your function returns. "
                "To prevent crashes, your code MUST:\n"
                "- Convert all `numpy.int64` → `int()`, all `numpy.float64` → `float()`.\n"
                "- Replace all `float('nan')` and `float('inf')` values with `None`.\n"
                "- NEVER include NumPy arrays directly — convert them to Python lists with `.tolist()`.\n"
                "Since library functions handle this internally, this rule primarily applies to custom code.\n\n"

                "## DO's\n"
                "- DO call the library function directly if `library_function` is provided.\n"
                "- DO use actual column name strings (not role key names) as argument values.\n"
                "- DO write a meaningful `top_finding` with real numbers (e.g., 'Average session length is 4.2 events across 1,234 users').\n"
                "- DO handle missing or null column gracefully — return `{'status': 'error', 'top_finding': 'Reason'}` if data is insufficient.\n"
                "- DO import only from: `pandas`, `numpy`, `tools.analysis_library`. No other imports allowed.\n\n"

                "## DON'Ts\n"
                "- DON'T write custom analysis code when a library function exists.\n"
                "- DON'T use `matplotlib` — all charts are Plotly and handled by the executor.\n"
                "- DON'T hardcode dataset-specific column names — always read them from `column_roles`.\n"
                "- DON'T generate placeholder text in `top_finding` like 'N/A', 'See data', or 'Analysis complete'.\n"
                "- DON'T add explanatory text outside the code block — output ONLY the ```python ... ``` block.\n"
                "- DON'T wrap the library call in a try/except that silently swallows errors — let exceptions propagate.\n\n"

                "## ERROR FIXING PROTOCOL\n"
                "If `validation_errors` is provided, you are on a retry. Read the errors carefully. "
                "Common errors and their fixes:\n"
                "- `TypeError: missing argument 'col'` → You passed wrong key name. Check the library reference above.\n"
                "- `KeyError: 'column_name'` → The column does not exist. Use `df.columns.tolist()` to inspect and pick the closest match.\n"
                "- `ValueError: NaT/NaN in datetime` → Add `pd.to_datetime(df[col], errors='coerce').dropna()` before processing.\n"
            ),
            tools=[],
            after_model_callback=_coder_after_model_callback,
        )
    return _coder_agent_instance
