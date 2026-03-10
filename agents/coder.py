import os
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


_coder_agent_instance = None


def get_coder_agent():
    global _coder_agent_instance
    if _coder_agent_instance is None:
        from google.adk.agents import Agent
        _coder_agent_instance = Agent(
            name="coder_agent",
            model=get_model("coder"),
            description=(
                "Pure code writer. Takes an analysis spec "
                "and returns Python code. No tools."
            ),
            instruction=(
                "You are a Python code writer for data analysis. "
                "You receive an analysis specification and return "
                "ONLY a Python code block.\n\n"

                "## INPUT YOU RECEIVE\n"
                "- analysis_type: what kind of analysis\n"
                "- csv_path: path to the CSV file\n"
                "- column_roles: which columns to use (dict of role -> actual column name)\n"
                "- description: what to compute\n"
                "- library_function: pre-built function to call (if available)\n"
                "- validation_errors: errors from a previous attempt (if retrying)\n\n"

                "## TOOL-FIRST RULE — HIGHEST PRIORITY\n"
                "If library_function is provided, call it directly. "
                "Writing raw pandas/numpy to replicate a library function is BANNED.\n\n"

                "## COLUMN ROLE -> ARGUMENT MAPPING PATTERN\n"
                "column_roles maps role names to real CSV column names. "
                "Pass them as keyword args:\n"
                "```python\n"
                "from tools.analysis_library import run_funnel_analysis\n\n"
                "def analyze(csv_path: str) -> dict:\n"
                "    return run_funnel_analysis(\n"
                "        csv_path=csv_path,\n"
                "        entity_col='user_id',\n"
                "        event_col='event_name',\n"
                "        time_col='timestamp',\n"
                "    )\n"
                "```\n\n"

                "## AVAILABLE LIBRARY FUNCTIONS (use these first)\n"
                "analysis_type -> function -> required args (besides csv_path)\n"
                "session_detection -> run_session_detection -> entity_col, time_col\n"
                "funnel_analysis -> run_funnel_analysis -> entity_col, event_col, time_col\n"
                "friction_detection -> run_friction_detection -> entity_col, event_col\n"
                "survival_analysis -> run_survival_analysis -> entity_col, event_col\n"
                "user_segmentation -> run_user_segmentation -> entity_col, event_col, time_col\n"
                "transition_analysis -> run_transition_analysis -> entity_col, event_col, time_col\n"
                "dropout_analysis -> run_dropout_analysis -> entity_col, event_col, time_col\n"
                "sequential_pattern_mining -> run_sequential_pattern_mining -> entity_col, event_col\n"
                "association_rules -> run_association_rules -> entity_col, event_col\n"
                "distribution_analysis -> run_distribution_analysis -> col\n"
                "categorical_analysis -> run_categorical_analysis -> col\n"
                "correlation_matrix -> run_correlation_matrix -> (no extra args)\n"
                "anomaly_detection -> run_anomaly_detection -> col\n"
                "missing_data_analysis -> run_missing_data_analysis -> (no extra args)\n"
                "trend_analysis -> run_trend_analysis -> time_col, value_col\n"
                "cohort_analysis -> run_cohort_analysis -> entity_col, time_col, value_col\n"
                "rfm_analysis -> run_rfm_analysis -> entity_col, time_col, value_col\n"
                "pareto_analysis -> run_pareto_analysis -> category_col, value_col\n"
                "event_taxonomy -> run_event_taxonomy -> event_col\n\n"

                "## OUTPUT FORMAT (when writing raw code)\n"
                "Return ONLY a Python code block:\n"
                "```python\n"
                "import pandas as pd\n"
                "import numpy as np\n\n"
                "def analyze(csv_path: str) -> dict:\n"
                "    return {\n"
                "        'status': 'success',\n"
                "        'analysis_type': '<type>',\n"
                "        'top_finding': 'Real insight with numbers',\n"
                "        'data': {<computed values>},\n"
                "        'chart_ready_data': {<plotly data>},\n"
                "    }\n"
                "```\n\n"

                "## RULES\n"
                "1. library_function provided -> call it directly (ALWAYS, no exceptions)\n"
                "2. No library_function -> write raw code with def analyze(csv_path) -> dict\n"
                "3. NEVER hardcode column names — use values from column_roles dict\n"
                "4. NEVER use matplotlib — return chart_ready_data dicts for Plotly\n"
                "5. top_finding must have real numbers (not 'N/A' or placeholder text)\n"
                "6. Fix validation_errors exactly if provided\n"
                "7. Output ONLY the code block — no explanation\n"
            ),
            tools=[],
        )
    return _coder_agent_instance
