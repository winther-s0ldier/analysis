import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from google.adk.agents import Agent
from tools.csv_profiler import (
    profile_csv,
    infer_column_semantics,
    classify_dataset,
)
from a2a_messages import create_message, Intent


_profile_store: dict = {}


def get_profile_result(session_id: str) -> dict | None:
    """Read and consume stored profile result for a session."""
    return _profile_store.pop(session_id, None)


def tool_profile_and_classify(
    csv_path: str,
    session_id: str,
) -> dict:
    """
    Full profiling pipeline in one call.
    Step 1: Raw profile — column stats, types, correlations.
    Step 2: Semantic inference — what each column represents.
    Step 3: Dataset classification — what type of data is this.

    Returns everything the Discovery Agent needs to build
    the analysis plan. Never suggests analyses itself.

    Args:
        csv_path: Absolute path to the CSV file.
        session_id: Current session ID.

    Returns:
        dict with status, raw_profile, semantic_map,
        classification, intent=PROFILE_COMPLETE.
    """
    raw_profile = profile_csv(csv_path)
    if "error" in raw_profile:
        return {
            "status": "error",
            "error": raw_profile["error"],
            "intent": Intent.ERROR,
            "session_id": session_id,
        }

    semantic_map = infer_column_semantics(raw_profile)
    classification = classify_dataset(raw_profile, semantic_map)

    result = {
        "status": "success",
        "session_id": session_id,
        "intent": Intent.PROFILE_COMPLETE,
        "raw_profile": {
            "filename":     raw_profile["filename"],
            "row_count":    raw_profile["row_count"],
            "column_count": raw_profile["column_count"],
            "columns":      raw_profile["columns"],
            "correlations": raw_profile["correlations"],
            "memory_mb":    raw_profile["memory_mb"],
            "sample_rows":  raw_profile.get(
                                "sample_rows", []
                            )[:3],
        },
        "semantic_map":       semantic_map,
        "classification":     classification,
        "ready_for_discovery": True,
    }

    if classification["needs_clarification"]:
        ambiguous = classification["ambiguous_columns"][:3]
        result["clarification_needed"] = True
        result["clarification_message"] = (
            f"Some columns have ambiguous names "
            f"({', '.join(ambiguous)}). "
            f"Analysis will proceed with best-guess "
            f"interpretation. You can specify column roles "
            f"in a custom request if needed."
        )

    _profile_store[session_id] = result

    return result


from tools.model_config import get_model

_profiler_agent_instance = None

def get_profiler_agent():
    global _profiler_agent_instance
    if _profiler_agent_instance is None:
        from google.adk.agents import Agent
        _profiler_agent_instance = Agent(
            name="profiler_agent",
            model=get_model("profiler"),
            description=(
                "Data profiling specialist. I examine CSV files and "
                "return raw facts about structure, column semantics, "
                "and dataset type. I make no analysis decisions."
            ),
            instruction=(
                "You are a data profiling specialist. "
                "Your ONLY job is to understand the data "
                "you have been given.\n\n"

                "## YOUR ONE TASK\n"
                "Call tool_profile_and_classify with the csv_path "
                "and session_id from your prompt. "
                "Return the result immediately. Nothing else.\n\n"

                "## STRICT RULES\n"
                "- Call the tool EXACTLY ONCE\n"
                "- Do NOT suggest any analyses\n"
                "- Do NOT decide what to compute\n"
                "- Do NOT add commentary to the result\n"
                "- Do NOT call any other tools\n"
                "- If the tool returns an error, return it as-is\n\n"

                "Your output feeds directly into the Discovery Agent. "
                "Accuracy and brevity are required."
            ),
            tools=[tool_profile_and_classify],
        )
    return _profiler_agent_instance
