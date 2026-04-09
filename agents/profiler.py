import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from tools.csv_profiler import (
    profile_csv,
    infer_column_semantics,
    classify_dataset,
)
from tools.model_config import get_model
from pipeline_types import create_message, Intent

_PROMPT_DIR = os.path.join(os.path.dirname(__file__), '..', 'prompts')

def _load_prompt(name: str) -> str:
    with open(os.path.join(_PROMPT_DIR, name), 'r', encoding='utf-8') as f:
        return f.read()

_profile_store: dict = {}

def get_profile_result(session_id: str) -> dict | None:
    return _profile_store.pop(session_id, None)

def tool_profile_and_classify(
    csv_path: str,
    session_id: str,
) -> dict:
    raw_profile = profile_csv(csv_path)
    if "error" in raw_profile:
        return {"error": raw_profile["error"]}

    try:
        from tools.data_policy import get_active_policy
        _policy = get_active_policy()
        _custom_roles = _policy.get("custom_column_roles", {})
        if _custom_roles:
            existing_roles = raw_profile.get("column_roles", {})

            _reversed = {v: k for k, v in existing_roles.items() if v}
            for col_name, role in _custom_roles.items():

                existing_roles[col_name] = role
            raw_profile["column_roles"] = existing_roles
            print(f"[PolicyEngine] Applied custom_column_roles overrides: {_custom_roles}")
    except Exception as _pe:
        print(f"[PolicyEngine] custom_column_roles apply failed: {_pe}")

    _profile_store[session_id] = {
        "status": "success",
        "raw_profile": raw_profile,
        "classification": {
            "dataset_type": raw_profile.get("dataset_type", "tabular_generic"),
            "column_roles": raw_profile.get("column_roles", {}),
            "confidence": raw_profile.get("confidence", 0.0),
            "recommended_analyses": raw_profile.get("recommended_analyses", []),
        },
    }

    # Write per-agent trace file for RM audit / history restore
    try:
        import time as _t
        from agent_servers.a2a_client import lookup_session as _lookup
        _out = _lookup(session_id)
        if _out:
            import pathlib
            _trace = {
                "agent": "profiler",
                "completed_at": _t.time(),
                "input": {
                    "csv_filename": raw_profile.get("filename", ""),
                    "row_count": raw_profile.get("row_count", 0),
                    "column_count": raw_profile.get("column_count", 0),
                },
                "output": {
                    "dataset_type": raw_profile.get("dataset_type", ""),
                    "column_roles": raw_profile.get("column_roles", {}),
                    "confidence": raw_profile.get("confidence", 0.0),
                    "recommended_analyses": raw_profile.get("recommended_analyses", []),
                },
            }
            pathlib.Path(_out, "_agent_profiler.json").write_text(
                json.dumps(_trace, indent=2), encoding="utf-8"
            )
    except Exception as _pe:
        print(f"WARNING: [Profiler] Could not write agent trace: {_pe}")

    return {
        "raw_profile": raw_profile
    }

_profiler_agent_instance = None

def get_profiler_agent():
    global _profiler_agent_instance
    if _profiler_agent_instance is None:
        from google.adk.agents import Agent
        _profiler_agent_instance = Agent(
            name="profiler_agent",
            model=get_model("profiler"),
            description=(
                "Data profiling specialist. I examine raw CSV statistics and "
                "reason about column semantics and dataset type."
            ),
            instruction=_load_prompt("profiler.md"),
            tools=[FunctionTool(tool_profile_and_classify)],
        )
    return _profiler_agent_instance
