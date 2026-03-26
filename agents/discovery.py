import re
import os
import sys
import json
import traceback
from typing import Annotated
from pydantic import Field

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from a2a_messages import (
    create_message,
    Intent,
    MetricSpec,
)

_PROMPT_DIR = os.path.join(os.path.dirname(__file__), '..', 'prompts')


def _load_prompt(name: str) -> str:
    with open(os.path.join(_PROMPT_DIR, name), 'r', encoding='utf-8') as f:
        return f.read()


_plan_store: dict = {}


def get_analysis_plan(session_id: str) -> dict | None:
    """Read and consume stored plan for a session."""
    return _plan_store.pop(session_id, None)


def build_dag_deterministic(
    dataset_type: str,
    column_roles: dict,
    selected_analyses: list,
    row_count: int,
) -> dict:
    nodes = []
    
    # If the LLM profiler didn't provide entity/time/event but we requested
    # a session-dependent analysis, we add session_detection as a prerequisite
    needs_session = any(a in selected_analyses for a in [
        "funnel_analysis", "survival_analysis", "user_segmentation",
        "sequential_pattern_mining", "transition_analysis", "dropout_analysis"
    ])

    if needs_session:
        nodes.append({
            "id": "A1",
            "name": "Session Detection",
            "analysis_type": "session_detection",
            "column_roles": column_roles,
            "depends_on": [],
            "priority": "critical"
        })

    for i, analysis in enumerate(selected_analyses):
        if analysis == "session_detection":
            continue
            
        node_id = f"A{len(nodes) + 1}"
        deps = ["A1"] if needs_session and analysis not in [
            "distribution_analysis", "categorical_analysis", "correlation_matrix",
            "anomaly_detection", "missing_data_analysis", "pareto_analysis",
            "trend_analysis", "time_series_decomposition"
        ] else []

        nodes.append({
            "id": node_id,
            "name": analysis.replace("_", " ").title(),
            "analysis_type": analysis,
            "column_roles": column_roles,
            "depends_on": deps,
            "priority": "high" if i < 3 else "medium"
        })

    from tools.analysis_library import LIBRARY_REGISTRY
    
    final_dag = []
    metrics_ui = []

    for node in nodes:
        atype = node.get("analysis_type")
        if not atype:
            continue
            
        lib_entry = LIBRARY_REGISTRY.get(atype, {})
        name = node.get("name") or atype.replace("_", " ").title()
        desc = node.get("description") or lib_entry.get("description", atype)
        lib_fn = node.get("library_function") or lib_entry.get("function")
        
        spec = {
            "id": node.get("id"),
            "name": name,
            "description": desc,
            "analysis_type": atype,
            "library_function": lib_fn,
            "required_columns": node.get("required_columns", []),
            "column_roles": node.get("column_roles", {}),
            "depends_on": node.get("depends_on", []),
            "priority": node.get("priority", "medium"),
            "feasibility": node.get("feasibility", "HIGH" if lib_fn else "MEDIUM"),
            "status": "pending"
        }
        final_dag.append(spec)

    data_summary = f"{dataset_type.replace('_', ' ').title()} dataset with {row_count:,} rows. {len(final_dag)} analyses planned."
    return {"status": "success", "dag": final_dag, "data_summary": data_summary, "node_count": len(final_dag)}


def tool_submit_analysis_plan(
    session_id: Annotated[str, Field(description="Active pipeline session ID")],
    dag_json_str: Annotated[str, Field(description="JSON string of the analysis DAG -- a list of node objects or a dict with a 'dag' key")],
    tool_context=None,  # ADK injects ToolContext automatically when declared
) -> str:
    """
    Submit the final analysis plan as structured JSON.
    MUST be called to complete the discovery process.
    """
    try:
        from tools.analysis_library import LIBRARY_REGISTRY
        
        # Helper function to extract JSON from mixed text
        def _extract_json_from_text(text: str):
            text = text.strip()
            # 1. Try markdown block
            match = re.search(r'```(?:json)?\s*(.*?)\s*```', text, re.DOTALL)
            if match:
                text = match.group(1).strip()
            
            # 2. Find first [ or {
            start_idx = -1
            for i, c in enumerate(text):
                if c in '{[':
                    start_idx = i
                    break
            
            if start_idx == -1:
                return text # Give up, let json.loads fail naturally
                
            # 3. Find matching closing bracket (string-aware)
            bracket_stack = []
            in_string = False
            escape_next = False
            end_idx = -1
            for i in range(start_idx, len(text)):
                char = text[i]
                if escape_next:
                    escape_next = False
                    continue
                if char == '\\' and in_string:
                    escape_next = True
                    continue
                if char == '"':
                    in_string = not in_string
                    continue
                if not in_string:
                    if char in '{[':
                        bracket_stack.append(char)
                    elif char in '}]':
                        if not bracket_stack:
                            break
                        bracket_stack.pop()
                        if not bracket_stack:
                            end_idx = i
                            break
                        
            if end_idx != -1:
                return text[start_idx:end_idx+1]
                
            return text[start_idx:]
            
        clean_json = _extract_json_from_text(dag_json_str)
        
        try:
            parsed = json.loads(clean_json)
        except json.JSONDecodeError as e:
            print(f"DEBUG: JSONDecodeError: {e}")
            print(f"DEBUG: Raw input: {dag_json_str[:100]}...")
            print(f"DEBUG: Clean input: {clean_json[:100]}...")
            return f"Error parsing JSON: {e}. Please ensure you return valid JSON."
        
        if isinstance(parsed, list):
            nodes = parsed
        elif isinstance(parsed, dict) and "dag" in parsed:
            nodes = parsed["dag"]
        else:
            print(f"DEBUG: Invalid Discovery JSON structure: {type(parsed)}")
            return "Error: Invalid JSON schema. Expected a list of nodes or a dict with a 'dag' key."
            
        final_dag = []
        metrics_ui = []
        
        # -- Alias map: silently remap common LLM-generated synonyms to registered types
        # This keeps the LLM free to propose analyses by any reasonable name while
        # ensuring the coder agent gets a type it can look up. Unknown types without
        # an alias are kept as-is -- the coder agent will write custom code for them.
        ALIAS_MAP = {
            "path_analysis":           "user_journey_analysis",
            "retention_analysis":      "cohort_analysis",
            "time_to_event_analysis":  "trend_analysis",
            "user_flow_analysis":      "user_journey_analysis",
            "journey_analysis":        "user_journey_analysis",
            "churn_analysis":          "survival_analysis",
            "drop_off_analysis":       "dropout_analysis",
            "dropoff_analysis":        "dropout_analysis",
            "event_sequence_analysis": "sequential_pattern_mining",
            "segment_analysis":        "user_segmentation",
        }

        for node in nodes:
            atype = node.get("analysis_type")
            if not atype:
                continue

            # Apply alias remapping before registry lookup
            original_atype = atype
            atype = ALIAS_MAP.get(atype, atype)
            if atype != original_atype:
                print(f"INFO: Remapped analysis_type '{original_atype}' -> '{atype}'")
                node["analysis_type"] = atype

            lib_entry = LIBRARY_REGISTRY.get(atype, {})
            name = node.get("name") or atype.replace("_", " ").title()
            desc = node.get("description") or lib_entry.get("description", atype)
            lib_fn = node.get("library_function") or lib_entry.get("function")
            
            # Extract cohort_window from column_roles if LLM placed it there,
            # then remove it from column_roles so it doesn't confuse the coder agent.
            _col_roles = dict(node.get("column_roles", {}))
            _cohort_window = _col_roles.pop("cohort_window", None)

            spec = {
                "id": node.get("id"),
                "name": name,
                "description": desc,
                "analysis_type": atype,
                "library_function": lib_fn,
                "required_columns": node.get("required_columns", []),
                "column_roles": _col_roles,
                "depends_on": node.get("depends_on", []),
                "priority": node.get("priority", "medium"),
                "feasibility": node.get("feasibility", "HIGH" if lib_fn else "MEDIUM"),
                "status": "pending"
            }
            if _cohort_window and atype == "cohort_analysis":
                spec["cohort_window"] = _cohort_window
            final_dag.append(spec)
            
            metrics_ui.append({
                "id": spec["id"],
                "name": spec["name"],
                "description": spec["description"],
                "analysis_type": spec["analysis_type"],
                "priority": spec["priority"],
                "depends_on": spec["depends_on"],
                "status": "pending"
            })

        # Deduplicate by analysis_type -- keep first occurrence of each type.
        # The LLM sometimes proposes the same analysis_type multiple times with
        # slightly different node IDs, producing near-identical charts.
        _seen_types: set = set()
        _deduped: list = []
        _deduped_ui: list = []
        for _n, _m in zip(final_dag, metrics_ui):
            _at = _n.get("analysis_type")
            if _at and _at in _seen_types:
                print(f"INFO: Dropping duplicate analysis_type='{_at}' (node {_n.get('id')})")
                continue
            if _at:
                _seen_types.add(_at)
            _deduped.append(_n)
            _deduped_ui.append(_m)
        final_dag = _deduped
        metrics_ui = _deduped_ui

        # Check for LOW-feasibility nodes that could benefit from user clarification.
        # If ALL nodes are LOW feasibility, pause the pipeline and request clarification
        # rather than proceeding with a likely-broken DAG.
        _low_feas = [n for n in final_dag if n.get("feasibility", "").upper() == "LOW"]
        if _low_feas:
            try:
                from main import sessions
                _state = sessions.get(session_id)
                if _state:
                    _state.status = "clarification_needed"
                    _state.clarification_request = {
                        "ambiguous_nodes": [
                            {"id": n["id"], "analysis_type": n["analysis_type"],
                             "reason": "feasibility=LOW -- column roles uncertain"}
                            for n in _low_feas
                        ],
                        "message": (
                            "The Discovery Agent could not confidently assign column roles for all planned analyses. "
                            "Please confirm the role of each column below before proceeding."
                        ),
                    }
                    from a2a_messages import create_message, Intent
                    _state.post_message(create_message(
                        intent=Intent.CLARIFICATION_NEEDED,
                        sender="discovery_agent",
                        recipient="orchestrator",
                        payload=_state.clarification_request,
                    ))
            except Exception as _clar_err:
                print(f"WARNING: CLARIFICATION_NEEDED post failed: {_clar_err}")

        plan = {
            "data_summary": (parsed.get("data_summary") if isinstance(parsed, dict) else ""),
            "dag": final_dag,
            "metrics": metrics_ui,
            "node_count": len(final_dag)
        }
        _plan_store[session_id] = plan

        # A2A server mode: write plan to file so orchestrator (main process) can read it
        try:
            from agent_servers.a2a_orchestrator import lookup_session as _lookup_s
            _abs_out = _lookup_s(session_id)
            if _abs_out:
                os.makedirs(_abs_out, exist_ok=True)
                _plan_cache = os.path.join(_abs_out, "_plan_cache.json")
                with open(_plan_cache, "w", encoding="utf-8") as _pf:
                    json.dump(plan, _pf)
                print(f"INFO: [A2A] plan cache written for {session_id}")
        except Exception as _pce:
            print(f"WARNING: [A2A] plan cache write failed: {_pce}")

        # ADK-native: write to ToolContext state so downstream SequentialAgent
        # sub-agents (synthesis, dag_builder) can read dag without importing main
        if tool_context is not None:
            try:
                tool_context.state["dag"] = final_dag
                tool_context.state["plan"] = plan
                tool_context.state["dag_ready"] = True
            except Exception:
                pass

        return f"Successfully stored analysis plan with {len(final_dag)} nodes for session {session_id}."
    except Exception as e:
        try:
            print(f"Discovery parsing error: {traceback.format_exc()}".encode("utf-8", errors="replace").decode("utf-8"))
        except Exception:
            print("Discovery parsing error: (traceback unprintable)")
        return f"Error parsing JSON: {str(e)}"


_discovery_agent_instance = None


def _discovery_after_model_callback(callback_context, llm_response):
    """Validate that the discovery response contains an analysis_plan / dag key.

    Returns a corrective LlmResponse if the required structure is missing or
    the response is not valid JSON. Returns None to keep the original response.
    """
    from google.adk.models.llm_response import LlmResponse
    from google.genai import types

    text = ""
    if llm_response.content and llm_response.content.parts:
        for part in llm_response.content.parts:
            text += getattr(part, "text", "") or ""

    if not text:
        return None

    # Extract JSON block if wrapped in ```json ... ```
    match = re.search(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
    json_text = match.group(1) if match else text
    first = json_text.find("{")
    last = json_text.rfind("}")
    if first != -1 and last != -1:
        json_text = json_text[first:last + 1]

    try:
        data = json.loads(json_text)
        if "dag" not in data and "analysis_plan" not in data:
            return LlmResponse(content=types.Content(parts=[types.Part(text=(
                "Your response must contain a 'dag' key (a JSON array of analysis node objects). "
                "Please rewrite your full response as a JSON object with: "
                "{'data_summary': '...', 'dag': [{'id': 'A1', 'name': '...', "
                "'analysis_type': '...', 'column_roles': {}, 'depends_on': [], "
                "'priority': '...', 'description': '...'}]}"
            ))]))
    except (json.JSONDecodeError, ValueError):
        return LlmResponse(content=types.Content(parts=[types.Part(text=(
            "Your response must be valid JSON wrapped in ```json ... ```. "
            "Please rewrite your analysis plan as valid JSON with a 'dag' array."
        ))]))

    return None


def get_discovery_agent():
    global _discovery_agent_instance
    if _discovery_agent_instance is None:
        from google.adk.agents import Agent
        from google.adk.tools import FunctionTool
        from tools.model_config import get_model
        _discovery_agent_instance = Agent(
            name="discovery_agent",
            model=get_model("discovery"),
            description="Analysis planning specialist. I reason about which analyses are most valuable and build a custom analysis DAG.",
            instruction=_load_prompt("discovery.md"),
            tools=[FunctionTool(tool_submit_analysis_plan)],
            after_model_callback=_discovery_after_model_callback,
        )
    return _discovery_agent_instance

