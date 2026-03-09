import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from a2a_messages import (
    create_message,
    Intent,
    MetricSpec,
)


_plan_store: dict = {}


def get_analysis_plan(session_id: str) -> dict | None:
    """Read and consume stored plan for a session."""
    return _plan_store.pop(session_id, None)


def build_dag_deterministic(
    dataset_type: str,
    column_roles: dict,
    selected_analyses: list,
    row_count: int,
    custom_nodes: list = None,
) -> dict:
    """
    Deterministic version of tool_build_analysis_dag.
    Used for fallback paths and main.py.
    """
    dag = []
    node_counter = [0]

    def next_id() -> str:
        node_counter[0] += 1
        return f"A{node_counter[0]}"

    entity_col  = column_roles.get("entity_col")
    time_col    = column_roles.get("time_col")
    event_col   = column_roles.get("event_col")
    outcome_col = column_roles.get("outcome_col")

    from tools.analysis_library import LIBRARY_REGISTRY

    BEHAVIORAL = {
        "funnel_analysis", "friction_detection", "survival_analysis", 
        "user_segmentation", "sequential_pattern_mining", "association_rules",
        "transition_analysis", "dropout_analysis",
    }

    ROLE_MAP = {
        "session_detection": {
        "entity_col": entity_col,
        "time_col": time_col,
        "event_col": event_col,
    },
    "funnel_analysis": {"entity_col": entity_col, "time_col": time_col, "event_col": event_col, "session_col": "session_id"},
        "friction_detection": {"entity_col": entity_col, "event_col": event_col, "session_col": "session_id"},
        "survival_analysis": {"entity_col": entity_col, "event_col": event_col, "session_col": "session_id"},
        "sequential_pattern_mining": {"entity_col": entity_col, "event_col": event_col, "session_col": "session_id"},
        "user_segmentation": {"entity_col": entity_col, "time_col": time_col, "event_col": event_col, "session_col": "session_id"},
        "association_rules": {"entity_col": entity_col, "event_col": event_col, "session_col": "session_id"},
        "transition_analysis": {"entity_col": entity_col, "time_col": time_col, "event_col": event_col},
        "dropout_analysis": {"entity_col": entity_col, "time_col": time_col, "event_col": event_col},
        "event_taxonomy": {"event_col": event_col},
        "trend_analysis": {"time_col": time_col, "value_col": outcome_col},
        "anomaly_detection": {"col": outcome_col},
        "distribution_analysis": {"col": outcome_col},
        "correlation_matrix": {},
        "missing_data_analysis": {},
        "categorical_analysis": {"col": event_col or ""},
        "pareto_analysis": {"entity_col": entity_col, "value_col": outcome_col},
        "rfm_analysis": {"entity_col": entity_col, "time_col": time_col, "value_col": outcome_col},
        "cohort_analysis": {"entity_col": entity_col, "time_col": time_col, "value_col": outcome_col},
        "time_series_decomposition": {"time_col": time_col, "value_col": outcome_col},
    }

    id_map = {}
    has_behavioral = any(a in BEHAVIORAL for a in selected_analyses)
    if has_behavioral and "session_detection" not in selected_analyses:
        selected_analyses = ["session_detection"] + list(selected_analyses)

    for atype in selected_analyses:
        nid = next_id()
        id_map[atype] = nid
        lib_entry = LIBRARY_REGISTRY.get(atype, {})
        lib_fn = lib_entry.get("function") if lib_entry else None
        deps = []
        if atype in BEHAVIORAL and "session_detection" in id_map:
            deps.append(id_map["session_detection"])
        if atype == "association_rules" and "funnel_analysis" in id_map:
            deps.append(id_map["funnel_analysis"])

        roles = ROLE_MAP.get(atype, {})
        req_cols = [v for v in roles.values() if v and v != "session_id"]
        dag.append(MetricSpec(
            id=nid, name=atype.replace("_", " ").title(),
            description=(lib_entry.get("description", atype) if lib_entry else atype),
            analysis_type=atype, library_function=lib_fn,
            required_columns=req_cols, column_roles=roles, depends_on=deps,
            enables=[], priority=("critical" if atype == "session_detection" else "high" if atype in BEHAVIORAL else "medium"),
            feasibility="HIGH" if lib_fn else "MEDIUM",
        ).to_dict())

    if custom_nodes:
        for cn in custom_nodes:
            cid = next_id()
            cn_type = cn.get("analysis_type", "custom")
            lib_entry = LIBRARY_REGISTRY.get(cn_type, {})
            lib_fn = lib_entry.get("function") if lib_entry else cn.get("library_function")
            deps = [id_map[dt] for dt in cn.get("depends_on_types", []) if dt in id_map]
            dag.append(MetricSpec(
                id=cid, name=cn.get("name", cn_type.replace("_", " ").title()),
                description=cn.get("description", cn_type),
                analysis_type=cn_type, library_function=lib_fn,
                required_columns=cn.get("required_columns", []),
                column_roles=cn.get("column_roles", {}),
                depends_on=deps, enables=[], priority=cn.get("priority", "medium"),
                feasibility="HIGH" if lib_fn else "MEDIUM",
            ).to_dict())

    data_summary = f"{dataset_type.replace('_', ' ').title()} dataset with {row_count:,} rows. {len(dag)} analyses planned. Column roles: entity={column_roles.get('entity_col', 'none')}, time={column_roles.get('time_col', 'none')}, event={column_roles.get('event_col', 'none')}, outcome={column_roles.get('outcome_col', 'none')}."
    return {"status": "success", "dag": dag, "data_summary": data_summary, "node_count": len(dag)}


def tool_submit_analysis_plan(session_id: str, dag_json_str: str) -> str:
    """
    Submit the final analysis plan as structured JSON.
    MUST be called to complete the discovery process.
    """
    try:
        from tools.analysis_library import LIBRARY_REGISTRY
        
        import re
        json_match = re.search(r"```json\s*(.*?)\s*```", dag_json_str, re.DOTALL)
        clean_json = json_match.group(1) if json_match else dag_json_str.strip()
        
        parsed = json.loads(clean_json)
        
        if isinstance(parsed, list):
            nodes = parsed
        elif isinstance(parsed, dict) and "dag" in parsed:
            nodes = parsed["dag"]
        else:
            print(f"DEBUG: Invalid Discovery JSON structure: {type(parsed)}")
            return "Error: Invalid JSON schema. Expected a list of nodes or a dict with a 'dag' key."
            
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
            
            metrics_ui.append({
                "id": spec["id"],
                "name": spec["name"],
                "description": spec["description"],
                "analysis_type": spec["analysis_type"],
                "priority": spec["priority"],
                "depends_on": spec["depends_on"],
                "status": "pending"
            })

        plan = {
            "data_summary": (parsed.get("data_summary") if isinstance(parsed, dict) else ""),
            "dag": final_dag,
            "metrics": metrics_ui,
            "node_count": len(final_dag)
        }
        _plan_store[session_id] = plan
        return f"Successfully stored analysis plan with {len(final_dag)} nodes for session {session_id}."
    except Exception as e:
        import traceback
        print(f"Discovery parsing error: {traceback.format_exc()}")
        return f"Error parsing JSON: {str(e)}"


_discovery_agent_instance = None

def get_discovery_agent():
    global _discovery_agent_instance
    if _discovery_agent_instance is None:
        from google.adk.agents import Agent
        from tools.model_config import get_model
        _discovery_agent_instance = Agent(
            name="discovery_agent",
            model=get_model("discovery"),
            description="Analysis planning specialist. I reason about which analyses are most valuable and build a custom analysis DAG.",
            instruction=(
                "You are an Elite Data Architect and Intelligence Strategist. Your goal is to map any unknown "
                "event-stream dataset into a deep behavioral intelligence DAG.\n\n"
                
                "## 1. DOMAIN-AGNOSTIC REASONING\n"
                "You must be COMPLETELY DOMAIN-AGNOSTIC. Reasoning purely based on structure and distributions. "
                "Look at the profile provided and identify the best insights for THIS specific data.\n\n"
                
                "## 2. DYNAMIC SESSION ANCHORING (A1 - CRITICAL)\n"
                "The first node (A1) MUST be `session_detection`. Identify 'Anchors' (Session Start markers) by looking "
                "at the most frequent early events in the profile. Add these to node A1's `column_roles` under `marker_events`.\n\n"
                
                "## 3. AVAILABLE ANALYSES\n"
                "### Statistical / General\n"
                "- distribution_analysis: histograms, box plots, normality (numeric)\n"
                "- categorical_analysis: frequencies, Pareto, entropy (categorical)\n"
                "- correlation_matrix: Pearson + Spearman correlations (numeric)\n"
                "- anomaly_detection: IQR + Z-score outliers (numeric)\n"
                "- missing_data_analysis: missingness patterns\n"
                "- pareto_analysis: 80/20 contribution (category, value)\n\n"
                
                "### Time-Based / Retention\n"
                "- trend_analysis: Rolling averages, change-points (time, value)\n"
                "- cohort_analysis: Cohort retention tracking (entity, time, value)\n"
                "- rfm_analysis: RFM segmentation (entity, time, value)\n"
                "- time_series_decomposition: STL decomposition (time, value)\n\n"
                
                "### Behavioral (Requires A1 session_detection)\n"
                "- funnel_analysis: Path conversion rates (entity, event, time)\n"
                "- friction_detection: Repeat attempt loops (entity, event)\n"
                "- survival_analysis: Kaplan-Meier session survival (entity, event)\n"
                "- user_segmentation: DBSCAN behavioral clustering (entity, event)\n"
                "- sequential_pattern_mining: Frequent sub-sequences (entity, event)\n"
                "- transition_analysis: Markov transition matrix (entity, event)\n"
                "- dropout_analysis: Events before early exit (entity, event)\n\n"
                
                "## 4. SCHEMA RULES\n"
                "You MUST provide a `description` for every node that explains WHY this analysis is valuable for this data.\n\n"
                "Example A1 node:\n"
                "{\n"
                '  "id": "A1",\n'
                '  "name": "Dynamic Session Anchoring",\n'
                '  "description": "Establishes user session boundaries by detecting Entry Anchors like [EventName]...",\n'
                '  "analysis_type": "session_detection",\n'
                '  "column_roles": {\n'
                '    "entity_col": "...", "time_col": "...", "event_col": "...",\n'
                '    "marker_events": ["Learned_Marker_A", "Learned_Marker_B"]\n'
                "  },\n"
                '  "depends_on": [], "priority": "critical"\n'
                "}"
            ),
            tools=[tool_submit_analysis_plan],
        )
    return _discovery_agent_instance
    return _discovery_agent_instance
