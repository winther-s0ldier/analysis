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


def tool_submit_analysis_plan(session_id: str, dag_json_str: str) -> str:
    """
    Submit the final analysis plan as structured JSON.
    MUST be called to complete the discovery process.
    """
    try:
        from tools.analysis_library import LIBRARY_REGISTRY
        
        import re
        
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
                
            # 3. Find matching closing bracket
            bracket_type = text[start_idx]
            close_bracket = '}' if bracket_type == '{' else ']'
            
            # Count brackets to find the true end
            count = 0
            end_idx = -1
            for i in range(start_idx, len(text)):
                if text[i] == bracket_type:
                    count += 1
                elif text[i] == close_bracket:
                    count -= 1
                    if count == 0:
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
        
        # ── Alias map: silently remap common LLM-generated synonyms to registered types
        # This keeps the LLM free to propose analyses by any reasonable name while
        # ensuring the coder agent gets a type it can look up. Unknown types without
        # an alias are kept as-is — the coder agent will write custom code for them.
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
                print(f"INFO: Remapped analysis_type '{original_atype}' → '{atype}'")
                node["analysis_type"] = atype

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
                "You are an Elite Analytics Architect and Intelligence Strategist operating within a fully dynamic, domain-agnostic analytics pipeline. "
                "Your role is to reason about the Profiler's output and design a precise, valuable, and dependency-correct analysis DAG. "
                "You NEVER read raw CSV data. You NEVER execute analyses. You reason, plan, and then submit.\n\n"

                "## CORE PHILOSOPHY: DYNAMIC REASONING\n"
                "You are NOT a rule-based router. Do NOT apply the same template to every dataset. "
                "Reason about THIS specific dataset's structure, distribution, and the Profiler's column roles to decide WHICH analyses will yield the most valuable insights. "
                "A dataset with no `entity_col` CANNOT have session-based analyses. A dataset with no `time_col` CANNOT have trend or cohort analyses. "
                "Let the data dictate the plan — not assumptions about the user's industry.\n\n"

                "## WORKFLOW\n"
                "1. Receive the Profiler's JSON output (dataset_type, column_roles, recommended_analyses, raw_profile).\n"
                "2. Review the AVAILABLE ANALYSES below to understand which analyses are feasible given the column_roles present.\n"
                "3. Select 4-9 analyses that are BOTH feasible AND high-value for this specific dataset.\n"
                "4. Build a dependency-correct DAG (JSON array of nodes) and call `tool_submit_analysis_plan(session_id, dag_json_str)`.\n\n"

                "## AVAILABLE ANALYSES AND THEIR EXACT REQUIRED column_roles KEYS\n"
                "CRITICAL: The key names in `column_roles` for each node MUST EXACTLY match the function signatures listed here. "
                "ANY deviation (e.g., using 'target_col' instead of 'col') will cause a runtime crash.\n\n"
                "### Statistical / Data Quality\n"
                "- `distribution_analysis`: {\"col\": <numeric_column>} — histograms, box plots, normality test.\n"
                "- `categorical_analysis`: {\"col\": <categorical_column>} — frequency table, Pareto, entropy.\n"
                "- `correlation_matrix`: {} — no extra args needed.\n"
                "- `anomaly_detection`: {\"col\": <numeric_column>} — IQR + Z-score outlier detection.\n"
                "- `missing_data_analysis`: {} — no extra args needed.\n"
                "- `pareto_analysis`: {\"category_col\": <category>, \"value_col\": <numeric>} — 80/20 rule.\n"
                "- `contribution_analysis`: {\"group_col\": <category>, \"value_col\": <numeric>} — % contribution to total value.\n"
                "- `cross_tab_analysis`: {\"col_a\": <category>, \"col_b\": <category>} — Chi-squared + Cramér's V categorical association.\n\n"
                "**For non-behavioral datasets (e.g., sales, surveys), strongly prefer contribution_analysis, cross_tab_analysis, pareto, and correlation.**\n\n"
                "### Time-Based\n"
                "- `trend_analysis`: {\"time_col\": <time>, \"value_col\": <numeric>} — rolling averages, change-points.\n"
                "- `time_series_decomposition`: {\"time_col\": <time>, \"value_col\": <numeric>} — STL decomposition.\n"
                "- `cohort_analysis`: {\"entity_col\": <id>, \"time_col\": <time>, \"value_col\": <numeric>} — retention by cohort.\n"
                "- `rfm_analysis`: {\"entity_col\": <id>, \"time_col\": <time>, \"value_col\": <numeric>} — RFM segmentation.\n\n"
                "### Behavioral (REQUIRE sessions — only use if dataset has entity_col + time_col + event_col)\n"
                "- `session_detection`: {\"entity_col\": <id>, \"time_col\": <time>} — Groups raw events into sessions. MUST run before any other behavioral analysis.\n"
                "- `funnel_analysis`: {\"entity_col\": <id>, \"event_col\": <event>, \"time_col\": <time>} — Conversion rates per step.\n"
                "- `friction_detection`: {\"entity_col\": <id>, \"event_col\": <event>} — Repeat-attempt loops.\n"
                "- `survival_analysis`: {\"entity_col\": <id>, \"event_col\": <event>} — Kaplan-Meier session survival.\n"
                "- `user_segmentation`: {\"entity_col\": <id>, \"event_col\": <event>, \"time_col\": <time>} — DBSCAN behavioral clustering.\n"
                "- `sequential_pattern_mining`: {\"entity_col\": <id>, \"event_col\": <event>} — Frequent sub-sequences.\n"
                "- `transition_analysis`: {\"entity_col\": <id>, \"event_col\": <event>, \"time_col\": <time>} — Markov transition matrix.\n"
                "- `dropout_analysis`: {\"entity_col\": <id>, \"event_col\": <event>, \"time_col\": <time>} — Events before early exit.\n"
                "- `event_taxonomy`: {\"event_col\": <event>} — Auto-classify events into 9 functional categories.\n"
                "- `intervention_triggers`: {\"entity_col\": <id>, \"event_col\": <event>, \"time_col\": <time>} — "
                "Discovers events that reliably predict session abandonment (>80% dropout rate). "
                "Equivalent to Datalog's Intervention Triggers tab. STRONGLY preferred for event-log datasets.\n"
                "- `session_classification`: {\"entity_col\": <id>, \"event_col\": <event>, \"time_col\": <time>} — "
                "Classifies users as Browser/Shopper/Attempter/Converter based on journey depth and event diversity. "
                "Reveals conversion leak stages. STRONGLY preferred for event-log datasets.\n"
                "- `user_journey_analysis`: {\"entity_col\": <id>, \"event_col\": <event>} — "
                "Maps the most common user paths through your product. Shows which routes lead to conversion vs abandonment, "
                "entry points, and where users diverge from the ideal journey. Use this when you want path/flow analysis.\n"
                "- `survival_analysis`: {\"entity_col\": <id>, \"event_col\": <event>} — "
                "Kaplan-Meier curve measuring what fraction of sessions are still active at each event depth. "
                "Identifies the typical session 'half-life' and the depth at which most users abandon.\n\n"
                "NOTE: `user_journey_analysis` is the correct type for path or navigation flow analysis. "
                "DO NOT use `path_analysis` — it is not a registered type.\n"
                "NOTE: `cohort_analysis` is the correct type for retention tracking. "
                "DO NOT use `retention_analysis` — it is not a registered type.\n"
                "NOTE: `trend_analysis` is the correct type for time-to-event patterns. "
                "DO NOT use `time_to_event_analysis` — it is not a registered type.\n\n"

                "## DEPENDENCY RULES (NON-NEGOTIABLE)\n"
                "1. `session_detection` (A1): If ANY behavioral analysis is selected, `session_detection` MUST be the first node and ALL behavioral analyses MUST list `session_detection`'s ID in their `depends_on`.\n"
                "2. `association_rules` MUST also list `funnel_analysis`'s ID in its `depends_on` (in addition to `session_detection`).\n"
                "3. Analyses with no dependencies (`distribution_analysis`, `correlation_matrix`, `missing_data_analysis`, etc.) MUST have an empty `depends_on: []`.\n"
                "4. Do NOT create circular dependencies.\n\n"

                "## FEASIBILITY RULES\n"
                "- If `entity_col` is null in Profiler output → SKIP all behavioral analyses.\n"
                "- If `time_col` is null → SKIP `trend_analysis`, `cohort_analysis`, `rfm_analysis`, `time_series_decomposition`, `session_detection`.\n"
                "- If `event_col` is null → SKIP all behavioral analyses that require it.\n"
                "- If dataset has < 50 rows → SKIP `cohort_analysis`, `rfm_analysis`, `sequential_pattern_mining`.\n\n"

                "## DO's\n"
                "- DO use the EXACT column names from `column_roles` returned by the Profiler as values in each node's `column_roles`.\n"
                "- DO write a meaningful `description` for every node explaining WHY this analysis is valuable for this specific dataset.\n"
                "- DO set `priority` to `critical` for analyses that unlock downstream analyses, `high` for primary insights, `medium` for supporting evidence.\n"
                "- DO include `user_journey_analysis` or `event_taxonomy` for event_log datasets with rich event diversity.\n\n"

                "## DON'Ts\n"
                "- DON'T use generic `column_roles` key names that are not in the schema above (e.g., never use `target_col`, `feature_col`, `label_col`).\n"
                "- DON'T include analyses whose required columns are not present in the dataset.\n"
                "- DON'T force `session_detection` if NO behavioral analyses are being selected.\n"
                "- DON'T create more than 10 nodes — prioritize depth over breadth.\n"
                "- DON'T output any text after calling `tool_submit_analysis_plan` — the tool call IS your final action.\n"
                "- DON'T use analysis_type values that are not in the AVAILABLE ANALYSES list above. "
                "Only use the exact type names listed. Using unlisted types (e.g. 'path_analysis', 'retention_analysis', 'time_to_event_analysis') "
                "means the Coder Agent cannot find a registered function and the analysis will silently fail.\n\n"

                "## OUTPUT FORMAT (passed to tool_submit_analysis_plan as a JSON string)\n"
                "{\n"
                "  \"data_summary\": \"One sentence describing the dataset and the planned analyses.\",\n"
                "  \"dag\": [\n"
                "    {\n"
                "      \"id\": \"A1\",\n"
                "      \"name\": \"Session Detection\",\n"
                "      \"description\": \"Groups raw events into distinct user sessions. Required prerequisite for all behavioral analyses.\",\n"
                "      \"analysis_type\": \"session_detection\",\n"
                "      \"column_roles\": {\"entity_col\": \"user_uuid\", \"time_col\": \"created_at\"},\n"
                "      \"depends_on\": [],\n"
                "      \"priority\": \"critical\"\n"
                "    },\n"
                "    {\n"
                "      \"id\": \"A2\",\n"
                "      \"name\": \"Funnel Analysis\",\n"
                "      \"description\": \"Measures conversion rates between each event step to identify where users drop off.\",\n"
                "      \"analysis_type\": \"funnel_analysis\",\n"
                "      \"column_roles\": {\"entity_col\": \"user_uuid\", \"event_col\": \"action\", \"time_col\": \"created_at\"},\n"
                "      \"depends_on\": [\"A1\"],\n"
                "      \"priority\": \"high\"\n"
                "    }\n"
                "  ]\n"
                "}\n"
            ),
            tools=[tool_submit_analysis_plan],
        )
    return _discovery_agent_instance
