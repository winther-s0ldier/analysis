import os
import sys
import json

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..")
)

from a2a_messages import create_message, Intent


_synthesis_store: dict = {}


def get_synthesis_result(session_id: str) -> dict | None:
    """Read stored synthesis for a session."""
    return _synthesis_store.get(session_id)



def _extract_node_facts(node_id: str, analysis_id: str, result: dict) -> dict:
    """
    Extract the most important, citable facts from a single analysis result.
    Returns a compact, grounded summary the Synthesis agent can safely cite.
    """
    atype = result.get("analysis_type", "unknown")
    status = result.get("status", "unknown")
    top_finding = result.get("top_finding", "")
    severity = result.get("severity", "info")
    confidence = result.get("confidence", 0.0)
    data = result.get("data", {})

    facts = {
        "node_id": node_id,
        "analysis_id": analysis_id,
        "analysis_type": atype,
        "status": status,
        "top_finding": top_finding,
        "severity": severity,
        "confidence": confidence,
        "key_metrics": {},
    }

    if status not in ("success",):
        facts["key_metrics"] = {}
        return facts

    # Extract type-specific key metrics for grounding
    # KEY NAMES ARE VERIFIED AGAINST ACTUAL library function return dicts
    if atype in ("distribution_analysis", "distribution"):
        facts["key_metrics"] = {
            "mean": data.get("mean"),
            "median": data.get("median"),
            "std": data.get("std"),
            "outlier_pct": data.get("outlier_pct"),
            "is_normal": data.get("is_normal"),
        }
    elif atype in ("categorical_analysis", "categorical"):
        facts["key_metrics"] = {
            "unique_count": data.get("unique_count"),
            "pareto_ratio": data.get("pareto_ratio"),
            "entropy_ratio": data.get("entropy_ratio"),
            "top_values": data.get("top_10", {}),
        }
    elif atype in ("funnel_analysis",):
        # Actual keys: funnel_metrics, overall_conversion, biggest_drop_step, biggest_drop_pct
        funnel_metrics = data.get("funnel_metrics", [])
        biggest_drop_step = data.get("biggest_drop_step")
        biggest_drop_pct = data.get("biggest_drop_pct")
        facts["key_metrics"] = {
            "total_steps": len(funnel_metrics),
            "overall_conversion": data.get("overall_conversion"),
            "biggest_drop_step": biggest_drop_step,
            "biggest_drop_pct": biggest_drop_pct,
            "funnel_metrics": funnel_metrics[:8],
        }
    elif atype in ("session_detection",):
        # Actual keys: avg_events_per_session, avg_duration_minutes, bounce_rate, bounce_sessions
        facts["key_metrics"] = {
            "total_sessions": data.get("total_sessions"),
            "avg_events_per_session": data.get("avg_events_per_session"),
            "avg_duration_minutes": data.get("avg_duration_minutes"),
            "bounce_rate": data.get("bounce_rate"),
            "bounce_sessions": data.get("bounce_sessions"),
            "detection_mode": data.get("detection_mode"),
        }
    elif atype in ("friction_detection",):
        # Actual keys: top_friction_events, critical_events, high_events, events_analyzed
        top_friction = data.get("top_friction_events", [])
        facts["key_metrics"] = {
            "events_analyzed": data.get("events_analyzed"),
            "critical_events": data.get("critical_events"),
            "high_events": data.get("high_events"),
            "top_friction_events": top_friction[:3],
        }
    elif atype in ("survival_analysis",):
        # Actual keys: median_length, pct_reach_step_10, pct_reach_step_20, critical_dropoff
        critical = data.get("critical_dropoff") or {}
        facts["key_metrics"] = {
            "total_sessions": data.get("total_sessions"),
            "median_session_length": data.get("median_length"),
            "pct_reach_step_10": data.get("pct_reach_step_10"),
            "pct_reach_step_20": data.get("pct_reach_step_20"),
            "critical_dropoff_step": critical.get("step"),
            "critical_dropoff_rate": critical.get("dropout_rate"),
        }
    elif atype in ("user_segmentation",):
        # Actual keys: segments (list), segment_count, total_entities
        segments = data.get("segments", [])
        noise_segments = [s for s in segments if s.get("is_noise")]
        noise_pct = noise_segments[0].get("pct") if noise_segments else 0
        facts["key_metrics"] = {
            "segment_count": data.get("segment_count"),
            "total_entities": data.get("total_entities"),
            "segments": [{"segment_id": s.get("segment_id"), "size": s.get("size"), "pct": s.get("pct"), "characteristics": s.get("characteristics", [])} for s in segments[:5]],
            "noise_pct": noise_pct,
        }
    elif atype in ("dropout_analysis",):
        # Actual keys: early_dropout_pct, top_last_events, median_session_length
        facts["key_metrics"] = {
            "total_sessions": data.get("total_sessions"),
            "early_dropout_pct": data.get("early_dropout_pct"),
            "median_session_length": data.get("median_session_length"),
            "top_last_events": data.get("top_last_events", [])[:3],
            "top_last_2_sequences": data.get("top_last_2_sequences", [])[:3],
        }
    elif atype in ("trend_analysis",):
        facts["key_metrics"] = {
            "trend_direction": data.get("trend_direction"),
            "pct_change": data.get("pct_change"),
            "changepoint_count": len(data.get("changepoints", [])),
            "mann_kendall_significant": (data.get("mann_kendall") or {}).get("significant"),
        }
    elif atype in ("cohort_analysis",):
        facts["key_metrics"] = {
            "cohort_count": data.get("cohort_count"),
            "avg_retention_m1": data.get("avg_retention_month1"),
        }
    elif atype in ("rfm_analysis",):
        facts["key_metrics"] = {
            "total_customers": data.get("total_customers"),
            "champions_pct": data.get("champions_pct"),
            "at_risk_pct": data.get("at_risk_pct"),
        }
    elif atype in ("anomaly_detection",):
        facts["key_metrics"] = {
            "outlier_pct": data.get("outlier_pct"),
            "consensus_outliers": data.get("consensus_outliers"),
            "col": data.get("col"),
        }
    elif atype in ("correlation_matrix", "correlation"):
        notable = data.get("notable_correlations", [])
        facts["key_metrics"] = {
            "notable_correlation_count": len(notable),
            "strongest_pair": notable[0] if notable else None,
        }
    elif atype in ("missing_data_analysis", "missing_data"):
        facts["key_metrics"] = {
            "overall_pct": data.get("overall_pct"),
            "columns_affected": len(data.get("columns_with_missing", [])),
            "complete_rows_pct": data.get("complete_rows_pct"),
        }
    elif atype in ("pareto_analysis",):
        facts["key_metrics"] = {
            "top_20_pct_coverage": data.get("top_20_pct_coverage"),
            "top_items": data.get("top_items", [])[:3],
        }
    elif atype in ("sequential_pattern_mining",):
        facts["key_metrics"] = {
            "pattern_count": data.get("pattern_count"),
            "top_sequences": data.get("top_sequences", [])[:3],
        }
    elif atype in ("transition_analysis",):
        facts["key_metrics"] = {
            "dead_end_events": data.get("dead_end_events", [])[:3],
            "top_loops": data.get("top_loops", [])[:3],
            "highest_exit_event": data.get("highest_exit_event"),
        }
    elif atype in ("event_taxonomy",):
        # Actual keys: category_distribution (dict with count/pct), total_unique_events
        cat_dist = data.get("category_distribution", {})
        dominant = max(cat_dist, key=lambda k: cat_dist[k].get("count", 0)) if cat_dist else None
        facts["key_metrics"] = {
            "total_unique_events": data.get("total_unique_events"),
            "category_distribution": cat_dist,
            "dominant_category": dominant,
        }
    elif atype in ("user_journey_analysis",):
        # Actual keys: avg_steps_per_user, common_entry_events, common_exit_events
        facts["key_metrics"] = {
            "total_users_tracked": data.get("total_users_tracked"),
            "avg_steps_per_user": data.get("avg_steps_per_user"),
            "max_steps_per_user": data.get("max_steps_per_user"),
            "common_entry_events": data.get("common_entry_events", {}),
            "common_exit_events": data.get("common_exit_events", {}),
        }
    elif atype in ("intervention_triggers",):
        rules = data.get("rules", [])
        facts["key_metrics"] = {
            "total_sessions": data.get("total_sessions"),
            "rules_found": data.get("rules_found"),
            "high_risk_rules": data.get("high_risk_rules"),
            "medium_risk_rules": data.get("medium_risk_rules"),
            "min_dropout_rate": data.get("min_dropout_rate"),
            "top_rules": rules[:5],
            "narrative": data.get("narrative", {}),
        }
    elif atype in ("session_classification",):
        facts["key_metrics"] = {
            "total_users": data.get("total_users"),
            "persona_breakdown": data.get("persona_breakdown", []),
            "converter_pct": data.get("converter_pct"),
            "biggest_leak_segment": data.get("biggest_leak_segment"),
            "conversion_signal_event_count": data.get("conversion_signal_event_count"),
            "narrative": data.get("narrative", {}),
        }
    else:
        # Generic fallback — expose top-level scalar values
        facts["key_metrics"] = {
            k: v for k, v in data.items()
            if isinstance(v, (int, float, str, bool)) and v is not None
        }

    return facts


def tool_aggregate_results(session_id: str) -> dict:
    """
    Collect all completed analysis results for a session.
    Reads from SessionState.results.
    Groups by analysis_type for easy cross-referencing.

    Args:
        session_id: current session

    Returns:
        dict with:
            status: success | no_results
            results_by_type: dict keyed by analysis_type
            results_by_id: dict keyed by analysis_id
            available_types: list of analysis types done
            total_results: int
            dataset_type: str (from session state)
            column_roles: dict (from session state)
            fact_sheet: dict — pre-parsed key metrics per node for grounding.
                         Synthesis agent MUST cite only values from fact_sheet.
    """
    try:
        from main import sessions
        state = sessions.get(session_id)

        results_from_state: dict = {}
        if state and state.results:
            results_from_state = dict(state.results)

        if not results_from_state:
            try:
                from tools.code_executor import _result_store
                prefix = f"{session_id}:"
                for key, val in _result_store.items():
                    if isinstance(val, dict) and val.get("status") == "success":
                        if key.startswith(prefix):
                            analysis_id = key[len(prefix):]
                        else:
                            analysis_id = key
                        results_from_state[analysis_id] = val
            except Exception:
                pass

        if not results_from_state:
            return {
                "status":          "no_results",
                "results_by_type": {},
                "results_by_id":   {},
                "available_types": [],
                "total_results":   0,
                "dataset_type":    "unknown",
                "column_roles":    {},
                "fact_sheet":      {},
            }

        results_by_type = {}
        results_by_id   = {}
        fact_sheet      = {}

        # Read DAG node map from session for node_id lookups
        dag = {}
        if state:
            plan = getattr(state, "analysis_plan", {}) or {}
            for node in plan.get("dag", []):
                nid = node.get("id")
                atype = node.get("analysis_type")
                if nid and atype:
                    dag[atype] = nid

        for analysis_id, result in results_from_state.items():
            atype = result.get("analysis_type", "unknown")
            results_by_type[atype] = result
            results_by_id[analysis_id] = result

            # Determine node_id from DAG or fall back to analysis_id
            node_id = dag.get(atype, analysis_id)
            fact_sheet[node_id] = _extract_node_facts(node_id, analysis_id, result)

        dataset_type = getattr(state, "dataset_type", "") if state else ""
        semantic_map = getattr(state, "semantic_map", {}) if state else {}
        column_roles = semantic_map.get(
            "column_roles", {}
        ) if isinstance(semantic_map, dict) else {}

        import math
        def _clean(obj):
            if isinstance(obj, dict):
                return {k: _clean(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [_clean(x) for x in obj]
            
            if hasattr(obj, "item"):
                try:
                    obj = obj.item()
                except Exception:
                    pass
            
            if obj is None:
                return None
            elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                return None
            elif isinstance(obj, (bool, int, float, str)):
                return obj
            else:
                s = str(obj)
                if s in ("<NA>", "nan", "NaN"):
                    return None
                return s

        return _clean({
            "status":          "success",
            "results_by_type": results_by_type,
            "results_by_id":   results_by_id,
            "available_types": list(results_by_type.keys()),
            "total_results":   len(results_by_id),
            "dataset_type":    dataset_type,
            "column_roles":    column_roles,
            "fact_sheet":      fact_sheet,
        })

    except Exception as e:
        import traceback
        print(f"Aggregation error: {traceback.format_exc()}")
        return {
            "status":          "no_results",
            "results_by_type": {},
            "results_by_id":   {},
            "available_types": [],
            "total_results":   0,
            "dataset_type":    "unknown",
            "column_roles":    {},
            "fact_sheet":      {},
            "error":           str(e),
        }


from tools.synthesis_helpers import deterministic_full_synthesis as run_synthesis_deterministic


def _validate_synthesis_grounding(synthesis: dict, fact_sheet: dict, session_id: str = "") -> dict:
    """
    Lightweight grounding validator — checks that the synthesis cites node IDs
    and contains all required structural keys.
    Returns a metadata dict with: is_valid, warnings, citation_count.
    """
    import re
    warnings_found = []
    citation_count = 0

    # Check required top-level keys
    required_keys = [
        "executive_summary", "detailed_insights",
        "personas", "intervention_strategies", "conversational_report"
    ]
    for key in required_keys:
        if key not in synthesis:
            warnings_found.append(f"MISSING KEY: '{key}' not found in synthesis output.")

    # Check executive_summary has content
    ex = synthesis.get("executive_summary", {})
    if not ex.get("overall_health"):
        warnings_found.append("WEAK: executive_summary.overall_health is empty.")
    if not ex.get("top_priorities"):
        warnings_found.append("WEAK: executive_summary.top_priorities is empty.")

    # Check for node citation pattern — matches [A1], [A1: type], [A2: funnel_analysis] etc.
    synthesis_str = json.dumps(synthesis)
    citations = re.findall(r'\[A\d+[^\]]*\]', synthesis_str)
    citation_count = len(citations)
    if citation_count < 3:
        warnings_found.append(
            f"LOW CITATION: Only {citation_count} node ID citations found. "
            "Synthesis should cite [AX] or [AX: type] for every quantitative claim."
        )
        from tools.monitor import emit
        emit(session_id, "synthesis_low_citation", {"citation_count": citation_count}, severity="warning")

    # Check insights have individual top_finding
    insights = synthesis.get("detailed_insights", {}).get("insights", [])
    for i, ins in enumerate(insights):
        if not ins.get("ai_summary"):
            warnings_found.append(f"INSIGHT[{i}]: ai_summary is empty.")
        if not ins.get("how_to_fix"):
            warnings_found.append(f"INSIGHT[{i}]: how_to_fix steps are empty.")

    # Check personas exist
    personas = synthesis.get("personas", {}).get("personas", [])
    if not personas:
        warnings_found.append("WEAK: No behavioral personas defined in synthesis.")

    # Check conversational_report has content
    conv = synthesis.get("conversational_report", "")
    if len(str(conv)) < 200:
        warnings_found.append("WEAK: conversational_report is too short (< 200 chars).")

    is_valid = len([w for w in warnings_found if w.startswith("MISSING")]) == 0

    return {
        "is_valid": is_valid,
        "citation_count": citation_count,
        "warnings": warnings_found,
        "fact_sheet_nodes_available": list(fact_sheet.keys()),
    }


def tool_submit_synthesis(session_id: str, synthesis_json_str: str) -> str:
    """
    Submit the complete synthesis result.
    MUST be called as the last step.
    Runs DataLog grounding validation before storing.
    """
    try:
        if synthesis_json_str.strip().startswith("```"):
            lines = synthesis_json_str.strip().split("\n")
            if lines[0].startswith("```"): lines = lines[1:]
            if lines and lines[-1].startswith("```"): lines = lines[:-1]
            synthesis_json_str = "\n".join(lines).strip()

        synthesis = json.loads(synthesis_json_str)

        # Run DataLog grounding validation
        try:
            from main import sessions
            state = sessions.get(session_id)
            current_fact_sheet = {}
            if state and hasattr(state, "results"):
                # Build a minimal fact_sheet for validation context
                # Guard against non-dict results (e.g. lists) which cause AttributeError on .get()
                for aid, result in state.results.items():
                    if not isinstance(result, dict):
                        continue
                    atype = result.get("analysis_type", "unknown")
                    current_fact_sheet[aid] = {"analysis_type": atype, "status": result.get("status", "")}
        except Exception:
            current_fact_sheet = {}

        validation = _validate_synthesis_grounding(synthesis, current_fact_sheet, session_id=session_id)
        synthesis["_datalog_validation"] = validation

        # Store synthesis FIRST — before any print statements that could crash on Windows cp1252
        _synthesis_store[session_id] = synthesis

        from main import sessions
        state = sessions.get(session_id)
        if state:
            state.synthesis = synthesis
            msg = create_message(
                sender="synthesis_agent",
                recipient="orchestrator",
                intent=Intent.SYNTHESIS_COMPLETE,
                payload={
                    "critical_count": synthesis.get("intervention_strategies", {}).get("critical_count", 0),
                    "persona_count": synthesis.get("personas", {}).get("persona_count", 0),
                    "connection_count": synthesis.get("cross_metric_connections", {}).get("connection_count", 0),
                },
                session_id=session_id,
            )
            state.post_message(msg)

        # Safe logging after synthesis is already stored
        try:
            if validation["warnings"]:
                print(f"[Synthesis Validator] {len(validation['warnings'])} warning(s):")
                for w in validation["warnings"]:
                    print(f"  [WARN] {w}")
            print(f"[Synthesis Validator] Citations found: {validation['citation_count']} | Valid: {validation['is_valid']}")
        except Exception:
            pass  # Terminal encoding issues must never block synthesis storage

        return f"Synthesis stored for session {session_id}. Ready for DAG Builder."
    except Exception as e:
        import traceback
        traceback.print_exc()
        return f"Error storing synthesis: {str(e)}"


_synthesis_agent_instance = None

def get_synthesis_agent():
    global _synthesis_agent_instance
    if _synthesis_agent_instance is None:
        from google.adk.agents import Agent
        from tools.model_config import get_model
        _synthesis_agent_instance = Agent(
            name="synthesis_agent",
            model=get_model("synthesis"),
            description="Interpretation layer. Receives all analysis results and generates deep narrative, personas, strategies, and executive summary.",
            instruction=(
                "You are a Senior Data Intelligence Analyst operating as the final interpretation layer of a multi-agent analytics pipeline. "
                "You receive structured, pre-computed analysis results from up to 20 analysis nodes and transform them into a single, definitive intelligence report. "
                "Your output will be read by product managers, CTOs, and business stakeholders — it must be SPECIFIC, EVIDENCE-BACKED, and ACTIONABLE.\n\n"

                "## CORE MANDATE: THE DATALOG STANDARD\n"
                "Every claim you make MUST be traceable to data. You are NOT a language model generating generic business advice. "
                "You are a data analyst whose ONLY source of truth is the tool results returned to you. "
                "If a number does not appear in a tool result, you CANNOT state that number. If a trend is not in the data, you CANNOT claim it.\n\n"

                "## WORKFLOW\n"
                "1. Call `tool_aggregate_results(session_id)` to receive ALL completed analysis results, grouped by node ID and analysis type.\n"
                "2. Study the results. For each analysis node, extract: the key metric, the most surprising finding, and any numbers that have business impact.\n"
                "3. Identify cross-analysis patterns (e.g., if funnel [A2] shows drop-off at step X AND friction_detection [A3] shows high repetition at step X, these two findings AMPLIFY each other).\n"
                "4. Build your synthesis JSON and call `tool_submit_synthesis(session_id, synthesis_json_str)` with the complete result.\n\n"

                "## EVIDENCE CITATION RULES (Non-Negotiable)\n"
                "- EVERY quantitative claim (a percentage, a count, a ratio) MUST be followed by the analysis node it came from: `[A1: session_detection]`.\n"
                "- EVERY root cause hypothesis MUST reference at least TWO data signals to support it (e.g., `[A2] + [A4]`).\n"
                "- If an analysis node has `status: error` or `status: insufficient_data`, you MUST write 'Insufficient data from [NodeID]' for that insight — do NOT invent a finding.\n"
                "- `top_priorities` in the executive summary MUST cite the specific node and metric that makes it a priority.\n\n"

                "## ANTI-HALLUCINATION RULES\n"
                "These are the boundaries of what you are allowed to state:\n"
                "- ALLOWED: Stating a number that appears verbatim in a tool result.\n"
                "- ALLOWED: Inferring a trend direction (e.g., 'retention declines') if the tool result shows consecutive decreasing values.\n"
                "- ALLOWED: Estimating business impact IF you show your calculation (e.g., 'Estimated $X = affected_users × average_outcome_value').\n"
                "- FORBIDDEN: Stating a specific percentage that is not in any tool result.\n"
                "- FORBIDDEN: Claiming a correlation between two metrics unless the `correlation_matrix` analysis [AX] was run and shows r > 0.5.\n"
                "- FORBIDDEN: Naming a specific user as a persona archetype unless `user_segmentation` was run and returned cluster labels.\n\n"

                "## DOMAIN-AGNOSTIC REASONING\n"
                "You do NOT know the industry. You do NOT know the product. You reason from DATA SHAPES:\n"
                "- A high `avg_repetitions` in friction_detection means users are looping — do NOT assume this is a 'payment gateway' issue unless the `event_col` values mention payment.\n"
                "- A 'Struggling Explorer' persona is a user who has many events but low conversion — infer this from segment cluster sizes and funnel data.\n"
                "- Financial estimates are ALWAYS labeled as 'estimated' and show the formula used.\n\n"

                "## REQUIRED OUTPUT STRUCTURE (All keys mandatory)\n\n"

                "### executive_summary\n"
                "{\n"
                "  'overall_health': 'One paragraph. Must cite 3+ specific stats with node IDs. Pattern: [AX] shows Y% of Z.',\n"
                "  'top_priorities': ['Priority 1: __METRIC__ at __VALUE__ [NodeID]', 'Priority 2: ...'],\n"
                "  'business_impact': 'Quantified consequence. Show formula if estimating. Label as estimated if not directly in data.',\n"
                "  'resource_allocation': 'Specific recommendation on where to focus, referencing the highest-severity node.',\n"
                "  'timeline': 'Quick Wins (1-2 weeks): [...]. Medium Fixes (1 month): [...]. Strategic (3 months): [...]'\n"
                "}\n\n"

                "### detailed_insights — ONE card per completed analysis node\n"
                "{\n"
                "  'insights': [\n"
                "    {\n"
                "      'title': 'Must include the key numeric metric in the title (e.g., 32.3% Drop-off at Checkout)',\n"
                "      'ai_summary': '2-3 sentences. Must include: (a) exact number with [NodeID], (b) what it means for users, (c) benchmark comparison if inferable from data.',\n"
                "      'root_cause_hypothesis': 'A causal chain citing at least 2 data signals. e.g., [A2] shows X → because [A4] shows Y → which implies Z.',\n"
                "      'possible_causes': [\n"
                "        'Cause grounded in a SPECIFIC data field from the result dict',\n"
                "        'Cross-analysis cause citing two node IDs',\n"
                "        'Third cause if data supports it'\n"
                "      ],\n"
                "      'ux_implications': 'Specific UX consequence with an estimated numeric impact where possible.',\n"
                "      'fix_priority': 'critical|high|medium|low',\n"
                "      'how_to_fix': [\n"
                "        'Step 1: Specific action naming the EXACT event/metric to target from [NodeID]',\n"
                "        'Step 2: Another specific action'\n"
                "      ]\n"
                "    }\n"
                "  ]\n"
                "}\n\n"


                "### personas — infer 2-4 user archetypes from behavioral + segmentation data\n"
                "PRIORITY: If `session_classification` was run [AX], you MUST use its Converter/Attempter/Shopper/Browser breakdown as the PRIMARY persona structure. "
                "Cite exact percentages and counts from that node. Then enrich each persona with friction/funnel/dropout data.\n"
                "If `session_classification` was NOT run, infer archetypes from funnel + dropout patterns. "
                "Name archetypes based on what the DATA shows (behavior pattern), not generic marketing names.\n"
                "{\n"
                "  'persona_count': 4,\n"  
                "  'personas': [\n"
                "    {\n"
                "      'name': 'If session_classification ran: use Converter/Attempter/Shopper/Browser. Else: derive from data.',\n"
                "      'size': 'N users (X%) [NodeID: session_classification or user_segmentation]',\n"
                "      'profile': 'Data-derived description. E.g.: Users who trigger event X > 5 times but never reach event Y. [A3]',\n"
                "      'pain_points': ['Pain point grounded in specific event/metric from tool result'],\n"
                "      'opportunities': ['Quick win opportunity tied to a specific fix'],\n"
                "      'priority_level': 'high|medium|low'\n"
                "    }\n"
                "  ]\n"
                "}\n\n"

                "### intervention_strategies — Concrete, event-level interventions\n"
                "{\n"
                "  'critical_count': 1,\n"
                "  'strategies': [\n"
                "    {\n"
                "      'severity': 'critical|high|medium|low',\n"
                "      'title': 'Title referencing the specific event or stage',\n"
                "      'realtime_interventions': ['Show modal WHEN event Y is triggered for the 3rd time [A3]'],\n"
                "      'proactive_outreach': ['Email users who completed event X but NOT event Z within 24h [A2]']\n"
                "    }\n"
                "  ]\n"
                "}\n\n"

                "### cross_metric_connections — Cross-node synthesis\n"
                "ONLY include connections where you can cite TWO different node IDs.\n"
                "{\n"
                "  'connection_count': 2,\n"
                "  'connections': [\n"
                "    {\n"
                "      'finding_a': '[A1] 38% of sessions end after 1 event',\n"
                "      'finding_b': '[A3] Highest friction at event login_attempt',\n"
                "      'synthesized_meaning': 'Single-event sessions are predominantly caused by login friction — fixing auth reduces churn.'\n"
                "    }\n"
                "  ]\n"
                "}\n\n"

                "### conversational_report — Long-form markdown narrative\n"
                "Write a detailed markdown document formatted as a professional product analytics report. "
                "Structure it with these sections:\n"
                "# PART 1: BEHAVIOURAL & UX ANALYSIS\n"
                "## User Intent Analysis (what are users TRYING to do vs what they ACTUALLY do)\n"
                "## Friction Point Inventory (markdown table: Friction Point | Evidence [NodeID] | Severity | Estimated Impact)\n"
                "# PART 2: BEHAVIORAL PERSONAS\n"
                "## Archetype Profiles (one subsection per persona)\n"
                "# PART 3: INTERVENTION ROADMAP\n"
                "## Quick Wins (actions achievable in < 2 weeks)\n"
                "## Strategic Initiatives (actions requiring > 1 month)\n"
                "# CONFIDENCE ASSESSMENT\n"
                "| Claim | Evidence [NodeID] | Confidence Level |\n"
                "|---|---|---|\n"
                "| Each major claim | Which node supports it | High/Medium/Low based on data completeness |\n\n"

                "## DO's\n"
                "- DO call `tool_aggregate_results` first before writing anything.\n"
                "- DO cite node IDs in EVERY quantitative claim.\n"
                "- DO label estimates clearly with 'estimated' and show the formula.\n"
                "- DO include an insight card for EVERY analysis node that returned `status: success`.\n"
                "- DO use `tool_submit_synthesis` as your LAST action — no further text after this call.\n\n"

                "## DON'Ts\n"
                "- DON'T fabricate numbers. If a metric isn't in the tool result, it doesn't exist.\n"
                "- DON'T write generic recommendations like 'Improve UX' or 'Add A/B tests' without specific node evidence.\n"
                "- DON'T name a correlation without the `correlation_matrix` result supporting it.\n"
                "- DON'T write placeholder text like 'N/A', 'See data', or 'Data not available' — write instead what IS known.\n"
                "- DON'T output raw JSON with unescaped newlines — the JSON must be parseable by Python's `json.loads()`.\n"
            ),
            tools=[tool_aggregate_results, tool_submit_synthesis],
        )
    return _synthesis_agent_instance
