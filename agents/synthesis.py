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
            }

        results_by_type = {}
        results_by_id   = {}

        for analysis_id, result in results_from_state.items():
            atype = result.get("analysis_type", "unknown")
            results_by_type[atype] = result
            results_by_id[analysis_id] = result

        dataset_type = getattr(state, "dataset_type", "") if state else ""
        semantic_map = getattr(state, "semantic_map", {}) if state else {}
        column_roles = semantic_map.get(
            "column_roles", {}
        ) if isinstance(semantic_map, dict) else {}

        return {
            "status":          "success",
            "results_by_type": results_by_type,
            "results_by_id":   results_by_id,
            "available_types": list(results_by_type.keys()),
            "total_results":   len(results_by_id),
            "dataset_type":    dataset_type,
            "column_roles":    column_roles,
        }

    except Exception as e:
        return {
            "status":          "no_results",
            "results_by_type": {},
            "results_by_id":   {},
            "available_types": [],
            "total_results":   0,
            "dataset_type":    "unknown",
            "column_roles":    {},
            "error":           str(e),
        }


from tools.synthesis_helpers import deterministic_full_synthesis as run_synthesis_deterministic


def tool_submit_synthesis(session_id: str, synthesis_json_str: str) -> str:
    """
    Submit the complete synthesis result.
    MUST be called as the last step.
    """
    try:
        if synthesis_json_str.strip().startswith("```"):
            lines = synthesis_json_str.strip().split("\n")
            if lines[0].startswith("```"): lines = lines[1:]
            if lines and lines[-1].startswith("```"): lines = lines[:-1]
            synthesis_json_str = "\n".join(lines).strip()
            
        synthesis = json.loads(synthesis_json_str)
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
                "You are a Senior Data Intelligence Analyst. You receive structured analysis results (with raw data dicts) "
                "and produce a deep, specific, evidence-backed report. You output a single JSON blob via tool_submit_synthesis.\n\n"

                "## STRICT RULES (NON-NEGOTIABLE)\n"
                "1. CITE SPECIFIC NUMBERS in every claim. BAD: 'High bounce rate'. GOOD: 'Bounce rate of 38.4% means 4 in 10 sessions end after 1 event'.\n"
                "2. CITE ANALYSIS IDs in every finding — format: [A1: session_detection], [A3: funnel_analysis].\n"
                "3. EVERY `how_to_fix` step must reference the exact metric or event it targets.\n"
                "4. EVERY `possible_cause` must be grounded in the data provided, not generic advice.\n"
                "5. If data for a finding is insufficient, write 'Insufficient data from [AnalysisID]' — do NOT fabricate.\n"
                "6. Be DOMAIN-AGNOSTIC. Reason from the data shapes, not assumptions about the industry.\n\n"

                "## OUTPUT SCHEMA (EXACT KEYS REQUIRED)\n\n"
                "### executive_summary\n"
                "{\n"
                "  'overall_health': 'One paragraph with 3+ specific stats from the analyses',\n"
                "  'top_priorities': ['Priority 1 with number [A1]', 'Priority 2 with number [A2]'],\n"
                "  'business_impact': 'Quantified revenue/retention consequence where data supports it',\n"
                "  'resource_allocation': 'Where to focus engineering/product effort first and why',\n"
                "  'timeline': 'Suggested phased timeline: Quick wins (1-2 weeks) vs Medium fixes (1 month) vs Long-term'\n"
                "}\n\n"

                "### detailed_insights — ONE card per completed analysis (analysis_type)\n"
                "{\n"
                "  'insights': [\n"
                "    {\n"
                "      'title': 'Short descriptive title with the key metric number in it',\n"
                "      'ai_summary': '2-3 sentences. Must include: (a) the specific number, (b) what it means for users, (c) how it compares to a benchmark if inferable. [AnalysisID]',\n"
                "      'root_cause_hypothesis': 'A causal chain, not a vague correlation. e.g. X causes Y because Z metric shows W',\n"
                "      'possible_causes': [\n"
                "        'Cause 1 — grounded in a specific data field from the analysis result',\n"
                "        'Cause 2 — grounded in a different data field',\n"
                "        'Cause 3 — cross-analysis inference [A1 + A3]'\n"
                "      ],\n"
                "      'ux_implications': 'Specific UX consequence with an estimated numeric impact',\n"
                "      'fix_priority': 'critical|high|medium|low',\n"
                "      'how_to_fix': [\n"
                "        'Step 1: Specific action explicitly naming the event/metric to change',\n"
                "        'Step 2: Another specific action'\n"
                "      ]\n"
                "    }\n"
                "  ]\n"
                "}\n\n"

                "### intervention_strategies\n"
                "{\n"
                "  'critical_count': 1,\n"
                "  'strategies': [\n"
                "    {\n"
                "      'severity': 'critical',\n"
                "      'title': 'High Dropout at Step X',\n"
                "      'realtime_interventions': ['Show modal when event Y happens'],\n"
                "      'proactive_outreach': ['Email users who reached X but not Z']\n"
                "    }\n"
                "  ]\n"
                "}\n\n"

                "### personas — infer 2-4 user archetypes from segmentation + behavioral data\n"
                "{\n"
                "  'persona_count': 2,\n"
                "  'personas': [\n"
                "    {\n"
                "      'name': 'Struggling Explorer',\n"
                "      'profile': 'Users who trigger search 10+ times without booking',\n"
                "      'pain_points': ['Point 1', 'Point 2'],\n"
                "      'opportunities': ['Opp 1'],\n"
                "      'priority_level': 'high'\n"
                "    }\n"
                "  ]\n"
                "}\n\n"
                
                "### conversational_report (NEW!)\n"
                "Provide a deeply detailed markdown-formatted string (not JSON dict here, just a long string) that explicitly reads like a product analytics report. Model it off of standard product analysis documents.\n"
                "Must include headers like `# PART 1: BEHAVIORAL & UX ANALYSIS`, `## User Intent Analysis`, `## Friction Points Identified` (with a markdown table of Friction Point, Evidence, Severity), and an overarching `# CONFIDENCE ASSESSMENT` table at the end.\n"
                "Ensure every insight in the text cites `[Analysis: id]` or `[Event: name]`.\n\n"

                "### cross_metric_connections (Optional)\n"
                "{\n"
                "  'connection_count': 1,\n"
                "  'connections': [\n"
                "    {\n"
                "      'finding_a': '[A1] Users fail OTP',\n"
                "      'finding_b': '[A2] Segment 3 bounces mostly at login',\n"
                "      'synthesized_meaning': 'OTP failure is specifically destroying Segment 3 retention'\n"
                "    }\n"
                "  ]\n"
                "}\n"
                "```\n"
            ),
            tools=[tool_aggregate_results, tool_submit_synthesis],
        )
    return _synthesis_agent_instance
