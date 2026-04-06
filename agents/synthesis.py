import os
import sys
import json
import threading
from typing import Annotated
from pydantic import Field

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..")
)

from pipeline_types import create_message, Intent
from tools.config_loader import get_config as _get_config

_PROMPT_DIR = os.path.join(os.path.dirname(__file__), '..', 'prompts')

def _load_prompt(name: str) -> str:
    path = os.path.join(_PROMPT_DIR, name)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        raise RuntimeError(f"Prompt file missing: {path}. Ensure the prompts/ directory is present.")
    except Exception as e:
        raise RuntimeError(f"Failed to load prompt {name}: {e}") from e

_synthesis_store: dict = {}
_synthesis_lock = threading.Lock()

_reasoning_store: dict = {}

def get_synthesis_result(session_id: str) -> dict | None:
    return _synthesis_store.get(session_id)

def _extract_node_facts(node_id: str, analysis_id: str, result: dict) -> dict:
    atype = result.get("analysis_type", "unknown")
    status = result.get("status", "unknown")
    top_finding = result.get("top_finding", "")
    severity = result.get("severity", "info")
    confidence = result.get("confidence", 0.0)
    data = result.get("data", {})

    _insight_summary = result.get("insight_summary") or {}
    _dm_takeaway = _insight_summary.get("decision_maker_takeaway", "") if isinstance(_insight_summary, dict) else ""

    facts = {
        "node_id": node_id,
        "analysis_id": analysis_id,
        "analysis_type": atype,
        "status": status,
        "top_finding": top_finding,
        "severity": severity,
        "confidence": confidence,

        "decision_maker_takeaway": _dm_takeaway,
        "key_metrics": {},
    }

    if status not in ("success",):
        facts["key_metrics"] = {}
        return facts

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

        facts["key_metrics"] = {
            "total_sessions": data.get("total_sessions"),
            "avg_events_per_session": data.get("avg_events_per_session"),
            "avg_duration_minutes": data.get("avg_duration_minutes"),
            "bounce_rate": data.get("bounce_rate"),
            "bounce_sessions": data.get("bounce_sessions"),
            "detection_mode": data.get("detection_mode"),
        }
    elif atype in ("friction_detection",):

        top_friction = data.get("top_friction_events", [])
        facts["key_metrics"] = {
            "events_analyzed": data.get("events_analyzed"),
            "critical_events": data.get("critical_events"),
            "high_events": data.get("high_events"),
            "top_friction_events": top_friction[:3],
        }
    elif atype in ("survival_analysis",):

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

        cat_dist = data.get("category_distribution", {})
        dominant = max(cat_dist, key=lambda k: cat_dist[k].get("count", 0)) if cat_dist else None
        facts["key_metrics"] = {
            "total_unique_events": data.get("total_unique_events"),
            "category_distribution": cat_dist,
            "dominant_category": dominant,
        }
    elif atype in ("user_journey_analysis",):

        facts["key_metrics"] = {
            "total_entities_tracked": data.get("total_entities_tracked"),
            "avg_steps_per_entity":   data.get("avg_steps_per_entity"),
            "max_steps_per_entity":   data.get("max_steps_per_entity"),
            "common_entry_events":    data.get("common_entry_events", {}),
            "common_exit_events":     data.get("common_exit_events", {}),
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

        facts["key_metrics"] = {
            k: v for k, v in data.items()
            if isinstance(v, (int, float, str, bool)) and v is not None
        }

    return facts

def tool_aggregate_results(session_id: str) -> dict:
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

def _validate_synthesis_grounding(synthesis: dict, fact_sheet: dict, session_id: str = "") -> dict:
    import re
    warnings_found = []
    citation_count = 0

    required_keys = ["executive_summary", "detailed_insights", "conversational_report"]
    for key in required_keys:
        if key not in synthesis:
            warnings_found.append(f"MISSING KEY: '{key}' not found in synthesis output.")

    ex = synthesis.get("executive_summary", {})
    if not ex.get("overall_health"):
        warnings_found.append("WEAK: executive_summary.overall_health is empty.")
    if not ex.get("top_priorities"):
        warnings_found.append("WEAK: executive_summary.top_priorities is empty.")

    synthesis_str = json.dumps(synthesis)
    citations = re.findall(r'\[[AC]\d+[^\]]*\]', synthesis_str)
    citation_count = len(citations)
    if citation_count < 3:
        warnings_found.append(
            f"LOW CITATION: Only {citation_count} node ID citations found. "
            "Synthesis should cite [AX]/[CX] or [AX: type] for every quantitative claim."
        )
        try:
            from tools.monitor import emit
            emit(session_id, "synthesis_low_citation", {"citation_count": citation_count}, severity="warning")
        except Exception as _emit_err:
            logging.warning(f"Failed to emit low citation warning: {_emit_err}")

    _di = synthesis.get("detailed_insights", {})
    insights = _di if isinstance(_di, list) else _di.get("insights", [])
    for i, ins in enumerate(insights):
        if not ins.get("ai_summary"):
            warnings_found.append(f"INSIGHT[{i}]: ai_summary is empty.")
        if not ins.get("how_to_fix"):
            warnings_found.append(f"INSIGHT[{i}]: how_to_fix steps are empty.")

    _p = synthesis.get("key_segments", synthesis.get("personas", {}))
    personas = _p.get("segments", _p.get("personas", [])) if isinstance(_p, dict) else _p
    if (synthesis.get("key_segments") is not None or synthesis.get("personas") is not None) and not personas:
        warnings_found.append("WEAK: key_segments section present but no segments defined.")

    _syn_cfg = _get_config()["synthesis"]
    conv = synthesis.get("conversational_report", "")
    if len(str(conv)) < _syn_cfg["min_report_chars_warn"]:
        warnings_found.append(f"WEAK: conversational_report is too short (< {_syn_cfg['min_report_chars_warn']} chars) — narrative lacks depth.")

    is_valid = len([w for w in warnings_found if w.startswith("MISSING")]) == 0

    return {
        "is_valid": is_valid,
        "citation_count": citation_count,
        "warnings": warnings_found,
        "fact_sheet_nodes_available": list(fact_sheet.keys()),
    }

def tool_submit_synthesis(
    session_id: Annotated[str, Field(description="Active pipeline session ID")],
    synthesis_json_str: Annotated[str, Field(description="Complete synthesis result as a JSON string matching the required output schema")],
    output_folder: Annotated[str, Field(description="Absolute path to the session output directory. Copy exactly from the prompt.")],
    reasoning_notes: Annotated[str, Field(description="2-3 sentence summary of key deductions made during synthesis. Stored and injected on retry.", default="")] = "",
    tool_context=None,
) -> str:
    try:
        if synthesis_json_str.strip().startswith("```"):
            lines = synthesis_json_str.strip().split("\n")
            if lines[0].startswith("```"): lines = lines[1:]
            if lines and lines[-1].startswith("```"): lines = lines[:-1]
            synthesis_json_str = "\n".join(lines).strip()

        import re
        match = re.search(r'```(?:json)?\s*(.*?)\s*```', synthesis_json_str, re.DOTALL)
        if match:
            synthesis_json_str = match.group(1)
        else:
            first_brace = synthesis_json_str.find('{')
            last_brace = synthesis_json_str.rfind('}')
            if first_brace != -1 and last_brace != -1:
                synthesis_json_str = synthesis_json_str[first_brace:last_brace+1]

        def _repair_json(s):

            s = s.replace('\u201c', '"').replace('\u201d', '"')
            s = s.replace('\u2018', "'").replace('\u2019', "'")

            s = re.sub(r',\s*([}\]])', r'\1', s)

            _in_str = False
            _i2 = 0
            while _i2 < len(s):
                _c = s[_i2]
                if _c == '\\' and _in_str:
                    _i2 += 2
                    continue
                if _c == '"':
                    _in_str = not _in_str
                _i2 += 1
            if _in_str:

                s = s.rstrip() + '"'
                s = re.sub(r',\s*$', '', s)

            first_open = s.find('{')
            if first_open != -1:
                depth = 0
                for i in range(first_open, len(s)):
                    if s[i] == '{': depth += 1
                    elif s[i] == '}':
                        depth -= 1
                        if depth == 0:
                            s = s[:i+1]
                            break

            open_b = s.count('{'); close_b = s.count('}')
            if open_b > close_b: s += '}' * (open_b - close_b)
            open_sq = s.count('['); close_sq = s.count(']')
            if open_sq > close_sq: s += ']' * (open_sq - close_sq)

            valid_esc = set('"\\bfnrtu/')
            out = []
            i = 0
            while i < len(s):
                if s[i] == '\\' and i + 1 < len(s) and s[i+1] not in valid_esc:
                    out.append('\\\\')
                else:
                    out.append(s[i])
                i += 1
            s = ''.join(out)

            result = []
            in_str = False
            i = 0
            while i < len(s):
                c = s[i]
                if c == '"' and (i == 0 or s[i-1] != '\\'):
                    in_str = not in_str
                    result.append(c)
                elif in_str and c == '\n':
                    result.append('\\n')
                elif in_str and c == '\r':
                    result.append('\\r')
                elif in_str and c == '\t':
                    result.append('\\t')
                else:
                    result.append(c)
                i += 1
            return ''.join(result)

        try:
            synthesis = json.loads(synthesis_json_str)
        except json.JSONDecodeError as _jde:
            _repaired = _repair_json(synthesis_json_str)
            try:
                synthesis = json.loads(_repaired)
                print(f"[Synthesis] JSON repaired (original error: {_jde.msg} at char {_jde.pos})")
            except json.JSONDecodeError as _jde2:
                raise json.JSONDecodeError(
                    f"JSON parse failed after repair. Original: {_jde.msg}. After repair: {_jde2.msg}",
                    _jde2.doc, _jde2.pos
                ) from _jde2

        _quality_failures = []

        _syn_cfg = _get_config()["synthesis"]
        _exec = synthesis.get("executive_summary", {})
        _overall_health = str(_exec.get("overall_health", ""))
        if len(_overall_health) < _syn_cfg["min_exec_summary_chars"]:
            _quality_failures.append(
                f"executive_summary.overall_health is too short ({len(_overall_health)} chars, minimum {_syn_cfg['min_exec_summary_chars']}). "
                "Re-write with 3+ specific stats citing node IDs."
            )

        _insights = synthesis.get("detailed_insights", {})
        if isinstance(_insights, dict):
            _insights = _insights.get("insights", [])
        if len(_insights) == 0:
            _quality_failures.append("detailed_insights.insights is empty — must have one card per analysis node.")
        else:
            for _i, _ins in enumerate(_insights):
                _summary = str(_ins.get("ai_summary", ""))
                if len(_summary) < _syn_cfg["min_insight_summary_chars"]:
                    _quality_failures.append(
                        f"detailed_insights.insights[{_i}].ai_summary is too short ({len(_summary)} chars, minimum {_syn_cfg['min_insight_summary_chars']}). "
                        "Must include: (a) the key metric with [NodeID], (b) what it means, (c) an internal comparison to another node finding. Do NOT invent benchmarks."
                    )
                _rc = str(_ins.get("root_cause_hypothesis", ""))
                if len(_rc) < _syn_cfg["min_insight_hypothesis_chars"]:
                    _quality_failures.append(
                        f"detailed_insights.insights[{_i}].root_cause_hypothesis is too short ({len(_rc)} chars, minimum {_syn_cfg['min_insight_hypothesis_chars']}). "
                        "Must cite at least two node IDs in a causal chain."
                    )

        _conv = str(synthesis.get("conversational_report", ""))
        if len(_conv) < _syn_cfg["min_report_chars"]:
            _quality_failures.append(
                f"conversational_report is too short ({len(_conv)} chars, minimum {_syn_cfg['min_report_chars']}). "
                "Must be a full narrative section covering all major findings with cited metrics."
            )

        _required_headers = ["# Key Findings", "# Action Roadmap", "# Confidence Assessment"]
        _missing_headers = [h for h in _required_headers if h not in _conv]
        if _missing_headers:
            _quality_failures.append(
                f"conversational_report is missing required section headers: {_missing_headers}. "
                "The report MUST contain exactly these markdown headers: "
                "'# Key Findings', '# Action Roadmap', '# Confidence Assessment'."
            )

        _cmc_raw = synthesis.get("cross_metric_connections", {})
        if isinstance(_cmc_raw, list):
            _connections = _cmc_raw
        elif isinstance(_cmc_raw, dict):
            _connections = _cmc_raw.get("connections", [])
        else:
            _connections = []
        _n_insights = len(_insights) if '_insights' in dir() else 2
        _min_connections = max(0, min(2, _n_insights - 1))
        if len(_connections) < _min_connections:
            _quality_failures.append(
                f"cross_metric_connections.connections has {len(_connections)} entr"
                f"{'y' if len(_connections) == 1 else 'ies'} (minimum {_min_connections} required). "
                "Each connection MUST cite two different [AX] node IDs and explain the causal mechanism."
            )
        else:
            import re as _re_cmc
            for _ci, _conn in enumerate(_connections):
                if not isinstance(_conn, dict):
                    continue
                _fa = str(_conn.get("finding_a", ""))
                _fb = str(_conn.get("finding_b", ""))
                _cited = set(_re_cmc.findall(r'\[[AC]\d+\]', _fa + _fb))
                if len(_cited) < 2:
                    _quality_failures.append(
                        f"cross_metric_connections.connections[{_ci}] cites fewer than 2 distinct node IDs "
                        f"(found: {sorted(_cited) if _cited else 'none'}). "
                        "finding_a and finding_b must each reference a different [AX]/[CX] node."
                    )

        if _quality_failures:

            if reasoning_notes and reasoning_notes.strip():
                _reasoning_store[session_id] = reasoning_notes.strip()
                print(f"[Synthesis QA] Stored reasoning notes for {session_id} ({len(reasoning_notes)} chars)")

            try:
                import os as _os_rej
                _rej_out = None
                if output_folder and output_folder.strip():
                    _rej_out = output_folder.strip()
                if not _rej_out:
                    try:
                        from main import sessions as _sess_rej
                        _st_rej = _sess_rej.get(session_id)
                        if _st_rej:
                            _o = getattr(_st_rej, "output_folder", None)
                            if _o:
                                _root = _os_rej.path.dirname(_os_rej.path.dirname(_os_rej.path.abspath(__file__)))
                                _rej_out = _os_rej.path.join(_root, "output", _o) if not _os_rej.path.isabs(_o) else _o
                    except Exception:
                        pass
                if not _rej_out:
                    try:
                        from agent_servers.a2a_client import lookup_session as _a2a_lookup
                        _rej_out = _a2a_lookup(session_id) or None
                    except Exception:
                        pass
                if _rej_out:
                    _os_rej.makedirs(_rej_out, exist_ok=True)
                    _fallback = _os_rej.path.join(_rej_out, "_synthesis_cache.json")
                    _syn_copy = dict(synthesis)
                    _syn_copy["_qc_passed"] = False
                    _syn_copy["_qc_failures"] = _quality_failures
                    with open(_fallback, "w", encoding="utf-8") as _ff:
                        json.dump(_syn_copy, _ff)
                    print(f"[Synthesis QA] Fallback cache written despite rejection: {_fallback}")
            except Exception as _fbe:
                print(f"[Synthesis QA] Fallback cache write failed: {_fbe}")

            _prior_reasoning = _reasoning_store.get(session_id, "")
            _reasoning_context = ""
            if _prior_reasoning:
                _reasoning_context = (
                    f"\nYour reasoning from this attempt:\n"
                    f"{_prior_reasoning}\n"
                    f"Build on this — do NOT start from scratch. Only fix the issues below.\n"
                )

            _msg = (
                "SYNTHESIS REJECTED — quality below minimum standards."
                + _reasoning_context +
                "\nFix specifically:\n"
                + "\n".join(f"  • {f}" for f in _quality_failures)
            )
            print(f"[Synthesis QA] REJECTED for {session_id}: {len(_quality_failures)} issue(s)")
            return _msg

        print(f"[Synthesis QA] Passed for {session_id} — {len(_insights)} insights, "
              f"conv_report={len(_conv)} chars")

        _sev_to_score = {"critical": 9.0, "high": 7.0, "medium": 5.0, "low": 2.0, "info": 1.0}
        _priority_to_bucket = {
            "critical": "Quick Win (1–2 weeks)",
            "high":     "Medium Fix (1 month)",
            "medium":   "Strategic (3 months)",
            "low":      "Strategic (3+ months)",
        }
        _enriched = []
        for _ins in _insights:
            _fp = str(_ins.get("fix_priority", "medium")).lower()
            _ins_copy = dict(_ins)
            if "impact_score" not in _ins_copy:
                _ins_copy["impact_score"] = _sev_to_score.get(_fp, 5.0)
            if "timeline_bucket" not in _ins_copy:
                _ins_copy["timeline_bucket"] = _priority_to_bucket.get(_fp, "Strategic (3 months)")
            _enriched.append(_ins_copy)

        if isinstance(synthesis.get("detailed_insights"), dict):
            synthesis["detailed_insights"]["insights"] = _enriched
        else:
            synthesis["detailed_insights"] = _enriched

        try:
            from main import sessions
            state = sessions.get(session_id)
            current_fact_sheet = {}
            if state and hasattr(state, "results"):

                for aid, result in state.results.items():
                    if not isinstance(result, dict):
                        continue
                    atype = result.get("analysis_type", "unknown")
                    current_fact_sheet[aid] = {"analysis_type": atype, "status": result.get("status", "")}
        except Exception:
            current_fact_sheet = {}

        validation = _validate_synthesis_grounding(synthesis, current_fact_sheet, session_id=session_id)
        synthesis["_datalog_validation"] = validation

        _reasoning_store.pop(session_id, None)

        synthesis["_qc_passed"] = True

        with _synthesis_lock:
            _synthesis_store[session_id] = synthesis

        if tool_context is not None:
            try:
                tool_context.state["synthesis"] = synthesis
            except Exception:
                pass

        try:
            from main import sessions
            state = sessions.get(session_id)
        except Exception:
            state = None
        if state:
            state.synthesis = synthesis

        try:
            import os as _os
            _abs_out = None

            if output_folder and output_folder.strip():
                _abs_out = output_folder.strip()

            if not _abs_out and state:
                _out = getattr(state, "output_folder", None)
                if _out:
                    _adk_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
                    _abs_out = _os.path.join(_adk_root, "output", _out) if not _os.path.isabs(_out) else _out

            if not _abs_out:
                try:
                    from agent_servers.a2a_client import lookup_session
                    _abs_out = lookup_session(session_id) or None
                except Exception:
                    pass
            if _abs_out:
                _os.makedirs(_abs_out, exist_ok=True)
                _tmp = _os.path.join(_abs_out, "_synthesis_cache.json")
                with open(_tmp, "w", encoding="utf-8") as _f:
                    json.dump(synthesis, _f)
                print(f"INFO: synthesis cache written to {_tmp}")
            else:
                print(f"WARNING: synthesis cache not written — output_folder unknown for {session_id}")
        except Exception as _ce:
            print(f"WARNING: synthesis cache write failed: {_ce}")
            if state:
                try:
                    _int = synthesis.get("recommendations", synthesis.get("intervention_strategies", {}))
                    _per = synthesis.get("key_segments", synthesis.get("personas", {}))
                    _cmc = synthesis.get("cross_metric_connections", {})
                    msg = create_message(
                        sender="synthesis_agent",
                        recipient="orchestrator",
                        intent=Intent.SYNTHESIS_COMPLETE,
                        payload={
                            "critical_count": _int.get("critical_count", 0) if isinstance(_int, dict) else 0,
                            "segment_count": _per.get("segment_count", _per.get("persona_count", 0)) if isinstance(_per, dict) else len(synthesis.get("key_segments", synthesis.get("personas", []))),
                            "connection_count": _cmc.get("connection_count", 0) if isinstance(_cmc, dict) else len(synthesis.get("cross_metric_connections", [])),
                        },
                        session_id=session_id,
                    )
                    state.post_message(msg)
                except Exception:
                    pass

        try:
            if validation["warnings"]:
                print(f"[Synthesis Validator] {len(validation['warnings'])} warning(s):")
                for w in validation["warnings"]:
                    print(f"  [WARN] {w}")
            print(f"[Synthesis Validator] Citations found: {validation['citation_count']} | Valid: {validation['is_valid']}")
        except Exception:
            pass

        return f"Synthesis stored for session {session_id}. Ready for DAG Builder."
    except Exception as e:
        import traceback
        traceback.print_exc()
        return f"Error storing synthesis: {str(e)}"

_synthesis_agent_instance = None

def _synthesis_after_model_callback(callback_context, llm_response):
    from google.adk.models.llm_response import LlmResponse
    from google.genai import types

    text = ""
    if llm_response.content and llm_response.content.parts:
        for part in llm_response.content.parts:
            text += getattr(part, "text", "") or ""

    if not text:
        return None

    import re
    match = re.search(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
    json_text = match.group(1) if match else text
    first = json_text.find("{")
    last = json_text.rfind("}")
    if first != -1 and last != -1:
        json_text = json_text[first:last + 1]

    try:
        data = json.loads(json_text)
        required = {"executive_summary", "detailed_insights", "conversational_report"}
        missing = required - set(data.keys())
        if missing:
            return LlmResponse(content=types.Content(parts=[types.Part(text=(
                f"Your response is missing required top-level keys: {sorted(missing)}. "
                "Please rewrite your full synthesis JSON including all required fields: "
                "executive_summary, detailed_insights, conversational_report, "
                "cross_metric_connections, key_segments, and recommendations."
            ))]))
    except (json.JSONDecodeError, ValueError):
        return LlmResponse(content=types.Content(parts=[types.Part(text=(
            "Your response must be valid JSON wrapped in ```json ... ```. "
            "The JSON must parse without errors via Python's json.loads(). "
            "Please rewrite your complete synthesis as valid JSON."
        ))]))

    return None

def get_synthesis_agent():
    global _synthesis_agent_instance
    if _synthesis_agent_instance is None:
        from google.adk.agents import Agent
        from google.adk.tools import FunctionTool
        from tools.model_config import get_model
        _synthesis_agent_instance = Agent(
            name="synthesis_agent",
            model=get_model("synthesis"),
            description="Interpretation layer. Receives all analysis results and generates deep narrative, personas, strategies, and executive summary.",
            instruction=_load_prompt("synthesis.md"),
            tools=[FunctionTool(tool_aggregate_results), FunctionTool(tool_submit_synthesis)],
            after_model_callback=_synthesis_after_model_callback,
        )
    return _synthesis_agent_instance
