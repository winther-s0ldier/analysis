import os
import re
import sys
import json
import math
import copy
import asyncio
import logging
import difflib
import functools
import traceback
from typing import Optional, List, Dict, Any

from logging.handlers import RotatingFileHandler as _RotatingFileHandler
_log_handler = _RotatingFileHandler(
    'pipeline.log', maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
)
_log_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))

logging.basicConfig(level=logging.INFO, handlers=[_log_handler])

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..")
)

from pipeline_types import (
    create_message,
    Intent,
    NodeStatus,
    PipelineState,
    get_messages_by_intent,
    get_latest_message,
    is_dependency_resolved,
    get_completed_analysis_ids,
    get_failed_analysis_ids,
)
from tools.analysis_library import LIBRARY_REGISTRY
from tools.model_config import get_model
from tools.config_loader import get_config as _get_config

try:
    from google.adk.agents.sequential_agent import SequentialAgent as _SequentialAgent
    from google.adk.agents.loop_agent import LoopAgent as _LoopAgent
    _SEQUENTIAL_AVAILABLE = True
except ImportError:
    _SequentialAgent = None
    _LoopAgent = None
    _SEQUENTIAL_AVAILABLE = False

_USE_A2A_MULTISERVER = os.getenv("USE_A2A_MULTISERVER", "true").lower() != "false"

async def _a2a_with_fallback(a2a_coro, fallback_coro, agent_name, session_id):
    try:
        return await a2a_coro
    except Exception as e:
        print(f"WARNING: [{session_id}] A2A {agent_name} failed: {e}, falling back to in-process")
        logging.warning(f"[{session_id}] A2A {agent_name} failed: {e}, falling back to in-process")
        return await fallback_coro

_pipeline_store: dict = {}

_pipeline_event_hooks: dict = {}

_global_threads: dict = {}

_genai_client = None

def _get_llm_client():
    global _genai_client
    if _genai_client is None:
        import google.genai as _genai
        _genai_client = _genai.Client()
    return _genai_client

async def _llm_generate_with_retry(
    contents: str,
    model: str,
    label: str = "LLM",
    max_attempts: int | None = None,
    backoff_base: float | None = None,
):
    _retry_cfg = _get_config()["retry"]
    if max_attempts is None:
        max_attempts = _retry_cfg["max_attempts"]
    if backoff_base is None:
        backoff_base = _retry_cfg["backoff_base"]
    client = _get_llm_client()
    resp = None
    for _attempt in range(max_attempts):
        try:
            resp = client.models.generate_content(model=model, contents=contents)
            break
        except Exception as _err:
            _err_s = str(_err).lower()
            if (
                any(x in _err_s for x in ("429", "rate", "quota", "exhausted"))
                and _attempt < max_attempts - 1
            ):
                _wait = backoff_base * (_attempt + 1)
                print(f"WARNING: [{label}] Rate-limit on attempt {_attempt + 1}, retrying in {_wait:.0f}s")
                await asyncio.sleep(_wait)
            else:
                raise
    if resp is None:
        raise RuntimeError(f"[{label}] No response after {max_attempts} attempts")
    return resp

def get_pipeline_state(
    session_id: str
) -> Optional[PipelineState]:
    return _pipeline_store.get(session_id)

def create_pipeline_state(
    session_id: str,
    dag: list,
) -> PipelineState:
    state = PipelineState(
        session_id=session_id,
        total_nodes=len(dag),
        nodes={
            node["id"]: node for node in dag
        },
        pending=[node["id"] for node in dag]
    )
    _pipeline_store[session_id] = state
    return state

_pre_dag_agent_instance = None
_post_dag_agent_instance = None
_synthesis_critic_loop_instance = None

def get_pre_dag_agent():
    global _pre_dag_agent_instance
    if _pre_dag_agent_instance is not None:
        return _pre_dag_agent_instance
    if not _SEQUENTIAL_AVAILABLE:
        raise RuntimeError("google.adk.agents.sequential_agent not available")
    from agents.profiler import get_profiler_agent
    from agents.discovery import get_discovery_agent
    _pre_dag_agent_instance = _SequentialAgent(
        name="pre_dag_pipeline",
        description="Profile dataset then build analysis plan",
        sub_agents=[get_profiler_agent(), get_discovery_agent()],
    )
    return _pre_dag_agent_instance

def _get_synthesis_critic_loop():
    global _synthesis_critic_loop_instance
    if _synthesis_critic_loop_instance is not None:
        return _synthesis_critic_loop_instance
    if not _SEQUENTIAL_AVAILABLE:
        raise RuntimeError("google.adk.agents.loop_agent not available")
    from agents.synthesis import get_synthesis_agent
    from agents.critic import get_critic_agent
    _synthesis_critic_loop_instance = _LoopAgent(
        name="synthesis_critic_loop",
        description="Synthesize results then critique; retries synthesis if critic rejects (max 2 iterations)",
        sub_agents=[get_synthesis_agent(), get_critic_agent()],
        max_iterations=1,
    )
    return _synthesis_critic_loop_instance

def get_post_dag_agent():
    global _post_dag_agent_instance
    if _post_dag_agent_instance is not None:
        return _post_dag_agent_instance
    if not _SEQUENTIAL_AVAILABLE:
        raise RuntimeError("google.adk.agents.sequential_agent not available")
    from agents.dag_builder import get_dag_builder_agent
    _post_dag_agent_instance = _SequentialAgent(
        name="post_dag_pipeline",
        description="Run synthesis-critic loop until approved, then build HTML report",
        sub_agents=[_get_synthesis_critic_loop(), get_dag_builder_agent()],
    )
    return _post_dag_agent_instance

def build_synthesis_prompt(session_id: str, state, dag: list = None, output_folder: str = None) -> tuple:
    dag = dag or getattr(state, "dag", []) or []

    def _clean(obj):
        if isinstance(obj, dict):   return {k: _clean(v) for k, v in obj.items()}
        if isinstance(obj, list):   return [_clean(x) for x in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)): return None
        if hasattr(obj, "item"):    return _clean(obj.item())
        if isinstance(obj, (bool, int, float, str)) or obj is None: return obj
        return str(obj)

    try:
        from agents.synthesis import _extract_node_facts as _efs
        _fact_sheet = {}
        _atype_to_id = {
            n.get("analysis_type"): n.get("id")
            for n in dag if n.get("id") and n.get("analysis_type")
        }
        for _aid, _res in state.results.items():
            if not isinstance(_res, dict): continue
            _atype = _res.get("analysis_type", "unknown")
            _nid   = _atype_to_id.get(_atype, _aid)
            _fact_sheet[_nid] = _efs(_nid, _aid, _res)
        _fact_json = json.dumps(_clean(_fact_sheet), indent=2)
    except Exception as _e:
        _fact_json = "{}"
        print(f"WARNING: fact_sheet build failed in build_synthesis_prompt: {_e}")

    _dag_lines = []
    for _n in dag:
        _d = _n.get("depends_on", [])
        _dag_lines.append(
            f"  {_n.get('id')} [{_n.get('analysis_type')}]"
            + (f" -> depends on {_d}" if _d else " -> root node")
        )
    _dag_graph = "\n".join(_dag_lines) if _dag_lines else "  (no DAG available)"

    _user_goal = ""
    if getattr(state, "user_instructions", ""):
        _user_goal = (
            f"\n== USER GOAL / STAKEHOLDER QUESTION ==\n"
            f"{state.user_instructions}\n"
            f"Your synthesis MUST directly address this. "
            f"top_priorities and the Action Roadmap must map to this goal.\n"
        )

    _profile = ""
    _raw = getattr(state, "raw_profile", {}) or {}
    if _raw:
        _col_roles = (getattr(state, "semantic_map", {}) or {}).get("column_roles", {})
        _profile = (
            f"\n== DATASET DESCRIPTION ==\n"
            f"Rows: {_raw.get('row_count', 'unknown')} | "
            f"Columns: {_raw.get('column_count', 'unknown')} | "
            f"Type: {getattr(state, 'dataset_type', '')}\n"
            f"Column roles: {json.dumps(_col_roles, default=str)}\n"
        )

    _non_normal_cols = []
    _has_corr = any(
        isinstance(_r, dict) and _r.get("analysis_type") == "correlation_matrix"
        for _r in state.results.values()
    )
    for _nid, _res in state.results.items():
        if not isinstance(_res, dict): continue
        if _res.get("analysis_type") == "distribution_analysis":
            _d = _res.get("data", {})
            _stats = _d.get("stats", {}) if isinstance(_d, dict) else {}
            if _stats.get("is_normal") is False:
                _col_name = _d.get("col", _nid) if isinstance(_d, dict) else _nid
                _non_normal_cols.append(f"{_nid} ({_col_name})")
    _normality_warning = ""
    if _non_normal_cols:
        _normality_warning = (
            f"\n== NORMALITY WARNING ==\n"
            f"Non-normally distributed columns: {', '.join(_non_normal_cols)}.\n"
            + (
                "Pearson correlations and parametric p-values for these columns may be unreliable. "
                "Prefer citing Spearman r when discussing correlations for these columns.\n"
                if _has_corr else ""
            )
        )

    prompt = (
        f"Session ID: {session_id}\n"
        f"Dataset type: {getattr(state, 'dataset_type', '')}\n"
        f"Total analyses completed: {len(state.results)}\n"
        f"{_profile}"
        f"{_normality_warning}"
        f"{_user_goal}"
        f"\n== DAG DEPENDENCY GRAPH (use this to reason causally) ==\n"
        f"{_dag_graph}\n"
        f"\n== PRE-EXTRACTED FACT SHEET (cite ONLY these numbers  - do not invent) ==\n"
        f"{_fact_json}\n\n"
        f"INSTRUCTIONS (complete all steps in order):\n"
        f"1. Call tool_aggregate_results(session_id) to get column_roles context.\n"
        f"2. Think briefly: most surprising finding, strongest node interaction, top bottleneck. "
        f"Keep thinking to 3-5 sentences. Do NOT write a long essay.\n"
        f"3. Build the synthesis JSON using the fact sheet above. "
        f"Cite [NodeID] + specific number for every insight. "
        f"conversational_report MUST contain '# Key Findings', '# Action Roadmap', '# Confidence Assessment'.\n"
        f"4. Call tool_submit_synthesis(session_id='{session_id}', synthesis_json_str=..., output_folder='{output_folder}') immediately after step 3."
    )

    images = []
    for _nid, _res in state.results.items():
        if not isinstance(_res, dict): continue
        cp = _res.get("chart_file_path")
        if cp:
            pp = cp.replace(".html", ".png")
            if os.path.exists(pp):
                images.append(pp)

    return prompt, images

async def run_full_pipeline(
    session_id: str,
    csv_path: str,
    output_folder: str,
    approved_metrics: list = None,
    custom_requests: list = None,
    state=None,
) -> dict:
    if state is None:
        try:
            from main import sessions
            state = sessions.get(session_id)
        except Exception as e:
            print(f"ERROR: Cannot find session: {e}")
            return {"status": "error", "error": "session not found"}

    if state is None:
        print(f"ERROR: Session {session_id} not found")
        return {"status": "error", "error": "session not found"}

    print(f"INFO: run_full_pipeline session found: {session_id}")
    print(f"INFO: session status before: {state.status}")

    from tools.analysis_library import LIBRARY_REGISTRY
    from tools.workflow_loader import register_custom_analyses
    register_custom_analyses(LIBRARY_REGISTRY)

    os.makedirs(output_folder, exist_ok=True)

    try:
        from main import run_agent_pipeline, extract_json

        if _USE_A2A_MULTISERVER:
            from agent_servers.a2a_client import register_session as _reg_session
            _reg_session(session_id, output_folder)

        if getattr(state, "raw_profile", None):
            print(f"INFO: Reusing /profile result for {session_id}")
            profiler_data = {
                "status": "success",
                "raw_profile": state.raw_profile,
                "classification": state.semantic_map or {},
            }
        else:
            _update_session_status(state, "profiling")

            profiler_prompt = (
                f"csv_path: {csv_path}\n"
                f"session_id: {session_id}\n"
                f"Call tool_profile_and_classify now."
            )

            if _USE_A2A_MULTISERVER:
                from agent_servers.a2a_client import (
                    call_profiler as _a2a_profiler,
                )
                print(f"INFO: [{session_id}] Calling profiler via A2A HTTP")
                profiler_response = await _a2a_profiler(session_id, csv_path)
            else:
                profiler_response = await run_agent_pipeline(
                    f"{session_id}_profile",
                    profiler_prompt,
                    agent_getter="profiler",
                )
            profiler_data = extract_json(profiler_response)

            if not profiler_data or \
               profiler_data.get("status") != "success":
                return {
                    "status": "error",
                    "error":  "Profiling failed",
                    "detail": profiler_data,
                }

            state.raw_profile  = profiler_data.get("raw_profile", {})
            state.semantic_map = profiler_data.get("classification", {})
            state.dataset_type = profiler_data.get("classification", {}).get("dataset_type", "")

        try:
            import json as _json_meta
            _meta_path = os.path.join(output_folder, "_dataset_meta.json")
            with open(_meta_path, "w", encoding="utf-8") as _mf:
                _json_meta.dump({
                    "dataset_type": getattr(state, "dataset_type", ""),
                    "csv_filename": getattr(state, "csv_filename", ""),
                }, _mf, indent=2)
        except Exception as _meta_err:
            logging.warning(f"[{session_id}] Meta JSON write failed: {_meta_err}")

        _post_message(
            state, "profiler_agent", "discovery_agent",
            Intent.PROFILE_COMPLETE, session_id,
            {
                "dataset_type": state.dataset_type,
                "ready": True,
            }
        )

        _update_session_status(state, "discovering")

        classification = profiler_data.get(
            "classification", {}
        )
        row_count = profiler_data.get(
            "raw_profile", {}
        ).get("row_count", 0)

        profile_summary = _build_profile_summary(
            profiler_data, csv_path
        )

        try:
            from tools.data_policy import get_active_policy, build_policy_context_for_discovery
            active_policy = get_active_policy()

            from tools.workflow_loader import get_dataset_profile
            wf_profile = get_dataset_profile(state.dataset_type)
            if wf_profile:
                if "force_analyses" in wf_profile:
                    active_policy["required_analyses"] = wf_profile["force_analyses"]
                if "exclude_analyses" in wf_profile:
                    active_policy["excluded_analyses"] = wf_profile["exclude_analyses"]
                if "max_nodes" in wf_profile:
                    active_policy["max_nodes"] = wf_profile["max_nodes"]

            classification = profiler_data.get("classification", {})
            col_roles_for_policy = classification.get("column_roles", {})
            policy_context = build_policy_context_for_discovery(active_policy, col_roles_for_policy)
        except Exception as e:
            print(f"Policy load Error: {e}")
            active_policy = {}
            policy_context = ""

        _profiler_confidence = classification.get("confidence", 1.0)
        _confidence_warning = ""
        if _profiler_confidence < _get_config()["data_quality"]["profiler_confidence_threshold"]:
            _confidence_warning = (
                f"\nâšWARNING: LOW PROFILER CONFIDENCE ({_profiler_confidence:.2f}): "
                f"The dataset type classification is uncertain. "
                f"Apply conservative feasibility rules: "
                f"(a) prefer analyses with no required column roles over behavioral ones, "
                f"(b) mark any node requiring entity_col or time_col as feasibility=LOW if those roles are ambiguous, "
                f"(c) cap the DAG at 5 nodes maximum, "
                f"(d) include missing_data_analysis and distribution_analysis as the first two nodes.\n"
            )

        discovery_prompt = (
            f"Session ID: {session_id}\n"
            f"CSV file path: {csv_path}\n"
            f"Output folder: {output_folder}\n\n"
            f"PROFILER OUTPUT:\n{profile_summary}\n\n"
            + (f"{policy_context}\n\n" if policy_context else "")
            + (_confidence_warning if _confidence_warning else "")
            + f"INSTRUCTIONS:\n"
            f"1. Reason about the data and the policy context above.\n"
            f"2. Construct a JSON DAG of MetricSpec nodes (id, name, analysis_type, library_function, column_roles, depends_on).\n"
            f"3. Call tool_submit_analysis_plan(session_id, dag_json_str) with your JSON result.\n"
        )

        if getattr(state, "dag", None):
            plan = {
                "dag":        state.dag,
                "metrics":    getattr(state, "discovery", {}).get("metrics", []),
                "node_count": len(state.dag),
            }
            print(f"INFO: Reusing /discover plan with {len(state.dag)} nodes for {session_id}")
        else:

            from agents.discovery import get_analysis_plan
            if _USE_A2A_MULTISERVER:
                from agent_servers.a2a_client import call_discovery as _a2a_discovery
                print(f"INFO: [{session_id}] Calling discovery via A2A HTTP")
                discovery_response = await _a2a_discovery(
                    session_id=session_id,
                    csv_path=csv_path,
                    output_folder=output_folder,
                    profile_summary=profile_summary,
                    policy_context=policy_context,
                    confidence_warning=_confidence_warning,
                )
            else:
                discovery_response = await run_agent_pipeline(
                    f"{session_id}_discovery",
                    discovery_prompt,
                    agent_getter="discovery",
                )

            if _USE_A2A_MULTISERVER:

                try:
                    _plan_cache_path = os.path.join(output_folder, "_plan_cache.json")
                    with open(_plan_cache_path, "r", encoding="utf-8") as _pcf:
                        plan = json.load(_pcf)
                    print(f"INFO: [A2A] loaded plan cache: {len(plan.get('dag', []))} nodes")
                except Exception as _pce:
                    print(f"WARNING: [A2A] plan cache read failed: {_pce}")
                    plan = None
            else:
                plan = get_analysis_plan(session_id)

            if not plan or not plan.get("dag"):
                return {
                    "status": "error",
                    "error":  "Discovery produced no DAG",
                }

        dag  = plan["dag"]

        _should_apply_policy = bool(active_policy) and (
            active_policy.get("focus", "none") != "none"
            or bool(active_policy.get("excluded_analyses"))
            or bool(active_policy.get("required_analyses"))
            or active_policy.get("max_nodes", 10) < len(dag)
        )
        if _should_apply_policy:
            try:
                from tools.data_policy import apply_policy_to_dag
                dag = apply_policy_to_dag(dag, active_policy, col_roles_for_policy)
                print(f"[Policy] DAG after policy: {len(dag)} nodes")
            except Exception as policy_err:
                print(f"[Policy] apply_policy_to_dag failed (non-fatal): {policy_err}")

        if approved_metrics:
            dag = [
                n for n in dag
                if n["id"] in approved_metrics
            ]

        state.dag = dag

        pipeline = create_pipeline_state(
            session_id, dag
        )

        _post_message(
            state, "discovery_agent", "orchestrator",
            Intent.PLAN_READY, session_id,
            {"node_count": len(dag), "ready": True}
        )

        _update_session_status(state, "analyzing")

        results = await _execute_dag(
            session_id=session_id,
            dag=dag,
            pipeline=pipeline,
            csv_path=csv_path,
            output_folder=output_folder,
            state=state,
            run_agent_pipeline=run_agent_pipeline,
        )

        from tools.monitor import check_failure_threshold
        check_failure_threshold(session_id, results["total"], len(results["failed"]))

        _successful_count = len([
            r for r in state.results.values()
            if isinstance(r, dict) and r.get("status") == "success"
        ])
        if _successful_count == 0:
            print(f"ERROR: [{session_id}] All {results['total']} analyses failed. Skipping synthesis.")
            _update_session_status(state, "error")
            _evt_hook = _pipeline_event_hooks.get(session_id)
            if _evt_hook:
                _evt_hook("stream_end", {
                    "session_id": session_id,
                    "status": "error",
                    "error": "All analyses failed — likely due to API rate limiting. Please retry when the API is available.",
                })
            _pipeline_event_hooks.pop(session_id, None)
            return {"status": "error", "error": "All analyses failed", "session_id": session_id}

        if _USE_A2A_MULTISERVER:
            try:
                _results_payload = {
                    aid: res for aid, res in state.results.items()
                    if isinstance(res, dict) and res.get("status") == "success"
                }
                _results_cache_path = os.path.join(output_folder, "_results_cache.json")
                with open(_results_cache_path, "w", encoding="utf-8") as _rcf:
                    json.dump(_results_payload, _rcf)
                print(f"INFO: [A2A] results cache written: {len(_results_payload)} nodes for {session_id}")
            except Exception as _rce:
                print(f"WARNING: [A2A] results cache write failed: {_rce}")

        synthesis_prompt, image_paths = build_synthesis_prompt(session_id, state, dag, output_folder=output_folder)

        _mode = "A2A HTTP" if _USE_A2A_MULTISERVER else "SequentialAgent (ADK-native)"
        print(f"INFO: [{session_id}] Stages 4-6  - Synthesis -> Critic -> Report ({_mode})")
        _update_session_status(state, "synthesizing")

        await asyncio.sleep(_get_config()["pipeline"]["synthesis_delay"])

        try:
            _evt_hook = _pipeline_event_hooks.get(session_id)
            if _evt_hook:

                _synth_nodes_payload = []
                if state and hasattr(state, "results"):
                    for _aid, _r in state.results.items():
                        if isinstance(_r, dict) and _r.get("status") == "success":
                            _synth_nodes_payload.append({
                                "analysis_id":             _aid,
                                "analysis_type":            _r.get("analysis_type", ""),
                                "top_finding":              _r.get("top_finding", ""),
                                "chart_path":               bool(_r.get("chart_file_path")),
                                "severity":                 _r.get("severity", "info"),
                                "what_it_means":            _r.get("data", {}).get("narrative", {}).get("what_it_means", ""),
                                "recommendation":           _r.get("insight_summary", {}).get("recommendation", ""),
                                "proposed_fix":             _r.get("data", {}).get("narrative", {}).get("proposed_fix", ""),
                                "key_finding":              _r.get("insight_summary", {}).get("key_finding", ""),
                                "top_values":               _r.get("insight_summary", {}).get("top_values", ""),
                                "anomalies":                _r.get("insight_summary", {}).get("anomalies", ""),
                                "decision_maker_takeaway":  _r.get("insight_summary", {}).get("decision_maker_takeaway", ""),
                            })
                _evt_hook("synthesis_started", {
                    "session_id": session_id,
                    "completed_nodes": _synth_nodes_payload,
                })
        except Exception as _synth_start_err:
            logging.warning(f"[{session_id}] synthesis_started SSE failed: {_synth_start_err}")

        _post_dag_error = None
        try:
            if _USE_A2A_MULTISERVER:
                from agent_servers.a2a_client import (
                    call_synthesis as _a2a_synthesis,
                    call_critic as _a2a_critic,
                    call_dag_builder as _a2a_dag_builder,
                )
                print(f"INFO: [{session_id}] Calling synthesis via A2A HTTP")
                await _a2a_synthesis(session_id, output_folder, synthesis_prompt)
                print(f"INFO: [{session_id}] Calling critic via A2A HTTP")
                await _a2a_critic(session_id)
                print(f"INFO: [{session_id}] Calling dag_builder via A2A HTTP")
                await _a2a_dag_builder(session_id, output_folder)
            else:
                post_dag_prompt = (
                    f"Session ID: {session_id}\n"
                    f"Output folder: {output_folder}\n"
                    f"{synthesis_prompt}\n\n"
                    f"After synthesizing, review the synthesis for quality, then build the HTML report "
                    f"by calling tool_build_report(session_id='{session_id}', output_folder='{output_folder}')."
                )
                await asyncio.wait_for(
                    run_agent_pipeline(
                        f"{session_id}_post_dag",
                        post_dag_prompt,
                        agent=get_post_dag_agent(),
                        image_paths=image_paths,
                        max_turns=_get_config()["agents"]["max_turns"]["post_dag"],
                    ),
                    timeout=_get_config()["retry"]["llm_timeout"],
                )
        except asyncio.TimeoutError:
            _timeout_val = _get_config()["retry"]["llm_timeout"]
            _post_dag_error = f"Post-DAG pipeline timed out (>{_timeout_val:.0f}s)"
            print(f"ERROR: [{session_id}] {_post_dag_error}")
            logging.error(f"[{session_id}] {_post_dag_error}")
        except Exception as e:
            _post_dag_error = f"Post-DAG pipeline failed: {type(e).__name__}: {e}"
            print(f"ERROR: [{session_id}] {_post_dag_error}")
            logging.error(f"[{session_id}] {_post_dag_error}")

        try:
            _evt_hook = _pipeline_event_hooks.get(session_id)
            if _evt_hook:

                from agents.synthesis import get_synthesis_result as _get_syn
                _syn = _get_syn(session_id) or (state.synthesis if hasattr(state, "synthesis") else {}) or {}
                _narrative = (_syn.get("conversational_report") or "").strip()

                if _narrative:
                    import re as _re

                    _sections = _re.split(r'(?=\n#{1,2} |<h[12][\s>])', _narrative)
                    _sections = [s for s in _sections if s.strip()]
                    if len(_sections) < 2:

                        _sz = max(len(_narrative) // 4, 200)
                        _sections = [_narrative[i:i + _sz] for i in range(0, len(_narrative), _sz)]

                    for _ci, _section in enumerate(_sections):
                        _evt_hook("synthesis_chunk", {
                            "chunk":  _section,
                            "index":  _ci,
                            "first":  _ci == 0,
                            "total":  len(_sections),
                        })
                        if _ci < len(_sections) - 1:

                            await asyncio.sleep(_get_config()["pipeline"]["sse_sleep_spacing"])

                _completed_nodes_payload = []
                if state and hasattr(state, "results"):
                    for _aid, _r in state.results.items():
                        if isinstance(_r, dict) and _r.get("status") == "success":
                            _completed_nodes_payload.append({
                                "analysis_id":             _aid,
                                "analysis_type":            _r.get("analysis_type", ""),
                                "top_finding":              _r.get("top_finding", ""),
                                "chart_path":               bool(_r.get("chart_file_path")),
                                "severity":                 _r.get("severity", "info"),
                                "what_it_means":            _r.get("data", {}).get("narrative", {}).get("what_it_means", ""),
                                "recommendation":           _r.get("insight_summary", {}).get("recommendation", ""),
                                "proposed_fix":             _r.get("data", {}).get("narrative", {}).get("proposed_fix", ""),
                                "key_finding":              _r.get("insight_summary", {}).get("key_finding", ""),
                                "top_values":               _r.get("insight_summary", {}).get("top_values", ""),
                                "anomalies":                _r.get("insight_summary", {}).get("anomalies", ""),
                                "decision_maker_takeaway":  _r.get("insight_summary", {}).get("decision_maker_takeaway", ""),
                            })
                _evt_hook("synthesis_complete", {
                    "session_id":      session_id,
                    "completed_nodes": _completed_nodes_payload,
                })
        except Exception as _se:
            print(f"WARNING: [{session_id}] synthesis streaming failed: {_se}")

            try:
                _fb = _pipeline_event_hooks.get(session_id)
                if _fb:
                    _fb("synthesis_complete", {"session_id": session_id})
            except Exception as _fb_err:
                logging.warning(f"[{session_id}] synthesis_complete fallback SSE failed: {_fb_err}")

        if _USE_A2A_MULTISERVER:
            import os as _os
            _expected = _os.path.join(output_folder, "report.html")
            if _os.path.exists(_expected):
                report = {"status": "success", "report_path": _expected}
            else:

                print(f"WARNING: [{session_id}] A2A report missing — falling back to local tool_build_report")
                try:
                    from agents.dag_builder import tool_build_report
                    report = tool_build_report(session_id, output_folder)
                except Exception as _fb_err:
                    print(f"ERROR: [{session_id}] Local tool_build_report fallback also failed: {_fb_err}")
                    report = {"status": "error", "error": f"Report generation failed: {_fb_err}"}
        else:
            from agents.dag_builder import get_report_result
            report = get_report_result(session_id)

        report_path = report.get("report_path") if report else None

        if _post_dag_error and (not report or report.get("status") == "error"):
            _report_err = _post_dag_error
        elif not report or report.get("status") == "error":
            _report_err = (report.get("error", "Report not generated") if report
                           else "dag_builder returned no result")
        else:
            _report_err = None

        if _report_err:
            print(f"ERROR: [{session_id}] Report build failed: {_report_err}")
            try:
                _evt_hook = _pipeline_event_hooks.get(session_id)
                if _evt_hook:
                    _evt_hook("report_error", {
                        "session_id": session_id,
                        "error": _report_err,
                    })
            except Exception as _rpt_err:
                logging.warning(f"[{session_id}] report_error SSE failed: {_rpt_err}")

        try:
            _evt_hook = _pipeline_event_hooks.get(session_id)
            if _evt_hook:
                if report_path:
                    _evt_hook("report_ready", {"session_id": session_id, "report_path": str(report_path)})
                _evt_hook("stream_end", {"session_id": session_id, "status": "complete" if report_path else "error"})
        except Exception as _rpt_sse_err:
            logging.warning(f"[{session_id}] final SSE failed: {_rpt_sse_err}")
        finally:
            _pipeline_event_hooks.pop(session_id, None)

        _update_session_status(state, "complete")

        _global_threads.pop(session_id, None)
        _pipeline_store.pop(session_id, None)

        try:
            from tools.data_gate import register_schema
            register_schema(csv_path, state.dataset_type or None)
        except Exception as _rs_err:
            print(f"[DataGate] register_schema non-fatal: {_rs_err}")

        _post_message(
            state, "orchestrator", "frontend",
            Intent.REPORT_READY, session_id,
            {
                "report_path":   report_path,
                "total_results": len(state.results),
                "status":        "complete",
            }
        )

        return {
            "status":         "complete",
            "session_id":     session_id,
            "report_path":    report_path,
            "total_analyses": len(state.results),
            "dataset_type":   state.dataset_type,
            "results_summary": _build_results_summary(
                state
            ),
        }

    except Exception as e:
        return {
            "status": "error",
            "error":  str(e),
            "trace":  traceback.format_exc()[:1000],
        }

async def _execute_dag(
    session_id: str,
    dag: list,
    pipeline: PipelineState,
    csv_path: str,
    output_folder: str,
    state,
    run_agent_pipeline,
) -> dict:
    results      = {}
    max_rounds   = len(dag) + 2
    round_num    = 0

    _global_threads[session_id] = []

    for node_id, node in pipeline.nodes.items():
        status = pipeline.get_status(node_id)
        deps = node.get("depends_on") or [] if isinstance(node, dict) else []
        print(f"INFO INIT: {node_id} status={status} deps={deps}")

    print(f"INFO DAG: Starting with {len(dag)} nodes")
    for node in dag:
        print(f"INFO DAG: Node {node.get('id')} "
              f"type={node.get('analysis_type')} "
              f"deps={node.get('depends_on', [])}")

    async def run_node_with_retry(node_id):
        node = pipeline.nodes[node_id]
        pipeline.mark_running(node_id)

        from tools.monitor import emit
        emit(session_id, "node_started", {"node_id": node_id, "analysis_type": node.get("analysis_type")})

        try:
            _evt_hook_start = _pipeline_event_hooks.get(session_id)
            if _evt_hook_start:
                _evt_hook_start("node_started", {
                    "node_id":       node_id,
                    "analysis_type": node.get("analysis_type"),
                })
        except Exception as _start_err:
            logging.warning(f"[{session_id}] node_started SSE for {node_id} failed: {_start_err}")

        result = await _execute_single_node(
            session_id=session_id,
            node=node,
            csv_path=csv_path,
            output_folder=output_folder,
            state=state,
            run_agent_pipeline=run_agent_pipeline,
        )

        if result.get("status") == "success":

            _post_message(
                state, "coder_agent", "orchestrator",
                Intent.ANALYSIS_COMPLETE, session_id,
                {
                    "analysis_id":   node_id,
                    "analysis_type": node.get("analysis_type"),
                    "status":        "complete",
                }
            )
            pipeline.mark_complete(node_id)
            results[node_id] = result
            msg = f"SUCCESS NODE {node_id}: chart={result.get('chart_file_path','NONE')}"
            print(msg)
            logging.info(msg)
            emit(session_id, "node_complete", {"analysis_id": node_id, "retry": False, "chart": bool(result.get('chart_file_path')), "analysis_type": result.get("analysis_type") or node.get("analysis_type"), "top_finding": result.get("top_finding", ""), "severity": result.get("severity", "info")})
            return True

        last_error = result.get("error", "Unknown error")
        msg = (f"ERROR NODE {node_id}: First attempt failed: "
               f"status={result.get('status')} "
               f"error={last_error}")
        print(msg)
        logging.error(msg)
        emit(session_id, "node_failed_retry_pending", {"node_id": node_id, "error": last_error})

        corrected_node = _attempt_column_role_correction(node, last_error)
        if corrected_node is not node:
            print(f"[Self-Correction] Node {node_id}: column_roles auto-corrected. Retrying with fixed roles.")
            logging.info(f"[Self-Correction] {node_id}: corrected column_roles={corrected_node.get('column_roles')}")

        retry = await _execute_single_node(
            session_id=session_id,
            node=corrected_node,
            csv_path=csv_path,
            output_folder=output_folder,
            state=state,
            run_agent_pipeline=run_agent_pipeline,
            last_error=last_error,
        )

        if retry.get("status") == "success":

            _post_message(
                state, "coder_agent", "orchestrator",
                Intent.ANALYSIS_COMPLETE, session_id,
                {
                    "analysis_id":   node_id,
                    "analysis_type": node.get("analysis_type"),
                    "status":        "complete",
                    "top_finding":   retry.get("top_finding", ""),
                    "severity":      retry.get("severity", "info"),
                    "has_chart":     bool(retry.get("chart_file_path")),
                }
            )
            pipeline.mark_complete(node_id)
            results[node_id] = retry
            msg = f"SUCCESS NODE {node_id} (retry): chart={retry.get('chart_file_path','NONE')}"
            print(msg)
            logging.info(msg)
            emit(session_id, "node_complete", {"analysis_id": node_id, "retry": True, "chart": bool(retry.get('chart_file_path')), "analysis_type": retry.get("analysis_type") or node.get("analysis_type"), "top_finding": retry.get("top_finding", ""), "severity": retry.get("severity", "info")})
            return True
        else:
            msg = (f"ERROR NODE {node_id}: Retry also failed: "
                   f"status={retry.get('status')} "
                   f"error={retry.get('error', 'no error field')}")
            print(msg)
            logging.error(msg)
            emit(session_id, "node_failed", {"node_id": node_id, "error": retry.get('error', 'no error field')}, severity="error")
            try:
                _evt_hook = _pipeline_event_hooks.get(session_id)
                if _evt_hook:
                    _evt_hook("node_failed", {"node_id": node_id, "error": retry.get("error", "no error field")})
            except Exception as _fail_err:
                logging.warning(f"[{session_id}] node_failed SSE for {node_id} failed: {_fail_err}")

            _post_message(
                state, "coder_agent", "orchestrator",
                Intent.ANALYSIS_FAILED, session_id,
                {"analysis_id": node_id, "error": retry.get("error", "")},
            )
            pipeline.mark_failed(node_id)

            return False

    while not pipeline.is_complete() and \
          round_num < max_rounds:
        round_num += 1

        _completed_ids = get_completed_analysis_ids(state.message_log)
        _failed_ids    = get_failed_analysis_ids(state.message_log)
        ready_nodes    = pipeline.get_ready_to_run(_completed_ids, _failed_ids)
        print(f"INFO READY: {ready_nodes} (completed={len(_completed_ids)} failed={len(_failed_ids)})")

        if not ready_nodes:
            pending = [
                n for n in pipeline.nodes.values()
                if n.get("status") == NodeStatus.PENDING
            ]
            if pending:
                for node in pending:
                    pipeline.mark_blocked(node["id"])
            break

        _max_parallel = int(os.environ.get("MAX_PARALLEL_NODES", str(_get_config()["pipeline"]["max_parallel_nodes"])))
        semaphore = asyncio.Semaphore(_max_parallel)

        async def run_with_semaphore(nid):
            async with semaphore:
                return await run_node_with_retry(nid)

        results_list = await asyncio.gather(*(run_with_semaphore(nid) for nid in ready_nodes), return_exceptions=True)

        _c = get_completed_analysis_ids(state.message_log)
        _f = get_failed_analysis_ids(state.message_log)
        print(f"INFO DAG: Round complete. completed={len(_c)} failed={len(_f)}")

        for node_id, result in zip(ready_nodes, results_list):
            if isinstance(result, Exception):
                msg = (f"ERROR NODE {node_id}: Unhandled exception: "
                       f"{type(result).__name__}: {result}")
                print(msg)
                logging.error(msg)
                traceback.print_exception(result)

                _post_message(
                    state, "coder_agent", "orchestrator",
                    Intent.ANALYSIS_FAILED, session_id,
                    {"analysis_id": node_id, "error": str(result)},
                )
                pipeline.mark_failed(node_id)

                try:
                    _evt_hook = _pipeline_event_hooks.get(session_id)
                    if _evt_hook:
                        _evt_hook("node_failed", {
                            "node_id": node_id,
                            "error":   str(result),
                        })
                except Exception as _fail_err:
                    logging.warning(f"[{session_id}] node_failed SSE for {node_id} failed: {_fail_err}")

    return {
        "completed": get_completed_analysis_ids(state.message_log),
        "failed":    get_failed_analysis_ids(state.message_log),
        "total":     len(dag),
    }

async def _execute_single_node(
    session_id: str,
    node: dict,
    csv_path: str,
    output_folder: str,
    state,
    run_agent_pipeline,
    last_error: Optional[str] = None,
) -> dict:
    msg = f"INFO NODE: Executing {node.get('id')} type={node.get('analysis_type')}"
    print(msg)
    logging.info(msg)
    analysis_id   = node["id"]
    analysis_type = node.get("analysis_type") or "unknown"
    column_roles  = node.get("column_roles", {})
    library_fn    = node.get("library_function")
    description   = node.get("description", "")

    behavioral = {
        atype
        for atype, entry in LIBRARY_REGISTRY.items()
        if entry.get("col_role") == "behavioral"
    }

    behavioral |= {
        "funnel_analysis", "friction_detection",
        "survival_analysis", "user_segmentation",
        "sequential_pattern_mining", "association_rules",
        "dropout_analysis", "user_journey_analysis",
        "intervention_triggers", "session_classification",
        "path_analysis", "transition_analysis",
    }
    effective_csv = csv_path
    if analysis_type in behavioral:
        enriched = csv_path.replace(
            ".csv", "_sessions.csv"
        )
        if os.path.exists(enriched):
            effective_csv = enriched

    if column_roles:
        # Build column list from cached profile to avoid re-reading the CSV header per node.
        _actual_cols = None
        if state is not None and getattr(state, "raw_profile", None):
            _ct = state.raw_profile.get("column_types", {})
            _all = _ct.get("numeric", []) + _ct.get("categorical", []) + _ct.get("datetime", [])
            if _all:
                _actual_cols = _all
        column_roles = _validate_column_roles_against_csv(column_roles, effective_csv, actual_cols=_actual_cols)

        node = {**node, "column_roles": column_roles}

    _parent_context = ""
    _deps = node.get("depends_on", [])
    if _deps and state:
        _parent_facts = []
        for _pid in _deps:
            _pres = state.results.get(_pid)
            if _pres and isinstance(_pres, dict) and _pres.get("status") == "success":
                _ptf = _pres.get("top_finding", "")
                _patype = _pres.get("analysis_type", _pid)
                _pdm = _pres.get("insight_summary") or {}
                _pdm_text = _pdm.get("decision_maker_takeaway", "") if isinstance(_pdm, dict) else ""
                _entry = f"  [{_pid}: {_patype}] {_ptf}"
                if _pdm_text:
                    _entry += f"\n  Key insight: {_pdm_text}"
                _parent_facts.append(_entry)
        if _parent_facts:
            _parent_context = (
                "\n== PARENT NODE RESULTS (this analysis depends on these) ==\n"
                "Your analysis builds on these completed nodes. "
                "Reference them where relevant and let their findings shape your approach:\n"
                + "\n".join(_parent_facts) + "\n"
            )

    _global_ctx = ""
    _global_thread = _global_threads.get(session_id, [])
    _non_parent_entries = [
        e for e in _global_thread
        if e["id"] not in (_deps or []) and e["id"] != analysis_id
    ]
    if _non_parent_entries:
        _recent = _non_parent_entries[-5:]
        _global_ctx = (
            "\n== COMPLETED ANALYSES (pipeline context) ==\n"
            "These analyses finished before yours. Reference them to avoid duplication "
            "and to connect your findings to the broader picture:\n"
            + "\n".join(
                f"  [{e['id']}: {e['type']}] {e['finding']}"
                + (f"\n  Key insight: {e['dm_insight']}" if e.get('dm_insight') else "")
                for e in _recent
            )
            + "\n"
        )

    code = None
    if library_fn and any(
        entry.get("function") == library_fn
        for entry in LIBRARY_REGISTRY.values()
    ):
        for lib_type, entry in LIBRARY_REGISTRY.items():
            if entry["function"] == library_fn:
                _extra = {}
                if node.get("cohort_window"):
                    _extra["cohort_window"] = node["cohort_window"]
                code = _build_library_call_code(lib_type, column_roles, _extra)
                break

    if not code and analysis_type in LIBRARY_REGISTRY:
        _extra = {}
        if node.get("cohort_window"):
            _extra["cohort_window"] = node["cohort_window"]
        code = _build_library_call_code(analysis_type, column_roles, _extra)

    if not code:
        prompt = (
            f"Session ID: {session_id}\n"
            f"Analysis ID: {analysis_id}\n"
            f"Analysis Type: {analysis_type}\n"
            f"CSV Path: {effective_csv}\n"
            f"Column Roles: {json.dumps(column_roles, default=str)}\n"
            f"Description: {description}\n"
            + (f"Library Function: {library_fn}\n" if library_fn else "")
            + _parent_context
            + _global_ctx
            + (f"PREVIOUS ERROR: {last_error}\n" if last_error else "")
            + "\nWrite a Python function `analyze(csv_path: str) -> dict` that performs this analysis. "
            "Return ONLY the code block. Use the rules in your system instructions."
        )

        if _USE_A2A_MULTISERVER:
            from agent_servers.a2a_client import call_coder as _a2a_coder
            response = await _a2a_with_fallback(
                _a2a_coder(session_id, prompt),
                run_agent_pipeline(f"{session_id}_{analysis_id}", prompt, agent_getter="coder"),
                "coder", session_id,
            )
        else:
            response = await run_agent_pipeline(
                f"{session_id}_{analysis_id}",
                prompt,
                agent_getter="coder",
            )

        import re
        code_match = re.search(r"```(?:python)?\s*(.*?)\s*```", response, re.DOTALL | re.IGNORECASE)
        if code_match:
            code = code_match.group(1).strip()
        else:
            func_match = re.search(r"(import\s+pandas.*|def\s+analyze.*)", response, re.DOTALL | re.IGNORECASE)
            code = func_match.group(1).strip() if func_match else response.strip()

    try:

        from tools.code_executor import (
            validate_code,
            execute_analysis,
            validate_output_quality,
            submit_result,
        )

        val = validate_code(code, effective_csv)
        if not val["valid"]:
             return {"status": "error", "error": f"Validation failed: {val['issues']}"}

        _exec_fn = functools.partial(
            execute_analysis,
            code=code,
            csv_path=effective_csv,
            analysis_id=analysis_id,
            analysis_type=analysis_type,
            output_folder=output_folder,
        )
        try:
            _node_timeout = _get_config()["retry"]["node_timeout"]
            result = await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(None, _exec_fn),
                timeout=_node_timeout,
            )
        except asyncio.TimeoutError:
            _node_timeout = _get_config()["retry"]["node_timeout"]
            return {"status": "error", "error": f"Execution timed out after {_node_timeout:.0f}s"}

        qual = validate_output_quality(result, analysis_type)
        if not qual["quality_pass"]:
            print(f"WARNING: Quality check failed for {analysis_id}: {qual['issues']}")

        submit_result(session_id, analysis_id, analysis_type, result)
        state.store_result(analysis_id, result)

        print(
            f"DEBUG NODE [{analysis_id}]: status={result.get('status')} "
            f"chart={result.get('chart_file_path', 'NONE')} "
            f"top_finding={str(result.get('top_finding', ''))[:80]}"
        )

        if analysis_type == "user_segmentation":
            try:
                _segments = (result.get("data") or {}).get("segments", [])
                if _segments:
                    _seg_descriptions = []
                    for _seg in _segments[:8]:
                        _seg_descriptions.append(
                            f"Cluster {_seg.get('cluster_id', '?')}: "
                            f"size={_seg.get('size', 0)}, "
                            f"top_events={_seg.get('top_events', [])[:3]}, "
                            f"mean_depth={_seg.get('mean_depth', 0)}"
                        )
                    _seg_prompt = (
                        "You are naming user segments from a behavioral clustering analysis.\n"
                        "For each cluster below, return ONE short, descriptive name (2-4 words).\n"
                        "Names should reflect the user behavior pattern, not the numbers.\n"
                        "Examples: 'Power Users', 'Window Shoppers', 'One-Time Visitors'.\n\n"
                        + "\n".join(_seg_descriptions)
                        + "\n\nReturn ONLY a JSON object: {\"0\": \"name\", \"1\": \"name\", ...}"
                    )
                    _seg_resp = await _llm_generate_with_retry(
                        contents=_seg_prompt,
                        model=get_model("coder"),
                        label=f"{analysis_id}/seg-naming",
                    )
                    if _seg_resp:
                        _seg_text = _seg_resp.text.strip()
                        _seg_match = re.search(r'\{[^}]+\}', _seg_text, re.DOTALL)
                        if _seg_match:
                            _seg_names = json.loads(_seg_match.group())
                            result["data"]["segment_names"] = _seg_names

                            for _seg in _segments:
                                _cid = str(_seg.get("cluster_id", ""))
                                if _cid in _seg_names:
                                    _seg["name"] = _seg_names[_cid]
                            state.store_result(analysis_id, result)
                            print(f"INFO: [{analysis_id}] Segment names assigned: {_seg_names}")
            except Exception as _seg_err:
                print(f"WARNING: [{analysis_id}] Cluster naming failed: {_seg_err}")

        try:
            _key_data = {
                k: v for k, v in result.get("data", {}).items()
                if isinstance(v, (int, float, str, bool)) and v is not None
            }
            _dm_prompt = (
                f"Analysis type: {analysis_type}\n"
                f"Top finding: {result.get('top_finding', '')}\n"
                f"Key metrics: {json.dumps(_key_data, default=str)[:400]}\n"
                + (_parent_context if _parent_context else "")
                + (_global_ctx if _global_ctx else "")
                + "\nWrite ONE sentence only  - what is the single most important thing "
                "a business decision-maker needs to act on from this result? "
                "Be specific. Cite the actual number. "
                "If prior analyses are shown above, connect this finding to the broader pattern. "
                "Do not start with 'The analysis shows' or 'Based on'."
            )
            _dm_resp = await _llm_generate_with_retry(
                contents=_dm_prompt,
                model=get_model("coder"),
                label=f"{analysis_id}/dm-insight",
            )
            _dm_text = _dm_resp.text.strip().split('\n')[0]

            _parts = re.split(r'\.\s+', _dm_text)
            _dm_text = _parts[0].strip()
            if not _dm_text.endswith('.'):
                _dm_text += '.'

            if not isinstance(result.get("insight_summary"), dict):
                result["insight_summary"] = {}
            result["insight_summary"]["decision_maker_takeaway"] = _dm_text
            if analysis_id in state.results:
                if not isinstance(state.results[analysis_id].get("insight_summary"), dict):
                    state.results[analysis_id]["insight_summary"] = {}
                state.results[analysis_id]["insight_summary"]["decision_maker_takeaway"] = _dm_text
            print(f"INFO: [{analysis_id}] Insight: {_dm_text[:120]}")
        except Exception as _dm_err:
            print(f"WARNING: Per-node insight skipped for {analysis_id}: {_dm_err}")

        _dm_text_for_thread = ""
        try:
            _dm_text_for_thread = (
                state.results[analysis_id].get("insight_summary", {}) or {}
            ).get("decision_maker_takeaway", "")
        except Exception as _thr_err:
            logging.warning(f"[{analysis_id}] Failed to read DM takeaway for thread: {_thr_err}")
        if session_id in _global_threads:
            _global_threads[session_id].append({
                "id": analysis_id,
                "type": analysis_type,
                "finding": result.get("top_finding", ""),
                "dm_insight": _dm_text_for_thread,
            })

        try:
            _evt_hook = _pipeline_event_hooks.get(session_id)
            if _evt_hook:
                _evt_hook("node_complete", {
                    "analysis_id":             analysis_id,
                    "analysis_type":            analysis_type,
                    "top_finding":              result.get("top_finding", ""),
                    "severity":                 result.get("severity", "info"),
                    "chart":                    bool(result.get("chart_file_path")),
                    "status":                   "success",
                    "what_it_means":            result.get("data", {}).get("narrative", {}).get("what_it_means", ""),
                    "recommendation":           result.get("insight_summary", {}).get("recommendation", ""),
                    "proposed_fix":             result.get("data", {}).get("narrative", {}).get("proposed_fix", ""),
                    "key_finding":              result.get("insight_summary", {}).get("key_finding", ""),
                    "top_values":               result.get("insight_summary", {}).get("top_values", ""),
                    "anomalies":                result.get("insight_summary", {}).get("anomalies", ""),
                    "decision_maker_takeaway":  result.get("insight_summary", {}).get("decision_maker_takeaway", ""),
                })
        except Exception as _hook_err:
            print(f"WARNING: SSE event hook failed for {analysis_id}: {_hook_err}")

        return {**result, "analysis_type": analysis_type, "status": "success"}

    except Exception as e:
        result = {
            "status": "error",
            "error":  str(e),
        }

    msg = (f"INFO NODE: {node.get('id')} done. "
           f"status={result.get('status','unknown')} "
           f"chart={result.get('chart_file_path','NONE')} "
           f"error={result.get('error','')}")
    print(msg)
    logging.info(msg)
    return result

async def execute_single_analysis(
    session_id: str,
    analysis_type: str,
    analysis_id: str,
    csv_path: str,
    output_folder: str,
    description: str,
    column_roles: dict,
    state,
) -> dict:
    from main import run_agent_pipeline

    node = {
        "id":               analysis_id,
        "analysis_type":    analysis_type,
        "description":      description,
        "column_roles":     column_roles,
        "library_function": LIBRARY_REGISTRY.get(analysis_type, {}).get("function"),
        "depends_on":       [],
    }

    return await _execute_single_node(
        session_id=session_id,
        node=node,
        csv_path=csv_path,
        output_folder=output_folder,
        state=state,
        run_agent_pipeline=run_agent_pipeline,
    )

def _build_library_call_code(
    analysis_type: str,
    column_roles: dict,
    extra_kwargs: dict = None,
) -> str:
    entry = LIBRARY_REGISTRY.get(analysis_type)
    if not entry:
        return ""

    fn = entry["function"]
    args = []
    args.append("csv_path=csv_path")

    for key, val in column_roles.items():
        if val:
            if key in entry["required_args"]:
                args.append(f"{key}='{val}'")

    if extra_kwargs:
        for k, v in extra_kwargs.items():
            args.append(f"{k}='{v}'")

    arg_str = ", ".join(args)

    if entry.get("is_custom"):
        script_dir  = entry.get("_script_dir", "")
        module_name = entry.get("_module_name", fn)
        code = (
            "import pandas as pd\n"
            "import numpy as np\n"
            "import sys as _sys, os as _os\n"
            f"_sd = {repr(script_dir)}\n"
            "if _sd and _sd not in _sys.path:\n"
            "    _sys.path.insert(0, _sd)\n"
            f"from {module_name} import {fn}\n\n"
            "def analyze(csv_path: str) -> dict:\n"
            f"    return {fn}({arg_str})\n"
        )
        return code

    code = (
        "import pandas as pd\n"
        "import numpy as np\n"
        f"from tools.analysis_library import {fn}\n\n"
        "def analyze(csv_path: str) -> dict:\n"
        f"    return {fn}({arg_str})\n"
    )
    return code

def _attempt_column_role_correction(node: dict, error_str: str) -> dict:
    analysis_type = node.get("analysis_type", "")
    column_roles = node.get("column_roles", {})
    entry = LIBRARY_REGISTRY.get(analysis_type, {})
    required_args = [a for a in entry.get("required_args", []) if a != "csv_path"]

    if not required_args or not error_str:
        return node

    corrected_roles = dict(column_roles)
    fix_applied = False

    missing_match = re.search(
        r"missing\s+(?:required\s+)?(?:keyword\s+)?argument[:\s'\"]+([a-z_]+)", error_str, re.IGNORECASE
    )
    if missing_match:
        missing_arg = missing_match.group(1)
        if missing_arg in required_args and missing_arg not in corrected_roles:

            existing_values = list(corrected_roles.values())
            if existing_values:
                corrected_roles[missing_arg] = existing_values[0]
                fix_applied = True
                print(f"[Self-Correction] Fixed missing arg '{missing_arg}' â†’ '{existing_values[0]}'")

    unexpected_match = re.search(
        r"unexpected\s+keyword\s+argument[:\s'\"]+([a-z_]+)", error_str, re.IGNORECASE
    )
    if unexpected_match:
        bad_key = unexpected_match.group(1)
        if bad_key in corrected_roles:
            value = corrected_roles.pop(bad_key)

            for req_arg in required_args:
                if req_arg not in corrected_roles:
                    corrected_roles[req_arg] = value
                    fix_applied = True
                    print(f"[Self-Correction] Remapped '{bad_key}' â†’ '{req_arg}' = '{value}'")
                    break

    for req_arg in required_args:
        if req_arg not in corrected_roles or not corrected_roles[req_arg]:
            existing_values = [v for v in corrected_roles.values() if v]
            if existing_values:
                corrected_roles[req_arg] = existing_values[0]
                fix_applied = True
                print(f"[Self-Correction] Filled missing key '{req_arg}' with '{existing_values[0]}'")

    if not fix_applied:
        return node

    corrected = copy.deepcopy(node)
    corrected["column_roles"] = corrected_roles
    return corrected

def _validate_column_roles_against_csv(
    column_roles: dict, csv_path: str, actual_cols: list = None
) -> dict:
    if not column_roles or not csv_path:
        return column_roles

    if actual_cols is None:
        try:
            import pandas as _pd
            actual_cols = list(_pd.read_csv(csv_path, nrows=0).columns)
        except Exception:
            return column_roles

    if not actual_cols:
        return column_roles

    actual_lower = {c.lower(): c for c in actual_cols}
    corrected = copy.copy(column_roles)
    fixed = False

    for role_key, col_name in column_roles.items():
        if not isinstance(col_name, str) or not col_name:
            continue

        if col_name in actual_cols:
            continue

        if col_name.lower() in actual_lower:
            canonical = actual_lower[col_name.lower()]
            corrected[role_key] = canonical
            fixed = True
            print(f"[ColGuard] '{col_name}' â†’ '{canonical}' (case fix for role '{role_key}')")
            logging.info(f"[ColGuard] case fix: '{col_name}' â†’ '{canonical}'")
            continue

        matches = difflib.get_close_matches(col_name, actual_cols, n=1, cutoff=_get_config()["data_quality"]["column_match_cutoff"])
        if matches:
            corrected[role_key] = matches[0]
            fixed = True
            print(f"[ColGuard] '{col_name}' â†’ '{matches[0]}' (fuzzy fix for role '{role_key}')")
            logging.info(f"[ColGuard] fuzzy fix: '{col_name}' â†’ '{matches[0]}'")
        else:

            print(f"[ColGuard] WARNING: '{col_name}' (role '{role_key}') not found in CSV headers "
                  f"and no close match found. CSV has: {actual_cols[:10]}")
            logging.warning(f"[ColGuard] unresolved column: '{col_name}' for role '{role_key}'")

    if not fixed:
        return column_roles
    return corrected

def _update_session_status(state, status: str):
    try:
        state.status = status
    except Exception as _uss_err:
        logging.warning(f"Failed to update session status to '{status}': {_uss_err}")

def _post_message(
    state, sender, recipient,
    intent, session_id, payload
):
    try:
        msg = create_message(
            sender=sender,
            recipient=recipient,
            intent=intent,
            payload=payload,
            session_id=session_id,
        )
        state.post_message(msg)
    except Exception as _pm_err:
        logging.warning(f"Failed to post A2A message ({sender}->{recipient}, intent={intent}): {_pm_err}")

def _build_profile_summary(
    profiler_data: dict,
    csv_path: str,
) -> str:
    classification = profiler_data.get(
        "classification", {}
    )
    raw = profiler_data.get("raw_profile", {})

    dataset_type = classification.get(
        "dataset_type", "unknown"
    )
    column_roles = classification.get(
        "column_roles", {}
    )
    recommended  = classification.get(
        "recommended_analyses", []
    )
    row_count    = raw.get("row_count", 0)
    col_count    = raw.get("column_count", 0)

    return (
        f"dataset_type: {dataset_type}\n"
        f"row_count: {row_count}\n"
        f"col_count: {col_count}\n"
        f"column_roles: {json.dumps(column_roles, default=str)}\n"
        f"recommended_analyses: "
        f"{json.dumps(recommended, default=str)}\n"
        f"csv_path: {csv_path}\n"
    )

def _build_results_summary(state) -> dict:
    summary = {}
    for aid, result in state.results.items():
        summary[aid] = {
            "analysis_type": result.get(
                "analysis_type"
            ),
            "status":        result.get("status"),
            "top_finding":   result.get(
                "top_finding", ""
            )[:100],
            "severity":      result.get("severity"),
        }
    return summary

def get_pipeline_status(session_id: str) -> dict:
    pipeline = _pipeline_store.get(session_id)
    if not pipeline:
        return {
            "session_id": session_id,
            "status":     "not_started",
            "progress":   0,
        }

    return {
        "session_id":    session_id,
        "progress_pct":  pipeline.progress_pct(),
        "is_complete":   pipeline.is_complete(),
        "node_statuses": {
            nid: node.get("status")
            for nid, node in pipeline.nodes.items()
        },
    }

_root_agent_instance = None

def get_root_agent():
    global _root_agent_instance
    if _root_agent_instance is None:
        from google.adk.agents import Agent
        from agents.profiler    import get_profiler_agent
        from agents.discovery   import get_discovery_agent
        from agents.coder       import get_coder_agent

        _root_agent_instance = Agent(
            name="orchestrator",
            model=get_model("orchestrator"),
            description=(
                "Central coordinator. Routes messages, "
                "manages DAG execution order, "
                "coordinates all agents."
            ),
            instruction=(
                "You are the central coordinator. "
                "You never run analyses directly. "
                "You delegate to sub-agents and "
                "track pipeline progress."
            ),
            sub_agents=[
                get_profiler_agent(),
                get_discovery_agent(),
                get_coder_agent(),
            ],
        )
    return _root_agent_instance

