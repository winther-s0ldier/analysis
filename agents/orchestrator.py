import os
import sys
import json
import asyncio
import logging
from typing import Optional, List, Dict, Any

logging.basicConfig(
    filename='pipeline.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s',
    encoding='utf-8'
)

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..")
)

from a2a_messages import (
    create_message,
    Intent,
    NodeStatus,
    PipelineState,
    get_messages_by_intent,
    get_latest_message,
    is_dependency_resolved,
    get_completed_analysis_ids,
)
from tools.analysis_library import LIBRARY_REGISTRY
from tools.model_config import get_model


_pipeline_store: dict = {}


def get_pipeline_state(
    session_id: str
) -> Optional[PipelineState]:
    """Get pipeline state for a session."""
    return _pipeline_store.get(session_id)


def create_pipeline_state(
    session_id: str,
    dag: list,
) -> PipelineState:
    """Create and store a new PipelineState."""
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



async def run_full_pipeline(
    session_id: str,
    csv_path: str,
    output_folder: str,
    approved_metrics: list = None,
    custom_requests: list = None,
    state=None,
) -> dict:
    """
    Run the complete analysis pipeline for a session.

    Stage 1: Profile (profiler_agent)
    Stage 2: Discover (discovery_agent)
    Stage 3: Execute DAG (coder_agent × N)
    Stage 4: Synthesize (synthesis_agent)
    Stage 5: Build Report (dag_builder_agent)

    Args:
        session_id: current session
        csv_path: path to normalized CSV
        output_folder: where to save charts + report
        approved_metrics: list of approved analysis IDs
                          (None = run all)
        custom_requests: user-added custom metrics

    Returns:
        dict with status, report_path, results_summary
    """
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

    # --- Phase 3 / Group D: Inject custom external analyses into registry
    from tools.analysis_library import LIBRARY_REGISTRY
    from tools.workflow_loader import register_custom_analyses
    register_custom_analyses(LIBRARY_REGISTRY)

    os.makedirs(output_folder, exist_ok=True)

    try:
        from main import run_agent_pipeline, extract_json

        _update_session_status(state, "profiling")

        profiler_prompt = (
            f"csv_path: {csv_path}\n"
            f"session_id: {session_id}\n"
            f"Call tool_profile_and_classify now."
        )

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

        state.raw_profile  = profiler_data.get(
            "raw_profile", {}
        )
        state.semantic_map = profiler_data.get(
            "classification", {}
        )
        state.dataset_type = profiler_data.get(
            "classification", {}
        ).get("dataset_type", "")

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

        # Load policy and build context for Discovery agent
        try:
            from tools.data_policy import get_active_policy, build_policy_context_for_discovery
            active_policy = get_active_policy()
            
            # --- Group D: Workflow Overrides ---
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

        discovery_prompt = (
            f"Session ID: {session_id}\n"
            f"CSV file path: {csv_path}\n"
            f"Output folder: {output_folder}\n\n"
            f"PROFILER OUTPUT:\n{profile_summary}\n\n"
            + (f"{policy_context}\n\n" if policy_context else "") +
            f"INSTRUCTIONS:\n"
            f"1. Reason about the data and the policy context above.\n"
            f"2. Construct a JSON DAG of MetricSpec nodes (id, name, analysis_type, library_function, column_roles, depends_on).\n"
            f"3. Call tool_submit_analysis_plan(session_id, dag_json_str) with your JSON result.\n"
        )

        # Reuse the plan already set on state.dag by the /discover endpoint.
        # get_analysis_plan() uses .pop() so it is consumed after the /discover
        # call — reading it again here would return None. state.dag is the
        # correct source of truth: set at main.py:/discover after the agent runs.
        if getattr(state, "dag", None):
            plan = {
                "dag":        state.dag,
                "metrics":    getattr(state, "discovery", {}).get("metrics", []),
                "node_count": len(state.dag),
            }
            print(f"INFO: Reusing /discover plan with {len(state.dag)} nodes for {session_id}")
        else:
            # No prior plan — run discovery from scratch (e.g. direct /analyze call)
            from agents.discovery import get_analysis_plan
            discovery_response = await run_agent_pipeline(
                f"{session_id}_discovery",
                discovery_prompt,
                agent_getter="discovery",
            )

            plan = get_analysis_plan(session_id)

            if not plan or not plan.get("dag"):
                return {
                    "status": "error",
                    "error":  "Discovery produced no DAG",
                }

        dag  = plan["dag"]

        # Apply active policy to filter/prioritise/cap the DAG
        # Explicit bool + parentheses prevents Python and/or precedence bug
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

        # Assign state.dag AFTER all filtering so the status endpoint
        # reports the exact node count that will actually execute.
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

        _rich_results = []
        for _nid, _res in state.results.items():
            _rich_results.append({
                "analysis_id":    _nid,
                "analysis_type":  _res.get("analysis_type", "unknown"),
                "top_finding":    _res.get("top_finding", ""),
                "severity":       _res.get("severity", "info"),
                "confidence":     _res.get("confidence", 0.0),
                "data":           _res.get("data", {}),
                "insight_summary": _res.get("insight_summary", {}),
            })
        import math
        def _clean_data(obj):
            if isinstance(obj, dict):
                return {k: _clean_data(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [_clean_data(x) for x in obj]
            elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                return None
            elif hasattr(obj, "item"): # Handle numpy types
                return _clean_data(obj.item())
            elif isinstance(obj, (bool, int, float, str)) or obj is None:
                return obj
            else:
                return str(obj)

        _rich_summary = json.dumps(_clean_data(_rich_results), indent=2) if _rich_results else "[]"

        # --- Pre-build fact_sheet so synthesis has verified numbers before reasoning begins ---
        # This means the LLM doesn't need a tool call to know what to cite.
        try:
            from agents.synthesis import _extract_node_facts as _efs
            _fact_sheet_for_prompt = {}
            _dag_atype_to_id = {
                n.get("analysis_type"): n.get("id")
                for n in dag
                if n.get("id") and n.get("analysis_type")
            }
            for _aid, _res in state.results.items():
                if not isinstance(_res, dict):
                    continue
                _atype = _res.get("analysis_type", "unknown")
                _nid = _dag_atype_to_id.get(_atype, _aid)
                _fact_sheet_for_prompt[_nid] = _efs(_nid, _aid, _res)
            _fact_sheet_json = json.dumps(_clean_data(_fact_sheet_for_prompt), indent=2)
        except Exception as _fse:
            _fact_sheet_json = "{}"
            print(f"WARNING: fact_sheet pre-build failed: {_fse}")

        # DAG dependency map — lets synthesis reason causally (child depends on parent)
        _dag_dep_lines = []
        for _n in dag:
            _deps = _n.get("depends_on", [])
            _dep_str = f" -> depends on {_deps}" if _deps else " -> root node"
            _dag_dep_lines.append(
                f"  {_n.get('id')} [{_n.get('analysis_type')}]{_dep_str}"
            )
        _dag_graph_str = "\n".join(_dag_dep_lines) if _dag_dep_lines else "  (no DAG available)"

        # User goal — focus the synthesis on what the user actually wants answered
        _user_goal_block = ""
        if getattr(state, "user_instructions", ""):
            _user_goal_block = (
                f"\n== USER GOAL / STAKEHOLDER QUESTION ==\n"
                f"{state.user_instructions}\n"
                f"Your synthesis MUST directly address this. "
                f"top_priorities and the Action Roadmap must map to this goal.\n"
            )

        # Dataset description
        _profile_block = ""
        _raw = getattr(state, "raw_profile", {}) or {}
        if _raw:
            _col_roles = (getattr(state, "semantic_map", {}) or {}).get("column_roles", {})
            _profile_block = (
                f"\n== DATASET DESCRIPTION ==\n"
                f"Rows: {_raw.get('row_count', 'unknown')} | "
                f"Columns: {_raw.get('column_count', 'unknown')} | "
                f"Type: {state.dataset_type}\n"
                f"Column roles: {json.dumps(_col_roles, default=str)}\n"
            )

        synthesis_prompt = (
            f"Session ID: {session_id}\n"
            f"Dataset type: {state.dataset_type}\n"
            f"Total analyses completed: {len(state.results)}\n"
            f"{_profile_block}"
            f"{_user_goal_block}"
            f"\n== DAG DEPENDENCY GRAPH (use this to reason causally) ==\n"
            f"{_dag_graph_str}\n"
            f"\n== PRE-EXTRACTED FACT SHEET (cite ONLY these numbers — do not invent) ==\n"
            f"{_fact_sheet_json}\n\n"
            f"== FULL ANALYSIS RESULTS (raw — for additional context) ==\n"
            f"{_rich_summary}\n\n"
            f"INSTRUCTIONS:\n"
            f"1. Call tool_aggregate_results(session_id) to get column_roles context (required).\n"
            f"2. Before writing anything, reason step-by-step: what did each node find? "
            f"What is surprising? What connects across nodes? THEN build your synthesis.\n"
            f"3. Use the DAG DEPENDENCY GRAPH to identify causal chains "
            f"(child node results are constrained by their parent — reason causally, not just correlationally).\n"
            f"4. Look closely at the provided chart images. Describe visual trends natively in your analysis.\n"
            f"5. SELF-REVIEW before calling tool_submit_synthesis: "
            f"(a) every insight cites a [NodeID] with a specific number, "
            f"(b) cross_metric_connections has entries citing 2 different node pairs, "
            f"(c) conversational_report contains '# Key Findings', '# Action Roadmap', '# Confidence Assessment'.\n"
            f"6. Call tool_submit_synthesis(session_id, synthesis_json_str)."
        )

        image_paths = []
        for _res in state.results.values():
            chart_path = _res.get("chart_file_path")
            if chart_path:
                png_path = chart_path.replace(".html", ".png")
                if os.path.exists(png_path):
                    image_paths.append(png_path)

        try:
            await run_agent_pipeline(
                f"{session_id}_synthesis",
                synthesis_prompt,
                agent_getter="synthesis",
                image_paths=image_paths,
            )
        except Exception as e:
            msg = f"ERROR: Synthesis agent failed for session {session_id}: {e}"
            print(msg)
            logging.error(msg)

        # Wait for state.synthesis to be populated before building the report.
        # The synthesis LLM may need 2 turns (first call fails, retries succeed),
        # and run_agent_pipeline returns after the first final response — which may
        # be the failed call. This poll ensures we capture the successful retry.
        _synthesis_wait_secs = 0
        while not bool(state.synthesis) and _synthesis_wait_secs < 8:
            # Also try to recover from _synthesis_store directly on each tick
            try:
                from agents.synthesis import get_synthesis_result
                _stored = get_synthesis_result(session_id)
                if _stored:
                    state.synthesis = _stored
                    print(f"INFO: Orchestrator recovered synthesis from _synthesis_store after {_synthesis_wait_secs}s")
                    break
            except Exception:
                pass
            await asyncio.sleep(0.5)
            _synthesis_wait_secs += 0.5
        if bool(state.synthesis):
            print(f"INFO: Synthesis ready after {_synthesis_wait_secs}s wait.")
        else:
            print("WARNING: Synthesis not stored after 8s wait — report will have no insights.")

        _update_session_status(
            state, "building_report"
        )

        report_prompt = (
            f"Session ID: {session_id}\n"
            f"Output folder: {output_folder}\n"
            f"Call tool_build_report(session_id, output_folder) now."
        )

        try:
            await run_agent_pipeline(
                f"{session_id}_report",
                report_prompt,
                agent_getter="dag_builder",
            )
        except Exception as e:
            msg = f"ERROR: DAG builder agent failed for session {session_id}: {e}"
            print(msg)
            logging.error(msg)

        from agents.dag_builder import get_report_result
        report = get_report_result(session_id)
        report_path = report.get(
            "report_path"
        ) if report else None

        _update_session_status(state, "complete")
        
        # Group A: Register the schema fingerprint so drift detection works on next run
        try:
            from tools.data_gate import register_schema
            register_schema(csv_path, state.dataset_type or None)
        except Exception as _rs_err:
            print(f"[DataGate] register_schema non-fatal: {_rs_err}")

        _post_message(
            state, "orchestrator", "frontend",
            Intent.BUILD_REPORT, session_id,
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
        import traceback
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
    """
    Execute DAG nodes in dependency order.
    Nodes with no dependencies run first.
    Dependent nodes run after their dependencies complete.
    Retries failed nodes up to 2 times.
    Marks blocked nodes if dependency failed.
    """
    results      = {}
    max_rounds   = len(dag) + 2
    round_num    = 0
    completed    = set()
    failed       = set()

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
        
        result = await _execute_single_node(
            session_id=session_id,
            node=node,
            csv_path=csv_path,
            output_folder=output_folder,
            state=state,
            run_agent_pipeline=run_agent_pipeline,
        )

        if result.get("status") == "success":
            pipeline.mark_complete(node_id)
            completed.add(node_id)
            results[node_id] = result
            msg = f"SUCCESS NODE {node_id}: chart={result.get('chart_file_path','NONE')}"
            print(msg)
            logging.info(msg)
            
            emit(session_id, "node_succeeded", {"node_id": node_id, "retry": False, "chart": bool(result.get('chart_file_path'))})
            
            _post_message(
                state, "coder_agent", "orchestrator",
                Intent.ANALYSIS_COMPLETE, session_id,
                {
                    "analysis_id":   node_id,
                    "analysis_type": node.get("analysis_type"),
                    "status":        "complete",
                }
            )
            return True

        last_error = result.get("error", "Unknown error")
        msg = (f"ERROR NODE {node_id}: First attempt failed: "
               f"status={result.get('status')} "
               f"error={last_error}")
        print(msg)
        logging.error(msg)
        emit(session_id, "node_failed_retry_pending", {"node_id": node_id, "error": last_error})

        # ── Stage 4: Self-Correction Hook ──────────────────────────────────
        # Before blind retry, attempt to auto-correct column role mismatches.
        # Detects TypeError / KeyError patterns and rewrites column_roles
        # using the LIBRARY_REGISTRY schema — zero extra LLM calls needed.
        corrected_node = _attempt_column_role_correction(node, last_error)
        if corrected_node is not node:
            print(f"[Self-Correction] Node {node_id}: column_roles auto-corrected. Retrying with fixed roles.")
            logging.info(f"[Self-Correction] {node_id}: corrected column_roles={corrected_node.get('column_roles')}")
        # ───────────────────────────────────────────────────────────────────

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
            pipeline.mark_complete(node_id)
            completed.add(node_id)
            results[node_id] = retry
            msg = f"SUCCESS NODE {node_id} (retry): chart={retry.get('chart_file_path','NONE')}"
            print(msg)
            logging.info(msg)
            
            emit(session_id, "node_succeeded", {"node_id": node_id, "retry": True, "chart": bool(retry.get('chart_file_path'))})
            
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
            return True
        else:
            msg = (f"ERROR NODE {node_id}: Retry also failed: "
                   f"status={retry.get('status')} "
                   f"error={retry.get('error', 'no error field')}")
            print(msg)
            logging.error(msg)
            
            emit(session_id, "node_failed", {"node_id": node_id, "error": retry.get('error', 'no error field')}, severity="error")
            
            pipeline.mark_failed(node_id)
            failed.add(node_id)
            for other_node in dag:
                if node_id in other_node.get("depends_on", []):
                    pipeline.mark_blocked(other_node["id"])
            return False

    while not pipeline.is_complete() and \
          round_num < max_rounds:
        round_num += 1

        ready_nodes = pipeline.get_ready_to_run(
            list(completed), list(failed)
        )
        print(f"INFO READY: {ready_nodes}")

        if not ready_nodes:
            pending = [
                n for n in pipeline.nodes.values()
                if n.get("status") == NodeStatus.PENDING
            ]
            if pending:
                for node in pending:
                    pipeline.mark_blocked(node["id"])
            break

        semaphore = asyncio.Semaphore(2)
        
        async def run_with_semaphore(nid):
            async with semaphore:
                return await run_node_with_retry(nid)
                
        results_list = await asyncio.gather(*(run_with_semaphore(nid) for nid in ready_nodes), return_exceptions=True)

        print(f"INFO DAG: Round complete. "
              f"completed={len(completed)} "
              f"failed={len(failed)}")

        for node_id, result in zip(ready_nodes, results_list):
            if isinstance(result, Exception):
                import traceback
                msg = (f"ERROR NODE {node_id}: Unhandled exception: "
                       f"{type(result).__name__}: {result}")
                print(msg)
                logging.error(msg)
                traceback.print_exception(result)
                pipeline.mark_failed(node_id)
                failed.add(node_id)

    return {
        "completed": list(completed),
        "failed":    list(failed),
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
    """
    Build prompt for one DAG node and run coder_agent.
    Returns result dict.
    """
    msg = f"INFO NODE: Executing {node.get('id')} type={node.get('analysis_type')}"
    print(msg)
    logging.info(msg)
    analysis_id   = node["id"]
    analysis_type = node.get("analysis_type") or "unknown"
    column_roles  = node.get("column_roles", {})
    library_fn    = node.get("library_function")
    description   = node.get("description", "")

    # Derive the behavioral set from LIBRARY_REGISTRY so it stays in sync
    # automatically as new analyses are added. Any entry with col_role == "behavioral"
    # needs the session-enriched CSV produced by run_session_detection.
    behavioral = {
        atype
        for atype, entry in LIBRARY_REGISTRY.items()
        if entry.get("col_role") == "behavioral"
    }
    # Hard-code the original set as a fallback in case LIBRARY_REGISTRY is empty
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

    code = None
    if library_fn and any(
        entry.get("function") == library_fn
        for entry in LIBRARY_REGISTRY.values()
    ):
        for lib_type, entry in LIBRARY_REGISTRY.items():
            if entry["function"] == library_fn:
                code = _build_library_call_code(lib_type, column_roles)
                break

    if not code:
        prompt = (
            f"Session ID: {session_id}\n"
            f"Analysis ID: {analysis_id}\n"
            f"Analysis Type: {analysis_type}\n"
            f"CSV Path: {effective_csv}\n"
            f"Column Roles: {json.dumps(column_roles, default=str)}\n"
            f"Description: {description}\n"
            + (f"Library Function: {library_fn}\n" if library_fn else "") +
            (f"PREVIOUS ERROR: {last_error}\n" if last_error else "") +
            "\nWrite a Python function `analyze(csv_path: str) -> dict` that performs this analysis. "
            "Return ONLY the code block. Use the rules in your system instructions."
        )

        response = await run_agent_pipeline(
            f"{session_id}_{analysis_id}",
            prompt,
            agent_getter="coder",
        )

        import re
        code_match = re.search(r"```python\s*(.*?)\s*```", response, re.DOTALL)
        code = code_match.group(1) if code_match else response.strip()

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

        result = execute_analysis(
            code=code,
            csv_path=effective_csv,
            analysis_id=analysis_id,
            analysis_type=analysis_type,
            output_folder=output_folder,
        )

        qual = validate_output_quality(result, analysis_type)
        if not qual["quality_pass"]:
            print(f"WARNING: Quality check failed for {analysis_id}: {qual['issues']}")

        submit_result(session_id, analysis_id, analysis_type, result)
        state.store_result(analysis_id, result)
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
    """
    Public wrapper — execute one analysis node independently.
    Used by the /add-metric and /retry endpoints in main.py.
    """
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


def _build_library_call_code(analysis_type: str, column_roles: dict) -> str:
    """Build deterministic Python code to call a library function."""
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
            
    arg_str = ", ".join(args)
    
    code = (
        "import pandas as pd\n"
        "import numpy as np\n"
        f"from tools.analysis_library import {fn}\n\n"
        "def analyze(csv_path: str) -> dict:\n"
        f"    return {fn}({arg_str})\n"
    )
    return code


def _attempt_column_role_correction(node: dict, error_str: str) -> dict:
    """
    Stage 4: Self-Correction Engine.

    Detects column_roles argument mismatches from error messages and
    auto-corrects the node's column_roles using the LIBRARY_REGISTRY schema.

    Patterns handled:
    - TypeError: missing required argument 'col' / 'entity_col' / etc.
    - TypeError: got an unexpected keyword argument 'target_col'
    - KeyError: 'column_name' (column not in DataFrame)

    Returns a corrected node dict (copy) if a fix was found,
    or the original node object if no correction could be determined.
    """
    import copy, re

    analysis_type = node.get("analysis_type", "")
    column_roles = node.get("column_roles", {})
    entry = LIBRARY_REGISTRY.get(analysis_type, {})
    required_args = [a for a in entry.get("required_args", []) if a != "csv_path"]

    if not required_args or not error_str:
        return node

    corrected_roles = dict(column_roles)
    fix_applied = False

    # Pattern 1: Missing required argument (TypeError: missing required argument 'X')
    missing_match = re.search(
        r"missing\s+(?:required\s+)?(?:keyword\s+)?argument[:\s'\"]+([a-z_]+)", error_str, re.IGNORECASE
    )
    if missing_match:
        missing_arg = missing_match.group(1)
        if missing_arg in required_args and missing_arg not in corrected_roles:
            # Try to infer the value from existing column_roles
            # Check if there's a value that matches a similar semantic role
            existing_values = list(corrected_roles.values())
            if existing_values:
                corrected_roles[missing_arg] = existing_values[0]
                fix_applied = True
                print(f"[Self-Correction] Fixed missing arg '{missing_arg}' → '{existing_values[0]}'")

    # Pattern 2: Unexpected keyword argument (wrong role key name)
    unexpected_match = re.search(
        r"unexpected\s+keyword\s+argument[:\s'\"]+([a-z_]+)", error_str, re.IGNORECASE
    )
    if unexpected_match:
        bad_key = unexpected_match.group(1)
        if bad_key in corrected_roles:
            value = corrected_roles.pop(bad_key)
            # Assign to the first missing required_arg
            for req_arg in required_args:
                if req_arg not in corrected_roles:
                    corrected_roles[req_arg] = value
                    fix_applied = True
                    print(f"[Self-Correction] Remapped '{bad_key}' → '{req_arg}' = '{value}'")
                    break

    # Pattern 3: Ensure all required args are present (fill from existing values)
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


def _update_session_status(state, status: str):
    """Update session state status."""
    try:
        state.status = status
    except Exception:
        pass


def _post_message(
    state, sender, recipient,
    intent, session_id, payload
):
    """Post an A2A message to session state."""
    try:
        msg = create_message(
            sender=sender,
            recipient=recipient,
            intent=intent,
            payload=payload,
            session_id=session_id,
        )
        state.post_message(msg)
    except Exception:
        pass


def _build_profile_summary(
    profiler_data: dict,
    csv_path: str,
) -> str:
    """Build a text summary of profiler output."""
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
    """Build a lightweight summary of all results."""
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
    """
    Get current pipeline progress for a session.
    Called by main.py /status endpoint.
    """
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
        from agents.synthesis   import get_synthesis_agent
        from agents.dag_builder import get_dag_builder_agent

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
                get_synthesis_agent(),
                get_dag_builder_agent(),
            ],
        )
    return _root_agent_instance
