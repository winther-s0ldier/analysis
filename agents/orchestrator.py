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
    os.makedirs(output_folder, exist_ok=True)

    try:
        from main import run_agent_pipeline, extract_json
        print(f"INFO: run_full_pipeline session found: {session_id}")
        print(f"INFO: session status before: {state.status}")

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

        discovery_prompt = (
            f"Session ID: {session_id}\n"
            f"CSV file path: {csv_path}\n"
            f"Output folder: {output_folder}\n\n"
            f"PROFILER OUTPUT:\n{profile_summary}\n\n"
            f"INSTRUCTIONS:\n"
            f"1. Reason about the data and the user's request.\n"
            f"2. Construct a JSON DAG of MetricSpec nodes (id, name, analysis_type, library_function, column_roles, depends_on).\n"
            f"3. Call tool_submit_analysis_plan(session_id, dag_json_str) with your JSON result.\n"
        )

        discovery_response = await run_agent_pipeline(
            f"{session_id}_discovery",
            discovery_prompt,
            agent_getter="discovery",
        )

        from agents.discovery import get_analysis_plan
        plan = get_analysis_plan(session_id)

        if not plan or not plan.get("dag"):
            return {
                "status": "error",
                "error":  "Discovery produced no DAG",
            }

        dag  = plan["dag"]
        state.dag = dag

        if approved_metrics:
            dag = [
                n for n in dag
                if n["id"] in approved_metrics
            ]

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
        _rich_summary = json.dumps(_rich_results, indent=2) if _rich_results else "[]"

        synthesis_prompt = (
            f"Session ID: {session_id}\n"
            f"Dataset type: {state.dataset_type}\n"
            f"Total analyses completed: {len(state.results)}\n\n"
            f"== FULL ANALYSIS RESULTS (with raw data) ==\n"
            f"{_rich_summary}\n\n"
            f"INSTRUCTIONS:\n"
            f"1. Call tool_aggregate_results(session_id) to also get column_roles context.\n"
            f"2. Reason across ALL findings above — cite analysis_id and specific numbers in every insight.\n"
            f"3. Build the required JSON schema and call tool_submit_synthesis(session_id, synthesis_json_str)."
        )

        try:
            await run_agent_pipeline(
                f"{session_id}_synthesis",
                synthesis_prompt,
                agent_getter="synthesis",
            )
        except Exception as e:
            msg = f"ERROR: Synthesis agent failed for session {session_id}: {e}"
            print(msg)
            logging.error(msg)


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
            "trace":  traceback.format_exc()[-1000:],
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

        retry = await _execute_single_node(
            session_id=session_id,
            node=node,
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

    behavioral = {
        "funnel_analysis", "friction_detection",
        "survival_analysis", "user_segmentation",
        "sequential_pattern_mining", "association_rules",
    }
    effective_csv = csv_path
    if analysis_type in behavioral:
        enriched = csv_path.replace(
            ".csv", "_sessions.csv"
        )
        if os.path.exists(enriched):
            effective_csv = enriched

    code = None
    if library_fn and library_fn in str(LIBRARY_REGISTRY):
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
            f"Column Roles: {json.dumps(column_roles)}\n"
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
        f"column_roles: {json.dumps(column_roles)}\n"
        f"recommended_analyses: "
        f"{json.dumps(recommended)}\n"
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
            model="openai/gpt-4o",
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
