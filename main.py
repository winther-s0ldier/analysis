import os
import sys
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

api_key = os.getenv("GEMINI_API_KEY")
if api_key:
<<<<<<< HEAD
    # Strip quotes if present
    os.environ["OPENAI_API_KEY"] = api_key.strip('"\'')
    print(f"INFO: OpenAI API Key loaded: {api_key[:4]}...")
=======
    print(f"INFO: Gemini API Key loaded: {api_key[:4]}...")
>>>>>>> 579f7d57b9da95b1bfd3c4b1022131f2d3830b99
else:
    print("WARNING: Gemini API Key NOT found in environment!")

gemini_key = os.getenv("GOOGLE_API_KEY")
if gemini_key:
    os.environ["GOOGLE_API_KEY"] = gemini_key.strip('"\'')
    print(f"INFO: Google API Key loaded: {gemini_key[:4]}...")
else:
    print("WARNING: Google API Key NOT found in environment!")

import uuid
import json
import re
import asyncio
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, BackgroundTasks, Body
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

sys.path.insert(0, os.path.dirname(__file__))
from agents.orchestrator import (
    get_root_agent,
    run_full_pipeline,
    get_pipeline_status,
)
from agents.discovery import get_discovery_agent, get_analysis_plan
from agents.profiler import get_profiler_agent, get_profile_result
from agents.coder import get_coder_agent
from agents.synthesis import get_synthesis_agent
from agents.dag_builder import get_dag_builder_agent
from tools.csv_profiler import profile_csv
from tools.ingestion_normalizer import (
    normalize_file,
    is_supported,
    get_supported_extensions,
)


app = FastAPI(title="Agentic Analytics", description="Multi-Agent CSV Analytics System")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "output"
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

session_service = InMemorySessionService()

class SessionState:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.csv_path: str = ""
        self.csv_filename: str = ""
        self.output_folder: str = ""
        self.status: str = "uploaded"
        
        self.raw_profile: dict = {}
        self.semantic_map: dict = {}
        self.dataset_type: str = ""
        self.dag: list = []
        self.approved_metrics: list = []
        self.discovery: dict = {}
        
        self.results: dict = {}
        self.failed_nodes: set = set()   # node IDs where status=error
        self.precomputed: dict = {}
        self.synthesis: dict = {}
        self.artifacts: list = []

        self.message_log: list = []
        self.normalization: dict = {}

        self.user_instructions: str = ""
    
    def post_message(self, message) -> None:
        self.message_log.append(message.to_dict())
    
    def get_messages_for(self, recipient: str, 
                          unread_only: bool = False) -> list:
        msgs = [m for m in self.message_log 
                if m.get("recipient") == recipient]
        return msgs
    
    def store_result(self, analysis_id: str, result: dict) -> None:
        self.results[analysis_id] = result
        if isinstance(result, dict) and result.get("status") == "error":
            self.failed_nodes.add(analysis_id)
    
    def get_result(self, analysis_id: str) -> dict | None:
        return self.results.get(analysis_id)
    
    def store_precomputed(self, analysis_type: str, 
                           result: dict) -> None:
        self.precomputed[analysis_type] = result
    
    def get_precomputed(self, analysis_type: str) -> dict | None:
        return self.precomputed.get(analysis_type)
    
    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "csv_path": self.csv_path,
            "csv_filename": self.csv_filename,
            "output_folder": self.output_folder,
            "status": self.status,
            "dataset_type": self.dataset_type,
            "dag": self.dag,
            "approved_metrics": self.approved_metrics,
            "results": self.results,
            "synthesis": self.synthesis,
            "artifacts": self.artifacts,
            "message_count": len(self.message_log)
        }

sessions: Dict[str, SessionState] = {}


async def run_agent_pipeline(
    pipeline_id: str,
    prompt: str,
    agent_getter: str = "root",
    max_turns: int = 15,
    image_paths: List[str] = None,
) -> str:
    """Run an agent with a user message and return the response.

    Args:
        pipeline_id: Session/pipeline identifier.
        prompt: User message to send to the agent.
        agent_getter: Which agent to use.
        max_turns: Maximum number of LLM round-trips before
            giving up. Prevents small models from looping forever.
        image_paths: Optional list of local file paths to images.
    """
    APP_NAME = "Analytics_analytics"
    USER_ID = "user_1"

    agent_map = {
        "root":        get_root_agent,
        "profiler":    get_profiler_agent,
        "discovery":   get_discovery_agent,
        "coder":       get_coder_agent,
        "synthesis":   get_synthesis_agent,
        "dag_builder": get_dag_builder_agent,
    }
    getter = agent_map.get(
        agent_getter, get_root_agent
    )
    target_agent = getter()

    session = await session_service.get_session(
        app_name=APP_NAME, user_id=USER_ID, session_id=pipeline_id
    )
    if session is None:
        session = await session_service.create_session(
            app_name=APP_NAME, user_id=USER_ID, session_id=pipeline_id
        )

    runner = Runner(
        agent=target_agent,
        app_name=APP_NAME,
        session_service=session_service,
    )

    parts = [types.Part.from_text(text=prompt)]
    if image_paths:
        for img_path in image_paths:
            if os.path.exists(img_path):
                try:
                    with open(img_path, "rb") as f:
                        img_bytes = f.read()
                    
                    mime_type = "image/png"
                    if img_path.lower().endswith(".html"):
                        continue
                    elif img_path.lower().endswith(".jpg") or img_path.lower().endswith(".jpeg"):
                        mime_type = "image/jpeg"
                        
                    parts.append(
                        types.Part.from_bytes(data=img_bytes, mime_type=mime_type)
                    )
                except Exception as e:
                    print(f"WARNING: Could not load image {img_path}: {e}")

    content = types.Content(
        role="user",
        parts=parts
    )

    max_retries = 3

    for attempt in range(max_retries):
        try:
            final_response = ""
            turn_count = 0

            async for event in runner.run_async(
                user_id=USER_ID,
                session_id=pipeline_id,
                new_message=content,
            ):
                turn_count += 1
                if event.is_final_response():
                    if event.content and event.content.parts:
                        final_response = event.content.parts[0].text

                if turn_count >= max_turns:
                    print(
                        f"WARNING: Agent '{agent_getter}' hit "
                        f"max_turns={max_turns}. Stopping."
                    )
                    break

            return final_response

        except Exception as e:
            error_str = str(e).lower()
            if "429" in error_str or "rate" in error_str or "exhausted" in error_str or "quota" in error_str:
                if attempt < max_retries - 1:
                    match = re.search(r"'retryDelay': '(\d+)s'", str(e))
                    delay = int(match.group(1)) + 1 if match else 20
                    print(f"Rate limit hit. Waiting {delay}s... (Attempt {attempt+1}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                else:
                    print(f"Rate limit exhausted after {max_retries} attempts")
                    return f"Error: API rate limit exceeded after {max_retries} retries. Please wait a minute and try again."
            raise

    return final_response


def extract_json(response: str) -> dict:
    """Robustly extract JSON from LLM text that may contain markdown wrappers."""
    try:
        match = re.search(r'```(?:json)?\s*(.*?)\s*```', response, re.DOTALL)
        if match:
            return json.loads(match.group(1))
        
        first_brace = response.find('{')
        first_bracket = response.find('[')
        
        start_idx = -1
        if first_brace != -1 and (first_bracket == -1 or first_brace < first_bracket):
            start_idx = first_brace
        elif first_bracket != -1:
            start_idx = first_bracket
        
        if start_idx != -1:
            last_brace = response.rfind('}')
            last_bracket = response.rfind(']')
            end_idx = max(last_brace, last_bracket)
            
            if end_idx != -1:
                return json.loads(response[start_idx:end_idx + 1])
        
        return json.loads(response.strip())
    except Exception as e:
        return {"error": str(e), "raw": response[:500]}


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the main UI."""
    html_path = BASE_DIR / "static" / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    """Upload a file and start the analysis pipeline."""
    if not is_supported(file.filename):
        raise HTTPException(
            status_code=415,
            detail=(
                f"Unsupported file type. "
                f"Supported formats: "
                f"{', '.join(get_supported_extensions())}"
            ),
        )

    session_id = str(uuid.uuid4())[:12]
    
    file_basename = file.filename.rsplit(".", 1)[0]
    safe_folder = file_basename.replace(" ", "_").replace("-", "_").strip()[:80]
    output_folder = safe_folder
    if (OUTPUT_DIR / output_folder).exists():
        output_folder = f"{safe_folder}_{session_id[:6]}"
    
    ext = Path(file.filename).suffix.lower()
    saved_file_path = UPLOAD_DIR / f"{output_folder}{ext}"

    content = await file.read()
    with open(saved_file_path, "wb") as f:
        f.write(content)

    norm_result = normalize_file(str(saved_file_path.resolve()))

    if norm_result["status"] == "unsupported":
        raise HTTPException(
            status_code=415,
            detail=(
                f"Unsupported file type. "
                f"Supported formats: "
                f"{', '.join(get_supported_extensions())}"
            ),
        )

    if norm_result["status"] == "error":
        raise HTTPException(
            status_code=422,
            detail=norm_result["error"],
        )

    csv_path = norm_result["csv_path"]

    # --- Pre-Flight Data Quality Gate ---
    from tools.data_gate import run_preflight_check
    dataset_type = norm_result["original_filename"].split(".")[0]
    gate_result = run_preflight_check(csv_path, dataset_type)

    if gate_result["gate_result"] == "block":
        raise HTTPException(
            status_code=422,
            detail=f"Data Quality Gate BLOCKED the file:\n" + "\n".join(gate_result["errors"])
        )

    (OUTPUT_DIR / output_folder).mkdir(exist_ok=True)

    state = SessionState(session_id)
    state.csv_path = csv_path
    state.csv_filename = norm_result["original_filename"]
    state.output_folder = output_folder
    state.normalization = {
        "original_filename": norm_result["original_filename"],
        "original_format":   norm_result["original_format"],
        "warnings":          norm_result["warnings"] + gate_result["warnings"],
        "row_count":         gate_result["row_count"],
        "col_count":         gate_result["col_count"],
    }
    state.gate_result = gate_result
    sessions[session_id] = state

    return {
        "session_id":   session_id,
        "filename":     norm_result["original_filename"],
        "format":       norm_result["original_format"],
        "rows":         gate_result["row_count"],
        "columns":      gate_result["col_count"],
        "Gate":         gate_result["gate_result"],
        "warnings":     state.normalization["warnings"],
        "status":       "uploaded",
    }


@app.post("/profile/{session_id}")
async def profile_dataset(session_id: str):
    """Run the profiler agent to analyze CSV structure. Returns profile + classification."""
    if session_id not in sessions:
        raise HTTPException(404, "Session not found")

    state = sessions[session_id]
    state.status = "profiling"

    profiler_response = await run_agent_pipeline(
        f"{session_id}_profile",
        f"csv_path: {state.csv_path}\nsession_id: {session_id}\nCall tool_profile_and_classify now.",
        agent_getter="profiler",
    )

    profiler_data = get_profile_result(session_id)
    if not profiler_data:
        profiler_data = extract_json(profiler_response)

    if profiler_data.get("status") == "error":
        raise HTTPException(500, profiler_data.get("error"))

    state.raw_profile = profiler_data.get("raw_profile", {})
    state.semantic_map = profiler_data.get("classification", {})
    state.dataset_type = state.semantic_map.get("dataset_type", "")
    state.status = "profiled"

    from a2a_messages import create_message, Intent
    msg = create_message(
        sender="profiler_agent",
        recipient="discovery_agent",
        intent=Intent.PROFILE_COMPLETE,
        payload={
            "dataset_type": state.dataset_type,
            "column_roles": state.semantic_map.get("column_roles", {}),
            "ready": True,
        },
        session_id=session_id,
    )
    state.post_message(msg)

    raw = state.raw_profile
    return {
        "session_id": session_id,
        "status": "profiled",
        "profile": {
            "filename": raw.get("filename"),
            "row_count": raw.get("row_count"),
            "column_count": raw.get("column_count"),
            "columns": raw.get("columns"),
            "column_types": raw.get("column_types", {}),
            "correlations": raw.get("correlations"),
        },
        "classification": profiler_data.get("classification"),
    }


def build_fallback_discovery(state: SessionState, session_id: str) -> dict:
    """
    Deterministic fallback: build the analysis DAG directly from
    the profiler's recommended_analyses, bypassing the LLM entirely.
    Works with ANY model (or no model at all).
    """
    from agents.discovery import (
        build_dag_deterministic,
        tool_submit_analysis_plan,
    )

    classification = state.semantic_map
    column_roles = classification.get("column_roles", {})
    dataset_type = classification.get("dataset_type", "tabular_generic")
    recommended = classification.get("recommended_analyses", [
        "distribution_analysis",
        "categorical_analysis",
        "correlation_matrix",
        "missing_data_analysis",
    ])
    row_count = state.raw_profile.get("row_count", 0)

    print(f"INFO: Using fallback discovery for {session_id}")
    print(f"  dataset_type={dataset_type}, analyses={recommended}")

    dag_result = build_dag_deterministic(
        dataset_type=dataset_type,
        column_roles=column_roles,
        selected_analyses=recommended,
        row_count=row_count,
    )

    plan_json = json.dumps({
        "data_summary": dag_result.get("data_summary", ""),
        "dag": dag_result.get("dag", []),
    })

    plan_json = json.dumps({
        "data_summary": dag_result.get("data_summary", ""),
        "dag": dag_result.get("dag", []),
        "node_count": dag_result.get("node_count", 0),
    })
    tool_submit_analysis_plan(
        session_id=session_id,
        dag_json_str=plan_json,
    )

    stored = get_analysis_plan(session_id)
    return stored or {
        "data_summary": dag_result.get("data_summary", ""),
        "dag": dag_result.get("dag", []),
        "metrics": [],
        "node_count": dag_result.get("node_count", 0),
    }


@app.post("/discover/{session_id}")
async def discover_metrics(session_id: str):
    """Run the discovery agent to build an analysis DAG from the existing profile."""
    if session_id not in sessions:
        raise HTTPException(404, "Session not found")

    state = sessions[session_id]
    if not state.raw_profile:
        raise HTTPException(400, "Profile not yet run. Call /profile first.")

    state.status = "discovering"

    profile_summary = json.dumps({
        "filename": state.raw_profile.get("filename"),
        "row_count": state.raw_profile.get("row_count"),
        "column_count": state.raw_profile.get("column_count"),
        "columns": state.raw_profile.get("columns"),
        "classification": state.semantic_map,
        "sample_rows": state.raw_profile.get("sample_rows", [])[:3],
        "correlations": state.raw_profile.get("correlations"),
    }, default=str)

    user_inst_block = ""
    if state.user_instructions:
        user_inst_block = (
            f"\n\nUSER INSTRUCTIONS:\n{state.user_instructions}\n"
            f"Consider these when choosing analyses. Prioritize what the user asked for.\n"
        )

    prompt = (
        f"Session ID: {session_id}\n"
        f"CSV file path: {state.csv_path}\n"
        f"Output folder: output/{state.output_folder}\n\n"
        f"PROFILER OUTPUT:\n{profile_summary}\n\n"
        f"{user_inst_block}"
        f"INSTRUCTIONS:\n"
        f"1. Reason about the data and the user's request.\n"
        f"2. Construct a JSON DAG of MetricSpec nodes.\n"
        f"3. Call tool_submit_analysis_plan(session_id, dag_json_str) with your JSON result.\n"
    )

    try:
        response = await run_agent_pipeline(
            session_id, prompt,
            agent_getter="discovery",
            max_turns=12,
        )
    except Exception as e:
        print(f"Discovery agent error: {e}")
        response = ""

    stored = get_analysis_plan(session_id)
    if stored:
        discovery_data = stored
    else:
        print(
            f"WARNING: Discovery agent did not submit a plan "
            f"for {session_id}. Using fallback."
        )
        discovery_data = build_fallback_discovery(state, session_id)

    state.status = "discovered"
    state.discovery = discovery_data
    state.dag = discovery_data.get("dag", [])

    return {
        "session_id": session_id,
        "status": "discovered",
        "discovery": discovery_data,
    }


@app.post("/validate-metric/{session_id}")
async def validate_metric(session_id: str, request: Request):
    """
    Validate a custom metric request against the data.
    The Discovery Agent reasons about whether the data supports this analysis.
    """
    if session_id not in sessions:
        raise HTTPException(404, "Session not found")

    body = await request.json()
    custom_metric = body.get("metric", "")
    if not custom_metric:
        raise HTTPException(400, "No metric description provided")

    session_info = sessions[session_id]
    csv_path = session_info.csv_path
    profile = session_info.raw_profile

    prompt = (
        f"CSV file path: {csv_path}\n"
        f"Available columns: {json.dumps([c['name'] + ' (' + c['type_category'] + ', ' + str(c['unique_count']) + ' unique)' for c in profile.get('columns', [])])}\n\n"
        f"The user wants to add a custom metric: \"{custom_metric}\"\n\n"
        f"Can this analysis be performed with the available data?\n"
        f"Respond with ONLY a JSON object: "
        f'{{\"valid\": true/false, \"reason\": \"...\", \"metric_name\": \"...\", \"description\": \"...\", \"analysis_type\": \"...\"}}'
    )

    response = await run_agent_pipeline(
        f"{session_id}_validate", prompt, agent_getter="discovery"
    )

    result = extract_json(response)
    return {"session_id": session_id, "validation": result}


class AnalyzeRequest(BaseModel):
    request: Optional[str] = "Analyze all metrics"
    custom_metrics: Optional[List[str]] = []
    approved_metrics: Optional[List[str]] = None
    user_instructions: Optional[str] = ""


async def run_pipeline_background(
    session_id, csv_path, output_folder,
    approved, state
):
    try:
        print(f"INFO: Pipeline starting for {session_id}")
        result = await run_full_pipeline(
            session_id=session_id,
            csv_path=csv_path,
            output_folder=output_folder,
            approved_metrics=approved,
            state=state,
        )
        print(f"INFO: Result: {result.get('status')}")
    except Exception as e:
        import traceback
        print(f"ERROR: {str(e)}")
        traceback.print_exc()
        state.status = "error"


def run_pipeline_sync(
    session_id, csv_path, output_folder,
    approved, state
):
    import asyncio
    asyncio.run(run_pipeline_background(
        session_id, csv_path, output_folder,
        approved, state
    ))

@app.post("/analyze/{session_id}")
async def analyze(
    session_id: str,
    background_tasks: BackgroundTasks,
    req_body: AnalyzeRequest = Body(default=AnalyzeRequest()),
):
    print(f"INFO: /analyze request received for {session_id}")
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )
    
    metrics = req_body.approved_metrics or req_body.custom_metrics
    output_folder_path = f"output/{state.output_folder}"

    if req_body.user_instructions:
        state.user_instructions = req_body.user_instructions

    print(f"Starting pipeline for {session_id}")
    print(f"csv_path: {state.csv_path}")
    print(f"output_folder: {output_folder_path}")
    if state.user_instructions:
        print(f"user_instructions: {state.user_instructions}")

    background_tasks.add_task(
        run_pipeline_sync,
        session_id=session_id,
        csv_path=state.csv_path,
        output_folder=output_folder_path,
        approved=metrics,
        state=state,
    )
    
    return {"status": "started", "session_id": session_id}

@app.get("/status/{session_id}")
async def get_status(session_id: str):
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )
    # Build inline — calling get_pipeline_status() without await returns a coroutine, not data
    node_statuses = {}
    failed = getattr(state, "failed_nodes", set())
    if hasattr(state, "dag") and state.dag:
        for node in state.dag:
            nid = node.get("id", "")
            if nid in failed:
                node_statuses[nid] = "failed"
            elif nid in state.results:
                node_statuses[nid] = "complete"
            else:
                node_statuses[nid] = "pending"

    return {
        "session_id":     session_id,
        "session_status": state.status,
        "pipeline":       {"node_statuses": node_statuses},
        "result_count":   len(state.results),
        "has_synthesis":  bool(state.synthesis),
        "has_report": any(
            a.get("type") == "report"
            for a in state.artifacts
        ),
    }

@app.post("/chat/{session_id}")
async def chat(session_id: str, request: Request):
    """
    Two-mode chat:
    - Pre-pipeline (status in uploaded/discovered): stores message as
      user_instructions for the next pipeline run.
    - Post-pipeline (status == complete): enriches prompt with session
      results and synthesis for context-aware answers.
    """
    if session_id not in sessions:
        raise HTTPException(404, "Session not found")

    body = await request.json()
    message = body.get("message", "")
    if not message:
        return {"session_id": session_id, "response": "Please provide a message."}

    session_info = sessions[session_id]
    csv_path = session_info.csv_path
    output_folder = session_info.output_folder

    if session_info.status in ("uploaded", "profiled", "discovered", "profiling", "discovering"):
        if session_info.user_instructions:
            session_info.user_instructions += f"\n{message}"
        else:
            session_info.user_instructions = message

        return {
            "session_id": session_id,
            "response": (
                f"Got it — I'll factor \"{message}\" into "
                f"the analysis when the pipeline runs. "
                f"You can add more instructions or click "
                f"'Execute Analysis Pipeline' when ready."
            ),
        }

    context_parts = [
        f"CSV file: {csv_path}",
        f"Output folder: output/{output_folder}",
        f"Dataset type: {session_info.dataset_type}",
    ]

    col_roles = session_info.semantic_map.get("column_roles", {})
    if col_roles:
        context_parts.append(f"Column roles: {json.dumps(col_roles)}")

    if session_info.results:
        findings = []
        for aid, result in session_info.results.items():
            atype = result.get("analysis_type", aid)
            finding = result.get("top_finding", "")
            if finding:
                findings.append(f"- {atype}: {finding[:200]}")
        if findings:
            context_parts.append(
                "COMPLETED ANALYSES:\n" + "\n".join(findings)
            )

    if session_info.synthesis:
        synth = session_info.synthesis
        overview = ""
        if isinstance(synth, dict):
            exec_sum = synth.get("executive_summary", {})
            if isinstance(exec_sum, dict):
                overview = exec_sum.get("overview", "")
        if overview:
            context_parts.append(
                f"SYNTHESIS OVERVIEW: {overview[:500]}"
            )

    context = "\n".join(context_parts)
    prompt = f"{context}\n\nUser question: {message}"
    response = await run_agent_pipeline(session_id, prompt)

    return {"session_id": session_id, "response": response}


@app.post("/add-metric/{session_id}")
async def add_metric(session_id: str, request: Request):
    """
    Add and execute a single custom metric after discovery.
    1. Validates the metric against available data
    2. Creates a custom DAG node
    3. Executes it via coder agent
    4. Returns the result (chart + finding)
    """
    if session_id not in sessions:
        raise HTTPException(404, "Session not found")

    body = await request.json()
    metric_text = body.get("metric", "")
    if not metric_text:
        raise HTTPException(400, "No metric description provided")

    state = sessions[session_id]

    validation = {
        "valid": True,
        "analysis_type": "custom",
        "metric_name": metric_text[:30],
        "description": metric_text
    }
    analysis_type = validation["analysis_type"]
    analysis_id = f"C{len(state.results) + 1}"
    output_folder = f"output/{state.output_folder}"

    column_roles = state.semantic_map.get("column_roles", {})

    try:
        from agents.orchestrator import execute_single_analysis
        result = await execute_single_analysis(
            session_id=session_id,
            analysis_type=analysis_type,
            analysis_id=analysis_id,
            csv_path=state.csv_path,
            output_folder=output_folder,
            description=metric_text,
            column_roles=column_roles,
            state=state,
        )

        if result.get("status") == "success":
            return {
                "session_id": session_id,
                "status": "success",
                "analysis_id": analysis_id,
                "analysis_type": result.get("analysis_type", analysis_type),
                "top_finding": result.get("top_finding", ""),
                "chart_path": result.get("chart_file_path"),
                "severity": result.get("severity", "info"),
            }
        else:
            return {
                "session_id": session_id,
                "status": "error",
                "error": result.get("error", "Analysis failed"),
            }
    except Exception as e:
        return {
            "session_id": session_id,
            "status": "error",
            "error": str(e),
        }


@app.get("/results/{session_id}")
async def get_results(session_id: str):
    """Return all completed analysis results."""
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(status_code=404, detail="Session not found")
    results = []
    for aid, result in state.results.items():
        results.append({
            "analysis_id":   aid,
            "analysis_type": result.get("analysis_type"),
            "top_finding":   result.get("top_finding",""),
            "severity":      result.get("severity","info"),
            "chart_path":    result.get("chart_file_path"),
        })
    return results


@app.get("/chart/{session_id}/{analysis_id}")
async def get_chart(
    session_id: str,
    analysis_id: str
):
    """Serve a chart HTML file."""
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(status_code=404, detail="Session not found")
    result = state.get_result(analysis_id)
    if not result or not result.get("chart_file_path"):
        raise HTTPException(status_code=404, detail="Chart not found")
    
    chart_path = Path(result["chart_file_path"])
    if not chart_path.exists():
        raise HTTPException(status_code=404, detail="Chart file deleted")
        
    return FileResponse(chart_path, media_type="text/html")


@app.get("/synthesis/{session_id}")
async def get_synthesis(session_id: str):
    """Return synthesis results."""
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(status_code=404, detail="Session not found")
    return state.synthesis or {}


@app.get("/report/{session_id}")
async def get_report(session_id: str):
    """Serve the final HTML report."""
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(status_code=404, detail="Session not found")
    
    report_artifact = next(
        (a for a in state.artifacts if a.get("type") == "report" or a.get("filename") == "report.html"),
        None
    )
    
    if not report_artifact:
        report_path = OUTPUT_DIR / state.output_folder / "report.html"
        if not report_path.exists():
            raise HTTPException(status_code=404, detail="Report not yet generated")
    else:
        if "path" in report_artifact:
            report_path = Path(report_artifact["path"])
        else:
            report_path = OUTPUT_DIR / state.output_folder / "report.html"

    return FileResponse(report_path, media_type="text/html")


@app.get("/output/{folder_name}/{filename}")
async def serve_artifact(folder_name: str, filename: str):
    """Serve a generated artifact file (HTML charts, PNGs, reports)."""
    filepath = OUTPUT_DIR / folder_name / filename
    if not filepath.exists():
        raise HTTPException(404, "File not found")
    
    if filename.endswith(".html"):
        return FileResponse(filepath, media_type="text/html")
    return FileResponse(filepath)


@app.get("/sessions")
async def list_sessions():
    """List all active sessions."""
    return {
        "sessions": [
            {"id": sid, "filename": info.csv_filename, "status": info.status}
            for sid, info in sessions.items()
        ]
    }


@app.get("/api/session/{session_id}/status")
async def get_pipeline_status(session_id: str):
    """
    Returns real-time pipeline status for the frontend grid panel.
    Includes node pass/fail states, gate results, and monitor alerts.
    """
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(status_code=404, detail="Session not found")
        
    from tools.monitor import get_session_events
    raw_events = get_session_events(session_id, min_severity="warning")
    
    events = []
    for e in raw_events:
        events.append({
            "event": e.get("type", "unknown"),
            "data": e.get("payload", {})
        })
    
    node_status_list = []
    if hasattr(state, "dag") and state.dag:
        for node in state.dag:
            nid = node["id"]
            if nid in state.results:
                status = "success"
                error = ""
            elif hasattr(state, "failed_nodes") and nid in state.failed_nodes:
                status = "failed"
                error = "Execution failed"
            else:
                status = "pending"
                error = ""
                
            node_status_list.append({
                "id": nid,
                "type": node.get("analysis_type", "unknown"),
                "status": status,
                "error": error
            })
            
    import json as _json

    raw_response = {
        "session_id": session_id,
        "pipeline_status": state.status,
        "gate_result": getattr(state, "gate_result", {}),
        "alerts": events,
        "nodes": node_status_list,
    }

    # Sanitize the entire response in one pass — catches numpy.float64, datetime,
    # coroutine objects, or any other non-JSON-serializable type via default=str.
    try:
        return _json.loads(_json.dumps(raw_response, default=str))
    except Exception as _e:
        # Absolute fallback — return minimal safe response
        return {
            "session_id": session_id,
            "pipeline_status": state.status,
            "gate_result": {},
            "alerts": [],
            "nodes": [],
        }



def _get_file_type(filename: str) -> str:
    ext = filename.rsplit(".", 1)[-1].lower()
    type_map = {
        "png": "chart", "jpg": "chart", "svg": "chart",
        "html": "report", "json": "data", "py": "code"
    }
    return type_map.get(ext, "other")


app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


if __name__ == "__main__":
    import uvicorn
    print("Analytics Server starting...")
    uvicorn.run(app, host="127.0.0.1", port=8000)
