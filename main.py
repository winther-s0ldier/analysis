import os
import sys
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

_raw_key = os.getenv("GEMINI_API_KEY")
if _raw_key:
    _raw_key = _raw_key.strip('"\'')
    os.environ["GEMINI_API_KEY"] = _raw_key
    os.environ["GOOGLE_API_KEY"] = _raw_key
    print("INFO: Gemini API Key loaded.")
else:
    print("WARNING: Gemini API Key NOT found in environment!")

import time
import uuid
import json
import re
import asyncio
import traceback
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, BackgroundTasks, Body
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from tools.redis_client import get_redis, redis_available
from tools.config_loader import get_config

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
from agents.chat_agent import get_chat_agent
from agents.critic import get_critic_agent
from tools.csv_profiler import profile_csv
from tools.ingestion_normalizer import (
    normalize_file,
    is_supported,
    get_supported_extensions,
)

try:
    import kaleido as _kaleido
    print("INFO: kaleido OK — static chart export available.")
except ImportError:
    print(
        "WARNING: kaleido not installed. Plotly PNG chart export will fail. "
        "Install with: pip install kaleido"
    )

_agent_server_procs: list = []

@asynccontextmanager
async def lifespan(app: FastAPI):

    _restore_sessions_from_redis()

    if os.getenv("USE_A2A_MULTISERVER", "true").lower() != "false":
        import subprocess
        _server_script = str(Path(__file__).parent / "agent_servers" / "server_base.py")
        _log_dir = Path(__file__).parent / "logs"
        _log_dir.mkdir(exist_ok=True)
        _agent_ports = get_config()["agents"]["ports"]
        for _agent_name, _port in [
            ("synthesis", _agent_ports["synthesis"]),
            ("critic", _agent_ports["critic"]),
            ("dag_builder", _agent_ports["dag_builder"]),
        ]:
            try:
                _log_out = open(_log_dir / f"{_agent_name}.log", "a", encoding="utf-8")
                proc = subprocess.Popen(
                    [sys.executable, _server_script, "--agent", _agent_name, "--port", str(_port)],
                    stdout=_log_out,
                    stderr=_log_out,
                )
                _agent_server_procs.append(proc)
                print(f"INFO: Started {_agent_name} A2A server (PID {proc.pid}) on port {_port}")
            except Exception as _e:
                print(f"WARNING: Could not start {_agent_name} A2A server: {_e}")

    _eviction_task = asyncio.create_task(_cleanup_stale_sessions())

    yield

    _eviction_task.cancel()

    for proc in _agent_server_procs:
        try:
            proc.terminate()
        except Exception:
            pass

    worker_count = int(os.environ.get("WEB_CONCURRENCY", "1"))
    if worker_count > 1 and not redis_available():
        print(
            f"ERROR: Multiple workers (WEB_CONCURRENCY={worker_count}) require Redis. "
            f"Either start Redis or set WEB_CONCURRENCY=1."
        )

app = FastAPI(
    title="Agentic Analytics",
    description="Multi-Agent CSV Analytics System",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_A2A_HOST     = os.getenv("A2A_HOST",     "localhost")
_A2A_PORT     = int(os.getenv("A2A_PORT", "8000"))
_A2A_PROTOCOL = os.getenv("A2A_PROTOCOL", "http")

_A2A_AGENTS = [
    ("profiler",    "agents.profiler",    "get_profiler_agent"),
    ("discovery",   "agents.discovery",   "get_discovery_agent"),
    ("coder",       "agents.coder",       "get_coder_agent"),
    ("synthesis",   "agents.synthesis",   "get_synthesis_agent"),
    ("critic",      "agents.critic",      "get_critic_agent"),
    ("dag_builder", "agents.dag_builder", "get_dag_builder_agent"),
]

_mounted_a2a_agents: dict = {}

try:
    import importlib
    from google.adk.a2a.utils.agent_to_a2a import to_a2a as _to_a2a

    _root_a2a = _to_a2a(
        get_root_agent(),
        host=_A2A_HOST, port=_A2A_PORT, protocol=_A2A_PROTOCOL,
    )
    app.mount("/a2a", _root_a2a)
    _mounted_a2a_agents["pipeline"] = "/a2a"
    print("INFO: A2A root   -> /a2a  |  Card -> /a2a/.well-known/agent-card.json")

    for _a2a_name, _a2a_module, _a2a_getter in _A2A_AGENTS:
        try:
            _mod    = importlib.import_module(_a2a_module)
            _agent  = getattr(_mod, _a2a_getter)()
            _path   = f"/agents/{_a2a_name}"
            _sub    = _to_a2a(
                _agent,
                host=_A2A_HOST, port=_A2A_PORT, protocol=_A2A_PROTOCOL,
            )
            app.mount(_path, _sub)
            _mounted_a2a_agents[_a2a_name] = _path
            print(f"INFO: A2A agent  -> {_path}  |  Card -> {_path}/.well-known/agent-card.json")
        except Exception as _agent_err:
            print(f"WARNING: A2A mount failed for {_a2a_name}: {_agent_err}")

except Exception as _a2a_err:
    print(f"WARNING: A2A endpoints unavailable: {_a2a_err}")

@app.get("/agents", tags=["A2A"])
async def list_a2a_agents():
    base = f"{_A2A_PROTOCOL}://{_A2A_HOST}:{_A2A_PORT}"
    return {
        "protocol": "Google A2A",
        "agents": [
            {
                "name":      name,
                "path":      path,
                "card_url":  f"{base}{path}/.well-known/agent-card.json",
                "rpc_url":   f"{base}{path}/",
            }
            for name, path in _mounted_a2a_agents.items()
        ],
    }

BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "output"
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

def _build_session_service():
    _redis_url = os.getenv("REDIS_URL")
    _a2a_active = os.getenv("USE_A2A_MULTISERVER", "true").lower() != "false"
    if _redis_url and _a2a_active:
        try:
            from google.adk.sessions import RedisSessionService
            svc = RedisSessionService(_redis_url)
            print(f"INFO: ADK session service → Redis ({_redis_url})")
            return svc
        except Exception as _e:
            print(f"WARNING: Redis ADK session init failed ({_e}), falling back to InMemory")
    return InMemorySessionService()

session_service = _build_session_service()

class SessionState:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.created_at: float = time.time()
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
        self.failed_nodes: set = set()
        self.precomputed: dict = {}
        self.synthesis: dict = {}
        self.artifacts: list = []

        self.message_log: list = []

        self._mailbox: dict = {}
        self.normalization: dict = {}
        self.gate_result: dict = {}

        self.user_instructions: str = ""
        self.conversation_history: list = []
        self.clarification_request: dict = {}

    def post_message(self, message) -> None:
        d = message.to_dict()
        self.message_log.append(d)

        recipient = d.get("recipient", "")
        if recipient:
            self._mailbox.setdefault(recipient, []).append(d)

    def get_messages_for(self, recipient: str) -> list:

        return list(self._mailbox.get(recipient, []))

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
            "session_id":           self.session_id,
            "created_at":           self.created_at,
            "csv_path":             self.csv_path,
            "csv_filename":         self.csv_filename,
            "output_folder":        self.output_folder,
            "status":               self.status,
            "dataset_type":         self.dataset_type,
            "dag":                  self.dag,
            "approved_metrics":     self.approved_metrics,
            "results":              self.results,
            "failed_nodes":         list(self.failed_nodes),
            "synthesis":            self.synthesis,
            "artifacts":            self.artifacts,
            "raw_profile":          self.raw_profile,
            "semantic_map":         self.semantic_map,
            "discovery":            self.discovery,
            "gate_result":          self.gate_result,
            "user_instructions":    self.user_instructions,
            "conversation_history": self.conversation_history,
            "message_count":        len(self.message_log),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "SessionState":
        state = cls(d["session_id"])
        state.created_at           = d.get("created_at", time.time())
        state.csv_path             = d.get("csv_path", "")
        state.csv_filename         = d.get("csv_filename", "")
        state.output_folder        = d.get("output_folder", "")
        state.status               = d.get("status", "uploaded")
        state.dataset_type         = d.get("dataset_type", "")
        state.dag                  = d.get("dag", [])
        state.approved_metrics     = d.get("approved_metrics", [])
        state.results              = d.get("results", {})
        state.failed_nodes         = set(d.get("failed_nodes", []))
        state.synthesis            = d.get("synthesis", {})
        state.artifacts            = d.get("artifacts", [])
        state.raw_profile          = d.get("raw_profile", {})
        state.semantic_map         = d.get("semantic_map", {})
        state.discovery            = d.get("discovery", {})
        state.gate_result          = d.get("gate_result", {})
        state.user_instructions    = d.get("user_instructions", "")
        state.conversation_history = d.get("conversation_history", [])
        return state

sessions: Dict[str, SessionState] = {}

_SESSION_TTL = get_config()["pipeline"]["session_ttl"]
_SESSION_MAX_AGE = get_config()["pipeline"]["session_ttl"]
_SESSION_PREFIX = "adk:session:"

async def _cleanup_stale_sessions() -> None:
    while True:
        await asyncio.sleep(get_config()["pipeline"]["session_restore_interval"])
        now = time.time()
        stale = [
            sid for sid, state in list(sessions.items())
            if (now - getattr(state, "created_at", now)) > _SESSION_MAX_AGE
        ]
        for sid in stale:
            sessions.pop(sid, None)
            _sse_events.pop(sid, None)

            try:
                from agents.synthesis import _synthesis_store, _reasoning_store
                _synthesis_store.pop(sid, None)
                _reasoning_store.pop(sid, None)
            except Exception as _evict_err:
                print(f"WARNING: [Eviction] synthesis store cleanup failed for {sid}: {_evict_err}")
            try:
                from agents.critic import _critic_store
                _critic_store.pop(sid, None)
            except Exception as _evict_err:
                print(f"WARNING: [Eviction] critic store cleanup failed for {sid}: {_evict_err}")
        if stale:
            print(f"INFO: [Eviction] Removed {len(stale)} stale session(s) from memory")

def _redis_key(session_id: str) -> str:
    return f"{_SESSION_PREFIX}{session_id}"

def _persist_session(state: SessionState) -> None:
    r = get_redis()
    if not r:
        return
    try:
        r.set(_redis_key(state.session_id), json.dumps(state.to_dict()), ex=_SESSION_TTL)
    except Exception as e:
        print(f"WARNING: Redis persist failed for {state.session_id}: {e}")

def _restore_sessions_from_redis() -> None:
    r = get_redis()
    if not r:
        return
    try:
        keys = r.keys(f"{_SESSION_PREFIX}*")
        restored = 0
        for key in keys:
            raw = r.get(key)
            if not raw:
                continue
            try:
                d = json.loads(raw)
                state = SessionState.from_dict(d)
                sessions[state.session_id] = state
                restored += 1
            except Exception as e:
                print(f"WARNING: Could not restore session from Redis key {key}: {e}")
        if restored:
            print(f"INFO: Restored {restored} session(s) from Redis")
    except Exception as e:
        print(f"WARNING: Redis session restore failed: {e}")

_sse_events: Dict[str, list] = {}

PIPELINE_TIMEOUT_SECONDS = 900

async def _pipeline_watchdog(session_id: str) -> None:
    await asyncio.sleep(PIPELINE_TIMEOUT_SECONDS)
    state = sessions.get(session_id)
    if state and state.status == "analyzing":
        state.status = "error"
        _sse_events.setdefault(session_id, []).append({
            "type": "stream_end",
            "data": {"status": "error", "reason": "Pipeline timed out"}
        })
        print(f"WARNING: [Watchdog] Session {session_id} timed out after {PIPELINE_TIMEOUT_SECONDS}s")

def push_sse_event(session_id: str, event_type: str, data: dict) -> None:
    if session_id not in _sse_events:
        _sse_events[session_id] = []

    try:
        clean_data = json.loads(json.dumps(data, default=str))
    except Exception:
        clean_data = {}
    _sse_events[session_id].append({"type": event_type, "data": clean_data})

async def run_agent_pipeline(
    pipeline_id: str,
    prompt: str,
    agent_getter: str = "root",
    max_turns: int = None,
    image_paths: List[str] = None,
    agent=None,
) -> str:
    if max_turns is None:
        max_turns = get_config()["agents"]["max_turns"]["default"]

    APP_NAME = "Analytics_analytics"
    USER_ID = "user_1"

    agent_map = {
        "root":        get_root_agent,
        "profiler":    get_profiler_agent,
        "discovery":   get_discovery_agent,
        "coder":       get_coder_agent,
        "synthesis":   get_synthesis_agent,
        "dag_builder": get_dag_builder_agent,
        "chat":        get_chat_agent,
        "critic":      get_critic_agent,
    }
    if agent is not None:
        target_agent = agent
    else:
        getter = agent_map.get(agent_getter, get_root_agent)
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
                    def _read_img(p=img_path):
                        with open(p, "rb") as f:
                            return f.read()
                    img_bytes = await asyncio.to_thread(_read_img)

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

    max_retries = 6

    _adk_trace = os.environ.get("ADK_TRACE", "1") == "1"

    for attempt in range(max_retries):
        try:
            final_response = ""
            turn_count = 0

            if _adk_trace:
                push_sse_event(pipeline_id, "turn_started", {"agent": agent_getter})

            async for event in runner.run_async(
                user_id=USER_ID,
                session_id=pipeline_id,
                new_message=content,
            ):
                turn_count += 1

                if _adk_trace and event.content and event.content.parts:
                    for _part in event.content.parts:
                        _fc = getattr(_part, "function_call", None)
                        if _fc and getattr(_fc, "name", None):
                            push_sse_event(pipeline_id, "tool_called", {
                                "agent": agent_getter,
                                "tool": _fc.name,
                            })

                if event.is_final_response():
                    if event.content and event.content.parts:
                        final_response = event.content.parts[0].text
                    if _adk_trace:
                        push_sse_event(pipeline_id, "turn_ended", {
                            "agent": agent_getter,
                            "turns_used": turn_count,
                        })

                if turn_count >= max_turns:
                    print(
                        f"WARNING: Agent '{agent_getter}' hit "
                        f"max_turns={max_turns}. Stopping."
                    )
                    break

            return final_response

        except Exception as e:
            error_str = str(e).lower()
            is_rate_limit = (
                "429" in error_str or "rate" in error_str
                or "exhausted" in error_str or "quota" in error_str
                or "resource_exhausted" in error_str
                or "503" in error_str or "unavailable" in error_str
                or "high demand" in error_str or "overloaded" in error_str
            )
            if is_rate_limit:
                if attempt < max_retries - 1:

                    match = re.search(r"'retryDelay': '(\d+)s'", str(e))
                    base_delay = int(match.group(1)) + 1 if match else get_config()["pipeline"]["rate_limit_default_delay"]
                    delay = min(base_delay * (attempt + 1), 120)
                    print(f"Rate limit hit. Waiting {delay}s... (Attempt {attempt+1}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                else:
                    print(f"Rate limit exhausted after {max_retries} attempts")
                    return json.dumps({"status": "error", "error": f"API rate limit exceeded after {max_retries} retries."})
            raise

    return final_response

def extract_json(response: str) -> dict:
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

async def _validate_via_llm(prompt: str) -> dict:
    import google.generativeai as genai
    from tools.model_config import get_model
    model = genai.GenerativeModel(get_model("discovery"))
    response = model.generate_content(prompt)
    return extract_json(response.text) or {}

@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = BASE_DIR / "frontend" / "dist" / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not is_supported(file.filename):
        raise HTTPException(
            status_code=415,
            detail=(
                f"Unsupported file type. "
                f"Supported formats: "
                f"{', '.join(get_supported_extensions())}"
            ),
        )

    session_id = str(uuid.uuid4())

    file_basename = file.filename.rsplit(".", 1)[0]
    safe_folder = re.sub(r'[^\w]', '_', file_basename).strip('_')[:80]
    output_folder = safe_folder
    if (OUTPUT_DIR / output_folder).exists():
        output_folder = f"{safe_folder}_{session_id[:6]}"

    ext = Path(file.filename).suffix.lower()
    saved_file_path = UPLOAD_DIR / f"{output_folder}{ext}"

    content = await file.read()
    def _write_upload():
        with open(saved_file_path, "wb") as f:
            f.write(content)
    await asyncio.to_thread(_write_upload)

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
    state.output_folder = str(OUTPUT_DIR / output_folder)
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
    _persist_session(state)

    from pipeline_types import create_message, Intent
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
            "memory_mb": raw.get("memory_mb"),
            "column_roles": raw.get("column_roles", {}),
        },
        "classification": profiler_data.get("classification"),
    }

def build_fallback_discovery(state: SessionState, session_id: str) -> dict:
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
        f"Output folder: {state.output_folder}\n\n"
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
            max_turns=get_config()["agents"]["max_turns"]["discovery"],
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
    _persist_session(state)

    return {
        "session_id": session_id,
        "status": "discovered",
        "discovery": discovery_data,
    }

@app.post("/validate-metric/{session_id}")
async def validate_metric(session_id: str, request: Request):
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
        f"Respond with ONLY a JSON object with these exact keys:\n"
        f'- "valid": true or false\n'
        f'- "reason": why it is or is not feasible\n'
        f'- "metric_name": short display name for the analysis\n'
        f'- "description": one sentence describing the insight\n'
        f'- "analysis_type": snake_case identifier (e.g. "weekly_batch_patterns")\n'
        f'- "column_roles": object mapping role keys to exact column names from the available columns list above '
        f'(e.g. {{"time_col": "created_at", "value_col": "amount"}}). Use only column names that exist in the data. '
        f'Use standard role keys: time_col, value_col, entity_col, event_col, category_col, group_col, col, col_a, col_b.\n\n'
        f'Example: {{"valid": true, "reason": "...", "metric_name": "Weekly Batch Patterns", "description": "...", '
        f'"analysis_type": "weekly_batch_patterns", "column_roles": {{"time_col": "created_at", "value_col": "amount"}}}}'
    )

    result = await _validate_via_llm(prompt)
    return {"session_id": session_id, "validation": result}

class AnalyzeRequest(BaseModel):
    request: Optional[str] = "Analyze all metrics"
    custom_metrics: Optional[List[str]] = []
    custom_nodes: Optional[List[dict]] = []
    approved_metrics: Optional[List[str]] = None
    user_instructions: Optional[str] = ""

async def run_pipeline_background(
    session_id, csv_path, output_folder,
    approved, state
):
    from agents.orchestrator import _pipeline_event_hooks
    _pipeline_event_hooks[session_id] = lambda evt, data: push_sse_event(session_id, evt, data)
    _sse_events[session_id] = []
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
        _persist_session(state)
    except Exception as e:
        print(f"ERROR: {str(e)}")
        traceback.print_exc()
        state.status = "error"
        _persist_session(state)
        try:
            push_sse_event(session_id, "stream_end", {"status": "error", "error": str(e)})
        except Exception:
            pass
    finally:
        _pipeline_event_hooks.pop(session_id, None)

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

    if state.status in ("analyzing", "synthesizing", "building_report"):
        raise HTTPException(
            status_code=409,
            detail=f"Analysis already in progress (status: {state.status}). Wait for it to complete."
        )

    metrics = req_body.approved_metrics or req_body.custom_metrics or None
    output_folder_path = state.output_folder

    if req_body.user_instructions:
        state.user_instructions = req_body.user_instructions

    if req_body.custom_nodes:
        existing_ids = {n.get("id") for n in (state.dag or [])}
        appended_ids = []
        for cn in req_body.custom_nodes:
            node_id = cn.get("id") or f"C{len(state.dag) + 1}"
            if node_id not in existing_ids:
                state.dag.append({
                    "id":            node_id,
                    "analysis_type": cn.get("analysis_type", "custom"),
                    "description":   cn.get("description", ""),
                    "name":          cn.get("name") or cn.get("analysis_type", "Custom Analysis"),
                    "column_roles":  cn.get("column_roles", {}),
                    "depends_on":    [],
                    "priority":      cn.get("priority", "medium"),
                })
                existing_ids.add(node_id)
                appended_ids.append(node_id)
        print(f"INFO: {len(appended_ids)} custom node(s) appended to DAG for {session_id}")

        if appended_ids and metrics is not None:
            metrics = list(metrics) + appended_ids

    print(f"Starting pipeline for {session_id}")
    print(f"csv_path: {state.csv_path}")
    print(f"output_folder: {output_folder_path}")
    if state.user_instructions:
        print(f"user_instructions: {state.user_instructions}")

    background_tasks.add_task(
        run_pipeline_background,
        session_id=session_id,
        csv_path=state.csv_path,
        output_folder=output_folder_path,
        approved=metrics,
        state=state,
    )
    asyncio.create_task(_pipeline_watchdog(session_id))

    return {"status": "started", "session_id": session_id}

@app.post("/clarify/{session_id}")
async def submit_clarification(session_id: str, request: Request):
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(status_code=404, detail="Session not found")
    if state.status != "clarification_needed":
        raise HTTPException(status_code=409, detail=f"Session is not awaiting clarification (status: {state.status})")

    body = await request.json()
    confirmed_roles = body.get("column_roles", {})
    if not confirmed_roles:
        raise HTTPException(status_code=400, detail="column_roles required in request body")

    if not isinstance(state.semantic_map, dict):
        state.semantic_map = {}
    if "column_roles" not in state.semantic_map:
        state.semantic_map["column_roles"] = {}
    state.semantic_map["column_roles"].update(confirmed_roles)

    from pipeline_types import create_message, Intent
    state.post_message(create_message(
        intent=Intent.CLARIFICATION_PROVIDED,
        sender="frontend",
        recipient="discovery_agent",
        payload={"confirmed_column_roles": confirmed_roles},
    ))

    state.status = "uploaded"
    push_sse_event(session_id, "status_update", {"status": "clarification_provided", "message": "Column roles confirmed — re-running discovery."})

    asyncio.create_task(_re_discover(session_id))
    return {"status": "clarification_provided", "session_id": session_id}

async def _re_discover(session_id: str) -> None:
    state = sessions.get(session_id)
    if not state:
        return
    try:
        from agents.orchestrator import run_full_pipeline
        state.status = "discovering"
        push_sse_event(session_id, "status_update", {"status": "discovering", "message": "Re-running discovery with confirmed column roles."})
        await run_agent_pipeline(
            f"{session_id}_rediscover",
            (
                f"Session ID: {session_id}\n"
                f"csv_path: {state.csv_path}\n"
                f"Profiler output already stored. Column roles confirmed by user: "
                f"{state.semantic_map.get('column_roles', {})}\n"
                f"Call tool_submit_analysis_plan now with a revised DAG."
            ),
            agent_getter="discovery",
        )
        push_sse_event(session_id, "status_update", {"status": "ready_to_analyze", "message": "Discovery complete — ready to analyze."})
        state.status = "discovered"
    except Exception as _e:
        print(f"ERROR: _re_discover failed for {session_id}: {_e}")
        state.status = "error"

@app.get("/status/{session_id}")
async def get_status(session_id: str):
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )

    live = get_pipeline_status(session_id)
    live_statuses = live.get("node_statuses", {})

    node_statuses = {}
    failed = getattr(state, "failed_nodes", set())
    if state.dag:
        for node in state.dag:
            nid = node.get("id", "")
            if nid in live_statuses:
                node_statuses[nid] = live_statuses[nid]
            elif nid in failed:
                node_statuses[nid] = "failed"
            elif nid in state.results:
                node_statuses[nid] = "complete"
            else:
                node_statuses[nid] = "pending"

    from tools.monitor import get_session_events
    raw_events = get_session_events(session_id, min_severity="warning")
    alerts = [{"event": e.get("type", "unknown"), "data": e.get("payload", {})} for e in raw_events]

    return {
        "session_id":     session_id,
        "session_status": state.status,
        "progress_pct":   live.get("progress_pct", 0),
        "pipeline": {
            "node_statuses": node_statuses,
            "is_complete":   live.get("is_complete", False),
        },
        "gate_result":    getattr(state, "gate_result", {}),
        "alerts":         alerts,
        "result_count":   len(state.results),
        "has_synthesis":  bool(state.synthesis),
        "has_report": any(
            a.get("type") == "report"
            for a in state.artifacts
        ),
    }

@app.get("/stream/{session_id}")
async def sse_stream(session_id: str, request: Request):
    async def event_generator():
        last_index = 0
        _keepalive_counter = 0
        _max_sse_seconds = 1800
        _elapsed = 0.0
        _poll_interval = 0.4
        while _elapsed < _max_sse_seconds:
            if await request.is_disconnected():
                break
            events = _sse_events.get(session_id, [])

            new_events = events[last_index:]
            for ev in new_events:
                try:
                    yield f"data: {json.dumps(ev, default=str)}\n\n".encode("utf-8")
                except Exception:
                    yield f"data: {json.dumps({'type': ev.get('type', 'unknown'), 'data': {}})}\n\n".encode("utf-8")
            last_index += len(new_events)

            state = sessions.get(session_id)
            if state and state.status in ("complete", "error") and last_index >= len(events):
                yield f"data: {json.dumps({'type': 'stream_end', 'data': {'status': state.status}})}\n\n".encode("utf-8")
                _sse_events.pop(session_id, None)
                break

            _keepalive_counter += 1
            if _keepalive_counter % 12 == 0:
                yield b": keepalive\n\n"
            await asyncio.sleep(_poll_interval)
            _elapsed += _poll_interval
        else:
            yield f"data: {json.dumps({'type': 'stream_end', 'data': {'status': 'error', 'error': 'SSE stream timed out after 30 minutes'}})}\n\n".encode("utf-8")
            _sse_events.pop(session_id, None)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )

@app.post("/rerun-synthesis/{session_id}")
async def rerun_synthesis(session_id: str, background_tasks: BackgroundTasks, body: dict = None):
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(404, "Session not found")

    if state.status not in ("complete", "error", "rerunning_synthesis"):
        raise HTTPException(
            409,
            f"Cannot rerun synthesis while pipeline is in state '{state.status}'. "
            "Wait for the pipeline to complete first."
        )

    completed = {
        k: v for k, v in state.results.items()
        if isinstance(v, dict) and v.get("status") == "success"
    }
    if not completed:
        raise HTTPException(400, "No completed analyses available to synthesize.")

    try:
        from agents.synthesis import _synthesis_store, _reasoning_store
        _synthesis_store.pop(session_id, None)
        _reasoning_store.pop(session_id, None)
    except Exception:
        pass
    state.synthesis = {}
    state.status = "rerunning_synthesis"
    _sse_events[session_id] = []

    user_instructions = (body or {}).get("instructions", "").strip()

    async def _do_rerun():
        try:
            from agents.orchestrator import build_synthesis_prompt, get_pipeline_state
            pipe_state = get_pipeline_state(session_id)
            effective_state = pipe_state or state
            dag = getattr(state, "dag", []) or []
            prompt, images = build_synthesis_prompt(session_id, effective_state, dag)
            if user_instructions:
                prompt = prompt + f"\n\nUSER INSTRUCTIONS FOR THIS SYNTHESIS:\n{user_instructions}\n"

            await run_agent_pipeline(
                f"{session_id}_synthesis_rerun",
                prompt,
                agent_getter="synthesis",
                image_paths=images,
            )
            push_sse_event(session_id, "synthesis_complete", {"session_id": session_id})

            try:
                from agents.critic import get_critic_store_result
                await run_agent_pipeline(
                    f"{session_id}_critic_rerun",
                    f"session_id: {session_id}\nReview the regenerated synthesis.",
                    agent_getter="critic",
                    max_turns=get_config()["agents"]["max_turns"]["critic"],
                )
                _crit = get_critic_store_result(session_id)
                if _crit:
                    from agents.synthesis import _synthesis_store as _ss
                    if session_id in _ss:
                        _ss[session_id]["_critic_review"] = _crit
                    if isinstance(state.synthesis, dict):
                        state.synthesis["_critic_review"] = _crit
            except Exception as _ce:
                print(f"WARNING: Critic rerun failed (non-fatal): {_ce}")

            await run_agent_pipeline(
                f"{session_id}_report_rerun",
                f"Session ID: {session_id}\nOutput folder: {state.output_folder}\n"
                f"Call tool_build_report(session_id, output_folder) now.",
                agent_getter="dag_builder",
            )
            push_sse_event(session_id, "report_ready", {"session_id": session_id})
            state.status = "complete"
            print(f"INFO: Synthesis rerun complete for {session_id}")
        except Exception as _e:
            print(f"ERROR: Synthesis rerun failed for {session_id}: {_e}")
            traceback.print_exc()
            state.status = "error"

    background_tasks.add_task(_do_rerun)
    return {
        "status": "rerunning",
        "session_id": session_id,
        "message": f"Synthesis restarted for {len(completed)} completed analyses. "
                   "Results will appear shortly.",
    }

async def _classify_chat_intent(message: str) -> str:
    classification_prompt = (
        "You are a message intent classifier for a data analytics platform.\n"
        "The user has already run an analysis pipeline and is now chatting.\n\n"
        "Classify this message as ONE of:\n"
        "- 'analysis' — the user wants to run a NEW analysis, compute a new metric, "
        "  create a new chart, or investigate something not already in the results. "
        "  Examples: 'analyze retention by cohort', 'show me churn by region', "
        "  'can you compute average revenue per user', 'break down sessions by device type'\n"
        "- 'question' — the user is asking about EXISTING results, wants an explanation, "
        "  summary, recommendation, or general conversation. "
        "  Examples: 'what does the churn rate mean?', 'summarize the top findings', "
        "  'what should we fix first?', 'explain the anomaly in A3'\n\n"
        f"User message: \"{message}\"\n\n"
        "Respond with ONLY the word 'analysis' or 'question'. Nothing else."
    )
    try:
        resp = await run_agent_pipeline(
            f"_intent_classify_{uuid.uuid4().hex[:8]}",
            classification_prompt,
            agent_getter="chat",
            max_turns=get_config()["agents"]["max_turns"]["intent_classify"],
        )
        intent = (resp or "").strip().lower().rstrip(".")
        if "analysis" in intent:
            return "analysis"
        return "question"
    except Exception:
        return "question"

async def _run_chat_analysis(session_id: str, state, message: str) -> dict:
    profile      = getattr(state, "raw_profile", {}) or {}
    dataset_type = getattr(state, "dataset_type", "unknown")

    col_lines = []
    for c in profile.get("columns", []):
        line = f"  - {c['name']} ({c.get('type_category', 'unknown')}, {c.get('unique_count', '?')} unique"
        samples = c.get("sample_values", [])
        if samples:
            line += f", e.g. {samples[:3]}"
        line += ")"
        col_lines.append(line)
    col_block = "\n".join(col_lines) if col_lines else "  (no profile available)"

    validation_prompt = (
        f"Dataset type: {dataset_type}\n"
        f"Available columns:\n{col_block}\n\n"
        f"User's analysis request: \"{message}\"\n\n"
        f"TASK:\n"
        f"1. Determine if this analysis can be performed with the columns listed above.\n"
        f"2. If feasible: map the required column roles from the ACTUAL column names above, "
        f"pick the closest analysis_type from the known library types, "
        f"and write a 2-sentence description of what will be computed and what insight it reveals.\n"
        f"3. If not feasible: list exactly what data or column types are missing.\n\n"
        f"Known analysis_type values (use the closest match, or 'custom' if none fit):\n"
        f"session_detection, funnel_analysis, friction_detection, survival_analysis, "
        f"user_segmentation, transition_analysis, dropout_analysis, sequential_pattern_mining, "
        f"association_rules, distribution_analysis, categorical_analysis, correlation_matrix, "
        f"anomaly_detection, missing_data_analysis, trend_analysis, cohort_analysis, "
        f"rfm_analysis, pareto_analysis, event_taxonomy, user_journey_analysis, "
        f"intervention_triggers, session_classification, contribution_analysis, cross_tab_analysis\n\n"
        f"Respond with ONLY this JSON (no extra text, no markdown):\n"
        f'{{"valid": true, "reason": "one sentence", '
        f'"metric_name": "3-5 word clean name", '
        f'"description": "2 sentences: what is computed and what insight it reveals", '
        f'"analysis_type": "exact type or custom", '
        f'"column_roles": {{"entity_col": "col_name or null", "time_col": "col_name or null", '
        f'"event_col": "col_name or null", "outcome_col": "col_name or null"}}, '
        f'"missing_requirements": []}}'
    )

    validation = {}
    try:
        validation = await _validate_via_llm(validation_prompt)
        print(f"INFO: Chat analysis validation for '{message[:40]}': "
              f"valid={validation.get('valid')} type={validation.get('analysis_type')}")
    except Exception as ve:
        print(f"WARNING: Chat analysis validation failed: {ve}")

    if validation.get("valid") is False:
        missing = validation.get("missing_requirements", [])
        reason  = validation.get("reason", "This analysis cannot be performed with the available data.")
        missing_text = ""
        if missing:
            missing_text = "\n\nMissing data: " + ", ".join(missing)
        return {
            "session_id": session_id,
            "response": f"I can't run that analysis — {reason}{missing_text}",
            "analysis_status": "unsupported",
        }

    analysis_type = validation.get("analysis_type") or "custom"
    description   = validation.get("description") or message
    ai_roles      = validation.get("column_roles") or {}
    metric_name   = validation.get("metric_name") or message[:40]

    base_roles   = (state.semantic_map or {}).get("column_roles", {})
    merged_roles = {**base_roles, **{k: v for k, v in ai_roles.items() if v}}

    existing_c_ids = [
        aid for aid in (state.results or {}).keys()
        if aid.startswith("C")
    ]
    next_c = len(existing_c_ids) + 1
    analysis_id = f"C{next_c}"
    output_folder = state.output_folder

    push_sse_event(session_id, "node_started", {
        "node_id": analysis_id,
        "analysis_type": analysis_type,
        "description": description,
    })

    try:
        from agents.orchestrator import execute_single_analysis
        result = await execute_single_analysis(
            session_id=session_id,
            analysis_type=analysis_type,
            analysis_id=analysis_id,
            csv_path=state.csv_path,
            output_folder=output_folder,
            description=description,
            column_roles=merged_roles,
            state=state,
        )

        if result.get("status") == "success":

            if not hasattr(state, "dag") or state.dag is None:
                state.dag = []
            existing_ids = {n.get("id") for n in state.dag}
            if analysis_id not in existing_ids:
                state.dag.append({
                    "id":            analysis_id,
                    "analysis_type": result.get("analysis_type", analysis_type),
                    "description":   description,
                    "name":          metric_name,
                    "column_roles":  merged_roles,
                    "depends_on":    [],
                    "priority":      "high",
                })

            push_sse_event(session_id, "node_complete", {
                "analysis_id": analysis_id,
                "analysis_type": result.get("analysis_type", analysis_type),
                "top_finding": result.get("top_finding", ""),
                "severity": result.get("severity", "info"),
                "chart_file_path": result.get("chart_file_path"),
            })

            finding = result.get("top_finding", "")
            narrative = result.get("data", {}).get("narrative", {}) if isinstance(result.get("data"), dict) else {}
            ins_sum   = result.get("insight_summary", {}) or {}

            return {
                "session_id":  session_id,
                "response": (
                    f"✓ **{metric_name}** analysis complete ({analysis_id}).\n\n"
                    f"**Finding:** {finding}\n\n"
                    f"The chart card should appear above. You can ask me about the results "
                    f"or request another analysis."
                ),
                "analysis_status": "success",
                "analysis_id": analysis_id,

                "chart": {
                    "id":                     analysis_id,
                    "analysisType":           result.get("analysis_type", analysis_type),
                    "finding":                finding,
                    "hasChart":               bool(result.get("chart_file_path")),
                    "severity":               result.get("severity", "info"),
                    "whatItMeans":             narrative.get("what_it_means"),
                    "recommendation":         ins_sum.get("recommendation"),
                    "proposedFix":            narrative.get("proposed_fix"),
                    "decisionMakerTakeaway":  ins_sum.get("decision_maker_takeaway"),
                    "keyFinding":             ins_sum.get("key_finding"),
                    "topValues":              ins_sum.get("top_values"),
                    "anomalies":              ins_sum.get("anomalies"),
                },
            }
        else:

            push_sse_event(session_id, "node_failed", {
                "node_id": analysis_id,
                "error": result.get("error", "Analysis execution failed"),
            })
            return {
                "session_id": session_id,
                "response": f"The analysis could not be completed: {result.get('error', 'unknown error')}. Try rephrasing or request a different metric.",
                "analysis_status": "error",
            }
    except Exception as e:
        push_sse_event(session_id, "node_failed", {
            "node_id": analysis_id,
            "error": str(e),
        })
        return {
            "session_id": session_id,
            "response": f"Analysis execution failed: {str(e)}",
            "analysis_status": "error",
        }

@app.post("/chat/{session_id}")
async def chat(session_id: str, request: Request):
    if session_id not in sessions:
        raise HTTPException(404, "Session not found")

    body = await request.json()
    message = body.get("message", "")
    if not message:
        return {"session_id": session_id, "response": "Please provide a message."}

    state = sessions[session_id]

    if state.status in ("uploaded", "profiled", "discovered", "profiling", "discovering"):
        if state.user_instructions:
            state.user_instructions += f"\n{message}"
        else:
            state.user_instructions = message
        return {
            "session_id": session_id,
            "response": (
                "Got it — I'll factor that into the analysis when the pipeline runs. "
                "Add more instructions or click 'Execute Analysis Pipeline' when ready."
            ),
        }

    if state.status in ("complete", "synthesized", "analyzing", "error"):
        intent = await _classify_chat_intent(message)
        print(f"INFO: Chat intent for '{message[:50]}': {intent}")

        if intent == "analysis":
            return await _run_chat_analysis(session_id, state, message)

    context_parts = []

    col_roles = (state.semantic_map or {}).get("column_roles", {}) if hasattr(state, "semantic_map") else {}
    profile   = getattr(state, "raw_profile", {}) or {}
    columns   = profile.get("columns", [])
    col_summary = ", ".join(
        f"{c['name']} ({c.get('type_category','?')})" for c in columns[:30]
    ) if columns else "unavailable"

    context_parts.append(
        f"DATASET\n"
        f"  File: {os.path.basename(state.csv_path)}\n"
        f"  Type: {getattr(state, 'dataset_type', 'unknown')}\n"
        f"  Rows: {profile.get('row_count', '?')}\n"
        f"  Columns: {col_summary}\n"
        f"  Column roles: {json.dumps(col_roles)}"
    )

    if state.results:
        lines = []
        for aid, result in state.results.items():
            atype   = result.get("analysis_type", aid)
            finding = result.get("top_finding", "")
            sev     = result.get("severity", "info")
            nav     = result.get("data", {}).get("narrative", {})
            what    = nav.get("what_it_means", "")
            fix     = nav.get("proposed_fix", "")
            ins_sum = result.get("insight_summary", {})
            rec     = ins_sum.get("recommendation", "")

            block = f"[{aid}] {atype} (severity: {sev})\n  Finding: {finding}"
            if what: block += f"\n  What it means: {what}"
            if fix:  block += f"\n  Proposed fix: {fix}"
            if rec:  block += f"\n  Recommendation: {rec}"
            lines.append(block)
        context_parts.append("ANALYSIS RESULTS\n" + "\n\n".join(lines))

    synth = getattr(state, "synthesis", {}) or {}
    if synth:
        synth_parts = []

        exec_sum = synth.get("executive_summary", {})
        if exec_sum:
            synth_parts.append(
                f"Executive Summary:\n"
                f"  Health: {exec_sum.get('overall_health','')}\n"
                f"  Priorities: {'; '.join(exec_sum.get('top_priorities', []))}\n"
                f"  Business impact: {exec_sum.get('business_impact','')}\n"
                f"  Timeline: {exec_sum.get('timeline','')}"
            )

        insights = synth.get("detailed_insights", {}).get("insights", [])
        if insights:
            ins_lines = [
                f"  - {i.get('title','')} [{i.get('fix_priority','')}]: "
                f"{i.get('ai_summary','')} | "
                f"Root cause: {i.get('root_cause_hypothesis','')} | "
                f"Fix: {'; '.join(i.get('how_to_fix', []))}"
                for i in insights
            ]
            synth_parts.append("Detailed Insights:\n" + "\n".join(ins_lines))

        strategies = synth.get("intervention_strategies", {}).get("strategies", [])
        if strategies:
            strat_lines = [
                f"  - [{s.get('severity','')}] {s.get('title','')}: "
                f"Real-time: {'; '.join(s.get('realtime_interventions',[]))} | "
                f"Proactive: {'; '.join(s.get('proactive_outreach',[]))}"
                for s in strategies
            ]
            synth_parts.append("Intervention Strategies:\n" + "\n".join(strat_lines))

        personas = synth.get("personas", {}).get("personas", [])
        if personas:
            p_lines = [
                f"  - {p.get('name','')} ({p.get('priority_level','')}): "
                f"{p.get('profile','')} | "
                f"Pain: {'; '.join(p.get('pain_points',[])[:3])} | "
                f"Opp: {'; '.join(p.get('opportunities',[])[:2])}"
                for p in personas
            ]
            synth_parts.append("User Profiles:\n" + "\n".join(p_lines))

        connections = synth.get("cross_metric_connections", {}).get("connections", [])
        if connections:
            cx_lines = [
                f"  - {c.get('finding_a','')} × {c.get('finding_b','')} "
                f"→ {c.get('synthesized_meaning','')}"
                for c in connections
            ]
            synth_parts.append("Cross-Metric Connections:\n" + "\n".join(cx_lines))

        conv = synth.get("conversational_report", "")
        if conv:
            synth_parts.append(f"Narrative Report:\n{conv}")

        context_parts.append("SYNTHESIS\n" + "\n\n".join(synth_parts))

    if state.conversation_history:
        history_lines = []
        for _q, _a in state.conversation_history[-5:]:
            history_lines.append(f"User: {_q}\nAssistant: {_a}")
        context_parts.append(
            "PRIOR CONVERSATION (most recent first)\n" + "\n\n".join(history_lines)
        )

    separator = "\n" + "=" * 60 + "\n"
    full_context = separator.join(context_parts)
    prompt = f"{full_context}\n\n{'=' * 60}\nUSER QUESTION: {message}"

    response = await run_agent_pipeline(
        f"{session_id}_chat",
        prompt,
        agent_getter="chat",
        max_turns=get_config()["agents"]["max_turns"]["chat"],
    )

    if response:
        state.conversation_history.append((message, response))

    return {"session_id": session_id, "response": response}

@app.post("/add-metric/{session_id}")
async def add_metric(session_id: str, request: Request):
    if session_id not in sessions:
        raise HTTPException(404, "Session not found")

    body = await request.json()
    metric_text = body.get("metric", "")
    if not metric_text:
        raise HTTPException(400, "No metric description provided")

    state = sessions[session_id]
    profile      = state.raw_profile or {}
    dataset_type = state.dataset_type or "unknown"

    col_lines = []
    for c in profile.get("columns", []):
        line = f"  - {c['name']} ({c.get('type_category', 'unknown')}, {c.get('unique_count', '?')} unique"
        samples = c.get("sample_values", [])
        if samples:
            line += f", e.g. {samples[:3]}"
        line += ")"
        col_lines.append(line)
    col_block = "\n".join(col_lines) if col_lines else "  (no profile available)"

    validation_prompt = (
        f"Dataset type: {dataset_type}\n"
        f"Available columns:\n{col_block}\n\n"
        f"User's analysis request: \"{metric_text}\"\n\n"
        f"TASK:\n"
        f"1. Determine if this analysis can be performed with the columns listed above.\n"
        f"2. If feasible: map the required column roles from the ACTUAL column names above, "
        f"pick the closest analysis_type from the known library types, "
        f"and write a 2-sentence description of what will be computed and what insight it reveals.\n"
        f"3. If not feasible: list exactly what data or column types are missing.\n\n"
        f"Known analysis_type values (use the closest match, or 'custom' if none fit):\n"
        f"session_detection, funnel_analysis, friction_detection, survival_analysis, "
        f"user_segmentation, transition_analysis, dropout_analysis, sequential_pattern_mining, "
        f"association_rules, distribution_analysis, categorical_analysis, correlation_matrix, "
        f"anomaly_detection, missing_data_analysis, trend_analysis, cohort_analysis, "
        f"rfm_analysis, pareto_analysis, event_taxonomy, user_journey_analysis, "
        f"intervention_triggers, session_classification, contribution_analysis, cross_tab_analysis\n\n"
        f"Respond with ONLY this JSON (no extra text, no markdown):\n"
        f'{{"valid": true, "reason": "one sentence", '
        f'"metric_name": "3-5 word clean name", '
        f'"description": "2 sentences: what is computed and what insight it reveals", '
        f'"analysis_type": "exact type or custom", '
        f'"column_roles": {{"entity_col": "col_name or null", "time_col": "col_name or null", '
        f'"event_col": "col_name or null", "outcome_col": "col_name or null"}}, '
        f'"missing_requirements": []}}'
    )

    validation = {}
    try:
        validation = await _validate_via_llm(validation_prompt)
        print(f"INFO: Custom metric validation for '{metric_text[:40]}': "
              f"valid={validation.get('valid')} type={validation.get('analysis_type')}")
    except Exception as ve:
        print(f"WARNING: Custom metric validation LLM call failed: {ve}. Proceeding with defaults.")

    if validation.get("valid") is False:
        missing = validation.get("missing_requirements", [])
        reason  = validation.get("reason", "This analysis cannot be performed with the available data.")
        return {
            "session_id":          session_id,
            "status":              "unsupported",
            "reason":              reason,
            "missing_requirements": missing,
        }

    analysis_type  = validation.get("analysis_type") or "custom"
    description    = validation.get("description") or metric_text
    ai_roles       = validation.get("column_roles") or {}

    base_roles   = state.semantic_map.get("column_roles", {})
    merged_roles = {**base_roles, **{k: v for k, v in ai_roles.items() if v}}

    analysis_id   = f"C{len(state.results) + 1}"
    output_folder = state.output_folder

    try:
        from agents.orchestrator import execute_single_analysis
        result = await execute_single_analysis(
            session_id=session_id,
            analysis_type=analysis_type,
            analysis_id=analysis_id,
            csv_path=state.csv_path,
            output_folder=output_folder,
            description=description,
            column_roles=merged_roles,
            state=state,
        )

        if result.get("status") == "success":

            if not hasattr(state, "dag") or state.dag is None:
                state.dag = []
            existing_ids = {n.get("id") for n in state.dag}
            if analysis_id not in existing_ids:
                state.dag.append({
                    "id":            analysis_id,
                    "analysis_type": result.get("analysis_type", analysis_type),
                    "description":   description,
                    "name":          validation.get("metric_name", metric_text[:40]),
                    "column_roles":  merged_roles,
                    "depends_on":    [],
                    "priority":      "high",
                })
            return {
                "session_id":  session_id,
                "status":      "success",
                "analysis_id": analysis_id,
                "analysis_type": result.get("analysis_type", analysis_type),
                "metric_name": validation.get("metric_name", metric_text[:40]),
                "top_finding": result.get("top_finding", ""),
                "chart_path":  result.get("chart_file_path"),
                "severity":    result.get("severity", "info"),
                "description": description,
            }
        else:
            return {
                "session_id": session_id,
                "status":     "error",
                "error":      result.get("error", "Analysis execution failed"),
            }
    except Exception as e:
        return {
            "session_id": session_id,
            "status":     "error",
            "error":      str(e),
        }

@app.get("/results/{session_id}")
async def get_results(session_id: str):
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(status_code=404, detail="Session not found")
    results = []
    for aid, result in state.results.items():
        results.append({
            "analysis_id":     aid,
            "analysis_type":   result.get("analysis_type"),
            "top_finding":     result.get("top_finding", ""),
            "severity":        result.get("severity", "info"),
            "chart_path":      result.get("chart_file_path"),
            "insight_summary": result.get("insight_summary", {}),
            "narrative":       result.get("data", {}).get("narrative", {}),
            "status":          result.get("status", "success"),
        })
    return results

@app.post("/retry/{session_id}/{node_id}")
async def retry_node(session_id: str, node_id: str):
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(404, "Session not found")

    node = next((n for n in (state.dag or []) if n.get("id") == node_id), None)
    if not node:
        raise HTTPException(404, f"Node {node_id} not found in pipeline plan")

    analysis_type = node.get("analysis_type", "custom")
    description   = node.get("description", analysis_type)
    column_roles  = state.semantic_map.get("column_roles", {})
    output_folder = state.output_folder

    state.failed_nodes.discard(node_id)
    state.results.pop(node_id, None)

    try:
        from agents.orchestrator import execute_single_analysis
        result = await execute_single_analysis(
            session_id=session_id,
            analysis_type=analysis_type,
            analysis_id=node_id,
            csv_path=state.csv_path,
            output_folder=output_folder,
            description=description,
            column_roles=column_roles,
            state=state,
        )
        ok = result.get("status") == "success"
        return {
            "status":        "success" if ok else "error",
            "analysis_id":   node_id,
            "analysis_type": result.get("analysis_type", analysis_type),
            "top_finding":   result.get("top_finding", ""),
            "chart_path":    result.get("chart_file_path"),
            "severity":      result.get("severity", "info"),
            "insight_summary": result.get("insight_summary", {}),
            "narrative":       result.get("data", {}).get("narrative", {}),
            "error":         result.get("error"),
        }
    except Exception as e:
        state.failed_nodes.add(node_id)
        return {"status": "error", "analysis_id": node_id, "error": str(e)}

@app.post("/report/refresh/{session_id}")
async def refresh_report(session_id: str):
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(404, "Session not found")
    if not state.output_folder:
        raise HTTPException(400, "No output folder — run the pipeline first")

    try:
        from agents.dag_builder import tool_build_report
        result = tool_build_report(
            session_id=session_id,
            output_folder=state.output_folder,
        )
        return {"status": "success", "chart_count": result.get("chart_count", 0)}
    except Exception as e:
        raise HTTPException(500, f"Report refresh failed: {e}")

@app.get("/chart/{session_id}/{analysis_id}")
async def get_chart(
    session_id: str,
    analysis_id: str
):
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
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(status_code=404, detail="Session not found")
    return state.synthesis or {}

@app.get("/report/{session_id}")
async def get_report(session_id: str):
    state = sessions.get(session_id)
    if not state:
        raise HTTPException(status_code=404, detail="Session not found")

    report_artifact = next(
        (a for a in state.artifacts if a.get("type") == "report" or a.get("filename") == "report.html"),
        None
    )

    if not report_artifact:
        report_path = Path(state.output_folder) / "report.html"
        if not report_path.exists():
            raise HTTPException(status_code=404, detail="Report not yet generated")
    else:
        if "path" in report_artifact:
            report_path = Path(report_artifact["path"])
        else:
            report_path = Path(state.output_folder) / "report.html"

    return FileResponse(report_path, media_type="text/html")

@app.get("/output/{folder_name}/{filename}")
async def serve_artifact(folder_name: str, filename: str):
    filepath = (OUTPUT_DIR / folder_name / filename).resolve()

    if not str(filepath).startswith(str(OUTPUT_DIR.resolve())):
        raise HTTPException(403, "Access denied")
    if not filepath.exists():
        raise HTTPException(404, "File not found")

    if filename.endswith(".html"):
        return FileResponse(filepath, media_type="text/html")
    return FileResponse(filepath)

@app.get("/sessions")
async def list_sessions():
    return {
        "sessions": [
            {"id": sid, "filename": info.csv_filename, "status": info.status}
            for sid, info in sessions.items()
        ]
    }

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
app.mount("/assets", StaticFiles(directory=str(BASE_DIR / "frontend" / "dist" / "assets")), name="dist_assets")
app.mount("/user-activity", StaticFiles(directory=str(BASE_DIR / "frontend" / "public" / "user-activity"), html=True), name="user_activity")

@app.get("/adhopsun.jpeg")
async def serve_logo():
    return FileResponse(str(BASE_DIR / "frontend" / "dist" / "adhopsun.jpeg"), media_type="image/jpeg")

@app.get("/icons.svg")
async def serve_icons():
    return FileResponse(str(BASE_DIR / "frontend" / "dist" / "icons.svg"), media_type="image/svg+xml")

@app.get("/{full_path:path}", response_class=HTMLResponse)
async def spa_fallback(full_path: str):
    html_path = BASE_DIR / "frontend" / "dist" / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))

if __name__ == "__main__":
    import uvicorn
    print("Analytics Server starting...")
    uvicorn.run(app, host="127.0.0.1", port=8000)
