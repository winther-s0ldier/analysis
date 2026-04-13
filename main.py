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
HISTORY_FILE = OUTPUT_DIR / "history_index.json"

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
        self.csv_hash: str = ""

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
            "csv_hash":             self.csv_hash,
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
        state.csv_hash             = d.get("csv_hash", "")
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
            _stale_state = sessions.pop(sid, None)
            _sse_events.pop(sid, None)
            # Free cached DataFrame for this session's CSV
            if _stale_state and getattr(_stale_state, "csv_path", None):
                try:
                    from tools.csv_profiler import clear_df_cache
                    clear_df_cache(_stale_state.csv_path)
                except Exception:
                    pass

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

# ── History helpers ───────────────────────────────────────────────────────────

def _load_history() -> list:
    """Return list of history entries from disk, newest-first."""
    try:
        if HISTORY_FILE.exists():
            return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception as _he:
        print(f"WARNING: Could not read history index: {_he}")
    return []


def _generate_session_title(state: "SessionState") -> str:
    """Ask Gemini to generate a concise 5-7 word descriptive title for this session."""
    try:
        insights = (state.synthesis or {}).get("detailed_insights", {}).get("insights", [])
        top_insights = ", ".join(i.get("title", "") for i in insights[:3] if i.get("title"))
        _rp = state.raw_profile or {}
        prompt = (
            f"Dataset filename: {state.csv_filename}, "
            f"type: {state.dataset_type}, "
            f"{_rp.get('row_count', 0):,} rows. "
            f"Top findings: {top_insights or 'N/A'}. "
            "Write a concise 5-7 word session title that captures what was analysed. "
            "Include month/year if detectable from the filename. "
            "Reply with ONLY the title text, no quotes, no punctuation at end."
        )
        import google.generativeai as _genai
        from tools.model_config import get_model as _get_model
        _model = _genai.GenerativeModel(_get_model("synthesis"))
        _resp = _model.generate_content(prompt)
        _title = (_resp.text or "").strip()[:80]
        return _title if _title else Path(state.csv_filename).stem.replace("_", " ").replace("-", " ").title()
    except Exception as _te:
        print(f"WARNING: Could not generate session title: {_te}")
        return Path(state.csv_filename).stem.replace("_", " ").replace("-", " ").title()


def _write_session_snapshot(state: "SessionState", title: str) -> None:
    """Write master _session.json snapshot for robust single-file restore."""
    try:
        _rp = state.raw_profile or {}
        _ct = _rp.get("column_types", {})
        col_count = len(
            (_ct.get("numeric", []) or []) +
            (_ct.get("categorical", []) or []) +
            (_ct.get("datetime", []) or [])
        )
        custom_node_ids = [
            nid for nid, r in state.results.items()
            if r.get("_was_custom", False)
        ]
        snapshot = {
            "version":          "1.0",
            "session_id":       state.session_id,
            "title":            title,
            "csv_filename":     state.csv_filename,
            "dataset_type":     state.dataset_type,
            "completed_at":     time.time(),
            "row_count":        _rp.get("row_count", 0),
            "col_count":        col_count,
            "dag":              state.dag,
            "node_count":       len(state.dag),
            "completed_nodes":  [
                nid for nid, r in state.results.items()
                if r.get("status") != "error"
            ],
            "failed_nodes":     list(state.failed_nodes),
            "has_report":       os.path.exists(os.path.join(state.output_folder, "report.html")),
            "has_conversation": len(state.conversation_history) > 0,
            "conversation_turns": len(state.conversation_history),
            "agent_traces": {
                "profiler":   "_agent_profiler.json",
                "discovery":  "_agent_discovery.json",
                "synthesis":  "_agent_synthesis.json",
                "critic":     "_agent_critic.json",
                "coder_nodes": {nid: f"_agent_coder_{nid}.json" for nid in custom_node_ids},
            },
            "cache_files": {
                "plan":         "_plan_cache.json",
                "results":      "_results_cache.json",
                "synthesis":    "_synthesis_cache.json",
                "profile":      "_profile_cache.json",
                "conversation": "_conversation_cache.json",
            },
        }
        _snap_path = Path(state.output_folder) / "_session.json"
        _snap_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        print(f"INFO: [History] _session.json written for {state.session_id}")
    except Exception as _se:
        print(f"WARNING: Could not write _session.json: {_se}")


def _save_to_history(state: "SessionState", title: str = "") -> None:
    """Append or update an entry in the persistent history index."""
    try:
        entries = _load_history()
        entry = next((e for e in entries if e.get("session_id") == state.session_id), None)
        top_priority = ""
        node_count = len(state.dag)
        if state.synthesis:
            exec_summary = state.synthesis.get("executive_summary", {})
            priorities = exec_summary.get("top_priorities", [])
            if priorities:
                top_priority = str(priorities[0])[:120]
        _rp = state.raw_profile if state.raw_profile else {}
        row_count = _rp.get("row_count", 0)
        _ct = _rp.get("column_types", {})
        col_count = len(
            (_ct.get("numeric", []) or []) +
            (_ct.get("categorical", []) or []) +
            (_ct.get("datetime", []) or [])
        )
        top_insights = [
            i.get("title", "") for i in
            (state.synthesis or {}).get("detailed_insights", {}).get("insights", [])[:3]
            if i.get("title")
        ]
        new_entry = {
            "session_id":         state.session_id,
            "title":              title or Path(state.csv_filename).stem.replace("_", " ").title(),
            "csv_filename":       state.csv_filename,
            "csv_hash":           state.csv_hash,
            "csv_path":           state.csv_path,
            "created_at":         state.created_at,
            "completed_at":       time.time(),
            "dataset_type":       state.dataset_type,
            "row_count":          row_count,
            "col_count":          col_count,
            "node_count":         node_count,
            "top_priority":       top_priority,
            "top_insights":       top_insights,
            "has_report":         os.path.exists(os.path.join(state.output_folder, "report.html")),
            "has_conversation":   len(state.conversation_history) > 0,
            "conversation_turns": len(state.conversation_history),
            "completed_node_count": len([r for r in state.results.values() if r.get("status") != "error"]),
            "failed_node_count":  len(state.failed_nodes),
            "output_folder":      state.output_folder,
            "status":             "complete",
        }
        if entry:
            entries = [new_entry if e.get("session_id") == state.session_id else e for e in entries]
        else:
            entries.insert(0, new_entry)
        HISTORY_FILE.write_text(json.dumps(entries, indent=2), encoding="utf-8")
    except Exception as _he:
        print(f"WARNING: Could not save history entry for {state.session_id}: {_he}")


def _find_cached_session(csv_hash: str) -> dict | None:
    """Return the history entry for a previously completed session with the same CSV hash."""
    if not csv_hash:
        return None
    for entry in _load_history():
        if entry.get("csv_hash") == csv_hash and entry.get("status") == "complete":
            return entry
    return None

# ── SSE events ────────────────────────────────────────────────────────────────

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
    from google import genai
    from google.genai import types as genai_types
    from tools.model_config import get_model
    client = genai.Client()
    response = await asyncio.to_thread(
        client.models.generate_content,
        model=get_model("discovery"),
        contents=prompt,
        config=genai_types.GenerateContentConfig(temperature=0.1),
    )
    return extract_json(response.text) or {}

@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = BASE_DIR / "frontend" / "dist" / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))

# Mount the Vite assets folder so JS and CSS load properly
app.mount("/assets", StaticFiles(directory=str(BASE_DIR / "frontend" / "dist" / "assets")), name="assets")

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

    # Compute hash before writing so we can detect duplicate uploads
    import hashlib as _hashlib
    csv_hash = _hashlib.sha256(content).hexdigest()
    cached_entry = _find_cached_session(csv_hash)
    if cached_entry:
        # Same CSV detected — create a brand-new session but pre-load profile + DAG
        # from the old session's caches so we skip re-running those expensive steps.
        # The user gets a fresh session they can work with normally (add metrics, run, etc.)
        old_output = Path(cached_entry.get("output_folder", ""))
        _profile_data: dict = {}
        _discovery_data: dict = {}

        _pcp = old_output / "_profile_cache.json"
        if _pcp.exists():
            try:
                _pc = json.loads(_pcp.read_text(encoding="utf-8"))
                _raw = _pc.get("raw_profile", {})
                _cls = _pc.get("classification", {})
                _profile_data = {
                    "profile": {
                        "filename":     _raw.get("filename", cached_entry.get("csv_filename", "")),
                        "row_count":    _raw.get("row_count", cached_entry.get("row_count", 0)),
                        "column_count": _raw.get("column_count", cached_entry.get("col_count", 0)),
                        "columns":      _raw.get("columns", []),
                        "column_types": _raw.get("column_types", {}),
                        "correlations": _raw.get("correlations"),
                        "memory_mb":    _raw.get("memory_mb"),
                        "column_roles": _raw.get("column_roles", {}),
                    },
                    "classification": _cls,
                    "dataset_type":   _cls.get("dataset_type", cached_entry.get("dataset_type", "")),
                    "row_count":      _raw.get("row_count", cached_entry.get("row_count", 0)),
                    "column_count":   _raw.get("column_count", cached_entry.get("col_count", 0)),
                    "status": "profiled",
                }
            except Exception:
                pass

        _dcp = old_output / "_plan_cache.json"
        if _dcp.exists():
            try:
                _dc = json.loads(_dcp.read_text(encoding="utf-8"))
                _discovery_data = {
                    "dag":          _dc.get("dag", []),
                    "node_count":   len(_dc.get("dag", [])),
                    "dataset_type": cached_entry.get("dataset_type", ""),
                    "status":       "discovered",
                }
            except Exception:
                pass

        # Write the CSV to uploads so the new session has a valid csv_path
        def _write_cached():
            with open(saved_file_path, "wb") as f:
                f.write(content)
        await asyncio.to_thread(_write_cached)

        norm_result = normalize_file(str(saved_file_path.resolve()))
        new_csv_path = norm_result.get("csv_path", str(saved_file_path))

        new_output_folder = output_folder
        (OUTPUT_DIR / new_output_folder).mkdir(exist_ok=True)

        new_state = SessionState(session_id)
        new_state.csv_path = new_csv_path
        new_state.csv_filename = cached_entry.get("csv_filename", file.filename)
        new_state.output_folder = str(OUTPUT_DIR / new_output_folder)
        new_state.csv_hash = csv_hash

        if _profile_data:
            _raw2 = _profile_data.get("profile", {})
            new_state.raw_profile = {
                "filename":     _raw2.get("filename", ""),
                "row_count":    _raw2.get("row_count", 0),
                "column_count": _raw2.get("column_count", 0),
                "columns":      _raw2.get("columns", []),
                "column_types": _raw2.get("column_types", {}),
                "correlations": _raw2.get("correlations"),
                "memory_mb":    _raw2.get("memory_mb"),
                "column_roles": _raw2.get("column_roles", {}),
            }
            new_state.semantic_map = _profile_data.get("classification", {})
            new_state.dataset_type = new_state.semantic_map.get("dataset_type", "")
            new_state.status = "profiled"

        if _discovery_data.get("dag"):
            new_state.dag = _discovery_data["dag"]
            new_state.status = "discovered"

        sessions[session_id] = new_state

        return {
            "session_id":       session_id,
            "output_folder":    new_output_folder,
            "filename":         new_state.csv_filename,
            "format":           "csv",
            "rows":             new_state.raw_profile.get("row_count", cached_entry.get("row_count", 0)),
            "columns":          new_state.raw_profile.get("column_count", cached_entry.get("col_count", 0)),
            "Gate":             "pass",
            "warnings":         [],
            "status":           new_state.status,
            "profile_cached":   bool(_profile_data),
            "dag_cached":       bool(_discovery_data.get("dag")),
            "profile":          _profile_data if _profile_data else None,
            "discovery":        _discovery_data if _discovery_data else None,
        }

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
    state.csv_hash = csv_hash
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

    # Return cached profile immediately if already computed this session
    if state.raw_profile:
        raw = state.raw_profile
        return {
            "session_id": session_id,
            "status": "profiled",
            "profile": {
                "filename":     raw.get("filename"),
                "row_count":    raw.get("row_count"),
                "column_count": raw.get("column_count"),
                "columns":      raw.get("columns"),
                "column_types": raw.get("column_types", {}),
                "correlations": raw.get("correlations"),
                "memory_mb":    raw.get("memory_mb"),
                "column_roles": raw.get("column_roles", {}),
            },
            "classification": state.semantic_map,
            "cached": True,
        }

    # Also try loading from disk if it was persisted in a previous run
    _profile_cache_path = Path(state.output_folder) / "_profile_cache.json"
    if _profile_cache_path.exists():
        try:
            _cached = json.loads(_profile_cache_path.read_text(encoding="utf-8"))
            state.raw_profile = _cached.get("raw_profile", {})
            state.semantic_map = _cached.get("classification", {})
            state.dataset_type = state.semantic_map.get("dataset_type", "")
            state.status = "profiled"
            raw = state.raw_profile
            return {
                "session_id": session_id,
                "status": "profiled",
                "profile": {
                    "filename":     raw.get("filename"),
                    "row_count":    raw.get("row_count"),
                    "column_count": raw.get("column_count"),
                    "columns":      raw.get("columns"),
                    "column_types": raw.get("column_types", {}),
                    "correlations": raw.get("correlations"),
                    "memory_mb":    raw.get("memory_mb"),
                    "column_roles": raw.get("column_roles", {}),
                },
                "classification": state.semantic_map,
                "cached": True,
            }
        except Exception as _pce:
            print(f"WARNING: Could not load profile cache from disk: {_pce}")

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

    # Persist profile to disk so it survives server restarts
    try:
        _profile_cache_path.write_text(
            json.dumps({"raw_profile": state.raw_profile, "classification": state.semantic_map}, indent=2),
            encoding="utf-8",
        )
    except Exception as _pce:
        print(f"WARNING: Could not save profile cache to disk: {_pce}")

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
        comp_ctx = _get_comparison_context(session_id)
        result = await run_full_pipeline(
            session_id=session_id,
            csv_path=csv_path,
            output_folder=output_folder,
            approved_metrics=approved,
            state=state,
            comparison_context=comp_ctx,
        )
        print(f"INFO: Result: {result.get('status')}")
        _persist_session(state)
        if result.get("status") != "error":
            _session_title = _generate_session_title(state)
            _write_session_snapshot(state, _session_title)
            _save_to_history(state, title=_session_title)
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

@app.get("/history")
async def list_history():
    """Return all completed analysis sessions, newest first."""
    return _load_history()


def _get_comparison_context(session_id: str) -> str:
    """Helper to gather summaries from previous related sessions for the Synthesis Agent."""
    state = sessions.get(session_id)
    if not state: return ""

    index = _load_history()
    current_entry = next((e for e in index if e.get("session_id") == session_id), None)
    if not current_entry: return ""

    # Find top 3 related sessions (same CSV hash or same dataset type)
    related = [
        e for e in index
        if e.get("session_id") != session_id
        and e.get("status") == "complete"
        and (e.get("csv_hash") == current_entry.get("csv_hash") or e.get("dataset_type") == current_entry.get("dataset_type"))
    ][:3]

    if not related: return ""

    context_blocks = []
    for r in related:
        out = Path(r.get("output_folder", ""))
        sync_p = out / "_synthesis_cache.json"
        if sync_p.exists():
            try:
                data = json.loads(sync_p.read_text(encoding="utf-8"))
                summ = data.get("executive_summary", {}).get("overall_health", "No summary available.")
                context_blocks.append(
                    f"PREVIOUS SESSION ({r.get('title')}, {r.get('completed_at')}):\n"
                    f"Summary: {summ}\n"
                    f"{'NOTE: This session used the EXACT SAME dataset hash.' if r.get('csv_hash') == current_entry.get('csv_hash') else ''}\n"
                )
            except Exception: pass

    if not context_blocks: return ""
    return "\n---\nHISTORICAL CONTEXT (for Comparison):\n" + "\n".join(context_blocks) + "\n---\n"


@app.get("/history/{session_id}/restore")
async def restore_history_session(session_id: str):
    """
    Reconstruct the full UI state for a past analysis so the frontend
    can restore chat messages, pipeline nodes, synthesis, and report.
    """
    # Look up in the history index
    entry = next((e for e in _load_history() if e.get("session_id") == session_id), None)
    if not entry:
        raise HTTPException(404, "History entry not found")

    output_folder = Path(entry.get("output_folder", ""))

    # Load cached artefacts from the session's output folder
    synthesis: dict = {}
    plan: dict = {}
    results: dict = {}

    for fname, target in [
        ("_synthesis_cache.json", "synthesis"),
        ("_plan_cache.json", "plan"),
        ("_results_cache.json", "results"),
    ]:
        p = output_folder / fname
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                if target == "synthesis":
                    synthesis = data
                elif target == "plan":
                    plan = data
                elif target == "results":
                    results = data
            except Exception:
                pass

    # Load profile cache from disk
    profile_payload: dict = {}
    profile_cache_path = output_folder / "_profile_cache.json"
    if profile_cache_path.exists():
        try:
            _pc = json.loads(profile_cache_path.read_text(encoding="utf-8"))
            _raw = _pc.get("raw_profile", {})
            _cls = _pc.get("classification", {})
            profile_payload = {
                "profile": {
                    "filename":     _raw.get("filename", entry.get("csv_filename", "")),
                    "row_count":    _raw.get("row_count", entry.get("row_count", 0)),
                    "column_count": _raw.get("column_count", entry.get("col_count", 0)),
                    "columns":      _raw.get("columns", []),
                    "column_types": _raw.get("column_types", {}),
                    "correlations": _raw.get("correlations", []),
                    "memory_mb":    _raw.get("memory_mb"),
                    "column_roles": _raw.get("column_roles", {}),
                },
                "classification": {
                    "dataset_type": _cls.get("dataset_type", entry.get("dataset_type", "")),
                    "reasoning":    _cls.get("reasoning", ""),
                    "confidence":   _cls.get("confidence", 1.0),
                    "column_roles": _cls.get("column_roles", {}),
                },
                "dataset_type":   _cls.get("dataset_type", entry.get("dataset_type", "")),
                "row_count":      _raw.get("row_count", entry.get("row_count", 0)),
                "column_count":   _raw.get("column_count", entry.get("col_count", 0)),
            }
        except Exception as _e:
            print(f"WARNING: restore could not load profile cache: {_e}")

    # Fallback: no _profile_cache.json — build a minimal profile object from
    # whatever data is available so ProfileCard can still render something.
    if not profile_payload:
        _row = entry.get("row_count", 0)
        _col = entry.get("col_count", 0)
        _dtype = entry.get("dataset_type", "tabular_generic")
        _fname = entry.get("csv_filename", "")
        # Try to get row/col count from synthesis cache if history entry has zeros
        if (_row == 0 or _col == 0) and synthesis:
            _synth_meta = synthesis.get("dataset_meta", {})
            _row = _row or _synth_meta.get("row_count", 0)
            _col = _col or _synth_meta.get("col_count", 0)
        profile_payload = {
            "profile": {
                "filename":     _fname,
                "row_count":    _row,
                "column_count": _col,
                "columns":      [],
                "column_types": {},
                "correlations": None,
                "memory_mb":    None,
                "column_roles": {},
            },
            "classification": {
                "dataset_type":          _dtype,
                "column_roles":          {},
                "confidence":            "restored",
                "recommended_analyses":  [],
            },
            "dataset_type":  _dtype,
            "row_count":     _row,
            "column_count":  _col,
        }

    # If session is still alive in memory, use that (richer)
    live = sessions.get(session_id)
    if live:
        if live.raw_profile:
            synthesis = live.synthesis or synthesis
            plan = {"dag": live.dag} if live.dag else plan
            results = live.results or results

    # Re-hydrate session into memory so follow-up API calls (/chat, /report, etc.) work
    if session_id not in sessions:
        restored_state = SessionState(session_id)
        restored_state.csv_filename = entry.get("csv_filename", "")
        restored_state.output_folder = str(output_folder)
        restored_state.status = "complete"
        restored_state.dataset_type = entry.get("dataset_type", "")
        restored_state.csv_hash = entry.get("csv_hash", "")

        # Restore csv_path from history entry (recorded at upload time)
        _stored_csv_path = entry.get("csv_path", "")
        if _stored_csv_path and Path(_stored_csv_path).exists():
            restored_state.csv_path = _stored_csv_path
        else:
            # Fallback: look for CSV in UPLOAD_DIR based on output folder name
            _folder_name = Path(str(output_folder)).name
            for _ext in (".csv", ".xlsx", ".xls", ".json", ".parquet"):
                _candidate = UPLOAD_DIR / f"{_folder_name}{_ext}"
                if _candidate.exists():
                    restored_state.csv_path = str(_candidate)
                    break

        if profile_payload.get("profile"):
            restored_state.raw_profile = {
                "filename":     profile_payload["profile"].get("filename", ""),
                "row_count":    profile_payload["profile"].get("row_count", 0),
                "column_count": profile_payload["profile"].get("column_count", 0),
                "columns":      profile_payload["profile"].get("columns", []),
                "column_types": profile_payload["profile"].get("column_types", {}),
                "correlations": profile_payload["profile"].get("correlations"),
                "memory_mb":    profile_payload["profile"].get("memory_mb"),
                "column_roles": profile_payload["profile"].get("column_roles", {}),
            }
            restored_state.semantic_map = profile_payload.get("classification", {})

        restored_state.dag = plan.get("dag", [])
        restored_state.results = results
        restored_state.synthesis = synthesis
        sessions[session_id] = restored_state

    dag_nodes = plan.get("dag", [])

    # Build nodes list for the pipeline store
    nodes = []
    for n in dag_nodes:
        nid = n.get("id", "")
        status = "complete" if nid in results and results[nid].get("status") != "error" else (
            "failed" if nid in results else "pending"
        )
        nodes.append({
            "id":            nid,
            "name":          n.get("name", nid),
            "type":          n.get("analysis_type", ""),
            "status":        status,
            "priority":      n.get("priority", ""),
            "description":   n.get("description", ""),
        })

    # Build messages array to restore the chat
    messages = []

    def _msg(role, msg_type, payload):
        return {"id": f"restore_{msg_type}_{len(messages)}", "role": role,
                "type": msg_type, "payload": payload, "category": "pipeline",
                "timestamp": entry.get("created_at", 0) * 1000}

    # File card
    messages.append(_msg("user", "file", {
        "filename": entry.get("csv_filename", ""),
        "rows":     entry.get("row_count", 0),
        "columns":  entry.get("col_count", 0),
    }))

    # Profile card — use full cached profile so ProfileCard renders correctly
    if profile_payload:
        messages.append(_msg("ai", "profile", profile_payload))

    # Discovery card
    if dag_nodes:
        messages.append(_msg("ai", "discovery", {
            "dag":          dag_nodes,
            "node_count":   len(dag_nodes),
            "dataset_type": entry.get("dataset_type", ""),
        }))

    # Chart cards — one per completed result
    for nid, result in results.items():
        if result.get("status") == "error":
            continue
        chart_path = result.get("chart_file_path", "")
        chart_url = ""
        if chart_path:
            # Convert absolute path to relative URL served by FastAPI
            # Use .resolve() on both so absolute vs relative never causes a mismatch
            try:
                chart_url = "/output/" + Path(chart_path).resolve().relative_to(OUTPUT_DIR.resolve()).as_posix()
            except Exception:
                # Last-resort: just use the filename
                chart_url = "/output/" + Path(output_folder).name + "/" + Path(chart_path).name if chart_path else ""
        _data = result.get("data", {})
        _narrative = _data.get("narrative", {}) if isinstance(_data, dict) else {}
        _insight_sum = result.get("insight_summary", {}) if isinstance(result.get("insight_summary"), dict) else {}

        messages.append(_msg("ai", "chart", {
            "id":             nid,
            "analysis_type":  result.get("analysis_type", ""),
            "finding":        result.get("top_finding", ""),
            "severity":       result.get("severity", "info"),
            "confidence":     result.get("confidence"),
            "hasChart":        bool(chart_url),
            "chartUrl":        chart_url,
            # Flatten narrative and summary into top-level props for ChartCard.jsx
            "decisionMakerTakeaway": _insight_sum.get("decision_maker_takeaway", ""),
            "keyFinding":            _insight_sum.get("key_finding", ""),
            "topValues":             _insight_sum.get("top_values", ""),
            "anomalies":              _insight_sum.get("anomalies", ""),
            "whatItMeans":           _narrative.get("what_it_means", ""),
            "recommendation":        _insight_sum.get("recommendation", ""),
            "proposedFix":           _narrative.get("proposed_fix", ""),
            "data":                  _data,
        }))

    # Synthesis Summary card - (Deep Insights removed from chat restore to reduce clutter, 
    # they are better viewed in the full Report as per user preference).
    # Synthesis Summary card - (Suppressed in chat to reduce clutter as per user feedback; 
    # synthesis content is better viewed in the full interactive report).
    # if synthesis:
    #     ...

    # Report card - (Suppressed in chat; report navigation is now handled via the report artifact)
    report_path = output_folder / "report.html"
    has_report = report_path.exists()
    # if has_report:
    #     messages.append(_msg("ai", "report", {
    #         "session_id": session_id,
    #         "ready":      True,
    #     }))

    # Conversation Q&A — restore with timestamps and date separators
    _conv_path = output_folder / "_conversation_cache.json"
    conversation_history = []
    if _conv_path.exists():
        try:
            conversation_history = json.loads(_conv_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    if not conversation_history and live and live.conversation_history:
        conversation_history = live.conversation_history

    if conversation_history:
        _last_date_str = ""
        _completed_ts = entry.get("completed_at", 0)
        for _i, _turn in enumerate(conversation_history):
            # Support both (q, a) and (q, a, ts) formats
            _q = _turn[0] if len(_turn) > 0 else ""
            _a = _turn[1] if len(_turn) > 1 else ""
            
            # Defensive check: if _q or _a are objects (poisoned data), extract the text
            if isinstance(_q, dict): _q = _q.get("text", str(_q))
            if isinstance(_a, dict): _a = _a.get("text", str(_a))

            _ts = _turn[2] if len(_turn) > 2 else (_completed_ts + _i)
            from datetime import datetime as _dt
            _dt_obj = _dt.fromtimestamp(_ts)
            # On Windows strftime does not support %-d (leading zero strip).
            # We use a more robust way to format the date.
            try:
                _date_str = _dt_obj.strftime("%B %d, %Y").replace(" 0", " ")
            except Exception:
                _date_str = str(_dt_obj.date()) if _dt_obj else ""
            _time_str = _dt_obj.strftime("%I:%M %p").lstrip("0")
            # Emit date separator only when date changes
            if _date_str != _last_date_str:
                messages.append({
                    "id": f"restore_date_{_i}",
                    "role": "system",
                    "type": "date_separator",
                    "payload": {"date": _date_str},
                    "category": "conversation",
                    "timestamp": _ts * 1000,
                })
                _last_date_str = _date_str
            messages.append({
                "id": f"restore_conv_{_i}_q",
                "role": "user",
                "type": "text",
                "payload": _q,
                "category": "conversation",
                "timestamp": _ts * 1000,
            })
            messages.append({
                "id": f"restore_conv_{_i}_a",
                "role": "ai",
                "type": "text",
                "payload": _a,
                "category": "conversation",
                "timestamp": (_ts + 0.5) * 1000,
            })

    # Restore conversation into session memory so follow-up /chat works
    if session_id in sessions and conversation_history:
        sessions[session_id].conversation_history = conversation_history

    canvas_narrative = synthesis.get("conversational_report", "")

    # Cross-session context: find related sessions (same dataset_type) for the sidebar
    _related = [
        {
            "session_id":   e["session_id"],
            "title":        e.get("title", e.get("csv_filename", "")),
            "completed_at": e.get("completed_at", 0),
            "top_priority": e.get("top_priority", ""),
            "row_count":    e.get("row_count", 0),
        }
        for e in _load_history()
        if e.get("session_id") != session_id
        and e.get("dataset_type") == entry.get("dataset_type", "")
        and e.get("status") == "complete"
    ][:5]

    return {
        "session_id":         session_id,
        "title":              entry.get("title", entry.get("csv_filename", "")),
        "csv_filename":       entry.get("csv_filename", ""),
        "csv_available":      bool(sessions.get(session_id) and getattr(sessions[session_id], "csv_path", "") and Path(getattr(sessions[session_id], "csv_path", "")).exists()),
        "output_folder":      str(output_folder),
        "phase":              "complete",
        "nodes":              nodes,
        "synthesis":          synthesis,
        "has_report":         has_report,
        "messages":           messages,
        "canvas_narrative":   canvas_narrative,
        "dataset_type":       entry.get("dataset_type", ""),
        "row_count":          entry.get("row_count", 0),
        "col_count":          entry.get("col_count", 0),
        "conversation_turns": len(conversation_history),
        "related_sessions":   _related,
    }


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
            comp_ctx = _get_comparison_context(session_id)
            prompt, images = build_synthesis_prompt(session_id, effective_state, dag, output_folder=getattr(state, "output_folder", None), comparison_context=comp_ctx)
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

            _save_to_history(state, title=getattr(state, "title", ""))
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
        for _turn in state.conversation_history[-5:]:
            # Support both (q, a) legacy and (q, a, ts) new format
            _q = _turn[0] if len(_turn) > 0 else ""
            _a = _turn[1] if len(_turn) > 1 else ""
            history_lines.append(f"User: {_q}\nAssistant: {_a}")
        context_parts.append(
            "PRIOR CONVERSATION (most recent first)\n" + "\n\n".join(history_lines)
        )

    separator = "\n" + "=" * 60 + "\n"

    # Cross-session context: inject full synthesis from related sessions (same dataset_type)
    _current_dtype = getattr(state, "dataset_type", "")
    _related_blocks = []
    for _hist_entry in _load_history():
        if (
            _hist_entry.get("session_id") == session_id
            or _hist_entry.get("dataset_type") != _current_dtype
            or _hist_entry.get("status") != "complete"
        ):
            continue
        _rel_out = _hist_entry.get("output_folder", "")
        if not _rel_out:
            continue
        _syn_path = Path(_rel_out) / "_synthesis_cache.json"
        if not _syn_path.exists():
            continue
        try:
            _rel_syn = json.loads(_syn_path.read_text(encoding="utf-8"))
            _rel_title = _hist_entry.get("title", _hist_entry.get("csv_filename", "??"))
            from datetime import datetime as _dtm
            _rel_date = _dtm.fromtimestamp(_hist_entry.get("completed_at", 0)).strftime("%Y-%m-%d")
            _rel_rows = _hist_entry.get("row_count", 0)

            _rel_exec = _rel_syn.get("executive_summary", {})
            _rel_health = _rel_exec.get("overall_health", "")
            _rel_priorities = _rel_exec.get("top_priorities", [])

            _rel_insights_raw = _rel_syn.get("detailed_insights", {})
            if isinstance(_rel_insights_raw, dict):
                _rel_insights = _rel_insights_raw.get("insights", [])
            else:
                _rel_insights = _rel_insights_raw or []

            _rel_connections_raw = _rel_syn.get("cross_metric_connections", {})
            if isinstance(_rel_connections_raw, dict):
                _rel_connections = _rel_connections_raw.get("connections", [])
            else:
                _rel_connections = _rel_connections_raw or []

            _rel_personas = _rel_syn.get("user_personas", {}).get("personas", [])
            _rel_interventions = _rel_syn.get("intervention_strategies", {}).get("strategies", [])

            _block_lines = [
                f"SESSION: {_rel_title}  |  Date: {_rel_date}  |  Rows: {_rel_rows:,}",
                f"Overall Health: {_rel_health}",
            ]
            if _rel_priorities:
                _block_lines.append("Top Priorities: " + " | ".join(str(p) for p in _rel_priorities[:3]))
            for _ins in _rel_insights[:6]:
                _block_lines.append(
                    f"  [{_ins.get('node_id','?')}] {_ins.get('title','')} — "
                    f"{_ins.get('ai_summary','')[:200]}"
                )
            for _conn in _rel_connections[:3]:
                _block_lines.append(
                    f"  Connection: {_conn.get('finding_a','')} ↔ {_conn.get('finding_b','')}"
                )
            for _per in _rel_personas[:3]:
                _block_lines.append(f"  Persona: {_per.get('name','')} — {_per.get('description','')[:120]}")
            for _inv in _rel_interventions[:3]:
                _block_lines.append(f"  Intervention: {_inv.get('title','')} — {_inv.get('description','')[:120]}")
            _rel_conv = str(_rel_syn.get("conversational_report", ""))[:600]
            if _rel_conv:
                _block_lines.append(f"Narrative (excerpt): {_rel_conv}")

            _related_blocks.append("\n".join(_block_lines))
        except Exception as _rse:
            print(f"WARNING: Could not load related session synthesis ({_hist_entry.get('session_id')}): {_rse}")
        if len(_related_blocks) >= 3:
            break

    if _related_blocks:
        context_parts.append(
            "RELATED PREVIOUS SESSIONS (same data type — use for comparisons)\n"
            + ("\n" + "-" * 40 + "\n").join(_related_blocks)
        )

    full_context = separator.join(context_parts)
    prompt = f"{full_context}\n\n{'=' * 60}\nUSER QUESTION: {message}"

    response = await run_agent_pipeline(
        f"{session_id}_chat",
        prompt,
        agent_getter="chat",
        max_turns=get_config()["agents"]["max_turns"]["chat"],
    )

    if response:
        state.conversation_history.append((message, response, time.time()))
        # Persist conversation to disk so it survives restarts and restores
        _conv_path = Path(state.output_folder) / "_conversation_cache.json"
        try:
            _conv_path.write_text(
                json.dumps(state.conversation_history, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as _ce:
            print(f"WARNING: Could not write conversation cache: {_ce}")
        
        # Sync with master history index so the session jumps to top of sidebar
        _save_to_history(state, title="") 

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
app.mount("/user-activity", StaticFiles(directory=str(BASE_DIR / "frontend" / "public" / "user-activity"), html=True), name="user_activity")

_dist_assets = BASE_DIR / "frontend" / "dist" / "assets"
if _dist_assets.exists():
    app.mount("/assets", StaticFiles(directory=str(_dist_assets)), name="dist_assets")

@app.get("/adhopsun.jpeg")
async def serve_logo():
    return FileResponse(str(BASE_DIR / "frontend" / "dist" / "adhopsun.jpeg"), media_type="image/jpeg")

@app.get("/icons.svg")
async def serve_icons():
    return FileResponse(str(BASE_DIR / "frontend" / "dist" / "icons.svg"), media_type="image/svg+xml")

@app.get("/{full_path:path}", response_class=HTMLResponse)
async def spa_fallback(full_path: str):
    dist_index = BASE_DIR / "frontend" / "dist" / "index.html"
    if dist_index.exists():
        return HTMLResponse(content=dist_index.read_text(encoding="utf-8"))
    return HTMLResponse(content="<p>Run <code>npm run build</code> in the frontend folder.</p>", status_code=503)

if __name__ == "__main__":
    import uvicorn
    print("Analytics Server starting...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
