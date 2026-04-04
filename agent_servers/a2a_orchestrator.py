import os
import uuid
import json
import asyncio
import threading
from typing import Optional

_A2A_SESSIONS_FILE = os.path.join(os.path.dirname(__file__), "..", "_a2a_sessions.json")
_registry_lock = threading.Lock()

_AGENT_AUTH_TOKEN = os.getenv("AGENT_AUTH_TOKEN", "")

def register_session(session_id: str, output_folder: str) -> None:
    try:
        with _registry_lock:
            data: dict = {}
            if os.path.exists(_A2A_SESSIONS_FILE):
                try:
                    with open(_A2A_SESSIONS_FILE, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except Exception:
                    data = {}
            data[session_id] = os.path.abspath(output_folder)
            import tempfile
            fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(_A2A_SESSIONS_FILE))
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_path, _A2A_SESSIONS_FILE)
    except Exception as e:
        print(f"WARNING: A2A session registry write failed: {e}")

def lookup_session(session_id: str) -> str:
    try:
        with open(_A2A_SESSIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get(session_id, "")
    except Exception:
        return ""

def _get_agent_url(name: str) -> str:
    defaults = {
        "profiler":    "http://localhost:8001",
        "discovery":   "http://localhost:8002",
        "coder":       "http://localhost:8003",
        "synthesis":   "http://localhost:8004",
        "critic":      "http://localhost:8005",
        "dag_builder": "http://localhost:8006",
    }
    env_keys = {
        "profiler":    "PROFILER_URL",
        "discovery":   "DISCOVERY_URL",
        "coder":       "CODER_URL",
        "synthesis":   "SYNTHESIS_URL",
        "critic":      "CRITIC_URL",
        "dag_builder": "DAG_BUILDER_URL",
    }
    return os.getenv(env_keys[name], defaults[name])

async def check_agent_available(name: str, timeout: float = 3.0) -> bool:
    try:
        import httpx
        url = f"{_get_agent_url(name)}/.well-known/agent-card.json"
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(url)
            return r.status_code == 200
    except Exception:
        return False

async def check_all_agents_available(timeout: float = 3.0) -> dict:
    agents_to_check = ["profiler", "discovery", "synthesis", "critic", "dag_builder"]
    results = await asyncio.gather(
        *[check_agent_available(a, timeout) for a in agents_to_check],
        return_exceptions=True
    )
    return {
        name: (isinstance(r, bool) and r)
        for name, r in zip(agents_to_check, results)
    }

async def _call_agent(name: str, message_text: str, timeout: float = 300.0) -> str:
    try:
        import httpx
        from a2a.client import A2AClient
        from a2a.types import (
            SendMessageRequest,
            MessageSendParams,
            Message,
            TextPart,
            Part,
            Role,
        )
    except ImportError as e:
        raise RuntimeError(f"a2a-sdk not installed: {e}. Run: pip install a2a-sdk") from e

    url = _get_agent_url(name)

    request = SendMessageRequest(
        id=str(uuid.uuid4()),
        params=MessageSendParams(
            message=Message(
                message_id=str(uuid.uuid4()),
                role=Role.user,
                parts=[Part(root=TextPart(text=message_text))],
            )
        ),
    )

    _auth_headers = {"Authorization": f"Bearer {_AGENT_AUTH_TOKEN}"} if _AGENT_AUTH_TOKEN else {}
    async with httpx.AsyncClient(timeout=timeout, headers=_auth_headers) as http:
        client = A2AClient(httpx_client=http, url=url)
        response = await client.send_message(request)

    return _extract_response_text(response)

def _extract_response_text(response) -> str:
    try:
        root = response.root

        result = getattr(root, "result", None)
        if result is None:

            err = getattr(root, "error", None)
            raise RuntimeError(f"A2A agent returned error: {err}")

        if hasattr(result, "history") and result.history:
            for msg in reversed(result.history):
                text = _parts_to_text(msg.parts)
                if text:
                    return text

        if hasattr(result, "parts"):
            text = _parts_to_text(result.parts)
            if text:
                return text

        if hasattr(result, "status") and hasattr(result.status, "message"):
            msg = result.status.message
            if msg and hasattr(msg, "parts"):
                text = _parts_to_text(msg.parts)
                if text:
                    return text

        return ""
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to extract response text: {e}") from e

def _parts_to_text(parts) -> str:
    if not parts:
        return ""
    chunks = []
    for part in parts:

        inner = getattr(part, "root", part)
        if hasattr(inner, "text"):
            chunks.append(inner.text or "")
    return "".join(chunks)

async def call_profiler(session_id: str, csv_path: str) -> str:
    prompt = (
        f"csv_path: {csv_path}\n"
        f"session_id: {session_id}\n"
        f"Call tool_profile_and_classify now."
    )
    return await _call_agent("profiler", prompt)

async def call_discovery(
    session_id: str,
    csv_path: str,
    output_folder: str,
    profile_summary: str,
    policy_context: str = "",
    confidence_warning: str = "",
) -> str:
    prompt = (
        f"Session ID: {session_id}\n"
        f"CSV file path: {csv_path}\n"
        f"Output folder: {output_folder}\n\n"
        f"PROFILER OUTPUT:\n{profile_summary}\n\n"
        + (f"{policy_context}\n\n" if policy_context else "")
        + (f"{confidence_warning}\n" if confidence_warning else "")
        + "INSTRUCTIONS:\n"
        "1. Reason about the data and the policy context above.\n"
        "2. Construct a JSON DAG of MetricSpec nodes.\n"
        "3. Call tool_submit_analysis_plan(session_id, dag_json_str).\n"
    )
    return await _call_agent("discovery", prompt)

async def call_synthesis(
    session_id: str,
    output_folder: str,
    synthesis_prompt_body: str,
) -> str:
    prompt = (
        f"Session ID: {session_id}\n"
        f"Output folder: {output_folder}\n"
        f"{synthesis_prompt_body}\n\n"
        f"Synthesize all results and call tool_submit_synthesis(session_id='{session_id}', "
        f"output_folder='{output_folder}', synthesis_json_str=<your JSON>)."
    )
    return await _call_agent("synthesis", prompt, timeout=600.0)

async def call_critic(session_id: str) -> str:
    prompt = (
        f"Session ID: {session_id}\n"
        "Review the synthesis and call tool_submit_critique."
    )
    return await _call_agent("critic", prompt, timeout=180.0)

async def call_dag_builder(session_id: str, output_folder: str) -> str:
    prompt = (
        f"Session ID: {session_id}\n"
        f"Output folder: {output_folder}\n"
        "Call tool_build_report(session_id, output_folder) now."
    )
    return await _call_agent("dag_builder", prompt, timeout=120.0)
