import os
import uuid
import json
import asyncio
import threading
from typing import Optional

_A2A_SESSIONS_FILE = os.path.join(os.path.dirname(__file__), "..", "_a2a_sessions.json")
_registry_lock = threading.Lock()

_AGENT_AUTH_TOKEN = os.getenv("AGENT_AUTH_TOKEN", "")

# JSON-RPC 2.0 standard error codes (mirrors a2a.types constants).
# Used by A2AAgentError when the failure originates client-side; server-side
# errors carry their own code in the JSONRPCErrorResponse envelope and are
# propagated as-is.
_ERR_INTERNAL = -32603
_ERR_INVALID_REQUEST = -32600


class A2AAgentError(Exception):
    """A2A error with a JSON-RPC error code.

    `code` follows JSON-RPC 2.0 semantics (and A2A's -32001..-32006 range
    for protocol-specific errors). Server-returned errors keep their
    original code; client-side failures use the standard codes above.
    """

    def __init__(self, code: int, message: str, data=None):
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"A2A error [{code}]: {message}")


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


# ---------------------------------------------------------------------------
# Agent URL resolution
#
# Bootstrap URL comes from config.yaml (agents.servers.<name>.url). On first
# successful contact we fetch /.well-known/agent-card.json and cache the
# AgentCard's own `url` field — that becomes the canonical endpoint for
# subsequent calls. This lets agents advertise proxy-rewritten URLs and keeps
# all URL knowledge in one place instead of scattered defaults.
# ---------------------------------------------------------------------------

_url_cache_lock = threading.Lock()
_url_cache: dict[str, str] = {}       # name -> canonical RPC URL (from AgentCard)


def _bootstrap_url(name: str) -> str:
    """Bootstrap URL for first contact — from config.yaml, with env override."""
    env_key = f"{name.upper()}_URL"
    env_val = os.getenv(env_key)
    if env_val:
        return env_val.rstrip("/")

    try:
        from tools.config_loader import get_config
        servers = get_config().get("agents", {}).get("servers", {})
        url = servers.get(name, {}).get("url")
        if not url:
            raise KeyError(
                f"config.yaml: agents.servers.{name}.url missing"
            )
        return url.rstrip("/")
    except Exception as e:
        raise A2AAgentError(
            code=_ERR_INTERNAL,
            message=f"Cannot resolve URL for agent '{name}': {e}",
        ) from e


def _resolve_url(name: str) -> str:
    """Return the canonical URL for an agent, falling back to bootstrap."""
    with _url_cache_lock:
        cached = _url_cache.get(name)
    return cached or _bootstrap_url(name)


def _cache_url_from_card(name: str, card_json: dict) -> None:
    url = card_json.get("url")
    if not url:
        return
    with _url_cache_lock:
        _url_cache[name] = url.rstrip("/")


async def check_agent_available(name: str, timeout: float = 3.0) -> bool:
    try:
        import httpx
        url = f"{_resolve_url(name)}/.well-known/agent-card.json"
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(url)
            if r.status_code != 200:
                return False
            try:
                _cache_url_from_card(name, r.json())
            except Exception:
                pass
            return True
    except Exception:
        return False


async def check_all_agents_available(timeout: float = 3.0) -> dict:
    agents_to_check = ["profiler", "discovery", "coder", "synthesis", "critic", "dag_builder"]
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
        raise A2AAgentError(
            code=_ERR_INTERNAL,
            message=f"a2a-sdk not installed: {e}. Run: pip install a2a-sdk",
        ) from e

    url = _resolve_url(name)

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

        err = getattr(root, "error", None)
        if err is not None:
            # Server returned a JSON-RPC error envelope — propagate code/message/data verbatim.
            raise A2AAgentError(
                code=getattr(err, "code", _ERR_INTERNAL),
                message=getattr(err, "message", "A2A agent returned an error"),
                data=getattr(err, "data", None),
            )

        result = getattr(root, "result", None)
        if result is None:
            raise A2AAgentError(
                code=_ERR_INVALID_REQUEST,
                message="A2A response had neither `result` nor `error`",
            )

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
    except A2AAgentError:
        raise
    except Exception as e:
        raise A2AAgentError(
            code=_ERR_INTERNAL,
            message=f"Failed to extract response text: {e}",
        ) from e


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


async def call_coder(session_id: str, prompt: str) -> str:
    return await _call_agent("coder", prompt, timeout=180.0)
