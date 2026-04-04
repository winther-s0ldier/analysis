import os
import sys
import argparse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=False)
except ImportError:
    pass

_raw = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
if _raw:
    os.environ.setdefault("GOOGLE_API_KEY", _raw)
    os.environ.setdefault("GEMINI_API_KEY", _raw)

_AGENT_AUTH_TOKEN = os.getenv("AGENT_AUTH_TOKEN", "")

class _BearerAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if request.url.path == "/.well-known/agent-card.json":
            return await call_next(request)
        if _AGENT_AUTH_TOKEN:
            auth = request.headers.get("Authorization", "")
            if auth != f"Bearer {_AGENT_AUTH_TOKEN}":
                return Response("Unauthorized", status_code=401)
        return await call_next(request)

AGENT_MAP = {
    "profiler":    ("agents.profiler",    "get_profiler_agent"),
    "discovery":   ("agents.discovery",   "get_discovery_agent"),
    "coder":       ("agents.coder",       "get_coder_agent"),
    "synthesis":   ("agents.synthesis",   "get_synthesis_agent"),
    "critic":      ("agents.critic",      "get_critic_agent"),
    "dag_builder": ("agents.dag_builder", "get_dag_builder_agent"),
}

DEFAULT_PORTS = {
    "profiler":    8001,
    "discovery":   8002,
    "coder":       8003,
    "synthesis":   8004,
    "critic":      8005,
    "dag_builder": 8006,
}

def main():
    parser = argparse.ArgumentParser(
        description="Start a single ADK agent as an A2A HTTP server"
    )
    parser.add_argument(
        "--agent", required=True, choices=list(AGENT_MAP.keys()),
        help="Which agent to serve"
    )
    parser.add_argument(
        "--port", type=int, default=None,
        help="Port to listen on (defaults: profiler=8001, discovery=8002, ...)"
    )
    parser.add_argument(
        "--host", default="0.0.0.0",
        help="Bind host (default: 0.0.0.0)"
    )
    args = parser.parse_args()

    port = args.port or DEFAULT_PORTS[args.agent]
    module_path, factory_name = AGENT_MAP[args.agent]

    print(f"INFO: Loading {args.agent} agent from {module_path}.{factory_name}...")
    import importlib
    mod = importlib.import_module(module_path)
    get_agent_fn = getattr(mod, factory_name)
    agent = get_agent_fn()

    try:
        from google.adk.a2a.utils.agent_to_a2a import to_a2a as _to_a2a
    except ImportError:
        print("ERROR: google.adk.a2a not available — install google-adk >= 1.0.0")
        sys.exit(1)

    app = _to_a2a(agent)
    app.add_middleware(_BearerAuthMiddleware)

    import uvicorn
    print(f"INFO: Starting {args.agent} A2A server on {args.host}:{port}")
    print(f"INFO: Agent Card -> http://{args.host}:{port}/.well-known/agent-card.json")
    print(f"INFO: A2A endpoint -> http://{args.host}:{port}/")
    uvicorn.run(app, host=args.host, port=port)

if __name__ == "__main__":
    main()
