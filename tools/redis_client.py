"""
tools/redis_client.py
=====================
Thin Redis wrapper. All other modules import from here — never import redis directly.

Usage:
    from tools.redis_client import get_redis, redis_available

    if redis_available():
        r = get_redis()
        r.set("key", "value", ex=3600)
        val = r.get("key")

If Redis is not running, get_redis() returns None and redis_available() returns False.
Every call site must handle the None case — the app always falls back gracefully.

Config (env vars, all optional):
    REDIS_HOST     default: localhost
    REDIS_PORT     default: 6379
    REDIS_DB       default: 0
    REDIS_PASSWORD default: (none)
"""
import os
import redis

_REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
_REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
_REDIS_DB   = int(os.getenv("REDIS_DB",   "0"))
_REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

_client: redis.Redis | None = None
_checked: bool = False
_available: bool = False


def get_redis() -> redis.Redis | None:
    """
    Return a connected Redis client, or None if Redis is unavailable.
    Connection is created once and reused (singleton).
    """
    global _client, _checked, _available
    if _checked:
        return _client if _available else None

    _checked = True
    try:
        _client = redis.Redis(
            host=_REDIS_HOST,
            port=_REDIS_PORT,
            db=_REDIS_DB,
            password=_REDIS_PASSWORD,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        _client.ping()
        _available = True
        print(f"INFO: Redis connected at {_REDIS_HOST}:{_REDIS_PORT} db={_REDIS_DB}")
    except Exception as e:
        _available = False
        _client = None
        print(f"INFO: Redis unavailable ({e}) — falling back to in-memory stores")

    return _client if _available else None


def redis_available() -> bool:
    """Return True if Redis is reachable. Triggers connection check on first call."""
    get_redis()
    return _available
