"""A2A artifact registry.

When an agent produces a durable file (chart PNG, synthesis JSON, HTML
report), it registers it here. The record carries the MIME type so the
UI / a follow-on agent can pick the right renderer, and a URI path so
the FastAPI edge can stream the bytes back via /artifacts/{id}.

This is the internal shape. At the A2A edge we can convert to a proper
a2a.types.Artifact envelope — keeping the internal model small means
we're not coupled to the protocol's exact shape as it evolves.

Design note: legacy code at agents/dag_builder.py:227 appends a
{"type": "report", "path": ...} dict directly onto SessionState.artifacts.
We keep that shape working (the UI reads it) and register *in addition*
here. Do not break the legacy path.
"""
from __future__ import annotations

import os
import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class ArtifactRecord:
    id: str
    session_id: str
    task_id: Optional[str]
    name: str
    mime_type: str
    uri_path: str                # absolute file path on disk
    kind: str = ""               # "chart" | "synthesis" | "report" | other
    size_bytes: int = 0
    created_at: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


_lock = threading.Lock()
_artifacts: dict[str, ArtifactRecord] = {}
_by_session: dict[str, list[str]] = {}


def register_artifact(
    session_id: str,
    name: str,
    mime_type: str,
    uri_path: str,
    kind: str = "",
    task_id: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> ArtifactRecord:
    """Register a file as an artifact. Safe to call repeatedly; each call
    creates a new record (charts can be regenerated on retry)."""
    try:
        size = os.path.getsize(uri_path) if os.path.exists(uri_path) else 0
    except OSError:
        size = 0
    rec = ArtifactRecord(
        id=str(uuid.uuid4()),
        session_id=session_id or "_orphan",
        task_id=task_id,
        name=name,
        mime_type=mime_type,
        uri_path=uri_path,
        kind=kind,
        size_bytes=size,
        metadata=metadata or {},
    )
    with _lock:
        _artifacts[rec.id] = rec
        _by_session.setdefault(rec.session_id, []).append(rec.id)
    return rec


def get_artifact(artifact_id: str) -> Optional[ArtifactRecord]:
    with _lock:
        return _artifacts.get(artifact_id)


def list_session_artifacts(session_id: str) -> list[ArtifactRecord]:
    with _lock:
        ids = list(_by_session.get(session_id, []))
        return [_artifacts[i] for i in ids if i in _artifacts]


def purge_session(session_id: str) -> None:
    with _lock:
        ids = _by_session.pop(session_id, [])
        for i in ids:
            _artifacts.pop(i, None)
