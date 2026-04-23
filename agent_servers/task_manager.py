"""A2A task lifecycle tracker.

Every outbound agent call (profiler, discovery, coder, synthesis, critic,
dag_builder) creates a Task here. The task moves through:

    submitted -> working -> completed | failed | canceled

The store is in-memory per process, keyed by task_id, with a secondary
index by session_id so the UI can list everything that happened in a
run. Each task optionally holds a reference to the asyncio.Task executing
the call, so /tasks/{id}/cancel can actually interrupt an in-flight HTTP
request rather than just flipping a status field.

This is deliberately an internal dataclass rather than a direct
a2a.types.Task — we convert to the A2A envelope at the FastAPI edge.
Keeps the internal model small and the protocol shape swappable.
"""
from __future__ import annotations

import asyncio
import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional


TASK_STATES = ("submitted", "working", "input_required", "completed", "failed", "canceled")
TERMINAL_STATES = {"completed", "failed", "canceled"}


@dataclass
class TaskRecord:
    id: str
    session_id: str
    agent_name: str
    state: str = "submitted"
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    input_summary: str = ""
    output_summary: str = ""
    error_code: Optional[int] = None
    error_message: Optional[str] = None
    history: list[dict] = field(default_factory=list)

    # Not serialised — live asyncio.Task for cancellation.
    _asyncio_handle: Optional[asyncio.Task] = field(default=None, repr=False)

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("_asyncio_handle", None)
        return d


_lock = threading.Lock()
_tasks: dict[str, TaskRecord] = {}
_by_session: dict[str, list[str]] = {}


def create_task(session_id: str, agent_name: str, input_summary: str = "") -> TaskRecord:
    task = TaskRecord(
        id=str(uuid.uuid4()),
        session_id=session_id,
        agent_name=agent_name,
        input_summary=(input_summary or "")[:500],
    )
    with _lock:
        _tasks[task.id] = task
        _by_session.setdefault(session_id, []).append(task.id)
    task.history.append({"ts": task.created_at, "state": "submitted"})
    return task


def _transition(task_id: str, new_state: str, **extra) -> Optional[TaskRecord]:
    with _lock:
        task = _tasks.get(task_id)
        if task is None:
            return None
        if task.state in TERMINAL_STATES and new_state != task.state:
            # Already terminal — ignore further transitions (e.g. a
            # completion racing with a cancel).
            return task
        task.state = new_state
        task.updated_at = time.time()
        for k, v in extra.items():
            if hasattr(task, k):
                setattr(task, k, v)
        task.history.append({"ts": task.updated_at, "state": new_state})
        return task


def start_task(task_id: str, asyncio_handle: Optional[asyncio.Task] = None) -> Optional[TaskRecord]:
    t = _transition(task_id, "working")
    if t is not None and asyncio_handle is not None:
        t._asyncio_handle = asyncio_handle
    return t


def complete_task(task_id: str, output_summary: str = "") -> Optional[TaskRecord]:
    return _transition(task_id, "completed", output_summary=(output_summary or "")[:500])


def fail_task(task_id: str, error_message: str, error_code: Optional[int] = None) -> Optional[TaskRecord]:
    return _transition(
        task_id, "failed",
        error_message=(error_message or "")[:2000],
        error_code=error_code,
    )


def cancel_task(task_id: str) -> tuple[Optional[TaskRecord], bool]:
    """Mark task canceled and interrupt its asyncio.Task if still running.

    Returns (task, interrupted). `interrupted` is True iff we actually
    called .cancel() on a live coroutine.
    """
    interrupted = False
    with _lock:
        task = _tasks.get(task_id)
        if task is None:
            return None, False
        handle = task._asyncio_handle
    if handle is not None and not handle.done():
        handle.cancel()
        interrupted = True
    t = _transition(task_id, "canceled")
    return t, interrupted


def get_task(task_id: str) -> Optional[TaskRecord]:
    with _lock:
        return _tasks.get(task_id)


def list_session_tasks(session_id: str) -> list[TaskRecord]:
    with _lock:
        ids = list(_by_session.get(session_id, []))
        return [_tasks[i] for i in ids if i in _tasks]


def list_in_flight(session_id: str) -> list[TaskRecord]:
    return [t for t in list_session_tasks(session_id) if t.state not in TERMINAL_STATES]


def cancel_all_for_session(session_id: str) -> int:
    """Cancel every non-terminal task on a session. Returns count interrupted."""
    interrupted = 0
    for t in list_in_flight(session_id):
        _, i = cancel_task(t.id)
        if i:
            interrupted += 1
    return interrupted


def purge_session(session_id: str) -> None:
    """Drop all task records for a session. Used when a session is evicted."""
    with _lock:
        ids = _by_session.pop(session_id, [])
        for i in ids:
            _tasks.pop(i, None)
