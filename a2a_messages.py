"""
A2A-style internal message protocol for agent-to-agent communication.
Messages are stored in Analytics SessionState for inter-agent coordination.
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid
import json


class Intent:
    PROFILE_REQUEST = "PROFILE_REQUEST"
    PROFILE_COMPLETE = "PROFILE_COMPLETE"

    DISCOVER_METRICS = "DISCOVER_METRICS"
    PLAN_READY = "PLAN_READY"
    PLAN_APPROVED = "PLAN_APPROVED"

    RUN_ANALYSIS = "RUN_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    ANALYSIS_FAILED = "ANALYSIS_FAILED"
    DEPENDENCY_BLOCKED = "DEPENDENCY_BLOCKED"
    DEPENDENCY_RESOLVED = "DEPENDENCY_RESOLVED"

    SYNTHESIZE = "SYNTHESIZE"
    SYNTHESIS_COMPLETE = "SYNTHESIS_COMPLETE"

    BUILD_REPORT = "BUILD_REPORT"
    REPORT_READY = "REPORT_READY"

    INSTALL_REQUIRED = "INSTALL_REQUIRED"
    INSTALL_COMPLETE = "INSTALL_COMPLETE"
    CLARIFICATION_NEEDED = "CLARIFICATION_NEEDED"
    CLARIFICATION_PROVIDED = "CLARIFICATION_PROVIDED"
    STATUS_UPDATE = "STATUS_UPDATE"
    ERROR = "ERROR"
    TASK_COMPLETE = "TASK_COMPLETE"


class NodeStatus:
    """Status of a single DAG node (analysis task)."""
    PENDING = "pending"
    BLOCKED = "blocked"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class A2AMessage:
    sender: str
    recipient: str
    intent: str
    payload: Dict[str, Any]
    session_id: str = ""
    message_id: str = field(
        default_factory=lambda: str(uuid.uuid4())[:8]
    )
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    parent_message_id: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict) -> "A2AMessage":
        return cls(**data)


def create_message(
    sender: str,
    recipient: str,
    intent: str,
    payload: Dict[str, Any],
    session_id: str = "",
    parent_id: Optional[str] = None,
) -> A2AMessage:
    """Create an A2A message."""
    return A2AMessage(
        sender=sender,
        recipient=recipient,
        intent=intent,
        payload=payload,
        session_id=session_id,
        parent_message_id=parent_id,
    )


def get_messages_by_intent(
    message_log: List[Any],
    intent: str,
) -> List[Any]:
    """Get all messages with a specific intent."""
    return [
        m for m in message_log
        if (m.get("intent") if isinstance(m, dict) else m.intent) == intent
    ]


def get_messages_from(
    message_log: List[Any],
    sender: str,
) -> List[Any]:
    """Get all messages from a specific sender."""
    return [
        m for m in message_log
        if (m.get("sender") if isinstance(m, dict) else m.sender) == sender
    ]


def get_latest_message(
    message_log: List[Any],
    intent: str = None,
    sender: str = None,
) -> Optional[Any]:
    """
    Get the most recent message matching filters.
    Can filter by intent, sender, or both.
    Returns None if no match found.
    """
    matches = message_log
    if intent:
        matches = [m for m in matches if (m.get("intent") if isinstance(m, dict) else m.intent) == intent]
    if sender:
        matches = [m for m in matches if (m.get("sender") if isinstance(m, dict) else m.sender) == sender]
    return matches[-1] if matches else None


def get_completed_analysis_ids(
    message_log: List[Any],
) -> List[str]:
    """
    Returns list of analysis_ids that have sent
    ANALYSIS_COMPLETE messages.
    Used by Orchestrator for dependency resolution.
    """
    complete_msgs = get_messages_by_intent(
        message_log, Intent.ANALYSIS_COMPLETE
    )
    return [
        (m.get("payload", {}) if isinstance(m, dict) else m.payload).get("analysis_id")
        for m in complete_msgs
        if (m.get("payload", {}) if isinstance(m, dict) else m.payload).get("analysis_id")
    ]


def is_dependency_resolved(
    depends_on: List[str],
    message_log: List[Any],
) -> bool:
    """
    Check if all dependency analysis_ids are complete.
    Used by Coder Agent before starting a blocked analysis.
    """
    completed = set(get_completed_analysis_ids(message_log))
    return all(dep in completed for dep in depends_on)


@dataclass
class MetricSpec:
    """
    Full specification for one node in the analysis DAG.
    Created by Discovery Agent, executed by Coder Agent.
    """
    id: str
    name: str
    description: str

    analysis_type: str
    library_function: Optional[str] = None
    required_columns: List[str] = field(default_factory=list)
    column_roles: Dict[str, str] = field(default_factory=dict)

    depends_on: List[str] = field(default_factory=list)
    enables: List[str] = field(default_factory=list)

    status: str = NodeStatus.PENDING
    feasible: bool = True
    feasibility: str = "HIGH"
    missing_columns: List[str] = field(default_factory=list)
    priority: str = "medium"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "MetricSpec":
        return cls(**data)


@dataclass
class AnalysisResult:
    """
    Standardized result envelope returned by every
    Coder Agent execution. Matches _make_result() in
    analysis_library.py exactly.
    The Synthesis Agent always receives this shape.
    """
    analysis_id: str
    analysis_type: str
    status: str

    data: Dict[str, Any] = field(default_factory=dict)
    top_finding: str = ""
    severity: str = "info"
    confidence: float = 0.0

    chart_ready_data: Dict[str, Any] = field(default_factory=dict)
    chart_file_path: Optional[str] = None

    insight_summary: Dict[str, str] = field(default_factory=lambda: {
        "key_finding": "",
        "top_values": "",
        "anomalies": "",
        "recommendation": ""
    })

    outputs_produced: List[str] = field(default_factory=list)

    error: Optional[str] = None
    retry_count: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "AnalysisResult":
        return cls(**data)

    def is_usable(self) -> bool:
        """
        True if result has meaningful content the
        Synthesis Agent can work with.
        """
        return (
            self.status == "success"
            and self.top_finding != ""
            and bool(self.data)
        )


@dataclass
class PipelineState:
    """
    Tracks the full execution state of the analysis DAG.
    Stored in SessionState. Read by Orchestrator to decide
    what to run next, what is blocked, what failed.
    """
    session_id: str
    total_nodes: int = 0
    nodes: Dict[str, str] = field(default_factory=dict)

    completed: List[str] = field(default_factory=list)
    running: List[str] = field(default_factory=list)
    blocked: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)
    pending: List[str] = field(default_factory=list)

    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    def get_status(self, analysis_id: str) -> str:
        """Get status of a node."""
        node_data = self.nodes.get(analysis_id)
        if isinstance(node_data, dict):
            return node_data.get("status", NodeStatus.PENDING)
        return node_data or NodeStatus.PENDING

    def mark_running(self, analysis_id: str) -> None:
        if isinstance(self.nodes.get(analysis_id), dict):
            self.nodes[analysis_id]["status"] = NodeStatus.RUNNING
        else:
            self.nodes[analysis_id] = NodeStatus.RUNNING
        if analysis_id in self.pending:
            self.pending.remove(analysis_id)
        if analysis_id in self.blocked:
            self.blocked.remove(analysis_id)
        if analysis_id not in self.running:
            self.running.append(analysis_id)

    def mark_complete(self, analysis_id: str) -> None:
        if isinstance(self.nodes.get(analysis_id), dict):
            self.nodes[analysis_id]["status"] = NodeStatus.COMPLETE
        else:
            self.nodes[analysis_id] = NodeStatus.COMPLETE
        if analysis_id in self.running:
            self.running.remove(analysis_id)
        if analysis_id not in self.completed:
            self.completed.append(analysis_id)

    def mark_failed(self, analysis_id: str) -> None:
        if isinstance(self.nodes.get(analysis_id), dict):
            self.nodes[analysis_id]["status"] = NodeStatus.FAILED
        else:
            self.nodes[analysis_id] = NodeStatus.FAILED
        if analysis_id in self.running:
            self.running.remove(analysis_id)
        if analysis_id not in self.failed:
            self.failed.append(analysis_id)

    def mark_blocked(self, analysis_id: str) -> None:
        if isinstance(self.nodes.get(analysis_id), dict):
            self.nodes[analysis_id]["status"] = NodeStatus.BLOCKED
        else:
            self.nodes[analysis_id] = NodeStatus.BLOCKED
        if analysis_id not in self.blocked:
            self.blocked.append(analysis_id)
        if analysis_id in self.pending:
            self.pending.remove(analysis_id)

    def get_ready_to_run(
        self,
        completed_ids: List[str],
        failed_ids: List[str] = None
    ) -> List[str]:
        """
        Returns analysis IDs that are either pending with
        no dependencies, or blocked but whose dependencies
        are now all complete.
        Called by Orchestrator after each ANALYSIS_COMPLETE.
        """
        ready = []
        completed_set = set(completed_ids)
        failed_set = set(failed_ids) if failed_ids else set()

        for aid, node_data in self.nodes.items():
            status = self.get_status(aid)
            if status in (NodeStatus.PENDING, NodeStatus.BLOCKED):
                depends_on = node_data.get("depends_on") or [] if isinstance(node_data, dict) else []
                if any(dep in failed_set for dep in depends_on):
                    continue
                if all(dep in completed_set for dep in depends_on):
                    ready.append(aid)
        return ready

    def is_complete(self) -> bool:
        return (
            len(self.running) == 0
            and len(self.pending) == 0
            and len(self.blocked) == 0
        )

    def progress_pct(self) -> float:
        if self.total_nodes == 0:
            return 0.0
        return round(len(self.completed) / self.total_nodes * 100, 1)

    def to_dict(self) -> dict:
        return asdict(self)
