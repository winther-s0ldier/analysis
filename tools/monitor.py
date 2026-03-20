from typing import Dict, List, Any
from datetime import datetime

# In-memory store: session_id -> list of event dicts
_events_store: Dict[str, List[Dict[str, Any]]] = {}

def emit(session_id: str, event_type: str, payload: dict, severity: str = "info") -> None:
    """
    Emit a monitoring event for a specific session.
    
    Args:
        session_id: The pipeline session ID.
        event_type: e.g., 'node_started', 'node_failed', 'HIGH_FAILURE_RATE'
        payload: Any contextual data dict.
        severity: "info", "warning", "error", or "critical".
    """
    if session_id not in _events_store:
        _events_store[session_id] = []
        
    event = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "type": event_type,
        "severity": severity,
        "payload": payload,
    }
    
    _events_store[session_id].append(event)
    
    # Also log it simply to stdout for the terminal user
    color = "\033[91m" if severity in ("error", "critical") else ("\033[93m" if severity == "warning" else "\033[94m")
    reset = "\033[0m"
    print(f"{color}[Monitor][{severity.upper()}] {event_type} - {payload.get('message', '')} {reset}")


def get_session_events(session_id: str, min_severity: str = "info") -> List[Dict[str, Any]]:
    """Retrieve all events for a session, optionally filtered by severity."""
    events = _events_store.get(session_id, [])
    
    if min_severity == "info":
        return list(events)
        
    levels = {"info": 0, "warning": 1, "error": 2, "critical": 3}
    min_level = levels.get(min_severity, 0)
    
    return [e for e in events if levels.get(e.get("severity", "info"), 0) >= min_level]


def clear_session(session_id: str) -> None:
    """Free memory for a session once it's completely done and exported."""
    if session_id in _events_store:
        del _events_store[session_id]


def check_failure_threshold(session_id: str, total_nodes: int, failed_nodes: int) -> None:
    """
    Called after DAG completion to emit a high-level alert 
    if the failure rate is unacceptable.
    """
    if total_nodes == 0:
        return
        
    fail_rate = failed_nodes / total_nodes
    if fail_rate > 0.3:
        emit(
            session_id=session_id,
            event_type="HIGH_FAILURE_RATE",
            severity="error",
            payload={
                "message": f"{failed_nodes} out of {total_nodes} nodes failed ({fail_rate:.0%}).",
                "fail_rate": fail_rate,
                "failed_nodes_count": failed_nodes,
                "total_nodes": total_nodes
            }
        )
