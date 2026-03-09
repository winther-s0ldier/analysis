"""
File Manager Tool — handles output artifacts (charts, reports, code files).
"""
import os
import json
from typing import List, Dict, Optional
from datetime import datetime, timezone


def get_artifact_path(session_id: str, base_dir: str = "output") -> str:
    """Get the output directory for a session."""
    path = os.path.join(base_dir, session_id)
    os.makedirs(path, exist_ok=True)
    return path


def save_artifact(
    content: str,
    filename: str,
    session_id: str,
    artifact_type: str = "report",
    base_dir: str = "output",
) -> dict:
    """
    Save an artifact to the session output directory.
    
    Args:
        content: File content (text).
        filename: Name of the file.
        session_id: Session identifier.
        artifact_type: Type of artifact (chart, report, code, data).
        base_dir: Base output directory.
    
    Returns:
        dict with file_path and metadata.
    """
    out_dir = get_artifact_path(session_id, base_dir)
    filepath = os.path.join(out_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    manifest_path = os.path.join(out_dir, "manifest.json")
    manifest = _load_manifest(manifest_path)
    manifest["artifacts"].append({
        "filename": filename,
        "type": artifact_type,
        "created": datetime.now(timezone.utc).isoformat(),
        "size_bytes": os.path.getsize(filepath),
    })
    manifest["last_updated"] = datetime.now(timezone.utc).isoformat()
    _save_manifest(manifest, manifest_path)

    return {"file_path": filepath, "type": artifact_type, "session_id": session_id}


def list_artifacts(session_id: str, base_dir: str = "output") -> List[Dict]:
    """List all artifacts for a session."""
    out_dir = get_artifact_path(session_id, base_dir)
    manifest_path = os.path.join(out_dir, "manifest.json")
    manifest = _load_manifest(manifest_path)
    
    existing_files = set(a["filename"] for a in manifest["artifacts"])
    for f in os.listdir(out_dir):
        if f not in existing_files and f != "manifest.json":
            ext = f.split(".")[-1].lower()
            artifact_type = "chart" if ext in ("png", "jpg", "svg", "html") else "other"
            manifest["artifacts"].append({
                "filename": f,
                "type": artifact_type,
                "created": datetime.now(timezone.utc).isoformat(),
                "size_bytes": os.path.getsize(os.path.join(out_dir, f)),
            })

    return manifest["artifacts"]


def _load_manifest(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {"artifacts": [], "last_updated": None}


def _save_manifest(manifest: dict, path: str):
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)
