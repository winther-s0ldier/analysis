"""BigQuery connector — OAuth + project/dataset/table discovery + table fetch + CSV normalize.

Contract: the rest of the pipeline only cares about `csv_path` on disk.
This module produces a CSV in `uploads/` that looks like a normal tabular
dataset to `tools/csv_profiler.profile_csv`, so profiler/discovery/coder
all run unchanged.

OAuth scopes:
  bigquery.readonly          — read BQ tables
  cloud-platform.read-only   — list GCP projects via Resource Manager

Token cache layout (single-user dev):
    uploads/.bq_tokens/default.json     # refresh token + access token

Raw-pull cache (keyed by project + dataset + table + row_limit):
    uploads/.bq_cache/<hash>.csv
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Optional

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

from google.cloud import bigquery

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE = Path(__file__).resolve().parent.parent
UPLOAD_DIR = _BASE / "uploads"
TOKEN_DIR = UPLOAD_DIR / ".bq_tokens"
CACHE_DIR = UPLOAD_DIR / ".bq_cache"
TOKEN_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

SCOPES = [
    "https://www.googleapis.com/auth/bigquery.readonly",
    "https://www.googleapis.com/auth/cloud-platform.read-only",
]

DEFAULT_ROW_LIMIT = 50_000


# ── OAuth helpers ─────────────────────────────────────────────────────────────

def _client_config() -> dict:
    client_id = os.getenv("BQ_OAUTH_CLIENT_ID")
    client_secret = os.getenv("BQ_OAUTH_CLIENT_SECRET")
    redirect_uri = os.getenv("BQ_OAUTH_REDIRECT_URI", "http://localhost:8000/bq/auth/callback")
    if not client_id or not client_secret:
        raise RuntimeError(
            "BQ_OAUTH_CLIENT_ID / BQ_OAUTH_CLIENT_SECRET not set in environment. "
            "Set these in .env after creating an OAuth 2.0 client in Google Cloud Console."
        )
    return {
        "web": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [redirect_uri],
        }
    }


def _token_path(user_key: str = "default") -> Path:
    safe = "".join(c for c in user_key if c.isalnum() or c in "-_")[:64] or "default"
    return TOKEN_DIR / f"{safe}.json"


def build_auth_url(user_key: str = "default") -> tuple[str, str]:
    """Return (auth_url, state). Front-end redirects user to auth_url."""
    flow = Flow.from_client_config(_client_config(), scopes=SCOPES)
    flow.redirect_uri = os.getenv("BQ_OAUTH_REDIRECT_URI", "http://localhost:8000/bq/auth/callback")
    flow.autogenerate_code_verifier = False
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
        state=user_key,
    )
    return auth_url, state


def exchange_code_for_token(code: str, user_key: str = "default") -> dict:
    """Exchange OAuth callback `code` → refresh+access token, save to disk."""
    flow = Flow.from_client_config(_client_config(), scopes=SCOPES)
    flow.redirect_uri = os.getenv("BQ_OAUTH_REDIRECT_URI", "http://localhost:8000/bq/auth/callback")
    flow.autogenerate_code_verifier = False
    flow.fetch_token(code=code)
    creds = flow.credentials

    data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes) if creds.scopes else [],
        "expiry": creds.expiry.isoformat() if creds.expiry else None,
    }
    _token_path(user_key).write_text(json.dumps(data, indent=2), encoding="utf-8")
    return {"connected": True, "user_key": user_key}


def load_credentials(user_key: str = "default") -> Optional[Credentials]:
    tp = _token_path(user_key)
    if not tp.exists():
        return None
    try:
        data = json.loads(tp.read_text(encoding="utf-8"))
    except Exception:
        return None
    return Credentials(
        token=data.get("token"),
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=data.get("scopes"),
    )


def is_connected(user_key: str = "default") -> bool:
    creds = load_credentials(user_key)
    return bool(creds and creds.refresh_token)


def disconnect(user_key: str = "default") -> bool:
    tp = _token_path(user_key)
    if tp.exists():
        tp.unlink()
        return True
    return False


# ── Project / dataset / table discovery ──────────────────────────────────────

def list_projects(user_key: str = "default") -> list[dict]:
    """List GCP projects the authenticated user can access.

    Returns [{"project_id", "name"}, ...]
    """
    creds = load_credentials(user_key)
    if not creds:
        raise RuntimeError("Not connected to BigQuery. Call /bq/auth/start first.")

    service = build("cloudresourcemanager", "v1", credentials=creds)
    out: list[dict] = []
    request = service.projects().list()
    while request is not None:
        response = request.execute()
        for p in response.get("projects", []):
            if p.get("lifecycleState") == "ACTIVE":
                out.append({
                    "project_id": p["projectId"],
                    "name": p.get("name", p["projectId"]),
                })
        request = service.projects().list_next(request, response)
    return out


def list_datasets(project_id: str, user_key: str = "default") -> list[dict]:
    """List datasets in a GCP project.

    Returns [{"dataset_id", "friendly_name"}, ...]
    """
    creds = load_credentials(user_key)
    if not creds:
        raise RuntimeError("Not connected to BigQuery.")

    client = bigquery.Client(credentials=creds, project=project_id)
    out = []
    for ds in client.list_datasets():
        out.append({
            "dataset_id": ds.dataset_id,
            "friendly_name": ds.friendly_name or ds.dataset_id,
        })
    return out


def list_tables(project_id: str, dataset_id: str, user_key: str = "default") -> list[dict]:
    """List tables in a dataset.

    Returns [{"table_id", "num_rows"}, ...]
    """
    creds = load_credentials(user_key)
    if not creds:
        raise RuntimeError("Not connected to BigQuery.")

    client = bigquery.Client(credentials=creds, project=project_id)
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    out = []
    for tbl in client.list_tables(dataset_ref):
        out.append({
            "table_id": tbl.table_id,
            "num_rows": tbl.num_rows,
        })
    return out


# ── Table fetch ───────────────────────────────────────────────────────────────

def _cache_key(project_id: str, dataset_id: str, table_id: str, row_limit: int) -> str:
    payload = json.dumps(
        {"proj": project_id, "ds": dataset_id, "tbl": table_id, "lim": row_limit},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def fetch_table(
    project_id: str,
    dataset_id: str,
    table_id: str,
    user_key: str = "default",
    row_limit: int = DEFAULT_ROW_LIMIT,
    use_cache: bool = True,
) -> dict:
    """Pull up to `row_limit` rows from a BigQuery table and write as CSV.

    Returns:
      {
        "csv_path": "<abs path>",
        "row_count": int,
        "col_count": int,
        "project_id": str,
        "dataset_id": str,
        "table_id": str,
        "row_limit": int,
        "cached": bool,
      }
    """
    creds = load_credentials(user_key)
    if not creds:
        raise RuntimeError("Not connected to BigQuery.")

    key = _cache_key(project_id, dataset_id, table_id, row_limit)
    cache_csv = CACHE_DIR / f"{key}.csv"
    if use_cache and cache_csv.exists():
        df = pd.read_csv(cache_csv)
        return {
            "csv_path": str(cache_csv),
            "row_count": len(df),
            "col_count": len(df.columns),
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "row_limit": row_limit,
            "cached": True,
        }

    client = bigquery.Client(credentials=creds, project=project_id)
    table_ref = bigquery.TableReference(
        bigquery.DatasetReference(project_id, dataset_id), table_id
    )
    rows = client.list_rows(table_ref, max_results=row_limit)
    df = rows.to_dataframe()

    df.to_csv(cache_csv, index=False, encoding="utf-8")

    return {
        "csv_path": str(cache_csv),
        "row_count": len(df),
        "col_count": len(df.columns),
        "project_id": project_id,
        "dataset_id": dataset_id,
        "table_id": table_id,
        "row_limit": row_limit,
        "cached": False,
    }


def ingest_to_session_csv(
    project_id: str,
    dataset_id: str,
    table_id: str,
    session_id: str,
    output_dir: Path,
    user_key: str = "default",
    row_limit: int = DEFAULT_ROW_LIMIT,
) -> dict:
    """Fetch BigQuery table and copy it to the session's uploads slot.

    Writes to `output_dir/bq_{table_id}_{hash}.csv`.
    """
    result = fetch_table(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        user_key=user_key,
        row_limit=row_limit,
    )
    src = Path(result["csv_path"])
    output_dir.mkdir(parents=True, exist_ok=True)
    dst = output_dir / f"bq_{table_id}_{src.stem}.csv"
    shutil.copy2(src, dst)
    result["csv_path"] = str(dst)
    result["original_filename"] = f"{table_id} ({project_id}.{dataset_id}).csv"
    result["session_id"] = session_id
    return result
