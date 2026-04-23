"""GA4 connector — OAuth + property list + report fetch + CSV normalize.

Contract: the rest of the pipeline only cares about `csv_path` on disk.
This module produces a CSV in `uploads/` that looks like a normal tabular
dataset to `tools/csv_profiler.profile_csv`, so profiler/discovery/coder
all run unchanged.

OAuth scope: `analytics.readonly` (covers both Admin + Data API).

Token cache layout (single-user dev):
    uploads/.ga_tokens/default.json     # refresh token + access token

Raw-pull cache (keyed by property_id + date range + dim/metric hash):
    uploads/.ga_cache/<hash>.csv
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Optional

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow

# GA Admin + Data APIs
from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE = Path(__file__).resolve().parent.parent
UPLOAD_DIR = _BASE / "uploads"
TOKEN_DIR = UPLOAD_DIR / ".ga_tokens"
CACHE_DIR = UPLOAD_DIR / ".ga_cache"
TOKEN_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

# ── Defaults pulled for a "first-look" GA4 session ────────────────────────────
DEFAULT_DIMENSIONS = [
    "date",
    "sessionDefaultChannelGroup",
    "deviceCategory",
    "country",
]
DEFAULT_METRICS = [
    "sessions",
    "activeUsers",
    "engagedSessions",
    "averageSessionDuration",
    "screenPageViews",
    "conversions",
    "totalRevenue",
]


# ── OAuth helpers ─────────────────────────────────────────────────────────────

def _client_config() -> dict:
    client_id = os.getenv("GA_OAUTH_CLIENT_ID")
    client_secret = os.getenv("GA_OAUTH_CLIENT_SECRET")
    redirect_uri = os.getenv("GA_OAUTH_REDIRECT_URI", "http://localhost:8000/ga/auth/callback")
    if not client_id or not client_secret:
        raise RuntimeError(
            "GA_OAUTH_CLIENT_ID / GA_OAUTH_CLIENT_SECRET not set in environment. "
            "See .env for Phase 3 GA4 configuration."
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
    flow.redirect_uri = os.getenv("GA_OAUTH_REDIRECT_URI", "http://localhost:8000/ga/auth/callback")
    # Web-app clients use client_secret, not PKCE. Disable auto code_verifier
    # so the callback (which builds a fresh Flow) doesn't need to replay one.
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
    flow.redirect_uri = os.getenv("GA_OAUTH_REDIRECT_URI", "http://localhost:8000/ga/auth/callback")
    flow.autogenerate_code_verifier = False
    flow.fetch_token(code=code)
    creds = flow.credentials

    data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
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
    creds = Credentials(
        token=data.get("token"),
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=data.get("scopes"),
    )
    return creds


def is_connected(user_key: str = "default") -> bool:
    creds = load_credentials(user_key)
    return bool(creds and creds.refresh_token)


def disconnect(user_key: str = "default") -> bool:
    tp = _token_path(user_key)
    if tp.exists():
        tp.unlink()
        return True
    return False


# ── Property discovery ────────────────────────────────────────────────────────

def list_ga4_properties(user_key: str = "default") -> list[dict]:
    """List GA4 properties the authenticated user can read.

    Returns [{"property_id", "display_name", "account_name", "time_zone"}, ...]
    """
    creds = load_credentials(user_key)
    if not creds:
        raise RuntimeError("Not connected to Google Analytics. Call /ga/auth/start first.")

    admin = AnalyticsAdminServiceClient(credentials=creds)
    out: list[dict] = []

    # Walk accounts → list properties per account
    for account in admin.list_account_summaries():
        account_name = account.display_name
        for prop in account.property_summaries:
            # prop.property is "properties/123456789"
            pid = prop.property.split("/")[-1] if prop.property else ""
            out.append({
                "property_id": pid,
                "display_name": prop.display_name,
                "account_name": account_name,
                "parent": account.account,
            })
    return out


# ── Report fetch ──────────────────────────────────────────────────────────────

def _cache_key(property_id: str, start: str, end: str,
               dimensions: list[str], metrics: list[str]) -> str:
    payload = json.dumps({
        "p": property_id, "s": start, "e": end,
        "d": sorted(dimensions), "m": sorted(metrics),
    }, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def fetch_ga4_report(
    property_id: str,
    start_date: str = "90daysAgo",
    end_date: str = "today",
    dimensions: Optional[list[str]] = None,
    metrics: Optional[list[str]] = None,
    user_key: str = "default",
    use_cache: bool = True,
) -> dict:
    """Pull a GA4 report and write it as a CSV suitable for the profiler.

    Returns:
      {
        "csv_path": "<abs path>",
        "row_count": int,
        "col_count": int,
        "property_id": str,
        "date_range": [start, end],
        "columns": [...],
        "cached": bool,
      }
    """
    dimensions = dimensions or DEFAULT_DIMENSIONS
    metrics = metrics or DEFAULT_METRICS

    creds = load_credentials(user_key)
    if not creds:
        raise RuntimeError("Not connected to Google Analytics.")

    key = _cache_key(property_id, start_date, end_date, dimensions, metrics)
    cache_csv = CACHE_DIR / f"{key}.csv"
    if use_cache and cache_csv.exists():
        df = pd.read_csv(cache_csv)
        return {
            "csv_path": str(cache_csv),
            "row_count": len(df),
            "col_count": len(df.columns),
            "property_id": property_id,
            "date_range": [start_date, end_date],
            "columns": df.columns.tolist(),
            "cached": True,
        }

    client = BetaAnalyticsDataClient(credentials=creds)
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        limit=250_000,
    )
    response = client.run_report(request)

    dim_headers = [h.name for h in response.dimension_headers]
    met_headers = [h.name for h in response.metric_headers]
    headers = dim_headers + met_headers

    rows = []
    for row in response.rows:
        dim_vals = [v.value for v in row.dimension_values]
        # Cast metric values to float/int — GA API returns strings
        met_vals = []
        for v in row.metric_values:
            raw = v.value
            try:
                # ints for counters; floats otherwise
                if "." in raw or "e" in raw.lower():
                    met_vals.append(float(raw))
                else:
                    met_vals.append(int(raw))
            except (ValueError, AttributeError):
                met_vals.append(raw)
        rows.append(dim_vals + met_vals)

    df = pd.DataFrame(rows, columns=headers)

    # Normalize `date` column from GA's YYYYMMDD string → ISO date
    if "date" in df.columns:
        try:
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    df.to_csv(cache_csv, index=False, encoding="utf-8")

    return {
        "csv_path": str(cache_csv),
        "row_count": len(df),
        "col_count": len(df.columns),
        "property_id": property_id,
        "date_range": [start_date, end_date],
        "columns": df.columns.tolist(),
        "cached": False,
    }


def ingest_to_session_csv(
    property_id: str,
    session_id: str,
    output_dir: Path,
    start_date: str = "90daysAgo",
    end_date: str = "today",
    dimensions: Optional[list[str]] = None,
    metrics: Optional[list[str]] = None,
    user_key: str = "default",
) -> dict:
    """Fetch GA4 report and copy it to the session's uploads slot.

    Writes to `output_dir/ga4_<property_id>_<hash>.csv` so it matches the
    naming the rest of the pipeline expects.
    """
    result = fetch_ga4_report(
        property_id=property_id,
        start_date=start_date,
        end_date=end_date,
        dimensions=dimensions,
        metrics=metrics,
        user_key=user_key,
    )
    src = Path(result["csv_path"])
    output_dir.mkdir(parents=True, exist_ok=True)
    dst = output_dir / f"ga4_{property_id}_{src.stem}.csv"
    # Copy (not move) so the cache stays warm for reruns
    dst.write_bytes(src.read_bytes())
    result["csv_path"] = str(dst)
    result["original_filename"] = f"GA4 property {property_id} ({start_date} → {end_date}).csv"
    result["session_id"] = session_id
    return result
