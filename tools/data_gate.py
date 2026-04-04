import os
import json
import hashlib
from typing import Optional

import pandas as pd

from tools.config_loader import get_config

def _registry_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)
    return os.path.join(root, "data", "schema_registry.json")

def _load_registry() -> dict:
    path = _registry_path()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_registry(registry: dict) -> None:
    path = _registry_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(registry, f, indent=2)
    except Exception as e:
        print(f"[DataGate] Failed to save schema registry: {e}")

def _col_fingerprint(df: pd.DataFrame) -> dict:
    return {col: str(df[col].dtype) for col in df.columns}

def _fingerprint_hash(fingerprint: dict) -> str:
    blob = json.dumps(fingerprint, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]

def run_preflight_check(
    csv_path: str,
    dataset_type: Optional[str] = None,
) -> dict:
    warnings: list[str] = []
    errors: list[str] = []
    checks: dict = {}
    schema_drift: dict = {"detected": False, "added": [], "removed": []}

    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except Exception as e:
        return {
            "gate_result": "block",
            "checks": {},
            "warnings": [],
            "errors": [f"Cannot read CSV: {e}"],
            "schema_drift": schema_drift,
        }

    n_rows, n_cols = df.shape

    if n_rows == 0:
        errors.append("File has 0 data rows — nothing to analyse.")
    if n_cols == 0:
        errors.append("File has 0 columns — nothing to analyse.")
    checks["sanity"] = {
        "rows": n_rows,
        "cols": n_cols,
        "result": "fail" if (n_rows == 0 or n_cols == 0) else "pass",
    }

    if n_rows == 0 or n_cols == 0:
        return {
            "gate_result": "block",
            "checks": checks,
            "warnings": warnings,
            "errors": errors,
            "schema_drift": schema_drift,
        }

    _dq = get_config()["data_quality"]
    null_pcts = (df.isnull().sum() / n_rows * 100).round(1)
    high_null = null_pcts[null_pcts > _dq["null_warn_pct"]]
    critical_null = null_pcts[null_pcts > _dq["null_critical_pct"]]

    for col in critical_null.index:
        errors.append(
            f"Column '{col}' is {critical_null[col]:.0f}% null — effectively empty. "
            "Remove it or check your export."
        )
    for col in high_null.index:
        if col not in critical_null.index:
            warnings.append(
                f"Column '{col}' is {high_null[col]:.0f}% null. "
                "Analyses using this column may be unreliable."
            )

    checks["null_density"] = {
        "result": "fail" if len(critical_null) > 0 else ("warn" if len(high_null) > 0 else "pass"),
        "high_null_columns": high_null.to_dict(),
        "critical_null_columns": critical_null.to_dict(),
    }

    dup_count = int(df.duplicated().sum())
    dup_pct = round(dup_count / n_rows * 100, 1)

    if dup_pct > _dq["duplicate_warn_pct"]:
        warnings.append(
            f"{dup_pct}% of rows ({dup_count:,}) are exact duplicates. "
            "This may indicate a data export error."
        )
    elif dup_pct > _dq["duplicate_secondary_pct"]:
        warnings.append(
            f"{dup_pct}% of rows ({dup_count:,}) are exact duplicates. "
            "Consider deduplication before analysis."
        )

    checks["duplicates"] = {
        "result": "warn" if dup_pct > _dq["duplicate_secondary_pct"] else "pass",
        "duplicate_count": dup_count,
        "duplicate_pct": dup_pct,
    }

    mixed_type_cols = []
    for col in df.select_dtypes(include=["object"]).columns:
        sample = df[col].dropna().head(_dq["type_check_sample_size"])
        numeric_like = pd.to_numeric(sample, errors="coerce").notna().sum()
        numeric_ratio = numeric_like / max(len(sample), 1)

        if 0.2 < numeric_ratio < 0.8:
            mixed_type_cols.append(col)

    if mixed_type_cols:
        warnings.append(
            f"Columns with mixed types detected: {mixed_type_cols}. "
            "These columns have both numeric and text values and may parse incorrectly."
        )

    checks["type_consistency"] = {
        "result": "warn" if mixed_type_cols else "pass",
        "mixed_type_columns": mixed_type_cols,
    }

    registry = _load_registry()
    registry_key = dataset_type or os.path.splitext(os.path.basename(csv_path))[0]

    current_fp = _col_fingerprint(df)
    current_hash = _fingerprint_hash(current_fp)

    if registry_key in registry:
        prev = registry[registry_key]
        prev_fp = prev.get("fingerprint", {})
        prev_hash = prev.get("hash", "")

        if current_hash != prev_hash:
            added_cols = [c for c in current_fp if c not in prev_fp]
            removed_cols = [c for c in prev_fp if c not in current_fp]
            type_changed = [
                c for c in current_fp
                if c in prev_fp and current_fp[c] != prev_fp[c]
            ]

            if added_cols or removed_cols or type_changed:
                schema_drift = {
                    "detected": True,
                    "added": added_cols,
                    "removed": removed_cols,
                    "type_changed": type_changed,
                    "prev_hash": prev_hash,
                    "curr_hash": current_hash,
                }

                drift_msg_parts = []
                if removed_cols:
                    drift_msg_parts.append(f"REMOVED columns: {removed_cols}")
                if added_cols:
                    drift_msg_parts.append(f"NEW columns: {added_cols}")
                if type_changed:
                    drift_msg_parts.append(f"TYPE CHANGED: {type_changed}")

                warnings.append(
                    f"SCHEMA DRIFT DETECTED vs last run of '{registry_key}'. "
                    + " | ".join(drift_msg_parts) +
                    ". Analyses that used renamed columns will auto-correct or skip."
                )
                checks["schema_drift"] = {"result": "warn", **schema_drift}
            else:
                checks["schema_drift"] = {"result": "pass", "detail": "Schema unchanged."}
    else:
        checks["schema_drift"] = {"result": "pass", "detail": "First run — no previous schema to compare."}

    if errors:
        gate_result = "block"
    elif warnings:
        gate_result = "warn"
    else:
        gate_result = "pass"

    return {
        "gate_result": gate_result,
        "checks": checks,
        "warnings": warnings,
        "errors": errors,
        "schema_drift": schema_drift,
        "row_count": n_rows,
        "col_count": n_cols,
    }

def register_schema(csv_path: str, dataset_type: Optional[str] = None) -> None:
    try:
        df = pd.read_csv(csv_path, nrows=0)
        fp = _col_fingerprint(df)
        registry = _load_registry()
        key = dataset_type or os.path.splitext(os.path.basename(csv_path))[0]
        registry[key] = {
            "fingerprint": fp,
            "hash": _fingerprint_hash(fp),
            "last_successful_run": __import__("datetime").datetime.utcnow().isoformat(),
            "csv_path": csv_path,
        }
        _save_registry(registry)
        print(f"[DataGate] Schema registered for '{key}' ({len(fp)} columns).")
    except Exception as e:
        print(f"[DataGate] register_schema failed (non-fatal): {e}")
