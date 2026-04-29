import os
import pandas as pd
from typing import Any

# Module-level DataFrame cache: csv_path -> (mtime, DataFrame)
# Avoids re-reading the same file multiple times within a session.
_df_cache: dict[str, tuple[float, pd.DataFrame]] = {}


def _load_df(csv_path: str) -> pd.DataFrame:
    """Return a cached DataFrame for csv_path, re-reading only if the file changed."""
    try:
        mtime = os.path.getmtime(csv_path)
    except OSError:
        mtime = 0.0
    cached = _df_cache.get(csv_path)
    if cached is not None and cached[0] == mtime:
        return cached[1]
    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, low_memory=False, encoding="latin-1")
    _df_cache[csv_path] = (mtime, df)
    return df


def clear_df_cache(csv_path: str) -> None:
    """Remove a cached DataFrame entry (call on session expiry to free memory)."""
    _df_cache.pop(csv_path, None)


def profile_csv(csv_path: str, schema_map: dict | None = None) -> dict:
    """Profile a CSV file.

    Optional ``schema_map`` is a ``{event_name_lower: description}`` dict
    sourced from a companion data-dictionary CSV uploaded alongside the
    data file (workaround for the single-file upload constraint). When
    provided AND the data has an ``event_name`` column, the result will
    include an ``event_name_descriptions`` map covering only the event
    values that actually appear in the data, plus a ``schema_coverage``
    ratio. The dict is metadata only — never used to rename columns or
    mutate data.
    """
    try:
        df = _load_df(csv_path)
    except Exception as e:
        return {"error": f"Failed to read CSV: {str(e)}"}

    columns_info = []
    numeric_cols = []
    categorical_cols = []
    datetime_cols = []

    for col in df.columns:
        col_info = {
            "name": col,
            "dtype": str(df[col].dtype),
            "non_null_count": int(df[col].notna().sum()),
            "null_count": int(df[col].isna().sum()),
            "null_percentage": round(float(df[col].isna().mean() * 100), 2),
            "unique_count": int(df[col].nunique()),
            "sample_values": [
                str(v)[:80] for v in df[col].dropna().head(8).tolist()
            ],
        }

        if pd.api.types.is_numeric_dtype(df[col]):
            col_info["type_category"] = "numeric"
            if not df[col].isna().all():
                col_info["stats"] = {
                    "mean": round(float(df[col].mean()), 4),
                    "median": round(float(df[col].median()), 4),
                    "std": round(float(df[col].std()), 4),
                    "min": float(df[col].min()),
                    "max": float(df[col].max()),
                    "q25": round(float(df[col].quantile(0.25)), 4),
                    "q75": round(float(df[col].quantile(0.75)), 4),
                }
            numeric_cols.append(col)

        elif _is_datetime_column(df[col]):
            col_info["type_category"] = "datetime"
            try:
                parsed = pd.to_datetime(df[col], errors="coerce", format="mixed")
                valid = parsed.dropna()
                if len(valid) > 0:
                    col_info["stats"] = {
                        "min_date": str(valid.min()),
                        "max_date": str(valid.max()),
                        "date_range_days": int((valid.max() - valid.min()).days),
                    }
            except Exception:
                col_info["stats"] = {}
            datetime_cols.append(col)

        else:
            col_info["type_category"] = "categorical"
            top_values = df[col].value_counts().head(10)
            col_info["stats"] = {
                "top_values": {
                    str(k)[:60]: int(v) for k, v in top_values.items()
                },
                "cardinality_ratio": round(
                    df[col].nunique() / max(len(df), 1), 4
                ),
            }
            categorical_cols.append(col)

        columns_info.append(col_info)

    correlations = []
    if len(numeric_cols) >= 2:
        try:
            corr_matrix = df[numeric_cols].corr()
            for i, c1 in enumerate(numeric_cols):
                for c2 in numeric_cols[i + 1:]:
                    corr_val = corr_matrix.loc[c1, c2]
                    if abs(corr_val) > 0.4:
                        correlations.append({
                            "columns": [c1, c2],
                            "correlation": round(float(corr_val), 4),
                        })
        except Exception:
            pass

    result = {
        "filename": csv_path.split("\\")[-1].split("/")[-1],
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": columns_info,
        "column_types": {
            "numeric": numeric_cols,
            "categorical": categorical_cols,
            "datetime": datetime_cols,
        },
        "sample_rows": df.head(5).astype(str).to_dict(orient="records"),
        "correlations": correlations,
        "memory_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
    }

    # Attach companion data-dictionary descriptions, if available. We only
    # surface descriptions for event_name values that actually appear in
    # the data, capped at 500, to keep downstream prompts focused.
    if schema_map and "event_name" in df.columns:
        try:
            distinct_vals = (
                df["event_name"].dropna().astype(str).unique().tolist()
            )
            distinct_vals = distinct_vals[:500]
            descriptions: dict[str, str] = {}
            for val in distinct_vals:
                key = val.strip().lower()
                if key in schema_map:
                    descriptions[val] = schema_map[key]
            if distinct_vals:
                result["event_name_descriptions"] = descriptions
                result["schema_coverage"] = round(
                    len(descriptions) / max(len(distinct_vals), 1), 4
                )
        except Exception as _se:
            print(f"WARNING: schema description attach failed: {_se}")

    return result

def _is_datetime_column(series: pd.Series) -> bool:
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    if series.dtype == object:
        sample = series.dropna().head(20)
        if len(sample) == 0:
            return False
        try:
            pd.to_datetime(sample)
            return True
        except Exception:
            return False
    return False

def infer_column_semantics(profile: dict) -> dict:
    return {}

def classify_dataset(profile: dict, semantic_map: dict) -> dict:
    return {
        "dataset_type": "tabular_generic",
        "confidence": 0.0,
        "column_roles": {},
        "recommended_analyses": [],
        "ambiguous_columns": [],
        "needs_clarification": True,
        "tag_summary": {},
    }
