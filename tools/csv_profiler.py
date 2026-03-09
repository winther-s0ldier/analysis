"""
CSV Profiler Tool — reads a CSV and produces a raw factual profile.
NO decision logic. NO metric suggestions. Just facts about the data.
The Discovery Agent reasons over these facts to decide what analyses to run.
"""
import pandas as pd
from typing import Any


def profile_csv(csv_path: str) -> dict:
    """
    Profile a CSV file and return raw facts about every column.
    
    This is a SENSOR — it reports what it sees. It does NOT decide what to do.
    The Discovery Agent (the BRAIN) reads this profile and reasons about it.
    
    Args:
        csv_path: Absolute path to the CSV file.
    
    Returns:
        dict with raw facts: row_count, column_count, columns (per-column stats),
        sample_data, correlations. No suggestions, no metric proposals.
    """
    try:
        df = pd.read_csv(csv_path, low_memory=False)
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

    semantic_map = infer_column_semantics(result)
    classification = classify_dataset(result, semantic_map)
    result.update(classification)
    result["semantic_map"] = semantic_map

    print(f"INFO PROFILER: entity_col={result.get('column_roles',{}).get('entity_col')}")
    print(f"INFO PROFILER: time_col={result.get('column_roles',{}).get('time_col')}")
    print(f"INFO PROFILER: event_col={result.get('column_roles',{}).get('event_col')}")
    print(f"INFO PROFILER: dataset_type={result.get('dataset_type')}")

    return result


def _is_datetime_column(series: pd.Series) -> bool:
    """Heuristic check if a column contains datetime values."""
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
    """
    Takes the raw profile dict from profile_csv().
    Infers what each column REPRESENTS semantically.
    Returns a dict keyed by column name, each value has:
        tag: str, confidence: float, dtype: str,
        unique_count: int, unique_ratio: float

    Semantic tags:
        entity_identifier  → unique ID per person/object
        timestamp          → datetime column
        event_or_state     → what happened (event_name, action)
        outcome_metric     → thing being measured (revenue, score)
        funnel_state       → process stages (pending, completed)
        numeric_measure    → a plain measurement (age, quantity)
        category_label     → a grouping column (region, product)
        text_content       → free text (description, notes)
        unknown            → cannot be determined
    """
    columns = profile.get("columns", [])
    row_count = max(profile.get("row_count", 1), 1)
    semantic_map = {}

    for col in columns:
        name = col["name"].lower()
        dtype = col.get("type_category", "")
        unique_count = col["unique_count"]
        unique_ratio = unique_count / row_count
        sample_vals = [
            str(v).lower()
            for v in col.get("sample_values", [])
        ]

        tag = "unknown"
        confidence = 0.5

        if dtype == "datetime":
            tag = "timestamp"
            confidence = 0.99

        exclude_keywords = ["time", "date", "day", "hour"]
        is_time_related = any(k in name for k in exclude_keywords) or \
                          dtype == "datetime" or \
                          any(":" in v for v in sample_vals)

        if not is_time_related:
            p1_keywords = ["user_id", "user_uuid", "uuid", "user"]
            p2_keywords = ["customer_id", "customer", "userid"]

            if any(k in name for k in p1_keywords):
                tag = "entity_identifier"
                confidence = 0.98
            elif any(k in name for k in p2_keywords):
                tag = "entity_identifier"
                confidence = 0.96
            elif "id" in name:
                tag = "entity_identifier"
                confidence = 0.94
            elif dtype == "categorical" and unique_ratio >= 0.5:
                tag = "entity_identifier"
                confidence = 0.90
            elif dtype == "categorical" and unique_ratio >= 0.3:
                tag = "entity_identifier"
                confidence = 0.72

            elif dtype == "categorical" and 5 < unique_count < 300:
                event_keywords = [
                    'event', 'action', 'activity', 'type', 'name',
                    'category', 'status', 'stage', 'step',
                    'operation', 'method', 'page', 'screen'
                ]
                action_words = [
                    'click', 'view', 'open', 'close', 'login',
                    'search', 'submit', 'load', 'start', 'end',
                    'create', 'update', 'delete', 'install',
                    'session', 'select', 'book', 'pay', 'purchase'
                ]
                keyword_hit = any(kw in name for kw in event_keywords)
                sample_hits = sum(
                    1 for v in sample_vals
                    if any(aw in v for aw in action_words)
                )
                if keyword_hit:
                    tag = "event_or_state"
                    confidence = 0.90
                elif sample_hits >= 2:
                    tag = "event_or_state"
                    confidence = 0.82
                else:
                    tag = "category_label"
                    confidence = 0.68

            elif dtype == "categorical" and unique_count <= 10:
                funnel_words = [
                    'pending', 'active', 'completed', 'failed',
                    'cancelled', 'converted', 'started', 'finished',
                    'abandoned', 'approved', 'rejected', 'processing',
                    'draft', 'published', 'archived', 'open', 'closed'
                ]
                funnel_hits = sum(
                    1 for v in sample_vals
                    if any(fw in v for fw in funnel_words)
                )
                if funnel_hits >= 2:
                    tag = "funnel_state"
                    confidence = 0.88
                else:
                    tag = "category_label"
                    confidence = 0.75

            elif dtype == "numeric":
                outcome_keywords = [
                    'revenue', 'sales', 'amount', 'price', 'cost',
                    'churn', 'rate', 'conversion', 'score', 'rating',
                    'profit', 'value', 'spend', 'income', 'loss',
                    'gain', 'mrr', 'arpu', 'ltv', 'nps', 'gmv',
                    'fee', 'charge', 'payment', 'total', 'sum'
                ]
                if any(kw in name for kw in outcome_keywords):
                    tag = "outcome_metric"
                    confidence = 0.88
                else:
                    tag = "numeric_measure"
                    confidence = 0.80

            elif dtype == "categorical" and unique_ratio > 0.3:
                text_keywords = [
                    'description', 'comment', 'note', 'text',
                    'message', 'content', 'feedback', 'review',
                    'reason', 'detail', 'remark', 'body', 'title'
                ]
                if any(kw in name for kw in text_keywords):
                    tag = "text_content"
                    confidence = 0.90
                else:
                    tag = "category_label"
                    confidence = 0.60

            elif dtype == "categorical":
                tag = "category_label"
                confidence = 0.70

        elif dtype == "categorical" and 5 < unique_count < 300:
            event_keywords = [
                'event', 'action', 'activity', 'type', 'name',
                'category', 'status', 'stage', 'step',
                'operation', 'method', 'page', 'screen'
            ]
            action_words = [
                'click', 'view', 'open', 'close', 'login',
                'search', 'submit', 'load', 'start', 'end',
                'create', 'update', 'delete', 'install',
                'session', 'select', 'book', 'pay', 'purchase'
            ]
            keyword_hit = any(kw in name for kw in event_keywords)
            sample_hits = sum(
                1 for v in sample_vals
                if any(aw in v for aw in action_words)
            )
            if keyword_hit:
                tag = "event_or_state"
                confidence = 0.90
            elif sample_hits >= 2:
                tag = "event_or_state"
                confidence = 0.82
            else:
                tag = "category_label"
                confidence = 0.68

        elif dtype == "categorical" and unique_count <= 10:
            funnel_words = [
                'pending', 'active', 'completed', 'failed',
                'cancelled', 'converted', 'started', 'finished',
                'abandoned', 'approved', 'rejected', 'processing',
                'draft', 'published', 'archived', 'open', 'closed'
            ]
            funnel_hits = sum(
                1 for v in sample_vals
                if any(fw in v for fw in funnel_words)
            )
            if funnel_hits >= 2:
                tag = "funnel_state"
                confidence = 0.88
            else:
                tag = "category_label"
                confidence = 0.75

        elif dtype == "numeric":
            outcome_keywords = [
                'revenue', 'sales', 'amount', 'price', 'cost',
                'churn', 'rate', 'conversion', 'score', 'rating',
                'profit', 'value', 'spend', 'income', 'loss',
                'gain', 'mrr', 'arpu', 'ltv', 'nps', 'gmv',
                'fee', 'charge', 'payment', 'total', 'sum'
            ]
            if any(kw in name for kw in outcome_keywords):
                tag = "outcome_metric"
                confidence = 0.88
            else:
                tag = "numeric_measure"
                confidence = 0.80

        elif dtype == "categorical" and unique_ratio > 0.3:
            text_keywords = [
                'description', 'comment', 'note', 'text',
                'message', 'content', 'feedback', 'review',
                'reason', 'detail', 'remark', 'body', 'title'
            ]
            if any(kw in name for kw in text_keywords):
                tag = "text_content"
                confidence = 0.90
            else:
                tag = "category_label"
                confidence = 0.60

        elif dtype == "categorical":
            tag = "category_label"
            confidence = 0.70

        semantic_map[col["name"]] = {
            "tag": tag,
            "confidence": round(confidence, 3),
            "dtype": dtype,
            "unique_count": unique_count,
            "unique_ratio": round(unique_ratio, 4),
        }

    return semantic_map


def classify_dataset(profile: dict, semantic_map: dict) -> dict:
    """
    Uses the semantic map to classify the overall dataset type
    and identify which column fills each role.

    Dataset types:
        event_log       → entity + timestamp + event
        transactional   → entity + timestamp + outcome/numeric
        time_series     → timestamp + numeric (no entity)
        funnel          → entity + funnel_state
        tabular_generic → doesn't match above patterns

    Returns:
        dataset_type, confidence, column_roles,
        recommended_analyses, ambiguous_columns,
        needs_clarification, tag_summary
    """
    tag_counts: dict = {}
    for col, info in semantic_map.items():
        t = info["tag"]
        tag_counts[t] = tag_counts.get(t, 0) + 1

    has_entity    = tag_counts.get("entity_identifier", 0) > 0
    has_timestamp = tag_counts.get("timestamp", 0) > 0
    has_event     = tag_counts.get("event_or_state", 0) > 0
    has_outcome   = tag_counts.get("outcome_metric", 0) > 0
    has_numeric   = tag_counts.get("numeric_measure", 0) > 0
    has_funnel    = tag_counts.get("funnel_state", 0) > 0

    def best_col(tag: str, min_conf: float = 0.70):
        candidates = [
            (col, info["confidence"])
            for col, info in semantic_map.items()
            if info["tag"] == tag
            and info["confidence"] >= min_conf
        ]
        return max(candidates, key=lambda x: x[1])[0] \
               if candidates else None

    entity_col  = best_col("entity_identifier")
    time_col    = best_col("timestamp")
    event_col   = best_col("event_or_state")
    outcome_col = best_col("outcome_metric")
    funnel_col  = best_col("funnel_state")

    if has_entity and has_timestamp and has_event:
        dataset_type = "event_log"
        confidence = 0.92
        recommended_analyses = [
            "session_detection",
            "funnel_analysis",
            "sequential_pattern_mining",
            "friction_detection",
            "survival_analysis",
            "user_segmentation",
            "association_rules",
            "distribution_analysis",
            "categorical_analysis",
            "missing_data_analysis",
        ]
    elif has_entity and has_timestamp and (has_outcome or has_numeric):
        dataset_type = "transactional"
        confidence = 0.88
        recommended_analyses = [
            "trend_analysis",
            "pareto_analysis",
            "rfm_analysis",
            "cohort_analysis",
            "distribution_analysis",
            "anomaly_detection",
            "correlation_matrix",
            "missing_data_analysis",
        ]
    elif has_timestamp and (has_numeric or has_outcome):
        dataset_type = "time_series"
        confidence = 0.85
        recommended_analyses = [
            "trend_analysis",
            "time_series_decomposition",
            "anomaly_detection",
            "distribution_analysis",
            "correlation_matrix",
            "missing_data_analysis",
        ]
    elif has_funnel and has_entity:
        dataset_type = "funnel"
        confidence = 0.82
        recommended_analyses = [
            "funnel_analysis",
            "distribution_analysis",
            "categorical_analysis",
            "correlation_matrix",
            "missing_data_analysis",
        ]
    else:
        dataset_type = "tabular_generic"
        confidence = 0.70
        recommended_analyses = [
            "distribution_analysis",
            "categorical_analysis",
            "correlation_matrix",
            "anomaly_detection",
            "missing_data_analysis",
        ]

    ambiguous = [
        col for col, info in semantic_map.items()
        if info["confidence"] < 0.70
    ]

    return {
        "dataset_type": dataset_type,
        "confidence": confidence,
        "column_roles": {
            "entity_col":  entity_col,
            "time_col":    time_col,
            "event_col":   event_col,
            "outcome_col": outcome_col,
            "funnel_col":  funnel_col,
        },
        "recommended_analyses": recommended_analyses,
        "ambiguous_columns": ambiguous,
        "needs_clarification": (
            confidence < 0.75 or len(ambiguous) > 2
        ),
        "tag_summary": tag_counts,
    }
