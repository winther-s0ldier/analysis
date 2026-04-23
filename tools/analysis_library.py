import math
import pandas as pd
import numpy as np
import os
from collections import Counter, defaultdict
from typing import Optional, List
from datetime import datetime
from pipeline_types import AnalysisResult
import warnings
warnings.filterwarnings('ignore')

def _json_safe(obj):
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(x) for x in obj]

    if hasattr(obj, 'item'):
        try:
            obj = obj.item()
        except Exception:
            return str(obj)
    if obj is None:
        return None
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, (bool, int, str)):
        return obj

    s = str(obj)
    if s in ("<NA>", "nan", "NaN", "inf", "-inf", "Inf"):
        return None

    return s

DEFAULT_SESSION_MARKERS = {

    "started", "start", "begin", "initiated", "opened",
    "created", "initialized", "launched", "connected", "activated",

    "Session Started", "Journey Started", "App Installed",
    "User Login", "app_start", "login", "landing_page_view",
}

LIBRARY_REGISTRY = {
    "distribution_analysis": {
        "function":      "run_distribution_analysis",
        "required_args": ["csv_path", "col"],
        "col_role":      "any_numeric",
        "description":   "Deep-dives into the distribution of a numeric column: histogram, box plot, skewness, kurtosis, and outlier detection (IQR + Z-score). Flags non-normal distributions and extreme values that could skew downstream calculations.",
    },
    "categorical_analysis": {
        "function":      "run_categorical_analysis",
        "required_args": ["csv_path", "col"],
        "col_role":      "any_categorical",
        "description":   "Frequency table, Pareto curve, and entropy score for a categorical column. Identifies dominant categories, the 80/20 distribution split, and whether one value is crowding out all others — a common sign of data quality issues or product concentration risk.",
    },
    "correlation_matrix": {
        "function":      "run_correlation_matrix",
        "required_args": ["csv_path"],
        "col_role":      None,
        "description":   "Pearson + Spearman correlation matrix across all numeric columns. Surfaces hidden relationships between metrics — e.g. does session length correlate with conversion? Strong correlations become the backbone of intervention hypotheses.",
    },
    "anomaly_detection": {
        "function":      "run_anomaly_detection",
        "required_args": ["csv_path", "col"],
        "col_role":      "any_numeric",
        "description":   "IQR, Z-score, and Isolation Forest consensus outlier detection. Flags data points that deviate significantly from the norm — useful for catching erroneous records, bot traffic, or genuinely anomalous user behaviour.",
    },
    "missing_data_analysis": {
        "function":      "run_missing_data_analysis",
        "required_args": ["csv_path"],
        "col_role":      None,
        "description":   "Scans every column for missing values and flags systematic patterns. High missingness in key columns (event, timestamp, user ID) can silently corrupt behavioural analyses — this runs first to surface those risks.",
    },
    "trend_analysis": {
        "function":      "run_trend_analysis",
        "required_args": ["csv_path", "time_col", "value_col"],
        "col_role":      "time_and_value",
        "description":   "Rolling averages, Mann-Kendall significance test, and changepoint detection on any time-series column. Answers: is a key metric improving or declining? When did it change? Statistical significance prevents false alarms from noise.",
    },
    "time_series_decomposition": {
        "function":      "run_time_series_decomposition",
        "required_args": ["csv_path", "time_col", "value_col"],
        "col_role":      "time_and_value",
        "description":   "STL decomposition separating your time-series into trend, seasonality, and residual noise. Isolates whether a metric change is a genuine trend or a predictable seasonal pattern — critical before attributing changes to product decisions.",
    },
    "cohort_analysis": {
        "function":      "run_cohort_analysis",
        "required_args": ["csv_path", "entity_col", "time_col", "value_col"],
        "optional_args": ["cohort_window"],
        "col_role":      "entity_time_value",
        "description":   "Groups users by their first-seen date (cohort) and tracks their activity in subsequent periods. Measures whether users acquired in different time windows behave differently. A declining cohort curve is the earliest signal of product-market fit erosion. Optional: cohort_window='W' for weekly, 'D' for daily, 'Q' for quarterly (default 'M' monthly).",
    },
    "session_detection": {
        "function":      "run_session_detection",
        "required_args": ["csv_path", "entity_col", "time_col"],
        "col_role":      "entity_and_time",
        "description":   "Infers session boundaries from raw event logs using time-gap heuristics. Groups events into discrete sessions per user. MUST run first — all downstream behavioural analyses (funnel, friction, dropout, path) depend on the session_id column this produces.",
    },
    "funnel_analysis": {
        "function":      "run_funnel_analysis",
        "required_args": ["csv_path", "entity_col", "event_col", "time_col"],
        "col_role":      "behavioral",
        "description":   "Calculates step-by-step conversion rates through your key user flow. Answers: what % of users reach each stage? Where is the biggest drop-off? Quantifies the exact size of each bottleneck in terms of user count and estimated revenue impact.",
    },
    "friction_detection": {
        "function":      "run_friction_detection",
        "required_args": ["csv_path", "entity_col", "event_col"],
        "col_role":      "behavioral",
        "description":   "Detects events that users repeat abnormally often within a session — a strong signal of UI friction or confusion. A user tapping 'search' 8 times is not engaged, they are stuck. Outputs a friction score per event to prioritise fixes.",
    },
    "survival_analysis": {
        "function":      "run_survival_analysis",
        "required_args": ["csv_path", "entity_col", "event_col"],
        "col_role":      "behavioral",
        "description":   "Kaplan-Meier survival curve showing what fraction of sessions are still active after N events. Identifies the 'half-life' of a typical session and the event depth at which users most commonly abandon.",
    },
    "user_segmentation": {
        "function":      "run_user_segmentation",
        "required_args": ["csv_path", "entity_col", "event_col", "time_col"],
        "col_role":      "behavioral",
        "description":   "DBSCAN clustering on behavioural event vectors to segment users into distinct groups without needing predefined labels. Surfaces natural user archetypes that share similar interaction patterns — beyond simple demographics.",
    },
    "sequential_pattern_mining": {
        "function":      "run_sequential_pattern_mining",
        "required_args": ["csv_path", "entity_col", "event_col"],
        "col_role":      "behavioral",
        "description":   "PrefixSpan algorithm to find the most common ordered event sequences across all sessions. Reveals the top 'mini-journeys' users take — e.g. '80% of users who viewed bus_list also tapped select_seat within 2 events.' Drives personalisation and UX ordering decisions.",
    },
    "association_rules": {
        "function":      "run_association_rules",
        "required_args": ["csv_path", "entity_col", "event_col"],
        "col_role":      "behavioral",
        "description":   "Mines IF-THEN association rules from co-occurring events: 'users who did X also did Y with Z% confidence.' Converts raw event co-occurrence into actionable intervention triggers for push notifications, in-app prompts, or onboarding nudges.",
    },
    "pareto_analysis": {
        "function":      "run_pareto_analysis",
        "required_args": ["csv_path", "entity_col", "value_col"],
        "col_role":      "category_and_value",
        "description":   "80/20 Pareto analysis showing which categories drive the majority of a numeric value. Answers: which 20% of events account for 80% of drop-offs? Which product segments generate 80% of revenue? Directs engineering and product prioritisation.",
    },
    "rfm_analysis": {
        "function":      "run_rfm_analysis",
        "required_args": ["csv_path", "entity_col", "time_col", "value_col"],
        "col_role":      "entity_time_value",
        "description":   "RFM (Recency, Frequency, Monetary) segmentation classifying entities into value tiers based on how recently, how often, and how much they engage. Each tier highlights distinct engagement patterns and intervention opportunities.",
    },
    "transition_analysis": {
        "function":      "run_transition_analysis",
        "required_args": ["csv_path", "entity_col", "event_col", "time_col"],
        "col_role":      "behavioral",
        "description":   "Markov transition matrix: P(next|current), exit probabilities, dead-end events, loops.",
    },
    "dropout_analysis": {
        "function":      "run_dropout_analysis",
        "required_args": ["csv_path", "entity_col", "event_col"],
        "col_role":      "behavioral",
        "description":   "Last N events before session end. Common dropout sequences, early exit rates.",
    },
    "user_journey_analysis": {
        "function":      "run_user_journey_analysis",
        "required_args": ["csv_path", "entity_col", "event_col"],
        "col_role":      "behavioral",
        "description":   "Per-user journey progression and entry/exit patterns.",
    },
    "event_taxonomy": {
        "function":      "run_event_taxonomy",
        "required_args": ["csv_path", "event_col"],
        "col_role":      "any_categorical",
        "description":   "Auto-classify events into functional categories using domain-agnostic keyword matching.",
    },
    "contribution_analysis": {
        "function":      "run_contribution_analysis",
        "required_args": ["csv_path", "group_col", "value_col"],
        "col_role":      "category_and_value",
        "description":   "Calculate % contribution of each group to total value, plus variance.",
    },
    "cross_tab_analysis": {
        "function":      "run_cross_tab_analysis",
        "required_args": ["csv_path", "col_a", "col_b"],
        "col_role":      "two_categoricals",
        "description":   "Chi-squared test and Cramér's V to measure relationship between two categorical variables.",
    },
    "intervention_triggers": {
        "function":      "run_intervention_triggers",
        "required_args": ["csv_path", "entity_col", "event_col", "time_col"],
        "col_role":      "behavioral",
        "description":   "High-confidence dropout trigger rules: events that reliably precede process or session abandonment (>80% dropout rate after the event). Produces a ranked list of risk-level triggers with exact dropout rates.",
    },
    "session_classification": {
        "function":      "run_session_classification",
        "required_args": ["csv_path", "entity_col", "event_col", "time_col"],
        "col_role":      "behavioral",
        "description":   "Classifies entities into behavioural archetypes based on session depth, event diversity, and outcome signals. Reveals which engagement tiers exist and where the largest volume of incomplete journeys occurs.",
    },
    "path_analysis": {
        "function":      "run_user_journey_analysis",
        "required_args": ["csv_path", "entity_col", "event_col"],
        "col_role":      "behavioral",
        "description":   "Maps the most common entity paths through the event sequence. Identifies where paths diverge, which entry points lead to completion, and which routes end in abandonment.",
    },
    "retention_analysis": {
        "function":      "run_cohort_analysis",
        "required_args": ["csv_path", "entity_col", "time_col", "value_col"],
        "col_role":      "entity_time_value",
        "description":   "Measures how many users return on subsequent days/weeks after their first session. Segments users into cohorts by acquisition date and tracks their activity over time. The primary metric for product stickiness — a drop in day-7 or day-30 retention directly translates to revenue loss.",
    },
    "time_to_event_analysis": {
        "function":      "run_trend_analysis",
        "required_args": ["csv_path", "time_col", "value_col"],
        "col_role":      "time_and_value",
        "description":   "Measures how long users take to reach a critical event (e.g. first purchase, first booking completion). Identifies whether time-to-conversion is increasing or decreasing, and flags sessions where excessive time indicates friction or confusion in the flow.",
    },
}

def _reliability_label(confidence: float, status: str = "success") -> str:
    """Bucket a numeric confidence into a human-readable label.

    The label is what the report shows as a badge — grounded in the same
    computed number, but easier to read than a raw percentage and harder
    to over-interpret as false precision.
    """
    if status != "success":
        return "failed"
    if confidence >= 0.80:
        return "strong"
    if confidence >= 0.60:
        return "suggestive"
    return "tentative"


def _make_result(analysis_type: str, data: dict,
                  top_finding: str, severity: str,
                  confidence: float,
                  chart_ready_data: dict,
                  enables: list = None) -> dict:
    return _json_safe({
        "analysis_type": analysis_type,
        "status": "success",
        "data": data,
        "top_finding": top_finding,
        "severity": severity,
        "confidence": confidence,
        "reliability_label": _reliability_label(confidence, "success"),
        "chart_ready_data": chart_ready_data,
        "enables": enables or [],
        "insight_summary": {
            "key_finding": top_finding,
            "top_values": "",
            "anomalies": "",
            "recommendation": ""
        }
    })

def compute_confidence(n: int, null_rate: float = 0.0,
                       p_value: float = None, effect_size: float = None,
                       base: float = 0.85) -> float:

    size_c = 0.55 + 0.43 * (1.0 - math.exp(-n / 1500))

    null_c = max(0.30, 1.0 - float(null_rate) * 0.70)

    if p_value is None:
        p_c = base
    elif p_value < 0.01:
        p_c = 1.00
    elif p_value < 0.05:
        p_c = 0.88
    elif p_value < 0.10:
        p_c = 0.72
    else:
        p_c = 0.48

    if effect_size is None:
        e_c = base
    else:
        abs_e = abs(float(effect_size))
        if abs_e >= 0.8:
            e_c = 1.00
        elif abs_e >= 0.5:
            e_c = 0.88
        elif abs_e >= 0.2:
            e_c = 0.74
        else:
            e_c = 0.55

    score = (
        base   * 1.0 +
        size_c * 1.8 +
        null_c * 0.8 +
        p_c    * 1.0 +
        e_c    * 0.6
    ) / (1.0 + 1.8 + 0.8 + 1.0 + 0.6)

    return round(max(0.10, min(0.99, score)), 3)

def _make_error_result(analysis_type: str, error: str, status: str = "error") -> dict:
    return _json_safe({
        "analysis_type": analysis_type,
        "status": status,
        "data": {},
        "top_finding": error,
        "severity": "info",
        "confidence": 0.0,
        "reliability_label": _reliability_label(0.0, status),
        "chart_ready_data": {},
        "enables": [],
        "insight_summary": {
            "key_finding": error,
            "top_values": "",
            "anomalies": "",
            "recommendation": ""
        },
        "error": error,
    })

def run_distribution_analysis(csv_path: str, col: str) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if col not in df.columns:
        return _make_error_result("distribution_analysis", f"Column '{col}' not found")

    data = df[col].dropna()
    if len(data) == 0:
        return _make_error_result("distribution_analysis", f"Column '{col}' has no non-null values", "insufficient_data")

    stats = {
        "count": len(data),
        "mean": round(float(data.mean()), 4),
        "median": round(float(data.median()), 4),
        "std": round(float(data.std()), 4),
        "min": float(data.min()),
        "max": float(data.max()),
        "q25": round(float(data.quantile(0.25)), 4),
        "q75": round(float(data.quantile(0.75)), 4),
        "skewness": round(float(data.skew()), 4),
        "kurtosis": round(float(data.kurtosis()), 4),
    }

    iqr = stats["q75"] - stats["q25"]
    lower = stats["q25"] - 1.5 * iqr
    upper = stats["q75"] + 1.5 * iqr
    outliers = data[(data < lower) | (data > upper)]
    outlier_pct = round(len(outliers) / len(data) * 100, 2)

    stats["outlier_count"] = len(outliers)
    stats["outlier_pct"] = outlier_pct
    stats["outlier_lower_bound"] = round(lower, 4)
    stats["outlier_upper_bound"] = round(upper, 4)

    z_scores = np.abs((data - data.mean()) / data.std())
    z_outliers = data[z_scores > 3]
    stats["z_outlier_count"] = len(z_outliers)

    try:
        from scipy import stats as scipy_stats
        sample = data.sample(min(500, len(data)),
                              random_state=42)
        stat, p_value = scipy_stats.shapiro(sample)
        stats["normality_p_value"] = round(float(p_value), 6)
        stats["is_normal"] = p_value > 0.05

        ci = scipy_stats.t.interval(
            confidence=0.95,
            df=len(data) - 1,
            loc=float(data.mean()),
            scale=scipy_stats.sem(data),
        )
        stats["mean_ci_95"] = [round(float(ci[0]), 4), round(float(ci[1]), 4)]
    except Exception:
        stats["normality_p_value"] = None
        stats["is_normal"] = None
        stats["mean_ci_95"] = None

    hist, bin_edges = np.histogram(data, bins=30)

    severity = "high" if outlier_pct > 10 else \
               "medium" if outlier_pct > 5 else "low"

    _ci = stats.get("mean_ci_95")
    _ci_str = f" (95% CI [{_ci[0]}, {_ci[1]}])" if _ci else ""
    _norm = stats.get("is_normal")
    if _norm is None:
        _normal_str = "normality unknown"
    elif bool(_norm):
        _normal_str = "normally distributed"
    else:
        _normal_str = "not normally distributed"
    top_finding = (
        f"{col}: mean={stats['mean']}{_ci_str}, "
        f"median={stats['median']}, "
        f"std={stats['std']}. "
        f"{outlier_pct}% outliers detected "
        f"({_normal_str})."
    )

    _null_rate = (len(df[col]) - len(data)) / max(len(df[col]), 1)
    _p_val = stats.get("normality_p_value")
    return _make_result(
        analysis_type="distribution",
        data=stats,
        top_finding=top_finding,
        severity=severity,
        confidence=compute_confidence(n=len(data), null_rate=_null_rate, p_value=_p_val, base=0.90),
        chart_ready_data={
            "type": "histogram_box",
            "col": col,
            "hist_values": hist.tolist(),
            "bin_edges": [round(e, 4) for e in bin_edges.tolist()],
            "box_stats": {
                "min": stats["min"],
                "q25": stats["q25"],
                "median": stats["median"],
                "q75": stats["q75"],
                "max": stats["max"],
                "outliers": outliers.head(50).tolist()
            }
        }
    )

def run_categorical_analysis(csv_path: str, col: str) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if col not in df.columns:
        return _make_error_result("categorical_analysis", f"Column '{col}' not found")

    counts = df[col].value_counts()
    total = len(df[col].dropna())

    if total == 0:
        return _make_error_result("categorical_analysis", "No non-null values", "insufficient_data")

    top_20_pct_count = max(1, int(len(counts) * 0.20))
    top_20_volume = counts.head(top_20_pct_count).sum()
    pareto_ratio = round(top_20_volume / total * 100, 1)

    probs = counts / total
    entropy = round(float(-np.sum(probs * np.log2(probs + 1e-10))), 4)
    max_entropy = round(np.log2(len(counts)), 4)
    entropy_ratio = round(entropy / max_entropy, 4) \
                    if max_entropy > 0 else 0

    top_10 = {str(k): int(v) for k, v in counts.head(10).items()}

    top_finding = (
        f"{col}: {len(counts)} unique values. "
        f"Top 20% of categories account for {pareto_ratio}% of records. "
        f"Distribution entropy: {entropy_ratio:.0%} "
        f"({'evenly spread' if entropy_ratio > 0.8 else 'highly concentrated'})."
    )

    return _make_result(
        analysis_type="categorical",
        data={
            "unique_count": int(len(counts)),
            "top_10": top_10,
            "pareto_ratio": pareto_ratio,
            "entropy": entropy,
            "entropy_ratio": entropy_ratio,
            "null_count": int(df[col].isna().sum()),
        },
        top_finding=top_finding,
        severity="info",
        confidence=compute_confidence(
            n=total,
            null_rate=df[col].isna().sum() / max(len(df[col]), 1),
            base=0.92,
        ),
        chart_ready_data={
            "type": "frequency_bar",
            "col": col,
            "labels": list(top_10.keys()),
            "values": list(top_10.values()),
        }
    )

def run_correlation_matrix(csv_path: str,
                             cols: list = None) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if cols:
        numeric_df = df[cols].select_dtypes(include=[np.number])
    else:
        numeric_df = df.select_dtypes(include=[np.number])

    if numeric_df.shape[1] < 2:
        return _make_error_result("correlation_matrix", "Need at least 2 numeric columns", "insufficient_data")

    pearson = numeric_df.corr(method='pearson')

    _n = len(numeric_df.dropna())
    def _corr_pvalue(r, n):
        import math
        if n <= 2 or abs(r) >= 1.0:
            return 1.0
        t_stat = r * math.sqrt((n - 2) / max(1 - r ** 2, 1e-15))
        try:
            from scipy import stats as _sc
            return float(_sc.t.sf(abs(t_stat), df=n - 2) * 2)
        except Exception:
            return 1.0

    notable = []
    cols_list = list(pearson.columns)
    raw_pvals = []
    raw_pairs = []
    for i in range(len(cols_list)):
        for j in range(i + 1, len(cols_list)):
            val = pearson.iloc[i, j]
            pv = _corr_pvalue(float(val), _n)
            raw_pvals.append(pv)
            raw_pairs.append((i, j, float(val)))

    m = len(raw_pvals)
    bonf_pvals = [min(p * m, 1.0) for p in raw_pvals]

    for idx, (i, j, val) in enumerate(raw_pairs):
        if abs(val) > 0.4:
            notable.append({
                "col1": cols_list[i],
                "col2": cols_list[j],
                "pearson": round(val, 4),
                "p_value": round(raw_pvals[idx], 6),
                "p_value_bonferroni": round(bonf_pvals[idx], 6),
                "significant": bonf_pvals[idx] < 0.05,
                "strength": "strong" if abs(val) > 0.7 else "moderate",
                "direction": "positive" if val > 0 else "negative",
            })

    notable.sort(key=lambda x: abs(x["pearson"]), reverse=True)

    vif_data = []
    try:
        if numeric_df.shape[1] >= 3 and len(numeric_df.dropna()) > numeric_df.shape[1]:
            from statsmodels.stats.outliers_influence import variance_inflation_factor as _vif
            _clean = numeric_df.dropna()
            _vals = _clean.values
            for _k, _col in enumerate(cols_list):
                vif_data.append({
                    "col": _col,
                    "vif": round(float(_vif(_vals, _k)), 3),
                })
    except Exception:
        vif_data = []

    sig_notable = [x for x in notable if x.get("significant", True)]
    top_finding = (
        f"Correlation analysis on {len(cols_list)} numeric columns. "
        f"{len(notable)} notable correlations found (|r| > 0.4); "
        f"{len(sig_notable)} survive Bonferroni correction. "
        + (f"Strongest: {notable[0]['col1']} \u2194 {notable[0]['col2']} "
           f"(r={notable[0]['pearson']}, p={notable[0]['p_value']:.4f})"
           if notable else "No strong correlations detected.")
    )
    high_vif = [v for v in vif_data if v["vif"] > 5]

    return _make_result(
        analysis_type="correlation",
        data={
            "columns": cols_list,
            "pearson_matrix": {
                col: {c: round(float(v), 4)
                      for c, v in pearson[col].items()}
                for col in pearson.columns
            },
            "notable_correlations": notable[:10],
            "vif": vif_data,
            "high_vif_cols": high_vif,
        },
        top_finding=top_finding,
        severity="medium" if notable else "low",
        confidence=compute_confidence(
            n=_n,
            null_rate=numeric_df.isnull().mean().mean(),
            p_value=notable[0]["p_value"] if notable else None,
            effect_size=notable[0]["pearson"] if notable else None,
            base=0.90,
        ),
        chart_ready_data={
            "type": "correlation_heatmap",
            "columns": cols_list,
            "matrix": [[round(float(pearson.iloc[i, j]), 4)
                        for j in range(len(cols_list))]
                       for i in range(len(cols_list))],
        }
    )

def run_anomaly_detection(csv_path: str, col: str) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if col not in df.columns:
        return _make_error_result("anomaly_detection", f"Column '{col}' not found")

    data = df[col].dropna()
    if len(data) == 0:
        return _make_error_result("anomaly_detection", "Insufficient data", "insufficient_data")

    q1 = float(data.quantile(0.25))
    q3 = float(data.quantile(0.75))
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    iqr_outliers = data[(data < lower) | (data > upper)]

    z_scores = np.abs((data - data.mean()) / (data.std() + 1e-10))
    z_outliers = data[z_scores > 3]

    iso_outlier_indices = []
    try:
        from sklearn.ensemble import IsolationForest
        iso = IsolationForest(contamination=0.05,
                               random_state=42)
        preds = iso.fit_predict(data.values.reshape(-1, 1))
        iso_outlier_indices = data.index[preds == -1].tolist()
    except Exception:
        pass

    iqr_set = set(iqr_outliers.index.tolist())
    z_set = set(z_outliers.index.tolist())
    iso_set = set(iso_outlier_indices)

    consensus = list(iqr_set & (z_set | iso_set))

    outlier_pct = round(len(consensus) / len(data) * 100, 2) \
                  if len(data) > 0 else 0

    severity = "critical" if outlier_pct > 15 else \
               "high" if outlier_pct > 8 else \
               "medium" if outlier_pct > 3 else "low"

    top_finding = (
        f"Anomaly detection on '{col}': "
        f"{len(consensus)} consensus outliers "
        f"({outlier_pct}% of data). "
        f"Boundaries: [{round(lower,2)}, {round(upper,2)}]. "
        f"Method agreement: IQR={len(iqr_set)}, "
        f"Z-score={len(z_set)}, "
        f"IsoForest={len(iso_set)}."
    )

    _null_rate = (len(df[col]) - len(data)) / max(len(df[col]), 1)
    return _make_result(
        analysis_type="anomaly_detection",
        data={
            "col": col,
            "total_points": len(data),
            "iqr_outliers": len(iqr_set),
            "z_outliers": len(z_set),
            "iso_outliers": len(iso_set),
            "consensus_outliers": len(consensus),
            "outlier_pct": outlier_pct,
            "lower_bound": round(lower, 4),
            "upper_bound": round(upper, 4),
            "top_outlier_values": sorted(
                [float(data[i]) for i in consensus[:20]],
                reverse=True
            ) if consensus else [],
        },
        top_finding=top_finding,
        severity=severity,
        confidence=compute_confidence(n=len(data), null_rate=_null_rate, base=0.88),
        chart_ready_data={
            "type": "anomaly_scatter",
            "col": col,
            "all_values": data.tolist()[:2000],
            "outlier_indices": consensus[:100],
            "lower_bound": round(lower, 4),
            "upper_bound": round(upper, 4),
        }
    )

def run_missing_data_analysis(csv_path: str) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    missing_stats = []
    for col in df.columns:
        null_count = int(df[col].isna().sum())
        if null_count > 0:
            missing_stats.append({
                "column": col,
                "null_count": null_count,
                "null_pct": round(null_count / len(df) * 100, 2),
                "severity": "critical" if null_count/len(df) > 0.3
                            else "high" if null_count/len(df) > 0.1
                            else "medium"
            })

    missing_stats.sort(key=lambda x: x["null_pct"], reverse=True)
    total_cells = df.shape[0] * df.shape[1]
    total_missing = int(df.isna().sum().sum())
    overall_pct = round(total_missing / total_cells * 100, 2)

    top_finding = (
        f"Missing data: {overall_pct}% of all cells are null "
        f"({total_missing:,} cells). "
        f"{len(missing_stats)} columns have missing values. "
        + (f"Most affected: '{missing_stats[0]['column']}' "
           f"({missing_stats[0]['null_pct']}% missing)"
           if missing_stats else "No missing data detected.")
    )

    severity = "critical" if overall_pct > 20 else \
               "high" if overall_pct > 10 else \
               "medium" if overall_pct > 2 else "low"

    return _make_result(
        analysis_type="missing_data",
        data={
            "total_cells": total_cells,
            "total_missing": total_missing,
            "overall_pct": overall_pct,
            "columns_with_missing": missing_stats,
            "complete_rows": int(df.dropna().shape[0]),
            "complete_rows_pct": round(
                df.dropna().shape[0] / len(df) * 100, 2
            ),
        },
        top_finding=top_finding,
        severity=severity,
        # missing_data: reliability scales with total sample size, not a magic 1.0.
        # null_rate is the finding itself, so it doesn't penalise confidence here.
        confidence=compute_confidence(n=int(total_cells), null_rate=0.0, base=0.90),
        chart_ready_data={
            "type": "missing_bar",
            "columns": [s["column"] for s in missing_stats[:15]],
            "null_pcts": [s["null_pct"] for s in missing_stats[:15]],
        }
    )

def run_trend_analysis(
    csv_path: str,
    time_col: str,
    value_col: str,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if time_col not in df.columns:
        return _make_error_result("trend_analysis", f"Column '{time_col}' not found")
    if value_col not in df.columns:
        return _make_error_result("trend_analysis", f"Column '{value_col}' not found")

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col, value_col])
    df = df.sort_values(time_col)

    if len(df) < 3:
        return _make_error_result("trend_analysis", "Need at least 3 data points", "insufficient_data")

    values = df[value_col].astype(float)
    times  = df[time_col]

    w7  = min(7,  len(df))
    w30 = min(30, len(df))
    roll7  = values.rolling(window=w7,  min_periods=1).mean()
    roll30 = values.rolling(window=w30, min_periods=1).mean()

    x = np.arange(len(values))
    try:
        slope, intercept = np.polyfit(x, values, 1)
        trend_direction = (
            "upward"   if slope > 0 else
            "downward" if slope < 0 else
            "flat"
        )
        trend_strength = abs(slope)
    except Exception:
        slope = 0.0
        trend_direction = "flat"
        trend_strength  = 0.0

    mk_result = None
    try:
        from scipy import stats as scipy_stats
        tau, p_value = scipy_stats.kendalltau(x, values)
        mk_result = {
            "tau":         round(float(tau), 4),
            "p_value":     round(float(p_value), 6),
            "significant": p_value < 0.05,
        }
    except Exception:
        pass

    changepoints = []
    if len(values) >= 10:
        window = max(5, len(values) // 10)
        for i in range(window, len(values) - window):
            left  = values.iloc[i - window:i]
            right = values.iloc[i:i + window]
            if abs(right.mean() - left.mean()) > values.std():
                changepoints.append({
                    "index": int(i),
                    "time":  str(times.iloc[i]),
                    "shift": round(
                        float(right.mean() - left.mean()), 4
                    ),
                })

    changepoint_p = None
    if changepoints:
        try:
            from scipy import stats as _sc
            _best_cp = max(changepoints, key=lambda x: abs(x["shift"]))
            _ci = _best_cp["index"]
            _cw = max(5, len(values) // 10)
            _left_w  = values.iloc[max(0, _ci - _cw):_ci].values
            _right_w = values.iloc[_ci:min(len(values), _ci + _cw)].values
            if len(_left_w) >= 3 and len(_right_w) >= 3:
                _, changepoint_p = _sc.mannwhitneyu(_left_w, _right_w, alternative="two-sided")
                changepoint_p = round(float(changepoint_p), 6)
                _best_cp["p_value"] = changepoint_p
                _best_cp["significant"] = changepoint_p < 0.05
        except Exception:
            pass

    first_val = float(values.iloc[0])
    last_val  = float(values.iloc[-1])
    pct_change = round(
        (last_val - first_val) / (abs(first_val) + 1e-10) * 100,
        2
    )

    top_finding = (
        f"'{value_col}' shows a {trend_direction} trend "
        f"over {len(df)} data points. "
        f"Overall change: {pct_change:+.1f}%. "
        f"{len(changepoints)} changepoint(s) detected. "
        + (
            f"Trend is statistically significant "
            f"(p={mk_result['p_value']:.4f})."
            if mk_result and mk_result["significant"]
            else "Trend is not statistically significant."
        )
        + (
            f" Largest changepoint is statistically significant (Mann-Whitney p={changepoint_p:.4f})."
            if changepoint_p is not None and changepoint_p < 0.05 else ""
        )
    )

    severity = (
        "high"   if abs(pct_change) > 50 else
        "medium" if abs(pct_change) > 20 else
        "low"
    )

    return _make_result(
        analysis_type="trend_analysis",
        data={
            "value_col":      value_col,
            "time_col":       time_col,
            "data_points":    len(df),
            "trend_direction": trend_direction,
            "trend_strength": round(float(trend_strength), 6),
            "pct_change":     pct_change,
            "first_value":    round(first_val, 4),
            "last_value":     round(last_val, 4),
            "mean":           round(float(values.mean()), 4),
            "std":            round(float(values.std()), 4),
            "mann_kendall":        mk_result,
            "changepoints":        changepoints[:5],
            "changepoint_p_value": changepoint_p,
        },
        top_finding=top_finding,
        severity=severity,
        # Prefer Mann-Kendall p as the trend's significance; fall back to
        # the changepoint p if MK didn't fire.
        confidence=compute_confidence(
            n=len(df),
            p_value=(mk_result["p_value"] if mk_result else changepoint_p),
            base=0.88,
        ),
        chart_ready_data={
            "type":       "trend_line",
            "time_col":   time_col,
            "value_col":  value_col,
            "times":      [str(t) for t in times.tolist()],
            "values":     values.tolist(),
            "roll7":      roll7.tolist(),
            "roll30":     roll30.tolist(),
            "changepoints": changepoints[:5],
        },
        enables=["anomaly_detection"],
    )

def run_time_series_decomposition(
    csv_path: str,
    time_col: str,
    value_col: str,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if time_col not in df.columns:
        return _make_error_result("time_series_decomposition", f"Column '{time_col}' not found")
    if value_col not in df.columns:
        return _make_error_result("time_series_decomposition", f"Column '{value_col}' not found")

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col, value_col])
    df = df.sort_values(time_col)

    if len(df) < 14:
        return _make_error_result("time_series_decomposition", "Need at least 14 data points for decomposition", "insufficient_data")

    values = df[value_col].astype(float)

    date_diffs = df[time_col].diff().dropna()
    median_diff = date_diffs.median()
    if median_diff.days <= 1:
        period = 7
    elif median_diff.days <= 7:
        period = 4
    else:
        period = 12

    period = min(period, len(df) // 2)

    trend_component     = []
    seasonal_component  = []
    residual_component  = []
    seasonality_strength = 0.0
    decomp_success = False

    try:
        from statsmodels.tsa.seasonal import STL
        stl    = STL(values, period=period, robust=True)
        result = stl.fit()

        trend_component    = result.trend.tolist()
        seasonal_component = result.seasonal.tolist()
        residual_component = result.resid.tolist()

        var_residual  = float(np.var(result.resid))
        var_detrended = float(
            np.var(result.seasonal + result.resid)
        )
        seasonality_strength = round(
            max(0, 1 - var_residual / (var_detrended + 1e-10)),
            4
        )
        decomp_success = True

    except Exception:
        window = max(3, period)
        trend_series = values.rolling(
            window=window, center=True, min_periods=1
        ).mean()
        trend_component    = trend_series.tolist()
        residual_component = (values - trend_series).tolist()
        seasonal_component = [0.0] * len(values)

    if residual_component:
        resid_arr  = np.array(residual_component)
        resid_std  = resid_arr.std()
        anomalies  = int(np.sum(np.abs(resid_arr) > 2 * resid_std))
    else:
        anomalies = 0

    top_finding = (
        f"Decomposition of '{value_col}' with period={period}. "
        f"Seasonality strength: {seasonality_strength:.0%}. "
        f"{anomalies} anomalous residuals detected. "
        + (
            "STL decomposition used."
            if decomp_success
            else "Simple moving average fallback used."
        )
    )

    return _make_result(
        analysis_type="time_series_decomposition",
        data={
            "value_col":           value_col,
            "time_col":            time_col,
            "period":              period,
            "data_points":         len(df),
            "seasonality_strength": seasonality_strength,
            "anomalous_residuals": anomalies,
            "decomp_method": (
                "STL" if decomp_success else "moving_average"
            ),
        },
        top_finding=top_finding,
        severity="medium" if seasonality_strength > 0.5
                  else "low",
        # STL success → full prior; moving-average fallback → reduced base.
        confidence=compute_confidence(
            n=len(df),
            base=(0.82 if decomp_success else 0.65),
        ),
        chart_ready_data={
            "type":       "decomposition",
            "times":      [
                str(t) for t in df[time_col].tolist()
            ],
            "original":   values.tolist(),
            "trend":      trend_component,
            "seasonal":   seasonal_component,
            "residual":   residual_component,
        },
    )

def run_cohort_analysis(
    csv_path: str,
    entity_col: str,
    time_col: str,
    value_col: str,
    cohort_window: str = "M",
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    for col in [entity_col, time_col, value_col]:
        if col not in df.columns:
            return _make_error_result("cohort_analysis", f"Column '{col}' not found")

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[entity_col, time_col])
    _cw = cohort_window if cohort_window in ("D", "W", "M", "Q") else "M"
    df["_period"] = df[time_col].dt.to_period(_cw)

    first_period = (
        df.groupby(entity_col)["_period"]
          .min()
          .rename("cohort")
    )
    df = df.join(first_period, on=entity_col)

    df["_cohort_index"] = (
        df["_period"] - df["cohort"]
    ).apply(lambda x: x.n if hasattr(x, 'n') else 0)

    cohort_data = (
        df.groupby(["cohort", "_cohort_index"])[entity_col]
          .nunique()
          .reset_index()
    )
    cohort_data.columns = [
        "cohort", "period_index", "entity_count"
    ]

    cohort_sizes = (
        cohort_data[cohort_data["period_index"] == 0]
        .set_index("cohort")["entity_count"]
    )

    cohort_data["retention_rate"] = cohort_data.apply(
        lambda r: round(
            r["entity_count"] /
            cohort_sizes.get(r["cohort"], 1) * 100, 2
        ),
        axis=1,
    )

    p1 = cohort_data[cohort_data["period_index"] == 1]
    avg_retention_p1 = round(
        float(p1["retention_rate"].mean()), 2
    ) if len(p1) > 0 else 0.0

    cohort_count = int(cohort_sizes.shape[0])

    top_finding = (
        f"Cohort analysis across {cohort_count} cohorts. "
        f"Average period-1 retention: {avg_retention_p1}%. "
        f"Total entities tracked: "
        f"{int(df[entity_col].nunique()):,}."
    )

    severity = (
        "high"   if avg_retention_p1 < 30 else
        "medium" if avg_retention_p1 < 60 else
        "low"
    )

    cohort_records = []
    for _, row in cohort_data.head(100).iterrows():
        cohort_records.append({
            "cohort":        str(row["cohort"]),
            "period_index":  int(row["period_index"]),
            "entity_count":  int(row["entity_count"]),
            "retention_rate": float(row["retention_rate"]),
        })

    return _make_result(
        analysis_type="cohort_analysis",
        data={
            "entity_col":         entity_col,
            "time_col":           time_col,
            "cohort_count":       cohort_count,
            "avg_retention_p1":   avg_retention_p1,
            "total_entities":     int(
                df[entity_col].nunique()
            ),
            "cohort_data":        cohort_records,
        },
        top_finding=top_finding,
        severity=severity,
        confidence=compute_confidence(n=int(df[entity_col].nunique()), base=0.87),
        chart_ready_data={
            "type":         "cohort_heatmap",
            "cohort_data":  cohort_records,
        },
    )

def run_session_detection(
    csv_path: str,
    entity_col: str,
    time_col: str,
    event_col: str = None,
    marker_events: list = None,
    gap_minutes: int = 30,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    for col in [entity_col, time_col]:
        if col not in df.columns:
            return _make_error_result("session_detection", f"Column '{col}' not found")

    df[time_col] = pd.to_datetime(
        df[time_col], errors="coerce"
    )
    df = df.dropna(subset=[time_col])
    df = df.sort_values([entity_col, time_col])

    df = df.sort_values([entity_col, time_col])

    if not marker_events and event_col and event_col in df.columns:
        found_defaults = [e for e in DEFAULT_SESSION_MARKERS if e in df[event_col].unique()]
        if found_defaults:
            marker_events = found_defaults
            print(f"INFO: Auto-injected session markers: {marker_events}")

    if marker_events and event_col and event_col in df.columns:
        df['_is_marker'] = df[event_col].isin(marker_events)
        df['_entity_changed'] = df[entity_col] != df[entity_col].shift(1)
        df['_new_session'] = df['_is_marker'] | df['_entity_changed']
        df['session_id'] = (
            df[entity_col].astype(str) + '_s' +
            df.groupby(entity_col)['_new_session'].cumsum().astype(str)
        )
        df = df.drop(columns=['_is_marker', '_entity_changed', '_new_session'])
    else:
        df['_prev_time'] = df.groupby(entity_col)[time_col].shift(1)
        df['_prev_entity'] = df[entity_col].shift(1)
        df['_time_diff'] = (
            df[time_col] - df['_prev_time']
        ).dt.total_seconds() / 60

        df['_new_session'] = (
            (df[entity_col] != df['_prev_entity']) |
            (df['_time_diff'] > gap_minutes) |
            (df['_time_diff'].isna())
        )

        df['session_id'] = (
            df[entity_col].astype(str) + '_s' +
            df.groupby(entity_col)['_new_session'].cumsum().astype(str)
        )

        df = df.drop(columns=[
            '_prev_time', '_prev_entity',
            '_time_diff', '_new_session'
        ])

    session_stats = (
        df.groupby("session_id")
          .agg(
              entity=(entity_col, "first"),
              event_count=(time_col, "count"),
              start_time=(time_col, "min"),
              end_time=(time_col, "max"),
          )
          .reset_index()
    )
    session_stats["duration_minutes"] = (
        (session_stats["end_time"] -
         session_stats["start_time"])
        .dt.total_seconds() / 60
    ).round(2)

    total_sessions   = int(len(session_stats))
    avg_events       = round(
        float(session_stats["event_count"].mean()), 2
    )
    avg_duration     = round(
        float(session_stats["duration_minutes"].mean()), 2
    )
    bounce_sessions  = int(
        (session_stats["event_count"] == 1).sum()
    )
    bounce_rate      = round(
        bounce_sessions / max(total_sessions, 1) * 100, 2
    )

    enriched_path = csv_path.replace(".csv", "_sessions.csv")
    df.to_csv(enriched_path, index=False)

    top_finding = (
        f"Detected {total_sessions:,} sessions across {df[entity_col].nunique():,} unique IDs. "
        f"The average visit lasts {avg_duration} minutes with {avg_events} interactions. "
        f"Bounce rate is {bounce_rate}%."
    )

    severity = (
        "high"   if bounce_rate > 40 else
        "medium" if bounce_rate > 20 else
        "low"
    )

    narrative = {
        "what_it_means": (
            f"{total_sessions:,} sessions detected across {bounce_rate}% bounce rate. "
            f"Average session: {avg_events} events lasting {avg_duration} min. "
            + (f"High bounce rate signals many users leave after a single interaction — "
               f"likely a landing or onboarding friction issue."
               if bounce_rate > 30 else
               f"Session engagement is healthy with most users progressing past the first event.")
        ),
        "proposed_fix": (
            f"Investigate what triggers the single-event sessions ({bounce_sessions:,} bounce sessions). "
            f"Add an entry hook or onboarding prompt to push bounce rate below 20%."
            if bounce_rate > 30 else
            f"Monitor session length trend as new features ship to maintain current engagement depth."
        ),
        "severity": severity,
    }

    length_dist = {
        "p25": round(float(
            session_stats["event_count"].quantile(0.25)
        ), 1),
        "p50": round(float(
            session_stats["event_count"].quantile(0.50)
        ), 1),
        "p75": round(float(
            session_stats["event_count"].quantile(0.75)
        ), 1),
        "p90": round(float(
            session_stats["event_count"].quantile(0.90)
        ), 1),
        "p95": round(float(
            session_stats["event_count"].quantile(0.95)
        ), 1),
    }

    return _make_result(
        analysis_type="session_detection",
        data={
            "total_sessions":    total_sessions,
            "total_entities":    int(
                df[entity_col].nunique()
            ),
            "total_events":      len(df),
            "avg_events_per_session":  avg_events,
            "avg_duration_minutes":    avg_duration,
            "bounce_rate":       bounce_rate,
            "bounce_sessions":   bounce_sessions,
            "detection_mode": (
                "event_based"
                if marker_events else "time_gap"
            ),
            "gap_minutes":       gap_minutes,
            "session_length_distribution": length_dist,
            "enriched_csv_path": enriched_path,
            "narrative": narrative,
        },
        top_finding=top_finding,
        severity=severity,
        confidence=compute_confidence(n=total_sessions, base=0.95),
        chart_ready_data={
            "type": "session_length_histogram",
            "event_counts": session_stats[
                "event_count"
            ].tolist()[:500],
            "duration_minutes": session_stats[
                "duration_minutes"
            ].tolist()[:500],
        },
        enables=[
            "funnel_analysis",
            "friction_detection",
            "survival_analysis",
            "sequential_pattern_mining",
            "user_segmentation",
            "association_rules",
        ],
    )

def run_funnel_analysis(
    csv_path: str,
    entity_col: str,
    event_col: str,
    time_col: str,
    session_col: str = "session_id",
    funnel_steps: list = None,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if session_col not in df.columns:
        enriched = csv_path.replace(".csv", "_sessions.csv")
        if os.path.exists(enriched):
            df = pd.read_csv(enriched, low_memory=False)
        else:
            return _make_error_result(
                "funnel_analysis",
                f"'{session_col}' column not found. Run session_detection first.",
            )

    for col in [entity_col, event_col]:
        if col not in df.columns:
            return _make_error_result("funnel_analysis", f"Column '{col}' not found")

    df[time_col] = pd.to_datetime(
        df[time_col], errors="coerce"
    )
    df = df.sort_values([session_col, time_col])

    if not funnel_steps:
        event_counts = df[event_col].value_counts()
        total_sessions = df[session_col].nunique()
        common_events = [
            e for e, c in event_counts.items()
            if c / total_sessions > 0.05
        ]
        event_positions = (
            df.groupby(event_col)
              .apply(
                  lambda x: x.groupby(session_col)
                              .cumcount()
                              .mean()
              )
        )
        funnel_steps = (
            event_positions.loc[event_positions.index.isin(common_events)]
            .sort_values()
            .head(8)
            .index.tolist()
        )

    funnel_metrics = []
    prev_count = None

    for step in funnel_steps:
        entities_at_step = df[
            df[event_col] == step
        ][entity_col].nunique()

        conversion = None
        if prev_count is not None and prev_count > 0:
            conversion = round(
                entities_at_step / prev_count * 100, 2
            )

        funnel_metrics.append({
            "step":              step,
            "entity_count":      entities_at_step,
            "conversion_from_prev": conversion,
        })
        prev_count = entities_at_step

    if funnel_metrics:
        top_count = funnel_metrics[0]["entity_count"]
        bot_count = funnel_metrics[-1]["entity_count"]
        overall_conversion = round(
            bot_count / max(top_count, 1) * 100, 2
        )
    else:
        overall_conversion = 0.0

    biggest_drop = None
    biggest_drop_pct = 0.0
    for m in funnel_metrics:
        if m["conversion_from_prev"] is not None:
            drop = 100 - m["conversion_from_prev"]
            if drop > biggest_drop_pct:
                biggest_drop_pct = drop
                biggest_drop     = m["step"]

    top_finding = (
        f"Funnel analysis: {len(funnel_steps)} steps, "
        f"overall conversion {overall_conversion}%. "
        + (
            f"Biggest drop-off at '{biggest_drop}' "
            f"({biggest_drop_pct:.1f}% lost)."
            if biggest_drop else ""
        )
        + (
            " Steps auto-detected from event patterns."
            if not funnel_steps else ""
        )
    )

    severity = (
        "critical" if overall_conversion < 20 else
        "high"     if overall_conversion < 40 else
        "medium"   if overall_conversion < 70 else
        "low"
    )

    if biggest_drop and biggest_drop_pct > 20:
        narrative = {
            "what_it_means": (
                f"End-to-end conversion is {overall_conversion}%. "
                f"The critical leak is at '{biggest_drop}' where {biggest_drop_pct:.1f}% of users "
                f"drop off — this single step accounts for the majority of lost conversions. "
                + ("This is a critical funnel health issue." if overall_conversion < 30 else "")
            ),
            "proposed_fix": (
                f"Prioritise '{biggest_drop}': reduce form fields, add progress indicators, "
                f"or simplify the required action. A 10% improvement here would significantly "
                f"lift overall conversion from {overall_conversion}%."
            ),
            "severity": severity,
        }
    else:
        narrative = {
            "what_it_means": f"Funnel conversion is {overall_conversion}% with no single catastrophic drop-off. "
                             f"Users are progressing through stages at an acceptable rate.",
            "proposed_fix": "Continue A/B testing each funnel step to incrementally improve overall conversion.",
            "severity": severity,
        }

    return _make_result(
        analysis_type="funnel_analysis",
        data={
            "funnel_steps":        funnel_steps,
            "funnel_metrics":      funnel_metrics,
            "overall_conversion":  overall_conversion,
            "biggest_drop_step":   biggest_drop,
            "biggest_drop_pct":    round(biggest_drop_pct, 2),
            "auto_detected":       not bool(funnel_steps),
            "narrative": narrative,
        },
        top_finding=top_finding,
        severity=severity,
        # n = entities entering the top of the funnel; confidence scales with that.
        confidence=compute_confidence(
            n=int(funnel_metrics[0]["entity_count"]) if funnel_metrics else 0,
            base=0.88,
        ),
        chart_ready_data={
            "type":   "funnel_bar",
            "steps":  funnel_steps,
            "counts": [
                m["entity_count"] for m in funnel_metrics
            ],
            "conversions": [
                m["conversion_from_prev"]
                for m in funnel_metrics
            ],
        },
    )

def run_friction_detection(
    csv_path: str,
    entity_col: str,
    event_col: str,
    session_col: str = "session_id",
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if session_col not in df.columns:
        enriched = csv_path.replace(".csv", "_sessions.csv")
        if os.path.exists(enriched):
            df = pd.read_csv(enriched, low_memory=False)
        else:
            return _make_error_result(
                "friction_detection",
                f"'{session_col}' column not found. Run session_detection first.",
            )

    for col in [entity_col, event_col]:
        if col not in df.columns:
            return _make_error_result("friction_detection", f"Column '{col}' not found")

    event_counts = (
        df.groupby([session_col, event_col])
          .size()
          .reset_index(name="count")
    )

    event_counts["repetitions"] = (
        event_counts["count"] - 1
    ).clip(lower=0)

    friction = (
        event_counts.groupby(event_col)
                    .agg(
                        total_occurrences=("count", "sum"),
                        total_repetitions=(
                            "repetitions", "sum"
                        ),
                        sessions_affected=(
                            session_col, "count"
                        ),
                    )
                    .reset_index()
    )

    friction["repetition_rate"] = (
        friction["total_repetitions"] /
        friction["total_occurrences"].clip(lower=1)
    ).round(4)

    friction["severity"] = friction["repetition_rate"].apply(
        lambda r:
        "critical" if r > 0.10 else
        "high"     if r > 0.05 else
        "medium"   if r > 0.02 else
        "low"
    )

    friction = friction.sort_values(
        "repetition_rate", ascending=False
    )

    critical_count = int(
        (friction["severity"] == "critical").sum()
    )
    high_count = int(
        (friction["severity"] == "high").sum()
    )

    top_event = friction.iloc[0] if len(friction) > 0 else None

    top_finding = (
        f"Friction detection: {len(friction)} events analyzed. "
        f"{critical_count} CRITICAL, {high_count} HIGH "
        f"friction events. "
        + (
            f"Most friction: '{top_event[event_col]}' "
            f"({top_event['repetition_rate']:.1%} "
            f"repetition rate)."
            if top_event is not None else ""
        )
    )

    severity = (
        "critical" if critical_count > 0 else
        "high"     if high_count > 0     else
        "medium"
    )

    friction_records = friction.head(20).to_dict("records")
    for r in friction_records:
        for k, v in r.items():
            if hasattr(v, 'item'):
                r[k] = v.item()

    if top_event is not None and float(top_event["repetition_rate"]) > 0.02:
        top_name = str(top_event[event_col])
        top_rate = float(top_event["repetition_rate"])
        affected = int(top_event["sessions_affected"])
        narrative = {
            "what_it_means": (
                f"'{top_name}' is triggered {top_rate:.0%} more times than needed across "
                f"{affected:,} sessions — a strong signal users are stuck or the step "
                f"isn't completing successfully on the first attempt. "
                f"{critical_count} event(s) are at CRITICAL friction levels (>10% repetition rate)."
            ),
            "proposed_fix": (
                f"Audit '{top_name}': add immediate visual confirmation on trigger, "
                f"check for silent failures or unresponsive UI. "
                f"Target: reduce repetition rate below 2% per session."
            ),
            "severity": severity,
        }
    else:
        narrative = {
            "what_it_means": "No significant friction detected — all events are completing on first attempt. UX flow is healthy.",
            "proposed_fix": "Maintain current interaction patterns. Monitor if new features are added.",
            "severity": "low",
        }

    return _make_result(
        analysis_type="friction_detection",
        data={
            "events_analyzed": len(friction),
            "critical_events": critical_count,
            "high_events":     high_count,
            "top_friction_events": friction_records,
            "narrative": narrative,
        },
        top_finding=top_finding,
        severity=severity,
        confidence=compute_confidence(n=int(event_counts[session_col].nunique()), base=0.91),
        chart_ready_data={
            "type":   "friction_heatmap",
            "events": [
                str(r[event_col])
                for r in friction_records
            ],
            "repetition_rates": [
                float(r["repetition_rate"])
                for r in friction_records
            ],
            "severities": [
                r["severity"] for r in friction_records
            ],
        },
    )

def run_survival_analysis(
    csv_path: str,
    entity_col: str,
    event_col: str,
    session_col: str = "session_id",
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if session_col not in df.columns:
        enriched = csv_path.replace(".csv", "_sessions.csv")
        if os.path.exists(enriched):
            df = pd.read_csv(enriched, low_memory=False)
        else:
            return _make_error_result(
                "survival_analysis",
                f"'{session_col}' not found. Run session_detection first.",
            )

    session_lengths = (
        df.groupby(session_col).size().values
    )
    total_sessions = len(session_lengths)

    if total_sessions == 0:
        return _make_error_result("survival_analysis", "No sessions found", "insufficient_data")

    max_steps = int(np.percentile(session_lengths, 95))
    max_steps = min(max_steps, 50)

    survival_curve  = []
    dropout_rates   = []
    prev_surviving  = total_sessions

    for step in range(1, max_steps + 1):
        surviving = int(np.sum(session_lengths >= step))
        survival_pct = round(
            surviving / total_sessions * 100, 2
        )
        dropout = round(
            (prev_surviving - surviving) /
            max(prev_surviving, 1) * 100, 2
        )

        zone = (
            "safe"    if survival_pct >= 80 else
            "warning" if survival_pct >= 50 else
            "danger"
        )

        survival_curve.append({
            "step":         step,
            "surviving":    surviving,
            "survival_pct": survival_pct,
            "dropout_rate": dropout,
            "zone":         zone,
        })
        dropout_rates.append(dropout)
        prev_surviving = surviving

    if survival_curve:
        critical = max(
            survival_curve, key=lambda x: x["dropout_rate"]
        )
    else:
        critical = None

    median_length = int(np.median(session_lengths))

    reach_10 = int(np.sum(session_lengths >= 10))
    reach_20 = int(np.sum(session_lengths >= 20))
    pct_10   = round(reach_10 / total_sessions * 100, 1)
    pct_20   = round(reach_20 / total_sessions * 100, 1)

    logrank_p = None
    try:
        from scipy import stats as _sc
        median_len = float(np.median(session_lengths))
        short = session_lengths[session_lengths < median_len]
        long_ = session_lengths[session_lengths >= median_len]
        if len(short) > 5 and len(long_) > 5:
            _, logrank_p = _sc.mannwhitneyu(short, long_, alternative="two-sided")
            logrank_p = round(float(logrank_p), 6)
    except Exception:
        logrank_p = None

    top_finding = (
        f"Session survival: {total_sessions:,} sessions. "
        f"Median session length: {median_length} events. "
        f"{pct_10}% reach step 10, "
        f"{pct_20}% reach step 20. "
        + (
            f"Critical drop-off at step "
            f"{critical['step']} "
            f"({critical['dropout_rate']:.1f}% leave)."
            if critical else ""
        )
        + (f" Short vs long session distributions significantly different "
           f"(Mann-Whitney p={logrank_p:.4f})."
           if logrank_p is not None and logrank_p < 0.05 else "")
    )

    severity = (
        "critical" if pct_10 < 30 else
        "high"     if pct_10 < 50 else
        "medium"   if pct_10 < 70 else
        "low"
    )

    if critical and critical["dropout_rate"] > 10:
        narrative = {
            "what_it_means": (
                f"Only {pct_10}% of sessions reach step 10 and {pct_20}% reach step 20. "
                f"The steepest drop occurs at step {critical['step']} where {critical['dropout_rate']:.1f}% "
                f"of still-active users abandon. Median session is just {median_length} events long."
            ),
            "proposed_fix": (
                f"Investigate what happens at step {critical['step']} in the user journey. "
                f"Reduce friction or add a recovery prompt at this point. "
                f"Target: push step-10 retention above 60%."
            ),
            "severity": severity,
        }
    else:
        narrative = {
            "what_it_means": (
                f"{pct_10}% of sessions reach step 10 — indicating reasonable session depth. "
                f"Median session length is {median_length} events."
            ),
            "proposed_fix": "Continue monitoring session depth as new features are released.",
            "severity": severity,
        }

    return _make_result(
        analysis_type="survival_analysis",
        data={
            "total_sessions":    total_sessions,
            "median_length":     median_length,
            "pct_reach_step_10": pct_10,
            "pct_reach_step_20": pct_20,
            "critical_dropoff":  critical,
            "survival_curve":    survival_curve,
            "max_steps_analyzed": max_steps,
            "logrank_p_value":   logrank_p,
            "narrative": narrative,
        },
        top_finding=top_finding,
        severity=severity,
        # log-rank p when the test ran; otherwise just n-weighted.
        confidence=compute_confidence(
            n=total_sessions,
            p_value=(logrank_p if logrank_p is not None else None),
            base=0.93,
        ),
        chart_ready_data={
            "type":   "survival_curve",
            "steps":  [s["step"] for s in survival_curve],
            "survival_pcts": [
                s["survival_pct"] for s in survival_curve
            ],
            "dropout_rates": [
                s["dropout_rate"] for s in survival_curve
            ],
            "zones": [
                s["zone"] for s in survival_curve
            ],
        },
    )

def run_user_segmentation(
    csv_path: str,
    entity_col: str,
    event_col: str,
    time_col: str = None,
    session_col: str = "session_id",
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if session_col not in df.columns:
        enriched = csv_path.replace(".csv", "_sessions.csv")
        if os.path.exists(enriched):
            df = pd.read_csv(enriched, low_memory=False)
        else:
            return _make_error_result(
                "user_segmentation",
                f"'{session_col}' not found. Run session_detection first.",
            )

    if time_col and time_col in df.columns:
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")

    if len(df) > 300_000:
        sample_frac = 300_000 / len(df)
        df = df.groupby(entity_col, group_keys=False).apply(
            lambda x: x.sample(frac=min(sample_frac, 1.0), random_state=42)
        ).reset_index(drop=True)

    agg_dict = {
        "total_events": (event_col, "count"),
        "event_diversity": (event_col, "nunique"),
        "session_count": (session_col, "nunique")
    }
    if time_col and time_col in df.columns:
        agg_dict["time_min"] = (time_col, "min")
        agg_dict["time_max"] = (time_col, "max")

    agg_df = df.groupby(entity_col).agg(**agg_dict).reset_index()

    agg_df["avg_sess_len"] = (
        agg_df["total_events"] / agg_df["session_count"].clip(lower=1)
    )

    if time_col and time_col in df.columns:
        agg_df["time_span"] = (
            (agg_df["time_max"] - agg_df["time_min"])
            .dt.total_seconds().fillna(0) / 86400
        )
    else:
        agg_df["time_span"] = 0.0

    rep_counts = (
        df.groupby([entity_col, session_col, event_col])
          .size()
          .reset_index(name="cnt")
    )
    rep_counts["reps"] = (rep_counts["cnt"] - 1).clip(lower=0)
    rep_per_entity = rep_counts.groupby(entity_col)["reps"].sum().reset_index()
    agg_df = agg_df.merge(rep_per_entity, on=entity_col, how="left")
    agg_df["reps"] = agg_df["reps"].fillna(0)
    agg_df["repetition_ratio"] = (
        agg_df["reps"] / agg_df["total_events"].clip(lower=1)
    )

    entity_ids = agg_df[entity_col].tolist()
    feature_cols = [
        "total_events", "event_diversity", "repetition_ratio",
        "session_count", "avg_sess_len", "time_span",
    ]
    features = agg_df[feature_cols].values.tolist()

    if len(features) < 5:
        return _make_error_result("user_segmentation", "Need at least 5 entities", "insufficient_data")

    feature_matrix = np.array(features, dtype=float)

    from sklearn.preprocessing import StandardScaler
    scaler  = StandardScaler()
    X_scaled = scaler.fit_transform(feature_matrix)

    labels = np.full(len(features), -1)
    try:
        from sklearn.cluster import DBSCAN
        from sklearn.neighbors import NearestNeighbors

        min_samples = max(3, len(features) // 50)

        k = min(min_samples, len(features) - 1)
        nbrs = NearestNeighbors(n_neighbors=k).fit(X_scaled)
        distances, _ = nbrs.kneighbors(X_scaled)
        k_distances = np.sort(distances[:, -1])

        if len(k_distances) >= 3:
            d2 = np.diff(np.diff(k_distances))
            elbow_idx = int(np.argmax(d2)) + 2
            eps_val = float(k_distances[elbow_idx])
            eps_val = max(0.3, min(eps_val, 2.5))
        else:
            eps_val = 0.8

        print(f"DEBUG DBSCAN: auto-eps={eps_val:.3f} min_samples={min_samples}")
        db = DBSCAN(eps=eps_val, min_samples=min_samples)
        labels = db.fit_predict(X_scaled)

        noise_ratio = (labels == -1).sum() / len(labels)
        if noise_ratio > 0.5:
            raise ValueError(f"Too much noise ({noise_ratio:.0%}) — use KMeans")

    except Exception:
        try:
            from sklearn.cluster import KMeans
            n_clusters = min(4, len(features) // 3)
            km         = KMeans(
                n_clusters=n_clusters,
                random_state=42,
                n_init=10,
            )
            labels = km.fit_predict(X_scaled)
        except Exception:
            labels = np.zeros(len(features), dtype=int)

    feature_names = [
        "total_events", "event_diversity",
        "repetition_ratio", "session_count",
        "avg_session_length", "time_span_days",
    ]
    segments_raw = defaultdict(list)
    for i, label in enumerate(labels):
        segments_raw[int(label)].append(i)

    segments = []
    for label, indices in sorted(segments_raw.items()):
        seg_features = feature_matrix[indices]
        means        = seg_features.mean(axis=0)

        profile = {
            f: round(float(means[j]), 3)
            for j, f in enumerate(feature_names)
        }

        characteristics = []
        if profile["repetition_ratio"] > 0.15:
            characteristics.append("high friction")
        if profile["avg_session_length"] < 3:
            characteristics.append("quick sessions")
        if profile["event_diversity"] > 10:
            characteristics.append("broad exploration")
        if profile["session_count"] > 5:
            characteristics.append("highly engaged")
        if label == -1:
            characteristics.append("outliers")

        segments.append({
            "segment_id":    label,
            "size":          len(indices),
            "pct":           round(
                len(indices) / len(features) * 100, 1
            ),
            "profile":       profile,
            "characteristics": characteristics,
            "is_noise":      label == -1,
        })

    segments.sort(key=lambda s: s["size"], reverse=True)

    top_finding = (
        f"User segmentation: {len(entity_ids):,} entities "
        f"grouped into {len(segments)} segments. "
        f"Largest segment: {segments[0]['size']} entities "
        f"({segments[0]['pct']}%) — "
        f"{', '.join(segments[0]['characteristics']) or 'mixed behavior'}."
    )

    dominant = segments[0] if segments else None
    noise_seg = next((s for s in segments if s["is_noise"]), None)
    narrative = {
        "what_it_means": (
            f"{len(entity_ids):,} users grouped into {len(segments)} behavioural segments. "
            + (f"Dominant segment ({dominant['pct']}%): {', '.join(dominant['characteristics']) or 'mixed behaviour'}. "
               if dominant else "")
            + (f"Noise segment ({noise_seg['pct']}%): users with atypical patterns who don’t fit any cluster."
               if noise_seg and noise_seg['pct'] > 5 else "")
        ),
        "proposed_fix": (
            f"Design personalised experiences for each segment. Focus product changes on the dominant segment "
            f"({dominant['pct']}% of users) for maximum impact."
            if dominant else
            "Collect more events per user to enable stable segmentation."
        ),
        "severity": "medium",
    }

    return _make_result(
        analysis_type="user_segmentation",
        data={
            "total_entities": len(entity_ids),
            "segment_count":  len(segments),
            "segments":       segments,
            "feature_names":  feature_names,
            "narrative": narrative,
        },
        top_finding=top_finding,
        severity="medium",
        confidence=compute_confidence(n=len(entity_ids), base=0.82),
        chart_ready_data={
            "type":     "segment_donut",
            "segments": segments,
        },
    )

def run_sequential_pattern_mining(
    csv_path: str,
    entity_col: str,
    event_col: str,
    session_col: str = "session_id",
    min_support: float = 0.03,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if session_col not in df.columns:
        enriched = csv_path.replace(".csv", "_sessions.csv")
        if os.path.exists(enriched):
            df = pd.read_csv(enriched, low_memory=False)
        else:
            return _make_error_result(
                "sequential_pattern_mining",
                f"'{session_col}' not found. Run session_detection first.",
            )

    sequences = []
    for _, grp in df.groupby(session_col):
        seq = grp[event_col].tolist()
        if len(seq) >= 2:
            sequences.append(seq)

    total_seqs = len(sequences)
    if total_seqs == 0:
        return _make_error_result("sequential_pattern_mining", "No sequences found", "insufficient_data")

    min_count = max(2, int(total_seqs * min_support))

    bigram_counts  = Counter()
    trigram_counts = Counter()

    for seq in sequences:
        for i in range(len(seq) - 1):
            bigram_counts[(seq[i], seq[i+1])] += 1
        for i in range(len(seq) - 2):
            trigram_counts[(
                seq[i], seq[i+1], seq[i+2]
            )] += 1

    frequent_bigrams = [
        {
            "sequence":  list(k),
            "count":     v,
            "support":   round(v / total_seqs, 4),
            "length":    2,
        }
        for k, v in bigram_counts.most_common(20)
        if v >= min_count
    ]

    frequent_trigrams = [
        {
            "sequence":  list(k),
            "count":     v,
            "support":   round(v / total_seqs, 4),
            "length":    3,
        }
        for k, v in trigram_counts.most_common(10)
        if v >= min_count
    ]

    all_patterns = sorted(
        frequent_bigrams + frequent_trigrams,
        key=lambda x: x["count"],
        reverse=True,
    )[:30]

    loops = [
        p for p in frequent_bigrams
        if p["sequence"][0] == p["sequence"][1]
    ]

    outcome_correlations = []
    try:
        from scipy import stats as _sc
        session_depths = df.groupby(session_col).size()
        for pat in all_patterns[:10]:
            seq = pat["sequence"]
            if len(seq) < 2:
                continue

            def _has_pattern(grp):
                events = grp[event_col].tolist()
                for _i in range(len(events) - len(seq) + 1):
                    if events[_i:_i + len(seq)] == seq:
                        return 1
                return 0
            has_pat = df.groupby(session_col).apply(_has_pattern)
            common_idx = session_depths.index.intersection(has_pat.index)
            if len(common_idx) < 10:
                continue
            depths = session_depths[common_idx].values
            flags = has_pat[common_idx].values
            if flags.sum() == 0 or flags.sum() == len(flags):
                continue
            corr, pval = _sc.pointbiserialr(flags, depths)
            outcome_correlations.append({
                "sequence": seq,
                "depth_correlation": round(float(corr), 4),
                "p_value": round(float(pval), 6),
                "significant": pval < 0.05,
                "interpretation": (
                    "positive — sessions with this pattern tend to be longer"
                    if corr > 0.1
                    else "negative — sessions with this pattern tend to exit early"
                    if corr < -0.1
                    else "neutral"
                ),
            })
        outcome_correlations.sort(key=lambda x: abs(x["depth_correlation"]), reverse=True)
    except Exception:
        outcome_correlations = []

    top_finding = (
        f"Sequential mining: {total_seqs:,} sessions, "
        f"{len(all_patterns)} frequent patterns found "
        f"(min support {min_support:.0%}). "
        f"{len(loops)} repetition loops detected. "
        + (
            f"Most common: "
            f"{' → '.join(all_patterns[0]['sequence'])} "
            f"({all_patterns[0]['support']:.0%} sessions)."
            if all_patterns else ""
        )
    )

    if all_patterns:
        top_seq = " → ".join(all_patterns[0]["sequence"])
        top_support = all_patterns[0]["support"]
        loop_msg = (
            f" {len(loops)} repetition loop(s) detected — strong friction signal."
            if loops else " No repetition loops detected."
        )
        narrative = {
            "what_it_means": (
                f"{len(all_patterns)} frequent pathway patterns found across {total_seqs:,} sessions. "
                f"The dominant flow is '{top_seq}' occurring in {top_support:.0%} of all sessions."
                + loop_msg
            ),
            "proposed_fix": (
                f"Optimise the '{top_seq}' path since it's the primary user journey. "
                + (f"Eliminate loops by adding clear exit/completion states for repeated events."
                   if loops else f"Monitor this pathway for changes when new features are released.")
            ),
            "severity": "medium" if loops else "low",
        }
    else:
        narrative = {
            "what_it_means": "No frequent sequential patterns found. User journeys are highly diverse with no dominant pathway.",
            "proposed_fix": "Consider adding clearer navigation pathways to guide users toward conversion.",
            "severity": "low",
        }

    return _make_result(
        analysis_type="sequential_pattern_mining",
        data={
            "total_sequences":    total_seqs,
            "patterns_found":     len(all_patterns),
            "repetition_loops":   len(loops),
            "top_patterns":       all_patterns,
            "loop_patterns":      loops[:5],
            "min_support_used":   min_support,
            "outcome_correlations": outcome_correlations,
            "narrative": narrative,
        },
        top_finding=top_finding,
        severity="medium" if loops else "low",
        # Use the top-pattern's outcome correlation p-value if one was computed,
        # else fall back to sample-size only.
        confidence=compute_confidence(
            n=total_seqs,
            p_value=(outcome_correlations[0]["p_value"]
                     if outcome_correlations else None),
            base=0.87,
        ),
        chart_ready_data={
            "type":     "sequence_bar",
            "patterns": [
                " → ".join(p["sequence"])
                for p in all_patterns[:15]
            ],
            "counts": [
                p["count"] for p in all_patterns[:15]
            ],
            "supports": [
                p["support"] for p in all_patterns[:15]
            ],
        },
    )

def run_association_rules(
    csv_path: str,
    entity_col: str,
    event_col: str,
    session_col: str = "session_id",
    outcome_events: list = None,
    min_confidence: float = 0.70,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if session_col not in df.columns:
        enriched = csv_path.replace(".csv", "_sessions.csv")
        if os.path.exists(enriched):
            df = pd.read_csv(enriched, low_memory=False)
        else:
            return _make_error_result(
                "association_rules",
                f"'{session_col}' not found. Run session_detection first.",
            )

    total_sessions = df[session_col].nunique()

    if not outcome_events:
        event_session_counts = (
            df.groupby(event_col)[session_col]
              .nunique()
        )
        event_pcts = event_session_counts / total_sessions

        outcome_events = event_pcts[
            (event_pcts > 0.05) & (event_pcts < 0.40)
        ].index.tolist()[:3]

    if not outcome_events:
        return _make_error_result("association_rules", "Could not identify outcome events", "insufficient_data")

    session_events = (
        df.groupby(session_col)[event_col]
          .apply(set)
          .to_dict()
    )

    rules = []
    all_events = df[event_col].unique().tolist()

    for outcome in outcome_events:
        sessions_with = {
            s for s, evts in session_events.items()
            if outcome in evts
        }
        sessions_without = {
            s for s, evts in session_events.items()
            if outcome not in evts
        }

        for event in all_events:
            if event == outcome:
                continue

            sessions_with_antecedent = {
                s for s, evts in session_events.items()
                if event in evts
            }

            if not sessions_with_antecedent:
                continue

            support = len(sessions_with_antecedent) / \
                      max(total_sessions, 1)
            confidence = len(
                sessions_with & sessions_with_antecedent
            ) / max(len(sessions_with_antecedent), 1)

            if (confidence >= min_confidence and
                    support >= 0.05):
                lift = confidence / max(
                    len(sessions_with) / total_sessions,
                    1e-10
                )
                rules.append({
                    "antecedent":  event,
                    "consequent":  outcome,
                    "support":     round(support, 4),
                    "confidence":  round(confidence, 4),
                    "lift":        round(lift, 4),
                    "risk_level": (
                        "high"   if confidence > 0.90 else
                        "medium" if confidence > 0.80 else
                        "low"
                    ),
                })

    rules.sort(key=lambda r: r["confidence"], reverse=True)
    rules = rules[:20]

    high_risk = [r for r in rules if r["risk_level"] == "high"]

    top_finding = (
        f"Association rules: {len(rules)} rules found "
        f"(confidence >= {min_confidence:.0%}). "
        f"{len(high_risk)} high-confidence rules. "
        f"Outcome events: {outcome_events}. "
        + (
            f"Strongest: IF '{rules[0]['antecedent']}' "
            f"THEN '{rules[0]['consequent']}' "
            f"({rules[0]['confidence']:.0%} confidence)."
            if rules else "No strong rules found."
        )
    )

    if rules:
        top_rule = rules[0]
        narrative = {
            "what_it_means": (
                f"{len(rules)} behavioural rules found, {len(high_risk)} with high confidence (>90%). "
                f"Strongest: when a user triggers '{top_rule['antecedent']}', they reach "
                f"'{top_rule['consequent']}' in {top_rule['confidence']:.0%} of cases "
                f"(lift: {top_rule['lift']:.1f}x above random). "
                f"These patterns reveal reliable predictors of key user actions."
            ),
            "proposed_fix": (
                f"Trigger personalised nudges or UI hints when '{top_rule['antecedent']}' is detected. "
                f"High-lift rules (>{1.5:.1f}x) are the strongest candidates for intervention."
            ),
            "severity": "high" if high_risk else "medium",
        }
    else:
        narrative = {
            "what_it_means": "No strong association rules found at this confidence threshold. User behaviour does not show consistent co-occurrence patterns.",
            "proposed_fix": "Lower the confidence threshold or collect more session data to surface weaker patterns.",
            "severity": "info",
        }

    return _make_result(
        analysis_type="association_rules",
        data={
            "total_sessions":    total_sessions,
            "outcome_events":    outcome_events,
            "rules_found":       len(rules),
            "high_risk_rules":   len(high_risk),
            "min_confidence":    min_confidence,
            "rules":             rules,
            "auto_detected_outcomes": not bool(outcome_events),
            "narrative": narrative,
        },
        top_finding=top_finding,
        severity="high" if high_risk else "medium",
        # Use top-rule lift as a rough effect size proxy (log-scaled so lift≈3
        # lands near 'large', lift≈1 near 'none').
        confidence=compute_confidence(
            n=total_sessions,
            effect_size=(math.log(max(rules[0]["lift"], 1e-6)) if rules else None),
            base=0.84,
        ),
        chart_ready_data={
            "type":  "rules_card",
            "rules": rules[:10],
        },
    )

def run_rfm_analysis(
    csv_path: str,
    entity_col: str,
    time_col: str,
    value_col: str,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    for col in [entity_col, time_col, value_col]:
        if col not in df.columns:
            return _make_error_result("rfm_analysis", f"Column '{col}' not found")

    df[time_col] = pd.to_datetime(
        df[time_col], errors="coerce"
    )
    df = df.dropna(subset=[entity_col, time_col, value_col])

    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col])

    if len(df) == 0:
        return _make_error_result("rfm_analysis", "Insufficient data", "insufficient_data")

    ref_date = df[time_col].max() + pd.Timedelta(days=1)

    rfm = (
        df.groupby(entity_col)
          .agg(
              recency=(time_col, lambda x: (ref_date - x.max()).days),
              frequency=(time_col, "count"),
              monetary=(value_col, "sum"),
          )
          .reset_index()
    )

    rfm["r_score"] = pd.qcut(
        rfm["recency"].rank(method="first"), 5, labels=[5, 4, 3, 2, 1]
    ).astype(int)

    rfm["f_score"] = pd.qcut(
        rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]
    ).astype(int)

    rfm["m_score"] = pd.qcut(
        rfm["monetary"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]
    ).astype(int)

    rfm["rfm_score"] = (
        rfm["r_score"].astype(str) +
        rfm["f_score"].astype(str) +
        rfm["m_score"].astype(str)
    )

    def assign_segment(r, f):
        if r >= 4 and f >= 4: return "High Value"
        if r >= 3 and f >= 3: return "Core"
        if r >= 4 and f <= 2: return "Recent"
        if r == 3 and f <= 2: return "Emerging"
        if r == 2 and f >= 3: return "Declining"
        if r <= 2 and f <= 2: return "Inactive"
        return "Other"

    rfm["segment"] = rfm.apply(
        lambda row: assign_segment(
            row["r_score"], row["f_score"]
        ),
        axis=1
    )

    seg_stats = (
        rfm.groupby("segment")
           .agg(
               entity_count=("recency", "count"),
               avg_recency=("recency", "mean"),
               avg_frequency=("frequency", "mean"),
               avg_monetary=("monetary", "mean"),
               total_monetary=("monetary", "sum"),
           )
           .reset_index()
    )

    total_value = rfm["monetary"].sum()
    seg_stats["value_pct"] = (
        seg_stats["total_monetary"] / total_value * 100
    ).round(1)
    seg_stats["entity_pct"] = (
        seg_stats["entity_count"] / len(rfm) * 100
    ).round(1)

    seg_stats = seg_stats.sort_values(
        "total_monetary", ascending=False
    )

    seg_stats["avg_recency"]   = seg_stats["avg_recency"].round(1)
    seg_stats["avg_frequency"] = seg_stats["avg_frequency"].round(2)
    seg_stats["avg_monetary"]  = seg_stats["avg_monetary"].round(2)
    seg_stats["total_monetary"]= seg_stats["total_monetary"].round(2)

    high_value_pct = float(
        seg_stats.loc[
            seg_stats["segment"] == "High Value",
            "value_pct"
        ].sum()
    )
    declining_count = int(
        seg_stats.loc[
            seg_stats["segment"] == "Declining",
            "entity_count"
        ].sum()
    )

    top_finding = (
        f"RFM Analysis on {len(rfm):,} entities. "
        f"High Value entities drive {high_value_pct}% of total value. "
        f"{declining_count} previously active entities are now Declining. "
    )

    return _make_result(
        analysis_type="rfm_analysis",
        data={
            "total_entities":  len(rfm),
            "total_monetary":  round(float(total_value), 2),
            "segment_stats":   seg_stats.to_dict("records"),
        },
        top_finding=top_finding,
        severity="medium" if declining_count > (len(rfm)*0.2) else "low",
        confidence=compute_confidence(n=len(rfm), base=0.92),
        chart_ready_data={
            "type": "rfm_scatter",
            "r_scores": rfm["r_score"].tolist()[:1000],
            "f_scores": rfm["f_score"].tolist()[:1000],
            "m_scores": rfm["m_score"].tolist()[:1000],
            "segments": rfm["segment"].tolist()[:1000],
        },
    )

def run_pareto_analysis(
    csv_path: str,
    entity_col: str,
    value_col: str,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    for col in [entity_col, value_col]:
        if col not in df.columns:
            return _make_error_result("pareto_analysis", f"Missing {col}")

    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[entity_col, value_col])

    if len(df) == 0:
        return _make_error_result("pareto_analysis", "Insufficient data", "insufficient_data")

    entity_value = df.groupby(entity_col)[value_col].sum().sort_values(ascending=False).reset_index()
    total_value = entity_value[value_col].sum()

    if total_value <= 0:
        return _make_error_result("pareto_analysis", "Total value is zero or negative", "insufficient_data")

    entity_value["cum_value_pct"] = entity_value[value_col].cumsum() / total_value
    entity_value["cum_entity_pct"] = (entity_value.index + 1) / len(entity_value)

    target_idx = (entity_value["cum_entity_pct"] - 0.2).abs().idxmin()
    value_at_20 = round(float(entity_value.loc[target_idx, "cum_value_pct"] * 100), 1)

    target_val_idx = (entity_value["cum_value_pct"] - 0.8).abs().idxmin()
    entities_at_80 = round(float(entity_value.loc[target_val_idx, "cum_entity_pct"] * 100), 1)

    top_finding = (
        f"Pareto Analysis: Top 20% of entities drive {value_at_20}% of total {value_col}. "
        f"80% of value is generated by {entities_at_80}% of entities."
    )

    severity = "high" if value_at_20 > 90 else "medium" if value_at_20 > 70 else "low"

    return _make_result(
        analysis_type="pareto_analysis",
        data={
            "total_entities": len(entity_value),
            "total_value": round(float(total_value), 2),
            "top_20_pct_value": value_at_20,
            "entities_for_80_pct": entities_at_80,
            "entity_value_top_10": entity_value.head(10).to_dict("records"),
        },
        top_finding=top_finding,
        severity=severity,
        confidence=compute_confidence(n=len(entity_value), base=0.95),
        chart_ready_data={
            "type": "pareto_curve",
            "entity_pct": entity_value["cum_entity_pct"].tolist(),
            "value_pct": entity_value["cum_value_pct"].tolist(),
        }
    )

def run_transition_analysis(
    csv_path: str,
    entity_col: str,
    event_col: str,
    time_col: str = None,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    required_cols = [entity_col, event_col]
    for col in required_cols:
        if col not in df.columns:
            return _make_error_result("transition_analysis", f"Column '{col}' not found")

    if time_col and time_col in df.columns:
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df = df.dropna(subset=[entity_col, event_col, time_col])
        df = df.sort_values([entity_col, time_col])
    else:
        df = df.dropna(subset=[entity_col, event_col])
        df = df.sort_values([entity_col])

    if len(df) < 10:
        return _make_error_result("transition_analysis", "Need at least 10 rows", "insufficient_data")

    session_col = "session_id" if "session_id" in df.columns else None

    transition_counts = defaultdict(lambda: defaultdict(int))
    exit_counts = defaultdict(int)
    total_from = defaultdict(int)

    if session_col:
        groups = df.groupby([entity_col, session_col])
    else:
        groups = df.groupby(entity_col)

    for _, group in groups:
        events = group[event_col].tolist()
        for i in range(len(events) - 1):
            src, dst = str(events[i]), str(events[i + 1])
            transition_counts[src][dst] += 1
            total_from[src] += 1
        if events:
            last_evt = str(events[-1])
            exit_counts[last_evt] += 1
            total_from[last_evt] += 1

    event_freq = Counter()
    for evt in df[event_col].astype(str):
        event_freq[evt] += 1
    top_events = [e for e, _ in event_freq.most_common(30)]

    matrix = []
    for src in top_events:
        row = []
        denom = total_from.get(src, 1)
        for dst in top_events:
            prob = round(transition_counts[src][dst] / denom, 4) if denom else 0
            row.append(prob)
        matrix.append(row)

    all_transitions = []
    for src in transition_counts:
        denom = total_from.get(src, 1)
        for dst, count in transition_counts[src].items():
            all_transitions.append({
                "from": src,
                "to": dst,
                "count": count,
                "probability": round(count / denom, 4),
            })
    all_transitions.sort(key=lambda x: x["count"], reverse=True)

    exit_probs = {}
    for evt in top_events:
        denom = total_from.get(evt, 1)
        exit_probs[evt] = round(exit_counts.get(evt, 0) / denom, 4) if denom else 0

    dead_ends = [
        {"event": evt, "exit_prob": prob}
        for evt, prob in sorted(exit_probs.items(), key=lambda x: -x[1])
        if prob > 0.3
    ][:10]

    loops = []
    for evt in top_events:
        denom = total_from.get(evt, 1)
        self_count = transition_counts[evt].get(evt, 0)
        self_prob = round(self_count / denom, 4) if denom else 0
        if self_prob > 0.1:
            loops.append({
                "event": evt,
                "self_prob": self_prob,
                "self_count": self_count,
            })
    loops.sort(key=lambda x: -x["self_prob"])

    n_unique = len(event_freq)
    n_transitions = sum(
        c for src in transition_counts
        for c in transition_counts[src].values()
    )

    stationary_distribution = {}
    try:
        n = len(top_events)
        if n > 1 and matrix:
            P = np.array(matrix[:n], dtype=float)
            row_sums = P.sum(axis=1, keepdims=True)
            zero_rows = (row_sums == 0).flatten()
            P[zero_rows] = 1.0 / n
            row_sums[zero_rows] = 1.0
            P = P / row_sums

            pi = np.ones(n) / n
            for _ in range(200):
                pi_new = pi @ P
                if np.max(np.abs(pi_new - pi)) < 1e-8:
                    break
                pi = pi_new
            pi = pi_new / pi_new.sum()

            stationary_distribution = {
                top_events[i]: round(float(pi[i]), 4)
                for i in range(n)
            }
    except Exception:
        stationary_distribution = {}

    top_pooling = sorted(
        stationary_distribution.items(), key=lambda x: -x[1]
    )[:5]

    top_finding = (
        f"Transition analysis: {n_unique} unique events, "
        f"{n_transitions:,} transitions observed. "
        f"{len(dead_ends)} dead-end events with >30% exit probability. "
        f"{len(loops)} events show significant self-looping (>10%). "
        + (
            f"Highest exit: {dead_ends[0]['event']} ({dead_ends[0]['exit_prob']:.0%}). "
            if dead_ends else ""
        )
        + (
            f"Traffic pools at: {', '.join(e for e, _ in top_pooling[:3])}."
            if top_pooling else ""
        )
    )

    if dead_ends:
        worst_dead_end = dead_ends[0]["event"]
        worst_exit_prob = dead_ends[0]["exit_prob"]
        narrative = {
            "what_it_means": (
                f"{len(dead_ends)} dead-end event(s) found where >30% of users abandon immediately after. "
                f"'{worst_dead_end}' is the worst offender with {worst_exit_prob:.0%} exit probability. "
                + (f"{len(loops)} self-loop event(s) indicate users are stuck repeating the same action."
                   if loops else "")
            ),
            "proposed_fix": (
                f"Redesign the '{worst_dead_end}' screen or step: add a clear next-action prompt or "
                f"breadcrumb navigation. Eliminate self-loops by adding progress confirmation."
            ),
            "severity": "high" if len(dead_ends) > 3 else "medium",
        }
    else:
        narrative = {
            "what_it_means": "No dead-end events found — users have clear navigation paths between all events.",
            "proposed_fix": "Monitor transition matrix as new features are added to catch emerging dead-ends early.",
            "severity": "low",
        }

    return _make_result(
        analysis_type="transition_analysis",
        data={
            "unique_events": n_unique,
            "total_transitions": n_transitions,
            "top_transitions": all_transitions[:20],
            "dead_ends": dead_ends,
            "self_loops": loops[:10],
            "exit_probabilities": exit_probs,
            "stationary_distribution": stationary_distribution,
            "top_pooling_events": [{
                "event": e, "steady_state_pct": round(p * 100, 2)
            } for e, p in top_pooling],
            "narrative": narrative,
        },
        top_finding=top_finding,
        severity="high" if len(dead_ends) > 3 else "medium",
        confidence=compute_confidence(n=int(n_transitions), base=0.88),
        chart_ready_data={
            "type": "transition_heatmap",
            "events": top_events[:20],
            "matrix": [row[:20] for row in matrix[:20]],
            "top_transitions": all_transitions[:15],
        },
    )

def run_dropout_analysis(
    csv_path: str,
    entity_col: str,
    event_col: str,
    time_col: str = None,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    required_cols = [entity_col, event_col]
    for col in required_cols:
        if col not in df.columns:
            return _make_error_result("dropout_analysis", f"Column '{col}' not found")

    if time_col and time_col in df.columns:
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df = df.dropna(subset=[entity_col, event_col, time_col])
        df = df.sort_values([entity_col, time_col])
    else:
        df = df.dropna(subset=[entity_col, event_col])
        df = df.sort_values([entity_col])

    if len(df) < 10:
        return _make_error_result("dropout_analysis", "Need at least 10 rows", "insufficient_data")

    session_col = "session_id" if "session_id" in df.columns else None

    if session_col:
        groups = df.groupby([entity_col, session_col])
    else:
        df["_time_diff"] = df.groupby(entity_col)[time_col].diff()
        df["_new_session"] = (
            df["_time_diff"] > pd.Timedelta(minutes=30)
        ).fillna(True)
        df["_synth_session"] = df.groupby(entity_col)["_new_session"].cumsum()
        groups = df.groupby([entity_col, "_synth_session"])

    last_1_events = []
    last_2_seqs = []
    last_3_seqs = []
    session_lengths = []
    dropout_positions = []

    for _, group in groups:
        events = group[event_col].astype(str).tolist()
        if not events:
            continue
        session_lengths.append(len(events))

        last_1_events.append(events[-1])

        if len(events) >= 2:
            last_2_seqs.append(
                f"{events[-2]} → {events[-1]}"
            )

        if len(events) >= 3:
            last_3_seqs.append(
                f"{events[-3]} → {events[-2]} → {events[-1]}"
            )

        dropout_positions.append(len(events))

    total_sessions = len(last_1_events)
    if total_sessions == 0:
        return _make_error_result("dropout_analysis", "No sessions found", "insufficient_data")

    last_1_counter = Counter(last_1_events)
    last_2_counter = Counter(last_2_seqs)
    last_3_counter = Counter(last_3_seqs)

    event_total_counts = Counter(df[event_col].astype(str))
    dropout_rate = {}
    for evt, last_count in last_1_counter.items():
        total = event_total_counts.get(evt, 1)
        dropout_rate[evt] = {
            "event": evt,
            "times_last": last_count,
            "total_occurrences": total,
            "dropout_rate": round(last_count / total, 4),
        }

    dropout_rate_sorted = sorted(
        dropout_rate.values(),
        key=lambda x: -x["dropout_rate"]
    )

    median_len = float(np.median(session_lengths)) if session_lengths else 1
    early_dropouts = sum(1 for l in session_lengths if l <= max(2, median_len * 0.3))
    early_pct = round(early_dropouts / total_sessions * 100, 1)

    top_last = last_1_counter.most_common(15)
    top_last_2 = last_2_counter.most_common(10)
    top_last_3 = last_3_counter.most_common(10)

    highest_dropout = dropout_rate_sorted[0] if dropout_rate_sorted else {}

    top_finding = (
        f"Dropout analysis across {total_sessions:,} sessions. "
        f"Most common last event: '{top_last[0][0]}' "
        f"({top_last[0][1]} times, "
        f"{round(top_last[0][1]/total_sessions*100,1)}% of exits). "
        f"Highest dropout rate: '{highest_dropout.get('event','')}' "
        f"({highest_dropout.get('dropout_rate',0):.0%} of its occurrences end sessions). "
        f"{early_pct}% of sessions are early dropouts."
    )

    if top_last:
        top_exit_event = top_last[0][0]
        top_exit_pct = round(top_last[0][1] / total_sessions * 100, 1)
        narrative = {
            "what_it_means": (
                f"{early_pct}% of sessions are early dropouts (exiting in the first few events). "
                f"The most common exit point is '{top_exit_event}' which ends "
                f"{top_exit_pct}% of all sessions. "
                f"{'High early dropout signals onboarding friction or mismatched user expectations.' if early_pct > 30 else 'Most users explore meaningfully before exiting.'}"
            ),
            "proposed_fix": (
                f"Investigate the '{top_exit_event}' experience: is it a dead-end, an error state, "
                f"or a completed action? Add re-engagement prompts or clearer next-step CTAs at this point."
            ),
            "severity": "high" if early_pct > 30 else "medium",
        }
    else:
        narrative = {
            "what_it_means": "No dropout patterns detected — all sessions appear to progress through multiple events.",
            "proposed_fix": "Monitor dropout points as user base scales.",
            "severity": "low",
        }

    return _make_result(
        analysis_type="dropout_analysis",
        data={
            "total_sessions": total_sessions,
            "median_session_length": round(median_len, 1),
            "early_dropout_pct": early_pct,
            "top_last_events": [
                {"event": e, "count": c,
                 "pct": round(c / total_sessions * 100, 1)}
                for e, c in top_last
            ],
            "top_last_2_sequences": [
                {"sequence": s, "count": c} for s, c in top_last_2
            ],
            "top_last_3_sequences": [
                {"sequence": s, "count": c} for s, c in top_last_3
            ],
            "dropout_rate_by_event": dropout_rate_sorted[:20],
            "narrative": narrative,
        },
        top_finding=top_finding,
        severity="high" if early_pct > 30 else "medium",
        confidence=compute_confidence(n=int(total_sessions), base=0.86),
        chart_ready_data={
            "type": "dropout_bar",
            "events": [e for e, _ in top_last],
            "counts": [c for _, c in top_last],
            "dropout_rates": [
                dropout_rate.get(e, {}).get("dropout_rate", 0)
                for e, _ in top_last
            ],
        },
    )

def run_event_taxonomy(
    csv_path: str,
    event_col: str,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if event_col not in df.columns:
        return _make_error_result("event_taxonomy", f"Column '{event_col}' not found")

    events = df[event_col].dropna().astype(str)
    if len(events) == 0:
        return _make_error_result("event_taxonomy", "No events found", "insufficient_data")

    TAXONOMY = [
        ("authentication", [
            "login", "log_in", "log in", "signin", "sign_in",
            "sign in", "register", "signup", "sign_up", "sign up",
            "verify", "otp", "password", "auth", "credential",
            "logout", "log_out", "log out", "signout", "sign_out",
            "token", "2fa", "mfa",
        ]),
        ("search", [
            "search", "query", "filter", "find", "browse",
            "discover", "explore", "lookup", "look_up",
            "autocomplete", "suggest",
        ]),
        ("selection", [
            "select", "view", "click", "tap", "open",
            "detail", "item", "product", "choose", "pick",
            "expand", "preview", "inspect", "read",
        ]),
        ("transaction", [
            "pay", "purchase", "buy", "checkout", "check_out",
            "cart", "order", "book", "reserve", "confirm",
            "invoice", "receipt", "billing", "subscribe",
            "redeem", "coupon", "promo",
        ]),
        ("navigation", [
            "back", "home", "menu", "settings", "navigate",
            "page", "tab", "scroll", "swipe", "drawer",
            "close", "dismiss", "return", "exit", "leave",
        ]),
        ("error", [
            "error", "fail", "crash", "retry", "timeout",
            "exception", "denied", "reject", "invalid",
            "404", "500", "unauthorized", "forbidden",
        ]),
        ("notification", [
            "push", "notification", "alert", "remind",
            "message", "inbox", "bell", "badge", "toast",
        ]),
        ("onboarding", [
            "welcome", "tutorial", "intro", "setup",
            "first", "install", "permission", "grant",
            "walkthrough", "tour", "getting_started",
        ]),
        ("social", [
            "share", "like", "comment", "follow", "invite",
            "refer", "review", "rate", "rating", "feedback",
            "recommend", "post",
        ]),
    ]

    def classify_event(event_name: str) -> str:
        lower = event_name.lower().replace("-", "_").replace(" ", "_")
        for category, keywords in TAXONOMY:
            for kw in keywords:
                if kw in lower:
                    return category
        return "other"

    event_counts = events.value_counts()
    unique_events = event_counts.index.tolist()

    event_categories = {}
    for evt in unique_events:
        event_categories[evt] = classify_event(evt)

    event_counts = df[event_col].value_counts().to_dict()

    mapping = {}
    category_counts = defaultdict(int)

    for evt, count in event_counts.items():
        cat = classify_event(evt)
        mapping[evt] = cat
        category_counts[cat] += count

    _ambiguous = [evt for evt, cat in mapping.items() if cat == "other"]
    if len(_ambiguous) > 5:
        try:
            import google.genai as _genai_tax
            import json as _json_tax
            _gc_tax = _genai_tax.Client()
            _valid_cats = [
                "authentication", "search", "selection", "transaction",
                "navigation", "error", "notification", "onboarding", "social", "other"
            ]
            _tax_prompt = (
                f"Classify these event names into one of these categories: "
                f"{', '.join(_valid_cats)}.\n"
                f"Events: {_ambiguous[:40]}\n"
                f"Return ONLY a JSON object mapping event_name → category string. "
                f"Use only the categories listed above."
            )
            _tax_resp = _gc_tax.models.generate_content(
                model="gemini-2.0-flash", contents=_tax_prompt
            )
            import re as _re_tax
            _tax_text = _tax_resp.text.strip()
            _tax_match = _re_tax.search(r'\{[^}]+\}', _tax_text, _re_tax.DOTALL)
            if _tax_match:
                _remapped = _json_tax.loads(_tax_match.group())
                for _evt, _new_cat in _remapped.items():
                    if _evt in mapping and _new_cat in _valid_cats and _new_cat != "other":
                        mapping[_evt] = _new_cat

                category_counts = defaultdict(int)
                for _evt2, _count2 in event_counts.items():
                    category_counts[mapping.get(_evt2, "other")] += _count2
        except Exception:
            pass

    total_events = sum(category_counts.values())
    cat_distribution = {
        cat: {"count": cnt, "pct": round((cnt / total_events) * 100, 2)}
        for cat, cnt in category_counts.items()
    }

    top_events_by_cat = defaultdict(list)
    for evt, count in sorted(event_counts.items(), key=lambda x: -x[1]):
        cat = mapping[evt]
        if len(top_events_by_cat[cat]) < 10:
            top_events_by_cat[cat].append({"event": evt, "count": count})

    top_finding = (
        f"Event Taxonomy: Classified {len(mapping)} unique events. "
        f"Top category is '{max(category_counts, key=category_counts.get)}' "
        f"({max(category_counts.values()) / total_events:.1%} of events). "
        f"{cat_distribution.get('other', {}).get('pct', 0)}% of events remained unclassified ('other')."
    )

    top_cat = max(category_counts, key=category_counts.get) if category_counts else "unknown"
    top_cat_pct = round(category_counts.get(top_cat, 0) / max(total_events, 1) * 100, 1)
    other_pct = cat_distribution.get("other", {}).get("pct", 0)
    narrative = {
        "what_it_means": (
            f"{len(mapping)} unique events classified into functional categories. "
            f"The dominant category is '{top_cat}' at {top_cat_pct}% of all events — "
            f"revealing the primary user activity in this dataset. "
            + (f"{other_pct}% of events couldn’t be classified, which may indicate domain-specific naming conventions."
               if other_pct > 20 else "")
        ),
        "proposed_fix": (
            f"Use this taxonomy to focus UX improvements on the '{top_cat}' category since it dominates user activity. "
            + (f"Rename unclassified events using standard UX terminology to improve future analysis accuracy."
               if other_pct > 20 else "")
        ),
        "severity": "info",
    }

    return _make_result(
        analysis_type="event_taxonomy",
        data={
            "total_unique_events": len(mapping),
            "category_distribution": cat_distribution,
            "top_events_by_category": dict(top_events_by_cat),
            "event_category_mapping": {k: mapping[k] for k in list(mapping.keys())[:100]},
            "narrative": narrative,
        },
        top_finding=top_finding,
        severity="info",
        # Effect size proxy: share of events that got a non-'other' category.
        # 100% classified → strong signal; lots of 'other' → weaker signal.
        confidence=compute_confidence(
            n=int(total_events),
            effect_size=(1.0 - (other_pct / 100.0)) if other_pct is not None else None,
            base=0.92,
        ),
        chart_ready_data={
            "type": "horizontal_bar",
            "labels": list(category_counts.keys()),
            "values": list(category_counts.values()),
            "title": "Event Volume by Functional Category",
        },
    )

def run_user_journey_analysis(
    csv_path: str,
    entity_col: str,
    event_col: str,
    time_col: str = None,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if entity_col not in df.columns or event_col not in df.columns:
        return _make_error_result("user_journey_analysis", "Missing required columns")

    if time_col and time_col in df.columns:
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df = df.sort_values([entity_col, time_col])
    else:
        df = df.sort_values([entity_col])

    journey_stats = []

    for entity, group in df.groupby(entity_col):
        events = group[event_col].tolist()
        if not events: continue

        journey_stats.append({
            "entity_id": str(entity),
            "step_count": len(events),
            "entry_event": str(events[0]),
            "exit_event": str(events[-1]),
            "unique_events": len(set(events))
        })

    if not journey_stats:
        return _make_error_result("user_journey_analysis", "No valid journeys found")

    journey_df = pd.DataFrame(journey_stats)

    common_entries = journey_df["entry_event"].value_counts().head(5).to_dict()
    common_exits = journey_df["exit_event"].value_counts().head(5).to_dict()
    avg_steps = round(journey_df["step_count"].mean(), 2)
    max_steps = int(journey_df["step_count"].max())

    top_finding = f"Analyzed journeys for {len(journey_df):,} entities. Average steps per journey: {avg_steps}. Most common entry: {list(common_entries.keys())[0] if common_entries else 'N/A'}."

    return _make_result(
        analysis_type="user_journey_analysis",
        data={
            "total_entities_tracked": len(journey_df),
            "avg_steps_per_entity": avg_steps,
            "max_steps_per_entity": max_steps,
            "common_entry_events": common_entries,
            "common_exit_events": common_exits,
            "sample_journeys": journey_stats[:10]
        },
        top_finding=top_finding,
        severity="info",
        confidence=compute_confidence(n=len(journey_df), base=0.90),
        chart_ready_data={
            "type": "bar_chart",
            "labels": list(common_entries.keys()),
            "values": list(common_entries.values()),
            "title": "Top Entry Points"
        }
    )

def run_contribution_analysis(csv_path: str, group_col: str, value_col: str) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)
    if group_col not in df.columns or value_col not in df.columns:
        return _make_error_result("contribution_analysis", f"Missing columns: {group_col} or {value_col}")

    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[group_col, value_col])

    if len(df) == 0:
        return _make_error_result("contribution_analysis", "No valid data after numeric conversion", "insufficient_data")

    total_val = df[value_col].sum()
    if total_val == 0:
        return _make_error_result("contribution_analysis", "Total sum of value_col is zero")

    grouped = df.groupby(group_col)[value_col].agg(["sum", "count", "mean"]).reset_index()
    grouped["contribution_pct"] = (grouped["sum"] / total_val * 100).round(2)
    grouped = grouped.sort_values(by="contribution_pct", ascending=False)

    top_group = grouped.iloc[0]
    top_finding = (
        f"The top group '{top_group[group_col]}' accounts for "
        f"{top_group['contribution_pct']}% of the total '{value_col}' "
        f"({top_group['sum']:,.2f} out of {total_val:,.2f} total)."
    )

    top_20_pct_count = max(1, int(len(grouped) * 0.2))
    top_20_pct_contrib = grouped.head(top_20_pct_count)["contribution_pct"].sum()

    if top_20_pct_contrib > 80:
        top_finding += f" High concentration: top 20% of groups drive {top_20_pct_contrib:.1f}% of total value."
        severity = "high"
    else:
        severity = "info"

    return _make_result(
        analysis_type="contribution_analysis",
        data={
            "total_value": float(total_val),
            "group_count": len(grouped),
            "top_20_pct_concentration": float(top_20_pct_contrib),
            "contributions": grouped.to_dict(orient="records")[:20]
        },
        top_finding=top_finding,
        severity=severity,
        # Effect size: Pareto concentration (top-20% share) indicates how
        # decisive the grouping is — 80%+ is a strong finding, 30% is weak.
        confidence=compute_confidence(
            n=len(df),
            effect_size=float(top_20_pct_contrib) / 100.0,
            base=0.95,
        ),
        chart_ready_data={
            "type": "pie_chart" if len(grouped) <= 10 else "bar_chart",
            "labels": grouped[group_col].astype(str).tolist()[:10],
            "values": grouped["contribution_pct"].tolist()[:10],
            "title": f"Top % Contribution by {group_col}"
        }
    )

def run_cross_tab_analysis(csv_path: str, col_a: str, col_b: str) -> dict:
    try:
        from scipy.stats import chi2_contingency
    except ImportError:
        return _make_error_result("cross_tab_analysis", "scipy is required for cross_tab_analysis")

    df = pd.read_csv(csv_path, low_memory=False)
    if col_a not in df.columns or col_b not in df.columns:
        return _make_error_result("cross_tab_analysis", f"Missing columns: {col_a} or {col_b}")

    df = df.dropna(subset=[col_a, col_b])

    if len(df) == 0:
        return _make_error_result("cross_tab_analysis", "No valid rows", "insufficient_data")

    for _col in [col_a, col_b]:
        _freq = df[_col].value_counts(normalize=True)
        _keep = _freq[_freq >= 0.01].index
        if len(_keep) < 5:
            _keep = _freq.nlargest(5).index
        if len(_keep) < df[_col].nunique():
            df[_col] = df[_col].where(df[_col].isin(_keep), other="Other")

    contingency = pd.crosstab(df[col_a], df[col_b])

    if contingency.sum().sum() == 0 or contingency.shape[0] < 2 or contingency.shape[1] < 2:
        return _make_error_result("cross_tab_analysis", "Contingency table invalid (too few groups or counts)")

    chi2_stat, p_val, dof, ex = chi2_contingency(contingency)

    n = contingency.sum().sum()
    min_dim = min(contingency.shape) - 1
    if min_dim > 0 and n > 0:
        cramer_v = np.sqrt(chi2_stat / (n * min_dim))
    else:
        cramer_v = 0.0

    is_significant = p_val < 0.05
    strength = "Strong" if cramer_v > 0.5 else "Moderate" if cramer_v > 0.25 else "Weak"

    if is_significant:
        top_finding = (
            f"Statistically significant relationship found between '{col_a}' and '{col_b}' "
            f"(p={p_val:.4f}). "
        )
        if strength != "Weak":
            top_finding += f"The association is {strength} (Cramér's V: {cramer_v:.2f})."
        severity = "medium"
    else:
        top_finding = f"No significant relationship between '{col_a}' and '{col_b}' (p={p_val:.4f})."
        severity = "info"

    observed = contingency.values
    expected = ex
    diff = observed - expected
    max_idx = np.unravel_index(np.argmax(diff, axis=None), diff.shape)

    driver = ""
    if is_significant:
        row_label = contingency.index[max_idx[0]]
        col_label = contingency.columns[max_idx[1]]
        driver = f"Biggest driver: '{row_label}' co-occurs with '{col_label}' much more than expected."
        top_finding += " " + driver

    return _make_result(
        analysis_type="cross_tab_analysis",
        data={
            "chi2_stat": float(chi2_stat),
            "p_value": float(p_val),
            "cramer_v": float(cramer_v),
            "is_significant": bool(is_significant),
            "strongest_driver": driver,
            "contingency_table": contingency.head(10).to_dict()
        },
        top_finding=top_finding,
        severity=severity,
        # Real p-value + Cramér's V (effect size) are both in-hand — use them.
        confidence=compute_confidence(
            n=int(n),
            p_value=float(p_val),
            effect_size=float(cramer_v),
            base=0.90,
        ),
        chart_ready_data={
            "type": "heatmap",
            "labels": {"x": list(contingency.columns)[:10], "y": list(contingency.index)[:10]},
            "values": contingency.values[:10, :10].tolist(),
            "title": f"Co-occurrence of {col_a} vs {col_b}"
        }
    )

def run_intervention_triggers(
    csv_path: str,
    entity_col: str,
    event_col: str,
    time_col: str,
    session_col: str = "session_id",
    min_dropout_rate: float = 0.80,
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if session_col not in df.columns:
        enriched = csv_path.replace(".csv", "_sessions.csv")
        if os.path.exists(enriched):
            df = pd.read_csv(enriched, low_memory=False)
        else:
            return _make_error_result(
                "intervention_triggers",
                "session_col not found. Run session_detection first.",
            )

    for col in [entity_col, event_col]:
        if col not in df.columns:
            return _make_error_result("intervention_triggers", f"Column '{col}' not found")

    if time_col and time_col in df.columns:
        try:
            df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
            df = df.sort_values([entity_col, time_col])
        except Exception:
            pass

    total_sessions = df[session_col].nunique()
    if total_sessions == 0:
        return _make_error_result("intervention_triggers", "No sessions found", "insufficient_data")

    session_last = (
        df.groupby(session_col)[event_col]
        .apply(lambda x: x.iloc[-1])
        .reset_index()
    )
    session_last.columns = [session_col, "last_event"]

    event_dropout_counts = session_last["last_event"].value_counts().to_dict()
    event_total_sessions = (
        df.groupby(event_col)[session_col].nunique().to_dict()
    )

    rules = []
    for event, dropout_count in event_dropout_counts.items():
        total_with_event = event_total_sessions.get(event, 1)
        dropout_rate = dropout_count / max(total_with_event, 1)
        support = dropout_count / total_sessions
        if dropout_rate >= min_dropout_rate and support >= 0.01:
            rules.append({
                "trigger_sequence": [str(event)],
                "dropout_count": int(dropout_count),
                "sessions_with_trigger": int(total_with_event),
                "dropout_rate": round(dropout_rate, 4),
                "support": round(support, 4),
                "risk_level": (
                    "high"   if dropout_rate >= 0.90 else
                    "medium" if dropout_rate >= 0.80 else
                    "low"
                ),
            })

    rules.sort(key=lambda r: (-r["dropout_rate"], -r["support"]))
    rules = rules[:20]

    high_risk   = [r for r in rules if r["risk_level"] == "high"]
    medium_risk = [r for r in rules if r["risk_level"] == "medium"]

    if rules:
        top_rule = rules[0]
        trigger_name = top_rule["trigger_sequence"][0]
        top_finding = (
            f"Intervention triggers: {len(rules)} dropout rules found "
            f"(threshold {min_dropout_rate:.0%}). "
            f"{len(high_risk)} HIGH risk, {len(medium_risk)} MEDIUM risk. "
            f"Strongest: '{trigger_name}' "
            f"({top_rule['dropout_rate']:.0%} dropout, {top_rule['dropout_count']:,} sessions)."
        )
        narrative = {
            "what_it_means": (
                f"{len(rules)} dropout trigger rules discovered. "
                f"{len(high_risk)} have >90% dropout rates — near-certain abandonment signals. "
                f"Strongest trigger: '{trigger_name}' causes abandonment in "
                f"{top_rule['dropout_rate']:.0%} of sessions where it appears."
            ),
            "proposed_fix": (
                f"Implement real-time intervention at '{trigger_name}': add a help prompt, "
                f"reduce required steps, or route to an alternative path. "
                f"Prioritise HIGH-risk rules first for maximum retention impact."
            ),
            "severity": "high" if high_risk else "medium",
        }
    else:
        top_finding = (
            f"Intervention triggers: no rules found at {min_dropout_rate:.0%} threshold. "
            f"Dropout is distributed across many events rather than clustered."
        )
        narrative = {
            "what_it_means": (
                f"No high-confidence dropout triggers found at the {min_dropout_rate:.0%} threshold. "
                f"Users abandon at varied, unpredictable points rather than at one consistent bottleneck."
            ),
            "proposed_fix": "Lower the dropout_rate threshold or collect more session data to surface weaker patterns.",
            "severity": "low",
        }

    return _make_result(
        analysis_type="intervention_triggers",
        data={
            "total_sessions": total_sessions,
            "rules_found": len(rules),
            "high_risk_rules": len(high_risk),
            "medium_risk_rules": len(medium_risk),
            "min_dropout_rate": min_dropout_rate,
            "rules": rules,
            "narrative": narrative,
        },
        top_finding=top_finding,
        severity="high" if high_risk else "medium",
        # Effect size proxy: top rule's dropout rate (how decisive the trigger is).
        confidence=compute_confidence(
            n=int(total_sessions),
            effect_size=(float(rules[0]["dropout_rate"]) if rules else None),
            base=0.88,
        ),
        chart_ready_data={
            "type": "intervention_bar",
            "triggers": [r["trigger_sequence"][0] for r in rules[:10]],
            "dropout_rates": [r["dropout_rate"] for r in rules[:10]],
            "risk_levels": [r["risk_level"] for r in rules[:10]],
        },
    )

def run_session_classification(
    csv_path: str,
    entity_col: str,
    event_col: str,
    time_col: str,
    session_col: str = "session_id",
) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)

    if session_col not in df.columns:
        enriched = csv_path.replace(".csv", "_sessions.csv")
        if os.path.exists(enriched):
            df = pd.read_csv(enriched, low_memory=False)
        else:
            return _make_error_result(
                "session_classification",
                "session_col not found. Run session_detection first.",
            )

    for col in [entity_col, event_col]:
        if col not in df.columns:
            return _make_error_result("session_classification", f"Column '{col}' not found")

    user_stats = (
        df.groupby(entity_col)
        .agg(
            total_events=(event_col, "count"),
            unique_events=(event_col, "nunique"),
            sessions=(session_col, "nunique"),
        )
        .reset_index()
    )

    user_stats["diversity_ratio"] = (
        user_stats["unique_events"] / user_stats["total_events"].clip(lower=1)
    ).round(4)

    user_stats["repetition_ratio"] = (
        (user_stats["total_events"] - user_stats["unique_events"]) /
        user_stats["total_events"].clip(lower=1)
    ).round(4)

    event_freq = df[event_col].value_counts(normalize=True)
    conversion_signal_events = set(event_freq[event_freq < 0.05].index.tolist())

    user_has_conversion = (
        df[df[event_col].isin(conversion_signal_events)]
        .groupby(entity_col)
        .size()
        .reset_index(name="conversion_events")
    )
    user_stats = user_stats.merge(user_has_conversion, on=entity_col, how="left")
    user_stats["conversion_events"] = user_stats["conversion_events"].fillna(0)

    depth_p50 = user_stats["total_events"].median()
    depth_p75 = user_stats["total_events"].quantile(0.75)
    diversity_p50 = user_stats["diversity_ratio"].median()

    def classify_user(row):
        depth = row["total_events"]
        diversity = row["diversity_ratio"]
        repetition = row["repetition_ratio"]
        conversion = row["conversion_events"]
        if conversion > 0 and depth >= depth_p50:
            return "Converter"
        elif depth >= depth_p75 and diversity >= diversity_p50:
            return "Attempter"
        elif repetition > 0.4 and depth >= depth_p50:
            return "Shopper"
        else:
            return "Browser"

    user_stats["persona"] = user_stats.apply(classify_user, axis=1)

    persona_counts = user_stats["persona"].value_counts().to_dict()
    total_users = len(user_stats)

    persona_breakdown = []
    for persona in ["Converter", "Attempter", "Shopper", "Browser"]:
        count = persona_counts.get(persona, 0)
        pct = round(count / max(total_users, 1) * 100, 1)
        subset = user_stats[user_stats["persona"] == persona]
        persona_breakdown.append({
            "persona": persona,
            "count": count,
            "pct": pct,
            "avg_events": round(float(subset["total_events"].mean()), 1) if count > 0 else 0.0,
            "avg_sessions": round(float(subset["sessions"].mean()), 1) if count > 0 else 0.0,
            "avg_diversity": round(float(subset["diversity_ratio"].mean()), 3) if count > 0 else 0.0,
        })

    non_converters = [
        (p["persona"], p["count"]) for p in persona_breakdown
        if p["persona"] != "Converter" and p["count"] > 0
    ]
    biggest_leak = max(non_converters, key=lambda x: x[1]) if non_converters else ("Browser", 0)

    converter_pct = round(persona_counts.get("Converter", 0) / max(total_users, 1) * 100, 1)
    browser_pct   = round(persona_counts.get("Browser", 0)   / max(total_users, 1) * 100, 1)

    severity = (
        "high"   if converter_pct < 10 else
        "medium" if converter_pct < 30 else
        "low"
    )

    top_finding = (
        f"Session classification: {total_users:,} users. "
        f"Converters: {converter_pct}%, "
        f"Attempters: {persona_counts.get('Attempter', 0):,}, "
        f"Shoppers: {persona_counts.get('Shopper', 0):,}, "
        f"Browsers: {browser_pct}%. "
        f"Biggest leak: {biggest_leak[0]} ({biggest_leak[1]:,} users)."
    )

    fix_map = {
        "Attempter": ("They reached the key step but did not complete it. "
                      "Simplify the final action or add social proof/reassurance."),
        "Shopper":   ("They explore but do not commit. "
                      "Add comparison tools, urgency signals, or clearer decision prompts."),
        "Browser":   ("They barely engage. "
                      "Improve the landing experience and clarify the core value proposition."),
    }

    narrative = {
        "what_it_means": (
            f"Only {converter_pct}% of users are Converters who completed a meaningful action. "
            f"{browser_pct}% are Browsers who exit without engaging meaningfully. "
            f"The biggest opportunity is the '{biggest_leak[0]}' segment "
            f"({biggest_leak[1]:,} users) — they show intent but do not convert."
        ),
        "proposed_fix": (
            f"Target '{biggest_leak[0]}' users with personalised interventions: "
            + fix_map.get(biggest_leak[0],
                          "Review the identified segment's journey for friction points.")
        ),
        "severity": severity,
    }

    return _make_result(
        analysis_type="session_classification",
        data={
            "total_users": total_users,
            "persona_breakdown": persona_breakdown,
            "biggest_leak_segment": biggest_leak[0],
            "converter_pct": converter_pct,
            "conversion_signal_event_count": len(conversion_signal_events),
            "narrative": narrative,
        },
        top_finding=top_finding,
        severity=severity,
        confidence=compute_confidence(n=int(total_users), base=0.82),
        chart_ready_data={
            "type": "persona_donut",
            "personas": [p["persona"] for p in persona_breakdown],
            "counts":   [p["count"] for p in persona_breakdown],
            "pcts":     [p["pct"] for p in persona_breakdown],
        },
    )
