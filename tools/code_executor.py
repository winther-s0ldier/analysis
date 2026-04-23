import os
import sys
import ast
import json
import logging
import traceback
from typing import Optional

_result_store: dict = {}

def get_analysis_result(
    session_id: str,
    analysis_id: str,
) -> dict | None:
    return _result_store.get(f"{session_id}:{analysis_id}")

def store_analysis_result(
    session_id: str,
    analysis_id: str,
    result: dict,
) -> None:
    _result_store[f"{session_id}:{analysis_id}"] = result

def lookup_library_function(analysis_type: str) -> dict:
    from tools.analysis_library import LIBRARY_REGISTRY

    entry = LIBRARY_REGISTRY.get(analysis_type)
    if not entry:
        return {
            "exists": False,
            "function_name": None,
            "required_args": None,
            "description": f"No library function for '{analysis_type}'. Write from scratch.",
            "import_statement": None,
            "example_call": None,
        }

    fn = entry["function"]
    args = entry["required_args"]

    import_stmt = f"from tools.analysis_library import {fn}"
    example = f"result = {fn}(" + ", ".join(f"{a}={a}" for a in args) + ")"

    return {
        "exists": True,
        "function_name": fn,
        "required_args": args,
        "description": entry["description"],
        "import_statement": import_stmt,
        "example_call": example,
        "col_role": entry.get("col_role"),
    }

def check_precomputed_result(
    session_id: str,
    analysis_type: str,
) -> dict:
    try:
        from main import sessions
        state = sessions.get(session_id)
        if state:
            precomputed = state.get_precomputed(analysis_type)
            if precomputed:
                return {"exists": True, "result": precomputed}
    except Exception as _pre_err:
        logging.warning(f"Precomputed result lookup failed for {analysis_type}: {_pre_err}")
    return {"exists": False, "result": None}

def validate_code(code: str, csv_path: str) -> dict:
    issues = []

    try:
        ast.parse(code)
    except SyntaxError as e:
        return {
            "valid": False,
            "issues": [f"SyntaxError: {str(e)}"],
            "fix_instructions": f"Fix syntax error at line {e.lineno}: {e.msg}",
        }

    if "def analyze(" not in code and "def analyze (" not in code:
        issues.append(
            "Missing analyze() function. "
            "Code must define def analyze(csv_path) that returns a result dict."
        )

    dangerous = [
        ("os.remove", "File deletion not allowed"),
        ("os.rmdir", "Directory deletion not allowed"),
        ("shutil.rmtree", "Tree deletion not allowed"),
        ("sys.exit", "sys.exit not allowed"),
        ("subprocess", "subprocess not allowed"),
        ("__import__", "__import__ not allowed"),
        ("eval(", "eval() not allowed"),
        ("exec(", "exec() not allowed"),
    ]
    for pattern, msg in dangerous:
        if pattern in code:
            issues.append(f"Unsafe code: {msg}")

    if issues:
        return {
            "valid": False,
            "issues": issues,
            "fix_instructions": (
                "Fix all issues listed above. "
                "Ensure code has def analyze(csv_path) and no unsafe operations."
            ),
        }

    try:
        import pandas as pd
        import tempfile

        df_sample = pd.read_csv(csv_path, nrows=5, low_memory=False)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False,
        ) as tmp:
            df_sample.to_csv(tmp.name, index=False)
            tmp_path = tmp.name

        namespace: dict = {"__file__": os.path.abspath(__file__)}
        exec(code, namespace)

        if "analyze" in namespace:
            test_result = namespace["analyze"](tmp_path)
            if test_result is None:
                issues.append("analyze() returned None on dry run. Must return a dict.")

        os.unlink(tmp_path)

    except Exception as e:
        issues.append(f"Dry run failed: {str(e)}")

    if issues:
        return {
            "valid": False,
            "issues": issues,
            "fix_instructions": "Fix the dry run failure. Test with a small subset of data first.",
        }

    return {"valid": True, "issues": [], "fix_instructions": None}

def execute_analysis(
    code: str,
    csv_path: str,
    analysis_id: str,
    analysis_type: str,
    output_folder: str,
) -> dict:
    try:
        os.makedirs(output_folder, exist_ok=True)

        namespace: dict = {"__file__": os.path.abspath(__file__)}
        exec(code, namespace)

        if "analyze" not in namespace:
            return {
                "status": "error",
                "execution_status": "error",
                "execution_error": "analyze() function not found after exec",
            }

        result = namespace["analyze"](csv_path)

        if result is None:
            return {
                "status": "error",
                "execution_status": "error",
                "execution_error": "analyze() returned None",
            }

        if result.get("status") == "error":
            result["execution_status"] = "error"
            result["execution_error"] = result.get("error", "analyze() returned status=error")
            return result

        if "status" not in result:
            result["status"] = "success"
        if "analysis_type" not in result:
            result["analysis_type"] = analysis_type
        if "top_finding" not in result:
            result["top_finding"] = ""
        if "data" not in result:
            result["data"] = {}
        if "chart_ready_data" not in result:
            result["chart_ready_data"] = {}

        chart_path = None
        if result.get("chart_ready_data"):
            chart_path = generate_chart(
                result["chart_ready_data"],
                analysis_id,
                analysis_type,
                output_folder,
            )
            if chart_path:
                result["chart_file_path"] = chart_path
                print(f"INFO: [{analysis_id}] Chart generated: {chart_path}")
            else:
                print(f"WARNING: [{analysis_id}] generate_chart() returned None despite chart_ready_data being present")
        else:
            print(f"INFO: [{analysis_id}] No chart_ready_data — skipping chart generation")

        flat = {
            "execution_status": "success",
            "execution_error": None,
        }
        flat.update(result)
        return flat

    except Exception as e:
        return {
            "status": "error",
            "execution_status": "error",
            "execution_error": (
                f"{type(e).__name__}: {str(e)}\n"
                f"{traceback.format_exc()[-500:]}"
            ),
        }

def validate_output_quality(
    result: dict,
    analysis_type: str,
) -> dict:
    issues = []

    if not result:
        return {
            "quality_pass": False,
            "issues": ["Result is None or empty"],
            "fix_instructions": "Return a valid result dict",
            "severity": "fail",
        }

    if result.get("execution_status") == "error":
        err = result.get("execution_error", "unknown error")
        return {
            "quality_pass": False,
            "issues": [f"Execution failed: {err}"],
            "fix_instructions": f"Fix your analyze() code. Error: {err}",
            "severity": "fail",
        }

    if result.get("status") != "success":
        issues.append(
            f"Result status is '{result.get('status')}' not 'success'."
        )

    finding = result.get("top_finding", "")
    if not finding or len(finding) < 10:
        issues.append(
            "top_finding is empty or too short. Must be a real insight with numbers."
        )

    placeholders = ["n/a", "none", "null", "undefined", "todo", "placeholder"]
    if finding and (
        finding.lower().strip() in placeholders
        or finding.lower().strip().startswith("todo")
    ):
        issues.append(
            "top_finding contains placeholder text. Must contain real numbers and insights."
        )

    data = result.get("data", {})
    if not data:
        issues.append("data dict is empty. Must contain computed values.")

    chart_data = result.get("chart_ready_data", {})
    if not chart_data:
        if analysis_type not in ("missing_data_analysis", "missing_data"):
            issues.append("chart_ready_data is empty. Must contain data for visualization.")

    if analysis_type == "session_detection":
        if data.get("total_sessions", 0) == 0:
            issues.append("session_detection produced 0 sessions. Check entity_col and time_col.")

    if analysis_type == "funnel_analysis":
        if not data.get("funnel_metrics"):
            issues.append("funnel_analysis has no funnel_metrics. Check event_col values.")

    if issues:
        return {
            "quality_pass": False,
            "issues": issues,
            "fix_instructions": "Fix the following issues and re-run: " + "; ".join(issues),
            "severity": "fail",
        }

    return {
        "quality_pass": True,
        "issues": [],
        "fix_instructions": None,
        "severity": "ok",
    }

def submit_result(
    session_id: str,
    analysis_id: str,
    analysis_type: str,
    result: dict,
) -> str:
    result["analysis_id"] = analysis_id
    store_analysis_result(session_id, analysis_id, result)

    _chart_path = result.get("chart_file_path")
    if _chart_path and os.path.exists(_chart_path):
        try:
            from agent_servers import artifacts as _art
            _art.register_artifact(
                session_id=session_id,
                name=f"{analysis_id}.png",
                mime_type="image/png",
                uri_path=_chart_path,
                kind="chart",
                metadata={"analysis_id": analysis_id, "analysis_type": analysis_type},
            )
        except Exception as _art_err:
            logging.warning(f"Failed to register chart artifact for {analysis_id}: {_art_err}")

    try:
        from main import sessions
        from pipeline_types import create_message, Intent
        state = sessions.get(session_id)
        if state:
            state.store_result(analysis_id, result)
            msg = create_message(
                sender="coder_agent",
                recipient="orchestrator",
                intent=Intent.ANALYSIS_COMPLETE,
                payload={
                    "analysis_id": analysis_id,
                    "analysis_type": analysis_type,
                    "top_finding": result.get("top_finding", ""),
                    "severity": result.get("severity", "info"),
                    "has_chart": bool(result.get("chart_file_path")),
                },
                session_id=session_id,
            )
            state.post_message(msg)
    except Exception as _msg_err:
        logging.warning(f"Failed to post ANALYSIS_COMPLETE message for {analysis_id}: {_msg_err}")

    return f"Result stored for {analysis_id} ({analysis_type}) in session {session_id}."

def generate_chart(
    chart_ready_data: dict,
    analysis_id: str,
    analysis_type: str,
    output_folder: str,
) -> str | None:
    try:
        import plotly.graph_objects as go
        import plotly.express as px

        output_folder = os.path.abspath(output_folder)
        os.makedirs(output_folder, exist_ok=True)

        _TYPE_ALIASES = {
            "bar":       "bar_chart",
            "scatter":   "generic_scatter",
            "line":      "generic_line",
            "histogram": "histogram_box",
            "pie":       "pie_chart",
            "donut":     "pie_chart",
            "grouped_bar": "bar_chart",
            "stacked_bar": "bar_chart",
        }
        chart_type = chart_ready_data.get("type", "")
        chart_type = _TYPE_ALIASES.get(chart_type, chart_type)
        print(f"DEBUG: Generating chart '{chart_type}' for {analysis_id} ({analysis_type}) -> {output_folder}")
        fig = None

        if chart_type == "histogram_box":
            fig = go.Figure()
            fig.add_trace(go.Histogram(
                x=chart_ready_data.get("hist_values", []),
                name="Distribution",
            ))

        elif chart_type == "frequency_bar":
            fig = go.Figure(go.Bar(
                x=chart_ready_data.get("labels", []),
                y=chart_ready_data.get("values", []),
            ))

        elif chart_type == "correlation_heatmap":
            fig = go.Figure(go.Heatmap(
                z=chart_ready_data.get("matrix", []),
                x=chart_ready_data.get("columns", []),
                y=chart_ready_data.get("columns", []),
                colorscale="RdBu",
                zmid=0,
            ))

        elif chart_type == "anomaly_scatter":
            vals = chart_ready_data.get("all_values", [])
            fig = go.Figure(go.Scatter(
                y=vals, mode="markers", name="Values",
            ))

        elif chart_type == "missing_bar":
            fig = go.Figure(go.Bar(
                x=chart_ready_data.get("columns", []),
                y=chart_ready_data.get("null_pcts", []),
            ))

        elif chart_type == "trend_line":
            times = chart_ready_data.get("times", [])
            values = chart_ready_data.get("values", [])
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=times, y=values, name="Value"))
            if chart_ready_data.get("roll7"):
                fig.add_trace(go.Scatter(
                    x=times, y=chart_ready_data["roll7"], name="7-period MA",
                ))

        elif chart_type == "funnel_bar":
            fig = go.Figure(go.Funnel(
                y=chart_ready_data.get("steps", []),
                x=chart_ready_data.get("counts", []),
            ))

        elif chart_type == "friction_heatmap":
            fig = go.Figure(go.Bar(
                x=chart_ready_data.get("events", []),
                y=chart_ready_data.get("repetition_rates", []),
                name="Repetition Rate",
            ))

        elif chart_type == "survival_curve":
            fig = go.Figure(go.Scatter(
                x=chart_ready_data.get("steps", []),
                y=chart_ready_data.get("survival_pcts", []),
                mode="lines+markers", name="Survival %",
            ))

        elif chart_type == "segment_donut":
            segs = chart_ready_data.get("segments", [])
            fig = go.Figure(go.Pie(
                labels=[f"Segment {s['segment_id']}" for s in segs],
                values=[s["size"] for s in segs],
                hole=0.4,
            ))

        elif chart_type == "pareto_bar":
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=chart_ready_data.get("categories", []),
                y=chart_ready_data.get("values", []),
                name="Value",
            ))
            fig.add_trace(go.Scatter(
                x=chart_ready_data.get("categories", []),
                y=chart_ready_data.get("cumulative", []),
                name="Cumulative %", yaxis="y2",
            ))

        elif chart_type == "rfm_donut":
            fig = go.Figure(go.Pie(
                labels=chart_ready_data.get("tiers", []),
                values=chart_ready_data.get("counts", []),
                hole=0.4,
            ))

        elif chart_type == "sequence_bar":
            fig = go.Figure(go.Bar(
                x=chart_ready_data.get("patterns", []),
                y=chart_ready_data.get("counts", []),
            ))

        elif chart_type == "cohort_heatmap":
            records = chart_ready_data.get("cohort_data", [])
            if records:
                import pandas as pd
                cohort_df = pd.DataFrame(records)
                pivot = cohort_df.pivot_table(
                    index="cohort", columns="period_index",
                    values="retention_rate",
                )
                fig = go.Figure(go.Heatmap(
                    z=pivot.values.tolist(),
                    x=[str(c) for c in pivot.columns],
                    y=[str(i) for i in pivot.index],
                    colorscale="Blues",
                ))

        elif chart_type == "transition_heatmap":
            events = chart_ready_data.get("events", [])
            matrix = chart_ready_data.get("matrix", [])
            if events and matrix:
                fig = go.Figure(go.Heatmap(
                    z=matrix, x=events, y=events,
                    colorscale="YlOrRd", zmin=0, zmax=1,
                    text=[[f"{v:.2f}" for v in row] for row in matrix],
                    texttemplate="%{text}",
                    hovertemplate="From: %{y}<br>To: %{x}<br>P: %{z:.3f}<extra></extra>",
                ))
                fig.update_layout(
                    xaxis_title="Next Event",
                    yaxis_title="Current Event",
                    yaxis_autorange="reversed",
                )

        elif chart_type == "dropout_bar":

            events = (
                chart_ready_data.get("events") or
                chart_ready_data.get("event_names") or
                chart_ready_data.get("event_types") or
                chart_ready_data.get("labels") or
                []
            )
            counts = (
                chart_ready_data.get("counts") or
                chart_ready_data.get("dropout_counts") or
                chart_ready_data.get("values") or
                chart_ready_data.get("frequencies") or
                []
            )
            rates = (
                chart_ready_data.get("dropout_rates") or
                chart_ready_data.get("rates") or
                chart_ready_data.get("dropout_rate") or
                []
            )

            if not events or not counts:
                str_keys = [k for k, v in chart_ready_data.items() if isinstance(v, list) and v and isinstance(v[0], str)]
                num_keys = [k for k, v in chart_ready_data.items() if isinstance(v, list) and v and isinstance(v[0], (int, float))]
                if str_keys and num_keys:
                    events = chart_ready_data[str_keys[0]]
                    counts = chart_ready_data[num_keys[0]]
            if events and counts:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=events, y=counts,
                    name="Times as Last Event",
                    marker_color="#e94560",
                ))
                if rates:
                    fig.add_trace(go.Scatter(
                        x=events, y=rates,
                        name="Dropout Rate", yaxis="y2",
                        mode="lines+markers", marker_color="#00ff88",
                    ))
                    fig.update_layout(yaxis2=dict(
                        title="Dropout Rate", overlaying="y",
                        side="right", range=[0, 1],
                    ))

        elif chart_type == "taxonomy_donut":
            cats = chart_ready_data.get("categories", [])
            counts = chart_ready_data.get("counts", [])
            if cats and counts:
                fig = go.Figure(go.Pie(
                    labels=cats, values=counts, hole=0.45,
                    textinfo="label+percent",
                    hovertemplate="%{label}<br>Count: %{value:,}<br>%{percent}<extra></extra>",
                ))

        elif chart_type == "session_length_histogram":
            event_counts = chart_ready_data.get("event_counts", [])
            duration_mins = chart_ready_data.get("duration_minutes", [])
            fig = go.Figure()
            if event_counts:
                fig.add_trace(go.Histogram(
                    x=event_counts, name="Events per Session",
                    marker_color="#e94560", opacity=0.75,
                ))
            if duration_mins:
                fig.add_trace(go.Histogram(
                    x=duration_mins, name="Duration (min)",
                    marker_color="#00ff88", opacity=0.65,
                    xaxis="x2", yaxis="y2",
                ))
                fig.update_layout(
                    xaxis=dict(title="Events per Session", domain=[0, 0.45]),
                    xaxis2=dict(title="Duration (minutes)", domain=[0.55, 1], anchor="y2"),
                    yaxis=dict(title="Frequency"),
                    yaxis2=dict(title="Frequency", anchor="x2", overlaying=None, side="right"),
                    barmode="overlay",
                )

        elif chart_type == "rules_card":
            rules = chart_ready_data.get("rules", [])
            if rules:
                header_vals = ["Antecedent", "Consequent", "Support", "Confidence", "Lift"]
                antecedents, consequents, supports, confidences, lifts = [], [], [], [], []
                for r in rules[:10]:
                    ant = r.get("antecedent", "")
                    if isinstance(ant, (list, tuple)):
                        ant = ", ".join(str(a) for a in ant)
                    antecedents.append(str(ant))
                    con = r.get("consequent", "")
                    if isinstance(con, (list, tuple)):
                        con = ", ".join(str(c) for c in con)
                    consequents.append(str(con))
                    supports.append(f"{r.get('support', 0):.3f}")
                    confidences.append(f"{r.get('confidence', 0):.2f}")
                    lifts.append(f"{r.get('lift', 0):.2f}")
                fig = go.Figure(go.Table(
                    header=dict(
                        values=header_vals,
                        fill_color="#1a1a2e",
                        font=dict(color="white", size=12),
                        align="left",
                    ),
                    cells=dict(
                        values=[antecedents, consequents, supports, confidences, lifts],
                        fill_color="#16213e",
                        font=dict(color="#e0e0e0", size=11),
                        align="left",
                    ),
                ))

        elif chart_type == "decomposition":
            times = chart_ready_data.get("times", [])
            original = chart_ready_data.get("original", [])
            trend = chart_ready_data.get("trend", [])
            seasonal = chart_ready_data.get("seasonal", [])
            residual = chart_ready_data.get("residual", [])
            if times and original:
                from plotly.subplots import make_subplots
                fig = make_subplots(
                    rows=4, cols=1, shared_xaxes=True,
                    subplot_titles=["Original", "Trend", "Seasonal", "Residual"],
                    vertical_spacing=0.06,
                )
                fig.add_trace(go.Scatter(
                    x=times, y=original, name="Original", mode="lines",
                ), row=1, col=1)
                if trend:
                    fig.add_trace(go.Scatter(
                        x=times, y=trend, name="Trend", mode="lines",
                        line=dict(color="#e94560"),
                    ), row=2, col=1)
                if seasonal:
                    fig.add_trace(go.Scatter(
                        x=times, y=seasonal, name="Seasonal", mode="lines",
                        line=dict(color="#00ff88"),
                    ), row=3, col=1)
                if residual:
                    fig.add_trace(go.Scatter(
                        x=times, y=residual, name="Residual", mode="lines",
                        line=dict(color="#ffa500"),
                    ), row=4, col=1)
                fig.update_layout(height=800)

        elif chart_type == "pareto_curve":
            entity_pct = chart_ready_data.get("entity_pct", [])
            value_pct = chart_ready_data.get("value_pct", [])
            if entity_pct and value_pct:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=entity_pct, y=value_pct,
                    mode="lines+markers", name="Cumulative Value %",
                    line=dict(color="#e94560"),
                ))
                fig.add_trace(go.Scatter(
                    x=[0, 100], y=[0, 100], mode="lines",
                    name="Equality Line", line=dict(color="#555", dash="dash"),
                ))
                fig.update_layout(
                    xaxis_title="Cumulative % of Entities",
                    yaxis_title="Cumulative % of Value",
                )

        elif chart_type == "rfm_scatter":
            r_scores = chart_ready_data.get("r_scores", [])
            f_scores = chart_ready_data.get("f_scores", [])
            m_scores = chart_ready_data.get("m_scores", [])
            segments = chart_ready_data.get("segments", [])
            if r_scores and f_scores:
                fig = go.Figure(go.Scatter(
                    x=r_scores, y=f_scores, mode="markers",
                    marker=dict(
                        size=6, color=m_scores or None,
                        colorscale="Viridis",
                        showscale=bool(m_scores),
                        colorbar=dict(title="Monetary") if m_scores else None,
                    ),
                    text=segments or None,
                    hovertemplate="Recency: %{x}<br>Frequency: %{y}<br>Segment: %{text}<extra></extra>",
                ))
                fig.update_layout(
                    xaxis_title="Recency Score",
                    yaxis_title="Frequency Score",
                )

        elif chart_type == "horizontal_bar":
            labels = chart_ready_data.get("labels", [])
            values = chart_ready_data.get("values", [])
            if labels and values:
                fig = go.Figure(go.Bar(
                    x=values, y=labels,
                    orientation="h",
                    marker_color="#3B5BDB",
                    hovertemplate="%{y}: %{x:,}<extra></extra>",
                ))
                fig.update_layout(
                    xaxis_title="Count",
                    yaxis=dict(autorange="reversed"),
                )

        elif chart_type == "bar_chart":
            labels = chart_ready_data.get("labels", [])
            values = chart_ready_data.get("values", [])
            if labels and values:
                fig = go.Figure(go.Bar(
                    x=labels, y=values,
                    marker_color="#3B5BDB",
                    hovertemplate="%{x}: %{y:,}<extra></extra>",
                ))
                fig.update_layout(yaxis_title="Count")

        elif chart_type == "pie_chart":
            labels = chart_ready_data.get("labels", [])
            values = chart_ready_data.get("values", [])
            if labels and values:
                fig = go.Figure(go.Pie(
                    labels=labels, values=values,
                    hole=0.35,
                    textinfo="label+percent",
                    hovertemplate="%{label}: %{value:.1f}%<extra></extra>",
                ))

        elif chart_type == "heatmap":
            lbl = chart_ready_data.get("labels", {})
            x_labels = lbl.get("x", []) if isinstance(lbl, dict) else chart_ready_data.get("x", [])
            y_labels = lbl.get("y", []) if isinstance(lbl, dict) else chart_ready_data.get("y", [])
            matrix   = chart_ready_data.get("values", chart_ready_data.get("z", []))
            if matrix:
                fig = go.Figure(go.Heatmap(
                    z=matrix, x=x_labels, y=y_labels,
                    colorscale="Blues",
                    hovertemplate="x=%{x}<br>y=%{y}<br>count=%{z:,}<extra></extra>",
                ))
                fig.update_layout(yaxis_autorange="reversed")

        elif chart_type == "intervention_bar":
            triggers      = chart_ready_data.get("triggers", [])
            dropout_rates = chart_ready_data.get("dropout_rates", [])
            risk_levels   = chart_ready_data.get("risk_levels", [])
            if triggers and dropout_rates:
                color_map = {"critical": "#C92A2A", "high": "#E67700", "medium": "#F59F00", "low": "#2F9E44"}
                colors = [color_map.get(str(r).lower(), "#3B5BDB") for r in risk_levels] if risk_levels else "#3B5BDB"
                fig = go.Figure(go.Bar(
                    x=[f"{r:.1%}" for r in dropout_rates],
                    y=triggers,
                    orientation="h",
                    marker_color=colors,
                    hovertemplate="%{y}<br>Dropout: %{x}<extra></extra>",
                ))
                fig.update_layout(
                    xaxis_title="Dropout Rate",
                    yaxis=dict(autorange="reversed"),
                )

        elif chart_type == "persona_donut":
            personas = chart_ready_data.get("personas", [])
            counts   = chart_ready_data.get("counts", [])
            pcts     = chart_ready_data.get("pcts", [])
            if personas and counts:
                fig = go.Figure(go.Pie(
                    labels=personas,
                    values=counts,
                    hole=0.42,
                    textinfo="label+percent",
                    hovertemplate="%{label}<br>%{value:,} sessions (%{percent})<extra></extra>",
                ))

        elif chart_type == "generic_scatter":
            x = chart_ready_data.get("x", chart_ready_data.get("labels", []))
            y = chart_ready_data.get("y", chart_ready_data.get("values", []))
            if x and y:
                fig = go.Figure(go.Scatter(
                    x=x, y=y, mode="markers+lines",
                    marker=dict(size=6, color="#6366F1"),
                    hovertemplate="%{x}: %{y}<extra></extra>",
                ))

        elif chart_type == "generic_line":
            x = chart_ready_data.get("x", chart_ready_data.get("labels", chart_ready_data.get("times", [])))
            y = chart_ready_data.get("y", chart_ready_data.get("values", []))
            if x and y:
                fig = go.Figure(go.Scatter(
                    x=x, y=y, mode="lines+markers",
                    line=dict(color="#6366F1"),
                    hovertemplate="%{x}: %{y}<extra></extra>",
                ))

        if fig is None:

            _labels = (chart_ready_data.get("labels") or chart_ready_data.get("x")
                       or chart_ready_data.get("categories") or chart_ready_data.get("names") or [])
            _values = (chart_ready_data.get("values") or chart_ready_data.get("y")
                       or chart_ready_data.get("counts") or chart_ready_data.get("amounts") or [])
            if _labels and _values and len(_labels) == len(_values):
                print(f"INFO: generate_chart — using universal fallback for chart_type='{chart_type}' "
                      f"(analysis_id={analysis_id})")

                try:
                    [float(v) for v in _values]
                    fig = go.Figure(go.Bar(
                        x=_labels, y=_values,
                        marker_color="#6366F1",
                        hovertemplate="%{x}: %{y:,}<extra></extra>",
                    ))
                except (ValueError, TypeError):
                    pass

        if fig is None:
            print(f"WARNING: generate_chart — no handler for chart_type='{chart_type}' "
                  f"(analysis_id={analysis_id}, analysis_type={analysis_type}). "
                  f"Available keys in chart_ready_data: {list(chart_ready_data.keys())}")
            return None

        a_title = str(analysis_type or "Analysis").replace('_', ' ').title()
        fig.update_layout(
            height=550,
            title=a_title,
            template="plotly_white",
        )

        chart_filename = f"{analysis_id}_{analysis_type}.html"
        chart_path = os.path.join(output_folder, chart_filename)
        fig.write_html(chart_path)

        try:
            png_path = chart_path.replace(".html", ".png")
            fig.write_image(png_path)
        except Exception as img_e:
            print(f"DEBUG: Could not generate PNG chart (Kaleido/Orca may be missing): {img_e}")

        return chart_path

    except Exception as chart_exc:
        print(f"ERROR: generate_chart crashed for {analysis_id} ({analysis_type}): "
              f"{type(chart_exc).__name__}: {chart_exc}\n{traceback.format_exc()[-600:]}")
        return None
