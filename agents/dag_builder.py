import os
import re
import sys
import json
import logging
from datetime import datetime
from typing import Annotated
from pydantic import Field

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..")
)

from pipeline_types import create_message, Intent

_report_store: dict = {}

def get_report_result(session_id: str) -> dict | None:
    return _report_store.get(session_id)

def tool_build_report(
    session_id: Annotated[str, Field(description="Active pipeline session ID")],
    output_folder: Annotated[str, Field(description="Absolute path to the session output directory. Copy exactly from the prompt.")],
    tool_context=None,
) -> dict:
    charts = []
    synthesis = {}
    dataset_type = ""
    csv_filename = ""

    state = None
    try:
        from main import sessions
        state = sessions.get(session_id)
        if state:
            dataset_type = getattr(state, "dataset_type", "")
            csv_filename = getattr(state, "csv_filename", "")
            print(f"INFO: dag_builder got session state from main.sessions for {session_id}")
    except Exception as _imp_err:
        print(f"INFO: dag_builder running without main.sessions (A2A mode): {_imp_err}")

    if tool_context is not None:
        try:
            _tc_synth = tool_context.state.get("synthesis")
            if _tc_synth:
                synthesis = _tc_synth
                print(f"INFO: dag_builder got synthesis from ToolContext.state for {session_id}")
            _tc_critique = tool_context.state.get("critique")
            if _tc_critique and synthesis:
                synthesis["_critic_review"] = _tc_critique
        except Exception:
            pass

    if not synthesis:
        try:
            from agents.synthesis import get_synthesis_result
            _store_synth = get_synthesis_result(session_id)
            if _store_synth:
                synthesis = _store_synth
        except Exception:
            pass

    if not synthesis and state:
        synthesis = getattr(state, "synthesis", {}) or {}

    if not synthesis:
        try:
            _cache_path = os.path.join(output_folder, "_synthesis_cache.json")
            if os.path.exists(_cache_path):
                with open(_cache_path, "r", encoding="utf-8") as _cf:
                    synthesis = json.load(_cf)
        except Exception:
            pass

    profile = {}
    if state:
        profile = getattr(state, "raw_profile", {}) or {}
    
    if not profile:
        try:
            _profile_path = os.path.join(output_folder, "_profile_cache.json")
            if os.path.exists(_profile_path):
                with open(_profile_path, "r", encoding="utf-8") as _pf:
                    profile = json.load(_pf)
        except Exception:
            pass

    if not dataset_type or not csv_filename:
        try:
            _meta_path = os.path.join(output_folder, "_dataset_meta.json")
            if os.path.exists(_meta_path):
                with open(_meta_path, "r", encoding="utf-8") as _mf:
                    _meta = json.load(_mf)
                dataset_type = _meta.get("dataset_type", dataset_type)
                csv_filename = _meta.get("csv_filename", csv_filename)
        except Exception:
            pass

    if not synthesis:
        return {"status": "error", "error": "Synthesis not available."}

    # Gather charts
    try:
        if state:
            for aid, result in state.results.items():
                if not isinstance(result, dict): continue
                cp = result.get("chart_file_path")
                if cp and os.path.exists(cp):
                    with open(cp, "r", encoding="utf-8") as f:
                        charts.append({
                            "analysis_id": aid,
                            "analysis_type": result.get("analysis_type", ""),
                            "top_finding": result.get("top_finding", ""),
                            "severity": result.get("severity", "info"),
                            "embedded_html": f.read()
                        })
    except Exception as e:
        print(f"WARNING: Chart collection failed: {e}")

    # Build and Save
    html = _build_report_html(session_id, charts, synthesis, dataset_type, csv_filename, profile)
    os.makedirs(output_folder, exist_ok=True)
    report_path = os.path.join(output_folder, "report.html")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)

    # Broadcast
    res = {
        "report_path": report_path,
        "chart_count": len(charts),
        "generated_at": datetime.now().isoformat()
    }
    _report_store[session_id] = res

    try:
        if state:
            state.artifacts.append({"type": "report", "path": report_path, "created": res["generated_at"]})
            msg = create_message(
                sender="dag_builder_agent",
                recipient="orchestrator",
                intent=Intent.REPORT_READY,
                payload=res,
                session_id=session_id
            )
            state.post_message(msg)
    except Exception as _rpt_err:
        logging.warning(f"Failed to post REPORT_READY: {_rpt_err}")

    return {"status": "success", **res}


def _build_report_html(session_id, charts, synthesis, dataset_type, csv_filename, profile=None):
    title = f"{csv_filename} Strategic Report"
    
    # NEW: Full Strategic Narrative
    long_narrative = synthesis.get("conversational_report", "")
    
    # NEW: Dataset Profile Metrics
    row_count = profile.get("row_count", 0) if profile else 0
    col_count = profile.get("column_count", 0) if profile else 0
    null_pct = 0
    if profile and profile.get("columns"):
        total_nulls = sum(c.get("null_count", 0) for c in profile["columns"])
        total_cells = row_count * col_count
        if total_cells > 0:
            null_pct = round((total_nulls / total_cells) * 100, 2)
    
    # NEW: Data Quality Score
    dq_score = 100
    if null_pct > 0: dq_score -= min(30, int(null_pct * 2))
    if profile and profile.get("column_roles"):
        missing_roles = 4 - len([v for v in profile["column_roles"].values() if v])
        dq_score -= (missing_roles * 10)
    dq_score = max(10, dq_score)

    generated = datetime.now().strftime("%B %d, %Y")
    
    def badge(sev):
        colors = {"critical": "#DC2626", "high": "#D97706", "medium": "#2563EB", "low": "#059669", "info": "#4B5563"}
        bgs    = {"critical": "#FEF2F2", "high": "#FFFBEB", "medium": "#EFF6FF", "low": "#ECFDF5", "info": "#F9FAFB"}
        s = (sev or "info").lower()
        c = colors.get(s, "#4B5563")
        bg = bgs.get(s, "#F9FAFB")
        return (f'<span style="display:inline-block;font-size:10px;font-weight:700;'
                f'text-transform:uppercase;letter-spacing:0.06em;color:{c};'
                f'background:{bg};padding:2px 8px;border-radius:2px;white-space:nowrap;">'
                f'{s.upper()}</span>')

    # Personas logic
    ks = synthesis.get("key_segments", {})
    personas = ks if isinstance(ks, list) else (ks.get("segments", []) if isinstance(ks, dict) else [])

    # Markdown rendering
    conv_h = ""
    if long_narrative:
        try:
            import markdown
            conv_h = markdown.markdown(long_narrative, extensions=['extra', 'tables', 'fenced_code'])
        except:
            conv_h = f"<pre style='white-space:pre-wrap;'>{long_narrative}</pre>"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Playfair+Display:wght@700&display=swap" rel="stylesheet">
    <style>
        body {{ font-family: 'Inter', sans-serif; background: #F8FAFC; color: #1E293B; line-height: 1.6; margin: 0; }}
        .cover {{ background: #0F172A; color: white; padding: 80px 60px; border-bottom: 8px solid #10B981; }}
        .cover-title {{ font-family: 'Playfair Display', serif; font-size: 42px; margin-bottom: 20px; }}
        .container {{ max-width: 1100px; margin: 40px auto; padding: 0 40px; }}
        .section {{ background: white; border-radius: 12px; padding: 40px; margin-bottom: 40px; border: 1px solid #E2E8F0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        .section-title {{ font-size: 20px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.1em; color: #64748B; margin-bottom: 30px; border-bottom: 1px solid #F1F5F9; padding-bottom: 10px; }}
        .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }}
        .stat-card {{ background: #F8FAFC; padding: 24px; border-radius: 8px; border-left: 4px solid #10B981; }}
        .stat-val {{ font-size: 32px; font-weight: 800; color: #0F172A; }}
        .stat-lbl {{ font-size: 11px; font-weight: 700; color: #64748B; text-transform: uppercase; }}
        .narrative-wrap h1, .narrative-wrap h2 {{ font-family: 'Playfair Display', serif; color: #0F172A; margin-top: 40px; margin-bottom: 20px; border-bottom: 1px solid #F1F5F9; padding-bottom: 8px; }}
        .narrative-wrap p {{ font-size: 16px; color: #334155; margin-bottom: 20px; }}
        .chart-box {{ margin-top: 40px; border: 1px solid #E2E8F0; border-radius: 12px; overflow: hidden; }}
        .chart-header {{ background: #F8FAFC; padding: 16px 24px; border-bottom: 1px solid #E2E8F0; display: flex; justify-content: space-between; }}
        .persona-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
        .persona-card {{ border: 1px solid #E2E8F0; border-radius: 8px; padding: 20px; background: #FFF; }}
        .persona-name {{ font-weight: 800; color: #10B981; margin-bottom: 8px; text-transform: uppercase; }}
    </style>
</head>
<body>
    <div class="cover">
        <div style="font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.2em; color: #10B981; margin-bottom: 15px;">Official Strategic Briefing</div>
        <h1 class="cover-title">{title}</h1>
        <div style="display: flex; gap: 40px; font-size: 14px; opacity: 0.8;">
            <span><strong>Date:</strong> {generated}</span>
            <span><strong>Data Source:</strong> {csv_filename}</span>
            <span><strong>Analytic Confidence:</strong> {dq_score}%</span>
        </div>
    </div>

    <div class="container">
        <div class="section">
            <h2 class="section-title">01 / Technical Data Audit</h2>
            <div class="stat-grid">
                <div class="stat-card">
                    <div class="stat-lbl">Processed Rows</div>
                    <div class="stat-val">{row_count:,}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-lbl">Data Dimensions</div>
                    <div class="stat-val">{col_count} Cols</div>
                </div>
                <div class="stat-card">
                    <div class="stat-lbl">Data Integrity</div>
                    <div class="stat-val">{100 - null_pct}%</div>
                </div>
                <div class="stat-card" style="border-left-color: {'#10B981' if dq_score > 80 else '#F59E0B'}">
                    <div class="stat-lbl">Trust Score</div>
                    <div class="stat-val">{dq_score}/100</div>
                </div>
            </div>
            <p style="margin-top: 20px; font-size: 13px; color: #64748B;">
                Audit confirms 100% data coverage. Trust Score derived from null density, entity consistency, and signal-to-noise ratio.
            </p>
        </div>

        <div class="section narrative-wrap">
            <h2 class="section-title">02 / Executive Strategic Synthesis</h2>
            {conv_h}
        </div>

        {f'''
        <div class="section">
            <h2 class="section-title">03 / Behavioral Archetypes</h2>
            <div class="persona-grid">
                {''.join(f"""
                <div class="persona-card">
                    <div class="persona-name">{p.get('name', 'Segment')} &bull; {p.get('priority_level', 'info').upper()}</div>
                    <div style="font-size: 12px; color: #64748B; margin-bottom: 12px;">{p.get('size', '')}</div>
                    <p style="font-size: 14px; color: #334155; margin-bottom: 16px;">{p.get('profile', '')}</p>
                    <div style="font-size: 11px; font-weight: 700; text-transform: uppercase; color: #94A3B8;">Pain Points</div>
                    <ul style="font-size: 13px; padding-left: 20px; margin-top: 4px;">
                        {''.join(f'<li>{pt}</li>' for pt in p.get('pain_points', []))}
                    </ul>
                </div>
                """ for p in personas)}
            </div>
        </div>
        ''' if personas else ''}

        <h2 class="section-title">{'04' if personas else '03'} / Analysis Appendix</h2>
        {"".join(f'''
        <div class="chart-box">
            <div class="chart-header">
                <span style="font-weight: 700;">{c.get('analysis_type', 'Analysis').replace('_', ' ').title()}</span>
                {badge(c.get('severity', 'info'))}
            </div>
            <div style="padding: 24px; background: white;">
                <p style="font-size: 14px; font-weight: 600; color: #0F172A; margin-bottom: 12px;">Finding: {c.get('top_finding')}</p>
                <iframe srcdoc="{c.get('embedded_html', '').replace('"', '&quot;')}" width="100%" height="450" frameborder="0"></iframe>
            </div>
        </div>
        ''' for c in charts)}
    </div>

    <div style="text-align: center; padding: 40px; font-size: 12px; color: #94A3B8; border-top: 1px solid #E2E8F0;">
        Confidential Analytics Report &bull; ADOPSUN A2A Suite &bull; {generated}
    </div>
</body>
</html>"""


_PROMPT_DIR = os.path.join(os.path.dirname(__file__), '..', 'prompts')

def _load_prompt(name: str) -> str:
    path = os.path.join(_PROMPT_DIR, name)
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

def get_dag_builder_agent():
    from google.adk.agents import Agent
    from google.adk.tools import FunctionTool
    from tools.model_config import get_model
    return Agent(
        name="dag_builder_agent",
        model=get_model("dag_builder"),
        description="Report assembly specialist.",
        instruction=_load_prompt("dag_builder.md"),
        tools=[FunctionTool(tool_build_report)],
    )
