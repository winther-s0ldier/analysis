import os
import sys
import json
from datetime import datetime

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..")
)

from a2a_messages import create_message, Intent


_report_store: dict = {}


def get_report_result(session_id: str) -> dict | None:
    """Read stored report path for a session."""
    return _report_store.get(session_id)



def tool_build_report(session_id: str, output_folder: str) -> dict:
    """
    Collect artifacts and assemble the final HTML report in one step.
    """
    charts = []
    synthesis = {}
    dataset_type = ""
    csv_filename = ""

    try:
        from main import sessions
        state = sessions.get(session_id)
        if state:
            dataset_type = getattr(state, "dataset_type", "")
            csv_filename = getattr(state, "csv_filename", "")
            synthesis = getattr(state, "synthesis", {}) or {}
            for aid, result in state.results.items():
                chart_path = result.get("chart_file_path")
                if chart_path and os.path.exists(chart_path):
                    charts.append({
                        "analysis_id": aid,
                        "analysis_type": result.get("analysis_type", ""),
                        "chart_path": chart_path,
                        "top_finding": result.get("top_finding", ""),
                        "severity": result.get("severity", "info"),
                        "confidence": result.get("confidence", 0.0),
                    })
    except Exception: pass

    if os.path.exists(output_folder):
        for fname in os.listdir(output_folder):
            if fname.endswith(".html") and fname != "report.html":
                fpath = os.path.join(output_folder, fname)
                if not any(c["chart_path"] == fpath for c in charts):
                    parts = fname.replace(".html", "").split("_", 1)
                    charts.append({
                        "analysis_id": parts[0],
                        "analysis_type": parts[1] if len(parts) > 1 else "",
                        "chart_path": fpath,
                        "top_finding": "",
                        "severity": "info",
                        "confidence": 0.0,
                    })
    charts.sort(key=lambda c: c["analysis_id"])

    for chart in charts:
        chart_path = chart.get("chart_path", "")
        content = ""
        if chart_path and os.path.exists(chart_path):
            try:
                with open(chart_path, "r", encoding="utf-8") as cf:
                    content = cf.read()
            except Exception: pass
        chart["_embedded_html"] = content

    html = _build_report_html(session_id, charts, synthesis, dataset_type, csv_filename)
    os.makedirs(output_folder, exist_ok=True)
    report_path = os.path.join(output_folder, "report.html")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)
        
    synthesis_path = os.path.join(output_folder, "synthesis.json")
    with open(synthesis_path, "w", encoding="utf-8") as f:
        json.dump(synthesis, f, indent=2)

    res = {
        "report_path": report_path,
        "chart_count": len(charts),
        "has_synthesis": bool(synthesis),
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
    except Exception: pass
    return {"status": "success", **res}



def _build_report_html(session_id: str, charts: list, synthesis: dict, dataset_type: str, csv_filename: str) -> str:
    generated = datetime.now().strftime("%Y-%m-%d %H:%M")
    title = f"Analysis Report — {csv_filename}" if csv_filename else "Analysis Report"

    def get_sect(name, content):
        if not content: return ""
        display_name = name.replace("_", " ").title()
        return f'<section class="section {name}"><h2>{display_name}</h2>{content}</section>'

    ex = synthesis.get("executive_summary", {})
    ex_h = ""
    if ex:
        health = ex.get("overall_health", "")
        priorities = ex.get("top_priorities", [])
        impact = ex.get("business_impact", "")
        resource = ex.get("resource_allocation", "")
        timeline = ex.get("timeline", "")
        
        ex_h += f'<div class="insight-block"><strong>Overall Health:</strong> <p>{health}</p></div>' if health else ""
        if priorities:
            bullets_html = "".join([f'<li class="bullet">{p}</li>' for p in priorities])
            ex_h += f'<div class="insight-block"><strong>Top Priorities:</strong><ul class="bullets">{bullets_html}</ul></div>'
        ex_h += f'<div class="insight-block"><strong>Business Impact:</strong> <p>{impact}</p></div>' if impact else ""
        ex_h += f'<div class="insight-block"><strong>Resource Allocation:</strong> <p>{resource}</p></div>' if resource else ""
        ex_h += f'<div class="insight-block"><strong>Timeline:</strong> <p>{timeline}</p></div>' if timeline else ""

    di = synthesis.get("detailed_insights", {})
    insights_list = di.get("insights", [])
    di_chunks = []
    for ins in insights_list:
        i_title = ins.get("title", "Insight")
        sev = ins.get("fix_priority", "Medium").lower()
        if sev not in ["critical", "high", "medium", "low"]: sev = "info"
        ai_sum = ins.get("ai_summary", "")
        rc = ins.get("root_cause_hypothesis", "")
        ux = ins.get("ux_implications", "")
        
        causes = ins.get("possible_causes", [])
        causes_html = "".join([f'<li>{c}</li>' for c in causes])
        
        sols = ins.get("how_to_fix", ins.get("recommended_solutions", []))
        sols_html = "".join([f'<li>{s}</li>' for s in sols])
        
        di_chunks.append(
            f'<div class="strategy-card {sev}">'
            f'<div class="strat-header"><span class="badge {sev}">{sev.upper()}</span> {i_title}</div>'
            f'<div class="strat-body">'
            f'<p><b>AI Summary:</b> {ai_sum}</p>'
            f'<p><b>Root Cause Hypothesis:</b> {rc}</p>'
            f'<p><b>Possible Causes:</b></p><ul style="margin-top:4px;">{causes_html}</ul>'
            f'<p><b>UX Implications:</b> {ux}</p>'
            f'<p><b>How to Fix:</b></p><ul style="margin-top:4px;">{sols_html}</ul>'
            f'</div></div>'
        )
    di_h = "".join(di_chunks)

    st = synthesis.get("intervention_strategies", {})
    strategies_list = st.get("strategies", [])
    st_chunks = []
    for s in strategies_list:
        sev = s.get("severity", "info").lower()
        if sev not in ["critical", "high", "medium", "low"]: sev = "info"
        ttl = s.get("title", "Strategy")
        rt = s.get("realtime_interventions", [])
        pro = s.get("proactive_outreach", [])
        
        rt_html = "".join([f'<li>{x}</li>' for x in rt])
        pro_html = "".join([f'<li>{x}</li>' for x in pro])
        
        st_chunks.append(
            f'<div class="strategy-card {sev}">'
            f'<div class="strat-header"><span class="badge {sev}">{sev.upper()}</span> {ttl}</div>'
            f'<div class="strat-body" style="display:flex; gap:20px;">'
            f'<div style="flex:1"><p style="margin-bottom:4px"><b>Real-Time Interventions:</b></p><ul style="margin-top:0;">{rt_html}</ul></div>'
            f'<div style="flex:1"><p style="margin-bottom:4px"><b>Proactive Outreach:</b></p><ul style="margin-top:0;">{pro_html}</ul></div>'
            f'</div></div>'
        )
    st_h = "".join(st_chunks)

    pe = synthesis.get("personas", {})
    personas_list = pe.get("personas", [])
    pe_chunks = []
    for p in personas_list:
        p_name = p.get("name", "User Segment")
        p_prof = p.get("profile", "")
        p_prior = p.get("priority_level", "Medium")
        p_prior_css = p_prior.lower() if p_prior.lower() in ["critical", "high", "medium", "low"] else "info"
        p_pain = p.get("pain_points", [])
        p_opp = p.get("opportunities", [])
        
        pain_html = "".join([f'<li>{x}</li>' for x in p_pain])
        opp_html = "".join([f'<li>{x}</li>' for x in p_opp])
        
        pe_chunks.append(
            f'<div class="persona-card">'
            f'<div class="persona-name" style="font-weight:bold; font-size:16px; margin-bottom:8px;">{p_name} <span class="badge {p_prior_css}" style="float:right">{p_prior.upper()}</span></div>'
            f'<div class="persona-size" style="font-style:italic; margin-bottom:12px; color:#555;">{p_prof}</div>'
            f'<div class="persona-desc">'
            f'<p style="margin-bottom:4px; font-weight:600;">Pain Points:</p><ul style="margin-top:0; padding-left:20px;">{pain_html}</ul>'
            f'<p style="margin-bottom:4px; font-weight:600;">Opportunities:</p><ul style="margin-top:0; padding-left:20px;">{opp_html}</ul>'
            f'</div></div>'
        )
    pe_h = f'<div class="persona-grid">{"".join(pe_chunks)}</div>' if pe_chunks else ""

    ch_chunks = []
    for c in charts:
        a_type = c.get("analysis_type", "").replace("_", " ").title()
        sev = c.get("severity", "info")
        html_content = c.get("_embedded_html", "")
        safe_html = (
            html_content
            .replace("&", "&amp;")
            .replace('"', "&quot;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        ch_chunks.append(
            f'<div class="chart-container">'
            f'<div class="chart-header"><span>{a_type}</span><span class="badge {sev}">{sev}</span></div>'
            f'<iframe srcdoc="{safe_html}" width="100%" height="500" frameborder="0"></iframe>'
            f'</div>'
        )
    ch_h = "".join(ch_chunks)

    conv_rep = synthesis.get("conversational_report", "")
    
    if conv_rep:
        try:
            import markdown
            conv_h = markdown.markdown(conv_rep, extensions=['tables'])
        except ImportError:
            conv_h = f'<div style="white-space: pre-wrap; font-family: monospace; background: #f5f5f5; padding: 20px;">{conv_rep}</div>'
    else:
        conv_h = ""

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>{title}</title>
    <style>
        body {{ font-family: system-ui, -apple-system, sans-serif; background:
        .header {{ background:
        .section {{ background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
        h2 {{ border-bottom: 2px solid
        .insight-block {{ background:
        .insight-block p {{ margin: 5px 0 0 0; }}
        .badge {{ padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; text-transform: uppercase; }}
        .critical {{ color:
        .high {{ color:
        .medium {{ color:
        .low {{ color:
        .info {{ color:
        .strategy-card {{ border: 1px solid
        .strat-header {{ padding: 12px; font-weight: bold; background:
        .strat-body {{ padding: 15px; }}
        .persona-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; }}
        .persona-card {{ border: 1px solid
        .chart-container {{ border: 1px solid
        .chart-header {{ background:
        /* Markdown generated table styles */
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 1rem; color:
        th, td {{ padding: 0.75rem; vertical-align: top; border-top: 1px solid
        thead th {{ vertical-align: bottom; border-bottom: 2px solid
        tbody tr:nth-of-type(odd) {{ background-color: rgba(0,0,0,.05); }}
    </style></head><body>
    <div class="header"><h1 style="margin:0 0 10px 0;">{title}</h1><p style="margin:0;opacity:0.8;">Dataset: {dataset_type} | Generated: {generated}</p></div>
    {get_sect("conversational_report", conv_h)}
    {get_sect("executive_summary", ex_h)}
    {get_sect("detailed_insights", di_h)}
    {get_sect("intervention_strategies", st_h)}
    {get_sect("personas", pe_h)}
    {get_sect("analysis_charts", ch_h)}
    <div style="text-align:center; color:#999; font-size:12px; margin-top:20px;">Generated by Analytics Session {session_id}</div>
    </body></html>"""


_dag_builder_instance = None


def get_dag_builder_agent():
    global _dag_builder_instance
    if _dag_builder_instance is None:
        from google.adk.agents import Agent
        from tools.model_config import get_model
        _dag_builder_instance = Agent(
            name="dag_builder_agent",
            model=get_model("dag_builder"),
            description="Report assembly specialist. Pure formatting.",
            instruction="You are a report assembly specialist. Your ONLY job is to call tool_build_report(session_id, output_folder) and return the result.",
            tools=[tool_build_report],
        )
    return _dag_builder_instance
