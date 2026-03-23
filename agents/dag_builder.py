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

from a2a_messages import create_message, Intent


_report_store: dict = {}


def get_report_result(session_id: str) -> dict | None:
    """Read stored report path for a session."""
    return _report_store.get(session_id)



def tool_build_report(
    session_id: Annotated[str, Field(description="Active pipeline session ID")],
    output_folder: Annotated[str, Field(description="Absolute path to the session output directory. Copy exactly from the prompt.")],
    tool_context=None,
) -> dict:
    """
    Collect artifacts and assemble the final HTML report in one step.
    """
    charts = []
    synthesis = {}
    dataset_type = ""
    csv_filename = ""

    # ── Source 0: in-process session state (only works when NOT in A2A subprocess) ──
    state = None
    try:
        from main import sessions
        state = sessions.get(session_id)
        if state:
            dataset_type = getattr(state, "dataset_type", "")
            csv_filename = getattr(state, "csv_filename", "")
            print(f"INFO: dag_builder got session state from main.sessions for {session_id}")
    except Exception as _imp_err:
        # Expected in A2A subprocess mode — main.py can't be imported cleanly.
        # File-based sources below will provide the data instead.
        print(f"INFO: dag_builder running without main.sessions (A2A mode): {_imp_err}")

    # ── Source 1: ToolContext state (SequentialAgent in-process mode) ──
    if tool_context is not None:
        try:
            _tc_synth = tool_context.state.get("synthesis")
            if _tc_synth:
                synthesis = _tc_synth
                print(f"INFO: dag_builder got synthesis from ToolContext.state for {session_id}")
            _tc_critique = tool_context.state.get("critique")
            if _tc_critique and synthesis:
                synthesis["_critic_review"] = _tc_critique
                print(f"INFO: dag_builder injected critique from ToolContext.state for {session_id}")
        except Exception:
            pass

    # ── Source 2: _synthesis_store (in-process module-level dict) ──
    if not synthesis:
        try:
            from agents.synthesis import get_synthesis_result
            _store_synth = get_synthesis_result(session_id)
            if _store_synth:
                synthesis = _store_synth
                print(f"INFO: dag_builder got synthesis from _synthesis_store for {session_id}")
        except Exception:
            pass

    # ── Source 3: state.synthesis (in-process session object) ──
    if not synthesis and state:
        synthesis = getattr(state, "synthesis", {}) or {}
        if synthesis:
            print(f"INFO: dag_builder got synthesis from state.synthesis for {session_id}")

    # ── Source 4: file-based cache (CRITICAL for A2A subprocess mode) ──
    if not synthesis:
        try:
            _cache_path = os.path.join(output_folder, "_synthesis_cache.json")
            if os.path.exists(_cache_path):
                with open(_cache_path, "r", encoding="utf-8") as _cf:
                    _file_synth = json.load(_cf)
                if _file_synth:
                    synthesis = _file_synth
                    print(f"INFO: dag_builder recovered synthesis from file cache for {session_id}")
        except Exception as _cache_err:
            print(f"WARNING: dag_builder failed to read _synthesis_cache.json: {_cache_err}")

    # ── Source 5: inject _critic_cache.json into synthesis if not already present ──
    if synthesis and not synthesis.get("_critic_review"):
        try:
            _critic_cache_path = os.path.join(output_folder, "_critic_cache.json")
            if os.path.exists(_critic_cache_path):
                with open(_critic_cache_path, "r", encoding="utf-8") as _ccf:
                    _file_crit = json.load(_ccf)
                if _file_crit:
                    synthesis["_critic_review"] = _file_crit
                    print(f"INFO: [A2A] dag_builder injected critic review from file cache for {session_id}")
        except Exception:
            pass

    # ── Read dataset metadata from file cache when state is unavailable (A2A mode) ──
    if not dataset_type or not csv_filename:
        try:
            _meta_path = os.path.join(output_folder, "_dataset_meta.json")
            if os.path.exists(_meta_path):
                with open(_meta_path, "r", encoding="utf-8") as _mf:
                    _meta = json.load(_mf)
                dataset_type = _meta.get("dataset_type", dataset_type)
                csv_filename = _meta.get("csv_filename", csv_filename)
                print(f"INFO: dag_builder recovered dataset metadata from file cache for {session_id}")
        except Exception:
            pass

    if not synthesis:
        print(f"WARNING: dag_builder found no synthesis for {session_id} — report will have no insights.")
        return {
            "status": "error",
            "error": "Synthesis not available. Cannot build report without insights. Re-run synthesis first.",
            "session_id": session_id,
        }

    # ── Collect chart artifacts from session state ──
    try:
        if state:
            for aid, result in state.results.items():
                if not isinstance(result, dict):
                    continue
                chart_path = result.get("chart_file_path")
                if chart_path and os.path.exists(chart_path):
                    _ins_sum = result.get("insight_summary") or {}
                    charts.append({
                        "analysis_id": aid,
                        "analysis_type": result.get("analysis_type", ""),
                        "chart_path": chart_path,
                        "top_finding": result.get("top_finding", ""),
                        "severity": result.get("severity", "info"),
                        "confidence": result.get("confidence", 0.0),
                        "narrative": result.get("data", {}).get("narrative", {}),
                        # Per-node decision-maker insight from #10
                        "decision_maker_takeaway": _ins_sum.get("decision_maker_takeaway", "") if isinstance(_ins_sum, dict) else "",
                    })
    except Exception as e:
        print(f"WARNING: dag_builder state chart collection failed: {e}")

    if os.path.exists(output_folder):
        for fname in os.listdir(output_folder):
            if fname.endswith(".html") and fname != "report.html":
                fpath = os.path.join(output_folder, fname)
                # Use abspath on both sides so absolute vs relative path
                # differences do not cause the same file to be added twice.
                if not any(os.path.abspath(c["chart_path"]) == os.path.abspath(fpath) for c in charts):
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
            except Exception as _cf_err:
                logging.warning(f"Failed to read chart file {chart_path}: {_cf_err}")
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
    except Exception as _rpt_err:
        logging.warning(f"Failed to post REPORT_READY message: {_rpt_err}")
    return {"status": "success", **res}



def _build_report_html(session_id: str, charts: list, synthesis: dict, dataset_type: str, csv_filename: str) -> str:
    generated = datetime.now().strftime("%Y-%m-%d %H:%M")
    title = f"Analysis Report — {csv_filename}" if csv_filename else "Analysis Report"

    def get_sect(name, content):
        if not content: return ""
        display_name = name.replace("_", " ").title()
        return f'<section class="section {name}"><h2>{display_name}</h2>{content}</section>'

    ex = synthesis.get("executive_summary", {})
    if not isinstance(ex, dict):
        ex = {}
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
    # LLM sometimes returns detailed_insights as a flat list instead of {"insights": [...]}
    if isinstance(di, list):
        insights_list = di
    elif isinstance(di, dict):
        insights_list = di.get("insights", [])
    else:
        insights_list = []
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
        
        downstream = ins.get("downstream_implications", "")
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
            + (f'<p><b>Downstream Impact:</b> {downstream}</p>' if downstream else '') +
            f'<p><b>How to Fix:</b></p><ul style="margin-top:4px;">{sols_html}</ul>'
            f'</div></div>'
        )
    di_h = "".join(di_chunks)

    # --- #8: Adversarial Critic Review section ---
    _crit_rev = synthesis.get("_critic_review", {})
    _critic_h = ""
    if isinstance(_crit_rev, dict) and _crit_rev:
        _cr_approved = _crit_rev.get("approved", True)
        _cr_challenges = _crit_rev.get("challenges", [])
        _cr_conf = _crit_rev.get("confidence_adjustment", 1.0)
        _cr_verdict = _crit_rev.get("overall_verdict", "")
        _verdict_label = "&#10003; Approved" if _cr_approved else "&#9888; Issues Found"
        _verdict_class = "low" if _cr_approved else "high"
        _chal_html = ""
        for _ch in _cr_challenges:
            if not isinstance(_ch, dict):
                continue
            _ch_sev = _ch.get("severity", "medium").lower()
            if _ch_sev not in ("critical", "high", "medium", "low"):
                _ch_sev = "medium"
            _chal_html += (
                f'<div class="strategy-card {_ch_sev}" style="margin-bottom:10px;">'
                f'<div class="strat-header">'
                f'<span class="badge {_ch_sev}">{_ch_sev.upper()}</span>'
                f'&nbsp;{_ch.get("claim", "")}'
                f'</div>'
                f'<div class="strat-body">{_ch.get("issue", "")}</div>'
                f'</div>'
            )
        if not _cr_challenges:
            _chal_html = '<p style="color:#27ae60;margin:0;">No significant issues found — synthesis is well-grounded.</p>'
        _critic_h = (
            f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">'
            f'<span class="badge {_verdict_class}" style="font-size:13px;padding:5px 12px;">{_verdict_label}</span>'
            f'<span style="font-size:12px;color:#666;">Confidence: {int(_cr_conf * 100)}%</span>'
            f'</div>'
            + (f'<p style="font-size:13px;color:#555;margin-bottom:14px;">{_cr_verdict}</p>' if _cr_verdict else "")
            + _chal_html
        )
        # Reliability badge injected into the report header
        _conf_pct = int(_cr_conf * 100)
        if _conf_pct >= 90:
            _badge_color = "#27ae60"
        elif _conf_pct >= 70:
            _badge_color = "#e67e22"
        else:
            _badge_color = "#c0392b"
        _reliability_badge = (
            f'<span style="display:inline-block;margin-top:8px;padding:3px 10px;'
            f'border-radius:12px;background:{_badge_color};color:#fff;'
            f'font-size:11px;font-family:system-ui,sans-serif;font-weight:600;letter-spacing:0.3px;">'
            f'Peer Review: {_conf_pct}% reliable</span>'
        )
    else:
        _reliability_badge = ""

    st = synthesis.get("recommendations", synthesis.get("intervention_strategies", {}))
    if isinstance(st, list):
        strategies_list = st
    elif isinstance(st, dict):
        strategies_list = st.get("strategies", [])
    else:
        strategies_list = []
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

    pe = synthesis.get("key_segments", synthesis.get("personas", {}))
    if isinstance(pe, list):
        personas_list = pe
    elif isinstance(pe, dict):
        personas_list = pe.get("segments", pe.get("personas", []))
    else:
        personas_list = []
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

    # Pre-index synthesis insight cards by analysis_id so each chart can
    # pull its matching insight for the detailed description below the chart.
    _insight_index = {}
    _di_raw = synthesis.get("detailed_insights", {})
    _all_insights = _di_raw if isinstance(_di_raw, list) else (_di_raw.get("insights", []) if isinstance(_di_raw, dict) else [])
    for _ins in _all_insights:
        if not isinstance(_ins, dict):
            continue
        # Scan ai_summary and root_cause_hypothesis for [AX] / [CX] node citations
        _text = str(_ins.get("ai_summary", "")) + str(_ins.get("root_cause_hypothesis", "")) + str(_ins.get("title", ""))
        for _m in re.findall(r'\[([AC]\d+)\]', _text):
            if _m not in _insight_index:
                _insight_index[_m] = _ins

    ch_chunks = []
    for c in charts:
        a_id   = c.get("analysis_id", "")
        a_type = c.get("analysis_type", "").replace("_", " ").title()
        sev    = c.get("severity", "info")
        html_content = c.get("_embedded_html", "")
        narrative = c.get("narrative", {}) if isinstance(c.get("narrative"), dict) else {}
        top_finding = c.get("top_finding", "")
        dm_takeaway = c.get("decision_maker_takeaway", "")
        confidence  = c.get("confidence", 0.0)
        conf_pct    = f"{round(confidence * 100)}%" if confidence else ""

        safe_html = (
            html_content
            .replace("&", "&amp;")
            .replace('"', "&quot;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

        # --- Pre-chart: decision-maker takeaway (from #10) ---
        pre_chart = ""
        if dm_takeaway:
            pre_chart = (
                f'<div class="dm-takeaway">'
                f'<span style="font-size:18px;">&#128161;</span>'
                f'<strong> Decision-Maker Insight</strong>'
                f'<p style="margin:6px 0 0 0;">{dm_takeaway}</p>'
                f'</div>'
            )
        elif top_finding:
            pre_chart = (
                f'<div class="dm-takeaway">'
                f'<span style="font-size:18px;">&#128269;</span>'
                f'<strong> Key Finding</strong>'
                f'<p style="margin:6px 0 0 0;">{top_finding}</p>'
                f'</div>'
            )

        # --- Post-chart: narrative (what it means + proposed fix) ---
        what_it_means = narrative.get("what_it_means", "")
        proposed_fix  = narrative.get("proposed_fix", "")
        narr_severity = narrative.get("severity", sev)
        if narr_severity not in ["critical", "high", "medium", "low", "info"]:
            narr_severity = "info"

        narrative_html = ""
        if what_it_means:
            narrative_html += (
                f'<div class="narrative-what">'
                f'<span class="narrative-icon">&#128269;</span>'
                f'<strong>WHAT IT MEANS</strong>'
                f'<p>{what_it_means}</p>'
                f'</div>'
            )
        if proposed_fix:
            narrative_html += (
                f'<div class="narrative-fix">'
                f'<span class="narrative-icon">&#9889;</span>'
                f'<strong>PROPOSED FIX</strong>'
                f'<span class="badge {narr_severity}" style="float:right;margin-top:-2px">{narr_severity.upper()}</span>'
                f'<p>{proposed_fix}</p>'
                f'</div>'
            )

        # --- Matched synthesis insight card for this node ---
        matched_ins = _insight_index.get(a_id)
        insight_detail_html = ""
        if matched_ins:
            _ai_sum  = matched_ins.get("ai_summary", "")
            _rc      = matched_ins.get("root_cause_hypothesis", "")
            _causes  = matched_ins.get("possible_causes", [])
            _ds_impl = matched_ins.get("downstream_implications", "")
            _fixes   = matched_ins.get("how_to_fix", matched_ins.get("recommended_solutions", []))
            _pri     = matched_ins.get("fix_priority", "medium").lower()
            if _pri not in ["critical","high","medium","low"]: _pri = "medium"
            _causes_html = "".join(f"<li>{x}</li>" for x in _causes) if _causes else ""
            _fixes_html  = "".join(f"<li>{x}</li>" for x in _fixes)  if _fixes  else ""
            insight_detail_html = (
                f'<div class="insight-deep-dive">'
                f'<div class="idive-header"><span class="badge {_pri}">{_pri.upper()}</span>&nbsp;Deep Dive Analysis [{a_id}]</div>'
                + (f'<div class="idive-row"><strong>&#129302; AI Analysis</strong><p>{_ai_sum}</p></div>' if _ai_sum else "")
                + (f'<div class="idive-row"><strong>&#128279; Root Cause Chain</strong><p>{_rc}</p></div>' if _rc else "")
                + (f'<div class="idive-row"><strong>&#128270; Possible Causes</strong><ul>{_causes_html}</ul></div>' if _causes_html else "")
                + (f'<div class="idive-row"><strong>&#128200; Downstream Impact</strong><p>{_ds_impl}</p></div>' if _ds_impl else "")
                + (f'<div class="idive-row"><strong>&#9989; How to Fix</strong><ul>{_fixes_html}</ul></div>' if _fixes_html else "")
                + f'</div>'
            )

        post_chart = ""
        if narrative_html or insight_detail_html:
            post_chart = f'<div class="narrative-block">{narrative_html}{insight_detail_html}</div>'

        conf_badge = f'<span style="font-size:11px;color:#888;margin-left:8px;">confidence {conf_pct}</span>' if conf_pct else ""

        ch_chunks.append(
            f'<div class="chart-container">'
            f'<div class="chart-header"><span>{a_type} [{a_id}]</span><span class="badge {sev}">{sev}</span>{conf_badge}</div>'
            + pre_chart +
            f'<iframe srcdoc="{safe_html}" width="100%" height="500" frameborder="0"></iframe>'
            + post_chart +
            f'</div>'
        )
    ch_h = "".join(ch_chunks)

    conv_rep = synthesis.get("conversational_report", "")

    if conv_rep:
        try:
            import markdown
            conv_h = markdown.markdown(conv_rep, extensions=['tables'])
        except ImportError:
            conv_h = f'<div style="white-space: pre-wrap; font-family: monospace; background: #f5f5f5; padding: 20px; border-radius: 8px;">{conv_rep}</div>'
    else:
        conv_h = ""

    # Cross-metric connections (present in UI, previously missing from report)
    cx = synthesis.get("cross_metric_connections", {})
    if isinstance(cx, list):
        connections_list = cx
    elif isinstance(cx, dict):
        connections_list = cx.get("connections", [])
    else:
        connections_list = []
    cx_chunks = []
    for conn in connections_list:
        fa = conn.get("finding_a", "")
        fb = conn.get("finding_b", "")
        meaning = conn.get("synthesized_meaning", "")
        cx_chunks.append(
            f'<div class="strategy-card info">'
            f'<div class="strat-header">&#128279; Cross-Metric Connection</div>'
            f'<div class="strat-body">'
            f'<p style="margin-bottom:6px;"><b>Finding A:</b> {fa}</p>'
            f'<p style="margin-bottom:6px;"><b>Finding B:</b> {fb}</p>'
            f'<p style="margin-bottom:0;"><b>Synthesised Meaning:</b> {meaning}</p>'
            f'</div></div>'
        )
    cx_h = "".join(cx_chunks)

    # Section display names (IEEE-style labels, no "Part N" prefixes)
    _sect_labels = {
        "executive_summary":       "Executive Summary",
        "key_findings":            "Key Findings",
        "detailed_insights":       "Detailed Insights",
        "adversarial_review":      "&#128269; Adversarial Review",
        "key_segments":            "Key Segments",
        "cross_metric_connections":"Cross-Metric Connections",
        "recommendations":         "Recommendations",
        "analysis_charts":         "Analysis Charts",
    }

    def get_sect_labeled(key, content):
        if not content:
            return ""
        label = _sect_labels.get(key, key.replace("_", " ").title())
        return f'<section class="section {key}"><h2>{label}</h2>{content}</section>'

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>{title}</title>
    <style>
        body {{ font-family: "Georgia", "Times New Roman", serif; background: #f5f6fa; color: #222; line-height: 1.65; }}
        .header {{ background: #1a2535; color: white; padding: 28px 32px; border-radius: 8px; margin-bottom: 28px; }}
        .header h1 {{ margin: 0 0 8px 0; font-size: 22px; font-weight: 700; letter-spacing: -0.3px; }}
        .header p {{ margin: 0; opacity: 0.75; font-size: 13px; font-family: system-ui, sans-serif; }}
        .section {{ background: white; padding: 28px 32px; border-radius: 8px; margin-bottom: 24px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
        h2 {{ font-size: 17px; font-weight: 700; border-bottom: 2px solid #e8e8e8; padding-bottom: 10px; margin-top: 0; margin-bottom: 18px; color: #1a2535; letter-spacing: -0.2px; }}
        .insight-block {{ background: #f8f9fa; border-left: 4px solid #3498db; padding: 12px 16px; margin-bottom: 12px; border-radius: 4px; }}
        .insight-block p {{ margin: 5px 0 0 0; font-size: 14px; }}
        .badge {{ padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; text-transform: uppercase; font-family: system-ui, sans-serif; }}
        .critical {{ color: #721c24; background-color: #f8d7da; border: 1px solid #f5c6cb; }}
        .high {{ color: #856404; background-color: #fff3cd; border: 1px solid #ffeeba; }}
        .medium {{ color: #004085; background-color: #cce5ff; border: 1px solid #b8daff; }}
        .low {{ color: #155724; background-color: #d4edda; border: 1px solid #c3e6cb; }}
        .info {{ color: #383d41; background-color: #e2e3e5; border: 1px solid #d6d8db; }}
        .strategy-card {{ border: 1px solid #e4e4e4; border-radius: 6px; margin-bottom: 16px; overflow: hidden; }}
        .strat-header {{ padding: 12px 16px; font-weight: bold; background: #fafafa; border-bottom: 1px solid #e4e4e4; font-size: 14px; }}
        .strat-body {{ padding: 16px; font-size: 14px; }}
        .strat-body p {{ margin-bottom: 8px; }}
        .strat-body ul {{ margin-top: 4px; padding-left: 20px; }}
        .strat-body li {{ margin-bottom: 4px; }}
        .persona-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; }}
        .persona-card {{ border: 1px solid #e4e4e4; border-radius: 6px; padding: 16px; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.05); font-size: 14px; }}
        /* Chart figures — numbered captions */
        .chart-container {{ border: 1px solid #ddd; border-radius: 8px; margin-bottom: 28px; overflow: hidden; background: white; }}
        .chart-header {{ background: #f8f9fa; padding: 10px 16px; font-weight: 600; border-bottom: 1px solid #ddd; display: flex; justify-content: space-between; align-items: center; font-size: 14px; font-family: system-ui, sans-serif; }}
        .chart-fig-num {{ font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: .05em; font-weight: 400; margin-right: 8px; }}
        /* Markdown table styles (IEEE-compatible) */
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 1.2rem; font-size: 13px; font-family: system-ui, sans-serif; }}
        th {{ background: #f0f0f0; font-weight: 600; }}
        th, td {{ padding: 8px 12px; vertical-align: top; border: 1px solid #ddd; text-align: left; }}
        thead th {{ border-bottom: 2px solid #bbb; }}
        tbody tr:nth-of-type(odd) {{ background-color: #fafafa; }}
        /* Narrative blocks */
        .narrative-block {{ margin-top: 0; border-top: 1px solid #eee; display: flex; gap: 0; }}
        .narrative-what {{
            flex: 1; padding: 14px 18px; background: #f0f4ff;
            border-left: 4px solid #3498db;
        }}
        .narrative-fix {{
            flex: 1; padding: 14px 18px; background: #fff9f0;
            border-left: 4px solid #e67e22;
        }}
        .narrative-what strong, .narrative-fix strong {{
            display: block; font-size: 11px; letter-spacing: .05em;
            text-transform: uppercase; color: #666; margin-bottom: 6px; font-family: system-ui, sans-serif;
        }}
        .narrative-what p, .narrative-fix p {{ margin: 0; font-size: 13px; line-height: 1.55; }}
        .narrative-icon {{ margin-right: 5px; }}
        /* Key findings / conversational report prose */
        .key_findings h1, .key_findings h2, .key_findings h3 {{ font-family: "Georgia", serif; }}
        .key_findings p {{ font-size: 14px; margin-bottom: 12px; }}
        .key_findings ul, .key_findings ol {{ font-size: 14px; padding-left: 22px; }}
        .key_findings li {{ margin-bottom: 6px; }}
        /* Decision-maker takeaway block (pre-chart highlight) */
        .dm-takeaway {{ padding: 12px 18px; background: #fffde7; border-left: 4px solid #f9a825; margin: 0; font-family: system-ui, sans-serif; }}
        .dm-takeaway p {{ margin: 6px 0 0 0; font-size: 14px; line-height: 1.5; }}
        /* Deep-dive synthesis insight section (post-chart) */
        .insight-deep-dive {{ border-top: 1px solid #e0e0e0; padding: 16px 18px; background: #fafafa; font-family: system-ui, sans-serif; }}
        .idive-header {{ font-weight: 700; font-size: 13px; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }}
        .idive-row {{ margin-bottom: 10px; font-size: 13px; line-height: 1.55; }}
        .idive-row strong {{ display: block; font-size: 11px; text-transform: uppercase; color: #666; margin-bottom: 4px; letter-spacing: 0.4px; }}
        .idive-row p {{ margin: 0; }}
        .idive-row ul {{ margin: 4px 0; padding-left: 20px; }}
        .idive-row li {{ margin-bottom: 4px; }}
    </style></head><body>
    <div class="header">
        <h1>{title}</h1>
        <p>Dataset: {dataset_type}&nbsp;&nbsp;|&nbsp;&nbsp;Generated: {generated}&nbsp;&nbsp;|&nbsp;&nbsp;Session: {session_id}</p>
        {_reliability_badge}
    </div>
    {get_sect_labeled("executive_summary", ex_h)}
    {get_sect_labeled("key_findings", conv_h)}
    {get_sect_labeled("detailed_insights", di_h)}
    {get_sect_labeled("adversarial_review", _critic_h)}
    {get_sect_labeled("key_segments", pe_h)}
    {get_sect_labeled("cross_metric_connections", cx_h)}
    {get_sect_labeled("recommendations", st_h)}
    {get_sect_labeled("analysis_charts", ch_h)}
    <div style="text-align:center; color:#aaa; font-size:12px; margin-top:28px; font-family:system-ui,sans-serif;">
        Analytics Report &mdash; {generated}
    </div>
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
            description="Report assembly specialist. Collects charts and synthesis narrative, assembles the final standalone HTML report.",
            instruction=(
                "You are the Report Assembly Specialist — the final agent in the analytics pipeline. "
                "Your SOLE responsibility is to call one tool correctly and return the result. "
                "You do NOT interpret data. You do NOT write insights. You do NOT modify any analysis results. "
                "You are a PURE FORMATTER.\n\n"

                "## WORKFLOW\n"
                "1. Receive `session_id` and `output_folder` from the Orchestrator.\n"
                "2. Call `tool_build_report(session_id, output_folder)` ONCE.\n"
                "3. Return the result from the tool call verbatim. Your job is done.\n\n"

                "## WHAT tool_build_report DOES (for your awareness — do NOT replicate manually)\n"
                "- Reads all `AnalysisResult` objects from session state\n"
                "- Collects all `.html` chart files from `output_folder`\n"
                "- Reads the `synthesis` dict from session state\n"
                "- Assembles a standalone `report.html` with all charts embedded as iframes\n"
                "- Writes `synthesis.json` alongside the report\n"
                "- Posts a REPORT_READY A2A message to the Orchestrator\n\n"

                "## DO's\n"
                "- DO call `tool_build_report(session_id, output_folder)` immediately with the exact arguments provided.\n"
                "- DO return the tool result to the Orchestrator as-is.\n"
                "- DO call the tool even if you suspect synthesis is empty — the tool handles missing synthesis gracefully.\n\n"

                "## DON'Ts\n"
                "- DON'T attempt to read files manually, construct HTML yourself, or call any other functions.\n"
                "- DON'T call `tool_build_report` more than once — it is idempotent but wasteful.\n"
                "- DON'T wait for additional data or request clarification — if session_id and output_folder are provided, act immediately.\n"
                "- DON'T output any text after the tool call — the tool return value is your complete response.\n"
            ),
            tools=[tool_build_report],
        )
    return _dag_builder_instance
