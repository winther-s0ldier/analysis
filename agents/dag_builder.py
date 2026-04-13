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
                print(f"INFO: dag_builder injected critique from ToolContext.state for {session_id}")
        except Exception:
            pass

    if not synthesis:
        try:
            from agents.synthesis import get_synthesis_result
            _store_synth = get_synthesis_result(session_id)
            if _store_synth:
                synthesis = _store_synth
                print(f"INFO: dag_builder got synthesis from _synthesis_store for {session_id}")
        except Exception:
            pass

    if not synthesis and state:
        synthesis = getattr(state, "synthesis", {}) or {}
        if synthesis:
            print(f"INFO: dag_builder got synthesis from state.synthesis for {session_id}")

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

    # Load profile for report metadata (row count, col count, etc.)
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
                print(f"INFO: dag_builder recovered dataset metadata from file cache for {session_id}")
        except Exception:
            pass

    if not synthesis:
        try:
            _results_cache_path = os.path.join(output_folder, "_results_cache.json")
            if os.path.exists(_results_cache_path):
                with open(_results_cache_path, "r", encoding="utf-8") as _rcf:
                    _raw_results = json.load(_rcf)
                if _raw_results:
                    synthesis = {
                        "executive_summary": {"overall_health": "Analysis completed. Synthesis unavailable — showing raw results only."},
                        "detailed_insights": {"insights": [
                            {
                                "node_id": _aid,
                                "analysis_type": _r.get("analysis_type", ""),
                                "ai_summary": _r.get("top_finding", ""),
                                "severity": _r.get("severity", "info"),
                                "root_cause_hypothesis": "",
                                "fix_priority": "medium",
                            }
                            for _aid, _r in _raw_results.items()
                            if isinstance(_r, dict) and _r.get("status") == "success"
                        ]},
                        "conversational_report": "Synthesis was not available. The following charts show the raw analysis results.",
                        "cross_metric_connections": {"connections": []},
                        "_qc_passed": False,
                        "_synthesis_skipped": True,
                    }
                    print(f"INFO: dag_builder built skeleton synthesis from {len(_raw_results)} results for {session_id}")
        except Exception as _sk_err:
            print(f"WARNING: dag_builder skeleton synthesis build failed: {_sk_err}")

    if not synthesis:
        print(f"WARNING: dag_builder found no synthesis for {session_id} — report will have no insights.")
        return {
            "status": "error",
            "error": "Synthesis not available. Cannot build report without insights. Re-run synthesis first.",
            "session_id": session_id,
        }

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
                        "decision_maker_takeaway": _ins_sum.get("decision_maker_takeaway", "") if isinstance(_ins_sum, dict) else "",
                    })
    except Exception as e:
        print(f"WARNING: dag_builder state chart collection failed: {e}")

    if os.path.exists(output_folder):
        for fname in os.listdir(output_folder):
            if fname.endswith(".html") and fname != "report.html":
                fpath = os.path.join(output_folder, fname)
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

    html = _build_report_html(session_id, charts, synthesis, dataset_type, csv_filename, profile)
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


def _build_report_html(session_id: str, charts: list, synthesis: dict, dataset_type: str, csv_filename: str, profile=None) -> str:
    generated = datetime.now().strftime("%B %d, %Y")
    generated_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    title = csv_filename or "Analytics Report"

    _SEV_COLOR = {"critical": "#BE123C", "high": "#B45309", "medium": "#1E40AF", "low": "#065F46", "info": "#475569"}
    _SEV_BG    = {"critical": "#FFF1F2", "high": "#FFFBEB", "medium": "#EFF6FF", "low": "#ECFDF5", "info": "#F8FAFC"}
    _SEV_LABEL = {"critical": "CRITICAL RISK", "high": "HIGH PRIORITY", "medium": "MODERATE", "low": "LOW", "info": "INFO"}

    def sc(s): return _SEV_COLOR.get(s, "#475569")
    def sb(s): return _SEV_BG.get(s, "#F8FAFC")
    def sl(s): return _SEV_LABEL.get(s, s.upper())

    def norm_sev(s, fallback="medium"):
        s = (s or fallback).lower()
        if s == "high" and "critical" in (s or ""): s = "critical"
        return s if s in _SEV_COLOR else fallback

    def badge(sev):
        c = sc(sev); bg = sb(sev)
        return (f'<span style="display:inline-block;font-size:10px;font-weight:700;'
                f'text-transform:uppercase;letter-spacing:0.06em;color:{c};'
                f'background:{bg};padding:2px 8px;border-radius:9999px;white-space:nowrap;">'
                f'{sl(sev)}</span>')

    def detail_row(label, content):
        if not content: return ""
        return (f'<div style="display:flex;gap:16px;padding:10px 0;'
                f'border-bottom:1px solid #F3F4F6;font-size:13px;line-height:1.65;">'
                f'<div style="flex-shrink:0;width:140px;font-size:10px;font-weight:700;'
                f'text-transform:uppercase;letter-spacing:0.06em;color:#9CA3AF;padding-top:2px;">'
                f'{label}</div>'
                f'<div style="flex:1;color:#374151;">{content}</div>'
                f'</div>')

    def li_list(items, ordered=False):
        if not items: return ""
        tag = "ol" if ordered else "ul"
        inner = "".join(f'<li style="margin-bottom:5px;">{x}</li>' for x in items)
        return f'<{tag} style="margin:0;padding-left:18px;">{inner}</{tag}>'

    def _extract_hero_stat(text):
        if not text:
            return None, None

        m = re.search(r'(\d+(?:\.\d+)?)\s*%', text)
        if m:
            val = float(m.group(1))
            if val >= 1.0:
                return f"{m.group(1)}%", "of total"

        m = re.search(r'(\d+(?:\.\d+)?)\s*[×xX]\b', text)
        if m:
            return f"{m.group(1)}×", "increase"

        m = re.search(r'\$\s*(\d+(?:[.,]\d+)*(?:\.\d+)?)\s*([KMBkmb]?)', text)
        if m:
            suffix = m.group(2).upper()
            return f"${m.group(1)}{suffix}", "impact"

        m = re.search(r'\b(\d{1,3}(?:,\d{3})+)\b', text)
        if m:
            return m.group(1), "records"
        return None, None

    ex = synthesis.get("executive_summary", {})
    if not isinstance(ex, dict): ex = {}
    health        = ex.get("overall_health", "")
    priorities    = ex.get("top_priorities", []) if ex else []
    biz_impact    = ex.get("business_impact", "")
    resource      = ex.get("resource_allocation", "")
    ex_timeline   = ex.get("timeline", "")

    di = synthesis.get("detailed_insights", {})
    insights_raw = di if isinstance(di, list) else (di.get("insights", []) if isinstance(di, dict) else [])
    insights_raw = [i for i in insights_raw if isinstance(i, dict)]
    _pri_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    insights_sorted = sorted(insights_raw, key=lambda x: _pri_order.get(norm_sev(x.get("fix_priority")), 2))

    _crit_rev     = synthesis.get("_critic_review", {})
    _cr_approved  = True
    _cr_conf      = 1.0
    _cr_verdict   = ""
    _cr_challenges = []
    if isinstance(_crit_rev, dict) and _crit_rev:
        _cr_approved   = _crit_rev.get("approved", True)
        _cr_conf       = float(_crit_rev.get("confidence_adjustment", 1.0))
        _cr_verdict    = _crit_rev.get("overall_verdict", "")
        _cr_challenges = _crit_rev.get("challenges", [])

    _node_confs = [float(c.get("confidence", 0) or 0) for c in charts if float(c.get("confidence", 0) or 0) > 0.05]
    if _node_confs:
        _avg_node_conf = sum(_node_confs) / len(_node_confs)
        _conf_pct = int(min(99, (_avg_node_conf * 0.70 + _cr_conf * 0.30) * 100))
    else:
        _conf_pct = int(min(99, _cr_conf * 75))

    st = synthesis.get("recommendations", synthesis.get("intervention_strategies", {}))
    strategies_list = st if isinstance(st, list) else (st.get("strategies", []) if isinstance(st, dict) else [])

    pe = synthesis.get("key_segments", synthesis.get("personas", {}))
    personas_list = pe if isinstance(pe, list) else (pe.get("segments", pe.get("personas", [])) if isinstance(pe, dict) else [])

    cx = synthesis.get("cross_metric_connections", {})
    connections_list = cx if isinstance(cx, list) else (cx.get("connections", []) if isinstance(cx, dict) else [])

    conv_rep = synthesis.get("conversational_report", "")
    if conv_rep:
        try:
            import markdown as _md
            conv_h = _md.markdown(conv_rep, extensions=["tables", "fenced_code"])
        except Exception:
            import html as _html
            import re as _re
            _s = _html.escape(conv_rep)
            _s = _re.sub(r'^### (.+)$', r'<h3 style="font-size:15px;font-weight:700;margin:18px 0 6px;">\1</h3>', _s, flags=_re.MULTILINE)
            _s = _re.sub(r'^## (.+)$',  r'<h2 style="font-size:17px;font-weight:700;margin:22px 0 8px;">\1</h2>', _s, flags=_re.MULTILINE)
            _s = _re.sub(r'^# (.+)$',   r'<h1 style="font-size:20px;font-weight:700;margin:26px 0 10px;">\1</h1>', _s, flags=_re.MULTILINE)
            _s = _re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', _s)
            _s = _re.sub(r'\*(.+?)\*',     r'<em>\1</em>', _s)
            _s = _re.sub(r'^[-*] (.+)$', r'<li style="margin-bottom:4px;">\1</li>', _s, flags=_re.MULTILINE)
            _s = _re.sub(r'(<li.*</li>)', r'<ul style="padding-left:20px;margin:8px 0;">\1</ul>', _s, flags=_re.DOTALL)
            _s = _re.sub(r'^---+$', r'<hr style="border:none;border-top:1px solid #E5E7EB;margin:16px 0;">', _s, flags=_re.MULTILINE)
            def _tbl(m):
                rows = [r.strip() for r in m.group(0).strip().split('\n') if '|' in r and not _re.match(r'^[\|\s\-:]+$', r)]
                if not rows: return m.group(0)
                html_rows = []
                for i, row in enumerate(rows):
                    cells = [c.strip() for c in row.strip('|').split('|')]
                    tag = 'th' if i == 0 else 'td'
                    style = 'padding:8px 12px;border:1px solid #E5E7EB;' + ('font-weight:600;background:#F9FAFB;' if i == 0 else '')
                    html_rows.append('<tr>' + ''.join(f'<{tag} style="{style}">{c}</{tag}>' for c in cells) + '</tr>')
                return '<table style="border-collapse:collapse;width:100%;margin:12px 0;">' + ''.join(html_rows) + '</table>'
            _s = _re.sub(r'((?:^\|.+\|\n?)+)', _tbl, _s, flags=_re.MULTILINE)
            def _paras(text):
                out, buf = [], []
                for line in text.split('\n'):
                    stripped = line.strip()
                    if not stripped:
                        if buf:
                            out.append(f'<p style="margin:8px 0;line-height:1.75;color:#374151;">{"".join(buf)}</p>')
                            buf = []
                    elif stripped.startswith('<'):
                        if buf:
                            out.append(f'<p style="margin:8px 0;line-height:1.75;color:#374151;">{"".join(buf)}</p>')
                            buf = []
                        out.append(stripped)
                    else:
                        buf.append(stripped + ' ')
                if buf:
                    out.append(f'<p style="margin:8px 0;line-height:1.75;color:#374151;">{"".join(buf)}</p>')
                return '\n'.join(out)
            conv_h = _paras(_s)
    else:
        conv_h = ""

    _insight_index = {}
    for _ins in insights_raw:
        _text = str(_ins.get("ai_summary", "")) + str(_ins.get("root_cause_hypothesis", "")) + str(_ins.get("title", ""))
        for _m in re.findall(r'\[([AC]\d+)\]', _text):
            if _m not in _insight_index:
                _insight_index[_m] = _ins

    total_nodes    = len(charts)
    critical_count = sum(1 for i in insights_sorted if norm_sev(i.get("fix_priority")) == "critical")
    high_count     = sum(1 for i in insights_sorted if norm_sev(i.get("fix_priority")) == "high")
    total_insights = len(insights_sorted)
    crit_col = "#059669" if _conf_pct >= 90 else "#D97706" if _conf_pct >= 70 else "#DC2626"
    crit_bg2 = "#ECFDF5" if _conf_pct >= 90 else "#FFFBEB" if _conf_pct >= 70 else "#FEF2F2"

    def stat_card(value, label, color, bg):
        return (f'<div style="flex:1;min-width:130px;background:#fff;border:1px solid #E2E8F0;'
                f'border-top:4px solid {color};border-radius:12px;padding:20px 22px;text-align:left;box-shadow: 0 1px 3px 0 rgb(0 0 0 / 0.1);">'
                f'<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.08em;color:#64748B;margin-bottom:6px;">{label}</div>'
                f'<div style="font-size:32px;font-weight:800;color:{color};line-height:1;">{value}</div>'
                f'</div>')

    kpi_row = (
        stat_card(total_nodes, "Analyses Run", "#4F46E5", "#EEF2FF") +
        stat_card(total_insights, "Findings", "#0F766E", "#F0FDFA") +
        (stat_card(critical_count, "Critical", "#DC2626", "#FEF2F2") if critical_count else "") +
        (stat_card(high_count, "High Priority", "#D97706", "#FFFBEB") if high_count else "") +
        stat_card(f"{_conf_pct}%", "Reliability", crit_col, crit_bg2)
    )

    _hero = next((i for i in insights_sorted if norm_sev(i.get("fix_priority")) in ("critical", "high")),
                 insights_sorted[0] if insights_sorted else None)
    if _hero:
        hero_title   = _hero.get("title", "")
        hero_summary = _hero.get("ai_summary", "")
        hero_sev     = norm_sev(_hero.get("fix_priority"))
        hero_col     = sc(hero_sev)
    else:
        hero_title   = ""
        hero_summary = health
        hero_sev     = "medium"
        hero_col     = "#2563EB"

    _sev_bar_parts = []
    for _s in ["critical", "high", "medium", "low"]:
        _cnt = sum(1 for i in insights_sorted if norm_sev(i.get("fix_priority")) == _s)
        if _cnt:
            _sev_bar_parts.append(
                f'<span style="display:inline-flex;align-items:center;gap:6px;font-size:12.5px;'
                f'font-weight:600;color:{sc(_s)};">'
                f'<span style="width:8px;height:8px;border-radius:50%;background:{sc(_s)};'
                f'display:inline-block;flex-shrink:0;"></span>{_cnt} {sl(_s)}</span>'
            )
    sev_bar = (
        f'<div style="display:flex;gap:24px;flex-wrap:wrap;padding:14px 0;'
        f'margin-bottom:20px;border-bottom:1px solid #F3F4F6;">'
        + "".join(_sev_bar_parts) + "</div>"
    ) if _sev_bar_parts else ""

    priority_cards = ""
    _sev_prog = ["critical", "high", "medium"]
    for i, p in enumerate(priorities[:3]):
        s = _sev_prog[min(i, 2)]
        priority_cards += (
            f'<div style="flex:1;min-width:220px;border:1px solid {sc(s)}22;border-left:3px solid {sc(s)};'
            f'border-radius:8px;padding:14px 16px;background:{sb(s)};">'
            f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;'
            f'color:{sc(s)};margin-bottom:6px;">Priority {i+1}</div>'
            f'<div style="font-size:13.5px;font-weight:600;color:#111827;line-height:1.45;">{p}</div>'
            f'</div>'
        )

    exec_left = ""
    if health:
        exec_left += (f'<p style="font-size:14px;color:#374151;line-height:1.8;margin:0 0 18px 0;">{health}</p>')
    if biz_impact:
        exec_left += (
            f'<div style="background:#FFFBEB;border-left:3px solid #F59E0B;padding:14px 16px;'
            f'border-radius:0 8px 8px 0;margin-bottom:18px;">'
            f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;'
            f'color:#B45309;margin-bottom:5px;">Business Impact</div>'
            f'<p style="font-size:13.5px;color:#78350F;margin:0;line-height:1.65;">{biz_impact}</p>'
            f'</div>'
        )
    if resource:
        exec_left += (f'<p style="font-size:13.5px;color:#4B5563;line-height:1.7;margin:0;">'
                      f'<strong style="color:#111827;">Resource Focus:</strong> {resource}</p>')

    exec_right = ""
    if ex_timeline:
        exec_right += (
            f'<div style="background:#F8F9FB;border:1px solid #E5E7EB;border-radius:8px;padding:16px;margin-bottom:16px;">'
            f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;'
            f'color:#6B7280;margin-bottom:10px;">Action Timeline</div>'
            f'<p style="font-size:13px;color:#374151;line-height:1.65;margin:0;">{ex_timeline}</p>'
            f'</div>'
        )

    exec_body = ""
    if exec_left or exec_right:
        exec_body = (
            f'<div style="display:flex;gap:32px;flex-wrap:wrap;">'
            f'<div style="flex:2;min-width:260px;">{exec_left}</div>'
            f'<div style="flex:1;min-width:200px;">{exec_right}</div>'
            f'</div>'
        )
    if priority_cards:
        exec_body += (
            f'<div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:20px;padding-top:20px;'
            f'border-top:1px solid #F3F4F6;">{priority_cards}</div>'
        )

    insight_cards = ""
    for idx, ins in enumerate(insights_sorted):
        sev = norm_sev(ins.get("fix_priority"))
        col = sc(sev); bg = sb(sev)
        i_title   = ins.get("title", "Insight")
        ai_sum    = ins.get("ai_summary", "")
        rc        = ins.get("root_cause_hypothesis", "")
        causes    = ins.get("possible_causes", [])
        ds_impl   = ins.get("downstream_implications", "")
        ux        = ins.get("ux_implications", "")
        fixes     = ins.get("how_to_fix", ins.get("recommended_solutions", []))
        impact    = float(ins.get("impact_score", 0) or 0)
        t_bucket  = ins.get("timeline_bucket", "")
        impact_pct = int((impact / 10) * 100)

        deep_rows = (
            detail_row("Root Cause", rc) +
            detail_row("Possible Causes", li_list(causes)) +
            detail_row("Downstream Impact", ds_impl) +
            detail_row("UX Implications", ux)
        )

        impact_bar = ""
        if impact:
            impact_bar = (
                f'<div style="display:flex;align-items:center;gap:10px;margin-top:10px;">'
                f'<span style="font-size:11px;color:#9CA3AF;white-space:nowrap;">Impact {impact:.1f}/10</span>'
                f'<div style="flex:1;height:4px;background:#F3F4F6;border-radius:9999px;">'
                f'<div style="height:4px;width:{impact_pct}%;background:{col};border-radius:9999px;"></div></div>'
                f'</div>'
            )

        bucket_tag = ""
        if t_bucket:
            bucket_tag = (f'<span style="font-size:10px;color:#9CA3AF;background:#F3F4F6;'
                          f'padding:2px 8px;border-radius:9999px;margin-left:6px;">{t_bucket}</span>')

        fixes_html = li_list(fixes, ordered=True)

        _stat_num, _stat_unit = _extract_hero_stat(ai_sum or i_title)
        stat_callout = ""
        if _stat_num and sev in ("critical", "high", "medium"):
            stat_callout = (
                f'<div style="float:right;margin-left:20px;margin-bottom:4px;'
                f'text-align:center;padding:10px 14px;background:{sb(sev)};'
                f'border-radius:10px;border:1px solid {col}33;flex-shrink:0;">'
                f'<div style="font-size:26px;font-weight:800;color:{col};'
                f'line-height:1;letter-spacing:-0.02em;">{_stat_num}</div>'
                f'<div style="font-size:9px;font-weight:600;color:{col}AA;'
                f'text-transform:uppercase;letter-spacing:0.06em;margin-top:3px;">'
                f'{_stat_unit}</div>'
                f'</div>'
            )

        _open_attr = "open" if sev in ("critical", "high") and idx < 3 else ""

        insight_cards += (
            f'<div style="border:1px solid #E5E7EB;border-radius:10px;overflow:hidden;'
            f'margin-bottom:14px;background:#fff;border-left:4px solid {col};">'
            f'<details {_open_attr} style="margin:0;">'

            f'<summary style="list-style:none;cursor:pointer;padding:18px 20px;'
            f'display:flex;align-items:flex-start;gap:14px;'
            f'transition:background 0.15s;" '
            f'onmouseover="this.style.background=\'#FAFAFA\'" '
            f'onmouseout="this.style.background=\'transparent\'">'
            f'<div style="flex-shrink:0;width:26px;height:26px;border-radius:50%;background:{bg};'
            f'display:flex;align-items:center;justify-content:center;font-size:11px;'
            f'font-weight:700;color:{col};margin-top:1px;">{idx+1}</div>'
            f'<div style="flex:1;min-width:0;">'
            f'<div style="display:flex;align-items:center;flex-wrap:wrap;gap:6px;margin-bottom:8px;">'
            f'{badge(sev)}{bucket_tag}'
            f'<span style="font-size:10px;color:#9CA3AF;margin-left:auto;padding-right:4px;">▼ details</span>'
            f'</div>'
            f'{stat_callout}'
            f'<div style="font-size:15px;font-weight:700;color:#111827;margin-bottom:8px;line-height:1.35;">{i_title}</div>'
            f'<p style="font-size:13.5px;color:#4B5563;line-height:1.75;margin:0;">{ai_sum}</p>'
            f'<div style="clear:both"></div>'
            f'{impact_bar}'
            f'</div></summary>'

            + (f'<div style="border-top:1px solid #F3F4F6;padding:4px 20px 4px 60px;background:#FAFAFA;">{deep_rows}</div>' if deep_rows else "")
            + (f'<div style="border-top:1px solid #F3F4F6;padding:16px 20px;">'
               f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;'
               f'color:#6B7280;margin-bottom:8px;">Action Steps</div>'
               f'<div style="font-size:13.5px;color:#374151;line-height:1.7;">{fixes_html}</div>'
               f'</div>' if fixes_html else "")
            + f'</details></div>'
        )

    _buckets_def = [
        ("Quick Win",  "1–2 weeks", "#059669", "#ECFDF5"),
        ("Medium Fix", "1 month",   "#D97706", "#FFFBEB"),
        ("Strategic",  "3+ months", "#6366F1", "#EEF2FF"),
    ]
    _bucket_rows_map = {b[0]: [] for b in _buckets_def}
    for ins in insights_sorted:
        tb = ins.get("timeline_bucket", "")
        bucket_key = "Strategic"
        for bname, _, _, _ in _buckets_def:
            if bname.lower() in tb.lower():
                bucket_key = bname
                break
        else:
            sev = norm_sev(ins.get("fix_priority"))
            bucket_key = "Quick Win" if sev in ("critical", "high") else ("Medium Fix" if sev == "medium" else "Strategic")
        fixes = ins.get("how_to_fix", ins.get("recommended_solutions", []))
        _bucket_rows_map[bucket_key].append({
            "title": ins.get("title", ""),
            "fix":   fixes[0] if fixes else "—",
            "sev":   norm_sev(ins.get("fix_priority")),
            "score": float(ins.get("impact_score", 0) or 0),
        })

    action_plan_html = ""
    for bname, bsub, bcol, bbg in _buckets_def:
        rows = _bucket_rows_map.get(bname, [])
        if not rows: continue
        rows_html = ""
        for r in rows:
            rows_html += (
                f'<tr style="border-bottom:1px solid #F3F4F6;">'
                f'<td style="padding:10px 14px;vertical-align:top;">{badge(r["sev"])}</td>'
                f'<td style="padding:10px 14px;font-weight:600;color:#111827;font-size:13.5px;vertical-align:top;">{r["title"]}</td>'
                f'<td style="padding:10px 14px;color:#4B5563;font-size:13px;vertical-align:top;">{r["fix"]}</td>'
                f'<td style="padding:10px 14px;text-align:center;vertical-align:top;">'
                f'<span style="font-size:13px;font-weight:700;color:{sc(r["sev"])};">{r["score"]:.1f}</span></td>'
                f'</tr>'
            )
        action_plan_html += (
            f'<div style="margin-bottom:24px;">'
            f'<div style="display:flex;align-items:baseline;gap:10px;margin-bottom:10px;">'
            f'<span style="font-size:13px;font-weight:700;color:{bcol};">{bname}</span>'
            f'<span style="font-size:11px;color:#9CA3AF;">{bsub}</span>'
            f'</div>'
            f'<div style="border:1px solid #E5E7EB;border-radius:8px;overflow:hidden;">'
            f'<table style="width:100%;border-collapse:collapse;font-size:13.5px;">'
            f'<thead><tr style="background:{bbg};">'
            f'<th style="padding:8px 14px;text-align:left;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;color:{bcol};border-bottom:2px solid {bcol}33;white-space:nowrap;">Priority</th>'
            f'<th style="padding:8px 14px;text-align:left;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;color:{bcol};border-bottom:2px solid {bcol}33;">Finding</th>'
            f'<th style="padding:8px 14px;text-align:left;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;color:{bcol};border-bottom:2px solid {bcol}33;">First Action</th>'
            f'<th style="padding:8px 14px;text-align:center;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;color:{bcol};border-bottom:2px solid {bcol}33;white-space:nowrap;">Impact</th>'
            f'</tr></thead>'
            f'<tbody>{rows_html}</tbody>'
            f'</table></div></div>'
        )

    segments_html = ""
    for p in personas_list:
        p_name  = p.get("name", "User Segment")
        p_prof  = p.get("profile", "")
        p_prior = norm_sev(p.get("priority_level"), "medium")
        p_col   = sc(p_prior); p_bg = sb(p_prior)
        pain_html = li_list(p.get("pain_points", []))
        opp_html  = li_list(p.get("opportunities", []))
        segments_html += (
            f'<div style="border:1px solid #E5E7EB;border-radius:10px;overflow:hidden;'
            f'background:#fff;border-top:3px solid {p_col};">'
            f'<div style="padding:14px 18px;background:#FAFAFA;border-bottom:1px solid #F3F4F6;'
            f'display:flex;align-items:center;justify-content:space-between;">'
            f'<div style="font-size:15px;font-weight:700;color:#111827;">{p_name}</div>'
            f'{badge(p_prior)}</div>'
            + (f'<div style="padding:10px 18px;font-size:13px;color:#6B7280;font-style:italic;'
               f'border-bottom:1px solid #F3F4F6;">{p_prof}</div>' if p_prof else "")
            + f'<div style="padding:16px 18px;display:flex;gap:24px;flex-wrap:wrap;">'
            f'<div style="flex:1;min-width:160px;">'
            f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;'
            f'color:#DC2626;margin-bottom:8px;">Pain Points</div>'
            f'<div style="font-size:13px;color:#374151;line-height:1.65;">{pain_html}</div></div>'
            f'<div style="flex:1;min-width:160px;">'
            f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;'
            f'color:#059669;margin-bottom:8px;">Opportunities</div>'
            f'<div style="font-size:13px;color:#374151;line-height:1.65;">{opp_html}</div></div>'
            f'</div></div>'
        )
    segments_grid = (f'<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));'
                     f'gap:16px;">{segments_html}</div>') if segments_html else ""

    connections_html = ""
    for conn in connections_list:
        fa      = conn.get("finding_a", "")
        fb      = conn.get("finding_b", "")
        meaning = conn.get("synthesized_meaning", "")
        connections_html += (
            f'<div style="border:1px solid #E5E7EB;border-radius:10px;overflow:hidden;'
            f'background:#fff;margin-bottom:12px;">'
            f'<div style="display:flex;align-items:center;gap:10px;padding:14px 18px;'
            f'background:#FAFAFA;border-bottom:1px solid #F3F4F6;flex-wrap:wrap;">'
            f'<span style="font-size:12px;font-weight:600;color:#4F46E5;background:#EEF2FF;'
            f'padding:4px 10px;border-radius:6px;">{fa}</span>'
            f'<span style="font-size:18px;color:#9CA3AF;line-height:1;">&#8594;</span>'
            f'<span style="font-size:12px;font-weight:600;color:#4F46E5;background:#EEF2FF;'
            f'padding:4px 10px;border-radius:6px;">{fb}</span>'
            f'</div>'
            + (f'<div style="padding:12px 18px;font-size:13.5px;color:#374151;line-height:1.65;">{meaning}</div>'
               if meaning else "")
            + f'</div>'
        )

    recommendations_html = ""
    for s in strategies_list:
        sev = norm_sev(s.get("severity"))
        col = sc(sev); bg = sb(sev)
        ttl = s.get("title", "Strategy")
        rt  = s.get("realtime_interventions", [])
        pro = s.get("proactive_outreach", [])
        recommendations_html += (
            f'<div style="border:1px solid #E5E7EB;border-radius:10px;overflow:hidden;'
            f'background:#fff;margin-bottom:16px;border-left:4px solid {col};">'
            f'<div style="display:flex;align-items:center;gap:10px;padding:14px 18px;'
            f'background:#FAFAFA;border-bottom:1px solid #F3F4F6;">'
            f'{badge(sev)}'
            f'<span style="font-size:14.5px;font-weight:700;color:#111827;">{ttl}</span>'
            f'</div>'
            f'<div style="display:flex;gap:0;flex-wrap:wrap;">'
            + (f'<div style="flex:1;min-width:220px;padding:16px 18px;border-right:1px solid #F3F4F6;">'
               f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;'
               f'color:#6B7280;margin-bottom:8px;">Real-Time Interventions</div>'
               f'<div style="font-size:13px;color:#374151;line-height:1.65;">{li_list(rt)}</div>'
               f'</div>' if rt else "")
            + (f'<div style="flex:1;min-width:220px;padding:16px 18px;">'
               f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;'
               f'color:#6B7280;margin-bottom:8px;">Proactive Outreach</div>'
               f'<div style="font-size:13px;color:#374151;line-height:1.65;">{li_list(pro)}</div>'
               f'</div>' if pro else "")
            + f'</div></div>'
        )

    charts_html = ""
    for fig_num, c in enumerate(charts, 1):
        a_id        = c.get("analysis_id", "")
        a_type      = c.get("analysis_type", "").replace("_", " ").title()
        sev         = norm_sev(c.get("severity"), "info")
        col         = sc(sev); bg = sb(sev)
        html_content = c.get("_embedded_html", "")
        narrative   = c.get("narrative", {}) if isinstance(c.get("narrative"), dict) else {}
        top_finding = c.get("top_finding", "")
        dm_takeaway = c.get("decision_maker_takeaway", "")
        confidence  = float(c.get("confidence", 0) or 0)
        conf_pct    = int(round(confidence * 100))

        safe_html = (html_content
                     .replace("&", "&amp;").replace('"', "&quot;")
                     .replace("<", "&lt;").replace(">", "&gt;"))

        conf_bar = ""
        if conf_pct:
            cc = "#059669" if conf_pct >= 80 else "#D97706" if conf_pct >= 60 else "#DC2626"
            conf_bar = (
                f'<div style="display:flex;align-items:center;gap:10px;padding:8px 20px;'
                f'background:#FAFAFA;border-bottom:1px solid #F3F4F6;">'
                f'<span style="font-size:11px;color:#9CA3AF;white-space:nowrap;">Confidence</span>'
                f'<div style="flex:1;height:3px;background:#F3F4F6;border-radius:9999px;">'
                f'<div style="height:3px;width:{conf_pct}%;background:{cc};border-radius:9999px;"></div></div>'
                f'<span style="font-size:11px;color:{cc};font-weight:600;">{conf_pct}%</span>'
                + (f'<span style="font-size:10px;color:#D97706;margin-left:4px;">directional</span>' if conf_pct < 75 else "")
                + f'</div>'
            )

        takeaway = dm_takeaway or top_finding
        takeaway_html = ""
        if takeaway:
            _t_num, _t_unit = _extract_hero_stat(takeaway)
            _t_stat = ""
            if _t_num:
                _t_stat = (
                    f'<div style="float:right;margin-left:16px;text-align:center;'
                    f'padding:6px 12px;background:rgba(245,158,11,0.12);'
                    f'border-radius:8px;border:1px solid rgba(245,158,11,0.3);">'
                    f'<div style="font-size:22px;font-weight:800;color:#B45309;line-height:1;">{_t_num}</div>'
                    f'<div style="font-size:9px;font-weight:600;color:#D97706;text-transform:uppercase;'
                    f'letter-spacing:0.06em;margin-top:2px;">{_t_unit}</div>'
                    f'</div>'
                )
            takeaway_html = (
                f'<div style="padding:12px 20px;background:#FFFBEB;border-bottom:1px solid #F3F4F6;'
                f'border-left:3px solid #F59E0B;">'
                f'{_t_stat}'
                f'<strong style="color:#92400E;font-size:10px;text-transform:uppercase;letter-spacing:0.06em;">Key Finding</strong>'
                f'<p style="margin:4px 0 0 0;font-size:13.5px;color:#78350F;line-height:1.55;">{takeaway}</p>'
                f'<div style="clear:both"></div>'
                f'</div>'
            )

        what_it_means = narrative.get("what_it_means", "")
        proposed_fix  = narrative.get("proposed_fix", "")
        narr_html = ""
        if what_it_means or proposed_fix:
            narr_html = '<div style="display:flex;gap:0;flex-wrap:wrap;border-top:1px solid #F3F4F6;">'
            if what_it_means:
                narr_html += (
                    f'<div style="flex:1;min-width:220px;padding:14px 18px;background:#EFF6FF;border-right:1px solid #DBEAFE;">'
                    f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;color:#1D4ED8;margin-bottom:6px;">What It Means</div>'
                    f'<p style="margin:0;font-size:13px;color:#1E3A8A;line-height:1.65;">{what_it_means}</p></div>'
                )
            if proposed_fix:
                narr_html += (
                    f'<div style="flex:1;min-width:220px;padding:14px 18px;background:#FFF7ED;">'
                    f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;color:#C2410C;margin-bottom:6px;">Proposed Fix</div>'
                    f'<p style="margin:0;font-size:13px;color:#7C2D12;line-height:1.65;">{proposed_fix}</p></div>'
                )
            narr_html += '</div>'

        matched_ins = _insight_index.get(a_id)
        deep_html = ""
        if matched_ins:
            _ai_sum  = matched_ins.get("ai_summary", "")
            _rc      = matched_ins.get("root_cause_hypothesis", "")
            _causes  = matched_ins.get("possible_causes", [])
            _ds_impl = matched_ins.get("downstream_implications", "")
            _fixes   = matched_ins.get("how_to_fix", matched_ins.get("recommended_solutions", []))
            _pri     = norm_sev(matched_ins.get("fix_priority"))
            deep_rows = (
                detail_row("AI Analysis", _ai_sum) +
                detail_row("Root Cause", _rc) +
                detail_row("Possible Causes", li_list(_causes)) +
                detail_row("Downstream Impact", _ds_impl) +
                detail_row("How to Fix", li_list(_fixes, ordered=True))
            )
            if deep_rows:
                deep_html = (
                    f'<div style="border-top:1px solid #F3F4F6;padding:14px 20px;background:#FAFAFA;">'
                    f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;'
                    f'color:#6B7280;margin-bottom:8px;">Synthesis Deep Dive [{a_id}] {badge(_pri)}</div>'
                    f'{deep_rows}</div>'
                )

        charts_html += (
            f'<div style="border:1px solid #E5E7EB;border-radius:10px;overflow:hidden;'
            f'background:#fff;margin-bottom:28px;">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;'
            f'padding:12px 20px;background:#FAFAFA;border-bottom:1px solid #F3F4F6;">'
            f'<div>'
            f'<span style="font-size:10px;color:#9CA3AF;text-transform:uppercase;letter-spacing:0.06em;font-weight:600;">Figure {fig_num}</span>'
            f'<span style="font-size:14.5px;font-weight:700;color:#111827;margin-left:10px;">{a_type}</span>'
            f'<span style="font-size:11px;color:#9CA3AF;margin-left:6px;">[{a_id}]</span>'
            f'</div>{badge(sev)}</div>'
            f'{conf_bar}'
            f'{takeaway_html}'
            f'<iframe srcdoc="{safe_html}" width="100%" height="480" frameborder="0" style="display:block;"></iframe>'
            f'{narr_html}'
            f'{deep_html}'
            f'</div>'
        )

    _nav_items = []
    _sec_num = [0]
    def _next_num():
        _sec_num[0] += 1
        return _sec_num[0]

    if exec_body:            _nav_items.append(("executive",       "Executive Summary"))
    if conv_h:               _nav_items.append(("findings",        "Key Findings"))
    if insight_cards:        _nav_items.append(("insights",        "Findings"))
    if action_plan_html:     _nav_items.append(("action-plan",     "Action Plan"))
    if segments_grid:        _nav_items.append(("segments",        "Segments"))
    if connections_html:     _nav_items.append(("connections",     "Connections"))
    if recommendations_html: _nav_items.append(("recommendations", "Recommendations"))
    if charts_html:          _nav_items.append(("charts",          "Appendix: Charts"))

    nav_links = "".join(
        f'<a href="#{a}" style="color:rgba(255,255,255,0.6);text-decoration:none;font-size:12px;'
        f'font-weight:500;padding:4px 8px;border-radius:4px;white-space:nowrap;"'
        f' onmouseover="this.style.background=\'rgba(255,255,255,0.12)\'"'
        f' onmouseout="this.style.background=\'transparent\'">{lbl}</a>'
        for a, lbl in _nav_items
    )

    toc_items_html = ""
    for i, (a, lbl) in enumerate(_nav_items):
        num_str = f"{i+1:02d}"
        toc_items_html += (
            f'<a href="#{a}" style="display:flex;align-items:center;gap:10px;padding:10px 12px;'
            f'border:1px solid #F3F4F6;border-radius:8px;text-decoration:none;'
            f'background:#FAFAFA;transition:background 0.15s;"'
            f' onmouseover="this.style.background=\'#EEF2FF\'" '
            f' onmouseout="this.style.background=\'#FAFAFA\'">'
            f'<span style="font-size:12px;font-weight:800;color:#C7D2FE;width:24px;">{num_str}</span>'
            f'<span style="font-size:13px;font-weight:600;color:#374151;">{lbl}</span>'
            f'</a>'
        )
    toc_html = (
        f'<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;'
        f'padding:32px;margin-bottom:48px;box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1);">'
        f'<div style="font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:0.1em;'
        f'color:#94A3B8;margin-bottom:20px;">Report Navigation</div>'
        f'<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px;">'
        f'{toc_items_html}</div></div>'
        f'<div style="margin-bottom:48px;padding:24px;background:#F8FAFC;border-radius:12px;border:1px solid #E2E8F0;display:flex;gap:20px;align-items:center;">'
        f'<div style="background:#10B981;color:white;padding:10px 14px;border-radius:8px;font-weight:800;font-size:14px;">Insight</div>'
        f'<div style="font-size:14px;color:#475569;line-height:1.5;">This AI-generated analysis follows the <strong>Pyramid Principle</strong>: conclusions and strategic recommendations are presented before technical data.</div>'
        f'</div>'
    ) if toc_items_html else ""

    def section(anchor, label, body, divider=False):
        if not body: return ""
        num = _next_num()
        num_str = f"{num:02d}"
        div_style = "border-top:1px solid #F3F4F6;margin-top:36px;padding-top:36px;" if divider else ""
        return (
            f'<section id="{anchor}" style="{div_style}margin-bottom:40px;page-break-inside:avoid;">'
            f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;">'
            f'<span style="font-size:12px;font-weight:800;color:#C7D2FE;">{num_str}</span>'
            f'<h2 style="font-size:18px;font-weight:800;color:#0F1F3D;margin:0;letter-spacing:-0.3px;">{label}</h2>'
            f'<div style="flex:1;height:1px;background:#E5E7EB;"></div>'
            f'</div>'
            f'{body}</section>'
        )

    # Pre-compute hero band to avoid triple-nested f-strings (Python 3.10 incompatible)
    if _conf_pct:
        _quality_div = (
            '<div style="width:1px;height:120px;background:#E2E8F0;"></div>'
            '<div style="width:200px;flex-shrink:0;">'
            '<div style="font-size:10px;font-weight:700;color:#94A3B8;text-transform:uppercase;margin-bottom:8px;">Data Quality</div>'
            f'<div style="font-size:24px;font-weight:800;color:{crit_col};">{_conf_pct}%</div>'
            '<div style="font-size:11px;color:#64748B;line-height:1.4;">Reliability index for current session.</div>'
            '</div>'
        )
    else:
        _quality_div = ""

    if hero_title or hero_summary:
        _hero_band_html = (
            '<div class="hero-band" style="background:#F8FAFC;border-bottom:1px solid #E2E8F0;padding:40px 48px;">'
            '<div style="max-width:1036px;margin:0 auto;display:flex;gap:40px;align-items:flex-start;">'
            '<div style="flex:1;">'
            '<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.12em;'
            'color:#10B981;margin-bottom:12px;">Strategic Priority</div>'
            f'<div style="font-size:24px;font-weight:800;color:#0F172A;line-height:1.2;margin-bottom:16px;'
            f'letter-spacing:-0.02em;">{hero_title}</div>'
            f'<p style="font-size:15px;color:#475569;line-height:1.7;margin:0;max-width:700px;">{hero_summary}</p>'
            '</div>'
            f'{_quality_div}'
            '</div>'
            '</div>'
        )
    else:
        _hero_band_html = ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} — Analytics Report</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #F0F2F5;
    color: #111827;
    line-height: 1.6;
    -webkit-font-smoothing: antialiased;
  }}
  .top-nav {{
    position: sticky; top: 0; z-index: 100;
    background: #0F1F3D;
    padding: 0 32px;
    display: flex; align-items: center; gap: 4px;
    overflow-x: auto; white-space: nowrap;
    height: 44px;
    box-shadow: 0 1px 0 rgba(255,255,255,0.08);
  }}
  .top-nav .brand {{
    font-size: 12px; font-weight: 700; color: rgba(255,255,255,0.9);
    letter-spacing: 0.04em; text-transform: uppercase; margin-right: 20px;
    white-space: nowrap; flex-shrink: 0;
  }}
  .cover {{
    background: #0F172A;
    color: white; padding: 64px 48px 48px 48px;
    border-bottom: 6px solid #10B981;
  }}
  .cover-eyebrow {{
    font-size: 11px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.15em; color: #10B981; margin-bottom: 16px;
  }}
  .cover-title {{
    font-size: 36px; font-weight: 800; color: #fff;
    letter-spacing: -1px; line-height: 1.1; margin-bottom: 16px;
    max-width: 800px;
  }}
  .cover-meta {{
    font-size: 12.5px; color: rgba(255,255,255,0.5);
    display: flex; gap: 20px; flex-wrap: wrap; margin-bottom: 28px;
  }}
  .cover-meta span {{ display: flex; align-items: center; gap: 5px; }}
  .kpi-band {{
    display: flex; gap: 12px; flex-wrap: wrap;
    margin-top: 0; padding-top: 24px;
    border-top: 1px solid rgba(255,255,255,0.12);
  }}
  .body-wrap {{ max-width: 1100px; margin: 0 auto; padding: 40px 32px 80px 32px; }}
  .prose h1 {{ font-size: 18px; font-weight: 700; color: #0F1F3D; margin: 24px 0 10px; }}
  .prose h2 {{ font-size: 16px; font-weight: 700; color: #0F1F3D; margin: 20px 0 8px; }}
  .prose h3 {{ font-size: 14px; font-weight: 600; color: #374151; margin: 16px 0 6px; }}
  .prose p  {{ font-size: 14px; color: #374151; margin-bottom: 12px; line-height: 1.75; }}
  .prose ul, .prose ol {{ font-size: 14px; color: #374151; padding-left: 22px; margin-bottom: 12px; }}
  .prose li {{ margin-bottom: 5px; line-height: 1.65; }}
  .prose strong {{ font-weight: 600; color: #111827; }}
  .prose table {{ width: 100%; border-collapse: collapse; margin: 16px 0; font-size: 13px; }}
  .prose th {{ background: #F8F9FB; font-weight: 600; color: #374151; padding: 8px 12px;
               border: 1px solid #E5E7EB; text-align: left; font-size: 11px;
               text-transform: uppercase; letter-spacing: 0.04em; }}
  .prose td {{ padding: 8px 12px; border: 1px solid #E5E7EB; color: #4B5563; vertical-align: top; }}
  .prose tr:nth-child(even) td {{ background: #FAFAFA; }}
  details > summary {{ list-style: none; }}
  details > summary::-webkit-details-marker {{ display: none; }}
  details[open] > summary .tog {{ transform: rotate(180deg); }}
  @media print {{
    .top-nav, .hero-band {{ display: none; }}
    body {{ background: white; font-size: 12px; }}
    .cover {{
      -webkit-print-color-adjust: exact; print-color-adjust: exact;
      padding: 32px 40px 24px;
    }}
    section {{ page-break-inside: avoid; }}
    details {{ open: true; }}
    details > summary {{ pointer-events: none; }}
    .body-wrap {{ padding: 24px 32px; }}
  }}
</style>
</head>
<body>

<nav class="top-nav">
  <div class="brand">Analytics</div>
  {nav_links}
</nav>

<div class="cover">
  <div style="max-width:1036px;margin:0 auto;">
    <div class="cover-eyebrow">Confidential &mdash; AI-Assisted Analysis</div>
    <div class="cover-title">{title}</div>
    <div class="cover-meta">
      <span>Dataset Type: {dataset_type or "—"}</span>
      <span>Generated: {generated}</span>
      <span>Session: {session_id[:8]}…</span>
    </div>
    <div class="kpi-band">{kpi_row}</div>
  </div>
</div>

{_hero_band_html}

<div class="body-wrap">

  {toc_html}

  {section("executive", "Executive Summary", exec_body)}
  {section("findings", "Key Findings", f'<div class="prose">{conv_h}</div>' if conv_h else "", divider=bool(exec_body))}
  {section("insights", "Detailed Findings", sev_bar + insight_cards, divider=bool(conv_h or exec_body))}
  {section("action-plan", "Action Plan", action_plan_html, divider=True)}
  {section("segments", "User Segments", segments_grid, divider=True)}
  {section("connections", "Cross-Metric Connections", connections_html, divider=True)}
  {section("recommendations", "Recommendations", recommendations_html, divider=True)}
  {section("charts", "Appendix: Analysis Charts", charts_html, divider=True)}

  <div style="text-align:center;color:#9CA3AF;font-size:11px;padding-top:32px;border-top:1px solid #F3F4F6;">
    {title} &mdash; {generated} &mdash; Confidential
  </div>

</div>
</body>
</html>"""


_dag_builder_instance = None

_PROMPT_DIR = os.path.join(os.path.dirname(__file__), '..', 'prompts')

def _load_prompt(name: str) -> str:
    path = os.path.join(_PROMPT_DIR, name)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        raise RuntimeError(f"Prompt file missing: {path}. Ensure the prompts/ directory is present.")
    except Exception as e:
        raise RuntimeError(f"Failed to load prompt {name}: {e}") from e

def get_dag_builder_agent():
    global _dag_builder_instance
    if _dag_builder_instance is None:
        from google.adk.agents import Agent
        from google.adk.tools import FunctionTool
        from tools.model_config import get_model

        _dag_builder_instance = Agent(
            name="dag_builder_agent",
            model=get_model("dag_builder"),
            description="Report assembly specialist. Collects charts and synthesis narrative, assembles the final standalone HTML report.",
            instruction=_load_prompt("dag_builder.md"),
            tools=[FunctionTool(tool_build_report)],
        )
    return _dag_builder_instance
