import os
import sys
import json
from datetime import datetime

def deterministic_executive_summary(results: dict, dataset_type: str) -> dict:
    """
    Generate a 5-7 bullet executive summary from all analysis results.
    Adapted from tool_generate_executive_summary in synthesis.py.
    """
    bullets = []
    caveats = []
    priority = ""

    for atype, result in results.items():
        finding = result.get("top_finding", "")
        severity = result.get("severity", "info")
        confidence = result.get("confidence", 0.0)

        if not finding or len(finding) < 10:
            continue

        if severity == "critical":
            bullets.insert(0, f"[CRITICAL] {finding}")
            if not priority:
                priority = finding
        elif severity == "high":
            bullets.append(f"[HIGH] {finding}")
        else:
            bullets.append(finding)

        if confidence < 0.75:
            caveats.append(f"{atype} confidence: {confidence:.0%} — treat with caution.")

    if not bullets:
        bullets = ["Analysis complete. No critical issues detected in available data."]

    if not priority and bullets:
        priority = bullets[0].replace("[CRITICAL] ", "").replace("[HIGH] ", "")

    return {
        "bullets":           bullets[:7],
        "top_priority":      priority,
        "data_quality_note": ("; ".join(caveats) if caveats else "All analyses completed with high confidence."),
    }

def deterministic_personas(seg: dict, friction: dict = None, funnel: dict = None) -> dict:
    """
    Generate named personas from segmentation clusters.
    Adapted from tool_generate_segment_personas in synthesis.py.
    """
    if not seg:
        return {"personas": [], "persona_count": 0, "coverage_pct": 0.0}

    segments = seg.get("data", {}).get("segments", [])
    if not segments:
        return {"personas": [], "persona_count": 0, "coverage_pct": 0.0}

    friction_events = []
    if friction:
        top_friction = friction.get("data", {}).get("top_friction_events", [])
        friction_events = [e.get("event_name", "") for e in top_friction[:3] if e.get("severity") in ("critical", "high")]

    funnel_bottleneck = None
    if funnel:
        funnel_bottleneck = funnel.get("data", {}).get("biggest_drop_step")

    personas = []
    total_users = sum(s.get("size", 0) for s in segments)

    for s in segments:
        if s.get("is_noise"): continue
        profile = s.get("profile", {})
        chars = s.get("characteristics", [])
        size = s.get("size", 0)
        pct = s.get("pct", 0.0)

        if "highly engaged" in chars and "broad exploration" in chars:
            name, description = "Power Users", f"{size:,} users ({pct}%) who explore extensively and return frequently. High value, low friction."
        elif "high friction" in chars and "quick sessions" in chars:
            name, description = "Struggling Users", f"{size:,} users ({pct}%) with short sessions and high repetition. " + (f"Likely stuck at: {', '.join(friction_events)}." if friction_events else "Need targeted support.")
        elif "quick sessions" in chars:
            name, description = "Casual Browsers", f"{size:,} users ({pct}%) with short, infrequent sessions. Low engagement, high dropout risk." + (f" Typically leave at '{funnel_bottleneck}'." if funnel_bottleneck else "")
        elif "highly engaged" in chars:
            name, description = "Loyal Users", f"{size:,} users ({pct}%) with frequent returns and consistent patterns. Core retention segment."
        else:
            name, description = f"Segment {s['segment_id']}", f"{size:,} users ({pct}%) with mixed behavioral patterns. Avg {profile.get('total_events',0):.0f} events, {profile.get('session_count',0):.0f} sessions."

        personas.append({
            "name": name, "size": size, "pct": pct, "description": description,
            "profile": profile, "characteristics": chars,
            "pain_points": (friction_events if "high friction" in chars else []),
            "opportunity": ("Reduce friction at key steps" if "high friction" in chars else "Increase engagement depth" if "casual" in name.lower() else "Leverage as advocates"),
        })

    coverage = round(sum(p["size"] for p in personas) / max(total_users, 1) * 100, 1)
    return {"personas": personas, "persona_count": len(personas), "coverage_pct": coverage}

def deterministic_strategies(results: dict, dataset_type: str) -> dict:
    """
    Generate ranked intervention strategies from all analysis results.
    Adapted from tool_generate_intervention_strategies in synthesis.py.
    """
    strategies = []
    session = results.get("session_detection", {})
    if session:
        bounce = session.get("data", {}).get("bounce_rate", 0)
        if bounce > 30:
            strategies.append({"severity": "critical", "title": "High Bounce Rate", "root_cause": f"{bounce:.1f}% of sessions end after a single event.", "action": "Investigate onboarding flow. Add progressive disclosure to reduce immediate abandonment.", "impact": "High", "complexity": "Medium"})

    friction = results.get("friction_detection", {})
    if friction:
        for event in [e for e in friction.get("data", {}).get("top_friction_events", []) if e.get("severity") == "critical"][:3]:
            strategies.append({"severity": "critical", "title": f"Critical Friction: {event.get('event_name','unknown')}", "root_cause": f"{event.get('repetition_rate',0):.1%} of interactions at this step are repeated attempts — users are stuck.", "action": f"Redesign the '{event.get('event_name','this')}' experience. Add helper text, simplify inputs, or add a fallback.", "impact": "High", "complexity": "Low"})

    funnel = results.get("funnel_analysis", {})
    if funnel:
        drop_step, drop_pct, overall = funnel.get("data", {}).get("biggest_drop_step"), funnel.get("data", {}).get("biggest_drop_pct", 0), funnel.get("data", {}).get("overall_conversion", 100)
        if drop_step and drop_pct > 20:
            strategies.append({"severity": ("critical" if drop_pct > 50 else "high"), "title": f"Funnel Drop-off at '{drop_step}'", "root_cause": f"{drop_pct:.1f}% of users leave at '{drop_step}'. Overall conversion: {overall:.1f}%.", "action": f"A/B test the '{drop_step}' step. Reduce required fields, add progress indicators, or simplify the action required.", "impact": "Very High", "complexity": "Medium"})

    survival = results.get("survival_analysis", {})
    if survival:
        pct_10 = survival.get("data", {}).get("pct_reach_step_10", 100)
        if pct_10 < 40:
            strategies.append({"severity": "high", "title": "Sessions Ending Too Early", "root_cause": f"Only {pct_10:.1f}% of sessions reach step 10. Most users abandon before engaging with core features.", "action": "Move core value delivery earlier in the user journey. Reduce steps required before meaningful outcome.", "impact": "High", "complexity": "High"})

    anomaly = results.get("anomaly_detection", {})
    if anomaly:
        outlier_pct = anomaly.get("data", {}).get("outlier_pct", 0)
        if outlier_pct > 10:
            strategies.append({"severity": "high", "title": "Significant Data Anomalies", "root_cause": f"{outlier_pct:.1f}% of records are statistical outliers.", "action": "Investigate outlier records. Determine if they represent data quality issues or genuine edge cases requiring special handling.", "impact": "Medium", "complexity": "Low"})

    trend = results.get("trend_analysis", {})
    if trend:
        direction, pct_change = trend.get("data", {}).get("trend_direction", ""), trend.get("data", {}).get("pct_change", 0)
        if direction == "downward" and pct_change < -20:
            strategies.append({"severity": "high", "title": "Significant Downward Trend", "root_cause": f"Metric has declined {abs(pct_change):.1f}% over the measured period.", "action": "Identify the time period where decline accelerated using changepoint analysis. Correlate with product or market changes.", "impact": "High", "complexity": "Medium"})

    strategies.sort(key=lambda s: {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(s["severity"], 4))
    if not strategies:
        strategies.append({"severity": "low", "title": "No Critical Issues Detected", "root_cause": "All analyzed metrics are within acceptable ranges.", "action": "Continue monitoring. Consider adding more specific custom metrics for deeper insight.", "impact": "Low", "complexity": "Low"})

    return {"strategies": strategies, "critical_count": sum(1 for s in strategies if s["severity"] == "critical"), "high_count": sum(1 for s in strategies if s["severity"] == "high")}

def deterministic_connections(results: dict) -> dict:
    """
    Find findings that reinforce each other across multiple analyses.
    Adapted from tool_generate_cross_metric_connections in synthesis.py.
    """
    connections = []
    friction, funnel, survival, segments, patterns = results.get("friction_detection"), results.get("funnel_analysis"), results.get("survival_analysis"), results.get("user_segmentation"), results.get("sequential_pattern_mining")

    if friction and funnel:
        fr_events = friction.get("data", {}).get("top_friction_events", [])
        top_friction_event = fr_events[0].get("event_name") if fr_events else None
        drop_step = funnel.get("data", {}).get("biggest_drop_step")
        if top_friction_event and drop_step and (top_friction_event == drop_step or top_friction_event in str(drop_step)):
            connections.append({"title": "Friction Causing Funnel Drop", "analyses": ["friction_detection", "funnel_analysis"], "insight": f"The highest-friction event ('{top_friction_event}') coincides with the biggest funnel drop-off. These are the same problem — users stuck here are leaving.", "severity": "critical", "confidence": 0.91})

    if segments and friction:
        seg_count, critical_fr = segments.get("data", {}).get("segment_count", 0), friction.get("data", {}).get("critical_events", 0)
        if seg_count > 1 and critical_fr > 0:
            connections.append({"title": "Friction Affects Specific Segments", "analyses": ["user_segmentation", "friction_detection"], "insight": f"With {seg_count} distinct user segments and {critical_fr} critical friction points, friction likely affects some segments far more than others. The struggling segment is the primary target for fixes.", "severity": "high", "confidence": 0.82})

    if survival and funnel:
        pct_10, overall_c = survival.get("data", {}).get("pct_reach_step_10", 100), funnel.get("data", {}).get("overall_conversion", 100)
        if pct_10 < 50 and overall_c < 50:
            connections.append({"title": "Volume Loss Before Funnel Starts", "analyses": ["survival_analysis", "funnel_analysis"], "insight": f"Only {pct_10:.0f}% of sessions reach step 10, and overall funnel conversion is {overall_c:.0f}%. Volume is being lost both before the funnel begins (early abandonment) and within the funnel itself.", "severity": "critical", "confidence": 0.88})

    if patterns and friction:
        loops, critical_fr = patterns.get("data", {}).get("repetition_loops", 0), friction.get("data", {}).get("critical_events", 0)
        if loops > 0 and critical_fr > 0:
            connections.append({"title": "Loop Patterns Confirm Friction", "analyses": ["sequential_pattern_mining", "friction_detection"], "insight": f"{loops} repetition loop pattern(s) found in sequences, confirming the {critical_fr} critical friction events detected. Users are not just retrying once — they are cycling.", "severity": "high", "confidence": 0.85})

    return {"connections": connections, "connection_count": len(connections), "strongest_connection": (connections[0] if connections else None)}

def deterministic_detailed_insights(results: dict) -> dict:
    """
    Generate detailed_insights list matching the schema expected by
    dag_builder._build_report_html: each insight has title, ai_summary,
    root_cause_hypothesis, possible_causes, ux_implications, fix_priority,
    how_to_fix.
    """
    insights = []

    session = results.get("session_detection", {})
    if session and session.get("top_finding"):
        d = session.get("data", {})
        bounce = d.get("bounce_rate", 0)
        avg_len = d.get("avg_session_length", 0)
        insights.append({
            "title": "Session Behaviour Overview",
            "ai_summary": session.get("top_finding", ""),
            "root_cause_hypothesis": (
                f"A {bounce:.1f}% bounce rate suggests users are not finding immediate value. "
                f"Average session length of {avg_len:.1f} events may indicate a shallow engagement loop."
                if bounce > 30 else
                "Sessions are reasonably long; drop-off is likely tied to specific friction points rather than first-impression failure."
            ),
            "possible_causes": [
                f"Onboarding does not communicate value quickly enough (bounce={bounce:.1f}%)",
                "First screens contain too many choices or require too much effort",
                "Users arrive with misaligned expectations (traffic quality issue)",
            ],
            "ux_implications": "Prioritise the first 3 events a new user encounters. Every extra step in an onboarding flow reduces retention exponentially.",
            "fix_priority": "critical" if bounce > 40 else "high" if bounce > 20 else "medium",
            "how_to_fix": [
                "Add a value-statement screen immediately after first login",
                "Reduce the number of mandatory fields / steps before a user reaches the core action",
                "A/B test two onboarding flows: guided vs. self-serve",
            ],
        })

    friction = results.get("friction_detection", {})
    if friction and friction.get("top_finding"):
        d = friction.get("data", {})
        top_events = d.get("top_friction_events", [])
        top_name = top_events[0].get("event_name", "Unknown") if top_events else "Unknown"
        rep_rate = top_events[0].get("repetition_rate", 0) if top_events else 0
        critical_count = d.get("critical_events", 0)
        insights.append({
            "title": f"User Friction — '{top_name}' Is the Primary Pain Point",
            "ai_summary": friction.get("top_finding", ""),
            "root_cause_hypothesis": (
                f"'{top_name}' has a {rep_rate:.1%} repetition rate, meaning users attempt this action multiple times before succeeding or giving up. "
                f"With {critical_count} critical friction events, the product has systemic UX issues at key decision points."
            ),
            "possible_causes": [
                f"'{top_name}' UI element is unclear, hidden, or broken",
                "Error messages at this step are unhelpful or do not guide recovery",
                "Required information is unavailable to the user at the point of action",
                "Mobile layout may not render the element correctly",
            ],
            "ux_implications": (
                f"Users experiencing '{top_name}' friction are {2 + round(rep_rate * 5)}x more likely to churn. "
                "Every failed attempt increases frustration and reduces trust."
            ),
            "fix_priority": "critical" if critical_count > 2 else "high",
            "how_to_fix": [
                f"Run a usability session specifically on '{top_name}' with 5 representative users",
                "Add inline helper text and real-time validation to this step",
                "Instrument the exact error states users hit and fix the top 3",
                "Consider a 'Need help?' tooltip triggered after 2 failed attempts",
            ],
        })

    funnel = results.get("funnel_analysis", {})
    if funnel and funnel.get("top_finding"):
        d = funnel.get("data", {})
        drop_step = d.get("biggest_drop_step", "Unknown")
        drop_pct = d.get("biggest_drop_pct", 0)
        overall = d.get("overall_conversion", 100)
        insights.append({
            "title": f"Funnel Conversion — Critical Drop-off at '{drop_step}'",
            "ai_summary": funnel.get("top_finding", ""),
            "root_cause_hypothesis": (
                f"Overall conversion of {overall:.1f}% with a {drop_pct:.1f}% drop at '{drop_step}' indicates "
                "this single step is the primary conversion bottleneck. "
                "Either the step requires too much from the user, or the value proposition at that point is unclear."
            ),
            "possible_causes": [
                f"'{drop_step}' has too many required fields or a high cognitive load",
                "The value of completing this step is not obvious to the user",
                "A technical bug or slow load time at this step discourages completion",
                "Users lack the information needed to proceed (e.g. need a document ready)",
            ],
            "ux_implications": (
                f"Fixing '{drop_step}' has the highest ROI of any UX change — "
                f"even a 10% improvement here would increase overall conversion by ~{round(overall * 0.1, 1)}%."
            ),
            "fix_priority": "critical" if drop_pct > 50 else "high",
            "how_to_fix": [
                f"Simplify '{drop_step}' — reduce it to the minimum required fields",
                "Add a progress indicator so users know how close they are to finishing",
                f"A/B test removing optional fields from '{drop_step}' entirely",
                "Add social proof or reassurance copy near the CTA at this step",
            ],
        })

    survival = results.get("survival_analysis", {})
    if survival and survival.get("top_finding"):
        d = survival.get("data", {})
        median_step = d.get("median_survival_step", 0)
        pct_10 = d.get("pct_reach_step_10", 100)
        insights.append({
            "title": f"Session Survival — 50% of Sessions End by Step {median_step}",
            "ai_summary": survival.get("top_finding", ""),
            "root_cause_hypothesis": (
                f"Only {pct_10:.1f}% of sessions reach step 10. The steep survival curve "
                "suggests users are hitting a wall early rather than gradually losing interest."
            ),
            "possible_causes": [
                "Core product value is delivered too late in the session",
                "An early mandatory step (e.g. account creation, permissions prompt) causes abandonment",
                "Sessions are cut short by technical errors or slow performance early on",
            ],
            "ux_implications": "Move the 'aha moment' earlier. Every step before a user gets value is a step where they might leave.",
            "fix_priority": "high" if pct_10 < 40 else "medium",
            "how_to_fix": [
                f"Identify the most common event at step {median_step} and investigate its failure modes",
                "Implement a 'quick win' flow that delivers value within the first 5 events",
                "Remove any mandatory registration or permissions barriers before the core action",
            ],
        })

    trend = results.get("trend_analysis", {})
    if trend and trend.get("top_finding"):
        d = trend.get("data", {})
        direction = d.get("trend_direction", "")
        pct_change = d.get("pct_change", 0)
        severity = trend.get("severity", "info")
        if direction:
            insights.append({
                "title": f"Metric Trend — {direction.title()} Trajectory ({pct_change:+.1f}%)",
                "ai_summary": trend.get("top_finding", ""),
                "root_cause_hypothesis": (
                    f"A {abs(pct_change):.1f}% {direction} trend suggests a structural change "
                    "in user behaviour rather than a one-off anomaly. "
                    "This should be correlated with product releases or marketing changes during the same period."
                ),
                "possible_causes": [
                    "A recent product change altered user behaviour",
                    "Seasonal or external market effects",
                    "A change in traffic source quality",
                    "A feature was deprecated or significantly redesigned",
                ],
                "ux_implications": f"{'Declining' if direction == 'downward' else 'Growing'} trends compound over time. Act quickly to {'reverse' if direction == 'downward' else 'capitalise on'} this trajectory.",
                "fix_priority": "high" if severity in ("critical", "high") else "medium",
                "how_to_fix": [
                    "Overlay the trend line against a deployment/changelog timeline",
                    "Segment the trend by user cohort to isolate which group is driving the change",
                    "Set up an automated alert if the metric continues in this direction for 7+ days",
                ],
            })

    anomaly = results.get("anomaly_detection", {})
    if anomaly and anomaly.get("top_finding"):
        d = anomaly.get("data", {})
        outlier_pct = d.get("outlier_pct", 0)
        outlier_count = d.get("outlier_count", 0)
        if outlier_pct > 5:
            insights.append({
                "title": f"Data Anomalies — {outlier_pct:.1f}% of Records Are Outliers",
                "ai_summary": anomaly.get("top_finding", ""),
                "root_cause_hypothesis": (
                    f"{outlier_count} outlier records detected. These may represent power users, "
                    "bots, data quality issues, or genuine edge cases that require special handling."
                ),
                "possible_causes": [
                    "Bot or automated traffic inflating certain metrics",
                    "Data pipeline issues causing duplicate or corrupted records",
                    "A small group of extreme power users skewing aggregate metrics",
                    "Testing or staging data included in the production dataset",
                ],
                "ux_implications": "If outliers are bots or test accounts, your aggregate metrics overstate or understate real user behaviour.",
                "fix_priority": "high" if outlier_pct > 15 else "medium",
                "how_to_fix": [
                    "Inspect the top 10 outlier records manually to classify their type",
                    "Add a bot-detection filter to your event tracking pipeline",
                    "Exclude test/internal accounts from analytics using an explicit flag",
                    "Report median alongside mean to reduce outlier distortion in dashboards",
                ],
            })

    for atype, label in [("cohort_analysis", "Cohort Retention"), ("rfm_analysis", "RFM Segmentation")]:
        r = results.get(atype, {})
        if r and r.get("top_finding"):
            insights.append({
                "title": f"{label} — Key Finding",
                "ai_summary": r.get("top_finding", ""),
                "root_cause_hypothesis": "Retention and value differences across cohorts typically reflect onboarding quality, product maturity at the time of signup, or changes in marketing targeting.",
                "possible_causes": [
                    "Earlier cohorts had a better or different onboarding experience",
                    "Product features available at signup time differ across cohorts",
                    "Channel mix (organic vs paid) varies across cohorts, affecting quality",
                ],
                "ux_implications": "Cohort gaps compound over time. A 5% retention difference in month 1 becomes a 30%+ LTV gap by month 6.",
                "fix_priority": "medium",
                "how_to_fix": [
                    "Identify the best-performing cohort and replicate its onboarding experience",
                    "Run a cohort-specific re-engagement campaign for the lowest-retention groups",
                    "Instrument feature adoption rates per cohort to find what drives long-term retention",
                ],
            })

    if not insights:
        for atype, result in results.items():
            finding = result.get("top_finding", "")
            if finding and len(finding) > 20:
                insights.append({
                    "title": f"{atype.replace('_', ' ').title()} — Key Finding",
                    "ai_summary": finding,
                    "root_cause_hypothesis": "Further investigation needed to establish a definitive root cause.",
                    "possible_causes": ["See the corresponding chart for detailed breakdown"],
                    "ux_implications": "Review the chart and apply the finding to your product roadmap.",
                    "fix_priority": result.get("severity", "medium"),
                    "how_to_fix": ["Review the chart", "Cross-reference with other analyses"],
                })

    return {"insights": insights}


def deterministic_full_synthesis(results: dict, dataset_type: str) -> dict:
    """Build complete synthesis from results without LLM."""
    results_by_type = {}
    for aid, result in results.items():
        atype = result.get("analysis_type", "unknown")
        results_by_type[atype] = result

    return {
        "executive_summary":       deterministic_executive_summary(results_by_type, dataset_type),
        "detailed_insights":       deterministic_detailed_insights(results_by_type),
        "personas":                deterministic_personas(
            results_by_type.get("user_segmentation"),
            results_by_type.get("friction_detection"),
            results_by_type.get("funnel_analysis"),
        ),
        "intervention_strategies": deterministic_strategies(results_by_type, dataset_type),
        "cross_metric_connections": deterministic_connections(results_by_type),
    }
