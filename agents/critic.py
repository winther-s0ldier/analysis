"""
critic.py — Adversarial Critic Agent (#8)
==========================================
Reviews the synthesis output for:
  1. Unsupported claims (no [AX] citation)
  2. Contradictions between node findings
  3. Overconfident assertions without statistical backing
  4. Vague action items without specific steps
  5. Important patterns in fact_sheet not addressed by synthesis

Output stored in _critic_store[session_id] and injected into
_synthesis_store[session_id]["_critic_review"] by orchestrator.
"""
import os
import sys
import re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── In-memory store ───────────────────────────────────────────────────────────
_critic_store: dict = {}
_critic_instance = None


def get_critic_store_result(session_id: str) -> dict:
    """Retrieve critic result for a session."""
    return _critic_store.get(session_id, {})


# ── Tools ─────────────────────────────────────────────────────────────────────

def tool_get_synthesis_for_critique(session_id: str) -> dict:
    """
    Retrieve the current synthesis + pre-extracted fact_sheet for this session.
    The synthesis contains all the insight cards, cross-metric connections,
    executive summary, and conversational report.
    The fact_sheet contains the verified numbers each node actually produced.
    Use both to check: does every claim in synthesis trace back to a real number?
    """
    try:
        from agents.synthesis import _synthesis_store
        synthesis = _synthesis_store.get(session_id, {})

        from agents.orchestrator import get_pipeline_state
        from agents.synthesis import _extract_node_facts
        state = get_pipeline_state(session_id)

        fact_sheet = {}
        if state:
            for aid, res in state.results.items():
                if isinstance(res, dict) and res.get("status") == "success":
                    try:
                        fact_sheet[aid] = _extract_node_facts(aid, aid, res)
                    except Exception:
                        fact_sheet[aid] = {
                            "analysis_type": res.get("analysis_type", "unknown"),
                            "top_finding": res.get("top_finding", ""),
                        }

        # Pull out key text blobs for easy review
        insights = []
        di = synthesis.get("detailed_insights", {})
        if isinstance(di, list):
            insights = di
        elif isinstance(di, dict):
            insights = di.get("insights", [])

        connections = []
        cx = synthesis.get("cross_metric_connections", {})
        if isinstance(cx, list):
            connections = cx
        elif isinstance(cx, dict):
            connections = cx.get("connections", [])

        return {
            "session_id": session_id,
            "synthesis_available": bool(synthesis),
            "fact_sheet": fact_sheet,
            "insight_count": len(insights),
            "insights": insights,
            "connections": connections,
            "executive_summary": synthesis.get("executive_summary", {}),
            "conversational_report_preview": str(synthesis.get("conversational_report", ""))[:800],
        }
    except Exception as e:
        return {"session_id": session_id, "error": str(e), "synthesis_available": False}


def tool_submit_critique(
    session_id: str,
    approved: bool,
    challenges: list,
    confidence_adjustment: float,
    overall_verdict: str,
    tool_context=None,
) -> dict:
    """
    Submit the adversarial critique of the synthesis.

    Args:
        session_id: Pipeline session.
        approved: True if synthesis is reliable enough for decisions despite minor issues.
                  False if there are 2+ high-severity factual/logical errors.
        challenges: List of dicts, each with keys:
                    - "claim": the exact text from synthesis that is problematic
                    - "issue": what is wrong (unsupported, contradicts X, vague, etc.)
                    - "severity": "high" | "medium" | "low"
        confidence_adjustment: Float 0.0–1.0. 1.0 = synthesis is fully reliable.
                                0.7 = some concerns. 0.4 = significant issues.
        overall_verdict: One paragraph summarising the review (2–4 sentences).
    """
    # Clamp confidence_adjustment
    confidence_adjustment = max(0.0, min(1.0, float(confidence_adjustment)))

    _critic_store[session_id] = {
        "approved": bool(approved),
        "challenges": challenges if isinstance(challenges, list) else [],
        "confidence_adjustment": confidence_adjustment,
        "overall_verdict": overall_verdict,
    }

    n = len(_critic_store[session_id]["challenges"])
    print(
        f"[Critic] Session {session_id}: approved={approved}, "
        f"challenges={n}, conf_adj={confidence_adjustment:.2f}"
    )

    # Escalate the LoopAgent when approved — signals "done" to LoopAgent wrapper
    if tool_context is not None and approved:
        try:
            tool_context.actions.escalate = True
        except AttributeError:
            pass  # Not inside a LoopAgent — ignore

    return {
        "status": "critique_stored",
        "approved": approved,
        "challenge_count": n,
        "confidence_adjustment": confidence_adjustment,
    }


# ── Agent factory ─────────────────────────────────────────────────────────────

def get_critic_agent():
    global _critic_instance
    if _critic_instance is not None:
        return _critic_instance

    from google.adk.agents import Agent
    from tools.model_config import get_model

    _critic_instance = Agent(
        name="critic_agent",
        model=get_model("synthesis"),
        description="Adversarial critic that reviews synthesis for unsupported claims, "
                    "contradictions, overconfidence, and vague recommendations.",
        instruction="""You are the Adversarial Critic — a rigorous scientific peer-reviewer for analytics pipelines.

## YOUR MANDATE
You receive a completed synthesis and the raw fact_sheet that powered it.
Your job is to find any of these five problem types:

1. **Unsupported claims** — A statement in synthesis that has NO [AX] citation (e.g., "users drop off due to poor onboarding" with zero citation).
2. **Contradictions** — Two insight cards or a card vs. a connection that make mutually inconsistent claims about the same metric without explaining why.
3. **Overconfidence** — "Users definitely do X" or "the primary cause is Y" stated without appropriate statistical caveats.
4. **Missed critical findings** — A top_finding in fact_sheet that is clearly important (high severity or large magnitude) but completely absent from insights.
5. **Vague actions** — A "how_to_fix" item that says things like "improve the experience" or "optimise the flow" without specifying WHAT to change.

## WORKFLOW
1. Call `tool_get_synthesis_for_critique(session_id)`.
2. Read every insight card (ai_summary, root_cause_hypothesis, how_to_fix) and every cross-metric connection.
3. For each problem found:
   - Record the EXACT problematic text as "claim"
   - Explain specifically WHY it is a problem as "issue"
   - Rate severity: "high" = factual/logical error; "medium" = missing nuance; "low" = minor wording
4. Count high-severity challenges:
   - If 0–1 high-severity: set approved=True
   - If 2+ high-severity: set approved=False
5. Set confidence_adjustment: start at 1.0, subtract 0.1 per high challenge, 0.05 per medium.
6. Write overall_verdict (2–4 sentences): what is the synthesis' overall reliability?
7. Call `tool_submit_critique(session_id, approved, challenges, confidence_adjustment, overall_verdict)`.

## STRICT RULES
- Quote the EXACT text from synthesis in every "claim" — no paraphrasing.
- Do NOT reject synthesis for stylistic issues, passive voice, or minor wording choices.
- Do NOT flag missing analyses that the DAG simply didn't include — only evaluate what IS in synthesis.
- Do NOT hallucinate issues that aren't there. If synthesis is well-grounded, say so.
- approved=True means: "This synthesis is reliable enough for a business decision-maker to act on."
- Be specific. Be honest. If the synthesis is good, approve it immediately.
""",
        tools=[tool_get_synthesis_for_critique, tool_submit_critique],
    )
    return _critic_instance
