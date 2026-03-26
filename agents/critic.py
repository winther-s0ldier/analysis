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
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── In-memory store ───────────────────────────────────────────────────────────
_critic_store: dict = {}
_critic_instance = None


def get_critic_store_result(session_id: str) -> dict:
    """Retrieve critic result for a session."""
    return _critic_store.get(session_id, {})


# ── Tools ─────────────────────────────────────────────────────────────────────

def tool_get_synthesis_for_critique(session_id: str, tool_context=None) -> dict:
    """
    Retrieve the current synthesis + pre-extracted fact_sheet for this session.
    The synthesis contains all the insight cards, cross-metric connections,
    executive summary, and conversational report.
    The fact_sheet contains the verified numbers each node actually produced.
    Use both to check: does every claim in synthesis trace back to a real number?
    """
    try:
        # Source 0: ToolContext.state — fastest path in SequentialAgent in-process mode.
        # synthesis_agent writes tool_context.state["synthesis"] before this tool is called.
        synthesis = {}
        if tool_context is not None:
            try:
                _tc_synth = tool_context.state.get("synthesis")
                if _tc_synth:
                    synthesis = _tc_synth
                    print(f"INFO: [ToolContext] critic got synthesis from state for {session_id}")
            except Exception:
                pass

        # Source 1: in-memory store (single-server in-process mode)
        if not synthesis:
            from agents.synthesis import _synthesis_store
            synthesis = _synthesis_store.get(session_id, {})

        # Source 2 (A2A server mode fallback): read _synthesis_cache.json when in-memory store is empty
        if not synthesis:
            try:
                from agent_servers.a2a_orchestrator import lookup_session as _lookup
                import json as _json
                _abs_out = _lookup(session_id)
                if _abs_out:
                    import os as _os
                    _cache = _os.path.join(_abs_out, "_synthesis_cache.json")
                    if _os.path.exists(_cache):
                        with open(_cache, "r", encoding="utf-8") as _f:
                            synthesis = _json.load(_f)
                        print(f"INFO: [A2A] critic loaded synthesis from file cache for {session_id}")
            except Exception as _fe:
                print(f"WARNING: [A2A] critic synthesis file fallback failed: {_fe}")

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
        elif not fact_sheet:
            # A2A server mode: build fact_sheet from _results_cache.json
            try:
                from agent_servers.a2a_orchestrator import lookup_session as _lookup_r
                import json as _rjson
                import os as _ros
                _abs_out_r = _lookup_r(session_id)
                if _abs_out_r:
                    _rcache = _ros.path.join(_abs_out_r, "_results_cache.json")
                    if _ros.path.exists(_rcache):
                        with open(_rcache, "r", encoding="utf-8") as _rf:
                            _cached_results = _rjson.load(_rf)
                        for aid, res in _cached_results.items():
                            try:
                                fact_sheet[aid] = _extract_node_facts(aid, aid, res)
                            except Exception:
                                fact_sheet[aid] = {
                                    "analysis_type": res.get("analysis_type", "unknown"),
                                    "top_finding": res.get("top_finding", ""),
                                }
                        print(f"INFO: [A2A] critic loaded {len(fact_sheet)} facts from results cache for {session_id}")
            except Exception as _rfe:
                print(f"WARNING: [A2A] critic results cache fallback failed: {_rfe}")

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


class ChallengeItem(BaseModel):
    claim: str = Field(description="Exact text from synthesis that is problematic")
    issue: str = Field(description="What is wrong: unsupported, contradicts X, vague, etc.")
    severity: str = Field(default="medium", description="high | medium | low")


class CritiqueInput(BaseModel):
    session_id: str = Field(description="Active pipeline session ID")
    approved: bool = Field(
        description="True if synthesis is reliable enough for decisions; False if 2+ high-severity errors exist"
    )
    challenges: List[ChallengeItem] = Field(
        default=[],
        description="List of factual or logical issues found in the synthesis",
    )
    confidence_adjustment: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="1.0=fully reliable, 0.7=some concerns, 0.4=significant issues",
    )
    overall_verdict: str = Field(
        default="",
        description="2–4 sentence summary of the review",
    )

    @field_validator("confidence_adjustment", mode="before")
    @classmethod
    def clamp_confidence(cls, v):
        try:
            return max(0.0, min(1.0, float(v)))
        except (TypeError, ValueError):
            return 1.0


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
    # Validate and coerce inputs via Pydantic model
    try:
        _validated = CritiqueInput(
            session_id=session_id,
            approved=approved,
            challenges=challenges if isinstance(challenges, list) else [],
            confidence_adjustment=confidence_adjustment,
            overall_verdict=overall_verdict or "",
        )
        approved = _validated.approved
        challenges = [c.model_dump() for c in _validated.challenges]
        confidence_adjustment = _validated.confidence_adjustment
        overall_verdict = _validated.overall_verdict
    except Exception:
        # Fallback: manual coercion (keeps existing behaviour on unexpected input)
        confidence_adjustment = max(0.0, min(1.0, float(confidence_adjustment)))
        challenges = challenges if isinstance(challenges, list) else []
        overall_verdict = str(overall_verdict) if overall_verdict else ""

    _critic_store[session_id] = {
        "approved": bool(approved),
        "challenges": challenges,
        "confidence_adjustment": confidence_adjustment,
        "overall_verdict": overall_verdict,
    }

    # ADK-native: write to ToolContext state so dag_builder (next in SequentialAgent)
    # reads critique directly — no more _synthesis_store injection race condition
    if tool_context is not None:
        try:
            tool_context.state["critique"] = _critic_store[session_id]
        except Exception:
            pass

    n = len(_critic_store[session_id]["challenges"])
    print(
        f"[Critic] Session {session_id}: approved={approved}, "
        f"challenges={n}, conf_adj={confidence_adjustment:.2f}"
    )

    # A2A server mode: write _critic_cache.json so dag_builder server can read it
    try:
        from agent_servers.a2a_orchestrator import lookup_session as _lookup
        import json as _json
        import os as _os
        _abs_out = _lookup(session_id)
        if _abs_out:
            _cache = _os.path.join(_abs_out, "_critic_cache.json")
            with open(_cache, "w", encoding="utf-8") as _cf:
                _json.dump(_critic_store[session_id], _cf)
            print(f"INFO: [A2A] critic cache written to {_cache}")
    except Exception as _ce:
        pass  # Non-fatal — in-process mode doesn't need this

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

_PROMPT_DIR = os.path.join(os.path.dirname(__file__), '..', 'prompts')


def _load_prompt(name: str) -> str:
    with open(os.path.join(_PROMPT_DIR, name), 'r', encoding='utf-8') as f:
        return f.read()


def get_critic_agent():
    global _critic_instance
    if _critic_instance is not None:
        return _critic_instance

    from google.adk.agents import Agent
    from google.adk.tools import FunctionTool
    from tools.model_config import get_model

    _critic_instance = Agent(
        name="critic_agent",
        model=get_model("synthesis"),
        description="Adversarial critic that reviews synthesis for unsupported claims, "
                    "contradictions, overconfidence, and vague recommendations.",
        instruction=_load_prompt("critic.md"),
        tools=[FunctionTool(tool_get_synthesis_for_critique), FunctionTool(tool_submit_critique)],
    )
    return _critic_instance
