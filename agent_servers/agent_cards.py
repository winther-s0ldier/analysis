"""
Explicit AgentCard definitions for each A2A agent.

Previously every agent relied on `to_a2a()` auto-deriving its card from
the agent name + description, which produced a skeletal skill block with
no tags, examples, or I/O modes. This module declares the real skills,
so A2A clients can meaningfully discover what each agent does.

`build_agent_card(name, port)` is called from `server_base.py` before
`to_a2a(agent, agent_card=card)`.
"""
from __future__ import annotations

from a2a.types import (
    AgentCard,
    AgentCapabilities,
    AgentSkill,
)

_PROTOCOL_VERSION = "0.3.0"
_AGENT_VERSION = "1.0.0"

_TEXT = ["text/plain"]
_JSON = ["application/json"]
_TEXT_JSON = ["text/plain", "application/json"]

_CAPS = AgentCapabilities(
    streaming=False,                  # flip on when point #2 (streaming) lands
    push_notifications=False,
    state_transition_history=False,
)

_SKILLS: dict[str, list[AgentSkill]] = {
    "profiler": [
        AgentSkill(
            id="profile_csv",
            name="Profile CSV dataset",
            description=(
                "Run statistical profiling on a CSV file, infer per-column semantics "
                "(dimension / measure / date / id), classify dataset type, and return "
                "a structured profile plus a recommended-analyses list."
            ),
            tags=["profiling", "classification", "csv", "schema-inference"],
            examples=[
                "csv_path: /data/sales.csv\nsession_id: abc123\nCall tool_profile_and_classify now.",
            ],
            input_modes=_TEXT,
            output_modes=_TEXT_JSON,
        ),
    ],
    "discovery": [
        AgentSkill(
            id="build_analysis_dag",
            name="Build analysis DAG",
            description=(
                "Given a profiled dataset and policy context, reason about which "
                "analyses are most valuable and emit a JSON DAG of MetricSpec nodes "
                "for the orchestrator to execute."
            ),
            tags=["planning", "dag", "analysis-design"],
            examples=[
                "Session ID: abc123\nPROFILER OUTPUT: {...}\nConstruct a JSON DAG and call tool_submit_analysis_plan.",
            ],
            input_modes=_TEXT,
            output_modes=_JSON,
        ),
    ],
    "coder": [
        AgentSkill(
            id="generate_analysis_code",
            name="Generate pandas analysis code",
            description=(
                "Pure code generator. Takes an analysis spec and returns "
                "executable Python (pandas / numpy) that computes the requested "
                "metric. No tool invocation — text-only output."
            ),
            tags=["code-generation", "pandas", "python"],
            examples=[
                "spec: {metric: revenue_by_segment, groupby: [region], agg: sum}",
            ],
            input_modes=_TEXT,
            output_modes=_TEXT,
        ),
    ],
    "synthesis": [
        AgentSkill(
            id="synthesize_insights",
            name="Synthesize insights from analysis results",
            description=(
                "Read the full set of executed analysis node outputs, rank findings "
                "by severity and confidence, write an executive summary, and emit a "
                "structured synthesis JSON plus a long-form conversational report."
            ),
            tags=["synthesis", "insights", "executive-summary", "narrative"],
            examples=[
                "Session ID: abc123\nOutput folder: /output/abc123\nSynthesize all node results.",
            ],
            input_modes=_TEXT,
            output_modes=_TEXT_JSON,
        ),
    ],
    "critic": [
        AgentSkill(
            id="critique_synthesis",
            name="Critique synthesis output",
            description=(
                "Review the synthesis agent's output for factual errors, weak claims, "
                "unsupported hypotheses, and missing context. Returns a structured "
                "critique with severity-ranked issues and rewrite suggestions."
            ),
            tags=["review", "qa", "critique"],
            examples=[
                "Session ID: abc123\nReview the synthesis and call tool_submit_critique.",
            ],
            input_modes=_TEXT,
            output_modes=_JSON,
        ),
    ],
    "dag_builder": [
        AgentSkill(
            id="build_html_report",
            name="Build final HTML report",
            description=(
                "Assemble the final client-facing HTML report from synthesis output, "
                "critic feedback, and generated chart artifacts. Writes report.html "
                "to the session output folder."
            ),
            tags=["reporting", "html", "assembly"],
            examples=[
                "Session ID: abc123\nOutput folder: /output/abc123\nCall tool_build_report now.",
            ],
            input_modes=_TEXT,
            output_modes=_TEXT,
        ),
    ],
}

_DESCRIPTIONS: dict[str, str] = {
    "profiler": (
        "Data profiling specialist. Examines raw CSV statistics and reasons "
        "about column semantics and dataset type."
    ),
    "discovery": (
        "Analysis planning specialist. Reasons about which analyses are most "
        "valuable and builds a custom analysis DAG."
    ),
    "coder": (
        "Pure code writer. Takes an analysis spec and returns executable "
        "pandas/numpy Python. No tools."
    ),
    "synthesis": (
        "Synthesis specialist. Aggregates analysis node results into ranked "
        "insights, executive summary, and a conversational narrative report."
    ),
    "critic": (
        "Synthesis reviewer. Audits synthesis output for factual errors, weak "
        "claims, and missing context; emits a structured critique."
    ),
    "dag_builder": (
        "Report assembler. Builds the final HTML deliverable from synthesis, "
        "critique, and chart artifacts."
    ),
}


def build_agent_card(agent_name: str, host: str, port: int, protocol: str = "http") -> AgentCard:
    """Build a rich AgentCard for the named agent."""
    if agent_name not in _SKILLS:
        raise ValueError(f"No skill definition for agent '{agent_name}'")

    url = f"{protocol}://{host}:{port}/"

    return AgentCard(
        name=f"{agent_name}_agent",
        description=_DESCRIPTIONS[agent_name],
        version=_AGENT_VERSION,
        protocol_version=_PROTOCOL_VERSION,
        url=url,
        preferred_transport="JSONRPC",
        default_input_modes=_TEXT,
        default_output_modes=_TEXT_JSON,
        capabilities=_CAPS,
        skills=_SKILLS[agent_name],
    )
