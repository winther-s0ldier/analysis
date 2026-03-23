import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from google.adk.agents import Agent
from tools.csv_profiler import (
    profile_csv,
    infer_column_semantics,
    classify_dataset,
)
from tools.model_config import get_model
from a2a_messages import create_message, Intent


_profile_store: dict = {}


def get_profile_result(session_id: str) -> dict | None:
    """Read and consume stored profile result for a session."""
    return _profile_store.pop(session_id, None)


def tool_profile_and_classify(
    csv_path: str,
    session_id: str,
) -> dict:
    """
    Step 1: Raw profile — column stats, types, correlations.
    Returns the raw profile to the LLM. The LLM then must reason
    about this profile to create the semantic map and classification.
    """
    raw_profile = profile_csv(csv_path)
    if "error" in raw_profile:
        return {"error": raw_profile["error"]}

    # Apply custom_column_roles from policy.json — override programmatic inference.
    # This is the user's explicit instruction: "treat this column as entity_col".
    try:
        from tools.data_policy import get_active_policy
        _policy = get_active_policy()
        _custom_roles = _policy.get("custom_column_roles", {})
        if _custom_roles:
            existing_roles = raw_profile.get("column_roles", {})
            # Invert: existing_roles = {role: col_name}; _custom_roles = {col_name: role}
            # Build reverse map and apply overrides
            _reversed = {v: k for k, v in existing_roles.items() if v}
            for col_name, role in _custom_roles.items():
                # Remove any existing binding for this role
                existing_roles[role] = col_name
            raw_profile["column_roles"] = existing_roles
            print(f"[PolicyEngine] Applied custom_column_roles overrides: {_custom_roles}")
    except Exception as _pe:
        print(f"[PolicyEngine] custom_column_roles apply failed: {_pe}")

    # Store immediately so get_profile_result() always returns valid data.
    # profile_csv already runs programmatic classification (infer_column_semantics +
    # classify_dataset), so we have row_count/column_count/dataset_type right now.
    # The LLM can still refine the classification — this is just a safe fallback.
    _profile_store[session_id] = {
        "status": "success",
        "raw_profile": raw_profile,
        "classification": {
            "dataset_type": raw_profile.get("dataset_type", "tabular_generic"),
            "column_roles": raw_profile.get("column_roles", {}),
            "confidence": raw_profile.get("confidence", 0.0),
            "recommended_analyses": raw_profile.get("recommended_analyses", []),
        },
    }

    return {
        "raw_profile": raw_profile
    }

_profiler_agent_instance = None

def get_profiler_agent():
    global _profiler_agent_instance
    if _profiler_agent_instance is None:
        from google.adk.agents import Agent
        _profiler_agent_instance = Agent(
            name="profiler_agent",
            model=get_model("profiler"),
            description=(
                "Data profiling specialist. I examine raw CSV statistics and "
                "reason about column semantics and dataset type."
            ),
            instruction=(
                "You are an Elite Data Profiling Specialist operating within a dynamic, domain-agnostic analytics pipeline. "
                "Your sole responsibility is to examine raw statistical facts about a dataset and produce a precise, semantically-grounded classification. "
                "You do NOT invent analyses, do NOT make assumptions about the business domain, and do NOT suggest solutions. "
                "You are a sensor — you report ONLY what the data tells you.\n\n"

                "## WORKFLOW\n"
                "1. Call `tool_profile_and_classify(csv_path, session_id)` to receive raw statistical facts (column types, value samples, ranges, distributions).\n"
                "2. Reason about the STRUCTURE of the data, not its domain. A column named '顧客ID' and a column named 'customer_id' may both be entity identifiers — reason from data type and uniqueness, not from keywords.\n"
                "3. Assign column roles based on the EVIDENCE in the raw profile.\n"
                "4. Output the EXACT JSON schema defined below. No extra text.\n\n"

                "## COLUMN ROLE DEFINITIONS (Infer from data shape, NOT column names)\n"
                "- `entity_col`: High-cardinality column of identifiers (UUIDs, IDs, hashes, codes). Evidence: high unique count, string or int type, consistent format.\n"
                "- `time_col`: Temporal column. Evidence: datetime dtype, epoch integers, or string parseable as ISO8601.\n"
                "- `event_col`: Categorical column describing WHAT happened. Evidence: low-to-medium cardinality, string type, repeating values across rows for the same entity.\n"
                "- `outcome_col`: Numeric column representing a measurable result (any quantity: revenue, score, count, duration, measurement). Evidence: numeric dtype, non-trivial variance.\n"
                "- `funnel_col`: Ordered categorical column representing PROGRESSION STAGES. Evidence: small cardinality (2–15 distinct values), values suggest sequential steps or levels.\n\n"
                "IMPORTANT: column roles describe DATA STRUCTURE only. `entity_col` might be a patient ID, machine ID, customer ID, or account ID — do not assume any domain. "
                "`event_col` might be a medical procedure, a log event type, a sensor reading label, or a web action — name does not matter, data pattern does.\n\n"

                "## DATASET TYPE RULES\n"
                "Assign `dataset_type` based on the PRESENCE of required columns. "
                "These types are based on DATA STRUCTURE — not domain. A medical trial log and a web event log can both be `event_log`.\n"
                "- `event_log`: Has `entity_col` + `time_col` + `event_col`. Entities perform discrete, named actions over time.\n"
                "- `transactional`: Has `entity_col` + `time_col` + `outcome_col`. Entities generate measurable outcomes over time, but no named event column.\n"
                "- `time_series`: Has `time_col` + `outcome_col`. Aggregate measurements over time; no per-entity column.\n"
                "- `funnel`: Has `entity_col` + `funnel_col`. Entities are at named sequential stages; no raw timestamp.\n"
                "- `survey_or_cross_sectional`: Rows represent a single observation per entity at one point in time. Typically no time or event column. Common for survey data, census snapshots, or scored records.\n"
                "- `tabular_generic`: Does not fit any of the above, or the data structure is mixed/ambiguous. Treat as a static cross-sectional table.\n\n"

                "## CONFIDENCE SCORING\n"
                "Set `confidence` between 0.0 and 1.0 based on how clearly the data fits the assigned type:\n"
                "- 0.9+: All required columns are unambiguously present with strong data distributions.\n"
                "- 0.7-0.89: Columns are likely present but column name or dtype is ambiguous.\n"
                "- 0.5-0.69: Best guess — data partially fits the assigned type.\n"
                "- Below 0.5: Assign `tabular_generic`.\n\n"

                "## DO's\n"
                "- DO infer column roles from data distributions and uniqueness ratios, not from column name patterns.\n"
                "- DO set `outcome_col` or `funnel_col` to `null` if no clear evidence exists.\n"
                "- DO embed the FULL `raw_profile` dict from the tool call into your output — do not truncate or summarise it.\n"
                "- DO produce `recommended_analyses` that are appropriate for the assigned dataset_type and the column roles actually found.\n"
                "- DO use `survey_or_cross_sectional` when rows are single per-entity observations with no time/event structure.\n"
                "- DO use `tabular_generic` when the data is genuinely mixed or ambiguous — do not force a structured type onto messy data.\n\n"

                "## DON'Ts\n"
                "- DON'T hardcode English keywords (e.g., never rely on 'user_id' or 'timestamp' matching as exact strings).\n"
                "- DON'T assign a column role based purely on column name without cross-checking data statistics.\n"
                "- DON'T suggest more than 6 recommended analyses — keep to the most relevant.\n"
                "- DON'T fabricate statistics or sample values that were not returned by the tool.\n"
                "- DON'T make domain assumptions — the dataset could be from ANY industry, language, or field.\n"
                "- DON'T output any text outside the JSON block.\n\n"

                "## OUTPUT SCHEMA (Strict JSON — no deviations)\n"
                "```json\n"
                "{\n"
                "  \"status\": \"success\",\n"
                "  \"raw_profile\": { <inject EXACTLY what the tool returned, unmodified> },\n"
                "  \"classification\": {\n"
                "    \"dataset_type\": \"event_log\",\n"
                "    \"confidence\": 0.92,\n"
                "    \"reasoning\": \"One sentence explaining WHY this dataset_type was chosen, referencing specific column names and their data characteristics.\",\n"
                "    \"column_roles\": {\n"
                "      \"entity_col\": \"<actual_column_name or null>\",\n"
                "      \"time_col\": \"<actual_column_name or null>\",\n"
                "      \"event_col\": \"<actual_column_name or null>\",\n"
                "      \"outcome_col\": \"<actual_column_name or null>\",\n"
                "      \"funnel_col\": \"<actual_column_name or null>\"\n"
                "    },\n"
                "    \"recommended_analyses\": [\"session_detection\", \"funnel_analysis\", \"dropout_analysis\"],\n"
                "    \"reasoning\": \"One sentence: WHY this dataset_type was assigned, referencing specific column names and their observed data characteristics.\"\n"
                "  }\n"
                "}\n"
                "```\n"
            ),
            tools=[tool_profile_and_classify],
        )
    return _profiler_agent_instance
