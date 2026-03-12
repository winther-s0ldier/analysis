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

    return {
        "raw_profile": raw_profile
    }


from tools.model_config import get_model

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
                "- `entity_col`: High-cardinality column of identifiers (UUIDs, IDs, hashes). Evidence: high unique count, string or int type.\n"
                "- `time_col`: Temporal column. Evidence: datetime dtype, or string parseable as ISO8601 / epoch.\n"
                "- `event_col`: Categorical column describing what happened. Evidence: low-to-medium cardinality, string type, repeating values per entity.\n"
                "- `outcome_col`: Numeric column representing a measurable result (revenue, score, duration). Evidence: numeric dtype, non-trivial variance.\n"
                "- `funnel_col`: Ordered categorical column representing progression stages. Evidence: small cardinality, values suggest an ordered journey.\n\n"

                "## DATASET TYPE RULES\n"
                "Assign `dataset_type` based on the PRESENCE of required columns:\n"
                "- `event_log`: Has `entity_col` + `time_col` + `event_col`. The most structured behavioral dataset.\n"
                "- `transactional`: Has `entity_col` + `time_col` + `outcome_col`. No event action column.\n"
                "- `time_series`: Has `time_col` + `outcome_col`. No entity column (aggregate over time).\n"
                "- `funnel`: Has `entity_col` + `funnel_col`. No raw event column.\n"
                "- `tabular_generic`: Does not fit any of the above. Treat as a static cross-sectional table.\n\n"

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
                "- DO produce `recommended_analyses` that are appropriate for the assigned dataset_type only.\n\n"

                "## DON'Ts\n"
                "- DON'T hardcode English keywords (e.g., never rely on 'user_id', 'timestamp' matching as strings).\n"
                "- DON'T assign a column role based purely on column name without cross-checking data statistics.\n"
                "- DON'T suggest more than 6 recommended analyses — keep to the most relevant.\n"
                "- DON'T fabricate statistics or sample values that were not returned by the tool.\n"
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
                "    \"recommended_analyses\": [\"session_detection\", \"funnel_analysis\", \"dropout_analysis\"]\n"
                "  }\n"
                "}\n"
                "```\n"
            ),
            tools=[tool_profile_and_classify],
        )
    return _profiler_agent_instance
