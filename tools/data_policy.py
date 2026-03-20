import os
import json
from typing import Optional


# Default policy (used when no policy.json exists)
DEFAULT_POLICY = {
    "focus": "none",
    "max_nodes": 10,
    "required_analyses": [],
    "excluded_analyses": [],
    "outcome_col_override": None,
}


def _policy_path() -> str:
    """Resolve the policy.json path relative to the ADK root."""
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)
    return os.path.join(root, "data", "policy.json")


def get_active_policy() -> dict:
    """
    Load and return the active policy configuration.
    Falls back to DEFAULT_POLICY if no policy.json exists or JSON is invalid.
    """
    path = _policy_path()
    if not os.path.exists(path):
        return dict(DEFAULT_POLICY)

    try:
        with open(path, "r", encoding="utf-8") as f:
            user_policy = json.load(f)

        # Merge with defaults so missing keys are always safe
        policy = dict(DEFAULT_POLICY)
        policy.update(user_policy)

        # Clamp max_nodes
        policy["max_nodes"] = max(3, min(int(policy.get("max_nodes", 10)), 15))
        return policy
    except Exception as e:
        print(f"[PolicyEngine] Failed to read policy.json: {e}. Using defaults.")
        return dict(DEFAULT_POLICY)


def validate_policy(policy: dict, column_roles: dict) -> list:
    """
    Validate a policy against the actual column_roles of a dataset.
    Returns a list of warnings (empty = valid).

    Checks:
    - required_analyses that need columns which are absent
    - outcome_col_override that doesn't exist in column_roles values
    """
    from tools.analysis_library import LIBRARY_REGISTRY
    warnings = []

    for atype in policy.get("required_analyses", []):
        entry = LIBRARY_REGISTRY.get(atype, {})
        required_args = entry.get("required_args", [])
        for arg in required_args:
            if arg == "csv_path":
                continue
            if arg not in column_roles or not column_roles[arg]:
                warnings.append(
                    f"Policy requires '{atype}' but `{arg}` is not available in column_roles. "
                    "This analysis will be skipped."
                )

    override = policy.get("outcome_col_override")
    if override:
        all_col_values = list(column_roles.values())
        if override not in all_col_values:
            warnings.append(
                f"Policy outcome_col_override='{override}' is not a known column. "
                "Check your dataset or policy.json."
            )

    return warnings


def apply_policy_to_dag(dag: list, policy: dict, column_roles: dict) -> list:
    """
    Apply the active policy to a planned DAG.
    Returns a (possibly modified) dag list.

    Actions:
    1. Remove analyses listed in excluded_analyses.
    2. Cap DAG at max_nodes (preserving critical and high priority).
    3. Verify required_analyses are feasible; warn if not.
    """
    from tools.analysis_library import LIBRARY_REGISTRY

    excluded = set(policy.get("excluded_analyses", []))
    required = set(policy.get("required_analyses", []))
    max_nodes = policy.get("max_nodes", 10)
    focus = policy.get("focus", "none")

    # Step 1: Remove explicitly excluded analyses
    dag = [node for node in dag if node.get("analysis_type") not in excluded]

    # Step 2: Apply focus bias — bump priority of focus-relevant analyses
    FOCUS_MAP = {
        "revenue":    {"rfm_analysis", "cohort_analysis", "pareto_analysis", "trend_analysis"},
        "retention":  {"cohort_analysis", "survival_analysis", "dropout_analysis", "funnel_analysis"},
        "quality":    {"missing_data_analysis", "anomaly_detection", "distribution_analysis"},
        "engagement": {"user_segmentation", "funnel_analysis", "friction_detection", "sequential_pattern_mining"},
    }
    if focus in FOCUS_MAP:
        for node in dag:
            if node.get("analysis_type") in FOCUS_MAP[focus]:
                if node.get("priority") not in ("critical",):
                    node["priority"] = "high"

    # Step 3: Sort by priority before capping (critical > high > medium > low)
    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    dag.sort(key=lambda n: priority_order.get(n.get("priority", "medium"), 2))

    # Step 4: Cap at max_nodes — but never remove required analyses or critical nodes
    if len(dag) > max_nodes:
        critical_and_required = [
            n for n in dag
            if n.get("priority") == "critical" or n.get("analysis_type") in required
        ]
        rest = [n for n in dag if n not in critical_and_required]
        dag = critical_and_required + rest[:max(0, max_nodes - len(critical_and_required))]

    # Step 5: Fix node IDs sequentially after filtering
    for i, node in enumerate(dag):
        node["id"] = f"A{i + 1}"

    # Step 6: Fix dependency references to new IDs
    atype_to_new_id = {node.get("analysis_type"): node["id"] for node in dag}
    for node in dag:
        new_deps = []
        for dep_id in node.get("depends_on", []):
            # Find what analysis_type had this dep_id, map to new_id
            for other in dag:
                if other["id"] == dep_id or atype_to_new_id.get(
                    other.get("analysis_type")
                ) == dep_id:
                    new_deps.append(other["id"])
                    break
        node["depends_on"] = list(set(new_deps))

    print(f"[PolicyEngine] Applied policy: focus={focus}, max_nodes={max_nodes}, "
          f"excluded={excluded}, dag_size={len(dag)}")

    return dag


def build_policy_context_for_discovery(policy: dict, column_roles: dict) -> str:
    """
    Build a human-readable policy context string to inject into the
    Discovery agent's prompt so it can self-bias its DAG selection.
    """
    lines = ["## ACTIVE PIPELINE POLICY"]
    focus = policy.get("focus", "none")
    if focus != "none":
        lines.append(f"- FOCUS: Prioritise analyses related to `{focus}`. "
                     f"Include relevant analyses even if data signals are marginal.")

    excluded = policy.get("excluded_analyses", [])
    if excluded:
        lines.append(f"- EXCLUDED: Do NOT include: {', '.join(excluded)}.")

    required = policy.get("required_analyses", [])
    if required:
        lines.append(f"- REQUIRED: Always include: {', '.join(required)} (if feasible).")

    max_nodes = policy.get("max_nodes", 10)
    lines.append(f"- MAX NODES: Limit DAG to {max_nodes} analysis nodes.")

    warnings = validate_policy(policy, column_roles)
    if warnings:
        lines.append("- POLICY WARNINGS:")
        for w in warnings:
            lines.append(f"  ⚠ {w}")

    return "\n".join(lines)
