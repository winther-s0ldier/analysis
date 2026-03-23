import os

MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-pro-preview")

_AGENT_MODELS = {
    "orchestrator": MODEL,
    "coder":        MODEL,
    "profiler":     MODEL,
    "discovery":    MODEL,
    "synthesis":    MODEL,
    "dag_builder":  MODEL,
    "chat":         MODEL,
    "critic":       MODEL,
}


def get_model(agent_name: str) -> str:
    """Return the model string for a given agent."""
    model = _AGENT_MODELS.get(agent_name)
    if not model:
        raise ValueError(f"No model configured for agent '{agent_name}'.")
    return model
