"""
Model Configuration — central model registry.
All agent files import get_model() from here.
"""
import os

MODEL = os.getenv("OPENAI_MODEL", "openai/gpt-4o")

_AGENT_MODELS = {
    "orchestrator": MODEL,
    "coder":        MODEL,
    "profiler":     MODEL,
    "discovery":    MODEL,
    "synthesis":    MODEL,
    "dag_builder":  MODEL,
}


def get_model(agent_name: str) -> str:
    """Return the model string for a given agent."""
    model = _AGENT_MODELS.get(agent_name)
    if not model:
        raise ValueError(f"No model configured for agent '{agent_name}'.")
    return model
