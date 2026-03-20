
"""
Model Configuration — central model registry.
All agent files import get_model() from here.
"""
import os

MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-pro-preview")
FLASH = os.getenv("GEMINI_FLASH_MODEL", "gemini-2.0-flash")

_AGENT_MODELS = {
    "orchestrator": MODEL,
    "coder":        MODEL,
    "profiler":     MODEL,
    "discovery":    MODEL,
    "synthesis":    MODEL,
    "dag_builder":  MODEL,
    "chat":         MODEL,
    # Critic uses Flash — higher RPM limits, plenty smart for text review
    "critic":       FLASH,
}


def get_model(agent_name: str) -> str:
    """Return the model string for a given agent."""
    model = _AGENT_MODELS.get(agent_name)
    if not model:
        raise ValueError(f"No model configured for agent '{agent_name}'.")
    return model
