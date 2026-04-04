import os
import yaml

_cfg = None
_CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config.yaml')

def get_config() -> dict:
    global _cfg
    if _cfg is None:
        with open(_CONFIG_PATH, 'r') as f:
            _cfg = yaml.safe_load(f)
    return _cfg
