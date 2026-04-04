import os
import sys
import yaml
from typing import Dict, List, Any

def _get_yaml_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)
    return os.path.join(root, "data", "workflow.yaml")

def load_workflow() -> Dict[str, Any]:
    path = _get_yaml_path()
    if not os.path.exists(path):
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"[WorkflowLoader] Failed to load {path}: {e}")
        return {}

def get_dataset_profile(dataset_type: str) -> Dict[str, Any]:
    wf = load_workflow()
    profiles = wf.get("dataset_profiles", [])
    if not isinstance(profiles, list):
        return {}

    return next(
        (p for p in profiles if isinstance(p, dict) and p.get("dataset_type") == dataset_type),
        {}
    )

def register_custom_analyses(library_registry: Dict[str, Any]) -> None:
    wf = load_workflow()
    custom_list = wf.get("custom_analyses")
    if not custom_list or not isinstance(custom_list, list):
        return

    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)

    for item in custom_list:
        if not isinstance(item, dict): continue

        name = item.get("name")
        script_path = item.get("script_path")
        func_name = item.get("function_name")

        if not all([name, script_path, func_name]):
            continue

        full_script_path = os.path.join(root, script_path)
        if not os.path.exists(full_script_path):
            print(f"[WorkflowLoader] Custom script not found: {full_script_path}")
            continue

        script_dir = os.path.dirname(full_script_path)
        module_name = os.path.splitext(os.path.basename(full_script_path))[0]

        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)

        try:
            mod = __import__(module_name)
            func = getattr(mod, func_name)

            library_registry[name] = {
                "function":     func_name,
                "_direct_fn":   func,
                "_script_dir":  script_dir,
                "_module_name": module_name,
                "required_args": item.get("required_args", []),
                "col_role": item.get("col_role", "custom"),
                "description": item.get("description", "Custom user analysis"),
                "is_custom": True
            }
            print(f"[WorkflowLoader] Registered custom analysis: '{name}' from {module_name}.py")
        except Exception as e:
            print(f"[WorkflowLoader] Failed to load custom analysis '{name}': {e}")
