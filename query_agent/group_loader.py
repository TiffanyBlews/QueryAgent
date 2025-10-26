"""
Loader for scenario triads where a single scenario includes L3/L4/L5 variants.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List

from .spec import QuerySpec
from .config_loader import load_specs


@dataclass
class ScenarioGroup:
    scenario_id: str
    title: str
    base_description: str
    specs: List[QuerySpec] = field(default_factory=list)


def load_scenario_triads(path: Path) -> List[ScenarioGroup]:
    """
    Load scenario triads from a JSON/YAML configuration file.
    """
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        from .config_loader import yaml  # reuse optional import
        if yaml is None:
            raise RuntimeError("PyYAML is required to load YAML configs. Please install pyyaml or use JSON.")
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)

    scenarios = data.get("scenarios") if isinstance(data, dict) else None
    if not scenarios or not isinstance(scenarios, list):
        raise ValueError("Scenario config must contain a 'scenarios' list.")

    groups: List[ScenarioGroup] = []
    for item in scenarios:
        if not isinstance(item, dict):
            raise ValueError("Each scenario entry must be a dictionary.")
        scenario_id = item["scenario_id"]
        title = item.get("title", scenario_id)
        base_description = item.get("base_description", "")
        level_entries = item.get("levels")
        if not isinstance(level_entries, list) or not level_entries:
            raise ValueError(f"Scenario '{scenario_id}' must include at least one level entry.")
        specs: List[QuerySpec] = []
        for level in level_entries:
            search_value = level.get("search_queries")
            if search_value is None:
                search_value = level.get("search_query")
            if search_value is None:
                raise ValueError(f"Scenario '{scenario_id}' 缺少 search_query/search_queries。")
            specs.append(
                QuerySpec(
                    query_id=level["query_id"],
                    level=level["level"],
                    language=level.get("language", "zh"),
                    search_query=search_value,
                    scenario=level["scenario"],
                    task_focus=level.get("task_focus", []) or [],
                    deliverable_requirements=level.get("deliverable_requirements", []) or [],
                    evaluation_focus=level.get("evaluation_focus", []) or [],
                    notes=level.get("notes"),
                    orientation=level.get("orientation", "positive"),
                )
            )
        groups.append(
            ScenarioGroup(
                scenario_id=scenario_id,
                title=title,
                base_description=base_description,
                specs=specs,
            )
        )
    return groups
