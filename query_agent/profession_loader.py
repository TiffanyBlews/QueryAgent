"""
Utilities for loading profession definitions and related tasks.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


@dataclass
class ProfessionTask:
    task_id: str
    category: str
    theme_id: str
    timebox: str
    complexity: str
    description: str
    expected_outputs: List[str] = field(default_factory=list)
    focus_tags: List[str] = field(default_factory=list)

    def normalized_level(self) -> str:
        value = (self.complexity or "").strip().upper()
        mapping = {"L3": "L3", "L4": "L4", "L5": "L5"}
        if value not in mapping:
            raise ValueError(f"Unsupported task complexity '{self.complexity}' for task {self.task_id}")
        return mapping[value]


@dataclass
class ProfessionProfile:
    industry: str
    profession: str
    task_template_version: Optional[str]
    daily_tasks: List[ProfessionTask] = field(default_factory=list)

    def choose_persona_seed(self) -> int:
        """
        Provide a deterministic seed for persona selection.
        """
        seed = hash((self.industry, self.profession))
        return seed & 0xFFFFFFFF


def load_profession_profiles(path: Path) -> List[ProfessionProfile]:
    """
    Load professions and their tasks from a JSON configuration file.
    """
    text = path.read_text(encoding="utf-8")
    data = json.loads(text)

    professions: List[ProfessionProfile] = []
    entries = data.get("professions")
    if not isinstance(entries, list):
        raise ValueError("Expected top-level 'professions' list in profession config.")

    for item in entries:
        daily_tasks = []
        for raw_task in item.get("daily_tasks", []):
            daily_tasks.append(
                ProfessionTask(
                    task_id=raw_task["task_id"],
                    category=raw_task.get("category", ""),
                    theme_id=raw_task.get("theme_id", ""),
                    timebox=raw_task.get("timebox", ""),
                    complexity=raw_task.get("complexity", ""),
                    description=raw_task.get("description", ""),
                    expected_outputs=list(raw_task.get("expected_outputs", []) or []),
                    focus_tags=list(raw_task.get("focus_tags", []) or []),
                )
            )

        professions.append(
            ProfessionProfile(
                industry=item.get("industry", "unknown"),
                profession=item.get("profession", "unknown"),
                task_template_version=item.get("task_template_version"),
                daily_tasks=daily_tasks,
            )
        )

    return professions


def iter_profession_tasks(
    profiles: Sequence[ProfessionProfile],
    *,
    professions: Optional[Sequence[str]] = None,
    industries: Optional[Sequence[str]] = None,
    task_ids: Optional[Sequence[str]] = None,
    levels: Optional[Sequence[str]] = None,
    shuffle: bool = False,
) -> Iterator[Tuple[ProfessionProfile, ProfessionTask]]:
    """
    Iterate over profession tasks applying optional filters.
    """
    normalized_professions = {p.lower() for p in professions} if professions else None
    normalized_industries = {i.lower() for i in industries} if industries else None
    normalized_task_ids = {tid.lower() for tid in task_ids} if task_ids else None
    normalized_levels = {lvl.upper() for lvl in levels} if levels else None

    items: List[Tuple[ProfessionProfile, ProfessionTask]] = []
    for profile in profiles:
        if normalized_professions and profile.profession.lower() not in normalized_professions:
            continue
        if normalized_industries and profile.industry.lower() not in normalized_industries:
            continue
        for task in profile.daily_tasks:
            if normalized_task_ids and task.task_id.lower() not in normalized_task_ids:
                continue
            if normalized_levels and task.normalized_level() not in normalized_levels:
                continue
            items.append((profile, task))

    if shuffle:
        rng = random.Random(42)
        rng.shuffle(items)

    for entry in items:
        yield entry


def sample_profession_tasks(
    profiles: Sequence[ProfessionProfile],
    *,
    max_per_profession: Optional[int] = None,
    professions: Optional[Sequence[str]] = None,
    industries: Optional[Sequence[str]] = None,
    task_ids: Optional[Sequence[str]] = None,
    levels: Optional[Sequence[str]] = None,
) -> List[Tuple[ProfessionProfile, ProfessionTask]]:
    """
    Select a limited number of tasks per profession for generation.
    """
    grouped: Dict[str, List[Tuple[ProfessionProfile, ProfessionTask]]] = {}
    for profile, task in iter_profession_tasks(
        profiles,
        professions=professions,
        industries=industries,
        task_ids=task_ids,
        levels=levels,
        shuffle=True,
    ):
        grouped.setdefault(profile.profession, []).append((profile, task))

    selections: List[Tuple[ProfessionProfile, ProfessionTask]] = []
    for profession, items in grouped.items():
        limit = max_per_profession or len(items)
        selections.extend(items[:limit])
    return selections
