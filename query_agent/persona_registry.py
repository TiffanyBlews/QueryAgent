"""
Persona registry utilities for contextual query generation.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Sequence


@dataclass
class PersonaRecord:
    persona_id: str
    title: str
    seniority: str
    summary: str
    motivations: List[str] = field(default_factory=list)
    pain_points: List[str] = field(default_factory=list)
    expertise: List[str] = field(default_factory=list)
    industries: List[str] = field(default_factory=list)
    professions: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    source: Optional[str] = None

    def matches(self, *, industry: Optional[str], profession: Optional[str], tags: Iterable[str]) -> bool:
        industry = (industry or "").lower()
        profession = (profession or "").lower()
        tag_set = {tag.lower() for tag in tags}

        if self.industries and industry and industry.lower() not in {item.lower() for item in self.industries}:
            return False
        if self.professions and profession and profession.lower() not in {item.lower() for item in self.professions}:
            return False
        if tag_set and self.tags:
            persona_tags = {tag.lower() for tag in self.tags}
            if not persona_tags.intersection(tag_set):
                return False
        return True


def load_persona_registry(path: Path) -> List[PersonaRecord]:
    if not path.exists():
        raise FileNotFoundError(f"Persona registry not found: {path}")
    records: List[PersonaRecord] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            data = json.loads(line)
            records.append(
                PersonaRecord(
                    persona_id=data["persona_id"],
                    title=data["title"],
                    seniority=data.get("seniority", "mid"),
                    summary=data.get("summary", ""),
                    motivations=list(data.get("motivations", []) or []),
                    pain_points=list(data.get("pain_points", []) or []),
                    expertise=list(data.get("expertise", []) or []),
                    industries=list(data.get("industries", []) or []),
                    professions=list(data.get("professions", []) or []),
                    tags=list(data.get("tags", []) or []),
                    source=data.get("source"),
                )
            )
    return records


def select_persona(
    registry: Sequence[PersonaRecord],
    *,
    industry: Optional[str],
    profession: Optional[str],
    tags: Iterable[str],
    preferred_seniority: Optional[str] = None,
    seed: Optional[int] = None,
) -> Optional[PersonaRecord]:
    candidates = [
        item
        for item in registry
        if item.matches(industry=industry, profession=profession, tags=tags)
    ]
    if preferred_seniority:
        seniority_lower = preferred_seniority.lower()
        preferred = [item for item in candidates if item.seniority.lower() == seniority_lower]
        if preferred:
            candidates = preferred

    if not candidates:
        return None
    rng = random.Random(seed)
    return rng.choice(candidates)


def dump_persona_registry(path: Path, records: Sequence[PersonaRecord]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for item in records:
            fh.write(
                json.dumps(
                    {
                        "persona_id": item.persona_id,
                        "title": item.title,
                        "seniority": item.seniority,
                        "summary": item.summary,
                        "motivations": item.motivations,
                        "pain_points": item.pain_points,
                        "expertise": item.expertise,
                        "industries": item.industries,
                        "professions": item.professions,
                        "tags": item.tags,
                        "source": item.source,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
