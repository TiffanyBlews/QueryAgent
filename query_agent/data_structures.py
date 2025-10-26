"""
Shared dataclasses used across the query generation pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .search import SearchResult


@dataclass
class PersonaProfile:
    """
    Represents a simulated user persona within a profession.
    """

    identifier: str
    name: str
    seniority: str
    description: str
    motivations: List[str] = field(default_factory=list)
    pain_points: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return {
            "id": self.identifier,
            "name": self.name,
            "seniority": self.seniority,
            "description": self.description,
            "motivations": list(self.motivations),
            "pain_points": list(self.pain_points),
        }


@dataclass
class ContextBundle:
    """
    Structured context information delivered to the query builder and final payload.
    """

    persona: PersonaProfile
    user_statement: str
    constraints: List[str] = field(default_factory=list)
    available_assets: List[str] = field(default_factory=list)
    success_metrics: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return {
            "persona": self.persona.to_dict(),
            "user_statement": self.user_statement,
            "constraints": list(self.constraints),
            "available_assets": list(self.available_assets),
            "success_metrics": list(self.success_metrics),
        }


@dataclass
class GroundTruthSource:
    """
    A single evidence source that can be used for evaluation.
    """

    title: str
    url: str
    snippet: Optional[str] = None
    source: Optional[str] = None
    date: Optional[str] = None
    search_query: Optional[str] = None

    @classmethod
    def from_search_result(cls, result: SearchResult) -> "GroundTruthSource":
        return cls(
            title=result.title,
            url=result.url,
            snippet=result.snippet,
            source=result.source,
            date=result.date,
            search_query=result.search_query,
        )

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "title": self.title,
            "url": self.url,
            "snippet": self.snippet,
            "source": self.source,
            "date": self.date,
            "search_query": self.search_query,
        }


@dataclass
class GroundTruthBundle:
    """
    Aggregates the primary ground truth and supporting references.
    """

    primary: GroundTruthSource
    supporting: List[GroundTruthSource] = field(default_factory=list)

    def all_sources(self) -> List[GroundTruthSource]:
        return [self.primary, *self.supporting]

    def to_dict(self) -> Dict[str, object]:
        return {
            "primary": self.primary.to_dict(),
            "supporting": [item.to_dict() for item in self.supporting],
        }


@dataclass
class EvaluationGuide:
    """
    Structured summary of what good answers should contain.
    """

    summary: str
    checkpoints: List[str] = field(default_factory=list)
    scoring_rubric: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return {
            "summary": self.summary,
            "checkpoints": list(self.checkpoints),
            "scoring_rubric": list(self.scoring_rubric),
        }
