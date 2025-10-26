"""
Data structures for configuring query construction.
"""

from __future__ import annotations

from dataclasses import dataclass, field, InitVar
import re
from typing import Dict, List, Optional, Sequence, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from .data_structures import ContextBundle


def normalize_search_queries(
    raw: Union[str, Sequence[str], None],
) -> List[str]:
    """
    Normalize search queries into a de-duplicated list of non-empty strings.
    Supports strings separated by semicolons/commas (including full-width variants).
    """
    if raw is None:
        return []

    queries: List[str] = []

    def _push(value: Optional[str]) -> None:
        if not value:
            return
        stripped = value.strip()
        if not stripped:
            return
        queries.append(stripped)

    splitter = re.compile(r"[;,，；]+")

    if isinstance(raw, str):
        parts = [part.strip() for part in splitter.split(raw)]
        for part in parts:
            _push(part)
    elif isinstance(raw, Sequence):
        for item in raw:
            if item is None:
                continue
            if isinstance(item, str):
                parts = [part.strip() for part in splitter.split(item)]
                for part in parts:
                    _push(part)
            else:
                _push(str(item))
    else:
        _push(str(raw))

    seen: set[str] = set()
    deduped: List[str] = []
    for query in queries:
        if query not in seen:
            seen.add(query)
            deduped.append(query)
    return deduped


@dataclass
class QuerySpec:
    """
    Configuration for a single query to be generated.
    """

    query_id: str
    level: str
    scenario: str
    search_query: InitVar[Union[str, Sequence[str], None]]
    language: str = "zh"
    task_focus: List[str] = field(default_factory=list)
    deliverable_requirements: List[str] = field(default_factory=list)
    evaluation_focus: List[str] = field(default_factory=list)
    notes: Optional[str] = None
    orientation: str = "positive"
    industry: Optional[str] = None
    profession: Optional[str] = None
    context_bundle: Optional["ContextBundle"] = None
    task_metadata: Dict[str, object] = field(default_factory=dict)
    context_documents: List[Dict[str, object]] = field(default_factory=list)
    search_queries: List[str] = field(init=False, repr=False, default_factory=list)

    def __post_init__(self, search_query: Union[str, Sequence[str], None]) -> None:
        self._set_search_queries(search_query)

    def normalized_level(self) -> str:
        upper = self.level.upper()
        if upper not in {"L3", "L4", "L5"}:
            raise ValueError(f"Unsupported level '{self.level}'. Expected one of L3/L4/L5.")
        return upper

    def normalized_orientation(self) -> str:
        value = (self.orientation or "positive").strip().lower()
        if value not in {"positive", "inverse"}:
            raise ValueError(f"Unsupported orientation '{self.orientation}'. Expected 'positive' or 'inverse'.")
        return value

    def _set_search_queries(self, value: Union[str, Sequence[str], None]) -> None:
        normalized = normalize_search_queries(value)
        if not normalized:
            raise ValueError(f"search_query for '{self.query_id}' must not be empty.")
        self.search_queries = normalized

    @property
    def search_query(self) -> str:
        return self.search_queries[0] if self.search_queries else ""

    @search_query.setter
    def search_query(self, value: Union[str, Sequence[str], None]) -> None:
        self._set_search_queries(value)

    def to_metadata(self) -> dict:
        payload = {
            "query_id": self.query_id,
            "level": self.normalized_level(),
            "language": self.language,
            "search_query": self.search_query,
            "search_queries": list(self.search_queries),
            "notes": self.notes,
            "orientation": self.normalized_orientation(),
            "industry": self.industry,
            "profession": self.profession,
            "task_metadata": self.task_metadata,
        }
        if self.context_bundle:
            payload["context"] = self.context_bundle.to_dict()
        if self.context_documents:
            payload["context_documents"] = self.context_documents
        return payload
