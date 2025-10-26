"""
Helpers for constructing inverse (negative orientation) tasks alongside positive ones.
"""

from __future__ import annotations

from dataclasses import replace
from typing import List, Optional, Sequence, Set

from .spec import QuerySpec

INVERSE_NOTES_HINT = (
    "本任务为负向任务：请参考《inverse_agency.md》，沿用其中总结的三类陷阱设计思路"
    "（违反领域根律、引用错误/不可复现数据、资源或能力根本不可行），重点考察智能体识别矛盾并"
    "提交证伪过程的能力。确保最终目标是得出“任务不可完成或前提有误”的结论，而非继续推进原需求。"
)


def _ensure_inverse_notes(notes: Optional[str], guidance_hint: str) -> str:
    text = (notes or "").strip()
    if guidance_hint in text:
        return text
    return f"{text}\n{guidance_hint}".strip() if text else guidance_hint


def build_inverse_spec(
    spec: QuerySpec,
    *,
    existing_ids: Optional[Set[str]] = None,
    guidance_hint: str = INVERSE_NOTES_HINT,
) -> QuerySpec:
    """
    Clone a positive-orientation QuerySpec into its inverse counterpart.
    """
    if spec.normalized_orientation() != "positive":
        raise ValueError("Only positive tasks can be inverted automatically.")

    base_id = f"{spec.query_id}-inverse"
    candidate = base_id

    if existing_ids is not None:
        counter = 1
        while candidate in existing_ids:
            counter += 1
            candidate = f"{base_id}-{counter}"
        existing_ids.add(candidate)

    inverse_notes = _ensure_inverse_notes(spec.notes, guidance_hint)

    return replace(
        spec,
        query_id=candidate,
        orientation="inverse",
        notes=inverse_notes,
    )


def expand_with_inverse_specs(
    specs: Sequence[QuerySpec],
    *,
    guidance_hint: str = INVERSE_NOTES_HINT,
) -> List[QuerySpec]:
    """
    Expand a list of QuerySpecs by appending inverse variants of positive-orientation tasks.
    """
    expanded: List[QuerySpec] = []
    seen_ids = {spec.query_id for spec in specs}

    for spec in specs:
        expanded.append(spec)
        try:
            orientation = spec.normalized_orientation()
        except ValueError:
            # Propagate validation error for unknown orientations.
            raise
        if orientation != "positive":
            continue
        inverse_spec = build_inverse_spec(
            spec,
            existing_ids=seen_ids,
            guidance_hint=guidance_hint,
        )
        expanded.append(inverse_spec)
    return expanded
