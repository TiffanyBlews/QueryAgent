"""
SOP compliance linter for generated query payloads.
"""

from __future__ import annotations

from typing import Dict, List
import re


def lint_payload(payload: Dict) -> List[str]:
    """Return a list of violations against Accurant_SOP expectations.

    Severity is not encoded; caller decides whether to fail/auto-fix.
    """
    issues: List[str] = []
    level = str(payload.get("level") or "").upper()

    # Required fields
    for field in ("role_and_background", "task_objectives", "deliverables", "grading_rubric"):
        if not payload.get(field):
            issues.append(f"missing_field:{field}")

    # Ground truth policy
    gt = payload.get("ground_truth") or {}
    primary = (gt.get("primary") or {})
    if not primary.get("url"):
        issues.append("ground_truth:missing_primary_url")

    # References must not include the same URL as ground truth
    gt_urls = set()
    if primary.get("url"):
        gt_urls.add(primary["url"]) 
    for it in gt.get("supporting") or []:
        url = it.get("url")
        if url:
            gt_urls.add(url)
    for ref in payload.get("references") or []:
        if ref.get("url") in gt_urls:
            issues.append("references:contains_ground_truth_url")

    # L4 training-free guardrails
    if level == "L4":
        text_blobs: List[str] = []
        text_blobs.extend(payload.get("task_objectives") or [])
        d = payload.get("deliverables") or {}
        text_blobs.extend(d.get("expected_outputs") or [])
        text_blobs.append(d.get("format_requirements") or "")
        text_blobs.append(d.get("quality_bar") or "")
        text_blobs.extend(payload.get("grading_rubric") or [])
        eg = payload.get("evaluation_guide") or {}
        text_blobs.extend(eg.get("checkpoints") or [])
        text_blobs.extend(eg.get("scoring_rubric") or [])
        txt = "\n".join(map(str, text_blobs))
        if re.search(r"训练|fine-?tune|微调", txt, flags=re.IGNORECASE):
            issues.append("l4:no_training_language")

    return issues

