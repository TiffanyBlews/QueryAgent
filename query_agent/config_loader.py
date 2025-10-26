"""
Utilities to load query specifications from YAML/JSON configuration files.
"""

from __future__ import annotations

import json
import hashlib
import re
from pathlib import Path
import os
from typing import Iterable, List

try:
    import yaml
except ImportError:  # pragma: no cover - fallback handled at runtime
    yaml = None

from .context_builder import build_context_bundle
from .context_repository import load_context_documents
from .profession_loader import ProfessionTask, iter_profession_tasks, load_profession_profiles
from .spec import QuerySpec
from .llm import OpenAIChatClient, LLMError


def _baseline_search_query(profession: str, task: ProfessionTask) -> str:
    """
    Default deterministic query builder (previous behavior):
    职业 + 任务类别 + 前两个标签 + 主题 + 固定后缀。
    """
    components = [profession, task.category]
    focus_tags = task.focus_tags or []
    if focus_tags:
        components.extend(focus_tags[:2])
    if task.theme_id:
        components.append(task.theme_id)
    components.extend(["最佳实践", "标准流程", "case study", "2024"])
    return " ".join(filter(None, components))


def _read_sop_excerpt(max_chars: int = 1800) -> str:
    """Read a short excerpt of Accurant_SOP.md if present to guide LLM."""
    for candidate in (Path("Accurant_SOP.md"), Path("Accurant_SOP copy.md")):
        if candidate.exists():
            try:
                txt = candidate.read_text(encoding="utf-8")
                return txt[:max_chars]
            except Exception:
                continue
    return ""


def _build_search_query_llm(profession: str, task: ProfessionTask) -> str:
    """
    Build a search query via LLM, using Accurant_SOP.md excerpt and the
    baseline deterministic query as an in-context example. Falls back to baseline on failure.
    """
    baseline = _baseline_search_query(profession, task)
    try:
        client = OpenAIChatClient()
    except LLMError:
        return baseline

    sop = _read_sop_excerpt()
    task_tags = ", ".join(task.focus_tags[:4]) if task.focus_tags else ""
    theme = task.theme_id or ""
    system = (
        "你是信息检索与证据搜集的研究助理。根据职业与任务场景，构造高命中率的搜索query。"
        "目标：更快找到权威、可验证的标准/指南/流程/监管/案例类资料；优先PDF、政府/学术/标准组织来源。"
    )
    user = (
        f"职业：{profession}\n"
        f"任务类别：{task.category}\n"
        f"主题：{theme}\n"
        f"标签：{task_tags}\n"
        f"任务描述：{task.description}\n\n"
        f"基线示例（不要原样返回，仅作风格参考）：{baseline}\n"
        "请返回 JSON：{\"queries\": [\"...\", \"...\"]}，长度1-3条，按优先级排序。"
        "查询要点：\n"
        "- 使用中文关键词为主，必要时附英文同义词（逗号或空格分隔）；\n"
        "- 偏好：标准/规范/指南/政策/白皮书/流程/PDF/案例；\n"
        "- 不同行业的任务需要的搜索关键词不同，请根据任务场景和行业特点生成搜索关键词，而不是生搬硬套；\n"
        "- 避免过于宽泛的词；包含年份或范围（如2025）有助于聚焦；\n"
        "- 不要包含解释文本，只返回JSON。\n"
    )
    # if sop:
    #     system += "\n以下是SOP摘录，可参考其对证据、流程与评估的偏好：\n" + sop

    try:
        data = client.run_json_completion([
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ])
        queries = data.get("queries") if isinstance(data, dict) else None
        if isinstance(queries, list) and queries:
            # pick the first non-empty string
            for q in queries:
                qs = str(q or "").strip()
                if qs:
                    return qs
    except LLMError:
        pass
    return baseline


def _build_search_query(profession: str, task: ProfessionTask) -> str:
    """
    Dispatch to LLM-based builder when LLM_SEARCH_QUERY is enabled; otherwise use baseline.
    """
    mode = os.environ.get("LLM_SEARCH_QUERY", "0").lower()
    if mode in ("1", "true", "yes", "on"):  # enable LLM-based construction
        return _build_search_query_llm(profession, task)
    return _baseline_search_query(profession, task)


DEFAULT_CONTEXT_BASE = Path("context_sources")


def slugify(text: str, max_length: int = 48) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", text or "").strip("-").lower()
    if cleaned:
        return cleaned[:max_length] or hashlib.sha1(text.encode("utf-8")).hexdigest()[:max_length]
    if not text:
        return "item"
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:max_length]


def _load_from_profession_config(path: Path) -> List[QuerySpec]:
    profiles = load_profession_profiles(path)
    specs: List[QuerySpec] = []
    for profile, task in iter_profession_tasks(profiles):
        context = build_context_bundle(profile, task)
        search_query = _build_search_query(profile.profession, task)
        task_focus = [
            f"围绕{task.category}场景完成：{task.description}",
        ]
        if task.focus_tags:
            task_focus.append("关键关注标签：" + "、".join(task.focus_tags))
        task_focus.append("交付需遵循SOP V7.0，所有关键判断必须引用Ground Truth来源。")

        deliverables = list(task.expected_outputs) or [
            "提交结构化主文档，包含任务目标、行动计划、验收标准与风险控制。",
        ]

        evaluation_focus = context.success_metrics or [
            "输出需可量化评估，并包含自检/复核步骤。",
        ]

        scenario = (
            f"{context.persona.name}正在负责{task.category}相关任务，需要在{task.timebox or '限定时间'}内完成："
            f"{task.description}。团队期待一个可执行、可验证的方案，便于复盘与质量审查。"
        )

        context_base = DEFAULT_CONTEXT_BASE / slugify(profile.industry) / slugify(profile.profession) / slugify(task.task_id)
        context_docs = load_context_documents(context_base, limit=3)

        spec = QuerySpec(
            query_id=task.task_id,
            level=task.normalized_level(),
            search_query=search_query,
            scenario=scenario,
            language="zh",
            task_focus=task_focus,
            deliverable_requirements=deliverables,
            evaluation_focus=evaluation_focus,
            notes=f"Persona：{context.persona.description}；时间盒：{task.timebox}",
            industry=profile.industry,
            profession=profile.profession,
            context_bundle=context,
            task_metadata={
                "task_id": task.task_id,
                "category": task.category,
                "theme_id": task.theme_id,
                "timebox": task.timebox,
                "focus_tags": task.focus_tags,
                "context_base": str(context_base),
            },
            context_documents=context_docs,
        )
        specs.append(spec)
    return specs


def load_specs(path: Path) -> List[QuerySpec]:
    """
    Load QuerySpec definitions from a YAML or JSON file.
    """
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    text = path.read_text(encoding="utf-8")

    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise RuntimeError("PyYAML is required to load YAML configs. Please install pyyaml or use JSON.")
        data = yaml.safe_load(text)
    elif suffix == ".json":
        data = json.loads(text)
    else:
        raise ValueError("Config file must be .yaml, .yml or .json")

    entries: Iterable[dict]
    if isinstance(data, dict) and "professions" in data and "queries" not in data:
        return _load_from_profession_config(path)
    if isinstance(data, dict) and "queries" in data:
        entries = data["queries"]
    elif isinstance(data, list):
        entries = data
    else:
        raise ValueError("Config structure must be a list or contain a 'queries' key.")

    specs: List[QuerySpec] = []
    for item in entries:
        if not isinstance(item, dict):
            raise ValueError("Each query entry must be a dictionary.")
        search_value = item.get("search_queries")
        if search_value is None:
            search_value = item.get("search_query")
        if search_value is None:
            raise ValueError("Each query entry must include 'search_query' or 'search_queries'.")
        specs.append(
            QuerySpec(
                query_id=item["query_id"],
                level=item["level"],
                search_query=search_value,
                scenario=item["scenario"],
                language=item.get("language", "zh"),
                task_focus=item.get("task_focus", []) or [],
                deliverable_requirements=item.get("deliverable_requirements", []) or [],
                evaluation_focus=item.get("evaluation_focus", []) or [],
                notes=item.get("notes"),
                industry=item.get("industry"),
                profession=item.get("profession"),
            )
        )
    return specs
def sanitize_filename(text: str, max_length: int = 64) -> str:
    text = (text or "").strip().lower()
    if not text:
        return "item"
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    if len(text) > max_length:
        text = text[:max_length].rstrip("-")
    return text or "item"
