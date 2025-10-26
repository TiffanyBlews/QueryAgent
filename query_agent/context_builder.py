"""
Helpers to convert profession tasks into structured Context information.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional

from .data_structures import ContextBundle, PersonaProfile
from .persona_registry import PersonaRecord, select_persona, load_persona_registry
from .profession_loader import ProfessionProfile, ProfessionTask


DEFAULT_PERSONA_ARCHETYPES = (
    {
        "identifier": "junior_specialist",
        "name": "新晋执行专员",
        "seniority": "junior",
        "description": "刚加入团队不到一年，负责把总部策略落地到一线执行。希望通过标准化流程快速交付成果。",
        "motivations": ["证明交付能力", "建立可信的工作底稿"],
        "pain_points": ["信息碎片化", "跨部门协调成本高"],
    },
    {
        "identifier": "senior_lead",
        "name": "资深项目负责人",
        "seniority": "senior",
        "description": "负责关键项目的端到端推进，需要兼顾策略规划与跨团队协同，对风险控制高度敏感。",
        "motivations": ["构建可复制的方法论", "降低高层对交付质量的担忧"],
        "pain_points": ["资源受限", "高管对齐节奏快"],
    },
    {
        "identifier": "cross_function_partner",
        "name": "跨部门接口经理",
        "seniority": "mid",
        "description": "负责协调各职能团队，确保节点和数据对齐，需要处理大量临时沟通和冲突。",
        "motivations": ["保障协同效率", "让关键风险透明可控"],
        "pain_points": ["上下游信息不同步", "缺乏统一的追踪工具"],
    },
)

DEFAULT_REGISTRY_PATH = Path("resources/persona_bank/personas.jsonl")


def _load_default_registry() -> List[PersonaRecord]:
    if DEFAULT_REGISTRY_PATH.exists():
        try:
            return load_persona_registry(DEFAULT_REGISTRY_PATH)
        except Exception:
            return []
    return []


_CACHED_REGISTRY: Optional[List[PersonaRecord]] = None


def _get_registry() -> List[PersonaRecord]:
    global _CACHED_REGISTRY
    if _CACHED_REGISTRY is None:
        _CACHED_REGISTRY = _load_default_registry()
    return _CACHED_REGISTRY or []


def _instantiate_personas(profile: ProfessionProfile) -> List[PersonaProfile]:
    personas: List[PersonaProfile] = []
    for base in DEFAULT_PERSONA_ARCHETYPES:
        personas.append(
            PersonaProfile(
                identifier=f"{profile.profession.lower().replace(' ', '_')}-{base['identifier']}",
                name=f"{profile.profession} · {base['name']}",
                seniority=base["seniority"],
                description=f"{base['description']}（行业：{profile.industry}）",
                motivations=list(base["motivations"]),
                pain_points=list(base["pain_points"]),
            )
        )
    return personas


def build_context_bundle(
    profile: ProfessionProfile,
    task: ProfessionTask,
    *,
    registry: Optional[Iterable[PersonaRecord]] = None,
) -> ContextBundle:
    """
    Create a context bundle with persona, constraints, assets and success metrics.
    """
    registry_items = list(registry or _get_registry())

    persona_record = select_persona(
        registry_items,
        industry=profile.industry,
        profession=profile.profession,
        tags=task.focus_tags,
        preferred_seniority=task.normalized_level(),
        seed=abs(hash((profile.profession, task.task_id))),
    )

    if persona_record:
        persona = PersonaProfile(
            identifier=persona_record.persona_id,
            name=persona_record.title,
            seniority=persona_record.seniority,
            description=persona_record.summary,
            motivations=persona_record.motivations,
            pain_points=persona_record.pain_points,
        )
    else:
        personas = _instantiate_personas(profile)
        persona_index = abs(hash(task.task_id)) % len(personas)
        persona = personas[persona_index]

    time_constraint = task.timebox or "需在当周内完成"
    constraints = [
        f"时间盒：{time_constraint}",
        "必须遵循AgencyBench SOP V7.0的Ground Truth先行原则，不得捏造证据。",
        "交付需支持Agent-as-a-Judge复核，所有结论需引用明确来源。",
    ]

    if task.category:
        constraints.append(f"任务类别：{task.category}，需符合该类工作的标准流程。")
    if task.focus_tags:
        constraints.append(
            "重点关注标签：" + "、".join(task.focus_tags)
        )

    available_assets = [
        "行业内已有的合规政策、操作手册或调研成果（需列出处）",
        "组织内部的看板/项目文档，可在交付物中引用结构但不得泄露敏感数据",
        "本任务提供的Ground Truth资料（详见ground_truth.sources）",
    ]

    success_metrics: List[str] = []
    if task.expected_outputs:
        success_metrics.append("交付物需覆盖以下内容：" + "；".join(task.expected_outputs))
    success_metrics.append("每个关键判断均需注明数据或文献依据及验证方式。")
    success_metrics.append("交付需包含自检或复核清单，保证可追溯。")

    user_statement = (
        f"我是{profile.industry}行业的{profile.profession}，当前负责任务：{task.description}"
        "。我需要一个结构化、可验证的方案来推动工作落地，并确保与管理层的验收标准对齐。"
    )

    return ContextBundle(
        persona=persona,
        user_statement=user_statement,
        constraints=constraints,
        available_assets=available_assets,
        success_metrics=success_metrics,
    )
