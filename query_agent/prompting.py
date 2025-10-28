"""
Prompt construction utilities following SOP V8.0 requirements.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence

from .data_structures import ContextBundle, GroundTruthBundle
from .spec import QuerySpec


LEVEL_GUIDELINES: Dict[str, Dict[str, str]] = {
    "L3": {
        "nickname": "基础题 / 课后习题",
        "timebox": "预期人类投入：数小时至1天。",
        "focus": (
            "任务必须是封闭且有明确验收标准的模块。强调实现、调试、验证，"
            "避免开放式探索或需要大型算力。"
        ),
        "evaluation": (
            "评分侧重功能正确性、是否通过验证/测试、调试过程中的问题排查能力。"
        ),
        "tooling": (
            "主要依赖单一Agent模式（如Claude Code或Deep Research）完成，多步复杂调研不是重点。"
        ),
    },
    "L4": {
        "nickname": "综合题 / 课程大作业",
        "timebox": "预期人类投入：数天至1周。",
        "focus": (
            "需要复现一个已有高质量成果，体现调研、规划、实现与对比分析的完整流程。"
        ),
        "evaluation": (
            "评分要关注产出与Ground Truth的一致性、方案合理性、调研深度。"
        ),
        "tooling": (
            "通常依赖单一核心Agent模式，但需要多轮迭代、系统的信息整合与复现步骤。"
        ),
    },
    "L5": {
        "nickname": "开放题 / 创新题",
        "timebox": "预期人类投入：至少1个月。",
        "focus": (
            "面向开放性战略问题，在约束条件下提出创新方案。应鼓励多轮调研、实验、分析。"
        ),
        "evaluation": (
            "评分关注逻辑严密性、创新性、可行性，以及与行业挑战或后验事件的契合度。"
        ),
        "tooling": (
            "要求Deep Research与Claude Code等多种能力交替使用，模拟调研-验证循环。"
        ),
    },
}

ORIENTATION_GUIDELINES: Dict[str, Dict[str, str]] = {
    "positive": {
        "label": "正向任务",
        "purpose": "设计一个可执行、可验证的高质量任务，帮助评测模型在真实业务中的交付能力。",
        "success": "交付物满足要求、完成度和质量可被客观评估。",
    },
    "inverse": {
        "label": "负向任务",
        "purpose": (
            "设计一个看似合理但本质不可完成或基于错误前提的陷阱任务，以评测智能体的批判性思维与证伪能力。"
        ),
        "success": (
            "理想输出应当引导执行者识别矛盾、验证不可行性，并提交证据链证明为何任务无法完成或前提有误。"
        ),
        "playbook": (
            "构造要点：构造一个看似合理但本质不可完成或基于错误前提的陷阱任务，以评测智能体的批判性思维与证伪能力。\n"
            "- 违反已知规律或关键约束，让任务目标与领域原理冲突；\n"
            "- 基于错误或不可复现的数据/结论，要求执行者通过实验或复盘证明标准答案不成立；\n"
            "- 设定远超现实资源/能力边界的目标，要求智能体在多轮探索后得出“不可行”结论；\n"
            "评测重点是执行者是否能识别陷阱、提供推理/实验日志，最终形成有据可依的‘拒绝执行’结论。"
            "不要在标题和任务要求中出现‘负向任务’‘带有冲突’等字眼暗示执行者这是负向任务。"
        ),
    },
}


SOP_SYSTEM_PRINCIPLES = (
    "SOP核心原则提醒：\n"
    # 教师命题与Ground Truth先行
    "1. 教师命题范式：先确定考察能力，然后找到高质量现实成果(Ground Truth)，再生成评分Rubric，再逆向设计题目；每道题都要像专业老师或企业领导布置的真实作业，禁止凭空脑洞。\n"
    "2. Ground Truth先行：必须先锁定可获取、可验证、质量高的资料（论文、研报、系统、法规等），并作为题目的最终标准答案。\n"
    # 场景真实与三E原则（V8新增显式要求）
    "3. 真实性与场景化：角色、组织、业务背景要真实可信，不要说这是给AI布置的任务，而是像老师或领导布置的真实任务，题目来源于实践痛点；避免‘AI味’或不可落地的幻想任务。\n"
    "4. 三E原则：Examining（考察能力清晰、能触发高阶能动性）；Executable（任务可执行，信息充分、边界明确、无需额外神秘上下文）；Evaluable（可评估，产出具备客观评分标准，可根据Ground Truth进行客观评分）。\n"
    # 层级匹配与红线
    "5. 层级与耗时匹配：L3=封闭模块+实现/调试（≤1天，不得要求复杂规划或重训练）；L4=复现已有成果+信息整合（数天至1周）；L5=开放战略题+Deep Research与Claude Code循环（≥1月）。严禁跨级滥用。\n"
    "6. 质量红线：保持题型多样；L3不能需要多轮复杂推理/大算力/L5式开放目标；L4/L5必须有可对比或可回溯的评估标准；任何层级都要提供可验证的交付物与评价指标。\n"
    # 安全与可控性（V8新增显式要求）
    "7. 安全与可控性：仅使用公开、中立、国际化数据源；避免隐私、内部统计、敏感政治内容；必要时做脱敏（如‘某市’、‘某公司’）。应根据参考资料和Ground Truth的发布时间设置资料使用时间窗口（如‘仅用至YYYY-MM-DD前信息’），并可限制信息源（如仅限给定文档库）。\n"
    # 训练与算力红线（V8强调）
    "8. 训练与算力红线：任务须training-free，禁止从头训练或长时间/昂贵算力依赖；评测以实现、复现、分析为主，不以训练为主。\n"
    # 自检与统一评估
    "9. 自检闭环：题目需可被目标Agent实际执行，并附自检/验证提示；如任务不可完成，必须设计清晰的证伪路径而非含糊拒绝。\n"
    "10. 统一评估框架：所有Query都应让评测Agent依据Ground Truth或Rubric客观打分，不得依赖隐性知识或无法重复的主观判断。"
)


MAX_CONTEXT_CHARS_PER_DOC = 1800


def build_messages(
    spec: QuerySpec,
    context_bundle: ContextBundle,
    ground_truth_bundle: GroundTruthBundle,
    context_blocks: Optional[Sequence[Dict[str, str]]] = None,
) -> List[Dict[str, str]]:
    """
    Construct chat messages for the LLM using the SOP requirements and ground truth.
    """

    level = spec.normalized_level()
    guideline = LEVEL_GUIDELINES[level]
    orientation = spec.normalized_orientation()
    orientation_meta = ORIENTATION_GUIDELINES[orientation]

    language_instruction = "请使用中文输出。" if spec.language.lower().startswith("zh") else "Please respond in English."

    extra_system_tail = ""
    if context_blocks:
        extra_system_tail = (
            " 你将收到若干补充上下文文档，这些内容代表项目方提供的真实背景，"
            "需要在命题时吸收其中的约束、术语与工作流要求。"
        )

    system_message = {
        "role": "system",
        "content": (
            "你是一名为AgencyBench提供高质量Query的命题教师。"
            "你的职责是基于提供的公开资料（以下统一称为‘参考资料’）逆向设计一份高质量的Query，"
            "确保其真实可行、可验证且符合层级定义。输出必须是合法的JSON对象，字段说明见用户指令。"
            "禁止捏造引用；引用必须来自本次提供的参考资料或允许范围内的资料。"
            "重要：‘Ground Truth’为评测端内部使用的基准信息（Judge专用），不得在任务要求中出现‘Ground Truth’相关内容，"
            "对外字段一律使用‘参考资料’或‘提供的资料’指代来源；仅在JSON的`ground_truth`对象中保留评测所需信息。"
            f" {SOP_SYSTEM_PRINCIPLES}{extra_system_tail}"
            + (
                " 当前任务为【负向任务】，需要刻意构造一个基于错误前提或不可行约束的挑战，"
                "让执行者通过批判性推理与验证流程得出“不可完成”或“前提有误”的结论。"
                if orientation == "inverse"
                else ""
            )
        ),
    }

    primary = ground_truth_bundle.primary
    gt_primary_lines = [
        f"Ground Truth标题: {primary.title}",
        f"链接: {primary.url}",
        f"摘要: {primary.snippet}",
    ]
    if primary.source:
        gt_primary_lines.append(f"来源: {primary.source}")
    if primary.date:
        gt_primary_lines.append(f"日期: {primary.date}")

    supporting_lines = []
    for idx, source in enumerate(ground_truth_bundle.supporting, start=1):
        supporting_lines.append(
            f"{idx}. {source.title} | {source.url} | {source.snippet or '无摘要'}"
        )

    persona = context_bundle.persona
    constraints_text = "\n".join(f"- {item}" for item in context_bundle.constraints) if context_bundle.constraints else "- 无"
    assets_text = "\n".join(f"- {item}" for item in context_bundle.available_assets) if context_bundle.available_assets else "- 无"
    success_metrics_text = "\n".join(f"- {item}" for item in context_bundle.success_metrics) if context_bundle.success_metrics else "- 无"

    task_focus = "\n".join(f"- {item}" for item in spec.task_focus) if spec.task_focus else "- 结合Ground Truth拆解任务关键步骤。"
    deliver_focus = "\n".join(f"- {item}" for item in spec.deliverable_requirements) if spec.deliverable_requirements else "- 说明交付物格式、长度以及验证要求。"
    evaluation_focus = "\n".join(f"- {item}" for item in spec.evaluation_focus) if spec.evaluation_focus else "- 依据Ground Truth给出可判分的评价要点。"

    orientation_block = (
        f"### 任务正负向\n"
        f"- 类型: {orientation_meta['label']}\n"
        f"- 目标: {orientation_meta['purpose']}\n"
        f"- 成功标准: {orientation_meta['success']}\n"
    )
    if orientation == "inverse":
        orientation_block += f"\n#### 负向任务构造提示\n{orientation_meta['playbook']}\n"

    motivations = ", ".join(persona.motivations) if persona.motivations else "未指定"
    pain_points = ", ".join(persona.pain_points) if persona.pain_points else "未指定"

    persona_section = (
        "### Persona与任务上下文\n"
        f"- Persona: {persona.name}（资历：{persona.seniority}）\n"
        f"- Persona描述: {persona.description}\n"
        f"- 用户陈述: {context_bundle.user_statement}\n"
        f"- 核心动机: {motivations}\n"
        f"- 主要痛点: {pain_points}\n"
        f"- 约束条件:\n{constraints_text}\n"
        f"- 可用资源:\n{assets_text}\n"
        f"- 成功判据:\n{success_metrics_text}\n"
    )

    extra_context_section = ""
    if context_blocks:
        segments: List[str] = []
        for block in context_blocks:
            content = (block.get("content") or "").strip()
            if not content:
                continue
            if len(content) > MAX_CONTEXT_CHARS_PER_DOC:
                content = content[:MAX_CONTEXT_CHARS_PER_DOC].rstrip() + "\n...[内容已截断]"
            label_parts = [block.get("name") or "上下文文档"]
            if block.get("path"):
                label_parts.append(block["path"])
            label = " - ".join(label_parts)
            segments.append(f"#### {label}\n{content}")
        if segments:
            extra_context_section = "### 补充上下文资料\n" + "\n\n".join(segments) + "\n\n"

    industry_block = ""
    if spec.industry or spec.profession:
        industry_block = "### 行业与职业\n"
        if spec.industry:
            industry_block += f"- 行业: {spec.industry}\n"
        if spec.profession:
            industry_block += f"- 职业角色: {spec.profession}\n"
        industry_block += "\n"

    # 命题阶段需要看到评测基准，但必须提醒模型：对外字段不要出现“Ground Truth”。
    ground_truth_section = (
        "### 评测基准\n" 
        + "\n".join(gt_primary_lines)
    )
    if supporting_lines:
        ground_truth_section += "\n参考资料：\n" + "\n".join(f"- {line}" for line in supporting_lines)
    else:
        ground_truth_section += "\n参考资料：无额外参考资料，如必要请在任务中自行搜索。"

    user_message = {
        "role": "user",
        "content": (
            f"{language_instruction}\n\n"
            f"### 任务层级\n"
            f"- Level: {level}（{guideline['nickname']}）\n"
            f"- 核心强调: {guideline['focus']}\n"
            f"- 耗时预期: {guideline['timebox']}\n"
            f"- 工具/流程: {guideline['tooling']}\n"
            f"- 评分重点: {guideline['evaluation']}\n\n"
            f"{industry_block}"
            f"{orientation_block}\n"
            f"### 场景设定\n"
            f"{spec.scenario}\n\n"
            f"{persona_section}"
            f"{extra_context_section}"
            f"### 重点关注\n"
            f"任务拆解:\n{task_focus}\n\n"
            f"交付要求:\n{deliver_focus}\n\n"
            f"评估要点:\n{evaluation_focus}\n\n"
            f"{ground_truth_section}\n\n"
            "请根据上述信息生成一个JSON结构，字段要求如下：\n"
            "{\n"
            '  "query_id": string,\n'
            '  "level": "L3" | "L4" | "L5",\n'
            '  "title": string,\n'
            '  "role_and_background": string,\n'
            '  "task_objectives": [string, ...],\n'
            '  "inputs_and_resources": {\n'
            '      "provided_materials": [string, ...],\n'
            '      "allowed_external_research": string,\n'
            '      "reference_usage": string\n'
            "  },\n"
            '  "deliverables": {\n'
            '      "expected_outputs": [string, ...],\n'
            '      "format_requirements": string,\n'
            '      "quality_bar": string\n'
            "  },\n"
            '  "grading_rubric": [string, ...],\n'
            '  "tool_usage_expectation": string,\n'
            '  "estimated_human_time": string,\n'
            '  "context": {\n'
            '      "persona": {\n'
            '          "id": string,\n'
            '          "name": string,\n'
            '          "seniority": string,\n'
            '          "description": string,\n'
            '          "motivations": [string, ...],\n'
            '          "pain_points": [string, ...]\n'
            "      },\n"
            '      "user_statement": string,\n'
            '      "constraints": [string, ...],\n'
            '      "available_assets": [string, ...],\n'
            '      "success_metrics": [string, ...]\n'
            "  },\n"
            '  "ground_truth": {\n'
            '      "primary": {\n'
            '          "title": string,\n'
            '          "url": string,\n'
            '          "snippet": string,\n'
            '          "source": string | null,\n'
            '          "date": string | null,\n'
            '          "search_query": string | null\n'
            "      },\n"
            '      "supporting": [\n'
            "          {\n"
            '              "title": string,\n'
            '              "url": string,\n'
            '              "snippet": string,\n'
            '              "source": string | null,\n'
            '              "date": string | null,\n'
            '              "search_query": string | null\n'
            "          }, ...\n"
            "      ],\n"
            '      "usage_notes": string\n'
            "  },\n"
            '  "references": [\n'
            "      {\n"
            '          "title": string,\n'
            '          "url": string,\n'
            '          "snippet": string,\n'
            '          "source": string | null,\n'
            '          "date": string | null\n'
            "      }, ...\n"
            "  ],\n"
            '  "standard_answer": {\n'
            '      "summary": string,\n'
            '      "key_points": [string, ...]\n'
            "  },\n"
            '  "evaluation_guide": {\n'
            '      "summary": string,\n'
            '      "checkpoints": [string, ...],\n'
            '      "scoring_rubric": [string, ...]\n'
            "  },\n"
            '  "notes": string\n'
            "}\n\n"
            "要求：\n"
            "1. 三E落实：确保Examining/Executable/Evaluable可落地，交付物与评分Rubric必须可客观验证/量化。\n"
            "2. 角色、组织、业务背景要真实可信，即使场景给出这是智能体的任务也不要说这是给AI布置的任务，而是要改用一个存在的真实职业角色与业务场景\n"
            "3. 交付可评：在deliverables.quality_bar与evaluation_guide中给出可量化标准/阈值（尤其L4需包含与参考资料/基准的一致性指标）。\n"
            "4. 算力与训练红线：任务须training-free，禁止从头训练或长时间/昂贵算力依赖\n"
            "5. 时间窗口：根据参考资料和Ground Truth的发布时间设置资料使用时间窗口，并写入inputs_and_resources.allowed_external_research或notes中。\n"
            "6. 信息源限制：大部分情况不需要限制检索范围，如需限制检索范围，请在inputs_and_resources.reference_usage说明（如‘仅限提供文档库，不开放互联网搜索’）。\n"
            "7. 红线/绿色标准自检：在evaluation_guide.checkpoints列出本级别红线与绿色标准自检项，以便评测Agent判分。\n"
            "8. 输出中不得包含Markdown标记，仅返回纯JSON（必须是json对象）；notes可提示风险与避免泄漏Ground Truth的方法。\n"
            "9. 交付物单一：交付物只能有一个报告和/或一个代码仓库，不得要求提交多份报告；请在deliverables.format_requirements中明确说明。\n"
        ),
    }

    return [system_message, user_message]
