#!/usr/bin/env python3
"""
Generate query specification configs for each profession using the taxonomy and LLM.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from query_agent.llm import OpenAIChatClient, LLMError  # noqa: E402
from query_agent.spec import normalize_search_queries  # noqa: E402
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
SOP_PRINCIPLES = (
    "SOP核心要点："
    "1) 先锁定高质量Ground Truth再逆向命题；"
    "2) 角色与任务必须真实可信，贴近专业人士日常工作；"
    "3) L3=封闭单元任务，主要依赖单一Agent模式（如Claude Code或Deep Research）完成(≤1天)、L4=多日复现，通常依赖单一Agent模式，但需要多轮迭代、系统的信息整合与复现步骤。(≤1周)、L5=战略创新，要求Deep Research与Claude Code等多种能力交替使用，模拟调研-验证循环。(≥1月)；"
    "4) 交付物与评分标准要可用互联网公开资料执行、方便验证；"
)


def load_taxonomy(path: Path) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    industries = {item["industry_id"]: item for item in data.get("industries", [])}
    return industries


def extract_profession_id_from_query(query: Dict[str, Any]) -> Optional[str]:
    if isinstance(query.get("profession_id"), str):
        return query["profession_id"]
    query_id = query.get("query_id")
    if not isinstance(query_id, str):
        return None
    parts = query_id.rsplit("-", 2)
    if parts:
        return parts[0]
    return None


def summarize_existing_tasks(tasks: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
    summary: List[Dict[str, Any]] = []
    for task in tasks[:limit]:
        item: Dict[str, Any] = {
            "query_id": task.get("query_id"),
            "level": task.get("level"),
            "scenario": task.get("scenario"),
        }
        search_value = task.get("search_queries")
        if search_value is None:
            search_value = task.get("search_query")
        if search_value is not None:
            try:
                item["search_queries"] = normalize_search_queries(search_value)
            except Exception:
                pass
        if isinstance(task.get("deliverable_requirements"), list):
            item["deliverable_requirements"] = task["deliverable_requirements"]
        if isinstance(task.get("evaluation_focus"), list):
            item["evaluation_focus"] = task["evaluation_focus"]
        summary.append(item)
    return summary


def build_user_prompt(
    industry: Dict[str, Any],
    profession: Dict[str, Any],
    existing_tasks: Optional[List[Dict[str, Any]]] = None,
) -> str:
    prompt = (
        f"行业：{industry['title']}（{industry['description']}）\n"
        f"职业：{profession['name']}（{profession['description']}）\n\n"
        "请基于上述职业在真实工作场景中的日常任务，设计三条任务说明（L3/L4/L5）。"
        "任务必须符合以下要求：\n"
        "- 围绕真实业务痛点或目标，所有任务必须能够只依赖公开互联网可获取的权威资料（报告、指南、标准、法规、案例等）完成，不得假设任务会提供内部数据库或保密信息。\n"
        "- L3：封闭、人类可在数小时内完成的模块；L4：人类数天内可复现的成果；L5：面向1个月以上的战略或创新规划。\n"
        "- 每个任务必须明确可网上检索到 Ground Truth（报告/标准/案例/代码仓库等），并生成能直接用于搜索引擎的检索词；检索词需覆盖核心信息（具体行业、职业、指标、年份/版本等），避免泛泛而谈，以确保能定位到公开资料。\n"
        "- 交付物最后只有一篇报告\n"
        "- 输出的scenario应描述角色、背景、约束；task_focus列出3-4条要点；deliverable_requirements、evaluation_focus分别给出3项以上具体要求。\n"
        "- 输出JSON，格式为：\n"
        "{\n"
        '  "profession_id": "...",\n'
        '  "profession_name": "...",\n'
        '  "queries": [\n'
        "    {\n"
        '      "level": "L3" | "L4" | "L5",\n'
        '      "search_query": "....",          # 兼容字段，等于 search_queries[0]\n'
        '      "search_queries": ["...","..."], # 1-5 条检索词，按优先级排序\n'
        '      "scenario": "....",\n'
        '      "task_focus": ["..."],\n'
        '      "deliverable_requirements": ["..."],\n'
        '      "evaluation_focus": ["..."]\n'
        "    }, ...\n"
        "  ]\n"
        "}\n"
        "- 禁止输出Markdown或额外解释，仅返回JSON对象。\n"
        "- 搜索关键词可使用中文或英文，但必须指向公开、可信的资料来源，并在任务中强调引用这些资料验证结论。\n"
        "- 保证三条任务的搜索关键词、场景和交付物互不雷同。"
    )
    if existing_tasks:
        prompt += (
            "\n\n已有任务如下（请确保新任务在场景、检索词与交付物上保持差异，避免重复）：\n"
            f"{json.dumps(existing_tasks, ensure_ascii=False, indent=2)}\n"
            "请基于真实工作需求补充新的评估任务。"
        )
    return prompt


def generate_profession_config(
    client: OpenAIChatClient,
    industry: Dict[str, Any],
    profession: Dict[str, Any],
    existing_tasks: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    existing_tasks = existing_tasks or []
    system_message = {
        "role": "system",
        "content": (
            "你是一名资深命题教师，负责设计真实可信、可验证且依托公开资料的评估任务。"
            "所有任务必须使执行者能够通过公开互联网资源收集证据并完成交付，禁止依赖内部或私有数据。"
            "检索词需要帮助定位权威资料，以支撑Ground Truth的比对与引用。"
            f"{SOP_PRINCIPLES} 所有输出必须严格遵循指令并保持JSON合法。"
        ),
    }
    existing_summary = summarize_existing_tasks(existing_tasks)
    user_message = {"role": "user", "content": build_user_prompt(industry, profession, existing_summary)}

    last_error: Exception | None = None
    for attempt in range(3):
        try:
            data = client.run_json_completion([system_message, user_message])
            break
        except LLMError as exc:
            last_error = exc
            wait = 5 * (attempt + 1)
            print(f"[WARN] {profession['profession_id']} 第{attempt+1}次调用失败：{exc}，{wait}s后重试", file=sys.stderr)
            time.sleep(wait)
    else:
        raise RuntimeError(f"LLM生成失败：{profession['profession_id']} - {last_error}") from last_error

    return transform_profession_data(profession, data, existing_tasks)


def transform_profession_data(
    profession: Dict[str, Any],
    data: Dict[str, Any],
    existing_tasks: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    existing_ids: set[str] = set()
    if existing_tasks:
        existing_ids = {
            task["query_id"]
            for task in existing_tasks
            if isinstance(task, dict) and isinstance(task.get("query_id"), str)
        }

    queries = data.get("queries")
    if not isinstance(queries, list) or len(queries) != 3:
        raise ValueError(f"{profession['profession_id']}：必须返回3条任务。")

    expected_levels = {"L3", "L4", "L5"}
    levels = {item.get("level") for item in queries}
    if levels != expected_levels:
        raise ValueError(f"{profession['profession_id']}：返回的层级不完整或重复，得到{levels}")

    converted: List[Dict[str, Any]] = []
    for item in queries:
        level = item.get("level")
        if level not in expected_levels:
            raise ValueError(f"{profession['profession_id']}：未知层级 {level}")
        search_value = item.get("search_queries")
        if search_value is None:
            search_value = item.get("search_query")
        scenario = item.get("scenario")
        search_queries = normalize_search_queries(search_value)
        if not search_queries:
            raise ValueError(f"{profession['profession_id']}：search_query 无效。")
        if not isinstance(scenario, str) or not scenario.strip():
            raise ValueError(f"{profession['profession_id']}：scenario 无效。")

        def ensure_list(field: str) -> List[str]:
            value = item.get(field)
            if not isinstance(value, list) or not value or not all(isinstance(x, str) and x.strip() for x in value):
                raise ValueError(f"{profession['profession_id']}：{field} 需为非空字符串列表。")
            return [x.strip() for x in value]

        query_id = build_query_id(profession["profession_id"], level, existing_ids)

        converted.append(
            {
                "query_id": query_id,
                "level": level,
                "orientation": "positive",
                "language": "zh",
                "search_query": search_queries[0],
                "search_queries": search_queries,
                "scenario": scenario.strip(),
                "profession_id": profession["profession_id"],
                "profession_name": profession["name"],
                "task_focus": ensure_list("task_focus"),
                "deliverable_requirements": ensure_list("deliverable_requirements"),
                "evaluation_focus": ensure_list("evaluation_focus"),
            }
        )
    return converted


def build_query_id(profession_id: str, level: str, existing_ids: set[str]) -> str:
    date_tag = datetime.utcnow().strftime("%Y%m%d")
    base = f"{profession_id}-{level.lower()}-{date_tag}"
    candidate = base
    suffix = 2
    while candidate in existing_ids:
        candidate = f"{base}-v{suffix}"
        suffix += 1
    existing_ids.add(candidate)
    return candidate


def save_industry_configs(
    output_dir: Path,
    industry_id: str,
    existing_queries: List[Dict[str, Any]],
    new_queries: List[Dict[str, Any]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{industry_id}.json"
    data = {"queries": [*existing_queries, *new_queries]}
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_existing_industry_queries(path: Path) -> List[Dict[str, Any]]:
    try:
        raw_text = path.read_text(encoding="utf-8")
        payload = json.loads(raw_text)
    except FileNotFoundError:
        return []
    except Exception as exc:  # pragma: no cover - diagnostics
        print(f"[WARN] 无法解析已有文件 {path}: {exc}", file=sys.stderr)
        return []

    queries = payload.get("queries")
    if not isinstance(queries, list):
        return []
    return [item for item in queries if isinstance(item, dict)]


def filter_tasks_for_profession(
    tasks: List[Dict[str, Any]],
    profession_id: str,
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for task in tasks:
        if not isinstance(task, dict):
            continue
        task_profession_id = extract_profession_id_from_query(task)
        if task_profession_id == profession_id:
            filtered.append(task)
    return filtered


def generate_additional_tasks(
    client: OpenAIChatClient,
    industry: Dict[str, Any],
    profession: Dict[str, Any],
    existing_tasks: List[Dict[str, Any]],
    target_count: int,
) -> List[Dict[str, Any]]:
    target = max(target_count, 0)
    if len(existing_tasks) >= target:
        return []

    tasks_so_far = list(existing_tasks)
    new_tasks: List[Dict[str, Any]] = []

    while len(tasks_so_far) < target:
        generated = generate_profession_config(client, industry, profession, tasks_so_far)
        if not generated:
            raise RuntimeError(f"{profession['profession_id']}：LLM 未返回任何任务，无法达到目标条数。")
        tasks_so_far.extend(generated)
        new_tasks.extend(generated)

    return new_tasks


def _generate_with_client(
    industry: Dict[str, Any],
    profession: Dict[str, Any],
    existing_tasks: Optional[List[Dict[str, Any]]] = None,
    target_count: int = 3,
) -> Tuple[str, List[Dict[str, Any]], int]:
    client = OpenAIChatClient()
    try:
        tasks = existing_tasks or []
        existing_len = len(tasks)
        new_tasks = generate_additional_tasks(client, industry, profession, tasks, target_count)
        return profession["profession_id"], new_tasks, existing_len
    finally:
        # explicit close hook not provided; rely on GC
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate profession-level configs via LLM.")
    parser.add_argument("--output-dir", type=Path, default=Path("configs/generated"), help="输出目录（默认：configs/generated）")
    parser.add_argument("--industries", nargs="*", help="需要生成的行业ID（默认全部）")
    parser.add_argument("--overwrite", action="store_true", help="若目标文件已存在是否覆盖")
    parser.add_argument("--incremental", action="store_true", help="在已有行业文件基础上追加新任务并避免重复")
    parser.add_argument(
        "--target-per-profession",
        type=int,
        default=3,
        help="每个职业目标任务条数（默认3，按3条一组追加）",
    )
    parser.add_argument("--taxonomy", type=Path, default=Path("configs/taxonomy.json"), help="行业-职业taxonomy文件路径（默认：configs/taxonomy.json）")
    parser.add_argument("--max-workers", type=int, default=16, help="并发调用的最大线程数（默认：16）")
    parser.add_argument("--limit", type=int, help="仅生成前N个职业（跨行业累计），便于测试。")
    args = parser.parse_args()

    industries = load_taxonomy(args.taxonomy)
    target_ids = args.industries or list(industries.keys())

    max_workers = max(1, args.max_workers)
    shared_client: OpenAIChatClient | None = None
    if max_workers == 1:
        shared_client = OpenAIChatClient()

    remaining_limit = args.limit if args.limit and args.limit > 0 else None
    industry_persist_existing: Dict[str, List[Dict[str, Any]]] = {}
    industry_output_map: Dict[str, Path] = {}
    industry_new_queries: Dict[str, List[Dict[str, Any]]] = {}
    jobs: List[Tuple[str, Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]] = []

    for industry_id in target_ids:
        if remaining_limit is not None and remaining_limit <= 0:
            print("[INFO] 已达到limit上限，停止后续行业。", file=sys.stderr)
            break

        industry = industries.get(industry_id)
        if industry is None:
            print(f"[WARN] 未找到行业 {industry_id}，跳过。", file=sys.stderr)
            continue

        output_file = args.output_dir / f"{industry_id}.json"
        industry_output_map[industry_id] = output_file

        existing_industry_queries: List[Dict[str, Any]] = []
        if output_file.exists():
            if not args.overwrite and not args.incremental:
                print(f"[INFO] {output_file} 已存在，使用 --overwrite 或 --incremental 可继续。", file=sys.stderr)
                continue
            if args.incremental:
                existing_industry_queries = load_existing_industry_queries(output_file)

        industry_persist_existing[industry_id] = existing_industry_queries if args.incremental else []
        industry_new_queries[industry_id] = []

        professions = industry.get("professions", [])

        for profession in professions:
            if remaining_limit is not None and remaining_limit <= 0:
                break
            if args.incremental:
                existing_tasks = filter_tasks_for_profession(existing_industry_queries, profession["profession_id"])
            else:
                existing_tasks = []
            jobs.append((industry_id, industry, profession, existing_tasks))
            if remaining_limit is not None:
                remaining_limit -= 1

    if not jobs:
        print("[INFO] 没有需要生成的职业任务，流程结束。")
        return

    print(f"[INFO] 待生成职业数：{len(jobs)}，使用线程数：{max_workers}")

    if max_workers == 1:
        assert shared_client is not None
        for industry_id, industry, profession, existing_tasks in jobs:
            print(f"[INFO] 生成 {industry_id} / {profession['profession_id']} ...")
            prof_queries = generate_additional_tasks(
                shared_client, industry, profession, existing_tasks, args.target_per_profession
            )
            if prof_queries:
                total_count = len(existing_tasks) + len(prof_queries)
                print(
                    f"[INFO] {industry_id} / {profession['profession_id']} 新增 {len(prof_queries)} 条，"
                    f"累计 {total_count} 条（目标≥{args.target_per_profession}）。"
                )
                industry_new_queries[industry_id].extend(prof_queries)
            else:
                print(
                    f"[INFO] {industry_id} / {profession['profession_id']} 已有 {len(existing_tasks)} 条，"
                    f"满足目标 {args.target_per_profession}，跳过新增。"
                )
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_job = {
                executor.submit(
                    _generate_with_client,
                    industry,
                    profession,
                    existing_tasks,
                    args.target_per_profession,
                ): (industry_id, profession["profession_id"], len(existing_tasks))
                for industry_id, industry, profession, existing_tasks in jobs
            }
            for future in as_completed(future_to_job):
                industry_id, prof_id, existing_len = future_to_job[future]
                try:
                    pid, prof_queries, recorded_existing = future.result()
                    # recorded_existing equals existing_len, but use returned value for correctness
                    existing_len = recorded_existing
                    if prof_queries:
                        total_count = existing_len + len(prof_queries)
                        print(
                            f"[INFO] 完成 {industry_id} / {pid}，新增 {len(prof_queries)} 条，"
                            f"累计 {total_count} 条（目标≥{args.target_per_profession}）。"
                        )
                        industry_new_queries[industry_id].extend(prof_queries)
                    else:
                        print(
                            f"[INFO] 完成 {industry_id} / {pid}，已有 {existing_len} 条，"
                            f"满足目标 {args.target_per_profession}，跳过新增。"
                        )
                except Exception as exc:
                    print(f"[ERROR] 生成 {industry_id} / {prof_id} 失败：{exc}", file=sys.stderr)
                    raise

    for industry_id, new_queries in industry_new_queries.items():
        output_file = industry_output_map[industry_id]
        persisted_existing = industry_persist_existing[industry_id]
        if new_queries:
            save_industry_configs(args.output_dir, industry_id, persisted_existing, new_queries)
            print(
                f"[INFO] 已生成 {output_file}（新增 {len(new_queries)} 条，"
                f"历史保留 {len(persisted_existing)} 条，总计 {len(new_queries) + len(persisted_existing)} 条）"
            )
        else:
            print(f"[INFO] {industry_id} 未生成新增任务，保持原有 {len(persisted_existing)} 条配置。")


if __name__ == "__main__":
    main()
