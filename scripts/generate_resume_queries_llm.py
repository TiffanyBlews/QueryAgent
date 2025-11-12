#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Use an LLM to extract project experiences from resumes and build query seeds."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import re
import sys
import threading
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests


TITLE_DATE_RE = re.compile(r"^\s*(\d{4}[.年/-]\d{1,2}.*)$")


def slugify(text: str, max_len: int = 64) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", (text or "").strip()).strip("-").lower()
    if not cleaned:
        return "item"
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip("-")
    return cleaned or "item"


def guess_profession_from_filename(name: str) -> Tuple[str, str]:
    m = re.search(r"【([^】]+)】", name)
    role = m.group(1) if m else name
    role_title = role.split("_")[0].strip()
    profession = role_title or "项目经历候选人"
    keywords = (role_title + name).lower()
    if any(k in keywords for k in ("算法", "大语言模型", "nlp", "ai", "算法")):
        industry = "信息技术"
    elif any(k in keywords for k in ("全栈", "开发", "web", "工程师")):
        industry = "信息技术"
    elif any(k in keywords for k in ("行研", "研究", "科研", "咨询")):
        industry = "科研/咨询"
    elif any(k in keywords for k in ("投资", "创业", "关系")):
        industry = "创投/运营"
    else:
        industry = "通用"
    return industry, profession


def choose_level(text: str) -> str:
    lowered = text.lower()
    keywords_l4 = (
        "强化学习",
        "reinforcement",
        "rl",
        "ppo",
        "dpo",
        "rag",
        "检索增强",
        "向量库",
        "faiss",
        "多模态",
        "agent",
        "mcp",
        "workflow",
    )
    return "L4" if any(k in lowered for k in keywords_l4) else "L3"


def build_search_query(title: str, keywords: Sequence[str]) -> str:
    base = re.sub(r"[，。、《》()（）——\\-]+", " ", title).strip()
    suffix = "最佳实践 标准流程 case study 2024"
    kw = " ".join(keywords[:3]) if keywords else ""
    components = [base, kw, suffix]
    return " ".join(filter(None, components))


def normalize_search_queries(raw: object) -> List[str]:
    if raw is None:
        return []

    queries: List[str] = []
    splitter = re.compile(r"[;,，；]+")

    def _push(value: Optional[str]) -> None:
        if not value:
            return
        cleaned = value.strip()
        if cleaned:
            queries.append(cleaned)

    if isinstance(raw, str):
        for part in splitter.split(raw):
            _push(part)
    elif isinstance(raw, Sequence) and not isinstance(raw, (bytes, bytearray)):
        for item in raw:
            if isinstance(item, str):
                for part in splitter.split(item):
                    _push(part)
            else:
                _push(str(item))
    else:
        _push(str(raw))

    deduped: List[str] = []
    seen: set[str] = set()
    for q in queries:
        if q not in seen:
            seen.add(q)
            deduped.append(q)
    return deduped


def load_existing_queries(out_path: Path, seen_ids: Set[str]) -> List[Dict[str, object]]:
    if not out_path.exists():
        return []
    try:
        raw_text = out_path.read_text(encoding="utf-8")
        data = json.loads(raw_text)
    except Exception as exc:
        print(f"[WARN] 无法读取已有查询 {out_path}: {exc}", file=sys.stderr)
        return []

    items = data.get("queries")
    if not isinstance(items, list):
        return []

    loaded: List[Dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        qid = item.get("query_id")
        if not isinstance(qid, str) or not qid:
            continue
        if qid in seen_ids:
            continue
        seen_ids.add(qid)
        loaded.append(item)
    return loaded


def write_query_outputs(out_path: Path, achievable_out: Path, queries: Sequence[Dict[str, object]]) -> None:
    queries_list = list(queries)
    payload = {"queries": queries_list}

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = out_path.with_name(out_path.name + ".tmp")
    tmp_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_out.replace(out_path)

    achievable_queries = [q for q in queries_list if q.get("is_achieveable", False)]
    achievable_payload = {"queries": achievable_queries}
    achievable_out.parent.mkdir(parents=True, exist_ok=True)
    tmp_achievable = achievable_out.with_name(achievable_out.name + ".tmp")
    tmp_achievable.write_text(json.dumps(achievable_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_achievable.replace(achievable_out)


def default_resume_dirs(root: Path) -> List[Path]:
    candidates = [root, root / "new"]
    seen: Set[Path] = set()
    dirs: List[Path] = []
    for p in candidates:
        resolved = p.resolve()
        if resolved.exists() and resolved.is_dir() and resolved not in seen:
            dirs.append(resolved)
            seen.add(resolved)
    return dirs


def get_resume_files(dirs: Iterable[Path]) -> List[Path]:
    seen: Set[Path] = set()
    files: List[Path] = []
    for base in dirs:
        for path in sorted(base.rglob("*.md")):
            if path.is_file() and path not in seen:
                files.append(path)
                seen.add(path)
    return files


def select_project_related_text(text: str) -> str:
    lines = text.splitlines()
    selected: List[str] = []

    # 1) Capture sections explicitly labelled as project-related (Markdown style headings)
    capture = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('#'):
            heading = stripped.strip('#').strip()
            capture = bool(re.search(r"项目|Project|科研|竞赛|比赛", heading, re.IGNORECASE))
            continue
        if capture:
            selected.append(line)

    # 2) If still short, gather paragraphs containing project keywords
    if len("\n".join(selected)) < 200:
        keyword_lines: List[str] = []
        for idx, line in enumerate(lines):
            if re.search(r"项目|课题|Project|研发|竞赛", line, re.IGNORECASE):
                keyword_lines.append(line)
                for offset in range(1, 4):
                    if idx + offset < len(lines):
                        keyword_lines.append(lines[idx + offset])
        if keyword_lines:
            selected = keyword_lines

    snippet = "\n".join(selected).strip()
    if len(snippet) < 200:
        return text
    if len(snippet) > 2500:
        snippet = snippet[:2500]
    return snippet


class LLMExtractor:
    def __init__(
        self,
        model: str,
        base_url: str,
        api_key: str,
        timeout: float = 400.0,
        max_tokens: int = 32000,
    ) -> None:
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.max_tokens = max_tokens
        self._local = threading.local()

    def _get_session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            self._local.session = session
        return session

    def extract(self, resume_text: str) -> List[Dict[str, object]]:
        snippet = select_project_related_text(resume_text)
        system_prompt = (
            "你是资深HR分析师兼SOP任务设计师，负责从中文简历中提炼可评测的项目任务，并尝试让大模型智能体独立完成。"
            "对简历中出现的公司、人名需脱敏（使用‘某公司’‘某团队’等），且不可泄露候选人隐私。"
            "只保留真实的项目经历/项目经验/科研项目/比赛项目/实习项目，不得输出岗位职责、教育、证书或奖项。"
            "必须输出 JSON 对象，结构严格为：{\"projects\": [{\"title\": str, \"timeframe\": str, \"summary\": str, \"level\": \"L3\" | \"L4\" | \"L5\", \"scenario\": str, \"task_focus\": [str], \"deliverable_requirements\": [str], \"evaluation_focus\": [str], \"is_achieveable\": bool, \"search_queries\": [str], \"bullets\": [str], \"keywords\": [str]}]}。"
            "所有字段必须存在（缺省时给空字符串或空数组），且禁止返回额外字段或非JSON文本。"
        )
        user_prompt = (
            "请读取以下简历内容，定位所有项目经历，并按照指定JSON结构输出。\n"
            "- timeframe：尽量精确（无法确定可留空）。\n"
            "- summary：概括项目目标、背景、成果。\n"
            "- level：L3：封闭、人类可在数小时内完成的模块；L4：人类数天内可复现的成果；L5：面向1个月以上的战略或创新规划。\n"
            "- scenario：撰写面向智能体的任务背景，包含角色、目标、限制，避免私人信息。\n"
            "- task_focus：2-4条行动要点，突出可执行步骤。\n"
            "- deliverable_requirements：1-3条交付要求，强调结构化输出与引用规范。\n"
            "- evaluation_focus：1-3条评估标准，关注可验证性、完整性、风险控制。\n"
            "- is_achieveable：大模型智能体如果被输入这个任务的话能否只使用互联网公开资料完成，如果可以则输出True，否则输出False。\n"
            "- search_queries：长度1-3的检索词列表，可含权威机构/政策/年份，用来让大模型智能体完成任务时检索相应资料\n"
            "- bullets：最多4条项目亮点或成果。\n"
            "- keywords：≤3个关键词（技术栈/工具/领域）。\n"
            "- 若无项目经历，可返回空数组。\n"
            "简历原文：\n" + snippet
        )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.2,
            "max_tokens": self.max_tokens,
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        url = f"{self.base_url}/chat/completions"
        session = self._get_session()
        resp = session.post(url, headers=headers, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        finish_reason = data["choices"][0].get("finish_reason")
        if finish_reason == "length" and not content:
            raise ValueError("LLM输出因长度限制被截断，请增加max_tokens或缩短输入。")
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as exc:
            raise ValueError(f"LLM返回内容无法解析为JSON: {exc}\n原始内容: {content[:200]}...") from exc
        projects = parsed.get("projects")
        if not isinstance(projects, list):
            return []
        normalized: List[Dict[str, object]] = []
        for item in projects:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            summary = str(item.get("summary") or "").strip()
            if not title:
                continue
            timeframe = str(item.get("timeframe") or "").strip()
            level = str(item.get("level") or "").strip().upper()
            scenario = str(item.get("scenario") or "").strip()

            def _norm_list(value) -> List[str]:
                if isinstance(value, list):
                    return [str(v).strip() for v in value if str(v).strip()]
                if isinstance(value, str) and value.strip():
                    return [value.strip()]
                return []

            def _norm_bool(value) -> bool:
                if isinstance(value, bool):
                    return value
                if isinstance(value, (int, float)):
                    return bool(value)
                if isinstance(value, str):
                    lowered = value.strip().lower()
                    if lowered in {"true", "yes", "y", "1"}:
                        return True
                    if lowered in {"false", "no", "n", "0"}:
                        return False
                return False

            task_focus = _norm_list(item.get("task_focus"))
            deliverables = _norm_list(item.get("deliverable_requirements"))
            evaluation = _norm_list(item.get("evaluation_focus"))
            bullets = _norm_list(item.get("bullets"))
            keywords = _norm_list(item.get("keywords"))
            search_queries = normalize_search_queries(item.get("search_queries") or item.get("search_query"))
            is_achieveable = _norm_bool(item.get("is_achieveable"))

            normalized.append(
                {
                    "title": title,
                    "summary": summary,
                    "timeframe": timeframe,
                    "level": level,
                    "scenario": scenario,
                    "task_focus": task_focus,
                    "deliverable_requirements": deliverables,
                    "evaluation_focus": evaluation,
                    "search_queries": search_queries,
                    "bullets": bullets,
                    "keywords": keywords,
                    "search_query": search_queries[0] if search_queries else "",
                    "is_achieveable": is_achieveable,
                }
            )
        return normalized


def build_query_entry(
    file_path: Path,
    profession: str,
    industry: str,
    project: Dict[str, object],
) -> Dict[str, object]:
    title = str(project.get("title") or "").strip()
    summary = str(project.get("summary") or "").strip()
    timeframe = str(project.get("timeframe") or "").strip()

    def _norm_list(value) -> List[str]:
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        if isinstance(value, str) and value.strip():
            return [value.strip()]
        return []

    bullets = _norm_list(project.get("bullets"))
    keywords = _norm_list(project.get("keywords"))

    level_raw = str(project.get("level") or "").strip().upper()
    if level_raw in {"L3", "L4"}:
        level = level_raw
    else:
        text_for_level = " ".join([title, summary, " ".join(bullets), " ".join(keywords)])
        level = choose_level(text_for_level)

    search_queries = normalize_search_queries(
        project.get("search_queries") or project.get("search_query")
    )
    if not search_queries:
        search_queries = [build_search_query(title, keywords)]
    search_query = search_queries[0]

    scenario = str(project.get("scenario") or "").strip()
    if not scenario:
        scenario_parts = [f"你是{profession}，需要围绕‘{title}’交付完整项目复盘。"]
        if timeframe:
            scenario_parts.append(f"项目时间：{timeframe}。")
        if summary:
            scenario_parts.append(f"背景概述：{summary}")
        scenario_parts.append("请结合公开、可验证资料，形成符合SOP的执行方案与验收标准。")
        scenario = " ".join(scenario_parts)

    task_focus = _norm_list(project.get("task_focus"))
    if not task_focus:
        task_focus = [
            "梳理项目目标、关键步骤与依赖，形成执行清单。",
            "列出主要风险、验证指标与复盘要点。",
        ]

    deliverable_requirements = _norm_list(project.get("deliverable_requirements"))
    if not deliverable_requirements:
        deliverable_requirements = [
            "提交结构化主文档（背景/方法/成果/风险/复盘），并附可验证引用。",
            "提供执行清单与验收标准，必要时附脚本/伪代码支持复核。",
        ]

    evaluation_focus = _norm_list(project.get("evaluation_focus"))
    if not evaluation_focus:
        evaluation_focus = [
            "可验证性：结论需有公开来源支撑，并能复核。",
            "完整性：覆盖目标、路径、风险、验收指标。",
            "可执行性：步骤明确、资源/依赖合理、时间盒清晰。",
        ]

    query_id = slugify(f"{file_path.stem}-{title}")

    is_achieveable_raw = project.get("is_achieveable")
    if isinstance(is_achieveable_raw, bool):
        is_achieveable = is_achieveable_raw
    elif isinstance(is_achieveable_raw, (int, float)):
        is_achieveable = bool(is_achieveable_raw)
    elif isinstance(is_achieveable_raw, str):
        lowered = is_achieveable_raw.strip().lower()
        is_achieveable = lowered in {"true", "yes", "y", "1"}
    else:
        is_achieveable = False

    entry = {
        "query_id": query_id,
        "level": level,
        "language": "zh",
        "search_query": search_query,
        "search_queries": search_queries,
        "scenario": scenario,
        "task_focus": task_focus,
        "deliverable_requirements": deliverable_requirements,
        "evaluation_focus": evaluation_focus,
        "notes": f"来源：{file_path.name}",
        "industry": industry,
        "profession": profession,
        "is_achieveable": is_achieveable,
    }

    return entry


def process_resume(
    path: Path,
    extractor: LLMExtractor,
    profession: str,
    industry: str,
) -> List[Dict[str, object]]:
    try:
        text = path.read_text(encoding="utf-8")
    except Exception as exc:
        print(f"[WARN] 读取失败 {path}: {exc}", file=sys.stderr)
        return []

    projects: List[Dict[str, object]]
    try:
        projects = extractor.extract(text)
    except Exception as exc:
        print(f"[WARN] LLM 解析失败 {path}: {exc}", file=sys.stderr)
        return []

    entries: List[Dict[str, object]] = []
    seen_titles: Set[str] = set()
    for proj in projects:
        title = proj.get("title")
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        entries.append(build_query_entry(path, profession, industry, proj))
    return entries


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate resume-derived query seeds via LLM extraction.")
    parser.add_argument(
        "--resume-dir",
        action="append",
        type=Path,
        help="Resume directories (repeatable). Defaults to 现有目录及其 new/ 子目录。",
    )
    parser.add_argument("--out", type=Path, default=Path("configs/generated/resume_queries.json"))
    parser.add_argument("--max-workers", type=int, default=16, help="Maximum concurrent LLM calls.")
    parser.add_argument("--model", type=str, default=os.environ.get("MODEL", ""))
    parser.add_argument(
        "--openai-base-url",
        type=str,
        default=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
    )
    parser.add_argument(
        "--openai-api-key",
        type=str,
        default=os.environ.get("OPENAI_API_KEY", ""),
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=int(os.environ.get("LLM_RESPONSE_MAX_TOKENS", "32000")),
        help="Max tokens per LLM completion (default 3200).",
    )
    args = parser.parse_args()

    if not args.model:
        print("[ERROR] MODEL 未设置（可在环境变量或 --model 指定）。", file=sys.stderr)
        sys.exit(2)
    if not args.openai_api_key:
        print("[ERROR] OPENAI_API_KEY 未设置。", file=sys.stderr)
        sys.exit(2)

    root = Path("行研信号模型评测组简历筛选")
    if args.resume_dir:
        dirs = []
        seen: Set[Path] = set()
        for d in args.resume_dir:
            resolved = d.resolve()
            if resolved.exists() and resolved.is_dir() and resolved not in seen:
                dirs.append(resolved)
                seen.add(resolved)
    else:
        dirs = default_resume_dirs(root)
        if not dirs and root.exists():
            dirs = [root.resolve()]

    if not dirs:
        print("[WARN] 未找到有效简历目录。", file=sys.stderr)

    files = get_resume_files(dirs)
    if not files:
        print("[WARN] 未找到任何简历文件。", file=sys.stderr)

    extractor = LLMExtractor(
        model=args.model,
        base_url=args.openai_base_url,
        api_key=args.openai_api_key,
        max_tokens=args.max_tokens,
    )

    achievable_out = args.out.with_name(args.out.stem + "_achievable" + args.out.suffix)

    queries: List[Dict[str, object]] = []
    seen_ids: Set[str] = set()
    queries_lock = threading.Lock()
    seen_lock = threading.Lock()

    loaded_queries = load_existing_queries(args.out, seen_ids)
    if loaded_queries:
        queries.extend(loaded_queries)
        print(f"[INFO] 已加载已有查询 {len(queries)} 条，将继续追加。")

    def write_outputs() -> None:
        with queries_lock:
            snapshot = list(queries)
        write_query_outputs(args.out, achievable_out, snapshot)

    def _worker(path: Path) -> List[Dict[str, object]]:
        industry, profession = guess_profession_from_filename(path.name)
        entries = process_resume(path, extractor, profession, industry)
        valid: List[Dict[str, object]] = []
        for entry in entries:
            qid = entry["query_id"]
            with seen_lock:
                if qid in seen_ids:
                    continue
                seen_ids.add(qid)
            valid.append(entry)
        return valid

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(_worker, path): path for path in files}
        for fut in concurrent.futures.as_completed(futures):
            path = futures[fut]
            try:
                entries = fut.result()
            except Exception as exc:
                print(f"[WARN] 处理失败 {path}: {exc}", file=sys.stderr)
                continue
            if entries:
                with queries_lock:
                    queries.extend(entries)
                    current_total = len(queries)
                write_outputs()
                print(f"[INFO] 已处理 {path.name}，新增 {len(entries)} 条（累计 {current_total} 条）。")

    write_outputs()

    with queries_lock:
        total = len(queries)
        achievable_count = sum(1 for q in queries if q.get("is_achieveable", False))
    not_achievable_count = total - achievable_count
    print(f"分布情况: 总查询数: {total}, 可实现 (is_achieveable=True): {achievable_count}, 不可实现: {not_achievable_count}")
    print(f"所有查询输出到: {args.out}")
    print(f"可实现查询输出到: {achievable_out}")


if __name__ == "__main__":
    main()
