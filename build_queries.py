#!/usr/bin/env python3
"""
CLI entry point for the SOP-based query construction agent.
"""

from __future__ import annotations

import argparse
import os
import json
import logging
from datetime import datetime
from pathlib import Path
import shutil
from typing import Iterable, List

from query_agent.agent import QueryConstructionAgent, generate_batch
from query_agent.config_loader import load_specs
from query_agent.context_loader import load_context_blocks
from query_agent.inverse_utils import expand_with_inverse_specs
from query_agent.llm import OpenAIChatClient, LLMError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate AgencyBench-style queries via SOP V7.0.")
    parser.add_argument("--config", required=True, type=Path, help="Path to query specification YAML/JSON.")
    parser.add_argument("--output", required=True, type=Path, help="Output file path (JSONL).")
    parser.add_argument("--serper-endpoint", default="https://google.serper.dev/search", help="Serper API endpoint.")
    parser.add_argument("--market", default="us", help="Serper market code (gl).")
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    parser.add_argument(
        "--industry",
        action="append",
        help="Filter to specific industries (repeatable, case-insensitive).",
    )
    parser.add_argument(
        "--profession",
        action="append",
        help="Filter to specific professions (repeatable, case-insensitive).",
    )
    parser.add_argument(
        "--task-id",
        dest="task_ids",
        action="append",
        help="Filter to specific task identifiers from the profession config.",
    )
    parser.add_argument(
        "--level",
        action="append",
        choices=["L3", "L4", "L5"],
        help="Filter to specific task levels.",
    )
    parser.add_argument(
        "--max-per-profession",
        type=int,
        help="Maximum queries to keep per profession after filtering.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit total number of queries after filtering.",
    )
    parser.add_argument(
        "--package-dir",
        type=Path,
        help="Optional directory to store packaged queries (with ground truth downloads and references).",
    )
    parser.add_argument(
        "--skip-downloads",
        action="store_true",
        help="Skip downloading ground-truth and reference artifacts when packaging queries.",
    )
    parser.add_argument(
        "--split-views",
        action="store_true",
        help="Also emit solver_query.json (hides ground_truth/standard_answer). Default: off.",
    )
    parser.add_argument(
        "--no-inverse",
        action="store_true",
        help="Do not auto-generate inverse (negative orientation) variants.",
    )
    # offline 选项已移除：统一改为仅调用大模型生成 query
    parser.add_argument(
        "--context",
        dest="context_paths",
        action="append",
        type=Path,
        help="Optional context file or directory to append to the prompt (repeatable).",
    )
    parser.add_argument(
        "--run-tag",
        help="Optional custom run tag. Defaults to UTC timestamp (YYYYMMDD-HHMMSS) so repeated runs do not overwrite outputs.",
    )
    parser.add_argument(
        "--emit-txt",
        action="store_true",
        help="After generating queries, also emit plain-text task descriptions (one aggregated .txt and, if packaging, per-task task.txt).",
    )
    parser.add_argument(
        "--txt-dir",
        type=Path,
        help="Optional directory to write aggregated .txt file. Defaults to the output JSONL directory.",
    )
    # Slim-package options (enabled by default): after writing packages, also emit a
    # mirrored, simplified copy that only contains: task.txt, data_room/, ground_truth/
    parser.add_argument(
        "--disable-slim",
        action="store_true",
        help="Disable emitting slim copies of packages (task.txt, data_room/, ground_truth/) under --slim-base-dir.",
    )
    parser.add_argument(
        "--slim-base-dir",
        type=Path,
        default=Path("final_packages"),
        help="Base directory to write slim package copies. Default: final_packages/",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        help="Maximum worker threads for query generation (default from QUERY_AGENT_MAX_WORKERS or 1).",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Skip specs whose query_id already exists in previous output files for this config.",
    )
    return parser.parse_args()


def _discover_existing_output_files(base_path: Path) -> List[Path]:
    """
    Given the CLI --output path (file or directory), discover historical JSONL outputs
    that should be inspected for already-generated query IDs.
    """
    candidates: List[Path] = []
    try:
        if base_path.suffix:
            parent = base_path.parent
            suffix = base_path.suffix
            stem = base_path.stem
            baseline = parent / base_path.name
            if baseline.exists():
                candidates.append(baseline)
            pattern = f"{stem}_*{suffix}"
            for path in sorted(parent.glob(pattern)):
                if path.exists():
                    candidates.append(path)
            # Also look into run_* subdirectories (e.g., output/run_20241014/foo.jsonl)
            for run_dir in sorted(parent.glob("run_*")):
                if not run_dir.is_dir():
                    continue
                candidate = run_dir / base_path.name
                if candidate.exists():
                    candidates.append(candidate)
        else:
            if base_path.exists() and base_path.is_dir():
                candidates.extend(sorted(base_path.glob("*.jsonl")))
                for run_dir in sorted(base_path.glob("run_*")):
                    if run_dir.is_dir():
                        candidates.extend(sorted(run_dir.glob("*.jsonl")))
    except OSError as exc:
        logging.warning("Failed to list existing output files for incremental mode: %s", exc)
    return candidates


def _load_existing_query_ids(paths: Iterable[Path]) -> set[str]:
    existing_ids: set[str] = set()
    for path in paths:
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line_no, line in enumerate(fh, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        logging.warning("Skipping malformed JSON in %s (line %d)", path, line_no)
                        continue
                    query_id = payload.get("query_id")
                    if isinstance(query_id, str) and query_id.strip():
                        existing_ids.add(query_id.strip())
        except OSError as exc:
            logging.warning("Failed to read existing output %s: %s", path, exc)
    return existing_ids


def apply_filters(specs, args: argparse.Namespace):
    if not specs:
        return []

    industries = {item.lower() for item in args.industry} if args.industry else None
    professions = {item.lower() for item in args.profession} if args.profession else None
    task_ids = {item.lower() for item in args.task_ids} if getattr(args, "task_ids", None) else None
    levels = {item.upper() for item in args.level} if args.level else None

    per_profession_counter: dict[str, int] = {}
    filtered = []
    for spec in specs:
        profession_key = (spec.profession or "unknown").lower()
        industry_key = (spec.industry or "unknown").lower()

        if industries and industry_key not in industries:
            continue
        if professions and profession_key not in professions:
            continue
        if levels and spec.normalized_level() not in levels:
            continue
        if task_ids:
            task_id = str(spec.task_metadata.get("task_id") or spec.query_id).lower()
            if task_id not in task_ids:
                continue

        current_count = per_profession_counter.get(profession_key, 0)
        if args.max_per_profession and current_count >= args.max_per_profession:
            continue

        per_profession_counter[profession_key] = current_count + 1
        filtered.append(spec)
        if args.limit and len(filtered) >= args.limit:
            break

    return filtered


def _maybe_rewrite_search_queries_with_llm(specs):
    """
    Optionally rebuild search_query via LLM for the already filtered/expanded specs.
    Enabled when environment variable LLM_SEARCH_QUERY is true-ish.
    """
    mode = (os.environ.get("LLM_REWRITE_SEARCH_QUERY", "0") or "").lower()
    if mode not in ("1", "true", "yes", "on"):
        return specs
    try:
        client = OpenAIChatClient()
    except LLMError:
        return specs

    def _rewrite_for(spec):
        baseline = spec.search_query
        md = spec.task_metadata or {}
        profession = spec.profession or "职业"
        category = md.get("category") or ""
        theme = md.get("theme_id") or ""
        tags = ", ".join((md.get("focus_tags") or [])[:4])
        scenario = spec.scenario or ""
        system = (
            "你是信息检索与证据搜集的研究助理。根据职业与任务场景，为中文网络环境构造高命中率的搜索query。"
            "目标：更快找到权威、可验证的标准/指南/流程/监管/案例类资料；优先PDF、政府/学术/标准组织来源。"
        )
        user = (
            f"职业：{profession}\n任务类别：{category}\n主题：{theme}\n标签：{tags}\n场景：{scenario[:400]}\n\n"
            f"基线示例（不要原样返回）：{baseline}\n"
            "请返回 JSON：{\"queries\": [\"...\"]}，长度1-2条，按优先级排序。\n"
            "要求：中文关键词为主，可含英文同义词；偏好 标准/规范/指南/政策/PDF/案例；包含近年范围（如2022..2025）；只返回JSON。"
        )
        try:
            data = client.run_json_completion([
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ])
            queries = data.get("queries") if isinstance(data, dict) else None
            if isinstance(queries, list) and queries:
                for q in queries:
                    s = str(q or "").strip()
                    if s:
                        return s
        except LLMError:
            return baseline
        return baseline

    for spec in specs:
        try:
            rewritten = _rewrite_for(spec)
            if not rewritten:
                continue
            tail = spec.search_queries[1:] if len(spec.search_queries) > 1 else []
            spec.search_query = [rewritten, *tail]
        except Exception:
            # Keep baseline on any failure
            pass
    return specs


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="[%(levelname)s] %(message)s")
    run_tag = args.run_tag or datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    logging.info("Run tag: %s", run_tag)

    output_base = args.output
    output_path = output_base
    if output_base.suffix:
        output_path = output_base.with_name(f"{output_base.stem}_{run_tag}{output_base.suffix}")
    else:
        output_path = output_base / f"{run_tag}.jsonl"
    existing_output_files = _discover_existing_output_files(output_base) if args.incremental else []

    specs = load_specs(args.config)
    if not specs:
        raise SystemExit("No query specifications found in config.")
    filtered_specs = apply_filters(specs, args)
    if not filtered_specs:
        logging.warning("No specs remain after applying filters; exiting.")
        return
    if len(filtered_specs) != len(specs):
        logging.info("Filtered specs from %d to %d based on CLI constraints.", len(specs), len(filtered_specs))
    specs = filtered_specs

    if args.no_inverse:
        expanded_specs = specs
        logging.info("Loaded %d specs; inverse generation disabled.", len(expanded_specs))
    else:
        expanded_specs = expand_with_inverse_specs(specs)
        logging.info(
            "Loaded %d base specs; expanded to %d tasks after adding inverse variants where applicable.",
            len(specs),
            len(expanded_specs),
        )

    # Optionally rewrite queries with LLM (only for the specs we will actually process)
    expanded_specs = _maybe_rewrite_search_queries_with_llm(expanded_specs)

    if args.incremental:
        existing_query_ids = _load_existing_query_ids(existing_output_files)
        if existing_query_ids:
            before = len(expanded_specs)
            expanded_specs = [spec for spec in expanded_specs if spec.query_id not in existing_query_ids]
            skipped = before - len(expanded_specs)
            logging.info(
                "Incremental mode: found %d previously generated queries; skipping %d duplicate specs.",
                len(existing_query_ids),
                skipped,
            )
        else:
            logging.info("Incremental mode: no historical outputs found, generating all specs.")

    if not expanded_specs:
        logging.info("No specs remain after incremental filtering; exiting without generating new tasks.")
        return

    context_blocks = load_context_blocks(args.context_paths) if args.context_paths else []
    if context_blocks:
        logging.info("Loaded %d context documents for prompting.", len(context_blocks))

    agent = QueryConstructionAgent(
        serper_endpoint=args.serper_endpoint,
        market=args.market,
        context_blocks=context_blocks,
    )
    package_dir = None
    if args.package_dir:
        args.package_dir.mkdir(parents=True, exist_ok=True)
        package_dir = args.package_dir

    output_path.parent.mkdir(parents=True, exist_ok=True)

    outputs = generate_batch(
        agent,
        expanded_specs,
        package_dir=package_dir,
        package_include_references=not args.skip_downloads,
        package_reference_limit=0 if args.skip_downloads else 3,
        package_download_ground_truth=not args.skip_downloads,
        package_split_views=args.split_views,
        max_workers=args.max_workers,
    )

    with output_path.open("w", encoding="utf-8") as fh:
        for item in outputs:
            fh.write(json.dumps(item, ensure_ascii=False) + "\n")

    # Helper used by slim-packaging and optional txt emission
    def _render_task_txt(payload: dict) -> str:
        def _fmt_list(items):
            if not items:
                return "- 无"
            return "\n".join(f"- {str(x).strip()}" for x in items if str(x).strip())

        parts: list[str] = []
        qid = str(payload.get("query_id") or "").strip()
        level = str(payload.get("level") or "").strip()
        orientation = str(payload.get("orientation") or "positive").strip()
        title = str(payload.get("title") or "").strip()
        parts.append(f"[{qid}] {level} / {orientation}")
        if title:
            parts.append(f"标题: {title}")
        role = str(payload.get("role_and_background") or "").strip()
        if role:
            parts.append(role)
        # Objectives
        parts.append("你的任务目标是：\n")
        parts.append(_fmt_list(payload.get("task_objectives") or []))
        # Inputs/resources (solver-facing already scrubbed of primary GT by post-processing)
        inres = payload.get("inputs_and_resources") or {}
        if isinstance(inres, dict):
            pm = inres.get("provided_materials") or []
            aer = inres.get("allowed_external_research") or ""
            refu = inres.get("reference_usage") or ""
            if pm or aer or refu:
                parts.append("你在任务中可以使用的输入与资源是：")
            if pm:
                parts.append(_fmt_list(pm))
            if aer:
                parts.append(aer)
            if refu:
                parts.append(refu)
        # Deliverables
        deliver = payload.get("deliverables") or {}
        if isinstance(deliver, dict):
            exp = deliver.get("expected_outputs") or []
            fmt = deliver.get("format_requirements") or ""
            parts.append(_fmt_list(exp))
            if fmt:
                parts.append(f"{fmt}")
        return "\n".join([p for p in parts if str(p).strip()])

    # Optionally emit plain-text task descriptions to aid human inspection or downstream tools.
    if args.emit_txt:
        # 1) Aggregated .txt next to the JSONL output (or in --txt-dir if provided)
        txt_dir = args.txt_dir or output_path.parent
        txt_dir.mkdir(parents=True, exist_ok=True)
        aggregate_txt = txt_dir / f"{output_path.stem}.txt"
        with aggregate_txt.open("w", encoding="utf-8") as tf:
            for idx, item in enumerate(outputs, start=1):
                tf.write(f"=== Task {idx} ===\n")
                tf.write(_render_task_txt(item))
                tf.write("\n\n")

        # 2) If packaged, also emit per-task task.txt inside each package directory
        for item in outputs:
            pkg_dir = item.get("_package_dir")
            if not pkg_dir:
                continue
            pkg_path = Path(pkg_dir)
            try:
                pkg_path.mkdir(parents=True, exist_ok=True)
                (pkg_path / "task.txt").write_text(_render_task_txt(item), encoding="utf-8")
            except OSError:
                # Non-fatal; continue silently
                pass

    # Emit slim (minimal) package copies by default, if packaging is enabled
    if package_dir and not args.disable_slim:
        slim_root = args.slim_base_dir / args.package_dir.name
        base_root = package_dir.resolve()
        count = 0
        for item in outputs:
            pkg_dir = item.get("_package_dir")
            if not pkg_dir:
                continue
            src_task_dir = Path(pkg_dir)
            try:
                rel = src_task_dir.resolve().relative_to(base_root)
            except Exception:
                # If relative path fails, fall back to leaf directory name
                rel = Path(src_task_dir.name)
            dest_task_dir = slim_root / rel
            try:
                dest_task_dir.mkdir(parents=True, exist_ok=True)
                # a) task.txt
                (dest_task_dir / "task.txt").write_text(_render_task_txt(item), encoding="utf-8")
                # b) data_room/
                src_data = src_task_dir / "data_room"
                if src_data.exists() and src_data.is_dir():
                    shutil.copytree(src_data, dest_task_dir / "data_room", dirs_exist_ok=True)
                # c) ground_truth/
                src_gt = src_task_dir / "ground_truth"
                if src_gt.exists() and src_gt.is_dir():
                    shutil.copytree(src_gt, dest_task_dir / "ground_truth", dirs_exist_ok=True)
                count += 1
            except OSError:
                continue
        logging.info("Emitted %d slim package(s) under %s", count, slim_root)

    logging.info("Generated %d queries -> %s", len(outputs), output_path)


if __name__ == "__main__":
    main()
