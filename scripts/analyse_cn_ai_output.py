#!/usr/bin/env python3
"""
Analyse generated query outputs (JSONL) for the CN AI taxonomy workflow.

Usage:
    python scripts/analyse_cn_ai_output.py --input output/cn_ai_class --report report.md

The script scans all *.jsonl files under the input directory, aggregates statistics
per classification (industry_id) and overall, and prints a Markdown summary. If
--report is given, the same content is written to the specified file.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional


@dataclass
class ClassificationStats:
    file_name: str
    total: int = 0
    by_level: Counter[str] = field(default_factory=Counter)
    by_orientation: Counter[str] = field(default_factory=Counter)
    search_queries_total: int = 0
    search_queries_count: int = 0
    unique_professions: set[str] = field(default_factory=set)

    def update(self, record: Dict[str, object]) -> None:
        self.total += 1
        level = str(record.get("level", "")).upper()
        if level:
            self.by_level[level] += 1
        orientation = str(record.get("orientation", "positive")).lower()
        if orientation:
            self.by_orientation[orientation] += 1

        sq = record.get("search_queries") or []
        if isinstance(sq, str):
            sq = [sq]
        if isinstance(sq, Iterable):
            sq_list = [str(item).strip() for item in sq if str(item).strip()]
            if sq_list:
                self.search_queries_total += len(sq_list)
                self.search_queries_count += 1

        meta = record.get("spec_metadata") or {}
        profession = meta.get("profession") or record.get("profession")
        if profession:
            self.unique_professions.add(str(profession))

    def average_search_queries(self) -> float:
        if self.search_queries_count == 0:
            return 0.0
        return self.search_queries_total / self.search_queries_count


def iter_jsonl(path: Path) -> Iterable[Dict[str, object]]:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def analyse_directory(input_dir: Path) -> Dict[str, ClassificationStats]:
    stats: Dict[str, ClassificationStats] = {}
    for jsonl_path in sorted(input_dir.glob("*.jsonl")):
        classification = jsonl_path.stem
        stat = stats.setdefault(classification, ClassificationStats(file_name=jsonl_path.name))
        for record in iter_jsonl(jsonl_path):
            stat.update(record)
    return stats


def render_markdown(stats: Dict[str, ClassificationStats]) -> str:
    headers = [
        "| 分类ID | 文件名 | 任务数 | L3 | L4 | L5 | 正向 | 逆向 | 平均检索词数 | 职业数 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    lines = []
    overall = ClassificationStats(file_name="ALL")
    for key, stat in sorted(stats.items()):
        overall.total += stat.total
        overall.by_level.update(stat.by_level)
        overall.by_orientation.update(stat.by_orientation)
        overall.search_queries_total += stat.search_queries_total
        overall.search_queries_count += stat.search_queries_count
        overall.unique_professions.update(stat.unique_professions)

        lines.append(
            f"| {key} | {stat.file_name} | {stat.total} | "
            f"{stat.by_level.get('L3', 0)} | {stat.by_level.get('L4', 0)} | {stat.by_level.get('L5', 0)} | "
            f"{stat.by_orientation.get('positive', 0)} | {stat.by_orientation.get('inverse', 0)} | "
            f"{stat.average_search_queries():.2f} | {len(stat.unique_professions)} |"
        )

    footer = (
        f"\n**合计**：{overall.total} 条任务；L3/L4/L5 分布 = "
        f"{overall.by_level.get('L3', 0)}/{overall.by_level.get('L4', 0)}/{overall.by_level.get('L5', 0)}；"
        f"正向/逆向 = {overall.by_orientation.get('positive', overall.total)}/"
        f"{overall.by_orientation.get('inverse', 0)}；"
        f"平均检索词数 = {overall.average_search_queries():.2f}；"
        f"独立职业数 = {len(overall.unique_professions)}。"
    )

    return "\n".join(headers + lines) + footer


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyse CN AI taxonomy query outputs (JSONL).")
    parser.add_argument("--input", type=Path, default=Path("output/cn_ai_class"), help="目录路径，包含 *.jsonl。")
    parser.add_argument("--report", type=Path, help="可选：将 Markdown 结果写入指定文件。")
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input directory not found: {args.input}")

    stats = analyse_directory(args.input)
    if not stats:
        raise SystemExit(f"No JSONL files found under {args.input}")

    markdown = render_markdown(stats)
    print(markdown)
    if args.report:
        args.report.write_text(markdown, encoding="utf-8")
        print(f"\n[info] 报告已写入 {args.report}")


if __name__ == "__main__":
    main()
