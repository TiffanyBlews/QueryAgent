"""
Agent for assessing task feasibility and ground-truth suitability on synthesized SOP packages.
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import shutil

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

from .llm import LLMError, OpenAIChatClient

logger = logging.getLogger(__name__)


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

MAX_DOC_CHARS = 3500
MAX_TOTAL_CONTEXT_CHARS = 18000


def _timestamp() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _truncate_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 200].rstrip() + "\n...\n" + text[-200:].lstrip()


def _clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for element in soup(["script", "style", "noscript", "iframe"]):
        element.decompose()
    text = soup.get_text("\n")
    lines = [line.strip() for line in text.splitlines()]
    collapsed = "\n".join(line for line in lines if line)
    return collapsed.strip()


def _read_text_file(path: Path, encoding: str = "utf-8") -> str:
    try:
        return path.read_text(encoding=encoding)
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="ignore")


def _load_pdf_text(path: Path) -> str:
    try:
        reader = PdfReader(str(path))
        fragments: List[str] = []
        for page in reader.pages:
            page_text = page.extract_text() or ""
            if page_text:
                fragments.append(page_text)
        content = "\n".join(fragments).strip()
        if content:
            return content
        return f"[WARNING] 无法从 PDF {path.name} 提取文本内容。"
    except Exception as exc:  # noqa: BLE001
        return f"[ERROR] 读取 PDF {path.name} 失败：{exc}"


def _fetch_url(url: str, *, timeout: float = 45.0) -> Tuple[Optional[str], Optional[str]]:
    try:
        response = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
        )
        response.raise_for_status()
        response.encoding = response.apparent_encoding or response.encoding
        text = response.text
        return text, None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


@dataclass
class DocumentArtifact:
    doc_type: str
    identifier: str
    title: Optional[str] = None
    url: Optional[str] = None
    path: Optional[str] = None
    status: str = "ok"
    error: Optional[str] = None
    raw_text: Optional[str] = None
    prompt_excerpt: Optional[str] = None
    metadata: Dict[str, object] = field(default_factory=dict)

    def to_prompt_block(self) -> Optional[str]:
        if not self.raw_text:
            return None
        header_parts = [
            f"类型: {self.doc_type}",
            f"标识: {self.identifier}",
        ]
        if self.title:
            header_parts.append(f"标题: {self.title}")
        if self.url:
            header_parts.append(f"URL: {self.url}")
        if self.path and not self.url:
            header_parts.append(f"路径: {self.path}")
        header = " | ".join(header_parts)
        excerpt = self.prompt_excerpt or _truncate_text(self.raw_text, MAX_DOC_CHARS)
        return f"{header}\n{excerpt}"

    def to_dict(self) -> Dict[str, object]:
        return {
            "doc_type": self.doc_type,
            "identifier": self.identifier,
            "title": self.title,
            "url": self.url,
            "path": self.path,
            "status": self.status,
            "error": self.error,
            "metadata": self.metadata,
        }


class FeasibilityAgent:
    """
    Agent that inspects synthesized SOP packages, calls an LLM for feasibility judgments,
    and optionally evaluates ground-truth suitability.
    """

    def __init__(
        self,
        package_root: Path,
        *,
        output_dir: Path,
        max_workers: int = 4,
        llm_client: Optional[OpenAIChatClient] = None,
    ) -> None:
        self.package_root = package_root
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.results_path = self.output_dir / "results.jsonl"
        self.state_path = self.output_dir / "state.json"
        self.artifacts_dir = self.output_dir / "artifacts"
        self.artifacts_dir.mkdir(exist_ok=True)
        self.executable_dir = self.output_dir / "executable_packages"
        self.executable_dir.mkdir(exist_ok=True)
        self.verified_dir = self.output_dir / "verified_packages"
        self.verified_dir.mkdir(exist_ok=True)
        self.verified_inverse_dir = self.output_dir / "verified_inverse_packages"
        self.verified_inverse_dir.mkdir(exist_ok=True)

        self.max_workers = max_workers
        self.llm = llm_client or OpenAIChatClient()

        self._lock = threading.Lock()
        self._completed: Dict[str, Dict[str, object]] = {}
        self._load_completed()

    # ------------------------------------------------------------------ #
    # Public API

    def run(self, packages: Optional[Iterable[Path]] = None) -> None:
        package_list = list(packages or self._discover_packages())
        if not package_list:
            logger.warning("No packages discovered under %s", self.package_root)
            return

        todo = [pkg for pkg in package_list if self._package_id(pkg) not in self._completed]
        if not todo:
            logger.info("All %d discovered packages already processed.", len(package_list))
            return

        logger.info(
            "Processing %d/%d packages with up to %d workers.",
            len(todo),
            len(package_list),
            self.max_workers,
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_map = {executor.submit(self._process_package_safe, pkg): pkg for pkg in todo}
            for future in as_completed(future_map):
                pkg = future_map[future]
                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001
                    logger.error("Package %s failed with exception: %s", pkg, exc)
                    continue
                if result:
                    self._persist_result(result)

        self._write_state()

    # ------------------------------------------------------------------ #
    # Package discovery and bookkeeping

    def _discover_packages(self) -> List[Path]:
        packages = []
        for task_file in self.package_root.rglob("task.txt"):
            packages.append(task_file.parent)
        packages.sort()
        return packages

    def _package_id(self, package_path: Path) -> str:
        return str(package_path.relative_to(self.package_root))

    def _is_inverse_package(self, package_path: Path) -> bool:
        parts = set(package_path.relative_to(self.package_root).parts)
        return "inverse" in parts

    def _safe_package_id(self, package_path: Path) -> str:
        return self._package_id(package_path).replace("/", "__")

    def _load_completed(self) -> None:
        if not self.results_path.exists():
            return
        with self.results_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed JSONL line in %s", self.results_path)
                    continue
                package_id = payload.get("package_id")
                if package_id:
                    self._completed[package_id] = payload

    def _persist_result(self, data: Dict[str, object]) -> None:
        package_id = data.get("package_id")
        if not package_id:
            logger.error("Result missing package_id: %s", data)
            return
        with self._lock:
            with self.results_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(data, ensure_ascii=False) + "\n")
            self._completed[package_id] = data

    def _write_state(self) -> None:
        state = {
            "completed": len(self._completed),
            "results_path": str(self.results_path),
            "updated_at": _timestamp(),
        }
        with self.state_path.open("w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)

    # ------------------------------------------------------------------ #
    # Package processing

    def _process_package_safe(self, package_path: Path) -> Optional[Dict[str, object]]:
        package_id = self._package_id(package_path)
        try:
            return self._process_package(package_path)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Unhandled exception while processing %s: %s", package_id, exc)
            return {
                "package_id": package_id,
                "status": "error",
                "error": str(exc),
                "timestamp": _timestamp(),
            }

    def _process_package(self, package_path: Path) -> Dict[str, object]:
        package_id = self._package_id(package_path)
        logger.info("Processing package %s", package_id)

        task_path = package_path / "task.txt"
        task_text = _read_text_file(task_path)

        level = self._infer_level(package_path)
        is_inverse = self._is_inverse_package(package_path)

        references = self._gather_references(package_path)
        pdf_docs = self._gather_pdfs(package_path)

        context_docs = references + pdf_docs

        common_result = {
            "package_id": package_id,
            "assessment_type": "inverse" if is_inverse else "positive",
            "timestamp": _timestamp(),
            "level": level,
            "task_excerpt": _truncate_text(task_text, 800),
            "references": [doc.to_dict() for doc in references],
            "pdfs": [doc.to_dict() for doc in pdf_docs],
        }

        if is_inverse:
            inverse_response, inverse_error = self._call_inverse_llm(
                package_id=package_id,
                level=level,
                task_text=task_text,
                context_docs=context_docs,
            )
            common_result["inverse_assessment"] = inverse_response
            common_result["inverse_error"] = inverse_error
            common_result["status"] = "ok" if inverse_response else "error"
            if inverse_response and inverse_response.get("non_feasible") is True:
                inverse_path = self._copy_verified_inverse_package(package_path)
                if inverse_path:
                    common_result["verified_inverse_to"] = inverse_path
            return common_result

        feasibility_response, feasibility_error = self._call_feasibility_llm(
            package_id=package_id,
            level=level,
            task_text=task_text,
            context_docs=context_docs,
        )

        common_result["feasibility"] = feasibility_response
        common_result["feasibility_error"] = feasibility_error
        common_result["status"] = "ok" if feasibility_response else "error"

        if feasibility_response and feasibility_response.get("feasible") is True:
            copied_path = self._copy_executable_package(package_path)
            if copied_path:
                common_result["copied_to"] = copied_path
            gt_artifact = self._load_primary_ground_truth(package_path)
            if gt_artifact:
                gt_evaluation = self._call_ground_truth_llm(
                    package_id=package_id,
                    level=level,
                    task_text=task_text,
                    feasibility=feasibility_response,
                    ground_truth_doc=gt_artifact,
                )
                common_result["ground_truth"] = gt_evaluation
                common_result["ground_truth_artifact"] = gt_artifact.to_dict()
                if gt_evaluation and gt_evaluation.get("gt_useful") is True:
                    gt_path = self._copy_verified_package(package_path)
                    if gt_path:
                        common_result["verified_to"] = gt_path

        return common_result

    def _infer_level(self, package_path: Path) -> Optional[str]:
        parts = package_path.relative_to(self.package_root).parts
        if len(parts) >= 2:
            return parts[1]
        return None

    def _gather_references(self, package_path: Path) -> List[DocumentArtifact]:
        data_room = package_path / "data_room"
        references_path = data_room / "references.json"
        artifacts: List[DocumentArtifact] = []
        if not references_path.exists():
            return artifacts

        try:
            payload = json.loads(references_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            artifact = DocumentArtifact(
                doc_type="reference",
                identifier="references.json",
                title="references.json",
                path=str(references_path),
                status="error",
                error=f"Failed to parse JSON: {exc}",
            )
            artifacts.append(artifact)
            return artifacts

        for idx, entry in enumerate(payload, start=1):
            title = entry.get("title")
            url = entry.get("url")
            identifier = f"ref-{idx:02d}"

            html, error = _fetch_url(url) if url else (None, "Missing URL")
            if html is None:
                artifact = DocumentArtifact(
                    doc_type="reference",
                    identifier=identifier,
                    title=title,
                    url=url,
                    status="error",
                    error=error,
                    metadata=entry,
                )
            else:
                text = _clean_html(html)
                excerpt = _truncate_text(text, MAX_DOC_CHARS)
                artifact = DocumentArtifact(
                    doc_type="reference",
                    identifier=identifier,
                    title=title,
                    url=url,
                    status="ok",
                    raw_text=text,
                    prompt_excerpt=excerpt,
                    metadata=entry,
                )
                self._persist_artifact_text(package_path, f"{identifier}.txt", text)
            artifacts.append(artifact)

        return artifacts

    def _gather_pdfs(self, package_path: Path) -> List[DocumentArtifact]:
        data_room = package_path / "data_room"
        artifacts: List[DocumentArtifact] = []
        if not data_room.exists():
            return artifacts

        for pdf_path in data_room.glob("*.pdf"):
            identifier = pdf_path.name
            text = _load_pdf_text(pdf_path)
            status = "ok" if not text.startswith("[ERROR]") else "error"
            if status == "ok" and text.startswith("[WARNING]"):
                status = "warn"
            excerpt = _truncate_text(text, MAX_DOC_CHARS)
            artifact = DocumentArtifact(
                doc_type="pdf",
                identifier=identifier,
                title=pdf_path.stem,
                path=str(pdf_path),
                status=status,
                raw_text=text,
                prompt_excerpt=excerpt,
            )
            self._persist_artifact_text(package_path, f"{identifier}.txt", text)
            artifacts.append(artifact)

        return artifacts

    def _persist_artifact_text(self, package_path: Path, filename: str, content: str) -> None:
        safe_id = self._safe_package_id(package_path)
        target_dir = self.artifacts_dir / safe_id
        target_dir.mkdir(exist_ok=True)
        target_path = target_dir / filename
        with target_path.open("w", encoding="utf-8") as handle:
            handle.write(content)

    def _copy_executable_package(self, package_path: Path) -> Optional[str]:
        safe_id = self._safe_package_id(package_path)
        target_dir = self.executable_dir / safe_id
        with self._lock:
            try:
                shutil.copytree(package_path, target_dir, dirs_exist_ok=True)
            except TypeError:
                if target_dir.exists():
                    shutil.rmtree(target_dir)
                shutil.copytree(package_path, target_dir)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to copy executable package %s to %s: %s",
                    package_path,
                    target_dir,
                    exc,
                )
                return None
        return str(target_dir)

    def _copy_verified_package(self, package_path: Path) -> Optional[str]:
        safe_id = self._safe_package_id(package_path)
        target_dir = self.verified_dir / safe_id
        with self._lock:
            try:
                shutil.copytree(package_path, target_dir, dirs_exist_ok=True)
            except TypeError:
                if target_dir.exists():
                    shutil.rmtree(target_dir)
                shutil.copytree(package_path, target_dir)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to copy verified positive package %s to %s: %s",
                    package_path,
                    target_dir,
                    exc,
                )
                return None
        return str(target_dir)

    def _copy_verified_inverse_package(self, package_path: Path) -> Optional[str]:
        safe_id = self._safe_package_id(package_path)
        target_dir = self.verified_inverse_dir / safe_id
        with self._lock:
            try:
                shutil.copytree(package_path, target_dir, dirs_exist_ok=True)
            except TypeError:
                if target_dir.exists():
                    shutil.rmtree(target_dir)
                shutil.copytree(package_path, target_dir)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to copy verified inverse package %s to %s: %s",
                    package_path,
                    target_dir,
                    exc,
                )
                return None
        return str(target_dir)

    # ------------------------------------------------------------------ #
    # LLM interaction

    def _build_context_snippet(self, docs: List[DocumentArtifact]) -> str:
        blocks: List[str] = []
        total_chars = 0
        for doc in docs:
            if doc.status != "ok" and doc.status != "warn":
                continue
            block = doc.to_prompt_block()
            if not block:
                continue
            if total_chars + len(block) > MAX_TOTAL_CONTEXT_CHARS:
                remaining = MAX_TOTAL_CONTEXT_CHARS - total_chars
                if remaining <= 0:
                    break
                block = _truncate_text(block, remaining)
            blocks.append(block)
            total_chars += len(block)
            if total_chars >= MAX_TOTAL_CONTEXT_CHARS:
                break
        return "\n\n".join(blocks).strip()

    def _call_feasibility_llm(
        self,
        *,
        package_id: str,
        level: Optional[str],
        task_text: str,
        context_docs: List[DocumentArtifact],
    ) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
        context_snippet = self._build_context_snippet(context_docs)
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a senior research director who evaluates whether a research task is executable "
                    "within a typical analyst workflow. Base your decision on the provided materials and on what "
                    "could realistically be located via Google searches of public sources. Respond in Chinese. "
                    "Output must be a JSON object only, with no reasoning text or extra fields."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"任务包: {package_id}\n"
                    f"任务等级: {level or '未知'}\n\n"
                    f"任务描述:\n{task_text.strip()}\n\n"
                    "可用参考资料（节选，已去噪音）:\n"
                    f"{context_snippet or '（无可用参考资料，仅可依赖公开搜索）'}\n\n"
                    "任务类型：正向（目标是可执行的真实任务）。请判断该任务能否在几天内通过公开资料完成。\n"
                    "只输出一个 JSON 对象，且仅包含以下字段（严禁添加reasoning或多余文字）：\n"
                    "- feasible: 布尔值\n"
                    "- missing_elements: 字符串数组，列出阻碍执行的关键缺口，无则给空数组\n"
                    "- recommended_actions: 字符串数组，给出若可执行时的高层步骤，条目简洁\n"
                    "- confidence: 0-1 之间的小数\n"
                ),
            },
        ]

        try:
            response = self.llm.run_json_completion(messages, temperature=0.1)
            return response, None
        except LLMError as exc:
            logger.error("Feasibility LLM call failed for %s: %s", package_id, exc)
            return None, str(exc)

    def _call_inverse_llm(
        self,
        *,
        package_id: str,
        level: Optional[str],
        task_text: str,
        context_docs: List[DocumentArtifact],
    ) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
        context_snippet = self._build_context_snippet(context_docs)
        messages = [
            {
                "role": "system",
                "content": (
                    "You evaluate intentionally adversarial research tasks that should be impossible to execute "
                    "with the permitted public information sources. Respond in Chinese. "
                    "Output must be a JSON object only, with the exact fields requested and no reasoning text."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"任务包: {package_id}\n"
                    f"任务等级: {level or '未知'}\n\n"
                    f"任务描述:\n{task_text.strip()}\n\n"
                    "可用参考资料（节选，已去噪音）:\n"
                    f"{context_snippet or '（无可用参考资料，仅可依赖公开搜索）'}\n\n"
                    "任务类型：反向（设计目标是无法在公开资料条件下完成）。请判断该任务是否确实不可执行，"
                    "并评估其是否符合反向任务标准。\n"
                    "只输出一个 JSON 对象，且仅包含以下字段：\n"
                    "- non_feasible: 布尔值，表示任务是否无法完成\n"
                    "- blocking_elements: 字符串数组，列出导致任务不可行的关键因素\n"
                    "- compliance: 布尔值，表示该任务是否符合反向任务标准（即设置合理且不可执行）\n"
                    "- confidence: 0-1 之间的小数\n"
                ),
            },
        ]

        try:
            response = self.llm.run_json_completion(messages, temperature=0.1)
            return response, None
        except LLMError as exc:
            logger.error("Inverse LLM call failed for %s: %s", package_id, exc)
            return None, str(exc)

    def _load_primary_ground_truth(self, package_path: Path) -> Optional[DocumentArtifact]:
        metadata_path = package_path / "ground_truth" / "metadata.json"
        if not metadata_path.exists():
            return None
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            logger.warning("Failed to parse ground truth metadata for %s: %s", package_path, exc)
            return DocumentArtifact(
                doc_type="ground_truth",
                identifier="primary",
                title="metadata.json",
                path=str(metadata_path),
                status="error",
                error=f"metadata.json decode error: {exc}",
            )

        primary = metadata.get("primary") or {}
        cache_info = (metadata.get("cache") or {}).get("primary")

        title = primary.get("title")
        url = primary.get("url")
        identifier = "primary"

        text: Optional[str] = None
        status = "ok"
        error: Optional[str] = None
        path: Optional[str] = None

        if cache_info and cache_info.get("local_path"):
            cache_path = Path(cache_info["local_path"])
            path = str(cache_path)
            if cache_path.exists():
                if cache_path.suffix.lower() == ".pdf":
                    text = _load_pdf_text(cache_path)
                else:
                    raw_html = _read_text_file(cache_path)
                    text = _clean_html(raw_html)
            else:
                status = "error"
                error = f"Cached ground-truth file missing at {cache_path}"

        if text is None and url:
            html, fetch_error = _fetch_url(url)
            if html:
                text = _clean_html(html)
            else:
                status = "error"
                error = fetch_error

        if not text:
            text = ""

        excerpt = _truncate_text(text, MAX_DOC_CHARS) if text else ""

        artifact = DocumentArtifact(
            doc_type="ground_truth",
            identifier=identifier,
            title=title,
            url=url,
            path=path,
            status=status,
            error=error,
            raw_text=text,
            prompt_excerpt=excerpt,
            metadata=primary,
        )

        if text:
            safe_id = self._safe_package_id(package_path)
            target_dir = self.artifacts_dir / safe_id
            target_dir.mkdir(exist_ok=True)
            target_path = target_dir / "ground_truth_primary.txt"
            with target_path.open("w", encoding="utf-8") as handle:
                handle.write(text)

        return artifact

    def _call_ground_truth_llm(
        self,
        *,
        package_id: str,
        level: Optional[str],
        task_text: str,
        feasibility: Dict[str, object],
        ground_truth_doc: DocumentArtifact,
    ) -> Optional[Dict[str, object]]:
        context_block = ground_truth_doc.to_prompt_block() or "（未能加载 ground truth 内容）"
        feasibility_summary = json.dumps(feasibility, ensure_ascii=False)

        messages = [
            {
                "role": "system",
                "content": (
                    "You are validating whether a ground-truth document can fairly evaluate the success of a completed research task. "
                    "Consider whether the document covers the required scope, has authoritative provenance, and can be mapped to measurable deliverables. "
                    "Respond in Chinese. Output must be a JSON object only with the specified keys; do not include reasoning or extra text."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"任务包: {package_id}\n"
                    f"任务等级: {level or '未知'}\n\n"
                    f"任务描述:\n{task_text.strip()}\n\n"
                    f"可执行性判断结果（供参考）:\n{feasibility_summary}\n\n"
                    "候选 ground truth 资料:\n"
                    f"{context_block}\n\n"
                    "请判断该 ground truth 是否适合作为任务完成后的评分参考。"
                    "只输出一个 JSON 对象，且仅包含以下字段：\n"
                    "- gt_useful: 布尔值\n"
                    "- coverage_gaps: 字符串数组，列出不足或风险点，无则空数组\n"
                    "- usage_notes: 字符串数组，给出使用注意事项或替代方案，无则空数组\n"
                    "- confidence: 0-1 之间的小数\n"
                ),
            },
        ]

        try:
            return self.llm.run_json_completion(messages, temperature=0.1)
        except LLMError as exc:
            logger.error("Ground-truth LLM call failed for %s: %s", package_id, exc)
            return {
                "gt_useful": False,
                "coverage_gaps": ["LLM 调用失败"],
                "usage_notes": [f"LLM 调用失败：{exc}"],
                "confidence": 0.0,
            }


def _collect_packages_from_path(path: Path) -> List[Path]:
    packages: List[Path] = []
    if path.is_file():
        if path.name == "task.txt":
            packages.append(path.parent)
            return packages
        raise ValueError(f"文件 {path} 不是 task.txt，无法识别为任务包。")
    if not path.is_dir():
        raise ValueError(f"路径不存在或不可访问：{path}")

    task_file = path / "task.txt"
    if task_file.exists():
        packages.append(path)
        return packages

    for candidate in sorted(path.rglob("task.txt")):
        packages.append(candidate.parent)

    if not packages:
        raise ValueError(f"{path} 下未找到任何 task.txt，无法确定任务包目录。")
    return packages


def _resolve_target_packages(package_root: Path, targets: Iterable[str]) -> List[Path]:
    resolved: List[Path] = []
    seen: set[str] = set()

    for raw in targets:
        candidate = Path(raw)
        if not candidate.exists():
            candidate = package_root / raw

        candidate = candidate.resolve()
        if not candidate.exists():
            raise ValueError(f"目标路径不存在：{raw}")

        packages = _collect_packages_from_path(candidate)
        for pkg in packages:
            key = str(pkg.resolve())
            if key not in seen:
                seen.add(key)
                resolved.append(pkg)

    return resolved


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="评估 packages/<...>/task.txt 任务包的可执行性与 Ground Truth 质量。",
    )
    parser.add_argument(
        "--package-root",
        type=Path,
        default=Path("packages/cn_ai_class"),
        help="Step2 输出的任务包根目录（默认: packages/cn_ai_class）",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output/feasibility"),
        help="评估结果输出目录（默认: output/feasibility）",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="并发评估的线程数（默认: 8）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="可选：只处理前 N 个待评估任务包，用于抽样或调试。",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="日志等级（默认: INFO）",
    )
    parser.add_argument(
        "targets",
        nargs="*",
        help=(
            "可选：指定一个或多个任务包路径/ID。"
            "可以传递绝对路径、相对路径或 package_id（相对于 --package-root）。"
            "如未传参则遍历 package-root 下全部任务包。"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    package_root = args.package_root.resolve()
    output_dir = args.output_dir.resolve()

    if not package_root.exists():
        raise SystemExit(f"[ERROR] package_root 不存在：{package_root}")

    agent = FeasibilityAgent(
        package_root=package_root,
        output_dir=output_dir,
        max_workers=args.max_workers,
    )

    if args.targets:
        try:
            packages = _resolve_target_packages(package_root, args.targets)
        except ValueError as exc:
            raise SystemExit(f"[ERROR] {exc}") from exc
    else:
        packages = agent._discover_packages()  # noqa: SLF001

    if args.limit is not None:
        packages = packages[: args.limit]

    logger.info(
        "启动可执行性评估：root=%s, output=%s, packages=%d",
        package_root,
        output_dir,
        len(packages),
    )
    agent.run(packages)


if __name__ == "__main__":
    main()
