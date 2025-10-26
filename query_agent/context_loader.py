"""
Utilities for loading free-form context documents that should condition prompt construction.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {".md", ".markdown", ".txt", ".rst"}

MAX_SECTION_CHARS = 1200
MAX_SECTIONS_PER_FILE = 24


def _iter_candidate_files(path: Path) -> Iterable[Path]:
    """
    Yield candidate files from a given path. Directories are searched recursively.
    """
    if path.is_file():
        yield path
        return

    if not path.exists():
        logger.warning("Context path does not exist and will be ignored: %s", path)
        return

    if not path.is_dir():
        logger.warning("Context path is not a file or directory and will be ignored: %s", path)
        return

    for candidate in sorted(path.rglob("*")):
        if not candidate.is_file():
            continue
        suffix = candidate.suffix.lower()
        if suffix in SUPPORTED_EXTENSIONS or not suffix:
            yield candidate


def load_context_blocks(paths: Sequence[Path]) -> List[Dict[str, str]]:
    """
    Load textual context blocks from the provided paths.

    Parameters
    ----------
    paths:
        Iterable of file or directory paths. Directories are searched recursively for
        Markdown/plain-text style files.

    Returns
    -------
    List of dictionaries containing `name` and `content`.
    """
    blocks: List[Dict[str, str]] = []
    seen: set[Path] = set()
    for raw_path in paths:
        path = raw_path.expanduser().resolve()
        for file_path in _iter_candidate_files(path):
            if file_path in seen:
                continue
            sections = _load_file_sections(file_path)
            if not sections:
                continue
            seen.add(file_path)
            blocks.extend(sections[:MAX_SECTIONS_PER_FILE])

    return blocks


def _load_file_sections(path: Path) -> List[Dict[str, str]]:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        logger.warning("Failed to read context file as UTF-8 and will skip: %s", path)
        return []
    except OSError as exc:  # pragma: no cover - filesystem dependent
        logger.warning("Unable to read context file %s: %s", path, exc)
        return []

    sections = _split_markdown_sections(text)
    blocks: List[Dict[str, str]] = []
    for idx, (title, body) in enumerate(sections):
        trimmed = body.strip()
        if not trimmed:
            continue
        if len(trimmed) > MAX_SECTION_CHARS:
            trimmed = trimmed[:MAX_SECTION_CHARS].rstrip() + "\n...[内容截断]"

        label = f"{path.name}"
        if title:
            label = f"{label} :: {title}"
        blocks.append(
            {
                "name": label,
                "path": f"{path}#{idx}",
                "content": trimmed,
            }
        )
    return blocks


def _split_markdown_sections(text: str) -> List[Tuple[str, str]]:
    """
    Very lightweight Markdown section splitter. Groups content under headings.
    """
    lines = text.splitlines()
    sections: List[Tuple[str, List[str]]] = []
    heading_stack: List[Tuple[int, str]] = []
    current_title = ""
    current_buffer: List[str] = []

    def _commit():
        nonlocal current_buffer, current_title
        if not current_buffer:
            return
        sections.append((current_title, current_buffer))
        current_buffer = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#"):
            _commit()
            level = len(stripped) - len(stripped.lstrip("#"))
            level_title = stripped.lstrip("#").strip()
            # Adjust heading stack to current level
            while heading_stack and heading_stack[-1][0] >= level:
                heading_stack.pop()
            heading_stack.append((level, level_title))
            current_title = " / ".join(title for _, title in heading_stack if title)
            continue
        current_buffer.append(line)

    _commit()

    if not sections:
        body = text.strip()
        return [("", body)]

    result: List[Tuple[str, str]] = []
    for title, buffer in sections:
        block = "\n".join(buffer).strip()
        if not block:
            continue
        summary = _summarize_block(block)
        result.append((title, summary))

    return result or [("", text)]


def _summarize_block(block: str) -> str:
    """
    Down-select the most salient lines from a section:
    - Keep short paragraphs / bullet points up to the limit
    - Drop repeated empty lines
    """
    lines = block.splitlines()
    cleaned: List[str] = []
    seen_blank = False
    for line in lines:
        stripped = line.rstrip()
        if not stripped:
            if seen_blank:
                continue
            seen_blank = True
            cleaned.append("")
            continue
        seen_blank = False
        cleaned.append(stripped)
        if len(cleaned) >= 80:  # avoid overly long sections
            break
    return "\n".join(cleaned).strip()
