"""
Utilities for loading pre-downloaded context documents from context_sources/.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

TEXT_EXTENSIONS = {".txt", ".md", ".markdown", ".rst", ".json", ".csv", ".yaml", ".yml"}
HTML_EXTENSIONS = {".html", ".htm"}
MAX_CHARS = 1800


def _strip_html(content: str) -> str:
    content = re.sub(r"(?is)<(script|style).*?>.*?(</\\1>)", "", content)
    content = re.sub(r"(?s)<[^>]+>", " ", content)
    content = re.sub(r"\\s{2,}", " ", content)
    return content.strip()


def _load_text_snippet(path: Path) -> Optional[str]:
    try:
        if path.suffix.lower() in HTML_EXTENSIONS:
            raw = path.read_text(encoding="utf-8", errors="ignore")
            text = _strip_html(raw)
        elif path.suffix.lower() in TEXT_EXTENSIONS:
            text = path.read_text(encoding="utf-8", errors="ignore")
        else:
            return None
    except OSError as exc:
        logger.warning("Failed to read context document %s: %s", path, exc)
        return None

    snippet = text.strip()
    if len(snippet) > MAX_CHARS:
        snippet = snippet[:MAX_CHARS].rstrip() + "\n...[内容截断]"
    return snippet or None


def load_context_documents(base_dir: Path, *, limit: int = 3) -> List[Dict[str, str]]:
    """
    Load context metadata + textual snippet for prompting.
    """
    metadata_path = base_dir / "metadata.json"
    if not metadata_path.exists():
        return []

    try:
        entries = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("Failed to parse metadata.json at %s", metadata_path)
        return []

    documents: List[Dict[str, str]] = []
    for entry in entries:
        local_path = entry.get("local_path")
        if not local_path:
            continue
        path = Path(local_path)
        snippet = _load_text_snippet(path)
        doc = {
            "name": entry.get("title") or path.stem,
            "content": snippet or f"[Refer to original document: {local_path}]",
            "source": entry.get("url"),
            "path": local_path,
            "sha256": entry.get("sha256"),
            "content_type": entry.get("content_type"),
            "query": entry.get("query"),
        }
        documents.append(doc)
        if limit and len(documents) >= limit:
            break
    return documents
