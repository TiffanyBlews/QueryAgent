"""
Utilities for persisting generated queries alongside their ground truth evidence.
"""

from __future__ import annotations

import json
import mimetypes
import re
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple
from urllib.parse import urlparse

import requests
import shutil

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/119.0 Safari/537.36"
    ),
}


def sanitize_filename(text: str, max_length: int = 80) -> str:
    """
    Create a safe filename fragment from a URL or title.
    """
    text = text.strip().lower()
    if not text:
        return "file"
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    if not text:
        text = "file"
    if len(text) > max_length:
        text = text[:max_length].rstrip("-")
    return text or "file"


def guess_extension(content_type: str, url: str) -> str:
    """
    Guess a reasonable file extension based on content type or URL.
    """
    if not content_type:
        return Path(urlparse(url).path).suffix or ".html"

    if "pdf" in content_type:
        return ".pdf"
    if "json" in content_type:
        return ".json"
    if "text/plain" in content_type:
        return ".txt"
    if "markdown" in content_type:
        return ".md"
    if "msword" in content_type:
        return ".doc"
    if "presentation" in content_type:
        return ".ppt"
    if "excel" in content_type:
        return ".xlsx"

    if "html" in content_type:
        return ".html"

    ext = Path(urlparse(url).path).suffix
    if ext:
        return ext
    guess = mimetypes.guess_extension(content_type)
    return guess or ".dat"


def download_resource(url: str, dest_dir: Path, prefix: str) -> Optional[Tuple[Path, str]]:
    """
    Download a URL to the destination directory.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=45)
        response.raise_for_status()
    except requests.RequestException as exc:  # noqa: BLE001
        return None

    content_type = response.headers.get("Content-Type", "")
    ext = guess_extension(content_type, url)
    filename = sanitize_filename(prefix or urlparse(url).path.split("/")[-1] or "ground-truth")
    path = dest_dir / f"{filename}{ext}"

    try:
        with path.open("wb") as fh:
            fh.write(response.content)
    except OSError:
        return None
    return path, content_type


def save_query_package(
    payload: Dict,
    destination: Path,
    *,
    include_references: bool = True,
    reference_limit: int = 3,
    download_ground_truth: bool = True,
    split_views: bool = False,
) -> Path:
    """
    Persist query JSON, search metadata, and downloaded ground truth/reference files.
    """
    level = payload.get("level", "L3")
    orientation = payload.get("orientation", "positive")
    orientation = str(orientation).strip().lower() or "positive"
    orientation_dir = orientation if orientation in {"positive", "inverse"} else "misc"
    query_id = payload.get("query_id", "unnamed_query")
    base_dir = destination / level / orientation_dir / query_id
    base_dir.mkdir(parents=True, exist_ok=True)

    # Save main query payload.
    query_path = base_dir / "query.json"
    query_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if split_views:
        # Solver view: hide ground truth and standard_answer to avoid leakage.
        solver_payload = dict(payload)
        solver_payload.pop("ground_truth", None)
        solver_payload.pop("standard_answer", None)

        # Best-effort sanitize: replace any literal 'Ground Truth' mentions in outward-facing fields.
        import re as _re

        def _scrub_text(x):
            if isinstance(x, str):
                return _re.sub(r"Ground\s*Truth", "参考资料", x, flags=_re.IGNORECASE)
            return x

        # Apply to common top-level fields
        for _k in ("title", "role_and_background", "tool_usage_expectation", "estimated_human_time", "notes"):
            if _k in solver_payload and isinstance(solver_payload[_k], str):
                solver_payload[_k] = _scrub_text(solver_payload[_k])

        # Arrays
        for _k in ("task_objectives", "grading_rubric"):
            if isinstance(solver_payload.get(_k), list):
                solver_payload[_k] = [_scrub_text(v) for v in solver_payload[_k]]

        # Nested blocks
        if isinstance(solver_payload.get("deliverables"), dict):
            _del = solver_payload["deliverables"]
            for _k in ("expected_outputs",):
                if isinstance(_del.get(_k), list):
                    _del[_k] = [_scrub_text(v) for v in _del[_k]]
            for _k in ("format_requirements", "quality_bar"):
                if isinstance(_del.get(_k), str):
                    _del[_k] = _scrub_text(_del[_k])
            solver_payload["deliverables"] = _del

        if isinstance(solver_payload.get("inputs_and_resources"), dict):
            _in = solver_payload["inputs_and_resources"]
            if isinstance(_in.get("provided_materials"), list):
                _in["provided_materials"] = [_scrub_text(v) for v in _in["provided_materials"]]
            for _k in ("allowed_external_research", "ground_truth_usage", "reference_usage"):
                if isinstance(_in.get(_k), str):
                    _in[_k] = _scrub_text(_in[_k])
            solver_payload["inputs_and_resources"] = _in

        # Evaluation guide
        if isinstance(solver_payload.get("evaluation_guide"), dict):
            _eg = solver_payload["evaluation_guide"]
            for _k in ("summary",):
                if isinstance(_eg.get(_k), str):
                    _eg[_k] = _scrub_text(_eg[_k])
            for _k in ("checkpoints", "scoring_rubric"):
                if isinstance(_eg.get(_k), list):
                    _eg[_k] = [_scrub_text(v) for v in _eg[_k]]
            solver_payload["evaluation_guide"] = _eg

        # Search results list (snippets may contain placeholder text)
        if isinstance(solver_payload.get("search_results"), list):
            _sr = []
            for it in solver_payload["search_results"]:
                if not isinstance(it, dict):
                    _sr.append(it)
                    continue
                _clean = dict(it)
                for _k in ("title", "snippet"):
                    if isinstance(_clean.get(_k), str):
                        _clean[_k] = _scrub_text(_clean[_k])
                _sr.append(_clean)
            solver_payload["search_results"] = _sr

        (base_dir / "solver_query.json").write_text(
            json.dumps(solver_payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        # Judge view: same as main payload (kept as query.json)

    # Save search metadata.
    if payload.get("search_results"):
        (base_dir / "search_results.json").write_text(
            json.dumps(payload["search_results"], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    gt_info = payload.get("ground_truth") or {}
    ground_truth_dir = base_dir / "ground_truth"
    ground_truth_dir.mkdir(exist_ok=True)
    (ground_truth_dir / "metadata.json").write_text(
        json.dumps(gt_info, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    primary_info = gt_info.get("primary") or {}
    supporting_info = gt_info.get("supporting") or []
    cache_info = gt_info.get("cache") or {}
    cached_primary = cache_info.get("primary") if isinstance(cache_info.get("primary"), dict) else None
    cached_supporting = cache_info.get("supporting") if isinstance(cache_info.get("supporting"), list) else []
    downloaded_paths: Dict[str, str] = {}
    if download_ground_truth:
        if cached_primary and cached_primary.get("local_path"):
            src = Path(cached_primary["local_path"])
            if src.exists():
                dest = ground_truth_dir / (src.name)
                try:
                    shutil.copy2(src, dest)
                    downloaded_paths["ground_truth_primary"] = str(dest.resolve())
                    downloaded_paths["ground_truth_primary_content_type"] = cached_primary.get("content_type")
                except OSError:
                    pass
        elif primary_info.get("url"):
            result = download_resource(primary_info["url"], ground_truth_dir, prefix="ground-truth-primary")
            if result:
                path, content_type = result
                downloaded_paths["ground_truth_primary"] = str(path.resolve())
                downloaded_paths["ground_truth_primary_content_type"] = content_type

    # IMPORTANT: Do not download supporting evidence into ground_truth directory.
    # We keep ground_truth as a single authoritative artifact (primary only).
    # Supporting sources will be available via metadata.json and can appear in
    # references/context sections if needed, but not duplicated here.

    # Build a unified references list for packaging into a single data room directory:
    # - Start from payload.references (or fallback to search_results)
    # - Add any URLs parsed from inputs_and_resources.provided_materials
    # - Add context_sources (derived from context_documents)
    # - Exclude any Ground Truth primary URL
    # - Deduplicate by URL (first occurrence wins)
    contexts: Iterable[Dict] = payload.get("references") or payload.get("search_results") or []
    data_room_dir = base_dir / "data_room"
    data_room_dir.mkdir(exist_ok=True)

    # Prepare primary GT URL to exclude from references packaging (supporting will be INCLUDED)
    primary_gt_url: Optional[str] = None
    try:
        _gt = payload.get("ground_truth") or {}
        _p = (_gt.get("primary") or {}).get("url")
        if _p:
            primary_gt_url = str(_p).strip()
    except Exception:
        pass

    # Seed map (first occurrence wins)
    by_url: Dict[str, Dict] = {}
    # 1) Add supporting sources first (prioritize GT-supporting metadata in manifest)
    try:
        for s in (_gt.get("supporting") or []):
            u = (s.get("url") or "").strip()
            if not u or u == primary_gt_url or u in by_url:
                continue
            by_url[u] = {
                "title": s.get("title") or u,
                "url": u,
                "snippet": s.get("snippet"),
                "source": s.get("source"),
                "date": s.get("date"),
            }
    except Exception:
        pass

    # 2) Seed with references/search_results
    for item in contexts:
        if not isinstance(item, dict):
            continue
        u = (item.get("url") or "").strip()
        if not u:
            continue
        if (primary_gt_url and u == primary_gt_url) or u in by_url:
            continue
        by_url[u] = {
            "title": item.get("title") or u,
            "url": u,
            "snippet": item.get("snippet"),
            "source": item.get("source"),
            "date": item.get("date"),
        }

    # 3) Add URLs parsed from provided_materials (strings may contain 0..n URLs)
    try:
        import re as _re
        inres = payload.get("inputs_and_resources") or {}
        pm_list = inres.get("provided_materials") or []
        for entry in pm_list:
            text = str(entry or "")
            for match in _re.findall(r"https?://[^\s)\]\"<>]+", text):
                u = match.strip()
                if not u or (primary_gt_url and u == primary_gt_url) or u in by_url:
                    continue
                by_url[u] = {
                    "title": text.strip()[:160] or u,
                    "url": u,
                    "snippet": None,
                    "source": None,
                    "date": None,
                }
    except Exception:
        pass

    # Final unified list (manifest contains ALL; downloads apply reference_limit)
    unified_refs: list[Dict] = list(by_url.values())

    # Attempt downloads for the first N, if enabled
    if include_references and reference_limit != 0:
        to_download = unified_refs if reference_limit is None else unified_refs[: max(0, reference_limit)]
        for idx, ref in enumerate(to_download, start=1):
            url = ref.get("url")
            if not url:
                continue
            download = download_resource(url, data_room_dir, prefix=f"reference-{idx}")
            if download:
                path, content_type = download
                # Keep only PDFs as packaged artifacts for references to avoid HTML clutter.
                if content_type and "pdf" in content_type.lower():
                    ref["local_path"] = str(path.resolve())
                    ref["content_type"] = content_type
                else:
                    # Remove non-PDF files; retain URL in references.json only.
                    try:
                        path.unlink(missing_ok=True)  # type: ignore[arg-type]
                    except Exception:
                        pass

    # Merge in context_sources and copy only PDF artifacts to data_room
    context_sources = payload.get("context_sources") or []
    context_entries: list[Dict] = []
    for item in context_sources:
        # Prepare a normalized record for aggregated manifest
        norm = {
            "title": item.get("name") or item.get("title") or (item.get("source_url") or item.get("local_path") or ""),
            "url": item.get("source_url"),
            "local_path": item.get("local_path"),
            "content_type": item.get("content_type"),
            "sha256": item.get("sha256"),
            "snippet": item.get("snippet"),
            "type": "context_document",
        }

        # Copy local PDF if available
        src_path = item.get("local_path")
        if src_path:
            source_path = Path(src_path)
            if source_path.exists() and source_path.suffix.lower() == ".pdf":
                dest_name = sanitize_filename(source_path.stem)
                dest_path = data_room_dir / f"context-{dest_name}{source_path.suffix}"
                try:
                    shutil.copy2(source_path, dest_path)
                    norm["package_path"] = str(dest_path.resolve())
                    norm["content_type"] = norm.get("content_type") or "application/pdf"
                except OSError:
                    pass
        # If only URL exists and it looks like a PDF, try to download it.
        elif item.get("source_url"):
            url = str(item.get("source_url")).strip()
            try:
                result = download_resource(url, data_room_dir, prefix="context-ref")
            except Exception:
                result = None
            if result:
                path, content_type = result
                if content_type and "pdf" in content_type.lower():
                    norm["package_path"] = str(path.resolve())
                    norm["content_type"] = content_type
                else:
                    try:
                        path.unlink(missing_ok=True)  # type: ignore[arg-type]
                    except Exception:
                        pass
        context_entries.append(norm)

    # Aggregate references.json for BOTH sources: unified_refs (type=reference) + context_entries (type=context_document)
    aggregated: list[Dict] = []
    seen_keys: set[str] = set()

    def _key_of(d: Dict) -> str:
        return (str(d.get("url") or "") + "|" + str(d.get("local_path") or "")).strip()

    for ref in unified_refs:
        rec = dict(ref)
        rec.setdefault("type", "reference")
        k = _key_of(rec)
        if k and k not in seen_keys:
            aggregated.append(rec)
            seen_keys.add(k)
    for doc in context_entries:
        k = _key_of(doc)
        if k and k not in seen_keys:
            aggregated.append(doc)
            seen_keys.add(k)

    # Write a single aggregated manifest under data_room
    (data_room_dir / "references.json").write_text(
        json.dumps(aggregated, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if downloaded_paths:
        (base_dir / "artifacts.json").write_text(
            json.dumps(downloaded_paths, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return base_dir
