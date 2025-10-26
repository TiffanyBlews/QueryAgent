"""
Utilities for caching ground truth documents locally.
"""

from __future__ import annotations

import base64
import hashlib
import zipfile
import io
import os
from pathlib import Path
from typing import Dict, Optional
import mimetypes

import requests

from .ground_truth import GroundTruthBundle, GroundTruthSource
from .packager import guess_extension, sanitize_filename, DEFAULT_HEADERS
from .pdf_parser import WebPDFParser, PDFParsingError

CACHE_DIR = Path("ground_truth_cache")


def _hash_bytes(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def _download(url: str, timeout: float = 45.0) -> Optional[tuple[bytes, str]]:
    if url.startswith("file://"):
        path = Path(url[7:])
        try:
            data = path.read_bytes()
        except OSError:
            return None
        return data, mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException:
        return None
    content_type = response.headers.get("Content-Type", "")
    return response.content, content_type


def _cache_single(source: GroundTruthSource, *, cache_dir: Path) -> Optional[Dict[str, object]]:
    if not source.url:
        return None
    download = _download(source.url)
    if not download:
        return None
    data, content_type = download
    sha = _hash_bytes(data)
    directory = cache_dir / sha[:2] / sha
    directory.mkdir(parents=True, exist_ok=True)
    filename = sanitize_filename(source.title or "ground-truth")
    ext = guess_extension(content_type, source.url)
    path = directory / f"{filename}{ext}"
    if not path.exists():
        path.write_bytes(data)

    metadata = {
        "local_path": str(path.resolve()),
        "sha256": sha,
        "content_type": content_type,
        "filesize": len(data),
        "source_url": source.url,
    }

    # If this is a PDF and PDF parsing is enabled, try to parse and cache the content
    enable_pdf_parsing = os.environ.get("ENABLE_PDF_PARSING", "0").lower() in ("1", "true", "yes")
    if enable_pdf_parsing and ("pdf" in content_type.lower() or source.url.lower().endswith('.pdf')):
        try:
            pdf_parser = WebPDFParser()
            success, parsed_content, images_b64 = pdf_parser.parse_pdf_url(source.url)

            if success and parsed_content.strip():
                # Save parsed content as markdown
                content_path = directory / f"{filename}_content.md"
                content_path.write_text(parsed_content, encoding='utf-8')
                metadata["parsed_content_path"] = str(content_path.resolve())
                metadata["parsed_content_length"] = len(parsed_content)

                # Save images if available
                if images_b64:
                    images_dir = directory / "images"
                    images_dir.mkdir(exist_ok=True)

                    try:
                        # Decode and extract images
                        zip_data = base64.b64decode(images_b64)
                        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
                            zf.extractall(images_dir)
                        metadata["images_dir"] = str(images_dir.resolve())
                        metadata["has_images"] = True
                    except Exception as e:
                        # If image extraction fails, log but don't fail the whole operation
                        import logging
                        logging.getLogger(__name__).warning(
                            "Failed to extract PDF images for %s: %s", source.url, e
                        )
                        metadata["has_images"] = False
                else:
                    metadata["has_images"] = False

        except (PDFParsingError, Exception) as e:
            # PDF parsing failed, but we still have the original file
            import logging
            logging.getLogger(__name__).warning(
                "Failed to parse PDF content for %s: %s", source.url, e
            )

    return metadata


def cache_ground_truth_bundle(bundle: GroundTruthBundle, *, cache_dir: Path = CACHE_DIR) -> Dict[str, object]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    metadata: Dict[str, object] = {}

    primary_cached = _cache_single(bundle.primary, cache_dir=cache_dir)
    if primary_cached:
        metadata["primary"] = primary_cached

    supporting_entries = []
    for source in bundle.supporting:
        cached = _cache_single(source, cache_dir=cache_dir)
        if cached:
            supporting_entries.append(cached)
    if supporting_entries:
        metadata["supporting"] = supporting_entries

    return metadata
