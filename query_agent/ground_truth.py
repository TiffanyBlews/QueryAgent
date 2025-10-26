"""
Ground truth selection helpers.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from .data_structures import GroundTruthBundle, GroundTruthSource
from .packager import DEFAULT_HEADERS
import requests
from .search import SearchError, SearchResult, serper_search
from .spec import QuerySpec

logger = logging.getLogger(__name__)

_SKIP_EXT = {".ico", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js", ".ttf", ".woff", ".woff2"}
_SKIP_DOMAINS = {"duckduckgo.com", "r.jina.ai", "apps.apple.com", "itunes.apple.com"}


def _is_viable_ground_truth(url: Optional[str]) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme in {"file"}:
        return True
    if not parsed.scheme.startswith("http"):
        return False
    host = (parsed.netloc or "").lower()
    if any(host == banned or host.endswith("." + banned) for banned in _SKIP_DOMAINS):
        return False
    path = parsed.path.lower()
    for ext in _SKIP_EXT:
        if path.endswith(ext):
            return False
    return True


def _is_downloadable(url: Optional[str], timeout: float = 15.0) -> bool:
    """Best-effort check whether a URL is directly downloadable (200 status).

    We avoid HEAD because many sites block it; use GET with small timeout and no content read.
    """
    if not _is_viable_ground_truth(url):
        return False
    try:
        resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, stream=True)
        if resp.status_code != 200:
            return False
        ctype = (resp.headers.get("Content-Type") or "").lower()
        # Accept common artifact types
        if any(x in ctype for x in ("pdf", "json", "text/html", "octet-stream")):
            return True
        # Heuristic: if content-type missing but status ok, still accept
        return True
    except requests.RequestException:
        return False


def _extract_domains(snippet: Optional[str]) -> List[str]:
    if not snippet:
        return []
    import re

    domains = []
    for match in re.finditer(r"([a-zA-Z0-9.-]+\.[a-z]{2,})", snippet):
        domain = match.group(1).lower()
        if any(domain.endswith(banned) for banned in _SKIP_DOMAINS):
            continue
        if domain not in domains:
            domains.append(domain)
    return domains[:3]


def select_ground_truth_bundle(
    spec: QuerySpec,
    results: Sequence[SearchResult],
    *,
    serper_endpoint: str,
    market: str,
    language: str,
    max_supporting: int = 3,
) -> GroundTruthBundle:
    """
    Choose a primary ground truth and supporting set from search results.
    """

    if not results:
        raise SearchError(f"No search results available for {spec.query_id}")

    viable = [res for res in results if _is_viable_ground_truth(res.url)]

    # Prefer a single, agent-consumable artifact as Ground Truth primary:
    # - PDFs (reports, papers)
    # - Code repositories (github/gitlab/bitbucket)
    # - Model cards or artifact pages (huggingface)
    def _primary_score(url: Optional[str]) -> Tuple[int, int]:
        if not url:
            return (99, 99)
        u = (url or "").lower()
        # Highest priority: explicit PDFs
        if u.endswith(".pdf") or "/pdf" in u:
            return (0, len(u))
        # Next: repositories
        if "github.com" in u or "gitlab.com" in u or "bitbucket.org" in u:
            return (1, len(u))
        # Next: model/artifact hubs
        if "huggingface.co" in u:
            return (2, len(u))
        # Next: HTML abstracts or general pages
        return (5, len(u))

    viable_sorted = sorted(viable, key=lambda r: _primary_score(r.url))
    # Prefer entries that are actually downloadable; keep the same scoring within downloadables.
    downloadable = [r for r in viable_sorted if _is_downloadable(r.url)]
    ordered = downloadable if downloadable else viable_sorted
    primary: Optional[SearchResult] = ordered[0] if ordered else None

    if primary is None:
        for res in results:
            for domain in _extract_domains(res.snippet):
                try:
                    refined = serper_search(
                        f"site:{domain} {spec.search_query}",
                        endpoint=serper_endpoint,
                        market=market,
                        language=language,
                        num=5,
                    )
                except SearchError:
                    continue
                refined_viable = [cand for cand in refined if _is_viable_ground_truth(cand.url)]
                if refined_viable:
                    primary = refined_viable[0]
                    supporting_candidates = refined_viable[1:]
                    supporting = [
                        GroundTruthSource.from_search_result(item) for item in supporting_candidates[:max_supporting]
                    ]
                    return GroundTruthBundle(
                        primary=GroundTruthSource.from_search_result(primary),
                        supporting=supporting,
                    )

    if primary is None:
        logger.warning("Falling back to first search result for %s due to lack of viable Ground Truth.", spec.query_id)
        primary = results[0]

    supporting = []
    for res in results:
        if res is primary:
            continue
        if not _is_viable_ground_truth(res.url):
            continue
        supporting.append(GroundTruthSource.from_search_result(res))
        if len(supporting) >= max_supporting:
            break

    return GroundTruthBundle(
        primary=GroundTruthSource.from_search_result(primary),
        supporting=supporting,
    )
