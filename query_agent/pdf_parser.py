"""
PDF parsing utilities using Web crawl service API.

PDF parsing is OPTIONAL and disabled by default.
To enable PDF parsing, set environment variable: ENABLE_PDF_PARSING=1

Required environment variables when PDF parsing is enabled:
- CRAWL_API_KEY: API key for the crawl service
- CRAWL_API_SECRET: Secret key for signature generation
- CRAWL_API_ENDPOINT: Service endpoint URL
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from typing import Dict, Optional, Tuple

import requests

logger = logging.getLogger(__name__)


class PDFParsingError(RuntimeError):
    """Raised when PDF parsing fails."""


class WebPDFParser:
    """
    PDF parser using the web crawl service API.
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        secret: Optional[str] = None,
        endpoint: Optional[str] = None,
        timeout: float = 1200.0,  # PDF parsing can take up to 1200 seconds
    ) -> None:
        self.api_key = api_key or os.environ.get("CRAWL_API_KEY")
        self.secret = secret or os.environ.get("CRAWL_API_SECRET")
        self.endpoint = endpoint or os.environ.get("CRAWL_API_ENDPOINT", "http://14.103.37.13:10010")
        self.timeout = timeout

        if not self.api_key:
            raise PDFParsingError("CRAWL_API_KEY is not set.")
        if not self.secret:
            raise PDFParsingError("CRAWL_API_SECRET is not set.")

        self._url2md_endpoint = self.endpoint.rstrip("/") + "/url2md"

    def _generate_signature(self, timestamp: str) -> str:
        """Generate signature for API authentication."""
        message = self.api_key + timestamp + self.secret
        return hashlib.sha256(message.encode()).hexdigest()

    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API requests."""
        timestamp = str(int(time.time()))
        signature = self._generate_signature(timestamp)

        return {
            "X-API-KEY": self.api_key,
            "X-TIMESTAMP": timestamp,
            "X-SIGNATURE": signature,
            "Content-Type": "application/json",
        }

    def is_pdf_url(self, url: str) -> bool:
        """Check if URL likely points to a PDF."""
        url_lower = url.lower()
        return url_lower.endswith(".pdf") or "/pdf" in url_lower or "pdf" in url_lower

    def parse_pdf_url(self, url: str, *, with_cache: bool = True) -> Tuple[bool, str, Optional[str]]:
        """
        Parse PDF from URL using the web crawl service.

        Returns:
            Tuple of (success, content, images_b64)
            - success: Whether parsing was successful
            - content: Parsed markdown content
            - images_b64: Base64 encoded images zip (if any)
        """
        if not self.is_pdf_url(url):
            logger.warning("URL does not appear to be a PDF: %s", url)

        headers = self._get_auth_headers()
        payload = {
            "url": url,
            "method": "pdf",  # Force PDF parsing strategy
            "with_cache": with_cache,
        }

        try:
            response = requests.post(
                self._url2md_endpoint,
                json=payload,
                headers=headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise PDFParsingError(f"PDF parsing timed out for URL: {url}")
        except requests.RequestException as exc:
            raise PDFParsingError(f"Failed to parse PDF from URL {url}: {exc}")

        try:
            data = response.json()
        except ValueError as exc:
            raise PDFParsingError(f"Invalid JSON response from PDF parsing service: {exc}")

        if response.status_code != 200:
            error_msg = data.get("error", "Unknown error")
            raise PDFParsingError(f"PDF parsing failed: {error_msg}")

        content = data.get("content", "")
        images_b64 = data.get("images")  # May be None
        title = data.get("title", "")
        source = data.get("source", "unknown")

        if not content.strip():
            raise PDFParsingError(f"PDF parsing returned empty content for URL: {url}")

        logger.info(
            "Successfully parsed PDF from %s using %s strategy. Content length: %d chars",
            url,
            source,
            len(content),
        )

        # Add title to content if available and not already included
        if title and title.strip() and not title.strip().lower() in content.lower():
            content = f"# {title}\n\n{content}"

        return True, content, images_b64

    def parse_pdf_url_safe(self, url: str, *, with_cache: bool = True) -> Tuple[bool, str]:
        """
        Safe wrapper for PDF parsing that returns success/failure without raising exceptions.

        Returns:
            Tuple of (success, content)
        """
        try:
            success, content, _ = self.parse_pdf_url(url, with_cache=with_cache)
            return success, content
        except PDFParsingError as exc:
            logger.warning("PDF parsing failed for %s: %s", url, exc)
            return False, ""
        except Exception as exc:
            logger.error("Unexpected error parsing PDF %s: %s", url, exc)
            return False, ""