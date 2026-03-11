"""Abstract base class for all portal scrapers."""

from __future__ import annotations

import asyncio
import random
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator

import httpx
from fake_useragent import UserAgent
from loguru import logger
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import MUNICIPIOS, MUNICIPIOS_ALIASES, SCRAPING, STATE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_ua() -> str:
    try:
        return UserAgent().random
    except Exception:
        return SCRAPING["fallback_ua"]


def _normalize_municipio(raw: str | None) -> str | None:
    """Normalize municipio name using alias map."""
    if raw is None:
        return None
    cleaned = raw.strip().lower()
    # Direct alias match
    if cleaned in MUNICIPIOS_ALIASES:
        return MUNICIPIOS_ALIASES[cleaned]
    # Exact match (case-insensitive)
    for m in MUNICIPIOS:
        if m.lower() == cleaned:
            return m
    # Partial match
    for m in MUNICIPIOS:
        if m.lower() in cleaned or cleaned in m.lower():
            return m
    return raw.strip().title()


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Base scraper
# ---------------------------------------------------------------------------

class BaseScraper(ABC):
    """Async scraper base with rate-limiting, retries and data normalisation."""

    PORTAL_NAME: str = "base"

    def __init__(self) -> None:
        self._semaphore = asyncio.Semaphore(SCRAPING["max_concurrent"])
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # HTTP client lifecycle
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "BaseScraper":
        headers = {
            "User-Agent": _get_ua(),
            "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
            "Accept": "application/json, text/html, */*",
        }
        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=SCRAPING["http_timeout"],
            http2=True,
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Rate-limited, retried GET
    # ------------------------------------------------------------------

    async def _get(
        self,
        url: str,
        *,
        params: dict | None = None,
        headers: dict | None = None,
        as_json: bool = True,
    ) -> Any:
        """Perform a GET request with exponential back-off retries."""
        if self._client is None:
            raise RuntimeError("Scraper must be used as async context manager")

        async with self._semaphore:
            # Random polite delay
            await asyncio.sleep(
                random.uniform(
                    SCRAPING["request_delay_min"],
                    SCRAPING["request_delay_max"],
                )
            )

            async for attempt in AsyncRetrying(
                retry=retry_if_exception_type(
                    (httpx.HTTPStatusError, httpx.TransportError, httpx.TimeoutException)
                ),
                stop=stop_after_attempt(SCRAPING["max_retries"]),
                wait=wait_exponential(
                    min=SCRAPING["retry_wait_min"],
                    max=SCRAPING["retry_wait_max"],
                ),
                reraise=True,
            ):
                with attempt:
                    resp = await self._client.get(
                        url,
                        params=params,
                        headers=headers or {},
                    )
                    resp.raise_for_status()

            if as_json:
                return resp.json()
            return resp.text

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def fetch_listings(
        self,
        municipio: str,
        operation: str = "venta",
    ) -> AsyncIterator[dict]:
        """Yield normalised listing dicts for *municipio*."""
        ...  # pragma: no cover

    # ------------------------------------------------------------------
    # Convenience: fetch all municipios
    # ------------------------------------------------------------------

    async def fetch_all(
        self, operation: str = "venta"
    ) -> AsyncIterator[dict]:
        """Iterate over all Colima municipios and yield listings."""
        for municipio in MUNICIPIOS:
            logger.info(
                "[{}] Fetching {} listings for {}",
                self.PORTAL_NAME,
                operation,
                municipio,
            )
            async for listing in self.fetch_listings(municipio, operation):
                yield listing

    # ------------------------------------------------------------------
    # Normalisation helpers
    # ------------------------------------------------------------------

    def _base_normalize(self, raw: dict) -> dict:
        """Apply common normalisation steps to a raw listing dict."""
        raw["state"] = STATE
        if "municipio" in raw:
            raw["municipio"] = _normalize_municipio(raw["municipio"])
        if "price" in raw:
            raw["price"] = _safe_float(raw["price"])
        if "area_total" in raw:
            raw["area_total"] = _safe_float(raw["area_total"])
        if "area_built" in raw:
            raw["area_built"] = _safe_float(raw["area_built"])
        if "bedrooms" in raw:
            raw["bedrooms"] = _safe_int(raw["bedrooms"])
        if "bathrooms" in raw:
            raw["bathrooms"] = _safe_float(raw["bathrooms"])
        if "parking" in raw:
            raw["parking"] = _safe_int(raw["parking"])

        # Compute price per m² if possible
        area = raw.get("area_built") or raw.get("area_total")
        if raw.get("price") and area and area > 0:
            raw["price_per_m2"] = round(raw["price"] / area, 2)

        raw["portal"] = self.PORTAL_NAME
        return raw
