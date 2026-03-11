"""Scraper for Vivanuncios (OLX Mexico).

Strategy:
  1. Extract JSON from __NEXT_DATA__ script tag.
  2. Fallback to HTML ad-card parsing.
"""

from __future__ import annotations

import json
import re
from typing import AsyncIterator

from bs4 import BeautifulSoup
from loguru import logger

from config import PORTALS, SCRAPING
from scrapers.base import BaseScraper, _safe_float, _safe_int

_CFG = PORTALS["vivanuncios"]

_OPERATION_SLUG = {
    "venta": "venta",
    "renta": "renta",
}


class VivanunciosScraper(BaseScraper):
    """Scrapes Vivanuncios.com.mx (OLX platform) for Colima real-estate listings."""

    PORTAL_NAME = "vivanuncios"

    async def fetch_listings(
        self,
        municipio: str,
        operation: str = "venta",
    ) -> AsyncIterator[dict]:
        mun_slug = (
            municipio.lower()
            .replace(" ", "-")
            .replace("á", "a").replace("é", "e")
            .replace("í", "i").replace("ó", "o").replace("ú", "u")
        )
        op_slug = _OPERATION_SLUG.get(operation, "venta")
        page = 1
        total_yielded = 0

        while page <= SCRAPING["max_pages"]:
            url = (
                f"{_CFG['base_url']}/s-inmuebles/{mun_slug}-colima/"
                f"v1c1085l{page}"
            )

            try:
                html = await self._get(url, as_json=False)
            except Exception as exc:
                logger.error(
                    "[{}] HTTP error page {} for {}: {}",
                    self.PORTAL_NAME, page, municipio, exc,
                )
                break

            items = (
                self._extract_next_data(html, op_slug)
                or self._extract_html_cards(html, municipio)
            )

            if not items:
                logger.debug(
                    "[{}] No items on page {} for {} ({}), stopping.",
                    self.PORTAL_NAME, page, municipio, operation,
                )
                break

            for item in items:
                listing = self._normalize(item, municipio, operation)
                if listing:
                    yield listing
                    total_yielded += 1

            if len(items) < SCRAPING["page_size"]:
                break

            page += 1

        logger.debug(
            "[{}] {} listings fetched for {} ({})",
            self.PORTAL_NAME, total_yielded, municipio, operation,
        )

    # ------------------------------------------------------------------
    # __NEXT_DATA__ extraction
    # ------------------------------------------------------------------

    def _extract_next_data(self, html: str, operation_slug: str) -> list[dict]:
        match = re.search(
            r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
            html,
            re.DOTALL,
        )
        if not match:
            return []
        try:
            data = json.loads(match.group(1))
        except json.JSONDecodeError:
            return []

        page_props = data.get("props", {}).get("pageProps", {})

        # OLX / Vivanuncios stores ads in several possible keys
        ads = (
            page_props.get("ads")
            or page_props.get("listings")
            or page_props.get("items")
            or page_props.get("initialState", {})
               .get("listing", {})
               .get("listingAds", {})
               .get("ads")
            or []
        )
        if isinstance(ads, dict):
            ads = ads.get("data") or ads.get("items") or []

        # Filter by operation when the field is present
        result = []
        for ad in (ads if isinstance(ads, list) else []):
            params = ad.get("parameters") or ad.get("params") or {}
            if isinstance(params, list):
                params = {p.get("key"): p.get("value") for p in params}
            deal = str(params.get("deal_type", "")).lower()
            if deal and deal not in operation_slug:
                continue
            result.append(ad)
        return result

    # ------------------------------------------------------------------
    # HTML card fallback
    # ------------------------------------------------------------------

    def _extract_html_cards(self, html: str, municipio: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select(
            "li[data-aut-id='itemBox'], article.olx-ad-card, div[data-aut-id='adCard']"
        )
        items = []
        for card in cards:
            try:
                item: dict = {}
                link = card.select_one("a[href]")
                if link:
                    href = link.get("href", "")
                    item["url"] = href if href.startswith("http") else _CFG["base_url"] + href
                    id_match = re.search(r"iid-(\d+)", href)
                    item["external_id"] = id_match.group(1) if id_match else href

                title_el = card.select_one(
                    "[data-aut-id='itemTitle'], h2.title, span.title"
                )
                if title_el:
                    item["title"] = title_el.get_text(strip=True)

                price_el = card.select_one(
                    "[data-aut-id='itemPrice'], span.price, div.price-block"
                )
                if price_el:
                    raw_price = re.sub(r"[^\d.]", "", price_el.get_text())
                    item["price"] = _safe_float(raw_price)
                    item["currency"] = "MXN"

                loc_el = card.select_one(
                    "[data-aut-id='item-location'], span.location"
                )
                if loc_el:
                    item["colonia"] = loc_el.get_text(strip=True)

                # Attribute chips
                for chip in card.select("[data-aut-id='property'], li.parameter"):
                    text = chip.get_text(strip=True).lower()
                    num = _safe_float(re.sub(r"[^\d.]", "", text))
                    if "recámara" in text or "cuarto" in text:
                        item["bedrooms"] = _safe_int(num)
                    elif "baño" in text:
                        item["bathrooms"] = num
                    elif "m²" in text or "m2" in text:
                        item.setdefault("area_total", num)

                item["municipio"] = municipio
                items.append(item)
            except Exception as exc:
                logger.warning("[{}] HTML card parse error: {}", self.PORTAL_NAME, exc)

        return items

    # ------------------------------------------------------------------
    # Normalisation
    # ------------------------------------------------------------------

    def _normalize(self, item: dict, municipio: str, operation: str) -> dict | None:
        try:
            ext_id = str(
                item.get("id")
                or item.get("ad_id")
                or item.get("external_id")
                or ""
            )
            if not ext_id:
                return None

            # OLX stores parameters as a list of {key, value} dicts or a plain dict
            params = item.get("parameters") or item.get("params") or {}
            if isinstance(params, list):
                params = {p.get("key"): p.get("value") for p in params if p.get("key")}

            location = item.get("location") or {}
            if isinstance(location, str):
                location = {}

            price_raw = (
                item.get("price")
                or item.get("priceValue")
                or params.get("price")
            )
            if isinstance(price_raw, dict):
                price_value = _safe_float(price_raw.get("value") or price_raw.get("amount"))
                currency = price_raw.get("currency", "MXN")
            else:
                price_value = _safe_float(price_raw)
                currency = "MXN"

            raw = {
                "portal": self.PORTAL_NAME,
                "external_id": ext_id,
                "url": item.get("url") or item.get("link") or "",
                "operation": operation,
                "property_type": (
                    item.get("category")
                    or params.get("property_type")
                    or params.get("tipo_inmueble")
                ),
                "municipio": (
                    location.get("city")
                    or location.get("municipality")
                    or item.get("municipio")
                    or municipio
                ),
                "colonia": (
                    location.get("neighborhood")
                    or location.get("colonia")
                    or item.get("colonia")
                ),
                "address": location.get("address") or item.get("address"),
                "latitude": location.get("lat"),
                "longitude": location.get("lon"),
                "price": price_value,
                "currency": currency,
                "area_total": _safe_float(
                    params.get("size") or params.get("surface_total") or item.get("area_total")
                ),
                "area_built": _safe_float(
                    params.get("surface_covered") or item.get("area_built")
                ),
                "bedrooms": _safe_int(
                    params.get("rooms") or params.get("bedrooms")
                ),
                "bathrooms": _safe_float(
                    params.get("bathrooms") or params.get("baths")
                ),
                "parking": _safe_int(params.get("parking_spaces")),
                "title": item.get("title") or item.get("subject"),
                "description": item.get("body") or item.get("description"),
                "amenities": [],
                "raw": item,
            }
            return self._base_normalize(raw)

        except Exception as exc:
            logger.warning("[{}] Normalize error: {} — {}", self.PORTAL_NAME, exc, item)
            return None
