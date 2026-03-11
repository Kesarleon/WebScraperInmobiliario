"""Scraper for Lamudi Mexico.

Primary strategy: parse JSON embedded in Next.js __NEXT_DATA__ script tag.
Fallback: parse HTML listing cards.
"""

from __future__ import annotations

import json
import re
from typing import AsyncIterator

from bs4 import BeautifulSoup
from loguru import logger

from config import PORTALS, SCRAPING
from scrapers.base import BaseScraper, _safe_float, _safe_int

_CFG = PORTALS["lamudi"]

_OPERATION_SLUG = {
    "venta": "venta",
    "renta": "renta",
}

_PROP_TYPE_SLUG = {
    "casa": "casa",
    "departamento": "departamento",
    "terreno": "terreno",
}


class LamudiScraper(BaseScraper):
    """Scrapes Lamudi.com.mx with __NEXT_DATA__ primary / HTML card fallback."""

    PORTAL_NAME = "lamudi"

    async def fetch_listings(
        self,
        municipio: str,
        operation: str = "venta",
    ) -> AsyncIterator[dict]:
        op_slug = _OPERATION_SLUG.get(operation, "venta")
        mun_slug = municipio.lower().replace(" ", "-").replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
        page = 1
        total_yielded = 0

        while page <= SCRAPING["max_pages"]:
            url = (
                f"{_CFG['base_url']}/colima/{mun_slug}/"
                f"inmueble/{op_slug}"
                f"?page={page}"
            )

            try:
                html = await self._get(url, as_json=False)
            except Exception as exc:
                logger.error(
                    "[{}] HTTP error page {} for {}: {}",
                    self.PORTAL_NAME, page, municipio, exc,
                )
                break

            items = self._extract_next_data(html) or self._extract_html_cards(html, municipio)

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

    def _extract_next_data(self, html: str) -> list[dict]:
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

        # Navigate the Next.js page props tree
        page_props = (
            data.get("props", {})
            .get("pageProps", {})
        )
        listings = (
            page_props.get("listings")
            or page_props.get("data", {}).get("listings")
            or page_props.get("results")
            or []
        )
        if isinstance(listings, dict):
            listings = listings.get("items") or listings.get("data") or []
        return listings if isinstance(listings, list) else []

    # ------------------------------------------------------------------
    # HTML card fallback
    # ------------------------------------------------------------------

    def _extract_html_cards(self, html: str, municipio: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select("div.listing-card, article.js-listing-card, div[data-test='listing-card']")
        items = []
        for card in cards:
            try:
                item: dict = {}

                # External id / URL
                link = card.select_one("a[href]")
                if link:
                    href = link.get("href", "")
                    item["url"] = href if href.startswith("http") else _CFG["base_url"] + href
                    id_match = re.search(r"-(\d+)\.html", href)
                    item["external_id"] = id_match.group(1) if id_match else href

                # Title
                title_el = card.select_one("h2, .listing-card__title, [data-test='listing-title']")
                if title_el:
                    item["title"] = title_el.get_text(strip=True)

                # Price
                price_el = card.select_one(".price, [data-test='listing-price'], .listing-card__price")
                if price_el:
                    raw_price = re.sub(r"[^\d.]", "", price_el.get_text())
                    item["price"] = _safe_float(raw_price)
                    item["currency"] = "MXN"

                # Location
                loc_el = card.select_one(".listing-card__location, [data-test='listing-location']")
                if loc_el:
                    item["colonia"] = loc_el.get_text(strip=True)

                # Attributes
                attrs = card.select(".listing-card__attribute, [data-test='listing-attribute']")
                for attr in attrs:
                    text = attr.get_text(strip=True).lower()
                    num = _safe_float(re.sub(r"[^\d.]", "", text))
                    if "recámara" in text or "habitacion" in text or "cuarto" in text:
                        item["bedrooms"] = _safe_int(num)
                    elif "baño" in text:
                        item["bathrooms"] = num
                    elif "m²" in text or "m2" in text:
                        item.setdefault("area_total", num)
                    elif "estacion" in text or "parking" in text or "garage" in text:
                        item["parking"] = _safe_int(num)

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
                or item.get("externalId")
                or item.get("external_id")
                or item.get("listingId")
                or ""
            )
            if not ext_id:
                return None

            location = item.get("location") or item.get("address") or {}
            if isinstance(location, str):
                location = {}

            price_info = item.get("price") or item.get("listing_price") or {}
            if isinstance(price_info, (int, float)):
                price_info = {"amount": price_info, "currency": "MXN"}

            raw = {
                "portal": self.PORTAL_NAME,
                "external_id": ext_id,
                "url": item.get("url") or item.get("slug") or "",
                "operation": operation,
                "property_type": (
                    item.get("propertyType")
                    or item.get("property_type")
                    or item.get("type")
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
                "latitude": location.get("lat") or location.get("latitude"),
                "longitude": location.get("lng") or location.get("longitude"),
                "price": _safe_float(
                    price_info.get("amount")
                    or price_info.get("value")
                    or item.get("price")
                ),
                "currency": price_info.get("currency", "MXN"),
                "area_total": _safe_float(
                    item.get("lotArea") or item.get("lot_area") or item.get("area_total")
                ),
                "area_built": _safe_float(
                    item.get("builtArea") or item.get("built_area") or item.get("area_built")
                ),
                "bedrooms": _safe_int(
                    item.get("bedrooms") or item.get("rooms")
                ),
                "bathrooms": _safe_float(
                    item.get("bathrooms") or item.get("baths")
                ),
                "parking": _safe_int(
                    item.get("parkingSpaces") or item.get("parking")
                ),
                "title": item.get("title") or item.get("name"),
                "description": item.get("description"),
                "amenities": item.get("amenities") or [],
                "raw": item,
            }
            return self._base_normalize(raw)

        except Exception as exc:
            logger.warning("[{}] Normalize error: {} — {}", self.PORTAL_NAME, exc, item)
            return None
