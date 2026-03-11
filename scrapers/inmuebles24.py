"""Scraper for Inmuebles24 using its internal JSON API."""

from __future__ import annotations

from typing import AsyncIterator

from loguru import logger

from config import PORTALS, SCRAPING
from scrapers.base import BaseScraper

_CFG = PORTALS["inmuebles24"]

# Operation codes used by the API
_OPERATION_MAP = {
    "venta": "1",
    "renta": "2",
}

# Property type codes
_PROP_TYPE_MAP = {
    "1": "casa",
    "2": "departamento",
    "3": "terreno",
    "4": "local_comercial",
    "5": "oficina",
    "6": "bodega",
    "13": "casa_en_condominio",
    "14": "rancho",
}


class Inmuebles24Scraper(BaseScraper):
    """Fetches listings from the Inmuebles24 internal REST API."""

    PORTAL_NAME = "inmuebles24"

    async def fetch_listings(
        self,
        municipio: str,
        operation: str = "venta",
    ) -> AsyncIterator[dict]:
        op_code = _OPERATION_MAP.get(operation, "1")
        page = 1
        total_yielded = 0

        while page <= SCRAPING["max_pages"]:
            params = {
                "operacion": op_code,
                "ubicacion": f"{municipio}, Colima, México",
                "desde": (page - 1) * SCRAPING["page_size"],
                "cantidad": SCRAPING["page_size"],
                "orden": "fecha_desc",
            }

            try:
                data = await self._get(
                    f"{_CFG['base_url']}{_CFG['search_path']}",
                    params=params,
                )
            except Exception as exc:
                logger.error(
                    "[{}] Error on page {} for {}: {}",
                    self.PORTAL_NAME, page, municipio, exc,
                )
                break

            items = (
                data.get("postings")
                or data.get("results")
                or data.get("items")
                or []
            )
            if not items:
                break

            for item in items:
                listing = self._parse_item(item, municipio, operation)
                if listing:
                    yield listing
                    total_yielded += 1

            # Pagination termination
            total_count = (
                data.get("paging", {}).get("total")
                or data.get("total")
                or 0
            )
            if total_yielded >= total_count or len(items) < SCRAPING["page_size"]:
                break

            page += 1

        logger.debug(
            "[{}] {} listings fetched for {} ({})",
            self.PORTAL_NAME, total_yielded, municipio, operation,
        )

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_item(self, item: dict, municipio: str, operation: str) -> dict | None:
        try:
            ext_id = str(item.get("posting_id") or item.get("id") or "")
            if not ext_id:
                return None

            location = item.get("location") or {}
            price_data = item.get("price_operation") or item.get("prices") or {}
            features = item.get("main_features") or {}
            amenities_raw = item.get("amenities") or []

            price = None
            currency = "MXN"
            if isinstance(price_data, list) and price_data:
                price = price_data[0].get("price")
                currency = price_data[0].get("currency", "MXN")
            elif isinstance(price_data, dict):
                price = price_data.get("price") or price_data.get("amount")
                currency = price_data.get("currency", "MXN")

            prop_type_code = str(
                item.get("property_type", {}).get("id", "")
                if isinstance(item.get("property_type"), dict)
                else item.get("property_type", "")
            )

            raw = {
                "portal": self.PORTAL_NAME,
                "external_id": ext_id,
                "url": item.get("url") or f"{_CFG['listing_url']}/propiedades/{ext_id}",
                "operation": operation,
                "property_type": _PROP_TYPE_MAP.get(prop_type_code, prop_type_code or None),
                "municipio": location.get("city") or municipio,
                "colonia": location.get("neighborhood"),
                "address": location.get("address"),
                "latitude": location.get("lat"),
                "longitude": location.get("lon"),
                "price": price,
                "currency": currency,
                "area_total": features.get("total_area_m2"),
                "area_built": features.get("covered_area_m2"),
                "bedrooms": features.get("rooms"),
                "bathrooms": features.get("full_bathrooms"),
                "parking": features.get("parking_lots"),
                "title": item.get("title"),
                "description": item.get("description"),
                "amenities": [
                    a.get("label") or a if isinstance(a, dict) else a
                    for a in amenities_raw
                ],
                "raw": item,
            }
            return self._base_normalize(raw)

        except Exception as exc:
            logger.warning("[{}] Could not parse item: {} — {}", self.PORTAL_NAME, exc, item)
            return None
