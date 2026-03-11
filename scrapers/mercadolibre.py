"""Scraper for MercadoLibre using its public search API (MLM-COL)."""

from __future__ import annotations

from typing import AsyncIterator

from loguru import logger

from config import PORTALS, SCRAPING
from scrapers.base import BaseScraper, _safe_float, _safe_int

_CFG = PORTALS["mercadolibre"]

# MercadoLibre Inmuebles category
_CATEGORY = _CFG["category"]
_STATE_ID = _CFG["state_id"]

_OPERATION_MAP = {
    "venta": "242073",   # Venta
    "renta": "242074",   # Renta / Alquiler
}

_PROP_TYPE_MAP = {
    "242075": "casa",
    "242076": "departamento",
    "242077": "terreno",
    "242078": "local_comercial",
    "242079": "oficina",
    "242080": "bodega",
    "242081": "rancho",
    "242082": "edificio",
}


class MercadoLibreScraper(BaseScraper):
    """Fetches listings from MercadoLibre public search API for Colima state."""

    PORTAL_NAME = "mercadolibre"

    async def fetch_listings(
        self,
        municipio: str,
        operation: str = "venta",
    ) -> AsyncIterator[dict]:
        op_id = _OPERATION_MAP.get(operation, _OPERATION_MAP["venta"])
        offset = 0
        total_yielded = 0

        while offset // SCRAPING["page_size"] < SCRAPING["max_pages"]:
            params = {
                "category": _CATEGORY,
                "state": _STATE_ID,
                "q": municipio,
                "OPERATION": op_id,
                "limit": SCRAPING["page_size"],
                "offset": offset,
                "sort": "date_desc",
            }

            try:
                data = await self._get(
                    f"{_CFG['base_url']}{_CFG['search_path']}",
                    params=params,
                )
            except Exception as exc:
                logger.error(
                    "[{}] API error offset={} for {}: {}",
                    self.PORTAL_NAME, offset, municipio, exc,
                )
                break

            results = data.get("results") or []
            if not results:
                break

            for item in results:
                listing = self._parse_item(item, municipio, operation)
                if listing:
                    yield listing
                    total_yielded += 1

            paging = data.get("paging") or {}
            total_count = paging.get("total", 0)
            offset += SCRAPING["page_size"]

            if offset >= total_count or len(results) < SCRAPING["page_size"]:
                break

        logger.debug(
            "[{}] {} listings fetched for {} ({})",
            self.PORTAL_NAME, total_yielded, municipio, operation,
        )

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_item(self, item: dict, municipio: str, operation: str) -> dict | None:
        try:
            ext_id = str(item.get("id") or "")
            if not ext_id:
                return None

            # Extract attributes from the attributes list
            attributes: dict[str, str] = {}
            for attr in item.get("attributes") or []:
                key = attr.get("id", "")
                val = attr.get("value_name") or attr.get("value_id") or ""
                attributes[key] = str(val)

            # Address / location
            address = item.get("address") or {}
            seller_address = item.get("seller_address") or {}
            city = (
                address.get("city", {}).get("name")
                or seller_address.get("city", {}).get("name")
                or municipio
            )
            colonia = (
                address.get("neighborhood", {}).get("name")
                or seller_address.get("neighborhood", {}).get("name")
            )

            # Coordinates
            geo = item.get("geolocation") or {}
            lat = _safe_float(geo.get("latitude"))
            lon = _safe_float(geo.get("longitude"))

            # Price
            price = _safe_float(item.get("price"))
            currency = item.get("currency_id", "MXN")

            # Property type from attributes or category path
            prop_type_raw = (
                attributes.get("PROPERTY_TYPE")
                or attributes.get("ITEM_CONDITION")
            )
            prop_type = None
            for code, name in _PROP_TYPE_MAP.items():
                if prop_type_raw and (prop_type_raw.lower() in name or name in prop_type_raw.lower()):
                    prop_type = name
                    break
            if prop_type is None and prop_type_raw:
                prop_type = prop_type_raw.lower()

            # Dimensions
            area_total = _safe_float(attributes.get("TOTAL_AREA") or attributes.get("LOT_AREA"))
            area_built = _safe_float(attributes.get("COVERED_AREA") or attributes.get("BUILT_AREA"))
            bedrooms = _safe_int(attributes.get("BEDROOMS") or attributes.get("ROOMS"))
            bathrooms = _safe_float(attributes.get("FULL_BATHROOMS") or attributes.get("BATHROOMS"))
            parking = _safe_int(attributes.get("PARKING_LOTS"))

            # Amenities — pick any attribute whose value is "yes" / "Sí"
            amenities = [
                attr.get("name", "")
                for attr in (item.get("attributes") or [])
                if str(attr.get("value_name", "")).lower() in ("yes", "sí", "si", "true", "1")
                and attr.get("name")
            ]

            raw = {
                "portal": self.PORTAL_NAME,
                "external_id": ext_id,
                "url": item.get("permalink") or "",
                "operation": operation,
                "property_type": prop_type,
                "municipio": city,
                "colonia": colonia,
                "address": (
                    address.get("address_line")
                    or seller_address.get("address_line")
                ),
                "latitude": lat,
                "longitude": lon,
                "price": price,
                "currency": currency,
                "area_total": area_total,
                "area_built": area_built,
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "parking": parking,
                "title": item.get("title"),
                "description": None,  # detail endpoint needed for full description
                "amenities": amenities,
                "raw": {k: v for k, v in item.items() if k != "attributes"},
            }
            return self._base_normalize(raw)

        except Exception as exc:
            logger.warning("[{}] Could not parse item: {} — {}", self.PORTAL_NAME, exc, item)
            return None
