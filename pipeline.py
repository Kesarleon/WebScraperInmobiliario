"""Orchestrator: runs all scrapers and persists data with upsert."""

from __future__ import annotations

import asyncio
from typing import Type

from loguru import logger
from tqdm import tqdm

from config import MUNICIPIOS
from db.database import create_run, finish_run, get_session, mark_inactive, upsert_listing
from scrapers.base import BaseScraper
from scrapers.inmuebles24 import Inmuebles24Scraper
from scrapers.lamudi import LamudiScraper
from scrapers.mercadolibre import MercadoLibreScraper
from scrapers.vivanuncios import VivanunciosScraper

# Registry of all available scrapers
SCRAPER_REGISTRY: dict[str, Type[BaseScraper]] = {
    "inmuebles24": Inmuebles24Scraper,
    "lamudi": LamudiScraper,
    "vivanuncios": VivanunciosScraper,
    "mercadolibre": MercadoLibreScraper,
}


async def _run_scraper(
    scraper_cls: Type[BaseScraper],
    operations: list[str],
) -> tuple[list[dict], list[str]]:
    """Run a single scraper for all municipios and operations.

    Returns (listings, errors).
    """
    listings: list[dict] = []
    errors: list[str] = []

    async with scraper_cls() as scraper:
        for operation in operations:
            async for listing in scraper.fetch_all(operation):
                listings.append(listing)

    return listings, errors


def _persist_listings(
    portal: str,
    listings: list[dict],
) -> tuple[int, int, int]:
    """Upsert listings into the DB.

    Returns (inserted, updated, errors).
    """
    inserted = updated = errors = 0
    seen_ids: set[str] = set()

    with get_session() as session:
        with tqdm(
            listings,
            desc=f"  Persisting {portal}",
            unit="listing",
            leave=False,
        ) as pbar:
            for data in pbar:
                try:
                    _, created = upsert_listing(session, data)
                    seen_ids.add(data["external_id"])
                    if created:
                        inserted += 1
                    else:
                        updated += 1
                except Exception as exc:
                    errors += 1
                    logger.warning("[{}] Persist error: {}", portal, exc)

        # Mark stale listings as inactive
        mark_inactive(session, portal, seen_ids)

    return inserted, updated, errors


async def run_pipeline(
    portals: list[str] | None = None,
    operations: list[str] | None = None,
) -> dict:
    """Run the full scraping pipeline.

    Args:
        portals:    List of portal names to scrape (default: all registered).
        operations: List of operation types (default: ["venta", "renta"]).

    Returns:
        Summary dict with counts per portal.
    """
    if portals is None:
        portals = list(SCRAPER_REGISTRY.keys())
    if operations is None:
        operations = ["venta", "renta"]

    # Validate portal names
    unknown = [p for p in portals if p not in SCRAPER_REGISTRY]
    if unknown:
        raise ValueError(f"Unknown portals: {unknown}. Available: {list(SCRAPER_REGISTRY)}")

    logger.info("Pipeline starting. Portals: {} | Operations: {}", portals, operations)

    summary: dict[str, dict] = {}
    total_fetched = total_inserted = total_updated = total_errors = 0

    with get_session() as session:
        run = create_run(session, portals)
        run_id = run.id

    for portal_name in portals:
        scraper_cls = SCRAPER_REGISTRY[portal_name]
        logger.info("Scraping portal: {}", portal_name)

        try:
            listings, scrape_errors = await _run_scraper(scraper_cls, operations)
        except Exception as exc:
            logger.error("Portal {} failed entirely: {}", portal_name, exc)
            summary[portal_name] = {
                "fetched": 0, "inserted": 0, "updated": 0, "errors": 1
            }
            total_errors += 1
            continue

        fetched = len(listings)
        inserted, updated, persist_errors = _persist_listings(portal_name, listings)
        errors = len(scrape_errors) + persist_errors

        summary[portal_name] = {
            "fetched": fetched,
            "inserted": inserted,
            "updated": updated,
            "errors": errors,
        }

        total_fetched += fetched
        total_inserted += inserted
        total_updated += updated
        total_errors += errors

        logger.success(
            "[{}] Done. fetched={} inserted={} updated={} errors={}",
            portal_name, fetched, inserted, updated, errors,
        )

    # Determine overall status
    if total_errors == 0:
        status = "success"
    elif total_inserted + total_updated > 0:
        status = "partial"
    else:
        status = "failed"

    with get_session() as session:
        from sqlalchemy import select
        from db.models import ScrapingRun
        run_obj = session.get(ScrapingRun, run_id)
        if run_obj:
            finish_run(
                session,
                run_obj,
                total_fetched=total_fetched,
                total_inserted=total_inserted,
                total_updated=total_updated,
                total_errors=total_errors,
                status=status,
            )

    summary["_total"] = {
        "fetched": total_fetched,
        "inserted": total_inserted,
        "updated": total_updated,
        "errors": total_errors,
        "status": status,
    }

    logger.info(
        "Pipeline complete. total_fetched={} inserted={} updated={} errors={} status={}",
        total_fetched, total_inserted, total_updated, total_errors, status,
    )
    return summary
