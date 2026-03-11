"""Database session management and helper operations."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator

from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from config import DATABASE_URL
from db.models import Base, Listing, PriceHistory, ScrapingRun


# ---------------------------------------------------------------------------
# Engine & session factory
# ---------------------------------------------------------------------------

engine = create_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_db() -> None:
    """Create all tables if they do not exist."""
    Base.metadata.create_all(bind=engine)
    logger.info("Database initialized at {}", DATABASE_URL)


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Yield a transactional session; rolls back on exception."""
    session: Session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Listing helpers
# ---------------------------------------------------------------------------

def upsert_listing(session: Session, data: dict) -> tuple[Listing, bool]:
    """Insert or update a listing.

    Returns (listing, created) where *created* is True if a new row was inserted.
    Also records a PriceHistory entry when the price changes.
    """
    portal = data["portal"]
    external_id = data["external_id"]

    stmt = select(Listing).where(
        Listing.portal == portal,
        Listing.external_id == external_id,
    )
    existing: Listing | None = session.scalars(stmt).first()

    if existing is None:
        listing = Listing(**data)
        listing.is_active = True
        session.add(listing)
        session.flush()  # populate listing.id

        # Initial price history entry
        if listing.price is not None:
            session.add(
                PriceHistory(
                    listing_id=listing.id,
                    price=listing.price,
                    currency=listing.currency,
                )
            )
        return listing, True

    # --- Update existing ---
    price_changed = (
        data.get("price") is not None
        and existing.price != data.get("price")
    )

    for field, value in data.items():
        if field not in ("id", "first_seen") and value is not None:
            setattr(existing, field, value)

    existing.is_active = True
    existing.last_seen = datetime.now(timezone.utc)

    if price_changed:
        session.add(
            PriceHistory(
                listing_id=existing.id,
                price=data["price"],
                currency=data.get("currency", existing.currency),
            )
        )
        logger.debug(
            "Price change detected for {}/{}: {} -> {}",
            portal,
            external_id,
            existing.price,
            data["price"],
        )

    return existing, False


def mark_inactive(session: Session, portal: str, seen_ids: set[str]) -> int:
    """Mark all listings from *portal* that are NOT in *seen_ids* as inactive.

    Returns the number of rows marked inactive.
    """
    stmt = select(Listing).where(
        Listing.portal == portal,
        Listing.is_active.is_(True),
        Listing.external_id.not_in(seen_ids),
    )
    stale = session.scalars(stmt).all()
    for listing in stale:
        listing.is_active = False
    count = len(stale)
    if count:
        logger.info("Marked {} listings as inactive for portal {}", count, portal)
    return count


# ---------------------------------------------------------------------------
# ScrapingRun helpers
# ---------------------------------------------------------------------------

def create_run(session: Session, portals: list[str]) -> ScrapingRun:
    """Create and persist a new ScrapingRun record."""
    run = ScrapingRun(portals=portals, status="running")
    session.add(run)
    session.flush()
    logger.info("ScrapingRun #{} started for portals: {}", run.id, portals)
    return run


def finish_run(
    session: Session,
    run: ScrapingRun,
    *,
    total_fetched: int = 0,
    total_inserted: int = 0,
    total_updated: int = 0,
    total_errors: int = 0,
    status: str = "success",
    notes: str | None = None,
) -> ScrapingRun:
    """Finalize a ScrapingRun record."""
    run.finished_at = datetime.now(timezone.utc)
    run.total_fetched = total_fetched
    run.total_inserted = total_inserted
    run.total_updated = total_updated
    run.total_errors = total_errors
    run.status = status
    run.notes = notes
    logger.info(
        "ScrapingRun #{} finished: status={} fetched={} inserted={} updated={} errors={}",
        run.id,
        status,
        total_fetched,
        total_inserted,
        total_updated,
        total_errors,
    )
    return run
