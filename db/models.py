"""SQLAlchemy ORM models for the real-estate scraper."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------

class Listing(Base):
    """A real-estate listing scraped from any portal."""

    __tablename__ = "listings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # -- Identity --
    portal: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    external_id: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)

    # -- Classification --
    operation: Mapped[str | None] = mapped_column(
        String(32), nullable=True, index=True
    )  # venta | renta
    property_type: Mapped[str | None] = mapped_column(
        String(64), nullable=True, index=True
    )  # casa | departamento | terreno | local | oficina | ...

    # -- Location --
    state: Mapped[str | None] = mapped_column(String(128), nullable=True)
    municipio: Mapped[str | None] = mapped_column(
        String(128), nullable=True, index=True
    )
    colonia: Mapped[str | None] = mapped_column(String(256), nullable=True)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)

    # -- Price --
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    price_per_m2: Mapped[float | None] = mapped_column(Float, nullable=True)

    # -- Dimensions --
    area_total: Mapped[float | None] = mapped_column(Float, nullable=True)   # m²
    area_built: Mapped[float | None] = mapped_column(Float, nullable=True)   # m²
    bedrooms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bathrooms: Mapped[float | None] = mapped_column(Float, nullable=True)
    parking: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # -- Description --
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # -- Amenities (list stored as JSON) --
    amenities: Mapped[list | None] = mapped_column(JSON, nullable=True)

    # -- Status --
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    first_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )
    last_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    # -- Raw payload (debugging / future fields) --
    raw: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # -- Relationships --
    price_history: Mapped[list[PriceHistory]] = relationship(
        "PriceHistory", back_populates="listing", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("portal", "external_id", name="uq_portal_external_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<Listing portal={self.portal!r} id={self.external_id!r} "
            f"municipio={self.municipio!r} price={self.price}>"
        )


# ---------------------------------------------------------------------------
# PriceHistory
# ---------------------------------------------------------------------------

class PriceHistory(Base):
    """Records price changes over time for a listing."""

    __tablename__ = "price_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    listing_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("listings.id", ondelete="CASCADE"), nullable=False, index=True
    )
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )

    listing: Mapped[Listing] = relationship("Listing", back_populates="price_history")

    def __repr__(self) -> str:
        return f"<PriceHistory listing_id={self.listing_id} price={self.price} at={self.recorded_at}>"


# ---------------------------------------------------------------------------
# ScrapingRun
# ---------------------------------------------------------------------------

class ScrapingRun(Base):
    """Metadata for each execution of the scraping pipeline."""

    __tablename__ = "scraping_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    portals: Mapped[list | None] = mapped_column(JSON, nullable=True)
    total_fetched: Mapped[int] = mapped_column(Integer, default=0)
    total_inserted: Mapped[int] = mapped_column(Integer, default=0)
    total_updated: Mapped[int] = mapped_column(Integer, default=0)
    total_errors: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(
        String(32), default="running", nullable=False
    )  # running | success | partial | failed
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<ScrapingRun id={self.id} status={self.status!r} "
            f"started={self.started_at}>"
        )
