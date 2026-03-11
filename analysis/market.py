"""Market analysis functions using Pandas.

Provides:
- median_price_by_municipio: precio mediano por municipio y tipo de operación
- price_per_m2_stats: estadísticas de precio/m² por municipio
- amenity_impact: impacto de amenidades en el precio mediano
- price_history_evolution: evolución histórica de precios a partir de PriceHistory
- export_summary: exporta resumen a CSV/Excel
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import select

from config import ANALYSIS, EXPORTS_DIR
from db.database import SessionLocal
from db.models import Listing, PriceHistory


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_listings(
    operation: str | None = None,
    active_only: bool = True,
) -> pd.DataFrame:
    """Load listings from the database into a DataFrame."""
    with SessionLocal() as session:
        stmt = select(Listing)
        if operation:
            stmt = stmt.where(Listing.operation == operation)
        if active_only:
            stmt = stmt.where(Listing.is_active.is_(True))
        rows = session.scalars(stmt).all()

    if not rows:
        return pd.DataFrame()

    records = [
        {
            "id": r.id,
            "portal": r.portal,
            "operation": r.operation,
            "property_type": r.property_type,
            "municipio": r.municipio,
            "colonia": r.colonia,
            "price": r.price,
            "currency": r.currency,
            "price_per_m2": r.price_per_m2,
            "area_total": r.area_total,
            "area_built": r.area_built,
            "bedrooms": r.bedrooms,
            "bathrooms": r.bathrooms,
            "parking": r.parking,
            "amenities": r.amenities or [],
            "first_seen": r.first_seen,
            "last_seen": r.last_seen,
        }
        for r in rows
    ]
    df = pd.DataFrame(records)
    df = _remove_price_outliers(df)
    return df


def _remove_price_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows whose price is more than N std deviations from the mean."""
    if df.empty or "price" not in df.columns:
        return df
    std_thresh = ANALYSIS["price_outlier_std"]
    mean = df["price"].mean()
    std = df["price"].std()
    if std == 0 or pd.isna(std):
        return df
    mask = (df["price"] - mean).abs() <= std_thresh * std
    removed = (~mask).sum()
    if removed:
        logger.debug("Removed {} price outliers (>{} σ)", removed, std_thresh)
    return df[mask].copy()


def _load_price_history() -> pd.DataFrame:
    """Load full PriceHistory table."""
    with SessionLocal() as session:
        stmt = select(
            PriceHistory.listing_id,
            PriceHistory.price,
            PriceHistory.currency,
            PriceHistory.recorded_at,
            Listing.municipio,
            Listing.operation,
            Listing.property_type,
            Listing.portal,
        ).join(Listing, PriceHistory.listing_id == Listing.id)
        rows = session.execute(stmt).all()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "listing_id", "price", "currency", "recorded_at",
        "municipio", "operation", "property_type", "portal",
    ])
    df["recorded_at"] = pd.to_datetime(df["recorded_at"], utc=True)
    return df


# ---------------------------------------------------------------------------
# Analysis functions
# ---------------------------------------------------------------------------

def median_price_by_municipio(
    operation: str = "venta",
    property_type: str | None = None,
) -> pd.DataFrame:
    """Return median listing price grouped by municipio.

    Args:
        operation:     "venta" or "renta"
        property_type: Optional filter (e.g. "casa", "departamento")

    Returns:
        DataFrame with columns: municipio, count, median_price, mean_price,
        min_price, max_price, std_price
    """
    df = _load_listings(operation=operation)
    if df.empty:
        return pd.DataFrame()

    if property_type:
        df = df[df["property_type"] == property_type]

    min_n = ANALYSIS["min_listings_for_stats"]
    agg = (
        df.groupby("municipio")["price"]
        .agg(
            count="count",
            median_price="median",
            mean_price="mean",
            min_price="min",
            max_price="max",
            std_price="std",
        )
        .reset_index()
    )
    agg = agg[agg["count"] >= min_n].sort_values("median_price", ascending=False)

    logger.info(
        "median_price_by_municipio: {} municipios with >= {} listings ({})",
        len(agg), min_n, operation,
    )
    return agg.round(2)


def price_per_m2_stats(
    operation: str = "venta",
    property_type: str | None = None,
) -> pd.DataFrame:
    """Return price-per-m² statistics grouped by municipio.

    Returns:
        DataFrame with columns: municipio, count, median_ppm2, mean_ppm2,
        min_ppm2, max_ppm2
    """
    df = _load_listings(operation=operation)
    if df.empty:
        return pd.DataFrame()

    df = df.dropna(subset=["price_per_m2"])
    if property_type:
        df = df[df["property_type"] == property_type]

    # Additional outlier pass on price_per_m2
    mean = df["price_per_m2"].mean()
    std = df["price_per_m2"].std()
    if std > 0:
        df = df[(df["price_per_m2"] - mean).abs() <= ANALYSIS["price_outlier_std"] * std]

    min_n = ANALYSIS["min_listings_for_stats"]
    agg = (
        df.groupby("municipio")["price_per_m2"]
        .agg(
            count="count",
            median_ppm2="median",
            mean_ppm2="mean",
            min_ppm2="min",
            max_ppm2="max",
        )
        .reset_index()
    )
    agg = agg[agg["count"] >= min_n].sort_values("median_ppm2", ascending=False)
    return agg.round(2)


def amenity_impact(
    operation: str = "venta",
    amenities: list[str] | None = None,
) -> pd.DataFrame:
    """Calculate the price premium (%) associated with each amenity.

    For each amenity, compares the median price of listings that have it
    versus those that don't.

    Returns:
        DataFrame with columns: amenity, listings_with, listings_without,
        median_with, median_without, premium_pct
    """
    df = _load_listings(operation=operation)
    if df.empty:
        return pd.DataFrame()

    amenity_list = amenities or ANALYSIS["amenities"]
    results = []

    for amenity in amenity_list:
        # Explode amenities list and do a case-insensitive substring match
        mask = df["amenities"].apply(
            lambda lst: any(amenity.lower() in str(a).lower() for a in (lst or []))
        )
        with_am = df[mask]["price"]
        without_am = df[~mask]["price"]

        if len(with_am) < ANALYSIS["min_listings_for_stats"]:
            continue

        med_with = with_am.median()
        med_without = without_am.median()
        premium = (
            ((med_with - med_without) / med_without * 100)
            if med_without and med_without > 0
            else np.nan
        )

        results.append({
            "amenity": amenity,
            "listings_with": len(with_am),
            "listings_without": len(without_am),
            "median_with": round(med_with, 2),
            "median_without": round(med_without, 2) if not pd.isna(med_without) else None,
            "premium_pct": round(premium, 2) if not pd.isna(premium) else None,
        })

    return pd.DataFrame(results).sort_values("premium_pct", ascending=False)


def price_history_evolution(
    municipio: str | None = None,
    operation: str = "venta",
    freq: str = "W",
) -> pd.DataFrame:
    """Return the median price evolution over time.

    Args:
        municipio:  Optional filter by municipio.
        operation:  "venta" or "renta".
        freq:       Pandas offset alias for resampling (default "W" = weekly).

    Returns:
        DataFrame indexed by period with columns: median_price, count,
        municipio (if no filter), operation.
    """
    df = _load_price_history()
    if df.empty:
        return pd.DataFrame()

    df = df[df["operation"] == operation]
    if municipio:
        df = df[df["municipio"] == municipio]

    if df.empty:
        return pd.DataFrame()

    df = df.set_index("recorded_at").sort_index()

    group_cols = ["municipio"] if not municipio else []
    agg_parts = []

    if group_cols:
        for mun, grp in df.groupby("municipio"):
            resampled = grp["price"].resample(freq).agg(
                median_price="median",
                count="count",
            )
            resampled["municipio"] = mun
            agg_parts.append(resampled)
        if not agg_parts:
            return pd.DataFrame()
        result = pd.concat(agg_parts).reset_index()
    else:
        resampled = df["price"].resample(freq).agg(
            median_price="median",
            count="count",
        )
        resampled["municipio"] = municipio
        result = resampled.reset_index()

    result["operation"] = operation
    result = result.dropna(subset=["median_price"])
    return result.round(2)


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_summary(
    operation: str = "venta",
    output_dir: Path | None = None,
) -> Path:
    """Export analysis tables to an Excel workbook.

    Returns the path of the generated file.
    """
    output_dir = output_dir or EXPORTS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"market_analysis_{operation}_{timestamp}.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        median_price_by_municipio(operation).to_excel(
            writer, sheet_name="Precio_por_Municipio", index=False
        )
        price_per_m2_stats(operation).to_excel(
            writer, sheet_name="Precio_m2", index=False
        )
        amenity_impact(operation).to_excel(
            writer, sheet_name="Impacto_Amenidades", index=False
        )
        price_history_evolution(operation=operation).to_excel(
            writer, sheet_name="Evolucion_Historica", index=False
        )

    logger.success("Analysis exported to {}", out_path)
    return out_path
