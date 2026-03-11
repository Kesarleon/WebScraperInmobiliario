"""Unified CLI for the Colima real-estate scraper.

Subcommands:
  init-db    Create database tables
  scrape     Run the scraping pipeline immediately
  schedule   Start the APScheduler daemon (6AM / 6PM MX)
  analyze    Run market analysis and optionally export to Excel
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from loguru import logger
from rich.console import Console
from rich.table import Table

from config import LOG_FILE, LOG_LEVEL, LOG_RETENTION, LOG_ROTATION
from pipeline import SCRAPER_REGISTRY

console = Console()


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool = False) -> None:
    level = "DEBUG" if verbose else LOG_LEVEL
    logger.remove()
    logger.add(sys.stderr, level=level, colorize=True, format="<level>{message}</level>")
    logger.add(
        str(LOG_FILE),
        level="DEBUG",
        rotation=LOG_ROTATION,
        retention=LOG_RETENTION,
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def cmd_init_db(_args: argparse.Namespace) -> None:
    """Create all database tables."""
    from db.database import init_db
    init_db()
    console.print("[bold green]Database initialized.[/bold green]")


def cmd_scrape(args: argparse.Namespace) -> None:
    """Run the scraping pipeline once."""
    portals = args.portals or None
    operations = args.operations or ["venta", "renta"]

    from pipeline import run_pipeline

    summary = asyncio.run(run_pipeline(portals=portals, operations=operations))

    # Pretty-print results table
    table = Table(title="Scraping Summary", show_lines=True)
    table.add_column("Portal", style="cyan")
    table.add_column("Fetched", justify="right")
    table.add_column("Inserted", justify="right", style="green")
    table.add_column("Updated", justify="right", style="yellow")
    table.add_column("Errors", justify="right", style="red")

    for portal, stats in summary.items():
        if portal == "_total":
            continue
        table.add_row(
            portal,
            str(stats["fetched"]),
            str(stats["inserted"]),
            str(stats["updated"]),
            str(stats["errors"]),
        )

    total = summary.get("_total", {})
    table.add_row(
        "[bold]TOTAL[/bold]",
        str(total.get("fetched", 0)),
        str(total.get("inserted", 0)),
        str(total.get("updated", 0)),
        str(total.get("errors", 0)),
    )

    console.print(table)
    status = total.get("status", "unknown")
    color = "green" if status == "success" else ("yellow" if status == "partial" else "red")
    console.print(f"Status: [{color}]{status}[/{color}]")


def cmd_schedule(args: argparse.Namespace) -> None:
    """Start the scheduler daemon."""
    portals = args.portals or None
    operations = args.operations or ["venta", "renta"]

    from scheduler import start
    start(portals=portals, operations=operations)


def cmd_analyze(args: argparse.Namespace) -> None:
    """Run market analysis and display / export results."""
    from rich.pretty import pprint

    operation = args.operation

    if args.report == "median":
        from analysis.market import median_price_by_municipio
        df = median_price_by_municipio(operation=operation, property_type=args.property_type)
        _print_df(df, title=f"Precio Mediano por Municipio ({operation})")

    elif args.report == "ppm2":
        from analysis.market import price_per_m2_stats
        df = price_per_m2_stats(operation=operation, property_type=args.property_type)
        _print_df(df, title=f"Precio por m² por Municipio ({operation})")

    elif args.report == "amenities":
        from analysis.market import amenity_impact
        df = amenity_impact(operation=operation)
        _print_df(df, title=f"Impacto de Amenidades en Precio ({operation})")

    elif args.report == "history":
        from analysis.market import price_history_evolution
        df = price_history_evolution(
            municipio=args.municipio,
            operation=operation,
            freq=args.freq,
        )
        _print_df(df, title=f"Evolución Histórica de Precios ({operation})")

    elif args.report == "export":
        from analysis.market import export_summary
        path = export_summary(operation=operation)
        console.print(f"[bold green]Exported to:[/bold green] {path}")
    else:
        console.print("[red]Unknown report type.[/red] Use: median | ppm2 | amenities | history | export")


def _print_df(df, title: str = "") -> None:
    """Print a DataFrame as a Rich table."""
    if df is None or df.empty:
        console.print(f"[yellow]No data available for: {title}[/yellow]")
        return

    table = Table(title=title, show_lines=False)
    for col in df.columns:
        table.add_column(str(col))
    for _, row in df.iterrows():
        table.add_row(*[str(v) if v is not None else "" for v in row])
    console.print(table)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scraper",
        description="Colima Real-Estate Scraper CLI",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG logging",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # init-db
    sub.add_parser("init-db", help="Create database tables")

    # scrape
    p_scrape = sub.add_parser("scrape", help="Run scraping pipeline now")
    p_scrape.add_argument(
        "--portals",
        nargs="+",
        choices=list(SCRAPER_REGISTRY.keys()),
        help="Portals to scrape (default: all)",
    )
    p_scrape.add_argument(
        "--operations",
        nargs="+",
        choices=["venta", "renta"],
        default=["venta", "renta"],
        help="Operations to fetch (default: venta renta)",
    )

    # schedule
    p_sched = sub.add_parser("schedule", help="Start APScheduler daemon")
    p_sched.add_argument(
        "--portals",
        nargs="+",
        choices=list(SCRAPER_REGISTRY.keys()),
        help="Portals to scrape (default: all)",
    )
    p_sched.add_argument(
        "--operations",
        nargs="+",
        choices=["venta", "renta"],
        default=["venta", "renta"],
    )

    # analyze
    p_an = sub.add_parser("analyze", help="Run market analysis")
    p_an.add_argument(
        "report",
        choices=["median", "ppm2", "amenities", "history", "export"],
        help=(
            "median=precio mediano por municipio, ppm2=precio/m², "
            "amenities=impacto amenidades, history=evolución histórica, "
            "export=exportar Excel"
        ),
    )
    p_an.add_argument(
        "--operation",
        choices=["venta", "renta"],
        default="venta",
    )
    p_an.add_argument(
        "--property-type",
        dest="property_type",
        default=None,
        help="Filter by property type (e.g. casa, departamento)",
    )
    p_an.add_argument(
        "--municipio",
        default=None,
        help="Filter by municipio (for history report)",
    )
    p_an.add_argument(
        "--freq",
        default="W",
        help="Resample frequency for history (default: W = weekly)",
    )

    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    _setup_logging(verbose=args.verbose)

    dispatch = {
        "init-db": cmd_init_db,
        "scrape": cmd_scrape,
        "schedule": cmd_schedule,
        "analyze": cmd_analyze,
    }

    handler = dispatch.get(args.command)
    if handler is None:
        parser.print_help()
        sys.exit(1)

    handler(args)


if __name__ == "__main__":
    main()
