"""APScheduler-based scheduler: runs the pipeline at 6AM and 6PM Mexico City time."""

from __future__ import annotations

import asyncio
import signal
import sys

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from rich.console import Console

from config import SCHEDULER as SCHED_CFG
from pipeline import SCRAPER_REGISTRY, run_pipeline

console = Console()

# ---------------------------------------------------------------------------
# Scheduled job
# ---------------------------------------------------------------------------

async def scheduled_scrape(
    portals: list[str] | None = None,
    operations: list[str] | None = None,
) -> None:
    """Async job executed by APScheduler."""
    logger.info("Scheduled scrape triggered.")
    try:
        summary = await run_pipeline(portals=portals, operations=operations)
        total = summary.get("_total", {})
        logger.success(
            "Scheduled scrape finished: fetched={} inserted={} updated={} errors={} status={}",
            total.get("fetched"),
            total.get("inserted"),
            total.get("updated"),
            total.get("errors"),
            total.get("status"),
        )
    except Exception as exc:
        logger.exception("Scheduled scrape failed: {}", exc)


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------

def build_scheduler(
    portals: list[str] | None = None,
    operations: list[str] | None = None,
) -> AsyncIOScheduler:
    """Create and configure the APScheduler instance."""
    timezone = SCHED_CFG["timezone"]
    scheduler = AsyncIOScheduler(timezone=timezone)

    for job_cfg in SCHED_CFG["jobs"]:
        scheduler.add_job(
            scheduled_scrape,
            trigger=CronTrigger(
                hour=job_cfg["hour"],
                minute=job_cfg["minute"],
                timezone=timezone,
            ),
            id=job_cfg["id"],
            name=f"Scrape {job_cfg['hour']:02d}:{job_cfg['minute']:02d} {timezone}",
            kwargs={"portals": portals, "operations": operations},
            misfire_grace_time=SCHED_CFG["misfire_grace_time"],
            coalesce=SCHED_CFG["coalesce"],
            replace_existing=True,
        )
        logger.info(
            "Scheduled job '{}' at {:02d}:{:02d} {}",
            job_cfg["id"],
            job_cfg["hour"],
            job_cfg["minute"],
            timezone,
        )

    return scheduler


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def start(
    portals: list[str] | None = None,
    operations: list[str] | None = None,
) -> None:
    """Start the scheduler and block until interrupted."""
    scheduler = build_scheduler(portals=portals, operations=operations)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown(sig: signal.Signals) -> None:
        console.print(f"\n[yellow]Signal {sig.name} received — shutting down scheduler…[/yellow]")
        scheduler.shutdown(wait=False)
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    scheduler.start()
    console.print(
        "[bold green]Scheduler running.[/bold green] "
        "Jobs: 6:00 AM & 6:00 PM (America/Mexico_City). "
        "Press Ctrl+C to stop."
    )

    # Print next run times
    for job in scheduler.get_jobs():
        console.print(f"  • [cyan]{job.name}[/cyan] — next run: {job.next_run_time}")

    try:
        loop.run_forever()
    finally:
        loop.close()
        console.print("[bold red]Scheduler stopped.[/bold red]")
