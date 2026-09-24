"""Programación de ingesta con APScheduler (intervalo 6–24 h, default 12 h)."""
from __future__ import annotations

import logging
import os

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from ingestion.config import SCHEDULE_HOURS
from ingestion.pipeline import IngestionPipeline

logger = logging.getLogger("ingestion.scheduler")

_bg_scheduler: BackgroundScheduler | None = None


def run_scheduled_job(pipeline: IngestionPipeline | None = None, **kwargs):
    pipe = pipeline or IngestionPipeline()
    tic_mode = kwargs.pop("tic_mode", True)
    logger.info("Disparo programado de ingesta (cada %.1f h) tic_mode=%s", SCHEDULE_HOURS, tic_mode)
    if tic_mode:
        return pipe.run_tic_update(
            lookback_days=int(kwargs.get("lookback_days") or 180),
            max_pages=kwargs.get("max_pages") or 20,
            page_size=kwargs.get("page_size") or 500,
        )
    return pipe.run(**kwargs)


def start_scheduler(
    hours: float = SCHEDULE_HOURS,
    run_immediately: bool = True,
    pipeline: IngestionPipeline | None = None,
    **kwargs,
):
    pipe = pipeline or IngestionPipeline()
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_scheduled_job,
        trigger=IntervalTrigger(hours=hours),
        kwargs={"pipeline": pipe, **kwargs},
        id="secop_ingestion",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    logger.info("Scheduler activo: cada %.1f horas", hours)
    if run_immediately:
        run_scheduled_job(pipe, **kwargs)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler detenido")


def start_background_scheduler(
    hours: float | None = None,
    pipeline: IngestionPipeline | None = None,
) -> BackgroundScheduler:
    """Scheduler no bloqueante para el servidor visual."""
    global _bg_scheduler
    if _bg_scheduler and _bg_scheduler.running:
        return _bg_scheduler
    hours = hours if hours is not None else SCHEDULE_HOURS
    pipe = pipeline or IngestionPipeline()
    sched = BackgroundScheduler()
    sched.add_job(
        run_scheduled_job,
        trigger=IntervalTrigger(hours=hours),
        kwargs={"pipeline": pipe, "tic_mode": True},
        id="secop_tic_ingestion",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    sched.start()
    _bg_scheduler = sched
    logger.info("Background scheduler TIC cada %.1f h", hours)
    return sched
