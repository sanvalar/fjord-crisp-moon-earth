"""CLI de ingesta continua SECOP."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.config import INGEST_LOG_FILE, PAGE_SIZE, SCHEDULE_HOURS
from ingestion.pipeline import IngestionPipeline
from ingestion.repository import IngestionRepository
from ingestion.scheduler import start_scheduler
from ingestion.tic_sync import load_meta


def setup_logging():
    INGEST_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(INGEST_LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _parse_since(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"Fecha inválida: {value}")


def cmd_once(args):
    pipeline = IngestionPipeline()
    result = pipeline.run(
        datasets=args.datasets,
        since=_parse_since(args.since),
        max_pages=args.max_pages,
        page_size=args.page_size,
        tic_only=args.tic_only,
    )
    payload = {
        "started_at": result.started_at.isoformat(),
        "finished_at": result.finished_at.isoformat(),
        "datasets": [
            {
                "dataset": item.dataset_key,
                "fetched": item.records_fetched,
                "written": item.records_written,
                "failed": item.records_failed,
                "status": item.status,
                "error": item.error_summary,
                "failed_batches": item.failed_batches,
            }
            for item in result.datasets
        ],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not result.ok:
        sys.exit(2)


def cmd_tic_update(args):
    pipeline = IngestionPipeline()

    def _progress(state):
        msg = state.get("message")
        if msg:
            logging.getLogger("ingestion.cli").info(msg)

    meta = pipeline.run_tic_update(
        lookback_days=args.lookback_days,
        max_pages=args.max_pages,
        page_size=args.page_size,
        on_progress=_progress,
    )
    print(json.dumps(meta, ensure_ascii=False, indent=2, default=str))


def cmd_schedule(args):
    start_scheduler(
        hours=args.hours,
        run_immediately=not args.skip_first,
        datasets=args.datasets,
        max_pages=args.max_pages,
        page_size=args.page_size,
        since=_parse_since(args.since),
        tic_only=args.tic_only,
    )


def cmd_status(_args):
    repo = IngestionRepository()
    print(json.dumps(
        {
            "meta_tic": load_meta(),
            "counts": repo.counts(),
            "runs": [
                {
                    "id": row["id"],
                    "dataset": row["dataset_key"],
                    "started_at": row["started_at"].isoformat() if row["started_at"] else None,
                    "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
                    "fetched": row["records_fetched"],
                    "written": row["records_written"],
                    "failed": row["records_failed"],
                    "status": row["status"],
                    "error": row["error_summary"],
                    "failed_batches": row["failed_batches_json"],
                }
                for row in repo.recent_runs()
            ],
        },
        ensure_ascii=False,
        indent=2,
        default=str,
    ))


def main(argv=None):
    setup_logging()
    parser = argparse.ArgumentParser(
        description="Ingesta continua SECOP II (SODA) + interfaz de scrapers"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    once = sub.add_parser("once", help="Ejecuta una corrida incremental y termina")
    once.add_argument("--datasets", nargs="+", choices=["contratos", "procesos", "secop_i"])
    once.add_argument("--since", help="ISO fecha inicial si no hay watermark (YYYY-MM-DD)")
    once.add_argument("--max-pages", type=int, default=None)
    once.add_argument("--page-size", type=int, default=PAGE_SIZE)
    once.add_argument("--tic-only", action="store_true", help="Aplica filtro de industria TIC")
    once.set_defaults(func=cmd_once)

    tic = sub.add_parser(
        "tic-update",
        help="Actualiza la base TIC (213.123+) con lambda upsert SODA + scrapers",
    )
    tic.add_argument("--lookback-days", type=int, default=180)
    tic.add_argument("--max-pages", type=int, default=30)
    tic.add_argument("--page-size", type=int, default=500)
    tic.set_defaults(func=cmd_tic_update)

    sched = sub.add_parser("schedule", help="Deja el proceso escuchando el intervalo APScheduler")
    sched.add_argument("--hours", type=float, default=SCHEDULE_HOURS)
    sched.add_argument("--skip-first", action="store_true")
    sched.add_argument("--datasets", nargs="+", choices=["contratos", "procesos", "secop_i"])
    sched.add_argument("--since")
    sched.add_argument("--max-pages", type=int, default=None)
    sched.add_argument("--page-size", type=int, default=PAGE_SIZE)
    sched.add_argument("--tic-only", action="store_true")
    sched.set_defaults(func=cmd_schedule)

    status = sub.add_parser("status", help="Muestra conteos y bitácora de corridas")
    status.set_defaults(func=cmd_status)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
