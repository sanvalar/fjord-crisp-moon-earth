"""Orquestador de ingesta: SODA incremental + scrapers, por lotes y con bitácora."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence

from ingestion.config import (
    DATASETS,
    INITIAL_LOOKBACK_DAYS,
    MAX_RETRIES,
    PAGE_SIZE,
    REQUEST_TIMEOUT,
    BACKOFF_FACTOR,
    SOCRATA_APP_TOKEN,
)
from ingestion.http_client import ResilientHttpClient
from ingestion.repository import IngestionRepository
from ingestion.scrapers import (
    BaseWebScraper,
    PaginationParams,
    default_scrapers,
)
from ingestion.soda import SodaConnector, iso_socrata
from ingestion.tic_taxonomy import tic_where_procesos, tic_where_secop_ii

logger = logging.getLogger("ingestion.pipeline")


@dataclass
class DatasetRunResult:
    dataset_key: str
    started_at: datetime
    finished_at: datetime
    records_fetched: int = 0
    records_written: int = 0
    records_failed: int = 0
    status: str = "ok"
    error_summary: Optional[str] = None
    failed_batches: list[dict] = field(default_factory=list)


@dataclass
class PipelineResult:
    started_at: datetime
    finished_at: datetime
    datasets: list[DatasetRunResult]
    extra: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return all(item.status == "ok" for item in self.datasets)


class IngestionPipeline:
    def __init__(
        self,
        repository: Optional[IngestionRepository] = None,
        http: Optional[ResilientHttpClient] = None,
        soda: Optional[SodaConnector] = None,
        scrapers: Optional[dict[str, BaseWebScraper]] = None,
    ):
        self.repository = repository or IngestionRepository()
        self.http = http or ResilientHttpClient(
            timeout=REQUEST_TIMEOUT,
            max_retries=MAX_RETRIES,
            backoff_factor=BACKOFF_FACTOR,
            app_token=SOCRATA_APP_TOKEN,
        )
        self.soda = soda or SodaConnector(self.http)
        self.scrapers = scrapers if scrapers is not None else default_scrapers(self.http)

    def run(
        self,
        datasets: Optional[Sequence[str]] = None,
        since: Optional[datetime] = None,
        max_pages: Optional[int] = None,
        page_size: int = PAGE_SIZE,
        scraper_jobs: Optional[list[dict]] = None,
        tic_only: bool = False,
    ) -> PipelineResult:
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        keys = list(datasets) if datasets else [k for k in DATASETS.keys() if k != "secop_i"]
        results: list[DatasetRunResult] = []
        for key in keys:
            try:
                extra = None
                if tic_only and key == "contratos":
                    extra = tic_where_secop_ii()
                elif tic_only and key == "procesos":
                    extra = tic_where_procesos()
                results.append(
                    self._run_dataset(
                        key,
                        since=since,
                        max_pages=max_pages,
                        page_size=page_size,
                        extra_where=extra,
                    )
                )
            except Exception as exc:
                logger.exception("Fallo no controlado en dataset %s: %s", key, exc)
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                results.append(
                    DatasetRunResult(
                        dataset_key=key,
                        started_at=started,
                        finished_at=now,
                        status="error",
                        error_summary=f"{type(exc).__name__}: {exc}",
                    )
                )

        for job in scraper_jobs or []:
            try:
                results.append(self._run_scraper_job(job))
            except Exception as exc:
                logger.exception("Fallo no controlado en scraper %s: %s", job, exc)
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                results.append(
                    DatasetRunResult(
                        dataset_key=f"scraper:{job.get('name', 'unknown')}",
                        started_at=started,
                        finished_at=now,
                        status="error",
                        error_summary=f"{type(exc).__name__}: {exc}",
                    )
                )

        finished = datetime.now(timezone.utc).replace(tzinfo=None)
        self._log_pipeline_summary(started, finished, results)
        return PipelineResult(started_at=started, finished_at=finished, datasets=results)

    def run_tic_update(
        self,
        lookback_days: int = 180,
        max_pages: Optional[int] = 30,
        page_size: int = 500,
        on_progress=None,
    ) -> dict:
        """Ingesta continua TIC con lambda upsert sobre la base de 213.123 contratos."""
        from ingestion.tic_sync import TicSyncJob

        job = TicSyncJob(
            repository=self.repository,
            soda=self.soda,
            on_progress=on_progress,
        )
        return job.run(
            lookback_days=lookback_days,
            max_pages=max_pages,
            page_size=page_size,
            include_scrapers=True,
        )

    def _run_dataset(
        self,
        dataset_key: str,
        since: Optional[datetime],
        max_pages: Optional[int],
        page_size: int,
        extra_where: Optional[str] = None,
    ) -> DatasetRunResult:
        spec = DATASETS[dataset_key]
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        run_id = self.repository.start_run(dataset_key)
        state = self.repository.get_state(dataset_key)
        watermark = state.get("watermark")
        last_pk = state.get("last_pk")
        first_since = since
        if not watermark and first_since is None:
            first_since = started - timedelta(days=INITIAL_LOOKBACK_DAYS)

        fetched = 0
        written = 0
        failed = 0
        failed_batches: list[dict] = []
        newest_watermark = watermark
        newest_pk = last_pk
        status = "ok"
        error_summary = None

        logger.info(
            "Inicio dataset=%s id=%s watermark=%s last_pk=%s since=%s tic_filter=%s",
            dataset_key,
            spec["dataset_id"],
            watermark,
            last_pk,
            iso_socrata(first_since) if first_since and not watermark else None,
            bool(extra_where),
        )

        for page in self.soda.iter_incremental(
            dataset_key=dataset_key,
            watermark=watermark,
            last_pk=last_pk,
            since=None if watermark else first_since,
            extra_where=extra_where,
            max_pages=max_pages,
            page_size=page_size,
        ):
            if page.error:
                failed += 1
                failed_batches.append(
                    {
                        "offset": page.offset,
                        "limit": page.limit,
                        "where": page.where_clause,
                        "error": page.error,
                    }
                )
                status = "partial"
                error_summary = page.error
                logger.error(
                    "Lote interrumpido dataset=%s offset=%s error=%s. "
                    "No se avanza la marca de tiempo más allá de lo ya escrito.",
                    dataset_key,
                    page.offset,
                    page.error,
                )
                break

            fetched += len(page.records)
            try:
                if dataset_key == "contratos":
                    written += self.repository.upsert_contratos(page.records)
                else:
                    written += self.repository.upsert_procesos(page.records)
            except Exception as exc:
                failed += len(page.records)
                msg = f"{type(exc).__name__}: {exc}"
                failed_batches.append(
                    {
                        "offset": page.offset,
                        "limit": page.limit,
                        "where": page.where_clause,
                        "error": msg,
                    }
                )
                status = "partial"
                error_summary = msg
                logger.error("Upsert fallido dataset=%s offset=%s error=%s", dataset_key, page.offset, exc)
                break

            for rec in page.records:
                wm = rec.get(spec["watermark_field"])
                pk = rec.get(spec["pk"])
                if not wm:
                    continue
                if newest_watermark is None or wm > newest_watermark or (
                    wm == newest_watermark and pk and (newest_pk is None or pk > newest_pk)
                ):
                    newest_watermark = wm
                    newest_pk = pk

        if newest_watermark:
            self.repository.set_state(dataset_key, newest_watermark, newest_pk)

        finished = datetime.now(timezone.utc).replace(tzinfo=None)
        self.repository.finish_run(
            run_id,
            records_fetched=fetched,
            records_written=written,
            records_failed=failed,
            status=status,
            error_summary=error_summary,
            failed_batches=failed_batches,
        )
        logger.info(
            "Fin dataset=%s fetched=%s written=%s failed=%s status=%s error=%s",
            dataset_key,
            fetched,
            written,
            failed,
            status,
            error_summary,
        )
        return DatasetRunResult(
            dataset_key=dataset_key,
            started_at=started,
            finished_at=finished,
            records_fetched=fetched,
            records_written=written,
            records_failed=failed,
            status=status,
            error_summary=error_summary,
            failed_batches=failed_batches,
        )

    def _run_scraper_job(self, job: dict) -> DatasetRunResult:
        name = job["name"]
        url = job["url"]
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        run_key = f"scraper:{name}"
        run_id = self.repository.start_run(run_key)
        scraper = self.scrapers.get(name)
        fetched = 0
        written = 0
        failed = 0
        failed_batches: list[dict] = []
        status = "ok"
        error_summary = None

        if scraper is None:
            error_summary = f"Scraper no registrado: {name}"
            status = "error"
            self.repository.finish_run(
                run_id,
                records_fetched=0,
                records_written=0,
                records_failed=1,
                status=status,
                error_summary=error_summary,
                failed_batches=[{"error": error_summary}],
            )
            return DatasetRunResult(
                dataset_key=run_key,
                started_at=started,
                finished_at=datetime.now(timezone.utc).replace(tzinfo=None),
                records_failed=1,
                status=status,
                error_summary=error_summary,
                failed_batches=[{"error": error_summary}],
            )

        pagination = PaginationParams(
            page=int(job.get("page", 1)),
            page_size=int(job.get("page_size", 50)),
            extra=job.get("extra") or {},
        )
        for page in scraper.iter_pages(url, pagination, max_pages=int(job.get("max_pages", 1))):
            if page.error:
                failed += 1
                failed_batches.append({"url": page.url, "error": page.error})
                status = "partial"
                error_summary = page.error
                continue
            records = [item.as_dict() for item in page.records]
            fetched += len(records)
            written += self.repository.upsert_scraped(records)

        finished = datetime.now(timezone.utc).replace(tzinfo=None)
        self.repository.finish_run(
            run_id,
            records_fetched=fetched,
            records_written=written,
            records_failed=failed,
            status=status,
            error_summary=error_summary,
            failed_batches=failed_batches,
        )
        return DatasetRunResult(
            dataset_key=run_key,
            started_at=started,
            finished_at=finished,
            records_fetched=fetched,
            records_written=written,
            records_failed=failed,
            status=status,
            error_summary=error_summary,
            failed_batches=failed_batches,
        )

    def _log_pipeline_summary(
        self, started: datetime, finished: datetime, results: list[DatasetRunResult]
    ):
        logger.info("=" * 64)
        logger.info("RESUMEN DE INGESTA started=%s finished=%s", started.isoformat(), finished.isoformat())
        for item in results:
            logger.info(
                "dataset=%s fetched=%s written=%s failed=%s status=%s error=%s batches_fallidos=%s",
                item.dataset_key,
                item.records_fetched,
                item.records_written,
                item.records_failed,
                item.status,
                item.error_summary,
                item.failed_batches,
            )
        logger.info("=" * 64)
