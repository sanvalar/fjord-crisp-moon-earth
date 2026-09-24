"""Ingesta continua de contratación pública (SECOP / SODA y scrapers)."""

__all__ = ["run_ingestion", "run_tic_update"]


def run_ingestion(**kwargs):
    from ingestion.pipeline import IngestionPipeline

    return IngestionPipeline().run(**kwargs)


def run_tic_update(**kwargs):
    from ingestion.pipeline import IngestionPipeline

    return IngestionPipeline().run_tic_update(**kwargs)
