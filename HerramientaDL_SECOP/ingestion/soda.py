"""Conector SODA/Socrata con SoQL, paginación e incremento por marca de tiempo."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional, Sequence

from ingestion.config import DATASETS, PAGE_SIZE, SODA_RESOURCE_URL
from ingestion.http_client import ResilientHttpClient

logger = logging.getLogger("ingestion.soda")


@dataclass
class SodaPage:
    dataset_key: str
    dataset_id: str
    offset: int
    limit: int
    where_clause: str
    records: list[dict]
    error: Optional[str] = None


def iso_socrata(dt: datetime) -> str:
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000")


def build_where(
    require_not_null: Sequence[str],
    watermark_field: str,
    watermark: Optional[str],
    pk_field: str,
    last_pk: Optional[str],
    extra_where: Optional[str] = None,
) -> str:
    """Excluye NULL de fechas (Socrata los ordena primero) y aplica watermark keyset."""
    parts = [f"{field} IS NOT NULL" for field in require_not_null]
    if watermark:
        wm = watermark.replace("'", "''")
        if last_pk:
            pk = last_pk.replace("'", "''")
            parts.append(
                f"(({watermark_field} > '{wm}') OR "
                f"({watermark_field} = '{wm}' AND {pk_field} > '{pk}'))"
            )
        else:
            parts.append(f"{watermark_field} > '{wm}'")
    if extra_where:
        parts.append(f"({extra_where})")
    return " AND ".join(parts)


class SodaConnector:
    def __init__(self, http: ResilientHttpClient, page_size: int = PAGE_SIZE):
        self.http = http
        self.page_size = page_size

    def fetch_page(
        self,
        dataset_key: str,
        where_clause: str,
        order_clause: str,
        limit: int,
        offset: int,
    ) -> SodaPage:
        spec = DATASETS[dataset_key]
        url = SODA_RESOURCE_URL.format(dataset_id=spec["dataset_id"])
        params = {
            "$where": where_clause,
            "$order": order_clause,
            "$limit": str(limit),
            "$offset": str(offset),
        }
        logger.info(
            "SODA GET %s offset=%s limit=%s where=%s",
            spec["dataset_id"],
            offset,
            limit,
            where_clause,
        )
        response = self.http.get_json(url, params=params)
        records = response.payload if isinstance(response.payload, list) else []
        return SodaPage(
            dataset_key=dataset_key,
            dataset_id=spec["dataset_id"],
            offset=offset,
            limit=limit,
            where_clause=where_clause,
            records=records,
        )

    def iter_incremental(
        self,
        dataset_key: str,
        watermark: Optional[str] = None,
        last_pk: Optional[str] = None,
        since: Optional[datetime] = None,
        extra_where: Optional[str] = None,
        max_pages: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> Iterable[SodaPage]:
        spec = DATASETS[dataset_key]
        if since is not None and not watermark:
            since_iso = iso_socrata(since)
            extra = f"{spec['watermark_field']} >= '{since_iso}'"
            extra_where = f"{extra_where} AND {extra}" if extra_where else extra
            last_pk = None

        where_clause = build_where(
            require_not_null=spec["require_not_null"],
            watermark_field=spec["watermark_field"],
            watermark=watermark,
            pk_field=spec["pk"],
            last_pk=last_pk if watermark else None,
            extra_where=extra_where,
        )
        order_clause = ", ".join(f"{field} ASC" for field in spec["order_fields"])
        limit = page_size or self.page_size
        offset = 0
        pages = 0
        while True:
            if max_pages is not None and pages >= max_pages:
                break
            try:
                page = self.fetch_page(
                    dataset_key=dataset_key,
                    where_clause=where_clause,
                    order_clause=order_clause,
                    limit=limit,
                    offset=offset,
                )
            except Exception as exc:
                logger.error(
                    "Lote fallido dataset=%s offset=%s error=%s",
                    dataset_key,
                    offset,
                    exc,
                )
                yield SodaPage(
                    dataset_key=dataset_key,
                    dataset_id=spec["dataset_id"],
                    offset=offset,
                    limit=limit,
                    where_clause=where_clause,
                    records=[],
                    error=f"{type(exc).__name__}: {exc}",
                )
                return
            yield page
            pages += 1
            if len(page.records) < limit:
                break
            offset += limit
