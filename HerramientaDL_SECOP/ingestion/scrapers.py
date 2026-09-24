"""Scrapers complementarios de fuentes públicas (SECOP I, procesos TIC, catálogo Datos.gov)."""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Optional
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

from ingestion.http_client import ResilientHttpClient
from ingestion.tic_taxonomy import es_tic_texto, tic_where_procesos, tic_where_secop_i

logger = logging.getLogger("ingestion.scrapers")


@dataclass
class PaginationParams:
    page: int = 1
    page_size: int = 50
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class NormalizedRecord:
    source: str
    external_id: str
    title: Optional[str] = None
    entity: Optional[str] = None
    value: Optional[float] = None
    status: Optional[str] = None
    published_at: Optional[str] = None
    url: Optional[str] = None
    payload: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "external_id": self.external_id,
            "title": self.title,
            "entity": self.entity,
            "value": self.value,
            "status": self.status,
            "published_at": self.published_at,
            "url": self.url,
            "payload": self.payload,
        }


@dataclass
class ScrapePage:
    url: str
    pagination: PaginationParams
    raw: Any
    records: list[NormalizedRecord]
    has_more: bool = False
    error: Optional[str] = None


class BaseWebScraper(ABC):
    """Contrato común: URL + paginación in → registros normalizados out."""

    name: str = "base"

    def __init__(self, http: ResilientHttpClient):
        self.http = http

    def build_url(self, url: str, pagination: PaginationParams) -> str:
        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query.update(
            {
                "page": str(pagination.page),
                "page_size": str(pagination.page_size),
                **{k: str(v) for k, v in pagination.extra.items()},
            }
        )
        return urlunparse(parsed._replace(query=urlencode(query)))

    @abstractmethod
    def parse(self, raw: Any, url: str, pagination: PaginationParams) -> ScrapePage:
        raise NotImplementedError

    def fetch_page(self, url: str, pagination: PaginationParams) -> ScrapePage:
        target = self.build_url(url, pagination)
        logger.info("Scraper %s GET %s", self.name, target)
        try:
            response = self.http.get_text(target)
            return self.parse(response.payload, target, pagination)
        except Exception as exc:
            logger.error("Scraper %s falló url=%s error=%s", self.name, target, exc)
            return ScrapePage(
                url=target,
                pagination=pagination,
                raw=None,
                records=[],
                has_more=False,
                error=f"{type(exc).__name__}: {exc}",
            )

    def iter_pages(
        self, url: str, pagination: Optional[PaginationParams] = None, max_pages: int = 1
    ) -> Iterable[ScrapePage]:
        current = pagination or PaginationParams()
        for _ in range(max_pages):
            page = self.fetch_page(url, current)
            yield page
            if page.error or not page.has_more:
                return
            current = PaginationParams(
                page=current.page + 1,
                page_size=current.page_size,
                extra=current.extra,
            )


class PlaceholderHtmlScraper(BaseWebScraper):
    """Plantilla lista para un parser HTML/JSON concreto."""

    name = "placeholder_html"

    def parse(self, raw: Any, url: str, pagination: PaginationParams) -> ScrapePage:
        logger.info(
            "PlaceholderHtmlScraper: interfaz lista. Implementar parse() para la fuente %s",
            url,
        )
        return ScrapePage(
            url=url,
            pagination=pagination,
            raw=raw,
            records=[],
            has_more=False,
        )


class SocrataJsonScraper(BaseWebScraper):
    """Scraper JSON sobre un recurso SODA, con filtro TIC en cliente."""

    name = "socrata_json"
    source = "socrata"
    id_fields: tuple[str, ...] = ("id",)
    title_fields: tuple[str, ...] = ("nombre",)
    entity_fields: tuple[str, ...] = ("entidad",)
    value_fields: tuple[str, ...] = ("valor",)
    status_fields: tuple[str, ...] = ("estado",)
    date_fields: tuple[str, ...] = ("fecha",)
    text_fields: tuple[str, ...] = ()

    def build_url(self, url: str, pagination: PaginationParams) -> str:
        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        offset = (pagination.page - 1) * pagination.page_size
        query["$limit"] = str(pagination.page_size)
        query["$offset"] = str(offset)
        query.setdefault("$order", ":id")
        if pagination.extra.get("where"):
            query["$where"] = str(pagination.extra["where"])
        if pagination.extra.get("q"):
            query["$q"] = str(pagination.extra["q"])
        return urlunparse(parsed._replace(query=urlencode(query)))

    def fetch_page(self, url: str, pagination: PaginationParams) -> ScrapePage:
        target = self.build_url(url, pagination)
        logger.info("Scraper %s GET %s", self.name, target)
        try:
            response = self.http.get_json(target)
            return self.parse(response.payload, target, pagination)
        except Exception as exc:
            logger.error("Scraper %s falló url=%s error=%s", self.name, target, exc)
            return ScrapePage(
                url=target,
                pagination=pagination,
                raw=None,
                records=[],
                has_more=False,
                error=f"{type(exc).__name__}: {exc}",
            )

    def _pick(self, rec: dict, fields: tuple[str, ...]) -> Optional[str]:
        for f in fields:
            if rec.get(f) not in (None, ""):
                return str(rec[f])
        return None

    def parse(self, raw: Any, url: str, pagination: PaginationParams) -> ScrapePage:
        rows = raw if isinstance(raw, list) else []
        records: list[NormalizedRecord] = []
        for rec in rows:
            texts = [str(rec.get(f) or "") for f in self.text_fields] or [
                json.dumps(rec, ensure_ascii=False, default=str)
            ]
            if self.text_fields and not es_tic_texto(*texts):
                continue
            ext_id = self._pick(rec, self.id_fields)
            if not ext_id:
                continue
            val_raw = self._pick(rec, self.value_fields)
            try:
                value = float(str(val_raw).replace(",", "")) if val_raw else None
            except ValueError:
                value = None
            records.append(
                NormalizedRecord(
                    source=self.source,
                    external_id=ext_id,
                    title=(self._pick(rec, self.title_fields) or "")[:500],
                    entity=self._pick(rec, self.entity_fields),
                    value=value,
                    status=self._pick(rec, self.status_fields),
                    published_at=self._pick(rec, self.date_fields),
                    url=url,
                    payload=rec,
                )
            )
        return ScrapePage(
            url=url,
            pagination=pagination,
            raw=raw,
            records=records,
            has_more=len(rows) >= pagination.page_size,
        )


class SecopITicScraper(SocrataJsonScraper):
    name = "secop_i_tic"
    source = "secop_i"
    id_fields = ("uid", "numero_de_proceso")
    title_fields = ("objeto_a_contratar", "detalle_del_objeto_a_contratar")
    entity_fields = ("nombre_entidad",)
    value_fields = ("cuantia_contrato",)
    status_fields = ("estado_del_proceso",)
    date_fields = ("fecha_de_firma_del_contrato",)
    text_fields = ("objeto_a_contratar", "detalle_del_objeto_a_contratar")

    def build_url(self, url: str, pagination: PaginationParams) -> str:
        extra = dict(pagination.extra)
        extra.setdefault("where", tic_where_secop_i())
        pagination = PaginationParams(
            page=pagination.page, page_size=pagination.page_size, extra=extra
        )
        return super().build_url(url, pagination)


class ProcesosTicScraper(SocrataJsonScraper):
    name = "procesos_tic"
    source = "procesos_secop_ii"
    id_fields = ("id_del_proceso", "referencia_del_proceso")
    title_fields = ("nombre_del_procedimiento",)
    entity_fields = ("entidad",)
    value_fields = ("precio_base",)
    status_fields = ("estado_del_procedimiento",)
    date_fields = ("fecha_de_publicacion_del",)
    text_fields = ("nombre_del_procedimiento",)

    def build_url(self, url: str, pagination: PaginationParams) -> str:
        extra = dict(pagination.extra)
        extra.setdefault(
            "where",
            "nombre_del_procedimiento IS NOT NULL AND "
            "(lower(nombre_del_procedimiento) like '%software%' OR "
            "lower(nombre_del_procedimiento) like '%tecnolog%' OR "
            "lower(nombre_del_procedimiento) like '%informatic%')",
        )
        pagination = PaginationParams(
            page=pagination.page, page_size=pagination.page_size, extra=extra
        )
        return super().build_url(url, pagination)


class DatosGovCatalogScraper(SocrataJsonScraper):
    """Catálogo de datasets de datos.gov.co relacionados con contratación."""

    name = "datos_gov_catalog"
    source = "datos_gov_catalog"
    id_fields = ("id",)
    title_fields = ("name",)
    entity_fields = ("attribution",)
    date_fields = ("updatedAt", "createdAt")
    text_fields = ()

    def build_url(self, url: str, pagination: PaginationParams) -> str:
        parsed = urlparse(url)
        offset = (pagination.page - 1) * pagination.page_size
        query = {
            "limit": str(pagination.page_size),
            "offset": str(offset),
            "q": pagination.extra.get("q", "SECOP contratación"),
        }
        return urlunparse(parsed._replace(query=urlencode(query)))


def default_scrapers(http: ResilientHttpClient) -> dict[str, BaseWebScraper]:
    return {
        PlaceholderHtmlScraper.name: PlaceholderHtmlScraper(http),
        SecopITicScraper.name: SecopITicScraper(http),
        ProcesosTicScraper.name: ProcesosTicScraper(http),
        DatosGovCatalogScraper.name: DatosGovCatalogScraper(http),
    }
