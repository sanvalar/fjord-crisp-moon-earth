"""
Ingesta continua TIC: SODA/Socrata + scrapers, con lambda upsert sobre el parquet base.

La base histórica (213.123 contratos) se conserva. Cada corrida:
  - consulta la API pública si el contrato ya existe → lo actualiza
  - si no existe → lo registra
y recarga la vista que consume la interfaz.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import duckdb
import pandas as pd

from ingestion.config import (
    BASE_DIR,
    DATA_DIR,
    DATASETS,
    PAGE_SIZE,
    INITIAL_LOOKBACK_DAYS,
)
from ingestion.http_client import ResilientHttpClient
from ingestion.lambda_upsert import (
    UNIFIED_COLUMNS,
    aplicar_lambda_upsert,
    duckdb_lambda_merge_sql,
    lambda_upsert,
)
from ingestion.repository import IngestionRepository
from ingestion.scrapers import default_scrapers, PaginationParams
from ingestion.soda import SodaConnector, iso_socrata
from ingestion.tic_taxonomy import (
    ANIO_MAX,
    ANIO_MIN,
    MAX_VALOR,
    MIN_VALOR,
    clasificar_subsector,
    es_tic_texto,
    flag_innovacion,
    tic_where_secop_i,
    tic_where_secop_ii,
    tic_where_procesos,
)

logger = logging.getLogger("ingestion.tic_sync")

TIC_PARQUET = DATA_DIR / "parquet" / "SECOP_INDUSTRIA_TIC_2015_2025.parquet"
META_PATH = DATA_DIR / "ingestion_meta.json"
BASE_HISTORICA = 213_123

ProgressCb = Callable[[dict], None]


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _to_float(value) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except ValueError:
        return 0.0


def _to_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _anio_from_fecha(fecha: str, fallback: Optional[int] = None) -> int:
    if fecha and len(str(fecha)) >= 4 and str(fecha)[:4].isdigit():
        return int(str(fecha)[:4])
    return fallback if fallback is not None else 2025


def load_meta() -> dict:
    if META_PATH.exists():
        try:
            return json.loads(META_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "base_historica": BASE_HISTORICA,
        "total_actual": BASE_HISTORICA,
        "insertados_acumulados": 0,
        "actualizados_acumulados": 0,
        "last_run": None,
        "last_status": "idle",
        "sources": [],
    }


def save_meta(meta: dict) -> None:
    META_PATH.parent.mkdir(parents=True, exist_ok=True)
    META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def count_parquet(path: Path = TIC_PARQUET) -> int:
    if not path.exists():
        return 0
    con = duckdb.connect()
    try:
        return int(con.execute(f"SELECT count(*) FROM read_parquet('{path.as_posix()}')").fetchone()[0])
    finally:
        con.close()


def parquet_watermark(path: Path = TIC_PARQUET) -> Optional[str]:
    if not path.exists():
        return None
    con = duckdb.connect()
    try:
        row = con.execute(
            f"SELECT max(fecha_firma) FROM read_parquet('{path.as_posix()}') "
            f"WHERE fecha_firma IS NOT NULL AND fecha_firma <> ''"
        ).fetchone()
        return row[0] if row and row[0] else None
    finally:
        con.close()


def existing_ids(path: Path = TIC_PARQUET) -> set[str]:
    if not path.exists():
        return set()
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT id_contrato_global FROM read_parquet('{path.as_posix()}') "
            f"WHERE id_contrato_global IS NOT NULL"
        ).fetchall()
        return {str(r[0]) for r in rows if r[0]}
    finally:
        con.close()


def normalize_secop_ii(rec: dict) -> Optional[dict]:
    cid = str(rec.get("id_contrato") or rec.get("proceso_de_compra") or "").strip()
    if not cid:
        return None
    objeto = str(rec.get("objeto_del_contrato") or "")
    desc = str(rec.get("descripcion_del_proceso") or "")
    if not es_tic_texto(objeto, desc):
        return None
    valor = _to_float(rec.get("valor_del_contrato"))
    if valor <= MIN_VALOR or valor >= MAX_VALOR:
        return None
    firma = str(rec.get("fecha_de_firma") or "")
    anio = _anio_from_fecha(firma, _to_int(rec.get("anio"), 2025))
    if anio < ANIO_MIN or anio > ANIO_MAX:
        return None
    return {
        "sistema_origen": "SECOP_II",
        "id_contrato_global": cid,
        "proceso_compra": str(rec.get("proceso_de_compra") or ""),
        "nit_entidad": str(rec.get("nit_entidad") or ""),
        "nombre_entidad": str(rec.get("nombre_entidad") or "NO_DEFINIDO"),
        "orden_gobierno": str(rec.get("orden") or "Territorial"),
        "departamento": str(rec.get("departamento") or "NO_DEFINIDO").upper().strip(),
        "municipio": str(rec.get("ciudad") or "NO_DEFINIDO").upper().strip(),
        "modalidad": str(rec.get("modalidad_de_contratacion") or "Contratación Directa"),
        "tipo_contrato": str(rec.get("tipo_de_contrato") or "Prestación de Servicios"),
        "estado_contrato": str(rec.get("estado_contrato") or "Celebrado"),
        "documento_proveedor": str(rec.get("documento_proveedor") or ""),
        "proveedor": str(rec.get("proveedor_adjudicado") or "NO_DEFINIDO"),
        "es_pyme_bin": 1 if str(rec.get("es_pyme", "")).lower() == "si" else 0,
        "fecha_firma": firma,
        "fecha_inicio": str(rec.get("fecha_de_inicio_del_contrato") or ""),
        "fecha_fin": str(rec.get("fecha_de_fin_del_contrato") or ""),
        "anio_contratacion": anio,
        "valor_contrato": valor,
        "valor_facturado": _to_float(rec.get("valor_facturado")) or valor,
        "valor_pagado": _to_float(rec.get("valor_pagado")) or valor,
        "dias_adicionados": _to_int(rec.get("dias_adicionados")),
        "objeto_resumido": desc or objeto,
        "objeto_detallado": objeto or desc,
        "subsector_tic": clasificar_subsector(objeto, desc),
        "flag_innovacion_conpes": flag_innovacion(objeto, desc),
    }


def normalize_secop_i(rec: dict) -> Optional[dict]:
    cid = str(rec.get("uid") or rec.get("numero_de_proceso") or "").strip()
    if not cid:
        return None
    objeto = str(rec.get("objeto_a_contratar") or "")
    detalle = str(rec.get("detalle_del_objeto_a_contratar") or "")
    if not es_tic_texto(objeto, detalle):
        return None
    valor = _to_float(rec.get("cuantia_contrato"))
    if valor <= MIN_VALOR or valor >= MAX_VALOR:
        return None
    anio = _to_int(rec.get("anno_cargue_secop"), _anio_from_fecha(str(rec.get("fecha_de_firma_del_contrato") or "")))
    if anio < ANIO_MIN or anio > ANIO_MAX:
        return None
    return {
        "sistema_origen": "SECOP_I",
        "id_contrato_global": cid,
        "proceso_compra": str(rec.get("numero_de_proceso") or ""),
        "nit_entidad": str(rec.get("nit_de_la_entidad") or ""),
        "nombre_entidad": str(rec.get("nombre_entidad") or "NO_DEFINIDO"),
        "orden_gobierno": str(rec.get("orden_entidad") or "Territorial"),
        "departamento": str(rec.get("departamento_entidad") or "NO_DEFINIDO").upper().strip(),
        "municipio": str(rec.get("municipio_entidad") or "NO_DEFINIDO").upper().strip(),
        "modalidad": str(rec.get("modalidad_de_contratacion") or "Contratación Directa"),
        "tipo_contrato": str(rec.get("tipo_de_contrato") or "Prestación de Servicios"),
        "estado_contrato": str(rec.get("estado_del_proceso") or "Celebrado"),
        "documento_proveedor": str(rec.get("identificacion_del_contratista") or ""),
        "proveedor": str(rec.get("nom_razon_social_contratista") or "NO_DEFINIDO"),
        "es_pyme_bin": 1 if str(rec.get("es_mipyme", "")).lower() == "si" else 0,
        "fecha_firma": str(rec.get("fecha_de_firma_del_contrato") or ""),
        "fecha_inicio": str(rec.get("fecha_ini_ejec_contrato") or ""),
        "fecha_fin": str(rec.get("fecha_fin_ejec_contrato") or ""),
        "anio_contratacion": anio,
        "valor_contrato": valor,
        "valor_facturado": valor,
        "valor_pagado": valor,
        "dias_adicionados": _to_int(rec.get("tiempo_adiciones_en_dias")),
        "objeto_resumido": objeto,
        "objeto_detallado": detalle or objeto,
        "subsector_tic": clasificar_subsector(objeto, detalle),
        "flag_innovacion_conpes": flag_innovacion(objeto, detalle),
    }


class TicSyncJob:
    def __init__(
        self,
        repository: Optional[IngestionRepository] = None,
        soda: Optional[SodaConnector] = None,
        on_progress: Optional[ProgressCb] = None,
        parquet_path: Path = TIC_PARQUET,
    ):
        self.repository = repository or IngestionRepository()
        self.http = ResilientHttpClient(
            timeout=int(os.environ.get("SECOP_REQUEST_TIMEOUT", "45")),
            max_retries=int(os.environ.get("SECOP_MAX_RETRIES", "6")),
            backoff_factor=float(os.environ.get("SECOP_BACKOFF_FACTOR", "2")),
            app_token=os.environ.get("SOCRATA_APP_TOKEN", "").strip(),
            user_agent="SECOP-TIC-Ingestion/2.0",
        )
        self.soda = soda or SodaConnector(self.http)
        self.scrapers = default_scrapers(self.http)
        self.on_progress = on_progress or (lambda _s: None)
        self.parquet_path = parquet_path

    def _emit(self, **kwargs):
        self.on_progress(kwargs)

    def _resolve_since(self, dataset_key: str, lookback_days: int) -> datetime:
        state = self.repository.get_state(f"tic:{dataset_key}")
        watermark = state.get("watermark")
        if watermark:
            try:
                return datetime.fromisoformat(str(watermark).replace("Z", "").split(".")[0])
            except ValueError:
                pass
        wm_pq = parquet_watermark(self.parquet_path)
        if wm_pq:
            try:
                dt = datetime.fromisoformat(str(wm_pq).replace("Z", "").split(".")[0])
                return dt - timedelta(days=lookback_days)
            except ValueError:
                pass
        return _now() - timedelta(days=max(lookback_days, INITIAL_LOOKBACK_DAYS))

    def _fetch_dataset(
        self,
        dataset_key: str,
        extra_where: str,
        since: datetime,
        max_pages: Optional[int],
        page_size: int,
        normalizer,
    ) -> list[dict]:
        rows: list[dict] = []
        fetched_raw = 0
        for page in self.soda.iter_incremental(
            dataset_key=dataset_key,
            watermark=None,
            last_pk=None,
            since=since,
            extra_where=extra_where,
            max_pages=max_pages,
            page_size=page_size,
        ):
            if page.error:
                logger.error("SODA %s error=%s", dataset_key, page.error)
                self._emit(
                    phase=f"error_{dataset_key}",
                    message=f"Error SODA {dataset_key}: {page.error}",
                    error=page.error,
                )
                break
            fetched_raw += len(page.records)
            for rec in page.records:
                norm = normalizer(rec)
                if norm:
                    rows.append(norm)
            self._emit(
                phase=f"soda_{dataset_key}",
                message=(
                    f"SODA {dataset_key}: {fetched_raw:,} leídos, "
                    f"{len(rows):,} TIC válidos (filtro industria)"
                ),
                fetched=fetched_raw,
                tic_valid=len(rows),
            )
        logger.info(
            "Dataset %s since=%s raw=%s tic=%s",
            dataset_key,
            iso_socrata(since),
            fetched_raw,
            len(rows),
        )
        return rows

    def _run_scrapers(self, max_pages: int) -> list[dict]:
        scraped_norm: list[dict] = []
        jobs = [
            {
                "name": "secop_i_tic",
                "url": "https://www.datos.gov.co/resource/f789-7hwg.json",
                "max_pages": max_pages,
                "page_size": 200,
            },
            {
                "name": "procesos_tic",
                "url": "https://www.datos.gov.co/resource/p6dx-8zbt.json",
                "max_pages": max_pages,
                "page_size": 200,
            },
        ]
        for job in jobs:
            scraper = self.scrapers.get(job["name"])
            if scraper is None:
                continue
            run_id = self.repository.start_run(f"scraper:{job['name']}")
            fetched = 0
            written = 0
            failed = 0
            error_summary = None
            pagination = PaginationParams(page=1, page_size=int(job["page_size"]))
            try:
                for page in scraper.iter_pages(job["url"], pagination, max_pages=int(job["max_pages"])):
                    if page.error:
                        failed += 1
                        error_summary = page.error
                        continue
                    records = [item.as_dict() for item in page.records]
                    fetched += len(records)
                    written += self.repository.upsert_scraped(records)
                    for rec in records:
                        payload = rec.get("payload") or rec
                        if rec.get("source") == "secop_i":
                            norm = normalize_secop_i(payload)
                        else:
                            continue
                        if norm:
                            scraped_norm.append(norm)
                self._emit(
                    phase=f"scraper_{job['name']}",
                    message=f"Scraper {job['name']}: {fetched:,} registros",
                    fetched=fetched,
                )
            except Exception as exc:
                error_summary = f"{type(exc).__name__}: {exc}"
                failed += 1
                logger.exception("Scraper %s falló", job["name"])
            self.repository.finish_run(
                run_id,
                records_fetched=fetched,
                records_written=written,
                records_failed=failed,
                status="ok" if not error_summary else "partial",
                error_summary=error_summary,
                failed_batches=[],
            )
        return scraped_norm

    def run(
        self,
        lookback_days: int = 180,
        max_pages: Optional[int] = 30,
        page_size: int = 500,
        include_scrapers: bool = True,
    ) -> dict:
        started = _now()
        self._emit(
            running=True,
            phase="inicio",
            message="Iniciando actualización de contratación pública en TIC (Socrata/SODA)…",
            error=None,
            inserted=0,
            updated=0,
            fetched=0,
        )
        total_antes = count_parquet(self.parquet_path)
        ids = existing_ids(self.parquet_path)

        since_ii = self._resolve_since("contratos", lookback_days)
        since_i = self._resolve_since("secop_i", lookback_days)

        incoming: list[dict] = []

        self._emit(phase="secop_ii", message="Consultando SECOP II (jbjy-vk9h) filtrado TIC…")
        run_ii = self.repository.start_run("tic:contratos")
        ii_rows = self._fetch_dataset(
            "contratos",
            tic_where_secop_ii(),
            since_ii,
            max_pages,
            page_size,
            normalize_secop_ii,
        )
        incoming.extend(ii_rows)
        self.repository.finish_run(
            run_ii,
            records_fetched=len(ii_rows),
            records_written=len(ii_rows),
            records_failed=0,
            status="ok",
            error_summary=None,
            failed_batches=[],
        )

        self._emit(phase="secop_i", message="Consultando SECOP I (f789-7hwg) filtrado TIC…")
        run_i = self.repository.start_run("tic:secop_i")
        i_rows = self._fetch_dataset(
            "secop_i",
            tic_where_secop_i(),
            since_i,
            max_pages,
            page_size,
            normalize_secop_i,
        )
        incoming.extend(i_rows)
        self.repository.finish_run(
            run_i,
            records_fetched=len(i_rows),
            records_written=len(i_rows),
            records_failed=0,
            status="ok",
            error_summary=None,
            failed_batches=[],
        )

        scraped_rows: list[dict] = []
        if include_scrapers:
            self._emit(phase="scrapers", message="Ejecutando scrapers complementarios…")
            scraped_rows = self._run_scrapers(max_pages=max(1, min(max_pages or 5, 5)))
            incoming.extend(scraped_rows)

        # Deduplicar incoming por id, conservando el último
        uniq: dict[str, dict] = {}
        for rec in incoming:
            uniq[rec["id_contrato_global"]] = rec
        incoming = list(uniq.values())

        self._emit(
            phase="lambda_upsert",
            message=f"Aplicando lambda upsert sobre {len(incoming):,} candidatos vs {len(ids):,} existentes…",
            fetched=len(incoming),
        )

        existentes_map = {pk: {} for pk in ids}
        stats = aplicar_lambda_upsert(incoming, existentes_map)
        inserted, updated = stats["inserted"], stats["updated"]

        # Persistencia SQLite (misma lambda: on_conflict update)
        if incoming:
            raw_ii = [
                {
                    "id_contrato": r["id_contrato_global"],
                    "nombre_entidad": r["nombre_entidad"],
                    "nit_entidad": r["nit_entidad"],
                    "departamento": r["departamento"],
                    "ciudad": r["municipio"],
                    "proveedor_adjudicado": r["proveedor"],
                    "documento_proveedor": r["documento_proveedor"],
                    "valor_del_contrato": r["valor_contrato"],
                    "objeto_del_contrato": r["objeto_detallado"],
                    "estado_contrato": r["estado_contrato"],
                    "fecha_de_firma": r["fecha_firma"],
                    "modalidad_de_contratacion": r["modalidad"],
                    "tipo_de_contrato": r["tipo_contrato"],
                    "proceso_de_compra": r["proceso_compra"],
                    "referencia_del_contrato": r["id_contrato_global"],
                }
                for r in incoming
                if r.get("sistema_origen") == "SECOP_II"
            ]
            if raw_ii:
                self.repository.upsert_contratos(raw_ii)

        if incoming and self.parquet_path.exists():
            self._emit(phase="parquet", message="Fusionando con la base histórica Parquet (213.123+)…")
            df = pd.DataFrame(incoming, columns=list(UNIFIED_COLUMNS))
            tmp = self.parquet_path.with_name(self.parquet_path.stem + ".tmp.parquet")
            con = duckdb.connect()
            try:
                con.register("incoming_tic", df)
                con.execute(duckdb_lambda_merge_sql(self.parquet_path.as_posix(), tmp.as_posix()))
            finally:
                con.close()
            os.replace(tmp, self.parquet_path)

        newest_ii = max((r["fecha_firma"] for r in ii_rows if r.get("fecha_firma")), default=None)
        newest_i = max((r["fecha_firma"] for r in i_rows if r.get("fecha_firma")), default=None)
        if newest_ii:
            self.repository.set_state("tic:contratos", newest_ii, None)
        if newest_i:
            self.repository.set_state("tic:secop_i", newest_i, None)

        total_despues = count_parquet(self.parquet_path)
        finished = _now()
        meta = load_meta()
        meta.update(
            {
                "base_historica": BASE_HISTORICA,
                "total_actual": total_despues,
                "total_antes": total_antes,
                "insertados": inserted,
                "actualizados": updated,
                "insertados_acumulados": int(meta.get("insertados_acumulados") or 0) + inserted,
                "actualizados_acumulados": int(meta.get("actualizados_acumulados") or 0) + updated,
                "last_run": finished.isoformat(),
                "last_status": "ok",
                "duration_s": round((finished - started).total_seconds(), 2),
                "sources": [
                    {"name": "SECOP II SODA jbjy-vk9h", "tic": len(ii_rows), "since": iso_socrata(since_ii)},
                    {"name": "SECOP I SODA f789-7hwg", "tic": len(i_rows), "since": iso_socrata(since_i)},
                    {"name": "Scrapers complementarios", "tic": len(scraped_rows)},
                ],
                "lambda": "si existe → UPDATE; si no existe → INSERT",
            }
        )
        save_meta(meta)
        self._emit(
            running=False,
            phase="listo",
            message=(
                f"Actualización completada. Base {BASE_HISTORICA:,} → {total_despues:,} "
                f"(+{inserted:,} nuevos, {updated:,} actualizados)."
            ),
            inserted=inserted,
            updated=updated,
            total=total_despues,
            base_historica=BASE_HISTORICA,
            fetched=len(incoming),
            last_finished_at=finished.isoformat(),
            sources=meta["sources"],
        )
        logger.info(
            "TIC sync done inserted=%s updated=%s total=%s (lambda_upsert=%s)",
            inserted,
            updated,
            total_despues,
            lambda_upsert.__name__ if hasattr(lambda_upsert, "__name__") else "lambda",
        )
        return meta
