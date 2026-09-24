"""Persistencia relacional con upsert por ID de contrato / proceso."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

from ingestion.config import CONTRATO_COLUMNS, DATABASE_URL, PROCESO_COLUMNS

logger = logging.getLogger("ingestion.repository")
metadata = MetaData()

contratos_electronicos = Table(
    "contratos_electronicos",
    metadata,
    Column("id_contrato", String(64), primary_key=True),
    Column("nombre_entidad", String(512)),
    Column("nit_entidad", String(32)),
    Column("departamento", String(128)),
    Column("ciudad", String(128)),
    Column("proveedor_adjudicado", String(512)),
    Column("documento_proveedor", String(64)),
    Column("valor_del_contrato", Float),
    Column("objeto_del_contrato", Text),
    Column("estado_contrato", String(128)),
    Column("fecha_de_firma", String(40), index=True),
    Column("modalidad_de_contratacion", String(256)),
    Column("tipo_de_contrato", String(256)),
    Column("proceso_de_compra", String(64)),
    Column("referencia_del_contrato", String(128)),
    Column("payload_json", Text),
    Column("ingested_at", DateTime, nullable=False),
)

procesos_contratacion = Table(
    "procesos_contratacion",
    metadata,
    Column("id_del_proceso", String(64), primary_key=True),
    Column("nombre_del_procedimiento", Text),
    Column("entidad", String(512)),
    Column("nit_entidad", String(32)),
    Column("departamento_entidad", String(128)),
    Column("ciudad_entidad", String(128)),
    Column("precio_base", Float),
    Column("nombre_del_proveedor", String(512)),
    Column("modalidad_de_contratacion", String(256)),
    Column("estado_del_procedimiento", String(128)),
    Column("estado_de_apertura_del_proceso", String(128)),
    Column("fecha_de_publicacion_del", String(40), index=True),
    Column("fecha_de_ultima_publicaci", String(40)),
    Column("tipo_de_contrato", String(256)),
    Column("adjudicado", String(32)),
    Column("referencia_del_proceso", String(128)),
    Column("payload_json", Text),
    Column("ingested_at", DateTime, nullable=False),
)

registros_scrapeados = Table(
    "registros_scrapeados",
    metadata,
    Column("source", String(128), primary_key=True),
    Column("external_id", String(256), primary_key=True),
    Column("title", Text),
    Column("entity", String(512)),
    Column("value", Float),
    Column("status", String(128)),
    Column("published_at", String(40)),
    Column("url", Text),
    Column("payload_json", Text),
    Column("ingested_at", DateTime, nullable=False),
)

ingestion_state = Table(
    "ingestion_state",
    metadata,
    Column("dataset_key", String(64), primary_key=True),
    Column("watermark", String(40)),
    Column("last_pk", String(128)),
    Column("updated_at", DateTime, nullable=False),
)

ingestion_runs = Table(
    "ingestion_runs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("started_at", DateTime, nullable=False),
    Column("finished_at", DateTime),
    Column("dataset_key", String(64), nullable=False),
    Column("records_fetched", Integer, default=0),
    Column("records_written", Integer, default=0),
    Column("records_failed", Integer, default=0),
    Column("status", String(32), nullable=False),
    Column("error_summary", Text),
    Column("failed_batches_json", Text),
)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except ValueError:
        return None


def _pick(record: dict, columns: list[str]) -> dict[str, Any]:
    row = {col: record.get(col) for col in columns}
    if "valor_del_contrato" in row:
        row["valor_del_contrato"] = _to_float(row.get("valor_del_contrato"))
    if "precio_base" in row:
        row["precio_base"] = _to_float(row.get("precio_base"))
    url = record.get("urlproceso")
    if isinstance(url, dict):
        record = {**record, "urlproceso": url.get("url")}
    row["payload_json"] = json.dumps(record, ensure_ascii=False, default=str)
    row["ingested_at"] = _now()
    return row


class IngestionRepository:
    def __init__(self, database_url: str = DATABASE_URL, engine: Optional[Engine] = None):
        self.engine = engine or create_engine(database_url, future=True)
        metadata.create_all(self.engine)

    def get_state(self, dataset_key: str) -> dict:
        with self.engine.begin() as conn:
            row = conn.execute(
                select(ingestion_state).where(ingestion_state.c.dataset_key == dataset_key)
            ).mappings().first()
        return dict(row) if row else {}

    def set_state(self, dataset_key: str, watermark: Optional[str], last_pk: Optional[str]):
        values = {
            "dataset_key": dataset_key,
            "watermark": watermark,
            "last_pk": last_pk,
            "updated_at": _now(),
        }
        with self.engine.begin() as conn:
            insert = self._insert(ingestion_state)
            stmt = insert.values(**values)
            stmt = stmt.on_conflict_do_update(
                index_elements=["dataset_key"],
                set_={
                    "watermark": values["watermark"],
                    "last_pk": values["last_pk"],
                    "updated_at": values["updated_at"],
                },
            )
            conn.execute(stmt)

    def start_run(self, dataset_key: str) -> int:
        with self.engine.begin() as conn:
            result = conn.execute(
                ingestion_runs.insert().values(
                    started_at=_now(),
                    dataset_key=dataset_key,
                    records_fetched=0,
                    records_written=0,
                    records_failed=0,
                    status="running",
                )
            )
            return int(result.inserted_primary_key[0])

    def finish_run(
        self,
        run_id: int,
        *,
        records_fetched: int,
        records_written: int,
        records_failed: int,
        status: str,
        error_summary: Optional[str],
        failed_batches: list[dict],
    ):
        with self.engine.begin() as conn:
            conn.execute(
                ingestion_runs.update()
                .where(ingestion_runs.c.id == run_id)
                .values(
                    finished_at=_now(),
                    records_fetched=records_fetched,
                    records_written=records_written,
                    records_failed=records_failed,
                    status=status,
                    error_summary=error_summary,
                    failed_batches_json=json.dumps(failed_batches, ensure_ascii=False),
                )
            )

    def recent_runs(self, limit: int = 20) -> list[dict]:
        with self.engine.begin() as conn:
            rows = conn.execute(
                select(ingestion_runs).order_by(ingestion_runs.c.id.desc()).limit(limit)
            ).mappings().all()
        return [dict(r) for r in rows]

    def counts(self) -> dict[str, int]:
        with self.engine.begin() as conn:
            contratos = conn.execute(select(func.count()).select_from(contratos_electronicos)).scalar_one()
            procesos = conn.execute(select(func.count()).select_from(procesos_contratacion)).scalar_one()
            scrapeados = conn.execute(select(func.count()).select_from(registros_scrapeados)).scalar_one()
        return {
            "contratos_electronicos": int(contratos),
            "procesos_contratacion": int(procesos),
            "registros_scrapeados": int(scrapeados),
        }

    def upsert_contratos(self, records: list[dict]) -> int:
        rows = [
            _pick(rec, CONTRATO_COLUMNS)
            for rec in records
            if rec.get("id_contrato")
        ]
        return self._upsert(contratos_electronicos, rows, ["id_contrato"])

    def upsert_procesos(self, records: list[dict]) -> int:
        rows = [
            _pick(rec, PROCESO_COLUMNS)
            for rec in records
            if rec.get("id_del_proceso")
        ]
        return self._upsert(procesos_contratacion, rows, ["id_del_proceso"])

    def upsert_scraped(self, records: list[dict]) -> int:
        now = _now()
        rows = []
        for rec in records:
            if not rec.get("source") or not rec.get("external_id"):
                continue
            rows.append(
                {
                    "source": rec["source"],
                    "external_id": rec["external_id"],
                    "title": rec.get("title"),
                    "entity": rec.get("entity"),
                    "value": _to_float(rec.get("value")),
                    "status": rec.get("status"),
                    "published_at": rec.get("published_at"),
                    "url": rec.get("url"),
                    "payload_json": json.dumps(rec.get("payload") or rec, ensure_ascii=False, default=str),
                    "ingested_at": now,
                }
            )
        return self._upsert(registros_scrapeados, rows, ["source", "external_id"])

    def _insert(self, table: Table):
        dialect = self.engine.dialect.name
        if dialect == "postgresql":
            return pg_insert(table)
        return sqlite_insert(table)

    def _upsert(self, table: Table, rows: list[dict], pk: list[str]) -> int:
        if not rows:
            return 0
        insert = self._insert(table)
        update_cols = {
            col.name: insert.excluded[col.name]
            for col in table.columns
            if col.name not in pk
        }
        with self.engine.begin() as conn:
            stmt = insert.values(rows)
            stmt = stmt.on_conflict_do_update(index_elements=pk, set_=update_cols)
            conn.execute(stmt)
        logger.info("Upsert %s filas en %s", len(rows), table.name)
        return len(rows)
