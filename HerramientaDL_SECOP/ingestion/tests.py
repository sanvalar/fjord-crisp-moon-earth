"""Pruebas de SoQL incremental, upsert, 403 y aislamiento de lotes fallidos."""
from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

from sqlalchemy import create_engine, func, select

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.http_client import PermissionDeniedError, ResilientHttpClient, classify_403
from ingestion.pipeline import IngestionPipeline
from ingestion.repository import IngestionRepository, contratos_electronicos
from ingestion.scrapers import PaginationParams, PlaceholderHtmlScraper
from ingestion.soda import SodaConnector, SodaPage, build_where


class FakeResponse:
    def __init__(self, status_code, text="", headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


class WhereClauseTests(unittest.TestCase):
    def test_excluye_fechas_nulas_y_usa_keyset(self):
        clause = build_where(
            require_not_null=("fecha_de_firma",),
            watermark_field="fecha_de_firma",
            watermark="2024-01-01T00:00:00.000",
            pk_field="id_contrato",
            last_pk="CO1.PCCNTR.1",
        )
        self.assertIn("fecha_de_firma IS NOT NULL", clause)
        self.assertIn("fecha_de_firma > '2024-01-01T00:00:00.000'", clause)
        self.assertIn("id_contrato > 'CO1.PCCNTR.1'", clause)


class Http403Tests(unittest.TestCase):
    def test_403_con_retry_after_es_rate_limit(self):
        resp = FakeResponse(403, text="", headers={"Retry-After": "2"})
        self.assertEqual(classify_403(resp), "rate_limit")

    def test_403_de_permiso_no_se_reintenta(self):
        sleeps = []
        session = MagicMock()
        denied = MagicMock()
        denied.status_code = 403
        denied.text = "Access denied / permission denied for this user"
        denied.headers = {}
        session.request.return_value = denied
        client = ResilientHttpClient(
            timeout=5, max_retries=4, backoff_factor=1, session=session, sleeper=sleeps.append
        )
        with self.assertRaises(PermissionDeniedError):
            client.get_json("https://example.test/resource.json")
        self.assertEqual(session.request.call_count, 1)
        self.assertEqual(sleeps, [])

    def test_403_de_cupo_espera_y_reintenta(self):
        sleeps = []
        session = MagicMock()
        limited = MagicMock()
        limited.status_code = 403
        limited.text = "Rate limit exceeded"
        limited.headers = {"Retry-After": "1"}
        limited.json.return_value = []
        limited.url = "https://example.test/resource.json"
        ok = MagicMock()
        ok.status_code = 200
        ok.text = "[]"
        ok.headers = {}
        ok.json.return_value = [{"id_contrato": "1"}]
        ok.url = "https://example.test/resource.json"
        session.request.side_effect = [limited, ok]
        client = ResilientHttpClient(
            timeout=5, max_retries=4, backoff_factor=1, session=session, sleeper=sleeps.append
        )
        result = client.get_json("https://example.test/resource.json")
        self.assertEqual(result.payload[0]["id_contrato"], "1")
        self.assertEqual(sleeps, [1.0])


class UpsertTests(unittest.TestCase):
    def setUp(self):
        engine = create_engine("sqlite:///:memory:", future=True)
        self.repo = IngestionRepository(engine=engine)

    def test_repetir_id_no_duplica(self):
        first = [{
            "id_contrato": "CO1.PCCNTR.1",
            "nombre_entidad": "Entidad A",
            "valor_del_contrato": "1000",
            "fecha_de_firma": "2024-06-01T00:00:00.000",
            "objeto_del_contrato": "Objeto inicial",
        }]
        second = [{
            "id_contrato": "CO1.PCCNTR.1",
            "nombre_entidad": "Entidad A",
            "valor_del_contrato": "2500",
            "fecha_de_firma": "2024-06-01T00:00:00.000",
            "objeto_del_contrato": "Objeto actualizado",
        }]
        self.repo.upsert_contratos(first)
        self.repo.upsert_contratos(second)
        with self.repo.engine.begin() as conn:
            total = conn.execute(select(func.count()).select_from(contratos_electronicos)).scalar_one()
            row = conn.execute(select(contratos_electronicos)).mappings().one()
        self.assertEqual(total, 1)
        self.assertEqual(row["valor_del_contrato"], 2500.0)
        self.assertEqual(row["objeto_del_contrato"], "Objeto actualizado")


class PipelineIsolationTests(unittest.TestCase):
    def test_fallo_de_un_lote_no_detiene_el_otro_dataset(self):
        engine = create_engine("sqlite:///:memory:", future=True)
        repo = IngestionRepository(engine=engine)
        soda = MagicMock()

        def iter_incremental(dataset_key, **_kwargs):
            if dataset_key == "contratos":
                yield SodaPage(
                    dataset_key="contratos",
                    dataset_id="jbjy-vk9h",
                    offset=0,
                    limit=10,
                    where_clause="fecha_de_firma IS NOT NULL",
                    records=[],
                    error="RateLimitError: 403",
                )
            else:
                yield SodaPage(
                    dataset_key="procesos",
                    dataset_id="p6dx-8zbt",
                    offset=0,
                    limit=10,
                    where_clause="fecha_de_publicacion_del IS NOT NULL",
                    records=[{
                        "id_del_proceso": "CO1.REQ.1",
                        "nombre_del_procedimiento": "Proceso demo",
                        "entidad": "Entidad",
                        "precio_base": "10",
                        "fecha_de_publicacion_del": "2024-01-02T00:00:00.000",
                    }],
                )

        soda.iter_incremental.side_effect = iter_incremental
        pipeline = IngestionPipeline(repository=repo, soda=soda, scrapers={})
        result = pipeline.run(datasets=["contratos", "procesos"])
        by_key = {item.dataset_key: item for item in result.datasets}
        self.assertEqual(by_key["contratos"].status, "partial")
        self.assertEqual(by_key["contratos"].failed_batches[0]["offset"], 0)
        self.assertEqual(by_key["procesos"].status, "ok")
        self.assertEqual(by_key["procesos"].records_written, 1)


class ScraperInterfaceTests(unittest.TestCase):
    def test_entrada_url_paginacion_y_salida_normalizada(self):
        http = MagicMock()
        http.get_text.return_value = MagicMock(payload="<html></html>")
        scraper = PlaceholderHtmlScraper(http)
        page = scraper.fetch_page(
            "https://example.test/fuente",
            PaginationParams(page=2, page_size=25, extra={"q": "secop"}),
        )
        self.assertEqual(scraper.name, "placeholder_html")
        self.assertIn("page=2", page.url)
        self.assertIn("page_size=25", page.url)
        self.assertIsInstance(page.records, list)
        self.assertFalse(page.has_more)


class LambdaUpsertTests(unittest.TestCase):
    def test_lambda_inserta_si_no_existe_y_actualiza_si_existe(self):
        from ingestion.lambda_upsert import aplicar_lambda_upsert, lambda_upsert

        existentes = {"CO1.PCCNTR.1": {"id_contrato_global": "CO1.PCCNTR.1", "valor_contrato": 1000, "objeto_detallado": "old"}}
        incoming = [
            {"id_contrato_global": "CO1.PCCNTR.1", "valor_contrato": 2500, "objeto_detallado": "software actualizado"},
            {"id_contrato_global": "CO1.PCCNTR.NUEVO", "valor_contrato": 8000, "objeto_detallado": "plataforma tic"},
        ]
        self.assertEqual(lambda_upsert("CO1.PCCNTR.1", existentes), "update")
        self.assertEqual(lambda_upsert("CO1.PCCNTR.NUEVO", existentes), "insert")
        stats = aplicar_lambda_upsert(incoming, existentes)
        self.assertEqual(stats["updated"], 1)
        self.assertEqual(stats["inserted"], 1)
        self.assertEqual(existentes["CO1.PCCNTR.1"]["valor_contrato"], 2500)
        self.assertEqual(existentes["CO1.PCCNTR.NUEVO"]["objeto_detallado"], "plataforma tic")

    def test_parquet_merge_sql_no_duplica(self):
        import tempfile
        import duckdb
        import pandas as pd
        from ingestion.lambda_upsert import UNIFIED_COLUMNS, duckdb_lambda_merge_sql

        def row(pk, valor, src="SECOP_II"):
            rec = {c: None for c in UNIFIED_COLUMNS}
            rec.update({
                "sistema_origen": src,
                "id_contrato_global": pk,
                "nombre_entidad": "ENTIDAD",
                "departamento": "ANTIOQUIA",
                "municipio": "MEDELLIN",
                "modalidad": "Directa",
                "tipo_contrato": "Prestación de servicios",
                "estado_contrato": "En ejecución",
                "proveedor": "PROV",
                "es_pyme_bin": 0,
                "fecha_firma": "2026-01-15T00:00:00.000",
                "anio_contratacion": 2026,
                "valor_contrato": float(valor),
                "valor_facturado": float(valor),
                "valor_pagado": float(valor),
                "dias_adicionados": 0,
                "objeto_resumido": "software",
                "objeto_detallado": "software",
                "subsector_tic": "Desarrollo de Software y Licenciamiento",
                "flag_innovacion_conpes": 0,
            })
            return rec

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "base.parquet"
            dest = Path(tmp) / "dest.parquet"
            pd.DataFrame([row("A", 1000), row("B", 2000)]).to_parquet(base, index=False)
            incoming = pd.DataFrame([row("A", 9999), row("C", 3000)])
            con = duckdb.connect()
            con.register("incoming_tic", incoming)
            con.execute(duckdb_lambda_merge_sql(base.as_posix(), dest.as_posix()))
            out = con.execute(f"SELECT id_contrato_global, valor_contrato FROM read_parquet('{dest.as_posix()}') ORDER BY 1").fetchall()
            self.assertEqual(len(out), 3)
            by_id = {r[0]: r[1] for r in out}
            self.assertEqual(by_id["A"], 9999)
            self.assertEqual(by_id["B"], 2000)
            self.assertEqual(by_id["C"], 3000)


if __name__ == "__main__":
    unittest.main()

