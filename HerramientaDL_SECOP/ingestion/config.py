"""Configuración de ingesta continua SECOP (SODA + scrapers)."""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

try:
    from dotenv import load_dotenv

    load_dotenv(BASE_DIR / ".env")
except ImportError:
    pass

DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
CHECKPOINTS_DIR = DATA_DIR / "checkpoints"
INGEST_LOG_FILE = LOGS_DIR / "ingestion.log"

for _d in (DATA_DIR, LOGS_DIR, CHECKPOINTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

DB_PATH = Path(os.environ.get("SECOP_DB_PATH", str(DATA_DIR / "secop_live.db")))


def _default_database_url() -> str:
    explicit = os.environ.get("SECOP_DATABASE_URL")
    if explicit:
        return explicit
    return "sqlite:///" + DB_PATH.resolve().as_posix()


DATABASE_URL = _default_database_url()
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "").strip()
SODA_DOMAIN = os.environ.get("SODA_DOMAIN", "https://www.datos.gov.co").rstrip("/")
SODA_RESOURCE_URL = f"{SODA_DOMAIN}/resource/{{dataset_id}}.json"

PAGE_SIZE = int(os.environ.get("SECOP_PAGE_SIZE", "1000"))
REQUEST_TIMEOUT = int(os.environ.get("SECOP_REQUEST_TIMEOUT", "45"))
MAX_RETRIES = int(os.environ.get("SECOP_MAX_RETRIES", "6"))
BACKOFF_FACTOR = float(os.environ.get("SECOP_BACKOFF_FACTOR", "2"))
SCHEDULE_HOURS = float(os.environ.get("SECOP_INGEST_INTERVAL_HOURS", "12"))
INITIAL_LOOKBACK_DAYS = int(os.environ.get("SECOP_INITIAL_LOOKBACK_DAYS", "180"))

DATASETS = {
    "contratos": {
        "dataset_id": "jbjy-vk9h",
        "label": "Contratos electrónicos SECOP II",
        "pk": "id_contrato",
        "watermark_field": "fecha_de_firma",
        "order_fields": ("fecha_de_firma", "id_contrato"),
        "require_not_null": ("fecha_de_firma",),
        "table": "contratos_electronicos",
    },
    "procesos": {
        "dataset_id": "p6dx-8zbt",
        "label": "Procesos de contratación SECOP II",
        "pk": "id_del_proceso",
        "watermark_field": "fecha_de_publicacion_del",
        "order_fields": ("fecha_de_publicacion_del", "id_del_proceso"),
        "require_not_null": ("fecha_de_publicacion_del",),
        "table": "procesos_contratacion",
    },
    "secop_i": {
        "dataset_id": "f789-7hwg",
        "label": "SECOP I — contratos históricos",
        "pk": "uid",
        "watermark_field": "fecha_de_firma_del_contrato",
        "order_fields": ("fecha_de_firma_del_contrato", "uid"),
        "require_not_null": ("fecha_de_firma_del_contrato",),
        "table": "contratos_secop_i",
    },
}

CONTRATO_COLUMNS = [
    "id_contrato",
    "nombre_entidad",
    "nit_entidad",
    "departamento",
    "ciudad",
    "proveedor_adjudicado",
    "documento_proveedor",
    "valor_del_contrato",
    "objeto_del_contrato",
    "estado_contrato",
    "fecha_de_firma",
    "modalidad_de_contratacion",
    "tipo_de_contrato",
    "proceso_de_compra",
    "referencia_del_contrato",
]

PROCESO_COLUMNS = [
    "id_del_proceso",
    "nombre_del_procedimiento",
    "entidad",
    "nit_entidad",
    "departamento_entidad",
    "ciudad_entidad",
    "precio_base",
    "nombre_del_proveedor",
    "modalidad_de_contratacion",
    "estado_del_procedimiento",
    "estado_de_apertura_del_proceso",
    "fecha_de_publicacion_del",
    "fecha_de_ultima_publicaci",
    "tipo_de_contrato",
    "adjudicado",
    "referencia_del_proceso",
]
