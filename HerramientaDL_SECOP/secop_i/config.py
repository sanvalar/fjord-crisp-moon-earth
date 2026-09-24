"""
Configuración para el pipeline de SECOP I (API f789-7hwg).
Rutas, parámetros de streaming y tipos de datos esperados.
"""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Carpetas de datos y resultados
DATA_DIR = BASE_DIR / "data"
PARQUET_DIR = DATA_DIR / "parquet"
CHECKPOINTS_DIR = DATA_DIR / "checkpoints"
RESULTS_DIR = BASE_DIR / "results"
METADATA_DIR = RESULTS_DIR / "metadata"
LOGS_DIR = BASE_DIR / "logs"

OUTPUT_SECOP_I_PARQUET = PARQUET_DIR / "SECOP_I_2015_2025.parquet"

for d in [PARQUET_DIR, CHECKPOINTS_DIR, RESULTS_DIR, METADATA_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Parámetros API
API_SECOP_I_URL = "https://www.datos.gov.co/api/v3/views/f789-7hwg/query.json"
DATASET_I_METADATA_URL = "https://www.datos.gov.co/api/views/f789-7hwg.json"
DATASET_I_ID = "f789-7hwg"

ANNO_MIN = 2015
ANNO_MAX = 2025

CHUNK_SIZE = 20000
REQUEST_TIMEOUT = 45
MAX_RETRIES = 5
BACKOFF_FACTOR = 2

# Tipado estricto para PyArrow
COLUMNAS_NUMERICAS_SECOP_I = [
    "anno_cargue_secop", "cuantia_proceso", "cuantia_contrato",
    "valor_total_de_adiciones", "valor_contrato_con_adiciones",
    "valor_rubro", "tiempo_adiciones_en_dias", "tiempo_adiciones_en_meses"
]

COLUMNAS_FECHAS_SECOP_I = [
    "fecha_de_cargue_en_el_secop", "fecha_de_firma_del_contrato",
    "fecha_ini_ejec_contrato", "fecha_fin_ejec_contrato",
    "fecha_liquidacion", "ultima_actualizacion",
    ":created_at", ":updated_at"
]

COLUMNAS_SENSIBLES_SECOP_I = [
    "nombre_del_represen_legal", "identific_representante_legal",
    "tipo_doc_representante_legal", "sexo_replegal"
]
