"""
Módulo de configuración central para el pipeline de SECOP II.
Define endpoints, rutas del proyecto, categorías de variables, esquemas de tipado y parámetros de rendimiento.
"""
from pathlib import Path

# ==========================================
# 1. RUTAS DEL PROYECTO
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
PARQUET_DIR = DATA_DIR / "parquet"
CHECKPOINTS_DIR = DATA_DIR / "checkpoints"

RESULTS_DIR = BASE_DIR / "results"
RESUMENES_DIR = RESULTS_DIR / "resumenes"
METADATA_DIR = RESULTS_DIR / "metadata"
VISUALIZACIONES_DIR = RESULTS_DIR / "visualizaciones"
ML_DIR = RESULTS_DIR / "ml"

LOGS_DIR = BASE_DIR / "logs"
LOG_FILE = LOGS_DIR / "procesamiento.log"

OUTPUT_PARQUET_FILE = PARQUET_DIR / "SECOP_II_2015_2025.parquet"
OUTPUT_ML_PARQUET_FILE = ML_DIR / "secop_ii_ml_features.parquet"

# Garantizar existencia de directorios
for d in [PARQUET_DIR, CHECKPOINTS_DIR, RESUMENES_DIR, METADATA_DIR, VISUALIZACIONES_DIR, ML_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ==========================================
# 2. CONFIGURACIÓN DE LA API DE DATOS ABIERTOS
# ==========================================
API_BASE_URL = "https://www.datos.gov.co/api/v3/views/jbjy-vk9h/query.json"
DATASET_METADATA_URL = "https://www.datos.gov.co/api/views/jbjy-vk9h.json"
DATASET_ID = "jbjy-vk9h"

# Periodo principal
FECHA_MIN = "2015-01-01T00:00:00.000"
FECHA_MAX = "2025-12-31T23:59:59.999"

# Parámetros de streaming y descarga
CHUNK_SIZE = 20000          # Registros por bloque para bajo consumo de RAM
REQUEST_TIMEOUT = 45        # Segundos de espera por petición HTTP
MAX_RETRIES = 5             # Reintentos con backoff exponencial
BACKOFF_FACTOR = 2          # Factor multiplicativo para pausas entre reintentos

# ==========================================
# 3. CLASIFICACIÓN DE COLUMNAS REALES (85 Cols)
# ==========================================
CATEGORIAS_COLUMNAS = {
    "Identificacion": [
        "proceso_de_compra", "id_contrato", "referencia_del_contrato", 
        ":id", ":version", "urlproceso"
    ],
    "Entidad": [
        "nombre_entidad", "nit_entidad", "codigo_entidad", "orden", 
        "sector", "rama", "entidad_centralizada"
    ],
    "Geografia": [
        "departamento", "ciudad", "localizaci_n", "direcci_n_de_ejecuci_n_del_contrato"
    ],
    "Tiempo": [
        "fecha_de_firma", "fecha_de_inicio_del_contrato", "fecha_de_fin_del_contrato", 
        "duraci_n_del_contrato", "dias_adicionados", ":created_at", ":updated_at"
    ],
    "Contratacion": [
        "estado_contrato", "tipo_de_contrato", "modalidad_de_contratacion", 
        "justificacion_modalidad_de", "condiciones_de_entrega", "liquidaci_n", 
        "reversion", "el_contrato_puede_ser_prorrogado", "documentos_tipo", 
        "descripcion_documentos_tipo"
    ],
    "Contratista": [
        "tipodocproveedor", "documento_proveedor", "proveedor_adjudicado", 
        "codigo_proveedor", "es_grupo", "es_pyme"
    ],
    "Valores_Economicos": [
        "valor_del_contrato", "valor_de_pago_adelantado", "valor_facturado", 
        "valor_pendiente_de_pago", "valor_pagado", "valor_amortizado", 
        "valor_pendiente_de", "valor_pendiente_de_ejecucion", "saldo_cdp", 
        "saldo_vigencia"
    ],
    "Ejecucion": [
        "habilita_pago_adelantado", "obligaci_n_ambiental", "obligaciones_postconsumo", 
        "espostconflicto", "puntos_del_acuerdo", "pilares_del_acuerdo"
    ],
    "Presupuesto": [
        "origen_de_los_recursos", "destino_gasto", "presupuesto_general_de_la_nacion_pgn", 
        "sistema_general_de_participaciones", "sistema_general_de_regal_as", 
        "recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_", 
        "recursos_de_credito", "recursos_propios"
    ],
    "Clasificacion": [
        "codigo_de_categoria_principal"
    ],
    "Texto": [
        "descripcion_del_proceso", "objeto_del_contrato"
    ],
    "Variables_Administrativas": [
        "nombre_representante_legal", "nacionalidad_representante_legal", 
        "domicilio_representante_legal", "tipo_de_identificaci_n_representante_legal", 
        "identificaci_n_representante_legal", "g_nero_representante_legal", 
        "nombre_del_banco", "tipo_de_cuenta", "n_mero_de_cuenta", 
        "nombre_ordenador_del_gasto", "tipo_de_documento_ordenador_del_gasto", 
        "n_mero_de_documento_ordenador_del_gasto", "nombre_supervisor", 
        "tipo_de_documento_supervisor", "n_mero_de_documento_supervisor", 
        "nombre_ordenador_de_pago", "tipo_de_documento_ordenador_de_pago", 
        "n_mero_de_documento_ordenador_de_pago"
    ]
}

# Columnas numéricas (monetarias y conteos)
COLUMNAS_NUMERICAS = [
    "valor_del_contrato", "valor_de_pago_adelantado", "valor_facturado",
    "valor_pendiente_de_pago", "valor_pagado", "valor_amortizado",
    "valor_pendiente_de", "valor_pendiente_de_ejecucion", "saldo_cdp",
    "saldo_vigencia", "presupuesto_general_de_la_nacion_pgn",
    "sistema_general_de_participaciones", "sistema_general_de_regal_as",
    "recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_",
    "recursos_de_credito", "recursos_propios", "dias_adicionados"
]

# Columnas temporales
COLUMNAS_FECHAS = [
    "fecha_de_firma", "fecha_de_inicio_del_contrato", "fecha_de_fin_del_contrato",
    ":created_at", ":updated_at"
]

# Columnas sensibles que deben excluirse o protegerse en el dataset analítico
COLUMNAS_SENSIBLES = [
    "nombre_representante_legal", "identificaci_n_representante_legal",
    "domicilio_representante_legal", "nombre_del_banco", "tipo_de_cuenta",
    "n_mero_de_cuenta", "nombre_ordenador_del_gasto", "n_mero_de_documento_ordenador_del_gasto",
    "nombre_supervisor", "n_mero_de_documento_supervisor",
    "nombre_ordenador_de_pago", "n_mero_de_documento_ordenador_de_pago"
]
