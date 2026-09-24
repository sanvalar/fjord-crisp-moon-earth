"""
Extractor Masivo Resiliente de la Industria TIC — SECOP I y SECOP II (2015-2025)
Descarga la totalidad de las contrataciones tecnológicas del Estado colombiano
directamente desde las APIs oficiales de Datos Abiertos (Socrata).
"""
import sys
import time
import json
import logging
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "parquet"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_TIC_PARQUET = DATA_DIR / "SECOP_INDUSTRIA_TIC_2015_2025.parquet"
TEMP_RAW_I = DATA_DIR / "temp_secop_i_tic.parquet"
TEMP_RAW_II = DATA_DIR / "temp_secop_ii_tic.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("extractor_tic_masivo")

def get_http_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=15, pool_maxsize=15)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "SecopTIC-FullExtractor/2.0",
        "Accept": "application/json"
    })
    return session

# Términos tecnológicos para búsqueda indexada
TIC_SEARCH_TERMS = [
    "software", "tecnologia", "computo", "conectividad",
    "telecomunicaciones", "ciberseguridad", "licenciamiento",
    "redes", "servidores", "datacenter", "informatica"
]

def fetch_secop_i_tic(session):
    logger.info(">>> INICIANDO EXTRACCIÓN SECOP I (API f789-7hwg) <<<")
    endpoint = "https://www.datos.gov.co/resource/f789-7hwg.json"
    all_records = []
    seen_ids = set()

    for term in TIC_SEARCH_TERMS:
        offset = 0
        limit = 10000
        logger.info(f"[SECOP I] Descargando término '{term}'...")
        while True:
            params = {
                "$select": "uid,numero_de_proceso,nit_de_la_entidad,nombre_entidad,orden_entidad,departamento_entidad,municipio_entidad,modalidad_de_contratacion,tipo_de_contrato,estado_del_proceso,identificacion_del_contratista,nom_razon_social_contratista,es_mipyme,fecha_de_firma_del_contrato,fecha_ini_ejec_contrato,fecha_fin_ejec_contrato,anno_cargue_secop,cuantia_contrato,tiempo_adiciones_en_dias,objeto_a_contratar,detalle_del_objeto_a_contratar",
                "$where": f"anno_cargue_secop >= 2015 AND anno_cargue_secop <= 2025",
                "$q": term,
                "$limit": limit,
                "$offset": offset,
                "$order": ":id"
            }
            try:
                resp = session.get(endpoint, params=params, timeout=40)
                if resp.status_code != 200:
                    logger.warning(f"Error {resp.status_code} en SECOP I con término '{term}' y offset {offset}: {resp.text[:150]}")
                    break
                rows = resp.json()
                if not rows or not isinstance(rows, list):
                    break
                
                new_count = 0
                for r in rows:
                    uid = r.get("uid") or r.get("numero_de_proceso")
                    if uid and uid not in seen_ids:
                        seen_ids.add(uid)
                        all_records.append(r)
                        new_count += 1

                logger.info(f"  [SECOP I - '{term}'] Bloque offset {offset}: {len(rows)} recibidos (+{new_count} nuevos únicos). Total acumulado: {len(all_records):,}")
                
                if len(rows) < limit:
                    break
                offset += limit
            except Exception as e:
                logger.error(f"Excepción en SECOP I ({term}, {offset}): {e}")
                break

    logger.info(f"✔ SECOP I finalizado. Total registros TIC únicos: {len(all_records):,}")
    return all_records

def fetch_secop_ii_tic(session):
    logger.info(">>> INICIANDO EXTRACCIÓN SECOP II (API jbjy-vk9h) <<<")
    endpoint = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
    all_records = []
    seen_ids = set()

    for term in TIC_SEARCH_TERMS:
        offset = 0
        limit = 10000
        logger.info(f"[SECOP II] Descargando término '{term}'...")
        while True:
            params = {
                "$select": "id_contrato,proceso_de_compra,nit_entidad,nombre_entidad,orden,departamento,ciudad,modalidad_de_contratacion,tipo_de_contrato,estado_contrato,documento_proveedor,proveedor_adjudicado,es_pyme,fecha_de_firma,fecha_de_inicio_del_contrato,fecha_de_fin_del_contrato,valor_del_contrato,valor_facturado,valor_pagado,dias_adicionados,descripcion_del_proceso,objeto_del_contrato",
                "$where": "fecha_de_firma >= '2015-01-01T00:00:00.000' AND fecha_de_firma <= '2025-12-31T23:59:59.999'",
                "$q": term,
                "$limit": limit,
                "$offset": offset,
                "$order": ":id"
            }
            try:
                resp = session.get(endpoint, params=params, timeout=40)
                if resp.status_code != 200:
                    logger.warning(f"Error {resp.status_code} en SECOP II con término '{term}' y offset {offset}: {resp.text[:150]}")
                    break
                rows = resp.json()
                if not rows or not isinstance(rows, list):
                    break
                
                new_count = 0
                for r in rows:
                    cid = r.get("id_contrato") or r.get("proceso_de_compra")
                    if cid and cid not in seen_ids:
                        seen_ids.add(cid)
                        all_records.append(r)
                        new_count += 1

                logger.info(f"  [SECOP II - '{term}'] Bloque offset {offset}: {len(rows)} recibidos (+{new_count} nuevos únicos). Total acumulado: {len(all_records):,}")
                
                if len(rows) < limit:
                    break
                offset += limit
            except Exception as e:
                logger.error(f"Excepción en SECOP II ({term}, {offset}): {e}")
                break

    logger.info(f"✔ SECOP II finalizado. Total registros TIC únicos: {len(all_records):,}")
    return all_records

def process_and_unify(records_i, records_ii):
    logger.info(">>> TRANSFORMANDO Y HOMOLOGANDO A PARQUET ZSTD <<<")
    con = duckdb.connect()

    # Normalizar SECOP I
    rows_i_norm = []
    for r in records_i:
        try:
            val = float(r.get("cuantia_contrato", 0) or 0)
            if val <= 0: continue
            anio = int(float(r.get("anno_cargue_secop", 2020) or 2020))
            rows_i_norm.append({
                "sistema_origen": "SECOP_I",
                "id_contrato_global": str(r.get("uid") or r.get("numero_de_proceso") or ""),
                "proceso_compra": str(r.get("numero_de_proceso") or ""),
                "nit_entidad": str(r.get("nit_de_la_entidad") or ""),
                "nombre_entidad": str(r.get("nombre_entidad") or "NO_DEFINIDO"),
                "orden_gobierno": str(r.get("orden_entidad") or "Territorial"),
                "departamento": str(r.get("departamento_entidad") or "NO_DEFINIDO").upper().strip(),
                "municipio": str(r.get("municipio_entidad") or "NO_DEFINIDO").upper().strip(),
                "modalidad": str(r.get("modalidad_de_contratacion") or "Contratación Directa"),
                "tipo_contrato": str(r.get("tipo_de_contrato") or "Prestación de Servicios"),
                "estado_contrato": str(r.get("estado_del_proceso") or "Celebrado"),
                "documento_proveedor": str(r.get("identificacion_del_contratista") or ""),
                "proveedor": str(r.get("nom_razon_social_contratista") or "NO_DEFINIDO"),
                "es_pyme_bin": 1 if str(r.get("es_mipyme", "")).lower() == "si" else 0,
                "fecha_firma": str(r.get("fecha_de_firma_del_contrato") or ""),
                "fecha_inicio": str(r.get("fecha_ini_ejec_contrato") or ""),
                "fecha_fin": str(r.get("fecha_fin_ejec_contrato") or ""),
                "anio_contratacion": anio,
                "valor_contrato": val,
                "valor_facturado": val,
                "valor_pagado": val,
                "dias_adicionados": int(float(r.get("tiempo_adiciones_en_dias", 0) or 0)),
                "objeto_resumido": str(r.get("objeto_a_contratar") or ""),
                "objeto_detallado": str(r.get("detalle_del_objeto_a_contratar") or "")
            })
        except Exception:
            continue

    # Normalizar SECOP II
    rows_ii_norm = []
    for r in records_ii:
        try:
            val = float(r.get("valor_del_contrato", 0) or 0)
            if val <= 0: continue
            firma = str(r.get("fecha_de_firma") or "")
            anio = int(firma[:4]) if len(firma) >= 4 and firma[:4].isdigit() else 2022
            rows_ii_norm.append({
                "sistema_origen": "SECOP_II",
                "id_contrato_global": str(r.get("id_contrato") or r.get("proceso_de_compra") or ""),
                "proceso_compra": str(r.get("proceso_de_compra") or ""),
                "nit_entidad": str(r.get("nit_entidad") or ""),
                "nombre_entidad": str(r.get("nombre_entidad") or "NO_DEFINIDO"),
                "orden_gobierno": str(r.get("orden") or "Territorial"),
                "departamento": str(r.get("departamento") or "NO_DEFINIDO").upper().strip(),
                "municipio": str(r.get("ciudad") or "NO_DEFINIDO").upper().strip(),
                "modalidad": str(r.get("modalidad_de_contratacion") or "Contratación Directa"),
                "tipo_contrato": str(r.get("tipo_de_contrato") or "Prestación de Servicios"),
                "estado_contrato": str(r.get("estado_contrato") or "Celebrado"),
                "documento_proveedor": str(r.get("documento_proveedor") or ""),
                "proveedor": str(r.get("proveedor_adjudicado") or "NO_DEFINIDO"),
                "es_pyme_bin": 1 if str(r.get("es_pyme", "")).lower() == "si" else 0,
                "fecha_firma": firma,
                "fecha_inicio": str(r.get("fecha_de_inicio_del_contrato") or ""),
                "fecha_fin": str(r.get("fecha_de_fin_del_contrato") or ""),
                "anio_contratacion": anio,
                "valor_contrato": val,
                "valor_facturado": float(r.get("valor_facturado", 0) or val),
                "valor_pagado": float(r.get("valor_pagado", 0) or val),
                "dias_adicionados": int(float(r.get("dias_adicionados", 0) or 0)),
                "objeto_resumido": str(r.get("descripcion_del_proceso") or ""),
                "objeto_detallado": str(r.get("objeto_del_contrato") or "")
            })
        except Exception:
            continue

    import pandas as pd
    df_i = pd.DataFrame(rows_i_norm)
    df_ii = pd.DataFrame(rows_ii_norm)
    df_all = pd.concat([df_i, df_ii], ignore_index=True)

    # Quitar duplicados absolutos por ID
    df_all = df_all.drop_duplicates(subset=["id_contrato_global"])

    logger.info(f"Consolidado pre-filtro: {len(df_all):,} registros. Aplicando clasificación de subsectores...")

    con.register("df_raw_tic", df_all)
    clean_out = str(OUTPUT_TIC_PARQUET).replace("\\", "/")

    sql_final = f"""
    COPY (
        SELECT 
            *,
            -- Subclasificación inteligente dentro de la industria TIC
            CASE 
                WHEN LOWER(objeto_resumido) LIKE '%software%' OR LOWER(objeto_resumido) LIKE '%licenci%' OR LOWER(objeto_resumido) LIKE '%desarrollo de aplicac%' THEN 'Desarrollo de Software y Licenciamiento'
                WHEN LOWER(objeto_resumido) LIKE '%conectividad%' OR LOWER(objeto_resumido) LIKE '%internet%' OR LOWER(objeto_resumido) LIKE '%telecomunicac%' OR LOWER(objeto_resumido) LIKE '%redes%' THEN 'Conectividad y Redes'
                WHEN LOWER(objeto_resumido) LIKE '%ciberseguridad%' OR LOWER(objeto_resumido) LIKE '%seguridad inform%' OR LOWER(objeto_resumido) LIKE '%cloud%' OR LOWER(objeto_resumido) LIKE '%nube%' THEN 'Ciberseguridad e Infraestructura Cloud'
                WHEN LOWER(objeto_resumido) LIKE '%computad%' OR LOWER(objeto_resumido) LIKE '%servidor%' OR LOWER(objeto_resumido) LIKE '%hardware%' OR LOWER(objeto_resumido) LIKE '%equipos de c%' THEN 'Hardware y Equipos de Cómputo'
                ELSE 'Servicios de Soporte y Consultoría TI'
            END as subsector_tic,
            -- Detección de alineación directa con CONPES 4069
            CASE 
                WHEN LOWER(objeto_resumido) LIKE '%innovaci%' OR LOWER(objeto_resumido) LIKE '%investigaci%' OR LOWER(objeto_resumido) LIKE '%ciencia%' OR LOWER(objeto_resumido) LIKE '%inteligencia artificial%' OR LOWER(objeto_resumido) LIKE '%analitica%' THEN 1
                ELSE 0
            END as flag_innovacion_conpes
        FROM df_raw_tic
        WHERE anio_contratacion >= 2015 AND anio_contratacion <= 2025
    ) TO '{clean_out}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """

    con.execute(sql_final)
    total_final = con.execute(f"SELECT count(*) FROM read_parquet('{clean_out}')").fetchone()[0]
    total_monto = con.execute(f"SELECT round(sum(valor_contrato)/1e9, 2) FROM read_parquet('{clean_out}')").fetchone()[0]

    logger.info(f"=================================================================")
    logger.info(f"✔ PROCESO COMPLETADO EXITOSAMENTE")
    logger.info(f"✔ Archivo Parquet: {OUTPUT_TIC_PARQUET}")
    logger.info(f"✔ Total Contratos TIC Extraídos: {total_final:,}")
    logger.info(f"✔ Monto Total TIC: ${total_monto:,} Miles de Millones COP")
    logger.info(f"=================================================================")
    return total_final

def main():
    start_t = time.time()
    session = get_http_session()
    
    rec_i = fetch_secop_i_tic(session)
    rec_ii = fetch_secop_ii_tic(session)
    
    process_and_unify(rec_i, rec_ii)
    logger.info(f"Tiempo total de extracción y procesamiento: {time.time() - start_t:.1f} segundos")

if __name__ == "__main__":
    main()
