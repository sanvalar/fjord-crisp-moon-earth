"""
Orquestador principal del pipeline: Descarga y unifica SECOP I y SECOP II
100% mediante las APIs oficiales de Datos Abiertos hacia Parquet ZSTD.
"""
import sys
import time
import argparse
import logging
from pathlib import Path

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from secop_i.api import SecopIAPIClient
from secop_i.downloader import SecopIDownloader
from secop_i.processor import SecopIProcessor
from secop_i.config import OUTPUT_SECOP_I_PARQUET

from secop_ii.api import SecopAPIClient
from secop_ii.downloader import SecopDownloader
from secop_ii.processor import SecopProcessor
from secop_ii.config import OUTPUT_PARQUET_FILE as OUTPUT_SECOP_II_PARQUET

from secop_unified import SecopUnifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "logs" / "pipeline_apis.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("secop_master_api")

OUTPUT_UNIFIED_PARQUET = BASE_DIR / "data" / "parquet" / "SECOP_UNIFICADO_2015_2025.parquet"

def run_both_apis(max_records_per_dataset: int = None, chunk_size: int = 20000):
    """Ejecuta la descarga de ambas APIs por bloques y las une en DuckDB."""
    start_total = time.time()
    logger.info("=" * 65)
    logger.info(">>> INICIANDO DESCARGA Y PROCESAMIENTO 100% VÍA APIs OFICIALES <<<")
    logger.info("=" * 65)

    # 1. SECOP I
    logger.info("\n--- [1/3] DESCARGANDO SECOP I (API f789-7hwg) ---")
    client_i = SecopIAPIClient()
    downloader_i = SecopIDownloader(client=client_i)
    processor_i = SecopIProcessor(output_parquet=OUTPUT_SECOP_I_PARQUET)

    t0 = time.time()
    for batch in downloader_i.stream_batches(
        chunk_size=chunk_size, 
        max_records=max_records_per_dataset, 
        anno_min=2015, 
        anno_max=2025, 
        resume=True
    ):
        processor_i.process_batch(batch)
    processor_i.close()
    logger.info(f"✔ SECOP I finalizado en {time.time() - t0:.1f}s. Registros: {processor_i.total_processed:,}")

    # 2. SECOP II
    logger.info("\n--- [2/3] DESCARGANDO SECOP II (API jbjy-vk9h) ---")
    client_ii = SecopAPIClient()
    downloader_ii = SecopDownloader(client=client_ii)
    processor_ii = SecopProcessor(output_parquet=OUTPUT_SECOP_II_PARQUET)

    t1 = time.time()
    for batch in downloader_ii.stream_batches(
        chunk_size=chunk_size, 
        max_records=max_records_per_dataset, 
        fecha_inicio="2015-01-01T00:00:00.000", 
        fecha_fin="2025-12-31T23:59:59.999", 
        resume=True
    ):
        processor_ii.process_batch(batch)
    processor_ii.close()
    logger.info(f"✔ SECOP II finalizado en {time.time() - t1:.1f}s. Registros: {processor_ii.total_processed:,}")

    # 3. Unificación en DuckDB
    logger.info("\n--- [3/3] UNIFICANDO SECOP I + SECOP II EN DUCKDB ---")
    unifier = SecopUnifier(
        parquet_secop_i=OUTPUT_SECOP_I_PARQUET,
        parquet_secop_ii=OUTPUT_SECOP_II_PARQUET,
        output_unified=OUTPUT_UNIFIED_PARQUET
    )
    total_unificados = unifier.build_unified_dataset()

    elapsed_total = time.time() - start_total
    size_mb = OUTPUT_UNIFIED_PARQUET.stat().st_size / (1024 * 1024) if OUTPUT_UNIFIED_PARQUET.exists() else 0

    logger.info("\n" + "=" * 65)
    logger.info("BALANCE FINAL DE DESCARGA Y UNIFICACIÓN")
    logger.info(f"• Archivo unificado final: {OUTPUT_UNIFIED_PARQUET}")
    logger.info(f"• Total contratos consolidados: {total_unificados:,}")
    logger.info(f"• Tamaño Parquet ZSTD: {size_mb:.2f} MB")
    logger.info(f"• Tiempo total invertido: {elapsed_total:.2f}s ({elapsed_total/60:.2f} min)")
    logger.info("=" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Descarga y unificación 100% API de SECOP I y SECOP II")
    parser.add_argument("--max_records", type=int, default=None, 
                        help="Límite de registros por API para pruebas (ej. 5000). Si se omite, descarga todo.")
    parser.add_argument("--chunk_size", type=int, default=20000, 
                        help="Tamaño de bloque para la paginación de la API")
    args = parser.parse_args()

    run_both_apis(max_records_per_dataset=args.max_records, chunk_size=args.chunk_size)
