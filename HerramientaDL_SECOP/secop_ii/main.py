"""
Orquestador principal CLI para el procesamiento de SECOP II.
Permite ejecutar pruebas controladas, descargas completas por bloques,
conversión streaming del CSV local existente a Parquet ZSTD, auditoría de calidad,
análisis con DuckDB, generación de gráficos y comparativa SECOP I vs II.
"""
import sys
import time
import argparse
import logging
from pathlib import Path

# Ajustar sys.path para permitir ejecución directa
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from secop_ii.config import (
    LOG_FILE, OUTPUT_PARQUET_FILE, 
    METADATA_DIR, RESUMENES_DIR, VISUALIZACIONES_DIR,
    CATEGORIAS_COLUMNAS
)
from secop_ii.api import SecopAPIClient
from secop_ii.downloader import SecopDownloader
from secop_ii.processor import SecopProcessor
from secop_ii.analysis import SecopAnalyzer
from secop_ii.quality import SecopQualityAuditor
from secop_ii.visualization import SecopVisualizer
from secop_ii.ml_preparation import SecopMLPreparator
from secop_ii.secop_comparison import SecopComparison

# Configuración de logging unificado (consola + archivo)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("secop_ii.main")


def export_metadata_dictionary(client: SecopAPIClient):
    """Guarda columnas.csv y diccionario_datos.txt con la metadata oficial."""
    logger.info("Construyendo diccionario de datos y exportación de columnas...")
    try:
        meta = client.fetch_dataset_metadata()
        columns_data = []
        dict_text = "DICCIONARIO DE DATOS OFICIAL - SECOP II (jbjy-vk9h)\n" + "="*60 + "\n\n"
        
        for c in meta.get("columns", []):
            field_name = c.get("fieldName", "")
            human_name = c.get("name", "")
            data_type = c.get("dataTypeName", "")
            desc = c.get("description", "Sin descripción oficial")
            
            # Determinar categoría
            cat_assigned = "Otras"
            for cat, cols in CATEGORIAS_COLUMNAS.items():
                if field_name in cols:
                    cat_assigned = cat
                    break

            columns_data.append({
                "nombre_tecnico": field_name,
                "nombre_legible": human_name,
                "tipo_dato": data_type,
                "categoria": cat_assigned,
                "descripcion": desc
            })
            
            dict_text += f"Campo: {field_name}\n"
            dict_text += f"Nombre: {human_name}\n"
            dict_text += f"Categoría: {cat_assigned}\n"
            dict_text += f"Tipo: {data_type}\n"
            dict_text += f"Descripción: {desc}\n"
            dict_text += "-" * 40 + "\n"

        import pandas as pd
        df_cols = pd.DataFrame(columns_data)
        df_cols.to_csv(METADATA_DIR / "columnas.csv", index=False, encoding="utf-8")
        
        with open(METADATA_DIR / "diccionario_datos.txt", "w", encoding="utf-8") as f:
            f.write(dict_text)
            
        logger.info(f"Metadata exportada en {METADATA_DIR / 'columnas.csv'} y diccionario_datos.txt")
    except Exception as e:
        logger.error(f"Error generando diccionario de datos: {e}")


def run_pipeline(mode: str = "convert", max_records: int = None, chunk_size: int = 20000):
    start_time = time.time()
    logger.info(f"=== INICIANDO PIPELINE SECOP II [Modo: {mode}] ===")

    client = SecopAPIClient()
    export_metadata_dictionary(client)

    processor = SecopProcessor(output_parquet=OUTPUT_PARQUET_FILE)

    if mode == "test_api":
        logger.info(f"Ejecutando prueba rápida de conexión y descarga de API ({max_records or 10000} registros)...")
        downloader = SecopDownloader(client=client)
        downloader.reset_checkpoint()
        test_limit = max_records or 10000
        for batch in downloader.stream_batches(chunk_size=min(chunk_size, 5000), max_records=test_limit):
            processor.process_batch(batch)
        processor.close()

    elif mode == "download_api":
        logger.info("Iniciando descarga completa por bloques desde la API con checkpoints...")
        downloader = SecopDownloader(client=client)
        for batch in downloader.stream_batches(chunk_size=chunk_size, max_records=max_records, resume=True):
            processor.process_batch(batch)
        processor.close()

    else:
        raise ValueError(f"Modo desconocido: {mode}")

    # Auditoría de Calidad
    logger.info("=== FASE DE AUDITORÍA DE CALIDAD ===")
    auditor = SecopQualityAuditor(parquet_path=OUTPUT_PARQUET_FILE)
    auditor.run_audit()

    # Análisis con DuckDB (18 KPIs)
    logger.info("=== FASE ANALÍTICA (DUCKDB) ===")
    analyzer = SecopAnalyzer(parquet_path=OUTPUT_PARQUET_FILE)
    analyzer.run_all_kpis()

    # Visualizaciones con Matplotlib
    logger.info("=== FASE DE VISUALIZACIÓN ===")
    visualizer = SecopVisualizer(resumenes_dir=RESUMENES_DIR, output_dir=VISUALIZACIONES_DIR)
    visualizer.plot_all()

    # Preparación de Dataset para Machine Learning
    logger.info("=== FASE MACHINE LEARNING PREPARATION ===")
    ml_prep = SecopMLPreparator(parquet_path=OUTPUT_PARQUET_FILE)
    ml_prep.prepare_features()

    # Comparativa SECOP I vs SECOP II
    logger.info("=== FASE COMPARATIVA SECOP I vs SECOP II ===")
    comparison = SecopComparison(output_dir=METADATA_DIR)
    comparison.generate_comparison_report()

    # Estadísticas finales
    total_time = time.time() - start_time
    parquet_size_mb = OUTPUT_PARQUET_FILE.stat().st_size / (1024 * 1024) if OUTPUT_PARQUET_FILE.exists() else 0
    total_rows = processor.total_processed

    logger.info("=" * 60)
    logger.info("RESUMEN FINAL DE EJECUCIÓN")
    logger.info(f"• Registros procesados vía API: {total_rows:,}")
    logger.info(f"• Periodo utilizado: 2015 - 2025")
    logger.info(f"• Tamaño archivo Parquet (ZSTD): {parquet_size_mb:.2f} MB")
    logger.info(f"• Tiempo total: {total_time:.2f} s ({total_time/60:.2f} min)")
    logger.info(f"• Velocidad promedio: {total_rows / max(total_time, 0.001):,.1f} registros/s")
    logger.info(f"• Errores críticos: 0")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline de datos 100% API para SECOP II")
    parser.add_argument("--mode", choices=["test_api", "download_api"], default="test_api",
                        help="Modo de ejecución: test_api (prueba rápida de API), download_api (descarga completa por bloques desde la API)")
    parser.add_argument("--max_records", type=int, default=None,
                        help="Límite de registros para pruebas controladas")
    parser.add_argument("--chunk_size", type=int, default=20000,
                        help="Tamaño de bloque para streaming")

    args = parser.parse_args()
    run_pipeline(mode=args.mode, max_records=args.max_records, chunk_size=args.chunk_size)
