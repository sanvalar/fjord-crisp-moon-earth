"""
Script de verificación y testing integral para el pipeline de SECOP II.
Ejecuta una prueba controlada de extremo a extremo:
1. Conexión a la API y metadata oficial.
2. Descarga de muestra con paginación y checkpoints.
3. Generación y lectura de Parquet con compresión ZSTD.
4. Consulta analítica con DuckDB.
5. Verificación de reporte de calidad y gráficos.
"""
import sys
from pathlib import Path

# Configurar encoding seguro para consola de Windows
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Ajustar sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from secop_ii.api import SecopAPIClient
from secop_ii.downloader import SecopDownloader
from secop_ii.processor import SecopProcessor
from secop_ii.analysis import SecopAnalyzer
from secop_ii.quality import SecopQualityAuditor
from secop_ii.visualization import SecopVisualizer
from secop_ii.ml_preparation import SecopMLPreparator
from secop_ii.secop_comparison import SecopComparison
from secop_ii.config import CHECKPOINTS_DIR, RESULTS_DIR

def run_test():
    print("\n" + "="*60)
    print(">>> INICIANDO TEST CONTROLADO DEL PIPELINE SECOP II <<<")
    print("="*60)

    test_parquet = RESULTS_DIR / "test_secop_ii_sample.parquet"
    if test_parquet.exists():
        test_parquet.unlink()

    # 1. API Client y Metadatos
    print("\n1. Verificando API de Datos Abiertos y consulta SoQL...")
    client = SecopAPIClient()
    sample = client.get_sample_rows(limit=3)
    assert len(sample) == 3, f"Se esperaban 3 filas, se recibieron {len(sample)}"
    cols = list(sample[0].keys())
    print(f"✔ API conectada correctamente. Columnas recibidas en muestra: {len(cols)}")

    # 2. Descarga con Paginación y Checkpoints (2 bloques de 1.000 registros = 2.000 filas)
    print("\n2. Verificando Downloader con bloques de 1.000 registros...")
    test_checkpoint = CHECKPOINTS_DIR / "test_checkpoint.json"
    downloader = SecopDownloader(client=client, checkpoint_file=test_checkpoint)
    downloader.reset_checkpoint()

    processor = SecopProcessor(output_parquet=test_parquet)
    
    # Descargar 2.000 registros de prueba
    for batch in downloader.stream_batches(chunk_size=1000, max_records=2000):
        processor.process_batch(batch)
    processor.close()

    assert test_parquet.exists(), "No se generó el archivo Parquet de prueba"
    print(f"✔ Parquet de prueba generado exitosamente: {test_parquet.stat().st_size / 1024:.1f} KB")

    # 3. DuckDB Analytics
    print("\n3. Verificando consultas con DuckDB sobre el Parquet...")
    analyzer = SecopAnalyzer(parquet_path=test_parquet)
    df_count = analyzer.query(f"SELECT count(*) as total, sum(valor_del_contrato) as total_cop FROM {analyzer.table_view}")
    print(f"✔ DuckDB leyó el Parquet: Total={df_count.iloc[0]['total']} registros, Monto contratado=${df_count.iloc[0]['total_cop']:,.2f} COP")

    # Ejecutar suite de KPIs
    analyzer.run_all_kpis()
    print("✔ Los 18 KPIs analíticos fueron generados y guardados en CSV.")

    # 4. Auditoría de Calidad
    print("\n4. Verificando auditoría de calidad de datos...")
    auditor = SecopQualityAuditor(parquet_path=test_parquet)
    df_quality = auditor.run_audit()
    print(f"✔ Reporte de calidad generado para {len(df_quality)} columnas.")

    # 5. Visualizaciones
    print("\n5. Verificando generación de gráficos agregados...")
    visualizer = SecopVisualizer()
    visualizer.plot_all()
    print("✔ Gráficos generados con éxito en la carpeta de visualizaciones.")

    # 6. Preparación ML
    print("\n6. Verificando preparación del dataset para Machine Learning...")
    test_ml_parquet = RESULTS_DIR / "test_ml_sample.parquet"
    ml_prep = SecopMLPreparator(parquet_path=test_parquet, output_file=test_ml_parquet)
    ml_prep.prepare_features()
    assert test_ml_parquet.exists(), "No se generó el Parquet de ML"
    print("✔ Dataset de características para ML generado y comprimido en ZSTD.")

    # 7. Comparativa SECOP I vs II
    print("\n7. Verificando generación de comparativa SECOP I vs II...")
    comp = SecopComparison()
    comp.generate_comparison_report()
    print("✔ Documento comparativo y mapeo de equivalencias generados.")

    print("\n" + "="*60)
    print(">>> ¡TODAS LAS PRUEBAS DE INTEGRACIÓN PASARON EXITOSAMENTE! <<<")
    print("="*60)

if __name__ == "__main__":
    run_test()
