"""
Script de verificación y testing del módulo SECOP I y Unificación con SECOP II.
"""
import sys
from pathlib import Path

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent))

from secop_i.api import SecopIAPIClient
from secop_i.downloader import SecopIDownloader
from secop_i.processor import SecopIProcessor
from secop_unified import SecopUnifier
from secop_i.config import RESULTS_DIR

def test_secop_i():
    print("\n" + "="*60)
    print(">>> INICIANDO TEST SECOP I + UNIFICACIÓN CON SECOP II <<<")
    print("="*60)

    # 1. API Client SECOP I
    print("\n1. Conectando a la API oficial de SECOP I (f789-7hwg)...")
    client = SecopIAPIClient()
    sample = client.get_sample_rows(limit=2)
    print(f"✔ Conectado exitosamente. Columnas recibidas en muestra: {len(sample[0].keys())}")

    # 2. Downloader SECOP I (descargar muestra controlada de 1.000 registros para 2015-2025)
    print("\n2. Descargando bloque de muestra de SECOP I (periodo 2015-2025)...")
    test_parquet_i = RESULTS_DIR / "test_secop_i_sample.parquet"
    if test_parquet_i.exists():
        test_parquet_i.unlink()

    downloader = SecopIDownloader(client=client)
    downloader.reset_checkpoint()
    processor = SecopIProcessor(output_parquet=test_parquet_i)

    for batch in downloader.stream_batches(chunk_size=1000, max_records=1000, anno_min=2015, anno_max=2025):
        processor.process_batch(batch)
    processor.close()
    print(f"✔ Parquet SECOP I generado: {test_parquet_i.stat().st_size / 1024:.1f} KB")

    # 3. Unificación SECOP I + SECOP II con DuckDB
    print("\n3. Unificando muestras de SECOP I y SECOP II...")
    test_parquet_ii = RESULTS_DIR / "test_secop_ii_sample.parquet"
    test_unified_parquet = RESULTS_DIR / "test_secop_unified.parquet"

    unifier = SecopUnifier(
        parquet_secop_i=test_parquet_i,
        parquet_secop_ii=test_parquet_ii,
        output_unified=test_unified_parquet
    )
    total_unificados = unifier.build_unified_dataset()
    print(f"✔ Tabla unificada creada exitosamente: {total_unificados:,} contratos consolidados.")

    # 4. Verificación de distribución por origen
    import duckdb
    con = duckdb.connect()
    clean_p = str(test_unified_parquet).replace("\\", "/")
    df_dist = con.execute(f"""
        SELECT 
            sistema_origen, 
            count(*) as total_contratos, 
            round(sum(valor_contrato)/1e9, 2) as total_miles_millones_cop
        FROM read_parquet('{clean_p}')
        GROUP BY 1
    """).df()
    print("\nDistribución del Dataset Unificado:")
    print(df_dist.to_string(index=False))

    print("\n" + "="*60)
    print(">>> TEST DE SECOP I Y UNIFICACIÓN FINALIZADO CON ÉXITO <<<")
    print("="*60)

if __name__ == "__main__":
    test_secop_i()
