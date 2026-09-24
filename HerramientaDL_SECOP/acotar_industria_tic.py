"""
Módulo de Limpieza, Filtrado y Acotamiento Sectorial: Industria TIC (2015-2025).
Aplica filtros rigurosos sobre el objeto contractual y limpia errores tipográficos
multibillonarios de las entidades públicas.
"""
import sys
import logging
from pathlib import Path
import duckdb

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "parquet"
RESULTS_DIR = BASE_DIR / "results" / "industria_tic"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

INPUT_TIC_PARQUET = DATA_DIR / "SECOP_INDUSTRIA_TIC_2015_2025.parquet"
OUTPUT_ML_TIC = DATA_DIR / "secop_tic_ml_features.parquet"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("secop_filtro_tic")

def clean_and_refine_tic():
    logger.info("=== REFINANDO Y AUDITANDO EL DATASET DE INDUSTRIA TIC ===")
    con = duckdb.connect()
    clean_in = str(INPUT_TIC_PARQUET).replace("\\", "/")
    temp_clean = str(DATA_DIR / "temp_clean_tic.parquet").replace("\\", "/")

    # Excluir errores tipográficos superiores a 500 mil millones y contratos no tecnológicos
    sql_clean = f"""
    COPY (
        SELECT 
            *
        FROM read_parquet('{clean_in}')
        WHERE 
            valor_contrato > 100000 
            AND valor_contrato < 500000000000
            AND anio_contratacion >= 2015 
            AND anio_contratacion <= 2025
            AND (
                LOWER(objeto_resumido) LIKE '%software%' OR 
                LOWER(objeto_resumido) LIKE '%tecnolog%' OR 
                LOWER(objeto_resumido) LIKE '%sistema%' OR 
                LOWER(objeto_resumido) LIKE '%comput%' OR
                LOWER(objeto_resumido) LIKE '%conectividad%' OR
                LOWER(objeto_resumido) LIKE '%telecomunicac%' OR
                LOWER(objeto_resumido) LIKE '%ciberseguridad%' OR
                LOWER(objeto_resumido) LIKE '%licenciamiento%' OR
                LOWER(objeto_resumido) LIKE '%internet%' OR
                LOWER(objeto_resumido) LIKE '%redes%' OR
                LOWER(objeto_resumido) LIKE '%servidor%' OR
                LOWER(objeto_resumido) LIKE '%datacenter%' OR
                LOWER(objeto_resumido) LIKE '%informatic%'
            )
            AND LOWER(objeto_resumido) NOT LIKE '%arrastre%'
            AND LOWER(objeto_resumido) NOT LIKE '%grua%'
            AND LOWER(objeto_resumido) NOT LIKE '%carcel%'
            AND LOWER(objeto_resumido) NOT LIKE '%emprstito%'
            AND LOWER(objeto_resumido) NOT LIKE '%emprestito%'
    ) TO '{temp_clean}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    con.execute(sql_clean)

    # Reemplazar con el archivo final limpio
    import shutil
    shutil.move(str(DATA_DIR / "temp_clean_tic.parquet"), str(INPUT_TIC_PARQUET))

    total = con.execute(f"SELECT count(*) FROM read_parquet('{clean_in}')").fetchone()[0]
    monto_b = con.execute(f"SELECT round(sum(valor_contrato)/1e9, 2) FROM read_parquet('{clean_in}')").fetchone()[0]
    logger.info(f"✔ Dataset Refinado Exitosamente!")
    logger.info(f"• Total Contratos TIC Válidos: {total:,}")
    logger.info(f"• Monto Total TIC: ${monto_b:,.2f} Mil Millones COP")

    # Guardar resúmenes
    con.execute(f"""
        COPY (
            SELECT 
                subsector_tic,
                count(*) as contratos,
                round(sum(valor_contrato)/1e9, 2) as monto_miles_millones,
                round(avg(valor_contrato)/1e6, 2) as promedio_millones
            FROM read_parquet('{clean_in}')
            GROUP BY 1 ORDER BY monto_miles_millones DESC
        ) TO '{str(RESULTS_DIR / "tic_por_subsector.csv").replace("\\", "/")}' (HEADER, DELIMITER ',');
    """)

    con.execute(f"""
        COPY (
            SELECT 
                anio_contratacion,
                count(*) as contratos,
                round(sum(valor_contrato)/1e9, 2) as monto_miles_millones,
                round(avg(valor_contrato)/1e6, 2) as promedio_millones
            FROM read_parquet('{clean_in}')
            GROUP BY 1 ORDER BY 1
        ) TO '{str(RESULTS_DIR / "tic_por_anio.csv").replace("\\", "/")}' (HEADER, DELIMITER ',');
    """)

    con.execute(f"""
        COPY (
            SELECT 
                departamento,
                count(*) as contratos,
                round(sum(valor_contrato)/1e9, 2) as monto_miles_millones
            FROM read_parquet('{clean_in}')
            GROUP BY 1 ORDER BY monto_miles_millones DESC
        ) TO '{str(RESULTS_DIR / "tic_por_departamento.csv").replace("\\", "/")}' (HEADER, DELIMITER ',');
    """)
    logger.info("✔ Resúmenes CSV generados en results/industria_tic/")

if __name__ == "__main__":
    clean_and_refine_tic()
