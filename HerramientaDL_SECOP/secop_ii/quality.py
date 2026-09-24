"""
Auditoría de calidad de datos en DuckDB.
Analiza completitud (% nulos), cardinalidad e inconsistencias de fechas y valores.
"""
import logging
from pathlib import Path
import pandas as pd
import duckdb

logger = logging.getLogger("secop_ii.quality")

class SecopQualityAuditor:
    def __init__(self, parquet_path: Path, output_csv: Path = None):
        self.parquet_path = parquet_path
        self.output_csv = output_csv or Path(__file__).resolve().parent.parent / "results" / "metadata" / "calidad_datos.csv"
        self.con = duckdb.connect(database=":memory:")
        self.clean_path = str(self.parquet_path).replace("\\", "/")

    def run_audit(self):
        """Audita columnas en Parquet y genera reporte en CSV."""
        if not self.parquet_path.exists():
            logger.warning(f"No existe el archivo Parquet {self.parquet_path}. Saltando auditoría.")
            return

        logger.info("Iniciando auditoría de calidad de datos con DuckDB...")
        total_rows = self.con.execute(f"SELECT count(*) FROM read_parquet('{self.clean_path}')").fetchone()[0]
        logger.info(f"Total registros a auditar: {total_rows:,}")

        schema_df = self.con.execute(f"DESCRIBE SELECT * FROM read_parquet('{self.clean_path}')").df()
        
        audit_results = []
        for _, row in schema_df.iterrows():
            col_name = row['column_name']
            col_type = row['column_type']

            stats = self.con.execute(f"""
                SELECT 
                    count(*) - count("{col_name}") as null_count,
                    approx_count_distinct("{col_name}") as distinct_count
                FROM read_parquet('{self.clean_path}')
            """).fetchone()

            null_count = stats[0]
            distinct_count = stats[1]
            pct_null = (null_count / total_rows * 100) if total_rows > 0 else 0

            audit_results.append({
                "columna": col_name,
                "tipo_dato": col_type,
                "total_nulos": null_count,
                "porcentaje_nulos": round(pct_null, 2),
                "valores_unicos_aprox": distinct_count
            })

        df_audit = pd.DataFrame(audit_results)
        self.output_csv.parent.mkdir(parents=True, exist_ok=True)
        df_audit.to_csv(self.output_csv, index=False, encoding="utf-8")
        logger.info(f"Auditoría guardada exitosamente en {self.output_csv}")

        # Comprobación de reglas de integridad
        val_zero = self.con.execute(f"""
            SELECT count(*) FROM read_parquet('{self.clean_path}') 
            WHERE TRY_CAST(valor_del_contrato AS DOUBLE) <= 0
        """).fetchone()[0]
        if val_zero > 0:
            logger.warning(f"Alerta: Se detectaron {val_zero:,} contratos con valor <= $0 COP.")

        date_inconsistencies = self.con.execute(f"""
            SELECT count(*) FROM read_parquet('{self.clean_path}')
            WHERE TRY_CAST(fecha_de_fin_del_contrato AS TIMESTAMP) < TRY_CAST(fecha_de_inicio_del_contrato AS TIMESTAMP)
        """).fetchone()[0]
        if date_inconsistencies > 0:
            logger.warning(f"Alerta: Se detectaron {date_inconsistencies:,} contratos con fecha de fin anterior a inicio.")
