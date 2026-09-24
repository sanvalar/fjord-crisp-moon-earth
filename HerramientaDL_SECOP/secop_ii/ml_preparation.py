"""
Módulo para preparar el Feature Store de Machine Learning a partir de los datos Parquet de SECOP II.
Genera variables matemáticas (logaritmo de valor, ratios financieros, duraciones y flags).
"""
import logging
from pathlib import Path
import duckdb

logger = logging.getLogger("secop_ii.ml_preparation")

class SecopMLPreparator:
    def __init__(self, parquet_path: Path, output_features_parquet: Path = None):
        self.parquet_path = parquet_path
        self.output_parquet = output_features_parquet or Path(__file__).resolve().parent.parent / "results" / "ml" / "secop_ii_ml_features.parquet"
        self.con = duckdb.connect(database=":memory:")
        self.clean_in = str(self.parquet_path).replace("\\", "/")
        self.clean_out = str(self.output_parquet).replace("\\", "/")

    def prepare_features(self):
        """Genera el dataset con atributos numéricos y categóricos listos para modelos de ML."""
        if not self.parquet_path.exists():
            logger.warning(f"No existe {self.parquet_path}. Saltando preparación de ML.")
            return

        logger.info(f"Preparando tabla de features para Machine Learning desde {self.parquet_path}...")
        self.output_parquet.parent.mkdir(parents=True, exist_ok=True)

        sql = f"""
        COPY (
            SELECT 
                id_contrato,
                -- Transformación logarítmica para mitigar asimetría de montos
                TRY_CAST(valor_del_contrato AS DOUBLE) as valor_contrato,
                ln(TRY_CAST(valor_del_contrato AS DOUBLE) + 1) as log_valor_contrato,
                -- Ratios financieros de ejecución
                round(TRY_CAST(valor_facturado AS DOUBLE) / (TRY_CAST(valor_del_contrato AS DOUBLE) + 1), 4) as ratio_facturado,
                round(TRY_CAST(valor_pagado AS DOUBLE) / (TRY_CAST(valor_del_contrato AS DOUBLE) + 1), 4) as ratio_pagado,
                -- Tiempos y plazos
                datediff('day', TRY_CAST(fecha_de_inicio_del_contrato AS DATE), TRY_CAST(fecha_de_fin_del_contrato AS DATE)) as duracion_dias_estimada,
                COALESCE(TRY_CAST(dias_adicionados AS INTEGER), 0) as dias_adicionados,
                -- Atributos de clasificación
                orden as orden_gobierno,
                COALESCE(departamento, 'DESCONOCIDO') as departamento,
                modalidad_de_contratacion as modalidad,
                tipo_de_contrato as tipo_contrato,
                estado_contrato,
                -- Flags binarias
                CASE WHEN LOWER(es_pyme) = 'si' THEN 1 ELSE 0 END as flag_pyme,
                CASE WHEN LOWER(espostconflicto) = 'si' THEN 1 ELSE 0 END as flag_postconflicto,
                -- Estacionalidad
                TRY_CAST(strftime(TRY_CAST(fecha_de_firma AS TIMESTAMP), '%Y') AS INTEGER) as anio_firma,
                TRY_CAST(strftime(TRY_CAST(fecha_de_firma AS TIMESTAMP), '%m') AS INTEGER) as mes_firma
            FROM read_parquet('{self.clean_in}')
            WHERE TRY_CAST(valor_del_contrato AS DOUBLE) > 0
        ) TO '{self.clean_out}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """

        self.con.execute(sql)
        logger.info(f"Tabla de Machine Learning creada exitosamente en {self.output_parquet}")
