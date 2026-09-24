"""
Módulo analítico en DuckDB sobre archivos Parquet.
Calcula los 18 KPIs requeridos y exporta resúmenes a CSV.
"""
import logging
from pathlib import Path
import duckdb
import pandas as pd
from secop_ii.config import OUTPUT_PARQUET_FILE, RESUMENES_DIR

logger = logging.getLogger("secop_ii.analysis")

class SecopAnalyzer:
    def __init__(self, parquet_path: Path = OUTPUT_PARQUET_FILE):
        self.parquet_path = parquet_path
        self.con = duckdb.connect(database=":memory:")
        clean_path = str(self.parquet_path).replace("\\", "/")
        self.table_view = f"read_parquet('{clean_path}')"
        logger.info(f"DuckDB conectado con vista sobre {self.table_view}")

    def query(self, sql_query: str) -> pd.DataFrame:
        """Ejecuta una consulta SQL en DuckDB y retorna un DataFrame."""
        logger.debug(f"Ejecutando SQL en DuckDB: {sql_query}")
        return self.con.execute(sql_query).df()

    def run_all_kpis(self) -> dict:
        """Calcula los 18 KPIs y guarda los resúmenes en la carpeta results/resumenes/."""
        logger.info("Ejecutando suite analítica completa (18 KPIs con DuckDB)...")
        results = {}

        # 1-4. Métricas anuales (cantidad, total, promedio, mediana)
        sql_anual = f"""
        SELECT 
            strftime(fecha_de_firma, '%Y') as anio,
            COUNT(*) as cantidad_contratos,
            ROUND(SUM(valor_del_contrato), 2) as valor_total_contratado,
            ROUND(AVG(valor_del_contrato), 2) as valor_promedio_contrato,
            ROUND(MEDIAN(valor_del_contrato), 2) as mediana_valor_contrato,
            ROUND(MIN(valor_del_contrato), 2) as valor_minimo,
            ROUND(MAX(valor_del_contrato), 2) as valor_maximo
        FROM {self.table_view}
        WHERE fecha_de_firma IS NOT NULL
        GROUP BY 1
        ORDER BY 1 ASC
        """
        df_anual = self.query(sql_anual)
        df_anual.to_csv(RESUMENES_DIR / "resumen_anual.csv", index=False)
        results["resumen_anual"] = df_anual

        # 5. Por departamento
        sql_depto = f"""
        SELECT 
            COALESCE(departamento, 'NO IDENTIFICADO') as departamento,
            COUNT(*) as cantidad_contratos,
            ROUND(SUM(valor_del_contrato), 2) as valor_total,
            ROUND(AVG(valor_del_contrato), 2) as valor_promedio
        FROM {self.table_view}
        GROUP BY 1
        ORDER BY cantidad_contratos DESC
        """
        df_depto = self.query(sql_depto)
        df_depto.to_csv(RESUMENES_DIR / "resumen_departamentos.csv", index=False)
        results["resumen_departamentos"] = df_depto

        # 6. Top municipios
        sql_muni = f"""
        SELECT 
            COALESCE(ciudad, 'NO IDENTIFICADO') as ciudad_municipio,
            COALESCE(departamento, 'NO IDENTIFICADO') as departamento,
            COUNT(*) as cantidad_contratos,
            ROUND(SUM(valor_del_contrato), 2) as valor_total
        FROM {self.table_view}
        GROUP BY 1, 2
        ORDER BY valor_total DESC
        LIMIT 20
        """
        df_muni = self.query(sql_muni)
        df_muni.to_csv(RESUMENES_DIR / "top_municipios.csv", index=False)
        results["top_municipios"] = df_muni

        # 7. Por sector
        sql_sector = f"""
        SELECT 
            COALESCE(sector, 'NO IDENTIFICADO') as sector,
            COUNT(*) as cantidad_contratos,
            ROUND(SUM(valor_del_contrato), 2) as valor_total,
            ROUND(AVG(valor_del_contrato), 2) as valor_promedio
        FROM {self.table_view}
        GROUP BY 1
        ORDER BY valor_total DESC
        """
        df_sector = self.query(sql_sector)
        df_sector.to_csv(RESUMENES_DIR / "resumen_sectores.csv", index=False)
        results["resumen_sectores"] = df_sector

        # 8. Por modalidad
        sql_modalidad = f"""
        SELECT 
            COALESCE(modalidad_de_contratacion, 'NO IDENTIFICADO') as modalidad,
            COUNT(*) as cantidad_contratos,
            ROUND(SUM(valor_del_contrato), 2) as valor_total,
            ROUND(AVG(valor_del_contrato), 2) as valor_promedio
        FROM {self.table_view}
        GROUP BY 1
        ORDER BY cantidad_contratos DESC
        """
        df_modalidad = self.query(sql_modalidad)
        df_modalidad.to_csv(RESUMENES_DIR / "resumen_modalidades.csv", index=False)
        results["resumen_modalidades"] = df_modalidad

        # 9. Por tipo de contrato
        sql_tipo = f"""
        SELECT 
            COALESCE(tipo_de_contrato, 'NO IDENTIFICADO') as tipo_contrato,
            COUNT(*) as cantidad_contratos,
            ROUND(SUM(valor_del_contrato), 2) as valor_total
        FROM {self.table_view}
        GROUP BY 1
        ORDER BY cantidad_contratos DESC
        """
        df_tipo = self.query(sql_tipo)
        df_tipo.to_csv(RESUMENES_DIR / "resumen_tipos_contrato.csv", index=False)
        results["resumen_tipos_contrato"] = df_tipo

        # 10. Top entidades
        sql_entidades = f"""
        SELECT 
            nit_entidad,
            nombre_entidad,
            orden,
            COUNT(*) as cantidad_contratos,
            ROUND(SUM(valor_del_contrato), 2) as valor_total
        FROM {self.table_view}
        WHERE nombre_entidad IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY valor_total DESC
        LIMIT 20
        """
        df_entidades = self.query(sql_entidades)
        df_entidades.to_csv(RESUMENES_DIR / "top_entidades_compradoras.csv", index=False)
        results["top_entidades"] = df_entidades

        # 11. Top proveedores
        sql_proveedores = f"""
        SELECT 
            documento_proveedor,
            proveedor_adjudicado,
            es_pyme,
            COUNT(*) as cantidad_contratos,
            ROUND(SUM(valor_del_contrato), 2) as valor_total
        FROM {self.table_view}
        WHERE proveedor_adjudicado IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY valor_total DESC
        LIMIT 20
        """
        df_proveedores = self.query(sql_proveedores)
        df_proveedores.to_csv(RESUMENES_DIR / "top_proveedores_adjudicados.csv", index=False)
        results["top_proveedores"] = df_proveedores

        # 12. Rangos de valor
        sql_rangos = f"""
        SELECT 
            CASE 
                WHEN valor_del_contrato <= 10000000 THEN '1. Menor a 10M COP'
                WHEN valor_del_contrato <= 50000000 THEN '2. 10M a 50M COP'
                WHEN valor_del_contrato <= 200000000 THEN '3. 50M a 200M COP'
                WHEN valor_del_contrato <= 1000000000 THEN '4. 200M a 1.000M COP'
                WHEN valor_del_contrato <= 5000000000 THEN '5. 1.000M a 5.000M COP'
                ELSE '6. Mayor a 5.000M COP'
            END as rango_valor,
            COUNT(*) as cantidad_contratos,
            ROUND(SUM(valor_del_contrato), 2) as valor_total
        FROM {self.table_view}
        WHERE valor_del_contrato > 0
        GROUP BY 1
        ORDER BY 1 ASC
        """
        df_rangos = self.query(sql_rangos)
        df_rangos.to_csv(RESUMENES_DIR / "distribucion_rangos_valor.csv", index=False)
        results["distribucion_rangos"] = df_rangos

        # 13. Estados
        sql_estados = f"""
        SELECT 
            COALESCE(estado_contrato, 'NO IDENTIFICADO') as estado,
            COUNT(*) as cantidad_contratos,
            ROUND(SUM(valor_del_contrato), 2) as valor_total
        FROM {self.table_view}
        GROUP BY 1
        ORDER BY cantidad_contratos DESC
        """
        df_estados = self.query(sql_estados)
        df_estados.to_csv(RESUMENES_DIR / "resumen_estados.csv", index=False)
        results["resumen_estados"] = df_estados

        # 14. Mensual
        sql_mensual = f"""
        SELECT 
            strftime(fecha_de_firma, '%Y-%m') as mes_firma,
            COUNT(*) as cantidad_contratos,
            ROUND(SUM(valor_del_contrato), 2) as valor_total
        FROM {self.table_view}
        WHERE fecha_de_firma IS NOT NULL
        GROUP BY 1
        ORDER BY 1 ASC
        """
        df_mensual = self.query(sql_mensual)
        df_mensual.to_csv(RESUMENES_DIR / "evolucion_mensual.csv", index=False)
        results["evolucion_mensual"] = df_mensual

        # 15. Adiciones
        sql_adiciones = f"""
        SELECT 
            COUNT(CASE WHEN dias_adicionados > 0 THEN 1 END) as contratos_con_adicion_tiempo,
            ROUND(AVG(CASE WHEN dias_adicionados > 0 THEN dias_adicionados END), 2) as promedio_dias_adicionados,
            MAX(dias_adicionados) as maximo_dias_adicionados
        FROM {self.table_view}
        """
        df_adiciones = self.query(sql_adiciones)
        df_adiciones.to_csv(RESUMENES_DIR / "resumen_adiciones.csv", index=False)
        results["resumen_adiciones"] = df_adiciones

        # 16. Comparativa financiera
        sql_financiero = f"""
        SELECT 
            ROUND(SUM(valor_del_contrato), 2) as valor_total_contratado,
            ROUND(SUM(TRY_CAST(valor_facturado AS DOUBLE)), 2) as valor_total_facturado,
            ROUND(SUM(TRY_CAST(valor_pagado AS DOUBLE)), 2) as valor_total_pagado,
            ROUND(SUM(TRY_CAST(valor_pendiente_de_pago AS DOUBLE)), 2) as valor_pendiente_pago
        FROM {self.table_view}
        """
        df_financiero = self.query(sql_financiero)
        df_financiero.to_csv(RESUMENES_DIR / "resumen_financiero_ejecucion.csv", index=False)
        results["resumen_financiero"] = df_financiero

        # 17. Estadísticas y percentiles
        sql_stats = f"""
        SELECT 
            COUNT(valor_del_contrato) as conteo,
            ROUND(AVG(valor_del_contrato), 2) as media,
            ROUND(STDDEV(valor_del_contrato), 2) as desviacion_estandar,
            ROUND(MIN(valor_del_contrato), 2) as minimo,
            ROUND(QUANTILE_CONT(valor_del_contrato, 0.25), 2) as p25_q1,
            ROUND(MEDIAN(valor_del_contrato), 2) as p50_mediana,
            ROUND(QUANTILE_CONT(valor_del_contrato, 0.75), 2) as p75_q3,
            ROUND(QUANTILE_CONT(valor_del_contrato, 0.99), 2) as p99,
            ROUND(MAX(valor_del_contrato), 2) as maximo
        FROM {self.table_view}
        WHERE valor_del_contrato > 0
        """
        df_stats = self.query(sql_stats)
        df_stats.to_csv(RESUMENES_DIR / "estadisticas_dispersion_valores.csv", index=False)
        results["estadisticas_dispersion"] = df_stats

        # 18. Concentración HHI
        sql_hhi = f"""
        WITH cuotas_proveedores AS (
            SELECT 
                proveedor_adjudicado,
                SUM(valor_del_contrato) as valor_proveedor,
                SUM(valor_del_contrato) * 100.0 / (SELECT SUM(valor_del_contrato) FROM {self.table_view} WHERE valor_del_contrato > 0) as cuota_mercado_pct
            FROM {self.table_view}
            WHERE proveedor_adjudicado IS NOT NULL AND valor_del_contrato > 0
            GROUP BY 1
        )
        SELECT 
            ROUND(SUM(cuota_mercado_pct * cuota_mercado_pct), 2) as indice_hhi_total
        FROM cuotas_proveedores
        """
        df_hhi = self.query(sql_hhi)
        df_hhi.to_csv(RESUMENES_DIR / "indice_hhi_concentracion.csv", index=False)
        results["indice_hhi"] = df_hhi

        logger.info(f"Análisis completado. Todos los 18 KPIs guardados en {RESUMENES_DIR}")
        return results
