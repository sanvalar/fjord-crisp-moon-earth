"""
Unificador de esquemas: Integra SECOP I y SECOP II en DuckDB.
Homologa variables a un esquema canónico de 24 columnas y exporta a Parquet ZSTD.
"""
import logging
from pathlib import Path
import duckdb

logger = logging.getLogger("secop_unified")

class SecopUnifier:
    def __init__(self, parquet_secop_i: Path, parquet_secop_ii: Path, output_unified: Path):
        self.parquet_i = parquet_secop_i
        self.parquet_ii = parquet_secop_ii
        self.output_unified = output_unified
        self.con = duckdb.connect(database=":memory:")

    def build_unified_dataset(self):
        """Une los Parquets de SECOP I y SECOP II con proyección homologada."""
        logger.info(f"Unificando SECOP I ({self.parquet_i}) y SECOP II ({self.parquet_ii})...")
        clean_i = str(self.parquet_i).replace("\\", "/")
        clean_ii = str(self.parquet_ii).replace("\\", "/")
        clean_out = str(self.output_unified).replace("\\", "/")

        sql_unify = f"""
        COPY (
            -- Proyección SECOP I
            SELECT 
                'SECOP_I' as sistema_origen,
                uid as id_contrato_global,
                numero_de_proceso as proceso_compra,
                nit_de_la_entidad as nit_entidad,
                nombre_entidad as nombre_entidad,
                orden_entidad as orden_gobierno,
                COALESCE(departamento_entidad, 'NO_DEFINIDO') as departamento,
                COALESCE(municipio_entidad, 'NO_DEFINIDO') as municipio,
                modalidad_de_contratacion as modalidad,
                tipo_de_contrato as tipo_contrato,
                estado_del_proceso as estado_contrato,
                identificacion_del_contratista as documento_proveedor,
                nom_razon_social_contratista as proveedor,
                CASE WHEN LOWER(es_mipyme) = 'si' THEN 1 ELSE 0 END as es_pyme_bin,
                fecha_de_firma_del_contrato as fecha_firma,
                fecha_ini_ejec_contrato as fecha_inicio,
                fecha_fin_ejec_contrato as fecha_fin,
                CAST(anno_cargue_secop AS INTEGER) as anio_contratacion,
                cuantia_contrato as valor_contrato,
                cuantia_contrato as valor_facturado,
                cuantia_contrato as valor_pagado,
                tiempo_adiciones_en_dias as dias_adicionados,
                objeto_a_contratar as objeto_resumido,
                detalle_del_objeto_a_contratar as objeto_detallado
            FROM read_parquet('{clean_i}')
            WHERE cuantia_contrato > 0

            UNION ALL

            -- Proyección SECOP II
            SELECT 
                'SECOP_II' as sistema_origen,
                id_contrato as id_contrato_global,
                proceso_de_compra as proceso_compra,
                nit_entidad as nit_entidad,
                nombre_entidad as nombre_entidad,
                orden as orden_gobierno,
                COALESCE(departamento, 'NO_DEFINIDO') as departamento,
                COALESCE(ciudad, 'NO_DEFINIDO') as municipio,
                modalidad_de_contratacion as modalidad,
                tipo_de_contrato as tipo_contrato,
                estado_contrato as estado_contrato,
                documento_proveedor as documento_proveedor,
                proveedor_adjudicado as proveedor,
                CASE WHEN LOWER(es_pyme) = 'si' THEN 1 ELSE 0 END as es_pyme_bin,
                fecha_de_firma as fecha_firma,
                fecha_de_inicio_del_contrato as fecha_inicio,
                fecha_de_fin_del_contrato as fecha_fin,
                CAST(strftime(fecha_de_firma, '%Y') AS INTEGER) as anio_contratacion,
                valor_del_contrato as valor_contrato,
                valor_facturado as valor_facturado,
                valor_pagado as valor_pagado,
                dias_adicionados as dias_adicionados,
                descripcion_del_proceso as objeto_resumido,
                objeto_del_contrato as objeto_detallado
            FROM read_parquet('{clean_ii}')
            WHERE valor_del_contrato > 0
        ) TO '{clean_out}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """

        self.con.execute(sql_unify)
        total_rows = self.con.execute(f"SELECT count(*) FROM read_parquet('{clean_out}')").fetchone()[0]
        logger.info(f"Dataset unificado SECOP I + II creado en {self.output_unified}. Total contratos: {total_rows:,}")
        return total_rows
