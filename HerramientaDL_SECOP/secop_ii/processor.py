"""
Módulo de procesamiento por bloques de datos de SECOP II.
Convierte registros (de API o archivo local) a tablas PyArrow con tipado estricto,
limpieza de nulos/inconsistencias y escritura incremental en Parquet con compresión ZSTD.
"""
import gc
import logging
from pathlib import Path
from typing import List, Dict, Generator
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from secop_ii.config import (
    OUTPUT_PARQUET_FILE, COLUMNAS_NUMERICAS, COLUMNAS_FECHAS,
    COLUMNAS_SENSIBLES
)

logger = logging.getLogger("secop_ii.processor")

class SecopProcessor:
    def __init__(self, output_parquet: Path = OUTPUT_PARQUET_FILE):
        self.output_parquet = output_parquet
        self.writer = None
        self.schema = None
        self.total_processed = 0
        self.total_discarded = 0

    def clean_and_cast_df(self, df: pd.DataFrame, drop_sensitive: bool = True) -> pd.DataFrame:
        """
        Limpia y castea las columnas a tipos correctos (numéricos, fechas, texto).
        """
        # 1. Tratar columnas sensibles si se solicita anonimizar/proteger
        if drop_sensitive:
            cols_to_drop = [c for c in COLUMNAS_SENSIBLES if c in df.columns]
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)

        # 2. Desanidar columnas tipo dict (como urlproceso)
        if "urlproceso" in df.columns:
            def extract_url(val):
                if isinstance(val, dict):
                    return val.get("url", "")
                return str(val) if pd.notna(val) else ""
            df["urlproceso"] = df["urlproceso"].apply(extract_url)

        # 3. Castear columnas numéricas (limpiando strings con comas, puntos o espacios)
        for col in COLUMNAS_NUMERICAS:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False).str.strip(),
                    errors="coerce"
                ).fillna(0.0).astype("float64")

        # 4. Normalizar columnas de fechas a formato ISO / datetime
        for col in COLUMNAS_FECHAS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # 5. Normalizar strings (eliminar espacios superfluos)
        for col in df.columns:
            if col not in COLUMNAS_NUMERICAS and col not in COLUMNAS_FECHAS:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({"nan": None, "None": None, "": None})

        return df

    def get_canonical_columns(self, drop_sensitive: bool = True):
        # 85 columnas oficiales de Socrata + 4 campos de sistema (:id, :version, :created_at, :updated_at)
        official_cols = [
            'nombre_entidad', 'nit_entidad', 'departamento', 'ciudad', 'localizaci_n', 
            'orden', 'sector', 'rama', 'entidad_centralizada', 'proceso_de_compra', 
            'id_contrato', 'referencia_del_contrato', 'estado_contrato', 
            'codigo_de_categoria_principal', 'descripcion_del_proceso', 'tipo_de_contrato', 
            'modalidad_de_contratacion', 'justificacion_modalidad_de', 'fecha_de_firma', 
            'fecha_de_inicio_del_contrato', 'fecha_de_fin_del_contrato', 'condiciones_de_entrega', 
            'tipodocproveedor', 'documento_proveedor', 'proveedor_adjudicado', 'es_grupo', 
            'es_pyme', 'habilita_pago_adelantado', 'liquidaci_n', 'obligaci_n_ambiental', 
            'obligaciones_postconsumo', 'reversion', 'origen_de_los_recursos', 'destino_gasto', 
            'valor_del_contrato', 'valor_de_pago_adelantado', 'valor_facturado', 
            'valor_pendiente_de_pago', 'valor_pagado', 'valor_amortizado', 'valor_pendiente_de', 
            'valor_pendiente_de_ejecucion', 'saldo_cdp', 'saldo_vigencia', 'espostconflicto', 
            'dias_adicionados', 'puntos_del_acuerdo', 'pilares_del_acuerdo', 'urlproceso', 
            'nombre_representante_legal', 'nacionalidad_representante_legal', 
            'domicilio_representante_legal', 'tipo_de_identificaci_n_representante_legal', 
            'identificaci_n_representante_legal', 'g_nero_representante_legal', 
            'presupuesto_general_de_la_nacion_pgn', 'sistema_general_de_participaciones', 
            'sistema_general_de_regal_as', 
            'recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_', 
            'recursos_de_credito', 'recursos_propios', 'ultima_actualizacion', 'codigo_entidad', 
            'codigo_proveedor', 'fecha_inicio_liquidacion', 'fecha_fin_liquidacion', 
            'objeto_del_contrato', 'duraci_n_del_contrato', 'nombre_del_banco', 'tipo_de_cuenta', 
            'n_mero_de_cuenta', 'el_contrato_puede_ser_prorrogado', 
            'fecha_de_notificaci_n_de_prorrogaci_n', 'nombre_ordenador_del_gasto', 
            'tipo_de_documento_ordenador_del_gasto', 'n_mero_de_documento_ordenador_del_gasto', 
            'nombre_supervisor', 'tipo_de_documento_supervisor', 'n_mero_de_documento_supervisor', 
            'nombre_ordenador_de_pago', 'tipo_de_documento_ordenador_de_pago', 
            'n_mero_de_documento_ordenador_de_pago', 'documentos_tipo', 
            'descripcion_documentos_tipo', 'direcci_n_de_ejecuci_n_del_contrato',
            ':id', ':version', ':created_at', ':updated_at'
        ]
        if drop_sensitive:
            official_cols = [c for c in official_cols if c not in COLUMNAS_SENSIBLES]
        return official_cols

    def process_batch(self, raw_records: List[Dict]) -> int:
        """
        Procesa una lista de diccionarios de la API y la añade al archivo Parquet.
        """
        if not raw_records:
            return 0

        df = pd.DataFrame(raw_records)
        df_clean = self.clean_and_cast_df(df)
        
        canonical_cols = self.get_canonical_columns()
        # Asegurar todas las columnas canónicas con valores por defecto
        for col in canonical_cols:
            if col not in df_clean.columns:
                if col in COLUMNAS_NUMERICAS:
                    df_clean[col] = 0.0
                elif col in COLUMNAS_FECHAS:
                    df_clean[col] = pd.NaT
                else:
                    df_clean[col] = None

        # Reordenar exactamente en el orden canónico
        df_clean = df_clean[canonical_cols]

        # Convertir a PyArrow
        table = pa.Table.from_pandas(df_clean, preserve_index=False)

        if self.writer is None:
            self.schema = table.schema
            self.writer = pq.ParquetWriter(
                self.output_parquet, 
                schema=self.schema, 
                compression="zstd"
            )
            logger.info(f"ParquetWriter inicializado en {self.output_parquet} con compresión ZSTD.")
        else:
            table = table.cast(self.schema)

        self.writer.write_table(table)
        count = len(df_clean)
        self.total_processed += count

        del df
        del df_clean
        del table
        gc.collect()

        return count

    def close(self):
        """Cierra el escritor Parquet liberando los buffers a disco."""
        if self.writer:
            self.writer.close()
            self.writer = None
            logger.info(f"Escritura Parquet completada. Total filas escritas: {self.total_processed}")

    def convert_csv_streaming(self, csv_path: Path = None, chunk_size: int = 50000, max_rows: int = None):
        """
        Convierte de manera ultraeficiente un CSV grande a Parquet por bloques (streaming),
        manteniendo un uso insignificante de RAM (<300 MB).
        """
        if csv_path is None or not csv_path.exists():
            raise FileNotFoundError(f"No existe el archivo especificado: {csv_path}")

        logger.info(f"Iniciando conversión streaming desde {csv_path} hacia {self.output_parquet} en bloques de {chunk_size}...")
        
        # Leemos el CSV por chunks con pandas
        chunks = pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            low_memory=False,
            encoding="utf-8-sig",
            on_bad_lines="skip"
        )

        rows_done = 0
        for i, chunk_df in enumerate(chunks):
            if max_rows and rows_done >= max_rows:
                break
            if max_rows and (rows_done + len(chunk_df)) > max_rows:
                chunk_df = chunk_df.iloc[:(max_rows - rows_done)]

            df_clean = self.clean_and_cast_df(chunk_df)
            table = pa.Table.from_pandas(df_clean, preserve_index=False)

            if self.writer is None:
                self.schema = table.schema
                self.writer = pq.ParquetWriter(
                    self.output_parquet, 
                    schema=self.schema, 
                    compression="zstd"
                )

            self.writer.write_table(table)
            count = len(df_clean)
            rows_done += count
            self.total_processed += count

            del chunk_df
            del df_clean
            del table
            gc.collect()

            if (i + 1) % 5 == 0 or max_rows:
                logger.info(f"Bloques procesados: {i+1} ({rows_done:,} registros guardados en Parquet)")

        self.close()
        logger.info(f"Conversión completada con éxito. Registros totales en Parquet: {self.total_processed:,}")
        return self.total_processed
