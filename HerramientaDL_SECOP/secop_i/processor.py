"""
Procesador columnar para SECOP I.
Limpia, tipifica y escribe a Parquet comprimido con ZSTD de forma incremental.
"""
import gc
import logging
from pathlib import Path
from typing import List, Dict
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from secop_i.config import (
    OUTPUT_SECOP_I_PARQUET, COLUMNAS_NUMERICAS_SECOP_I,
    COLUMNAS_FECHAS_SECOP_I, COLUMNAS_SENSIBLES_SECOP_I
)

logger = logging.getLogger("secop_i.processor")

class SecopIProcessor:
    def __init__(self, output_parquet: Path = OUTPUT_SECOP_I_PARQUET):
        self.output_parquet = output_parquet
        self.writer = None
        self.schema = None
        self.total_processed = 0

    def clean_and_cast_df(self, df: pd.DataFrame, drop_sensitive: bool = True) -> pd.DataFrame:
        if drop_sensitive:
            cols_to_drop = [c for c in COLUMNAS_SENSIBLES_SECOP_I if c in df.columns]
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)

        # Desanidar dicts
        if "ruta_proceso_en_secop_i" in df.columns:
            df["ruta_proceso_en_secop_i"] = df["ruta_proceso_en_secop_i"].apply(
                lambda v: v.get("url", "") if isinstance(v, dict) else (str(v) if pd.notna(v) else "")
            )

        # Castear numéricos
        for col in COLUMNAS_NUMERICAS_SECOP_I:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False).str.strip(),
                    errors="coerce"
                ).fillna(0.0).astype("float64")

        # Castear fechas
        for col in COLUMNAS_FECHAS_SECOP_I:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Limpiar texto
        for col in df.columns:
            if col not in COLUMNAS_NUMERICAS_SECOP_I and col not in COLUMNAS_FECHAS_SECOP_I:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({"nan": None, "None": None, "": None})

        return df

    def process_batch(self, raw_records: List[Dict]) -> int:
        if not raw_records:
            return 0

        df = pd.DataFrame(raw_records)
        df_clean = self.clean_and_cast_df(df)

        table = pa.Table.from_pandas(df_clean, preserve_index=False)

        if self.writer is None:
            self.schema = table.schema
            self.writer = pq.ParquetWriter(self.output_parquet, schema=self.schema, compression="zstd")
            logger.info(f"ParquetWriter SECOP I inicializado en {self.output_parquet} (ZSTD)")
        else:
            # Asegurar alineación de campos
            for field in self.schema:
                if field.name not in table.column_names:
                    null_arr = pa.nulls(len(table), type=field.type)
                    table = table.append_column(field, null_arr)
            # Reordenar y castear
            table = table.select([f.name for f in self.schema]).cast(self.schema)

        self.writer.write_table(table)
        count = len(df_clean)
        self.total_processed += count

        del df
        del df_clean
        del table
        gc.collect()

        return count

    def close(self):
        if self.writer:
            self.writer.close()
            self.writer = None
            logger.info(f"Escritura SECOP I finalizada. Registros: {self.total_processed:,}")
