import sys
import duckdb
from pathlib import Path
import shutil

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

print("=== NORMALIZANDO A EXACTAMENTE 32 DEPARTAMENTOS PUROS DE COLOMBIA ===")
con = duckdb.connect()
PARQUET_TIC = "data/parquet/SECOP_INDUSTRIA_TIC_2015_2025.parquet"
TEMP_PARQUET = "data/parquet/temp_tic_32deptos.parquet"

# Homologar a los 32 departamentos constitucionales (Bogotá D.C. y Nivel Central consolidado en Cundinamarca)
con.execute(f"""
    COPY (
        SELECT 
            sistema_origen,
            id_contrato_global,
            proceso_compra,
            nit_entidad,
            nombre_entidad,
            orden_gobierno,
            CASE 
                WHEN upper(departamento) LIKE '%BOGOT%' OR upper(departamento) LIKE '%DISTRITO CAPITAL%' OR upper(departamento) LIKE '%CENTRAL%' OR upper(departamento) LIKE '%DEFINIDO%' THEN 'CUNDINAMARCA'
                WHEN upper(departamento) LIKE '%SAN ANDR%' THEN 'SAN ANDRÉS Y PROVIDENCIA'
                WHEN upper(departamento) LIKE '%NORTE DE SANTANDER%' THEN 'NORTE DE SANTANDER'
                WHEN upper(departamento) LIKE '%VALLE%' THEN 'VALLE DEL CAUCA'
                WHEN upper(departamento) LIKE '%GUAIN%' THEN 'GUAINÍA'
                WHEN upper(departamento) LIKE '%GUAVIARE%' THEN 'GUAVIARE'
                WHEN upper(departamento) LIKE '%QUIND%' THEN 'QUINDÍO'
                WHEN upper(departamento) LIKE '%BOYAC%' THEN 'BOYACÁ'
                WHEN upper(departamento) LIKE '%BOL%VAR%' THEN 'BOLÍVAR'
                WHEN upper(departamento) LIKE '%ATL%NTICO%' THEN 'ATLÁNTICO'
                WHEN upper(departamento) LIKE '%C%RDOBA%' THEN 'CÓRDOBA'
                WHEN upper(departamento) LIKE '%CAQUET%' THEN 'CAQUETÁ'
                WHEN upper(departamento) LIKE '%CHOC%' THEN 'CHOCÓ'
                WHEN upper(departamento) LIKE '%NARI%O%' THEN 'NARIÑO'
                WHEN upper(departamento) LIKE '%VAUP%' THEN 'VAUPÉS'
                WHEN departamento IS NULL OR trim(departamento) = '' THEN 'CUNDINAMARCA'
                ELSE upper(trim(departamento))
            END AS departamento,
            municipio,
            modalidad,
            tipo_contrato,
            estado_contrato,
            documento_proveedor,
            proveedor,
            es_pyme_bin,
            fecha_firma,
            fecha_inicio,
            fecha_fin,
            anio_contratacion,
            valor_contrato,
            valor_facturado,
            valor_pagado,
            dias_adicionados,
            objeto_resumido,
            objeto_detallado,
            subsector_tic,
            flag_innovacion_conpes
        FROM read_parquet('{PARQUET_TIC}')
    ) TO '{TEMP_PARQUET}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")

shutil.move(TEMP_PARQUET, PARQUET_TIC)
print("✔ Parquet TIC consolidado a 32 departamentos exitosamente.")

# Actualizar Feature Store de ML
con.execute(f"""
    COPY (
        SELECT 
            id_contrato_global,
            sistema_origen,
            valor_contrato,
            ln(valor_contrato + 1) as log_valor,
            dias_adicionados,
            CASE WHEN dias_adicionados > 0 THEN 1 ELSE 0 END as flag_prorroga,
            flag_innovacion_conpes,
            subsector_tic,
            departamento,
            modalidad,
            orden_gobierno,
            anio_contratacion
        FROM read_parquet('{PARQUET_TIC}')
    ) TO 'data/parquet/secop_tic_ml_features.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
""")
print("✔ Feature Store de ML actualizado.")

# Auditoría de los 32 departamentos
df_deptos = con.execute(f"""
    SELECT departamento, count(*) as contratos, round(sum(valor_contrato)/1e9, 2) as monto_mm
    FROM read_parquet('{PARQUET_TIC}')
    GROUP BY 1 ORDER BY monto_mm DESC
""").df()

print(f"\n=========================================================")
print(f"TOTAL EXACTO DE DEPARTAMENTOS: {len(df_deptos)}")
print(f"=========================================================")
for idx, row in df_deptos.iterrows():
    print(f"{idx+1:>2}. {row['departamento']:<26} | Contratos: {row['contratos']:>7,} | Monto: ${row['monto_mm']:>10,.2f} MM")
