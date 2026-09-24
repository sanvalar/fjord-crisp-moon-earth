import sys
import duckdb
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest
import pandas as pd
import numpy as np

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


print("=== CONSTRUYENDO FEATURE STORE DE ML PARA 213.123 CONTRATOS TIC ===")
con = duckdb.connect()
con.execute("""
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
        FROM read_parquet('data/parquet/SECOP_INDUSTRIA_TIC_2015_2025.parquet')
    ) TO 'data/parquet/secop_tic_ml_features.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
""")

count = con.execute("SELECT count(*) FROM read_parquet('data/parquet/secop_tic_ml_features.parquet')").fetchone()[0]
print(f"✔ Feature Store guardado con {count:,} contratos.")

# Entrenar K-Means e Isolation Forest de validación
print("\n=== ENTRENANDO MODELOS DE MACHINE LEARNING SOBRE EL UNIVERSO COMPLETO ===")
df = con.execute("SELECT * FROM read_parquet('data/parquet/secop_tic_ml_features.parquet')").df()

X = df[['log_valor', 'dias_adicionados', 'flag_prorroga', 'flag_innovacion_conpes']].fillna(0)

# K-Means
km = KMeans(n_clusters=4, random_state=42, n_init=10)
df['cluster'] = km.fit_predict(X)
print("✔ K-Means entrenado exitosamente. Distribución por clúster:")
for c, grp in df.groupby('cluster'):
    print(f"  • Clúster {c}: {len(grp):>7,} contratos | Promedio: ${grp['valor_contrato'].mean()/1e6:>8.1f}M | Adición prom: {grp['dias_adicionados'].mean():>5.1f} días")

# Isolation Forest
iso = IsolationForest(contamination=0.01, random_state=42, n_jobs=-1)
df['anomalia'] = iso.fit_predict(X)
anom_count = (df['anomalia'] == -1).sum()
print(f"✔ Isolation Forest entrenado. Anomalías detectadas: {anom_count:,} contratos ({anom_count/len(df)*100:.1f}%)")

# Indicadores CONPES 4069
conpes_df = df[df['flag_innovacion_conpes'] == 1]
print(f"✔ Alineación CONPES 4069: {len(conpes_df):,} contratos ({len(conpes_df)/len(df)*100:.1f}%) por un total de ${conpes_df['valor_contrato'].sum()/1e9:,.2f} Mil Millones COP.")
