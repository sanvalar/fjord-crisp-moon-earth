import duckdb
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest

con = duckdb.connect()
df = con.execute("SELECT * FROM read_parquet('data/parquet/secop_tic_ml_features.parquet')").df()

# 1. K-Means
X_clust = df[['log_valor', 'dias_adicionados', 'flag_prorroga', 'flag_innovacion_conpes']].fillna(0)
kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
df['cluster'] = kmeans.fit_predict(X_clust)

# 2. Isolation Forest (Anomalías)
iso = IsolationForest(contamination=0.03, random_state=42)
df['anomaly_score'] = iso.fit_predict(X_clust)
df['score_anom'] = -iso.score_samples(X_clust)

# Resumen clusters
res_clust = []
for c in range(4):
    sub = df[df['cluster'] == c]
    res_clust.append({
        'cluster': c,
        'count': int(len(sub)),
        'monto_total_mm': float(round(sub['valor_contrato'].sum()/1e9, 2)),
        'promedio_m': float(round(sub['valor_contrato'].mean()/1e6, 2)),
        'adiciones_dias_prom': float(round(sub['dias_adicionados'].mean(), 1)),
        'pct_innovacion': float(round(sub['flag_innovacion_conpes'].mean()*100, 1)),
        'top_subsector': sub['subsector_tic'].value_counts().index[0] if len(sub) else 'N/A'
    })

# Anomalías reales detectadas
anomalias = df[df['anomaly_score'] == -1].sort_values('valor_contrato', ascending=False)[
    ['id_contrato_global', 'subsector_tic', 'valor_contrato', 'departamento', 'modalidad', 'dias_adicionados', 'score_anom']
].head(10).to_dict('records')

print("CLUSTERS:")
for r in res_clust:
    print(r)

print("\nTOP ANOMALIAS DETECTADAS:")
for a in anomalias[:3]:
    print(a)
