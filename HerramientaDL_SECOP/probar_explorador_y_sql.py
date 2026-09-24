import requests

print("=== TEST 1: EXPLORADOR DE DATOS (PAGINACIÓN SERVER-SIDE SOBRE 213.123 CONTRATOS) ===")
r_prev = requests.get('http://localhost:8050/api/preview?file=SECOP_INDUSTRIA_TIC_2015_2025.parquet&page=1&page_size=50', timeout=5)
print('Status Preview:', r_prev.status_code)
d_prev = r_prev.json()
print(f"Total contratos coincidentes: {d_prev.get('total_matched'):,}")
print(f"Páginas totales: {d_prev.get('total_pages'):,}")
print(f"Filas recibidas en pág 1: {len(d_prev.get('rows', []))}")

print("\n=== TEST 2: EXPLORADOR CON BÚSQUEDA DINÁMICA (q='oracle') ===")
r_search = requests.get('http://localhost:8050/api/preview?file=SECOP_INDUSTRIA_TIC_2015_2025.parquet&page=1&page_size=50&q=oracle', timeout=5)
d_search = r_search.json()
print(f"Contratos con 'oracle': {d_search.get('total_matched'):,}")

print("\n=== TEST 3: CONSOLA SQL (SELECT count(*) y agregaciones) ===")
r_sql = requests.post('http://localhost:8050/api/query', json={
    "file": "SECOP_INDUSTRIA_TIC_2015_2025.parquet",
    "sql": "SELECT anio_contratacion, count(*) as cttos, round(sum(valor_contrato)/1e9, 2) as total_mm FROM datos GROUP BY 1 ORDER BY 1"
}, timeout=5)
print('Status SQL:', r_sql.status_code)
d_sql = r_sql.json()
print(f"Total proyectado: {d_sql.get('total_count')} filas de agregación")
for r in d_sql.get('rows', [])[:5]:
    print(" ", r)
