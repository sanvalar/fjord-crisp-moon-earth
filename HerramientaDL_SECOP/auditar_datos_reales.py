import duckdb

con = duckdb.connect()

print("================================================================")
print("AUDITORÍA DE DATOS REALES EXTRAÍDOS DE LAS APIS EN DISCO")
print("================================================================")

# 1. SECOP I
r1 = con.execute("SELECT count(*), round(sum(cuantia_contrato)/1e9, 2), min(anno_cargue_secop), max(anno_cargue_secop) FROM read_parquet('data/parquet/SECOP_I_2015_2025.parquet')").fetchall()[0]
print(f"SECOP I real: {r1[0]:,} contratos | ${r1[1]:,} mil millones | Años: {r1[2]} a {r1[3]}")

# 2. SECOP II
r2 = con.execute("SELECT count(*), round(sum(valor_del_contrato)/1e9, 2), min(strftime(fecha_de_firma, '%Y')), max(strftime(fecha_de_firma, '%Y')) FROM read_parquet('data/parquet/SECOP_II_2015_2025.parquet')").fetchall()[0]
print(f"SECOP II real: {r2[0]:,} contratos | ${r2[1]:,} mil millones | Años: {r2[2]} a {r2[3]}")

# 3. UNIFICADO
r3 = con.execute("SELECT count(*), round(sum(valor_contrato)/1e9, 2), min(anio_contratacion), max(anio_contratacion) FROM read_parquet('data/parquet/SECOP_UNIFICADO_2015_2025.parquet')").fetchall()[0]
print(f"UNIFICADO real: {r3[0]:,} contratos | ${r3[1]:,} mil millones | Años: {r3[2]} a {r3[3]}")

# 4. TIC
r4 = con.execute("SELECT count(*), round(sum(valor_contrato)/1e9, 2), min(anio_contratacion), max(anio_contratacion) FROM read_parquet('data/parquet/SECOP_INDUSTRIA_TIC_2015_2025.parquet')").fetchall()[0]
print(f"INDUSTRIA TIC real: {r4[0]:,} contratos | ${r4[1]:,} mil millones | Años: {r4[2]} a {r4[3]}")

print("\n--- DISTRIBUCIÓN ANUAL REAL DEL UNIFICADO ---")
df_anual = con.execute("SELECT anio_contratacion, count(*) as contratos, round(sum(valor_contrato)/1e9, 2) as monto_mil_millones FROM read_parquet('data/parquet/SECOP_UNIFICADO_2015_2025.parquet') GROUP BY 1 ORDER BY 1").df()
print(df_anual.to_string(index=False))

print("\n--- DISTRIBUCIÓN ANUAL REAL DE LA INDUSTRIA TIC ---")
df_tic = con.execute("SELECT anio_contratacion, count(*) as contratos, round(sum(valor_contrato)/1e9, 2) as monto_mil_millones FROM read_parquet('data/parquet/SECOP_INDUSTRIA_TIC_2015_2025.parquet') GROUP BY 1 ORDER BY 1").df()
print(df_tic.to_string(index=False))
