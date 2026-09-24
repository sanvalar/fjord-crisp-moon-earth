import requests
import json
import time

print("=== CONSULTANDO TOTALES MACRO DEL ESTADO COLOMBIANO POR AÑO EN SECOP I Y II ===")
totales_por_anio = []

for anio in range(2016, 2026):
    t0 = time.time()
    
    # 1. SECOP I
    url_i = f"https://www.datos.gov.co/resource/f789-7hwg.json?$select=count(*),sum(cuantia_contrato)&$where=anno_cargue_secop={anio} AND cuantia_contrato>0"
    r_i = requests.get(url_i, timeout=20).json()
    c_i = int(r_i[0]['count']) if r_i and 'count' in r_i[0] else 0
    m_i = float(r_i[0]['sum_cuantia_contrato'] or 0) if r_i and 'sum_cuantia_contrato' in r_i[0] else 0
    
    # 2. SECOP II
    url_ii = f"https://www.datos.gov.co/resource/jbjy-vk9h.json?$select=count(*),sum(valor_del_contrato)&$where=fecha_de_firma>='{anio}-01-01T00:00:00.000' AND fecha_de_firma<='{anio}-12-31T23:59:59.999' AND valor_del_contrato>0"
    r_ii = requests.get(url_ii, timeout=20).json()
    c_ii = int(r_ii[0]['count']) if r_ii and 'count' in r_ii[0] else 0
    m_ii = float(r_ii[0]['sum_valor_del_contrato'] or 0) if r_ii and 'sum_valor_del_contrato' in r_ii[0] else 0
    
    total_cttos = c_i + c_ii
    total_monto_mm = round((m_i + m_ii) / 1e9, 2)
    
    totales_por_anio.append({
        "anio": anio,
        "cttos_secop_i": c_i,
        "cttos_secop_ii": c_ii,
        "total_contratos_estado": total_cttos,
        "total_monto_estado_mm": total_monto_mm
    })
    print(f"Año {anio}: {total_cttos:>9,} contratos totales Estado | ${total_monto_mm:>12,.2f} MM COP ({time.time()-t0:.2f}s)", flush=True)

with open("data/totales_macro_estado_anual.json", "w", encoding="utf-8") as f:
    json.dump(totales_por_anio, f, indent=2)
print("✔ Guardado en data/totales_macro_estado_anual.json")
