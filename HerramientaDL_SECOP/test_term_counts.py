import requests
import json
import time

terms = ["software", "tecnologia", "computo", "conectividad", "telecomunicaciones", "ciberseguridad", "licenciamiento"]

print("=== CONTEOS INDEXADOS SOCRATA EN SECOP I Y SECOP II (2015 - 2025) ===")
for t in terms:
    t0 = time.time()
    url_i = f"https://www.datos.gov.co/resource/f789-7hwg.json?$select=count(*)&$where=anno_cargue_secop>=2015 AND anno_cargue_secop<=2025&$q={t}"
    r_i = requests.get(url_i, timeout=20).json()
    c_i = r_i[0]['count'] if isinstance(r_i, list) and len(r_i) > 0 else '0'
    
    url_ii = f"https://www.datos.gov.co/resource/jbjy-vk9h.json?$select=count(*)&$where=fecha_de_firma>='2015-01-01T00:00:00.000' AND fecha_de_firma<='2025-12-31T23:59:59.999'&$q={t}"
    r_ii = requests.get(url_ii, timeout=20).json()
    c_ii = r_ii[0]['count'] if isinstance(r_ii, list) and len(r_ii) > 0 else '0'
    
    print(f"Termino: '{t:18}' -> SECOP I: {int(c_i):>7,} | SECOP II: {int(c_ii):>7,} ({time.time()-t0:.2f}s)", flush=True)
