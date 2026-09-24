import requests
import json
import sys

# Test 1: Socrata full text search q=software OR tecnologia OR telecomunicaciones OR computo
url_i = "https://www.datos.gov.co/resource/f789-7hwg.json?$select=count(*)&$where=anno_cargue_secop>=2015 AND anno_cargue_secop<=2025&$q=software"
r_i = requests.get(url_i, timeout=20).json()
print("SECOP I q=software:", r_i, flush=True)

url_ii = "https://www.datos.gov.co/resource/jbjy-vk9h.json?$select=count(*)&$where=fecha_de_firma>='2015-01-01T00:00:00.000' AND fecha_de_firma<='2025-12-31T23:59:59.999'&$q=software"
r_ii = requests.get(url_ii, timeout=20).json()
print("SECOP II q=software:", r_ii, flush=True)
