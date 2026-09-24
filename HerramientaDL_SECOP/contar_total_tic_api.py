import requests
import json

url_i = 'https://www.datos.gov.co/api/v3/views/f789-7hwg/query.json'
q_i = "SELECT count(*) WHERE anno_cargue_secop >= 2015 AND anno_cargue_secop <= 2025 AND (lower(objeto_a_contratar) like '%software%' or lower(objeto_a_contratar) like '%tecnolog%' or lower(objeto_a_contratar) like '%comput%' or lower(objeto_a_contratar) like '%conectividad%' or lower(objeto_a_contratar) like '%telecomunicac%' or lower(objeto_a_contratar) like '%ciberseguridad%' or lower(objeto_a_contratar) like '%internet%')"
try:
    r_i = requests.get(url_i, params={'query': q_i}, timeout=30).json()
    print('Total TIC SECOP I en API (10 anos):', r_i)
except Exception as e:
    print('Error SECOP I:', e)

url_ii = 'https://www.datos.gov.co/api/v3/views/jbjy-vk9h/query.json'
q_ii = "SELECT count(*) WHERE fecha_de_firma >= '2015-01-01T00:00:00.000' AND fecha_de_firma <= '2025-12-31T23:59:59.999' AND (lower(descripcion_del_proceso) like '%software%' or lower(descripcion_del_proceso) like '%tecnolog%' or lower(descripcion_del_proceso) like '%comput%' or lower(descripcion_del_proceso) like '%conectividad%' or lower(descripcion_del_proceso) like '%telecomunicac%' or lower(descripcion_del_proceso) like '%ciberseguridad%' or lower(descripcion_del_proceso) like '%internet%')"
try:
    r_ii = requests.get(url_ii, params={'query': q_ii}, timeout=30).json()
    print('Total TIC SECOP II en API (10 anos):', r_ii)
except Exception as e:
    print('Error SECOP II:', e)
