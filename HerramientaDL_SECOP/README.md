# Sistema de Inteligencia y Observatorio de Contratación Pública TIC (SECOP I & II)

Plataforma analítica para auditoría y modelado de la contratación pública TIC en Colombia (2015–2026), con **ingesta continua** desde la API pública Socrata/SODA de datos.gov.co y scrapers complementarios.

La **base histórica filtrada** son **213.123 contratos** de industria TIC. Cada actualización consulta la API: **si el contrato ya existe se actualiza (lambda UPDATE); si no existe se registra (lambda INSERT)**. El total de la interfaz crece con esos registros nuevos.

---

## Arranque rápido

Desde la carpeta del proyecto:

```bash
python3 -m pip install -r requirements.txt
python3 app_visual/servidor_visual.py
```

Abrir el navegador en `http://localhost:8080`.

En la cabecera pulse **«Realizar actualización de los contratos (contratación pública en TIC)»**. La barra superior y la pestaña **Ingesta continua SODA** muestran en vivo: leídos, nuevos, actualizados y el total (213.123 + delta).

---

## Ingesta continua (SODA + scrapers + lambda)

```bash
# Una corrida incremental TIC (recomendada)
python3 -m ingestion tic-update --lookback-days 180 --max-pages 30 --page-size 500

# Estado / bitácora
python3 -m ingestion status

# Ingesta SODA genérica (contratos y procesos)
python3 -m ingestion once --datasets contratos procesos --tic-only --max-pages 5

# Daemon cada 12 horas (también arranca con el servidor visual)
python3 -m ingestion schedule --hours 12 --tic-only
```

Fuentes:

| Fuente | Dataset Socrata | Qué hace |
|---|---|---|
| SECOP II contratos | `jbjy-vk9h` | Filtro TIC + lambda upsert al Parquet |
| SECOP I | `f789-7hwg` | Scraper/SODA TIC histórico |
| Procesos SECOP II | `p6dx-8zbt` | Scraper complementario |
| Catálogo datos.gov.co | `/api/catalog/v1` | Metadatos de fuentes |

Persistencia: `data/parquet/SECOP_INDUSTRIA_TIC_2015_2025.parquet` (universo de la UI) y `data/secop_live.db` (bitácora + upsert SQLite).

---

## Token opcional Socrata

Copie `.env.example` a `.env` y ponga `SOCRATA_APP_TOKEN` para subir la cuota. Sin token la API pública igual funciona, con más riesgo de 429.

---

## Estructura

```text
├── app_visual/servidor_visual.py   # UI + API REST (botón de actualización)
├── ingestion/
│   ├── tic_sync.py                 # Orquestador TIC + merge Parquet
│   ├── lambda_upsert.py            # lambda: existe → UPDATE; no → INSERT
│   ├── soda.py                     # Conector Socrata/SoQL incremental
│   ├── scrapers.py                 # SECOP I, procesos, catálogo
│   └── pipeline.py / scheduler.py
├── data/parquet/                   # Base 213.123 + lotes nuevos
└── requirements.txt
```
