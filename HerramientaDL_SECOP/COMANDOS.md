# Comandos para ejecutar el programa

Trabaje siempre desde la carpeta del proyecto (`HerramientaDL_SECOP`).

## 1. Dependencias (una vez)

```bash
python3 -m pip install -r requirements.txt
```

Opcional: copie `.env.example` a `.env` y agregue `SOCRATA_APP_TOKEN` (la API pública funciona sin token).

## 2. Interfaz (lo que se ve en el navegador)

```bash
python3 app_visual/servidor_visual.py
```

Abra `http://localhost:8080`.

En la cabecera pulse el botón **Realizar actualización de los contratos (contratación pública en TIC)**.
Los KPI y el explorador se recargan solos al terminar (base 213.123 + contratos nuevos).

Puerto distinto:

```bash
python3 app_visual/servidor_visual.py 8050
```

## 3. Ingesta continua por terminal

```bash
# Actualización TIC (SODA + scrapers + lambda upsert sobre el Parquet)
python3 -m ingestion tic-update --lookback-days 180 --max-pages 30 --page-size 500

# Ver totales y bitácora
python3 -m ingestion status

# Una pasada SODA (contratos / procesos), solo TIC
python3 -m ingestion once --datasets contratos procesos --tic-only --max-pages 10

# Daemon cada 12 horas (el servidor visual también lo arranca en segundo plano)
python3 -m ingestion schedule --hours 12
```

## 4. Pruebas

```bash
python3 -m ingestion.tests
```

## 5. Qué hace la lambda

`ingestion/lambda_upsert.py`:

```python
lambda_upsert = lambda pk, existentes: "update" if pk in existentes else "insert"
```

- Si `id_contrato_global` ya está en los 213.123 (o en lotes previos) → se **consulta y actualiza**.
- Si no existe → se **registra**.
