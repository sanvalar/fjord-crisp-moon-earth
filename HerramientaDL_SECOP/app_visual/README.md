# Plataforma Analítica Visual de Contratación Pública (SECOP I & II)

Esta carpeta contiene la aplicación web local completa para consultar, visualizar y analizar interactivamente los archivos Parquet de contratación pública en Colombia sin requerir software adicional ni saturar la memoria RAM.

---

## Cómo Iniciar la Plataforma

Para iniciar el servidor visual interactivo, ejecuta desde la raíz del proyecto:

```powershell
python app_visual/servidor_visual.py
```

Luego abre tu navegador en:
👉 **[http://localhost:8050](http://localhost:8050)**

---

## Funcionalidades Incluidas en la Interfaz

1. **Selector Inteligente de Dataset**:
   - `SECOP_UNIFICADO_2015_2025.parquet` (Consolidado histórico de 24 columnas).
   - `SECOP_I_2015_2025.parquet` (Base documental previa con 79 columnas).
   - `SECOP_II_2015_2025.parquet` (Plataforma transaccional con 77 columnas).

2. **Pestaña 1: Dashboard Ejecutivo y Gráficos (Chart.js)**:
   - 4 Tarjetas de KPIs en tiempo real: Total de contratos, monto total en billones, valor promedio y mediana.
   - 4 Gráficos interactivos dinámicos:
     - Barras de evolución anual del gasto.
     - Barras horizontales del Top 8 Departamentos.
     - Gráfico de dona con distribución por Modalidad de contratación.
     - Top Proveedores más beneficiados por adjudicación.

3. **Pestaña 2: Explorador de Registros**:
   - Tabla completa con todas las columnas físicas del dataset seleccionado.
   - Detección de valores nulos y formato numérico formateado con comas de miles.

4. **Pestaña 3: Consola SQL DuckDB Adaptativa**:
   - Detecta automáticamente los nombres de columnas de SECOP I, SECOP II o Unificado para que los botones rápidos nunca fallen.
   - Editor de código SQL libre con ejecución en milisegundos.

5. **Pestaña 4: Diccionario de Columnas**:
   - Inventario visual de todas las columnas descubiertas en el archivo Parquet y sus tipos de datos exactos (`VARCHAR`, `DOUBLE`, `TIMESTAMP`, etc.).
