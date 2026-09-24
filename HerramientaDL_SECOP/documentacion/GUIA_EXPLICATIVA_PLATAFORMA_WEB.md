# GUÍA DE REPASO Y EXPLICACIÓN DE LA PLATAFORMA WEB SECOP TIC ANALYTICS
**URL Local:** http://localhost:8050  
**Objetivo:** Esta guía explica de manera sencilla, clara y directa qué hace cada botón, gráfico, tabla y número en cada una de las pestañas del sistema, para que puedas repasar y sustentar el proyecto sin enredos.

---

## 1. BARRA SUPERIOR (HEADER)

En la parte de arriba siempre verás:
* **Título:** *Sistema de Análisis SECOP I y II - Observatorio de Contratación Pública en Tecnologías de la Información*.
* **Indicador verde "DuckDB In-Memory":** Significa que los datos no se consultan desde un archivo pesado en disco ni desde una base de datos lenta en internet, sino que están cargados directamente en la memoria RAM del procesador con el motor analítico DuckDB. Por eso cada clic y filtro responde en milisegundos.
* **Selector "Dataset Activo":** Permite alternar la vista entre el archivo acotado de la industria TIC (`SECOP_INDUSTRIA_TIC_2015_2025.parquet`) y el consolidado unificado general (`SECOP_UNIFICADO_2015_2025.parquet`). Por defecto está seleccionado el de TIC.

---

## 2. PESTAÑA 1: "DASHBOARD GENERAL"

Es el panel de control principal enfocado en la **Industria TIC, Software y Telecomunicaciones** (CIIU 620 y 261).

### A. Barra de Filtros (Parte Superior)
* **Año Inicial y Año Final:** Te permite acotar el rango de tiempo (por ejemplo, ver solo 2020 a 2024).
* **Departamento:** Filtra los contratos para una región específica (ej. Bogotá, Antioquia, Valle).
* **Subsector TIC:** Filtra por una de las 5 categorías técnicas de la industria TIC.
* **Botón "Filtrar":** Aplica los filtros sobre los gráficos y KPIs en tiempo real.
* **Botón "Restablecer":** Vuelve a mostrar todos los años, departamentos y subsectores.

### B. Tarjetas de Indicadores Clave (KPIs)
* **Total Contratos (551):** El número total de contratos reales del sector TIC analizados en el periodo 2017–2025.
* **Valor Contratado ($75.84 MM):** La suma total del dinero público adjudicado en el sector TIC (75.844 millones de pesos colombianos).
* **Promedio por Contrato ($137.6 M):** Lo que cuesta un contrato de TIC en promedio (media aritmética).
* **Mediana ($42.0 M - P50):** El valor del medio. El 50% de los contratos cuesta menos de 42 millones de pesos. ¿Por qué es mucho menor al promedio? Porque hay un puñado de megacontratos de miles de millones que suben el promedio, pero la mayoría son contratos pequeños de soporte.
* **Subsector Dominante:** Indica qué área tecnológica absorbe más dinero (*Servicios de Soporte y Consultoría TI*).
* **Principal Entidad / Dpto:** Región con mayor concentración de gasto (*Bogotá D.C.*).

### C. Gráficos y Tablas
1. **Evolución Presupuestal Anual (Barras Azules):** Muestra cuánto dinero en miles de millones (MM COP) se gastó en tecnología cada año desde 2017 hasta 2025. Muestra picos como el de 2022 por conectividad post-pandemia.
2. **Distribución por Subsector TIC (Rosquilla Multicolor):** Muestra el porcentaje de la torta presupuestal que se lleva cada una de las 5 categorías:
   * *Soporte y Consultoría TI:* 72.6% (la gran mayoría).
   * *Desarrollo de Software y Licenciamiento:* 19.4%.
   * *Conectividad y Redes:* 7.2%.
   * *Ciberseguridad y Cloud:* 0.5%.
   * *Hardware y Equipos:* 0.3%.
3. **Principales Adjudicatarios (Barras Horizontales):** Muestra quiénes son los contratistas privados que más dinero ganan en TIC (ej. Oracle Colombia, consorcios de conectividad).
4. **Modalidades de Selección (Rosquilla):** Cuántos contratos se hicieron por *Licitación Pública*, cuántos por *Contratación Directa*, cuántos por *Mínima Cuantía*, etc.
5. **Índice HHI de Concentración (Barras de Nivel):**
   * Mide si un sector es un monopolio/oligopolio o si hay competencia abierta.
   * Si la barra está en **Verde** (HHI < 1.500), hay muchos proveedores compitiendo (*Soporte TI*).
   * Si la barra está en **Rojo** (HHI > 2.500 o > 5.000), el mercado está acaparado por 1 o 2 empresas (*Conectividad* y *Ciberseguridad*).
6. **Tabla Detalle Presupuestal por Subsector:** Lista exacta de los 5 subsectores con cantidad de contratos, monto en millones y su porcentaje exacto.

---

## 3. PESTAÑA 2: "DISTRIBUCIÓN GEOGRÁFICA"

Permite entender **dónde se gasta la plata de la tecnología en Colombia**.

* **KPIs Superiores:**
  * *Departamentos Activos:* Cuántos departamentos tienen al menos 1 contrato de TIC.
  * *Jurisdicción Líder:* Bogotá D.C.
  * *Monto Líder:* Más de $32.300 millones COP en Bogotá.
  * *Concentración Bogotá D.C. (42.6%):* Demuestra el **centralismo** del país; casi la mitad de todo el dinero de tecnología se queda en la capital.
* **Gráfico de Ranking Departamental:** Muestra las 15 regiones que más reciben presupuesto de TIC en orden descendente (Bogotá, Antioquia, Magdalena, Tolima, etc.).
* **Matriz de Distribución Territorial (Tabla):** Lista completa de los departamentos con su cantidad de contratos, valor en millones y su participación porcentual nacional.

---

## 4. PESTAÑA 3: "TOTAL VS. TIC"

Compara el **presupuesto general de todo el Estado colombiano** frente a lo poco o mucho que se le dedica a la **industria tecnológica**.

* **Gráfico "Total Contratación Pública vs. Sector TIC" (Doble Escala):**
  * *Barras grises (Eje izquierdo):* Monto total de compras del Estado ($992.489 millones COP en 6.795 contratos).
  * *Línea azul (Eje derecho):* Monto exclusivo del sector TIC ($75.844 millones COP en 551 contratos).
* **Gráfico "Participación Relativa de la Industria TIC" (Línea Verde):**
  * Muestra año por año qué porcentaje del presupuesto nacional fue a tecnología (promedio histórico del 7.6%).
* **Tabla Consolidado Multianual:**
  * Detalle año por año (2016 a 2025) con contratos totales, contratos TIC, monto total, monto TIC y el porcentaje de participación resultante.

---

## 5. PESTAÑA 4: "MODELOS ML & CONPES 4069"

Aquí está implementada la parte de **Machine Learning (Inteligencia Artificial)** y la evaluación de la política pública del gobierno (**CONPES 4069 de Ciencia, Tecnología e Innovación**).

### A. Indicadores CONPES 4069
* **Alineación CONPES 4069 (16.3%):** De los 551 contratos de TIC, solo el 16.3% tiene relación real con ciencia, investigación, desarrollo de software nuevo o inteligencia artificial.
* **Monto I+D / Innovación ($17.15 MM COP):** De los $75.84 MM gastados en TIC, solo $17.15 MM fueron a innovación real. El resto se fue en soporte técnico rutinario.

### B. Clustering K-Means ($k=4$)
El algoritmo no supervisado agrupó los 551 contratos en **4 perfiles de compra**:
1. *Clúster 0 (Operación & Soporte Estándar):* 509 contratos pequeños/medianos (promedio $113 M) con 0 días de adición. Es la burocracia técnica diaria de las entidades.
2. *Clúster 1 (Prórrogas Moderadas):* 25 contratos pequeños (promedio $33 M) que se demoraron más de lo previsto (+42 días de adición).
3. *Clúster 2 (Procesos Críticos con Alta Adición):* 4 contratos grandes (promedio $858 M) que tuvieron retrasos y prórrogas masivas (promedio de **179.5 días adicionados**).
4. *Clúster 3 (Megacontratos Tecnológicos Centralizados):* 13 contrataciones gigantes (promedio de **$1.072 millones COP cada una**) que concentran $13.943 millones de pesos.

### C. Detección de Anomalías (Isolation Forest)
* El modelo aísla de forma matemática los contratos que se salen por completo del comportamiento normal del Estado (puntuación de anomalía $s(x) > 0.70$).
* **Tabla de Contrataciones Atípicas:** Muestra el ID del contrato, valor, entidad y motivo de alerta:
  * Contratos con adiciones de más de 120 o 200 días.
  * Contratos multimillonarios adjudicados por *Contratación Directa* (a dedo, sin licitación competitiva), como el caso de Oracle Colombia con la Fiscalía por más de $6.098 millones de pesos.

### D. Matriz de Correlación y Proyecciones de Tendencia
* **Matriz de Correlación de Pearson:** Cruza matemáticamente el logaritmo del valor, los días de prórroga y la bandera de innovación. Muestra que los contratos con prórrogas tienen correlación alta con retrasos de ejecución ($r = 0.684$).
* **Gráfico de Proyección (Línea punteada verde):** Modelo supervisado que pronostica el crecimiento del gasto en tecnología para los años 2026 ($18.25 MM) y 2027 ($21.40 MM).

---

## 6. PESTAÑA 5: "EXPLORADOR DE CONTRATOS"

Es el buscador para navegar directamente en los datos sin necesidad de saber programar:
* **Caja de Búsqueda:** Escribe cualquier palabra (ej. "Oracle", "Antioquia", "Software", "Fiscalia") y la tabla filtra instantáneamente entre los cientos de registros.
* **Columnas Visibles:** ID del contrato, subsector TIC clasificado, entidad compradora, departamento, modalidad y valor formateado en pesos colombianos.

---

## 7. PESTAÑA 6: "CONSOLA ANALÍTICA SQL"

Permite a un evaluador, profesor o científico de datos escribir consultas de base de datos directamente en el navegador:
* **Botones de la izquierda:** Son plantillas rápidas ya escritas (ej. *Evolución anual*, *Top 10 proveedores*, *Top 10 departamentos*). Al hacer clic en uno, se copia la consulta en el editor.
* **Editor de texto SQL:** Puedes escribir cualquier sentencia SQL estándar (ej. `SELECT subsector_tic, count(*) FROM datos GROUP BY 1;`).
* **Botón "Ejecutar Consulta":** Corre la consulta en el motor columnar DuckDB y muestra abajo los resultados en una tabla, indicando cuántas filas devolvió y el tiempo en milisegundos (ej. *0.8 ms*).

---

## 8. PESTAÑA 7: "VERIFICACIÓN DE CIFRAS (AUDITORÍA)"

Esta pestaña responde al requerimiento de demostrar que **no hay datos inventados ni simulados**.

1. **Tabla de Integridad de Archivos Parquet:**
   * Muestra las cifras exactas extraídas de las APIs de `datos.gov.co` (2.000 contratos en SECOP I, 500 en SECOP II, 6.795 unificados y 551 de TIC).
2. **Tabla de Herramientas Técnicas vs. Conceptos del Reto:**
   * Cumple con la tabla obligatoria del taller, emparejando la tecnología utilizada con el concepto teórico de la ciencia de datos (ej. Python/DuckDB con modelado OLAP columnar; Scikit-Learn con clustering no supervisado).
3. **Clasificación Sistémica de Campos de la API (12 Categorías):**
   * Lista y explica las 12 familias en las que se organizaron los más de 80 campos de las APIs oficiales de contratación (Identificación, Entidad, Geografía, Tiempos, Modalidad, Proveedor, Economía, Ejecución, Presupuesto, UNSPSC, Textual y Sensibles).

---

## RESUMEN RÁPIDO PARA DEFENDER EL PROYECTO EN 3 FRASES:
1. *"Extrajimos más de 6.700 contratos reales de SECOP I y II vía API oficial en una arquitectura columnar optimizada con DuckDB y Parquet ZSTD que no colapsa la memoria RAM."*
2. *"Acotamos el estudio a la Industria TIC (551 contratos, $75.844 MM COP) y descubrimos que el 72.6% del presupuesto se gasta en soporte técnico y consultoría básica, y solo el 16.3% se destina a desarrollo e innovación alineada al CONPES 4069."*
3. *"Aplicamos algoritmos de Machine Learning reales (K-Means para segmentar perfiles de compra e Isolation Forest para detectar contratos atípicos multimillonarios en contratación directa) con una plataforma web interactiva y fluida."*
