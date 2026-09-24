"""
Módulo de comparación técnica exhaustiva entre las estructuras oficiales de
SECOP I (dataset x6v4-i8gf) y SECOP II (dataset jbjy-vk9h).
Genera el informe comparativo en Markdown y el mapeo de equivalencias en CSV.
"""
import json
import logging
from pathlib import Path
import pandas as pd
import requests
from secop_ii.config import METADATA_DIR

logger = logging.getLogger("secop_ii.comparison")

SECOP_I_METADATA_URL = "https://www.datos.gov.co/api/views/x6v4-i8gf.json"
SECOP_II_METADATA_URL = "https://www.datos.gov.co/api/views/jbjy-vk9h.json"

class SecopComparison:
    def __init__(self, output_dir: Path = METADATA_DIR):
        self.output_dir = output_dir

    def fetch_columns(self, url: str) -> list:
        try:
            resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            data = resp.json()
            cols = [
                {
                    "name": c.get("name"),
                    "fieldName": c.get("fieldName"),
                    "dataType": c.get("dataTypeName"),
                    "description": c.get("description", "")
                }
                for c in data.get("columns", [])
            ]
            return cols
        except Exception as e:
            logger.error(f"Error descargando metadata desde {url}: {e}")
            return []

    def generate_comparison_report(self):
        logger.info("Obteniendo columnas de SECOP I y SECOP II...")
        secop_i_cols = self.fetch_columns(SECOP_I_METADATA_URL)
        secop_ii_cols = self.fetch_columns(SECOP_II_METADATA_URL)

        dict_i = {c["fieldName"]: c for c in secop_i_cols}
        dict_ii = {c["fieldName"]: c for c in secop_ii_cols}

        # Mapeo de equivalencias semánticas reales identificadas
        mapping = [
            ("UID / id_contrato", "uid", "id_contrato", "Identificador del contrato"),
            ("Proceso de Compra", "numero_de_proceso", "proceso_de_compra", "Código del proceso contractual"),
            ("Nombre Entidad", "nombre_de_la_entidad", "nombre_entidad", "Razón social de la entidad pública"),
            ("NIT Entidad", "nit_de_la_entidad", "nit_entidad", "Identificación tributaria de la entidad"),
            ("Código Entidad", "c_digo_de_la_entidad", "codigo_entidad", "Código asignado a la entidad"),
            ("Departamento", "departamento_entidad", "departamento", "Ubicación geográfica del departamento"),
            ("Municipio / Ciudad", "municipio_entidad", "ciudad", "Municipio sede o de ejecución"),
            ("Orden Entidad", "orden_entidad", "orden", "Nivel territorial (Nacional, Territorial)"),
            ("Tipo Proceso / Modalidad", "tipo_de_proceso", "modalidad_de_contratacion", "Modalidad contractual (Licitación, Directa, etc.)"),
            ("Estado Proceso / Contrato", "estado_del_proceso", "estado_contrato", "Estado contractual (Celebrado, En ejecución, Terminado)"),
            ("Objeto Contrato", "detalle_del_objeto_a_contratar", "objeto_del_contrato", "Descripción detallada del objeto contractual"),
            ("Tipo de Contrato", "tipo_de_contrato", "tipo_de_contrato", "Naturaleza (Prestación de servicios, Obra, Suministro)"),
            ("Fecha Firma", "fecha_de_firma_del_contrato", "fecha_de_firma", "Fecha en que se perfeccionó el contrato"),
            ("Fecha Inicio", "fecha_de_inicio_de_ejecucion", "fecha_de_inicio_del_contrato", "Inicio formal de la ejecución"),
            ("Fecha Fin", "fecha_de_fin_de_ejecucion", "fecha_de_fin_del_contrato", "Fecha prevista o real de terminación"),
            ("Valor del Contrato", "cuantia_contrato", "valor_del_contrato", "Valor económico pactado"),
            ("Nombre Proveedor", "nom_raz_social_contratista", "proveedor_adjudicado", "Nombre o razón social del contratista"),
            ("Documento Proveedor", "identificacion_del_contratista", "documento_proveedor", "Cédula o NIT del contratista"),
            ("Tipo Documento Proveedor", "tipo_doc_representante_legal", "tipodocproveedor", "Tipo de documento legal"),
            ("Plazo / Duración", "plazo_de_ejec_del_contrato", "duraci_n_del_contrato", "Plazo contractual"),
            ("Clasificación UNSPSC", "id_sub_unidad_ejecutora", "codigo_de_categoria_principal", "Clasificación de bienes y servicios"),
            ("Origen Recursos", "origen_de_los_recursos", "origen_de_los_recursos", "Fuente presupuestal"),
            ("Compromiso Presupuestal / CDP", "numero_del_compromiso", "saldo_cdp", "Registro presupuestal de respaldo")
        ]

        df_mapping = pd.DataFrame(mapping, columns=["Concepto", "Campo_SECOP_I", "Campo_SECOP_II", "Descripcion"])
        df_mapping.to_csv(self.output_dir / "mapeo_equivalencias_secop_i_ii.csv", index=False)

        # Análisis de exclusividades
        mapped_i_fields = set(df_mapping["Campo_SECOP_I"].dropna())
        mapped_ii_fields = set(df_mapping["Campo_SECOP_II"].dropna())

        exclusive_i = [c for c in dict_i.keys() if c not in mapped_i_fields and not c.startswith(":")]
        exclusive_ii = [c for c in dict_ii.keys() if c not in mapped_ii_fields and not c.startswith(":")]

        # Construcción del informe en Markdown
        doc = f"""# Estudio Comparativo de Estructuras: SECOP I vs SECOP II
**Datasets Oficiales en Datos Abiertos Colombia:**
- **SECOP I**: Dataset `x6v4-i8gf` ({len(secop_i_cols)} columnas)
- **SECOP II**: Dataset `jbjy-vk9h` ({len(secop_ii_cols)} columnas)

---

## 1. Resumen de Hallazgos y Compatibilidad

1. **Paradigmas Distintos de Información**:
   - **SECOP I**: Es un sistema de cargue posterior y registro de documentos en PDF/formularios estáticos. Gran parte de la trazabilidad financiera transaccional no está estructurada en campos numéricos independientes.
   - **SECOP II**: Es una plataforma transaccional completa de comercio electrónico donde todo el ciclo de vida (oferta, adjudicación, facturación, pagos, liquidación y órdenes) se registra digitalmente en campos específicos.

2. **Diferencias Clave en Variables Económicas**:
   - SECOP I solo registra principalmente la `cuantia_contrato` (o `cuantia_proceso`).
   - SECOP II cuenta con desagregación granular: `valor_del_contrato`, `valor_facturado`, `valor_pagado`, `valor_pendiente_de_pago`, `valor_amortizado`, `saldo_cdp`, y fuentes discriminadas (SGP, SGR, PGN, Recursos Propios).

3. **Claves de Integración Primarias**:
   - **No existe una clave foránea universal directa**: Los contratos de SECOP I usan `UID`, mientras que SECOP II usa `id_contrato` con prefijo `CO1.PCCNTR.XXXXXX`.
   - **Claves compuestas recomendadas para integración analítica**:
     1. `(nit_entidad, documento_proveedor, anio_firma, valor_contrato)`
     2. `(nit_entidad, numero_de_proceso / proceso_de_compra)`

---

## 2. Matriz de Columnas Equivalentes

| Concepto de Negocio | Campo SECOP I (`x6v4-i8gf`) | Campo SECOP II (`jbjy-vk9h`) |
|---|---|---|
"""
        for _, row in df_mapping.iterrows():
            doc += f"| **{row['Concepto']}** | `{row['Campo_SECOP_I']}` | `{row['Campo_SECOP_II']}` |\n"

        doc += f"""
---

## 3. Columnas Exclusivas y Ventajas de SECOP II
SECOP II incorpora {len(exclusive_ii)} variables no disponibles directamente en la estructura de contratos de SECOP I, entre las que destacan:
- **Trazabilidad de pagos y ejecución**: `valor_facturado`, `valor_pagado`, `valor_pendiente_de_pago`, `valor_amortizado`.
- **Enfoque de inclusión y posconflicto**: `es_pyme`, `espostconflicto`, `puntos_del_acuerdo`, `pilares_del_acuerdo`.
- **Sostenibilidad y medio ambiente**: `obligaci_n_ambiental`, `obligaciones_postconsumo`.
- **Auditoría de plataforma**: `:id`, `:version`, `:created_at`, `:updated_at`, `urlproceso`.

---

## 4. Recomendaciones para el Merge Futuro
1. **Normalización de Formatos de Fecha**: SECOP I maneja fechas en formato texto/fecha con horas dispersas; SECOP II estandariza en formato ISO (`YYYY-MM-DDTHH:MM:SS.sss`).
2. **Normalización de NITs y Cédulas**: Homogeneizar quitando dígito de verificación y caracteres especiales.
3. **Conversión Monetaria**: Ambas manejan Pesos Colombianos (COP), pero en SECOP I existen registros históricos en salarios mínimos o con campos de texto auxiliares.
"""
        report_path = self.output_dir / "comparativa_secop_i_vs_ii.md"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(doc)
        logger.info(f"Informe comparativo guardado en {report_path}")
