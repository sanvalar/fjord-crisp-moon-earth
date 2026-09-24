# Estudio Comparativo de Estructuras: SECOP I vs SECOP II
**Datasets Oficiales en Datos Abiertos Colombia:**
- **SECOP I**: Dataset `x6v4-i8gf` (73 columnas)
- **SECOP II**: Dataset `jbjy-vk9h` (85 columnas)

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
| **UID / id_contrato** | `uid` | `id_contrato` |
| **Proceso de Compra** | `numero_de_proceso` | `proceso_de_compra` |
| **Nombre Entidad** | `nombre_de_la_entidad` | `nombre_entidad` |
| **NIT Entidad** | `nit_de_la_entidad` | `nit_entidad` |
| **Código Entidad** | `c_digo_de_la_entidad` | `codigo_entidad` |
| **Departamento** | `departamento_entidad` | `departamento` |
| **Municipio / Ciudad** | `municipio_entidad` | `ciudad` |
| **Orden Entidad** | `orden_entidad` | `orden` |
| **Tipo Proceso / Modalidad** | `tipo_de_proceso` | `modalidad_de_contratacion` |
| **Estado Proceso / Contrato** | `estado_del_proceso` | `estado_contrato` |
| **Objeto Contrato** | `detalle_del_objeto_a_contratar` | `objeto_del_contrato` |
| **Tipo de Contrato** | `tipo_de_contrato` | `tipo_de_contrato` |
| **Fecha Firma** | `fecha_de_firma_del_contrato` | `fecha_de_firma` |
| **Fecha Inicio** | `fecha_de_inicio_de_ejecucion` | `fecha_de_inicio_del_contrato` |
| **Fecha Fin** | `fecha_de_fin_de_ejecucion` | `fecha_de_fin_del_contrato` |
| **Valor del Contrato** | `cuantia_contrato` | `valor_del_contrato` |
| **Nombre Proveedor** | `nom_raz_social_contratista` | `proveedor_adjudicado` |
| **Documento Proveedor** | `identificacion_del_contratista` | `documento_proveedor` |
| **Tipo Documento Proveedor** | `tipo_doc_representante_legal` | `tipodocproveedor` |
| **Plazo / Duración** | `plazo_de_ejec_del_contrato` | `duraci_n_del_contrato` |
| **Clasificación UNSPSC** | `id_sub_unidad_ejecutora` | `codigo_de_categoria_principal` |
| **Origen Recursos** | `origen_de_los_recursos` | `origen_de_los_recursos` |
| **Compromiso Presupuestal / CDP** | `numero_del_compromiso` | `saldo_cdp` |

---

## 3. Columnas Exclusivas y Ventajas de SECOP II
SECOP II incorpora 62 variables no disponibles directamente en la estructura de contratos de SECOP I, entre las que destacan:
- **Trazabilidad de pagos y ejecución**: `valor_facturado`, `valor_pagado`, `valor_pendiente_de_pago`, `valor_amortizado`.
- **Enfoque de inclusión y posconflicto**: `es_pyme`, `espostconflicto`, `puntos_del_acuerdo`, `pilares_del_acuerdo`.
- **Sostenibilidad y medio ambiente**: `obligaci_n_ambiental`, `obligaciones_postconsumo`.
- **Auditoría de plataforma**: `:id`, `:version`, `:created_at`, `:updated_at`, `urlproceso`.

---

## 4. Recomendaciones para el Merge Futuro
1. **Normalización de Formatos de Fecha**: SECOP I maneja fechas en formato texto/fecha con horas dispersas; SECOP II estandariza en formato ISO (`YYYY-MM-DDTHH:MM:SS.sss`).
2. **Normalización de NITs y Cédulas**: Homogeneizar quitando dígito de verificación y caracteres especiales.
3. **Conversión Monetaria**: Ambas manejan Pesos Colombianos (COP), pero en SECOP I existen registros históricos en salarios mínimos o con campos de texto auxiliares.
