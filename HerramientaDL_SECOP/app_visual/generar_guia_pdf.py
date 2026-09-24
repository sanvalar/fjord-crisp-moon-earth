import sys
from pathlib import Path
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
)
from reportlab.pdfgen import canvas

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR = Path(__file__).resolve().parent.parent
DOC_DIR = BASE_DIR / "documentacion"
DOC_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PDF = DOC_DIR / "GUIA_EXPLICATIVA_PLATAFORMA_WEB.pdf"

class NumberedCanvas(canvas.Canvas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_decorations(num_pages)
            super().showPage()
        super().save()

    def draw_page_decorations(self, page_count):
        self.saveState()
        self.setFont("Helvetica-Bold", 8)
        self.setFillColor(colors.HexColor("#0D3B66"))
        
        if self._pageNumber > 1:
            self.drawString(54, 752, "GUÍA EXPLICATIVA DE LA PLATAFORMA WEB SECOP TIC ANALYTICS")
            self.setFont("Helvetica", 8)
            self.setFillColor(colors.HexColor("#64748B"))
            self.drawRightString(558, 752, "Manual de Repaso y Sustentación")
            self.setStrokeColor(colors.HexColor("#CBD5E1"))
            self.setLineWidth(0.5)
            self.line(54, 744, 558, 744)

        self.setFont("Helvetica", 8)
        self.setFillColor(colors.HexColor("#64748B"))
        page_text = f"Página {self._pageNumber} de {page_count}"
        self.drawRightString(558, 34, page_text)
        self.drawString(54, 34, "Plataforma Local: http://localhost:8050 — SECOP Analytics Engine v3.0")
        self.setStrokeColor(colors.HexColor("#CBD5E1"))
        self.setLineWidth(0.5)
        self.line(54, 44, 558, 44)
        self.restoreState()

def build_pdf():
    doc = SimpleDocTemplate(
        str(OUTPUT_PDF),
        pagesize=letter,
        leftMargin=54,
        rightMargin=54,
        topMargin=50,
        bottomMargin=50
    )

    styles = getSampleStyleSheet()
    
    c_main = colors.HexColor("#0D3B66")
    c_blue = colors.HexColor("#0284C7")
    c_orange = colors.HexColor("#E65100")
    c_text = colors.HexColor("#1E293B")
    c_bg = colors.HexColor("#F8FAFC")
    c_card = colors.HexColor("#F1F5F9")

    title_style = ParagraphStyle(
        'MainTitle',
        parent=styles['Heading1'],
        fontName='Helvetica-Bold',
        fontSize=15,
        leading=19,
        textColor=c_main,
        spaceAfter=3
    )

    subtitle_style = ParagraphStyle(
        'SubTitle',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=8.5,
        leading=12,
        textColor=colors.HexColor("#475569"),
        spaceAfter=8
    )

    h1_style = ParagraphStyle(
        'H1_Point',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=10.5,
        leading=13.5,
        textColor=c_main,
        spaceBefore=9,
        spaceAfter=4,
        keepWithNext=True
    )

    h2_style = ParagraphStyle(
        'H2_Point',
        parent=styles['Heading3'],
        fontName='Helvetica-Bold',
        fontSize=8.5,
        leading=11.5,
        textColor=c_blue,
        spaceBefore=5,
        spaceAfter=2,
        keepWithNext=True
    )

    body_style = ParagraphStyle(
        'Body',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=8,
        leading=11,
        textColor=c_text,
        spaceAfter=4
    )

    bullet_style = ParagraphStyle(
        'Bullet',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=7.8,
        leading=10.8,
        textColor=c_text,
        leftIndent=10,
        spaceAfter=2
    )

    table_header = ParagraphStyle(
        'TH',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=7.5,
        leading=9.5,
        textColor=colors.white,
        alignment=1
    )

    table_body = ParagraphStyle(
        'TB',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=7.2,
        leading=9.2,
        textColor=c_text
    )

    table_body_center = ParagraphStyle(
        'TBC',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=7.2,
        leading=9.2,
        alignment=1,
        textColor=c_text
    )

    story = []

    # Encabezado
    story.append(Paragraph("GUÍA EXPLICATIVA DE LA PLATAFORMA WEB SECOP TIC ANALYTICS", title_style))
    story.append(Paragraph(
        "<b>Manual Práctico de Repaso: Qué Significa Cada Pestaña, Gráfico, Indicador y Tabla del Sistema en http://localhost:8050</b>",
        subtitle_style
    ))
    story.append(HRFlowable(width="100%", thickness=1.5, color=c_orange, spaceBefore=0, spaceAfter=6))

    # Resumen Inicial
    resumen_box = [
        [Paragraph("<b>¿Cómo iniciar el sistema?</b>", table_body), Paragraph("Ejecutar en la terminal: <code>python app_visual/servidor_visual.py</code> y abrir <b>http://localhost:8050</b> en el navegador.", table_body)],
        [Paragraph("<b>Propósito del Proyecto:</b>", table_body), Paragraph("Auditar y predecir con Machine Learning la contratación pública en la <b>Industria TIC (CIIU 620/261)</b> sobre 10 años de datos reales de SECOP I y II, evaluando el impacto del CONPES 4069.", table_body)]
    ]
    t_resumen = Table(resumen_box, colWidths=[130, 374])
    t_resumen.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), c_card),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor("#CBD5E1")),
        ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor("#E2E8F0")),
        ('TOPPADDING', (0,0), (-1,-1), 2.5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 2.5),
    ]))
    story.append(t_resumen)
    story.append(Spacer(1, 6))

    # 1. BARRA SUPERIOR
    story.append(Paragraph("1. Barra Superior (Header de Control)", h1_style))
    story.append(Paragraph("• <b>Indicador 'DuckDB In-Memory':</b> Significa que la base de datos no está en un servidor lento ni leyendo discos constantemente. Todo se cargó en la memoria RAM con el motor vectorial columnar DuckDB. Por eso las consultas y filtros responden en menos de 10 milisegundos.", bullet_style))
    story.append(Paragraph("• <b>Selector 'Dataset Activo':</b> Permite conmutar la vista entre el dataset de la <b>Industria TIC</b> (551 contratos acotados) y el <b>Dataset Unificado</b> (6.795 contratos generales).", bullet_style))

    # 2. PESTAÑA 1: DASHBOARD GENERAL
    story.append(Paragraph("2. Pestaña: Dashboard General", h1_style))
    story.append(Paragraph("Es el centro de mando del sector tecnológico. Muestra la dinámica del mercado con filtros interactivos.", body_style))
    story.append(Paragraph("<b>A. Barra de Filtros Superiores:</b>", h2_style))
    story.append(Paragraph("• <b>Año Inicial / Final:</b> Filtra el periodo (2017 a 2025) recalculando al instante todas las gráficas y KPIs.", bullet_style))
    story.append(Paragraph("• <b>Departamento:</b> Aísla la contratación de una región en específico (ej. Bogotá, Antioquia, Valle).", bullet_style))
    story.append(Paragraph("• <b>Subsector TIC:</b> Permite filtrar por una de las 5 categorías tecnológicas.", bullet_style))
    story.append(Paragraph("• <b>Botones 'Filtrar' y 'Restablecer':</b> Aplican o limpian los filtros en un clic.", bullet_style))

    story.append(Paragraph("<b>B. Tarjetas de Indicadores Clave (KPIs):</b>", h2_style))
    story.append(Paragraph("• <b>Total Contratos (551):</b> Registros reales del sector TIC extraídos directamente desde las APIs oficiales de datos.gov.co (SECOP I y II) y procesados en Apache Parquet ZSTD.", bullet_style))
    story.append(Paragraph("• <b>Valor Contratado ($75.84 MM COP):</b> 75.844 millones de pesos colombianos adjudicados en tecnología.", bullet_style))
    story.append(Paragraph("• <b>Promedio ($137.6 M COP):</b> Media aritmética del valor de un contrato tecnológico.", bullet_style))
    story.append(Paragraph("• <b>Mediana ($42.0 M COP - P50):</b> El 50% de los contratos cuesta $42 millones o menos. Es mucho menor al promedio porque existen pocos megacontratos multimillonarios que inflan la media.", bullet_style))
    story.append(Paragraph("• <b>Subsector y Dpto. Dominante:</b> Identifican que <i>Soporte y Consultoría TI</i> y <i>Bogotá D.C.</i> concentran la mayor asignación.", bullet_style))

    story.append(Paragraph("<b>C. Gráficos y Tablas del Dashboard:</b>", h2_style))
    story.append(Paragraph("• <b>Evolución Presupuestal Anual (Barras):</b> Gasto año por año con pico sobresaliente en 2022 ($16.908 MM) impulsado por conectividad.", bullet_style))
    story.append(Paragraph("• <b>Subsectores TIC (Rosquilla):</b> 72.6% Soporte y Consultoría ($55.080 M), 19.4% Software y Licencias ($14.748 M), 7.2% Conectividad ($5.452 M), 0.5% Cloud/Ciberseguridad ($374 M) y 0.3% Hardware ($190 M). Demuestra que el Estado gasta en asistencia básica y poco en software nuevo.", bullet_style))
    story.append(Paragraph("• <b>Top Proveedores TIC:</b> Contratistas con más dinero acumulado (Oracle Colombia, Blessing Industries, Comsistelco, Aldesarrollo).", bullet_style))
    story.append(Paragraph("• <b>Índice HHI de Concentración:</b> Semáforo matemático antimonopolio. En verde está <i>Soporte TI</i> (HHI 440, muy competido); en rojo están <i>Conectividad</i> (HHI 6.297) y <i>Ciberseguridad</i> (HHI 5.388), indicando oligopolios cerrados.", bullet_style))

    # 3. PESTAÑA 2: DISTRIBUCIÓN GEOGRÁFICA
    story.append(Paragraph("3. Pestaña: Distribución Geográfica", h1_style))
    story.append(Paragraph("Evidencia el <b>centralismo administrativo</b> del país en materia tecnológica:", body_style))
    story.append(Paragraph("• <b>Concentración Bogotá D.C. (42.6%):</b> Casi la mitad de todo el presupuesto tecnológico del país ($32.300 MM COP) se queda en la capital, dejando a las gobernaciones con contratos marginales de conectividad escolar.", bullet_style))
    story.append(Paragraph("• <b>Ranking y Matriz Territorial:</b> Lista interactiva ordenada de los departamentos con sus contratos, montos y porcentajes (Antioquia 9.2%, Magdalena 7.9%, Tolima 6.8%).", bullet_style))

    # 4. PESTAÑA 3: TOTAL VS. TIC
    story.append(Paragraph("4. Pestaña: Total Contratación Pública vs. Sector TIC", h1_style))
    story.append(Paragraph("Compara la escala del presupuesto estatal general frente al nicho tecnológico:", body_style))
    story.append(Paragraph("• <b>Gráfico Doble Escala:</b> Eje izquierdo muestra los $992.489 MM COP totales del Estado (6.795 contratos); eje derecho muestra los $75.844 MM COP de TIC (551 contratos).", bullet_style))
    story.append(Paragraph("• <b>Porcentaje de Participación (Línea Verde):</b> La tecnología representa históricamente un promedio de solo el <b>7.6%</b> del gasto público nacional, confirmando que las compras estatales siguen dominadas por obras civiles tradicionales y burocracia asistencial.", bullet_style))

    # 5. PESTAÑA 4: MODELOS ML & CONPES 4069
    story.append(Paragraph("5. Pestaña: Modelos de Machine Learning & CONPES 4069", h1_style))
    story.append(Paragraph("Aplica algoritmos de Inteligencia Artificial reales ejecutados por Scikit-Learn sobre las 551 contrataciones:", body_style))
    story.append(Paragraph("• <b>Alineación CONPES 4069 (16.3% / $17.15 MM COP):</b> Solo el 16.3% de los contratos de tecnología incluye componentes auténticos de ciencia, investigación, desarrollo de software nuevo o analítica/IA. El 83.7% restante es soporte de rutina.", bullet_style))
    story.append(Paragraph("• <b>Clustering K-Means (k=4):</b> Agrupa las compras en 4 perfiles: <i>Clúster 0</i> (Operación estándar, 509 contratos de $113 M promedio sin prórrogas), <i>Clúster 1</i> (Contratos con prórrogas moderadas de 42 días), <i>Clúster 2</i> (Contratos críticos con prórrogas masivas de 179.5 días de retraso), y <i>Clúster 3</i> (Megacontratos tecnológicos de más de $1.072 M cada uno).", bullet_style))
    story.append(Paragraph("• <b>Detección de Anomalías (Isolation Forest):</b> Identifica con score s(x) > 0.70 contratos atípicos por cuantías exorbitantes en Contratación Directa (ej. Oracle Colombia con Fiscalía por $6.098 M) o con prórrogas desmedidas de más de 200 días.", bullet_style))
    story.append(Paragraph("• <b>Matriz de Correlación de Pearson:</b> Cruza matemáticamente el logaritmo de valor, días de adición y banderas de innovación (demuestra correlación r=0.684 entre prórrogas y atrasos).", bullet_style))
    story.append(Paragraph("• <b>Proyección Supervisada (XGBoost R²=0.884):</b> Pronostica el crecimiento del sector hacia $18.250 MM en 2026 y $21.400 MM en 2027.", bullet_style))

    # 6. PESTAÑAS 5, 6 Y 7: EXPLORADOR, SQL Y AUDITORÍA
    story.append(Paragraph("6. Pestañas de Explorador, Consola SQL y Auditoría", h1_style))
    story.append(Paragraph("• <b>Explorador de Contratos:</b> Motor de búsqueda en tiempo real donde puedes tipear 'Oracle', 'Software', 'Antioquia' o cualquier entidad y ver las filas al instante.", bullet_style))
    story.append(Paragraph("• <b>Consola Analítica SQL:</b> Permite ejecutar sentencias SQL nativas sobre DuckDB en menos de 1 milisegundo mediante plantillas prediseñadas.", bullet_style))
    story.append(Paragraph("• <b>Verificación de Cifras (Auditoría):</b> Tabla formal con las 12 categorías que clasifican los 85 campos de las APIs de SECOP y la matriz de Herramientas vs. Conceptos del reto.", bullet_style))

    # Conclusión
    story.append(Spacer(1, 4))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#CBD5E1"), spaceBefore=3, spaceAfter=5))
    story.append(Paragraph("<b>3 Frases Clave para Defender el Proyecto con Éxito:</b>", h2_style))
    story.append(Paragraph("1. <i>'Extraemos los datos directamente desde las APIs de datos.gov.co mediante streaming con PyArrow y DuckDB en Parquet ZSTD, logrando una arquitectura de alto rendimiento con menos de 180 MB de RAM.'</i>", bullet_style))
    story.append(Paragraph("2. <i>'Demostramos que de los $75.844 millones gastados en TIC, el 72.6% se va en soporte básico y solo el 16.3% cumple las metas de innovación del CONPES 4069.'</i>", bullet_style))
    story.append(Paragraph("3. <i>'Aplicamos Machine Learning no supervisado (K-Means e Isolation Forest) para tipificar perfiles de compra y aislar contrataciones directas multimillonarias atípicas.'</i>", bullet_style))

    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"✔ PDF generado exitosamente en: {OUTPUT_PDF}")

if __name__ == "__main__":
    build_pdf()
