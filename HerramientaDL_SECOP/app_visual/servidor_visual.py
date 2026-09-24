"""
Plataforma Visual TIC Analytics — SECOP I & II Colombia
Optimizada para alto rendimiento:
  - Cache en memoria (DuckDB)
  - Sin consultas de red externas lentas
  - Diseño enterprise minimalista y limpio (Estrictamente SIN emojis)
  - Mapa de calor geográfico interactivo de Colombia (Leaflet.js + Gradiente Térmico)
  - Flujo analítico: Macro Nacional -> Filtrado & Drill-Down (Foco Antioquia y los 32 Deptos)
  - Estudio Analítico Multidimensional (OLAP Pivot con dimensiones, métricas y filtros a medida)
  - Sistema de Comparación Multidimensional (Departamentos, Años, Tipos, Entidades, Proveedores, Subsectores)
  - Generador de Reportes Ejecutivos Oficiales (PDF vía ReportLab + Impresión de Alta Fidelidad)
"""
import sys
import io
import json
import os
import threading
import time
import numpy as np
from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from pathlib import Path
import duckdb

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR  = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    from app_visual.motor_semantico_llm import get_perfil_universidad, evaluar_idoneidad_contrato
except ImportError:
    from motor_semantico_llm import get_perfil_universidad, evaluar_idoneidad_contrato
PARQUET_DIR = BASE_DIR / "data" / "parquet"
PORT = int(os.environ.get("SECOP_PORT", os.environ.get("PORT", "8080")))
TIC_FILE  = "SECOP_INDUSTRIA_TIC_2015_2025.parquet"
UNIF_FILE = "SECOP_UNIFICADO_2015_2025.parquet"
BASE_HISTORICA_TIC = 213_123

# Pre-conectar DuckDB en memoria persistente
DB = duckdb.connect(":memory:")
DB_LOCK = threading.Lock()

INGEST_STATE = {
    "running": False,
    "phase": "idle",
    "message": "Base histórica lista (213.123 contratos TIC). Pulse el botón para actualizar vía API pública Socrata/SODA.",
    "fetched": 0,
    "inserted": 0,
    "updated": 0,
    "total": BASE_HISTORICA_TIC,
    "base_historica": BASE_HISTORICA_TIC,
    "error": None,
    "last_finished_at": None,
    "sources": [],
    "started_at": None,
    "duration_s": None,
}
_INGEST_THREAD = None
_INGEST_THREAD_LOCK = threading.Lock()

def init_db():
    for f in PARQUET_DIR.glob("*.parquet"):
        clean = str(f).replace("\\", "/")
        view_name = f.stem.lower().replace("-", "_")
        try:
            DB.execute(f"CREATE OR REPLACE VIEW v_{view_name} AS SELECT * FROM read_parquet('{clean}')")
        except Exception as e:
            print(f"Error cargando {f.name}: {e}")
    try:
        total = DB.execute("SELECT count(*) FROM v_secop_industria_tic_2015_2025").fetchone()[0]
        INGEST_STATE["total"] = int(total)
    except Exception:
        pass


def reload_tic_views() -> int:
    with DB_LOCK:
        init_db()
        try:
            total = int(DB.execute("SELECT count(*) FROM v_secop_industria_tic_2015_2025").fetchone()[0])
        except Exception:
            total = int(INGEST_STATE.get("total") or BASE_HISTORICA_TIC)
    INGEST_STATE["total"] = total
    return total


def _ingest_progress(update: dict):
    for key, value in update.items():
        if key in INGEST_STATE or key in (
            "running", "phase", "message", "fetched", "inserted", "updated",
            "total", "error", "last_finished_at", "sources", "tic_valid",
        ):
            INGEST_STATE[key] = value


def _run_tic_ingest_job(lookback_days=180, max_pages=24, page_size=400):
    INGEST_STATE.update({
        "running": True,
        "phase": "inicio",
        "error": None,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "message": "Conectando con la API pública SECOP (Socrata/SODA)…",
    })
    try:
        from ingestion.pipeline import IngestionPipeline
        pipe = IngestionPipeline()
        meta = pipe.run_tic_update(
            lookback_days=lookback_days,
            max_pages=max_pages,
            page_size=page_size,
            on_progress=_ingest_progress,
        )
        total = reload_tic_views()
        INGEST_STATE.update({
            "running": False,
            "phase": "listo",
            "total": total,
            "inserted": int(meta.get("insertados") or 0),
            "updated": int(meta.get("actualizados") or 0),
            "last_finished_at": meta.get("last_run"),
            "sources": meta.get("sources") or [],
            "duration_s": meta.get("duration_s"),
            "message": (
                f"Actualización completada. Base histórica {BASE_HISTORICA_TIC:,} → "
                f"{total:,} contratos TIC "
                f"(+{int(meta.get('insertados') or 0):,} nuevos, "
                f"{int(meta.get('actualizados') or 0):,} actualizados vía lambda)."
            ),
            "error": None,
        })
    except Exception as exc:
        INGEST_STATE.update({
            "running": False,
            "phase": "error",
            "error": f"{type(exc).__name__}: {exc}",
            "message": f"La ingesta falló: {exc}",
        })


def kick_tic_ingest(lookback_days=180, max_pages=24, page_size=400) -> dict:
    global _INGEST_THREAD
    with _INGEST_THREAD_LOCK:
        if INGEST_STATE.get("running") and _INGEST_THREAD and _INGEST_THREAD.is_alive():
            return {"accepted": False, "reason": "already_running", **snapshot_ingest()}
        t = threading.Thread(
            target=_run_tic_ingest_job,
            kwargs={
                "lookback_days": lookback_days,
                "max_pages": max_pages,
                "page_size": page_size,
            },
            daemon=True,
            name="tic-ingest",
        )
        INGEST_STATE["running"] = True
        INGEST_STATE["phase"] = "inicio"
        INGEST_STATE["error"] = None
        _INGEST_THREAD = t
        t.start()
    return {"accepted": True, **snapshot_ingest()}


def snapshot_ingest() -> dict:
    from ingestion.tic_sync import load_meta
    meta = {}
    try:
        meta = load_meta()
    except Exception:
        meta = {}
    snap = dict(INGEST_STATE)
    snap["meta"] = meta
    snap["base_historica"] = BASE_HISTORICA_TIC
    total_meta = meta.get("total_actual")
    if not snap.get("total") or snap.get("total") == BASE_HISTORICA_TIC:
        if total_meta:
            snap["total"] = int(total_meta)
    if not snap.get("inserted"):
        snap["inserted"] = int(meta.get("insertados") or meta.get("insertados_acumulados") or 0)
    if not snap.get("updated"):
        snap["updated"] = int(meta.get("actualizados") or meta.get("actualizados_acumulados") or 0)
    if meta.get("last_run") and snap.get("phase") == "idle":
        snap["last_finished_at"] = meta.get("last_run")
        snap["sources"] = meta.get("sources") or snap.get("sources") or []
        snap["message"] = (
            f"Última sync {meta.get('last_run')}: base {BASE_HISTORICA_TIC:,} → "
            f"{int(snap.get('total') or BASE_HISTORICA_TIC):,} "
            f"(+{int(snap.get('inserted') or 0):,} nuevos, "
            f"{int(snap.get('updated') or 0):,} actualizados)."
        )
    return snap

def generate_executive_pdf(tipo="territorial", depto="", anio_desde="", anio_hasta=""):
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, leftMargin=36, rightMargin=36, topMargin=36, bottomMargin=36)
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        'DocTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=13,
        leading=16,
        textColor=colors.HexColor('#0F172A')
    )
    sub_style = ParagraphStyle(
        'DocSub',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=8.5,
        leading=11,
        textColor=colors.HexColor('#475569')
    )
    body_style = ParagraphStyle(
        'DocBody',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=8,
        leading=11,
        textColor=colors.HexColor('#1E293B')
    )
    bold_style = ParagraphStyle(
        'DocBold',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=8,
        leading=11,
        textColor=colors.HexColor('#0F172A')
    )

    clauses = ["valor_contrato > 0"]
    if depto: clauses.append(f"departamento = '{depto}'")
    if anio_desde: clauses.append(f"anio_contratacion >= {anio_desde}")
    if anio_hasta: clauses.append(f"anio_contratacion <= {anio_hasta}")
    where = " AND ".join(clauses)

    kpis = DB.execute(f"""
        SELECT count(*) as cttos,
               COALESCE(sum(valor_contrato), 0.0) as tot_cop,
               COALESCE(avg(valor_contrato), 0.0) as avg_cop,
               COALESCE(median(valor_contrato), 0.0) as med_cop,
               COALESCE(avg(dias_adicionados), 0.0) as dias_prom,
               COALESCE(avg(flag_innovacion_conpes)*100, 0.0) as pct_conpes,
               COALESCE(avg(es_pyme_bin)*100, 0.0) as pct_pyme
        FROM v_secop_industria_tic_2015_2025
        WHERE {where}
    """).df().to_dict('records')[0]

    subsectores = DB.execute(f"""
        SELECT COALESCE(subsector_tic, 'Sin clasificar') as subsector,
               count(*) as cttos,
               COALESCE(sum(valor_contrato), 0.0) as tot_cop,
               COALESCE(avg(valor_contrato), 0.0) as avg_cop
        FROM v_secop_industria_tic_2015_2025
        WHERE {where}
        GROUP BY 1 ORDER BY tot_cop DESC LIMIT 5
    """).df().to_dict('records')

    entidades = DB.execute(f"""
        SELECT COALESCE(nombre_entidad, 'NO DEFINIDA') as entidad,
               count(*) as cttos,
               COALESCE(sum(valor_contrato), 0.0) as tot_cop
        FROM v_secop_industria_tic_2015_2025
        WHERE {where}
        GROUP BY 1 ORDER BY tot_cop DESC LIMIT 5
    """).df().to_dict('records')

    titles_map = {
        "territorial": f"INFORME EJECUTIVO DE DESEMPEÑO TERRITORIAL: {depto or 'CONSOLIDADO NACIONAL'}",
        "riesgos": f"AUDITORÍA TÉCNICA DE PROCESOS CRÍTICOS, PRÓRROGAS Y ANOMALÍAS",
        "mercado": f"ESTUDIO DE ESTRUCTURA DE MERCADO, COMPETENCIA E ÍNDICE HHI",
        "conpes": f"BALANCE DE INNOVACIÓN Y TRANSFORMACIÓN DIGITAL (CONPES 4069)"
    }
    title_text = titles_map.get(tipo, f"INFORME EJECUTIVO SECOP TIC: {depto or 'NACIONAL'}")

    story = []
    story.append(Paragraph("SISTEMA DE INTELIGENCIA Y OBSERVATORIO DE CONTRATACIÓN PÚBLICA TIC", sub_style))
    story.append(Paragraph(title_text, title_style))
    story.append(Paragraph(f"Alcance: {depto or 'Nivel Nacional'} | Vigencia: {anio_desde or '2015'} - {anio_hasta or '2025'} | Fuente: SECOP I & II", sub_style))
    story.append(Spacer(1, 6))
    story.append(HRFlowable(width="100%", thickness=1.5, color=colors.HexColor('#2563EB'), spaceAfter=8))

    story.append(Paragraph(
        f"El presente informe consolida el comportamiento contractual auditado para <b>{depto or 'Colombia (32 Departamentos)'}</b>. "
        f"Se identificaron <b>{int(kpis['cttos']):,}</b> procesos de contratación en Tecnologías de la Información con una "
        f"inversión acumulada de <b>${round(kpis['tot_cop']):,} COP</b> (${round(kpis['tot_cop']/1e9, 2):,} MM COP). "
        f"El valor promedio por proceso se situó en <b>${round(kpis['avg_cop']):,} COP</b> con una mediana de <b>${round(kpis['med_cop']):,} COP</b>.",
        body_style
    ))
    story.append(Spacer(1, 8))

    data_kpis = [
        ["Total Contratos", "Inversión Total (COP)", "Promedio Proceso", "Mediana (P50)", "Prórroga Media", "% Innovación"],
        [
            f"{int(kpis['cttos']):,}",
            f"${round(kpis['tot_cop']/1e9, 2):,} MM",
            f"${round(kpis['avg_cop']):,}",
            f"${round(kpis['med_cop']):,}",
            f"{kpis['dias_prom']:.1f} días",
            f"{kpis['pct_conpes']:.1f}%"
        ]
    ]
    t_kpis = Table(data_kpis, colWidths=[90, 95, 90, 90, 85, 90])
    t_kpis.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1E293B')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.HexColor('#94A3B8')),
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,0), 7.5),
        ('FONTSIZE', (0,1), (-1,1), 8),
        ('TEXTCOLOR', (0,1), (-1,1), colors.HexColor('#0F172A')),
        ('BACKGROUND', (0,1), (-1,1), colors.HexColor('#F1F5F9')),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#CBD5E1')),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4)
    ]))
    story.append(t_kpis)
    story.append(Spacer(1, 10))

    story.append(Paragraph("1. DISTRIBUCIÓN POR SUBSECTOR TECNOLÓGICO", bold_style))
    story.append(Spacer(1, 4))
    tot_sum = kpis['tot_cop'] if kpis['tot_cop'] > 0 else 1.0
    data_sub = [["Subsector TIC", "Contratos", "Monto Total (COP)", "Participación (%)", "Promedio"]]
    for s in subsectores:
        data_sub.append([
            s['subsector'][:35],
            f"{int(s['cttos']):,}",
            f"${round(s['tot_cop']):,} COP",
            f"{(s['tot_cop']/tot_sum)*100:.1f}%",
            f"${round(s['avg_cop']):,} COP"
        ])
    t_sub = Table(data_sub, colWidths=[180, 65, 115, 80, 100])
    t_sub.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#0F172A')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 7.5),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#CBD5E1')),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#F8FAFC')]),
        ('ALIGN', (1,0), (-1,-1), 'RIGHT'),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3)
    ]))
    story.append(t_sub)
    story.append(Spacer(1, 10))

    story.append(Paragraph("2. PRINCIPALES ENTIDADES COMPRADORAS", bold_style))
    story.append(Spacer(1, 4))
    data_ent = [["Entidad Compradora", "Contratos", "Inversión Total (COP)", "Participación"]]
    for e in entidades:
        data_ent.append([
            e['entidad'][:45],
            f"{int(e['cttos']):,}",
            f"${round(e['tot_cop']):,} COP",
            f"{(e['tot_cop']/tot_sum)*100:.1f}%"
        ])
    t_ent = Table(data_ent, colWidths=[240, 70, 140, 90])
    t_ent.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#0F172A')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 7.5),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#CBD5E1')),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#F8FAFC')]),
        ('ALIGN', (1,0), (-1,-1), 'RIGHT'),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3)
    ]))
    story.append(t_ent)
    story.append(Spacer(1, 14))

    data_sign = [
        ["____________________________________________", "____________________________________________"],
        ["EQUIPO TÉCNICO DE AUDITORÍA TIC", "DIRECCIÓN DE INTELIGENCIA DE DATOS"],
        ["Observatorio de Contratación Pública", "Validado vía DuckDB Columnar OLAP"]
    ]
    t_sign = Table(data_sign, colWidths=[270, 270])
    t_sign.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica'),
        ('FONTSIZE', (0,0), (-1,-1), 7.5),
        ('TEXTCOLOR', (0,0), (-1,-1), colors.HexColor('#475569')),
        ('TOPPADDING', (0,0), (-1,-1), 2),
        ('BOTTOMPADDING', (0,0), (-1,-1), 2)
    ]))
    story.append(t_sign)

    doc.build(story)
    return buf.getvalue()

def generate_report_preview_data(tipo="territorial", depto="", anio_desde="", anio_hasta=""):
    clauses = ["valor_contrato > 0"]
    if depto: clauses.append(f"departamento = '{depto}'")
    if anio_desde: clauses.append(f"anio_contratacion >= {anio_desde}")
    if anio_hasta: clauses.append(f"anio_contratacion <= {anio_hasta}")
    where = " AND ".join(clauses)

    kpis = DB.execute(f"""
        SELECT count(*) as cttos,
               COALESCE(sum(valor_contrato), 0.0) as tot_cop,
               COALESCE(avg(valor_contrato), 0.0) as avg_cop,
               COALESCE(median(valor_contrato), 0.0) as med_cop,
               COALESCE(avg(dias_adicionados), 0.0) as dias_prom,
               COALESCE(avg(flag_innovacion_conpes)*100, 0.0) as pct_conpes,
               COALESCE(avg(es_pyme_bin)*100, 0.0) as pct_pyme
        FROM v_secop_industria_tic_2015_2025
        WHERE {where}
    """).df().to_dict('records')[0]

    subsectores = DB.execute(f"""
        SELECT COALESCE(subsector_tic, 'Sin clasificar') as subsector,
               count(*) as cttos,
               COALESCE(sum(valor_contrato), 0.0) as tot_cop,
               COALESCE(avg(valor_contrato), 0.0) as avg_cop
        FROM v_secop_industria_tic_2015_2025
        WHERE {where}
        GROUP BY 1 ORDER BY tot_cop DESC LIMIT 5
    """).df().to_dict('records')

    entidades = DB.execute(f"""
        SELECT COALESCE(nombre_entidad, 'NO DEFINIDA') as entidad,
               count(*) as cttos,
               COALESCE(sum(valor_contrato), 0.0) as tot_cop
        FROM v_secop_industria_tic_2015_2025
        WHERE {where}
        GROUP BY 1 ORDER BY tot_cop DESC LIMIT 5
    """).df().to_dict('records')

    return {
        "tipo": tipo,
        "depto": depto or "Nivel Nacional",
        "vigencia": f"{anio_desde or '2015'} - {anio_hasta or '2025'}",
        "kpis": kpis,
        "subsectores": subsectores,
        "entidades": entidades
    }

HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SECOP Analytics | Sistema de Inteligencia de Contratación Pública</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
:root {
  --bg-main: #0B0F19;
  --bg-panel: #111827;
  --bg-card: #1F2937;
  --bg-hover: #283548;
  --border: #374151;
  --border-focus: #4B5563;
  --accent: #2563EB;
  --accent-hover: #1D4ED8;
  --accent-subtle: rgba(37, 99, 235, 0.12);
  --cyan: #0891B2;
  --text-primary: #F9FAFB;
  --text-secondary: #9CA3AF;
  --text-muted: #6B7280;
  --font-sans: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
  --font-mono: 'JetBrains Mono', monospace;
  --success: #10B981;
  --warning: #F59E0B;
  --danger: #EF4444;
}

* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: var(--font-sans);
  background: var(--bg-main);
  color: var(--text-primary);
  min-height: 100vh;
  display: flex;
  flex-direction: column;
  line-height: 1.4;
}

/* Header */
header {
  background: var(--bg-panel);
  border-bottom: 1px solid var(--border);
  padding: 8px 20px;
  min-height: 56px;
  height: auto;
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 10px;
}
.brand {
  display: flex;
  align-items: center;
  gap: 12px;
}
.brand-mark {
  width: 28px;
  height: 28px;
  background: var(--accent);
  color: #fff;
  font-weight: 700;
  font-size: 13px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
}
.brand-text h1 {
  font-size: 14px;
  font-weight: 600;
  letter-spacing: -0.01em;
  color: var(--text-primary);
}
.brand-text p {
  font-size: 11px;
  color: var(--text-muted);
}
.header-actions {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}
.tag-live {
  font-size: 11px;
  font-weight: 500;
  color: var(--success);
  display: flex;
  align-items: center;
  gap: 6px;
}
.tag-live::before {
  content: '';
  width: 6px;
  height: 6px;
  background: var(--success);
  border-radius: 50%;
}
.ds-control {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 12px;
  color: var(--text-secondary);
}
.ds-select {
  background: var(--bg-card);
  border: 1px solid var(--border);
  color: var(--text-primary);
  font-family: inherit;
  font-size: 12px;
  padding: 5px 10px;
  border-radius: 4px;
  outline: none;
  cursor: pointer;
}
.ds-select:focus { border-color: var(--accent); }

.btn-actualizar-tic {
  background: linear-gradient(180deg, #0EA5E9 0%, #0369A1 100%);
  color: #F0F9FF;
  border: 1px solid #38BDF8;
  font-family: inherit;
  font-size: 11.5px;
  font-weight: 700;
  letter-spacing: 0.01em;
  padding: 8px 14px;
  border-radius: 6px;
  cursor: pointer;
  white-space: nowrap;
  box-shadow: 0 0 0 1px rgba(56,189,248,0.25), 0 8px 18px rgba(8,47,73,0.45);
  transition: transform 0.12s ease, filter 0.12s ease, box-shadow 0.12s ease;
}
.btn-actualizar-tic:hover {
  filter: brightness(1.08);
  transform: translateY(-1px);
}
.btn-actualizar-tic:disabled {
  opacity: 0.65;
  cursor: wait;
  transform: none;
}
.ingest-banner {
  background: #0B1220;
  border-bottom: 1px solid #1E3A5F;
  padding: 8px 20px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
  font-size: 12px;
}
.ingest-banner .ingest-stats {
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
  color: #94A3B8;
}
.ingest-banner strong { color: #E2E8F0; font-variant-numeric: tabular-nums; }
.ingest-banner .delta-up { color: #34D399; font-weight: 700; }
.ingest-banner .ingest-msg { color: #7DD3FC; font-size: 12px; }
.ingest-banner.running { border-bottom-color: #38BDF8; }
.ingest-banner.error { border-bottom-color: #EF4444; }
.progress-track {
  width: 160px;
  height: 5px;
  background: #1F2937;
  border-radius: 99px;
  overflow: hidden;
  flex-shrink: 0;
}
.progress-fill {
  height: 100%;
  width: 12%;
  background: #38BDF8;
  animation: ingestPulse 1.2s ease-in-out infinite;
}
@keyframes ingestPulse {
  0% { width: 12%; }
  50% { width: 78%; }
  100% { width: 12%; }
}
@media (prefers-reduced-motion: reduce) {
  .progress-fill { animation: none; width: 50%; }
}
@media (max-width: 480px) {
  body { overflow-x: hidden; }
  header { padding: 8px 12px; }
  .btn-actualizar-tic {
    white-space: normal;
    text-align: left;
    line-height: 1.3;
    max-width: 100%;
  }
  .ingest-banner { padding: 8px 12px; }
  .ingest-banner .ingest-stats { gap: 8px; font-size: 11px; }
  main { padding: 12px; max-width: 100%; }
  .filter-container { overflow-x: auto; }
}

/* Global Scrollbars */
::-webkit-scrollbar {
  width: 6px;
  height: 5px;
}
::-webkit-scrollbar-track {
  background: var(--bg-main);
}
::-webkit-scrollbar-thumb {
  background: #374151;
  border-radius: 4px;
}
::-webkit-scrollbar-thumb:hover {
  background: #4B5563;
}
* {
  scrollbar-width: thin;
  scrollbar-color: #374151 var(--bg-main);
}

/* Navigation */
nav {
  background: var(--bg-panel);
  border-bottom: 1px solid var(--border);
  padding: 0 16px;
  display: flex;
  align-items: center;
  gap: 2px;
  overflow-x: auto;
  scrollbar-width: none;
  -ms-overflow-style: none;
}
nav::-webkit-scrollbar {
  display: none;
}
.tab-item {
  padding: 11px 12px;
  font-size: 11.5px;
  font-weight: 500;
  color: var(--text-secondary);
  border-bottom: 2px solid transparent;
  cursor: pointer;
  background: none;
  border-top: none;
  border-left: none;
  border-right: none;
  white-space: nowrap;
  transition: all 0.15s ease;
  display: flex;
  align-items: center;
  gap: 5px;
  letter-spacing: -0.01em;
  border-radius: 4px 4px 0 0;
}
.tab-item:hover {
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.03);
}
.tab-item.active {
  color: #38BDF8;
  border-bottom: 2px solid #38BDF8;
  background: rgba(56, 189, 248, 0.06);
  font-weight: 600;
}

/* Layout */
main {
  flex: 1;
  padding: 20px 24px;
  max-width: 1600px;
  width: 100%;
  margin: 0 auto;
}
.tab-content { display: none; }
.tab-content.active { display: block; }

/* Filter Container */
.filter-container {
  background: var(--bg-panel);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 12px 16px;
  display: flex;
  align-items: flex-end;
  gap: 12px;
  flex-wrap: wrap;
  margin-bottom: 16px;
}
.filter-item {
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.filter-item label {
  font-size: 11px;
  font-weight: 500;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.02em;
}
.filter-input {
  background: var(--bg-card);
  border: 1px solid var(--border);
  color: var(--text-primary);
  font-family: inherit;
  font-size: 12px;
  padding: 6px 10px;
  border-radius: 4px;
  outline: none;
  min-width: 130px;
}
.filter-input:focus { border-color: var(--accent); }
.btn-primary {
  background: var(--accent);
  color: #fff;
  border: none;
  font-family: inherit;
  font-size: 12px;
  font-weight: 500;
  padding: 7px 14px;
  border-radius: 4px;
  cursor: pointer;
  transition: background 0.15s;
}
.btn-primary:hover { background: var(--accent-hover); }
.btn-secondary {
  background: var(--bg-card);
  color: var(--text-secondary);
  border: 1px solid var(--border);
  font-family: inherit;
  font-size: 12px;
  padding: 6px 12px;
  border-radius: 4px;
  cursor: pointer;
}
.btn-secondary:hover { background: var(--bg-hover); color: var(--text-primary); }
.filter-status-text {
  font-size: 11px;
  color: var(--text-muted);
  margin-left: auto;
  align-self: center;
}

/* KPI Cards */
.kpi-row {
  display: grid;
  grid-template-columns: repeat(6, 1fr);
  gap: 12px;
  margin-bottom: 16px;
}
@media (max-width: 1200px) {
  .kpi-row { grid-template-columns: repeat(3, 1fr); }
}
@media (max-width: 700px) {
  .kpi-row { grid-template-columns: repeat(2, 1fr); }
}
.kpi-box {
  background: var(--bg-panel);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 14px 16px;
}
.kpi-title {
  font-size: 11px;
  font-weight: 500;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.02em;
  margin-bottom: 6px;
}
.kpi-num {
  font-size: 20px;
  font-weight: 700;
  color: var(--text-primary);
  font-family: var(--font-mono);
  letter-spacing: -0.02em;
}
.kpi-meta {
  font-size: 11px;
  color: var(--text-secondary);
  margin-top: 4px;
}

/* Grids */
.grid-2, .grid-3, .grid-equal {
  display: grid;
  gap: 16px;
  margin-bottom: 16px;
  width: 100%;
}
.grid-2 { grid-template-columns: minmax(0, 1.4fr) minmax(0, 1fr); }
.grid-3 { grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1fr); }
.grid-equal { grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); }

@media (max-width: 1100px) {
  .grid-2, .grid-3, .grid-equal { grid-template-columns: minmax(0, 1fr); }
}

/* Chart Containers */
.chart-box {
  position: relative;
  width: 100%;
  height: 250px;
  max-height: 250px;
  overflow: hidden;
}
.chart-box-sm {
  position: relative;
  width: 100%;
  height: 210px;
  max-height: 210px;
  overflow: hidden;
}
.chart-box-lg {
  position: relative;
  width: 100%;
  height: 320px;
  max-height: 320px;
  overflow: hidden;
}
.chart-box-radar {
  position: relative;
  width: 100%;
  height: 280px;
  max-height: 280px;
  overflow: hidden;
}

/* Card */
.card-panel {
  background: var(--bg-panel);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 16px;
  min-width: 0;
}
.card-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 14px;
}
.card-heading {
  font-size: 13px;
  font-weight: 600;
  color: var(--text-primary);
}
.card-subtext {
  font-size: 11px;
  color: var(--text-muted);
}
.badge-clean {
  font-size: 10px;
  padding: 2px 7px;
  background: var(--accent-subtle);
  color: #60A5FA;
  border-radius: 3px;
  font-weight: 500;
}
.badge-highlight {
  font-size: 10px;
  padding: 3px 8px;
  background: rgba(16, 185, 129, 0.15);
  color: var(--success);
  border-radius: 4px;
  font-weight: 600;
}

/* Data Tables */
.table-responsive {
  width: 100%;
  overflow-x: auto;
}
table.clean-tbl {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}
table.clean-tbl th {
  background: var(--bg-card);
  color: var(--text-secondary);
  font-weight: 500;
  text-align: left;
  padding: 9px 12px;
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
}
table.clean-tbl td {
  padding: 8px 12px;
  border-bottom: 1px solid rgba(55, 65, 81, 0.5);
  color: var(--text-primary);
  white-space: nowrap;
}
table.clean-tbl tr:hover td { background: var(--bg-card); }
.num-cell {
  text-align: right;
  font-family: var(--font-mono);
}

/* LEAFLET GEOGRAPHIC HEATMAP CONTAINER */
#geoLeafletMap {
  width: 100%;
  height: 460px;
  border-radius: 6px;
  background: #0F172A;
  border: 1px solid var(--border);
}
.leaflet-popup-content-wrapper {
  background: #111827 !important;
  color: #F9FAFB !important;
  border: 1px solid #374151 !important;
  border-radius: 6px !important;
  font-family: 'Inter', sans-serif !important;
  padding: 4px !important;
}
.leaflet-popup-tip { background: #111827 !important; }

/* Drill Down Section */
.drilldown-container {
  background: rgba(17, 24, 39, 0.9);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 16px;
  margin-top: 16px;
}
.drilldown-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px;
  margin-bottom: 16px;
  padding-bottom: 12px;
  border-bottom: 1px solid var(--border);
}
.drilldown-title {
  display: flex;
  align-items: center;
  gap: 10px;
}
.drilldown-badge {
  background: #2563EB;
  color: #fff;
  font-size: 11px;
  font-weight: 700;
  padding: 3px 8px;
  border-radius: 4px;
  letter-spacing: 0.02em;
}

/* STUDIO MULTIDIMENSIONAL / INTELLIGENCE STUDIO */
.studio-pills-bar {
  display: flex;
  gap: 8px;
  overflow-x: auto;
  padding-bottom: 6px;
  margin-bottom: 14px;
}
.studio-pill {
  background: var(--bg-card);
  border: 1px solid var(--border);
  color: var(--text-secondary);
  font-family: inherit;
  font-size: 11px;
  font-weight: 600;
  padding: 6px 12px;
  border-radius: 4px;
  cursor: pointer;
  white-space: nowrap;
  transition: all 0.15s;
}
.studio-pill:hover {
  background: var(--bg-hover);
  color: var(--text-primary);
  border-color: var(--border-focus);
}
.studio-pill.active {
  background: var(--accent);
  color: #fff;
  border-color: var(--accent);
}

.studio-builder-box {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 14px;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 12px;
  align-items: flex-end;
  margin-bottom: 16px;
}

/* COMPARADOR MULTIDIMENSIONAL ESTILOS */
.compare-hero-box {
  background: var(--bg-panel);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 16px;
  margin-bottom: 16px;
}
.compare-select-grid {
  display: grid;
  grid-template-columns: 1fr 1.2fr 1.2fr auto;
  gap: 12px;
  align-items: flex-end;
  margin-top: 12px;
}
@media (max-width: 900px) {
  .compare-select-grid { grid-template-columns: 1fr; }
}
.compare-kpi-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px;
  margin-bottom: 16px;
}
@media (max-width: 1000px) {
  .compare-kpi-grid { grid-template-columns: 1fr; }
}
.compare-card {
  background: var(--bg-panel);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 14px 16px;
}
.compare-card-title {
  font-size: 11px;
  font-weight: 600;
  color: var(--text-muted);
  text-transform: uppercase;
  margin-bottom: 8px;
  letter-spacing: 0.02em;
}
.compare-values-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
}
.val-block {
  display: flex;
  flex-direction: column;
}
.val-tag-a {
  font-size: 10px;
  font-weight: 700;
  color: #3B82F6;
  margin-bottom: 2px;
}
.val-tag-b {
  font-size: 10px;
  font-weight: 700;
  color: #10B981;
  margin-bottom: 2px;
}
.val-num {
  font-family: var(--font-mono);
  font-size: 14px;
  font-weight: 700;
  color: var(--text-primary);
}
.delta-badge {
  font-family: var(--font-mono);
  font-size: 11px;
  font-weight: 700;
  padding: 3px 8px;
  border-radius: 4px;
}
.delta-pos { background: rgba(16, 185, 129, 0.15); color: var(--success); }
.delta-neg { background: rgba(239, 68, 68, 0.15); color: var(--danger); }
.delta-neutral { background: rgba(156, 163, 175, 0.15); color: var(--text-muted); }

/* REPORT GENERATOR & PREVIEW STYLES */
.report-paper-container {
  background: #0F172A;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 24px;
  max-width: 1100px;
  margin: 0 auto;
  color: #F9FAFB;
  box-shadow: 0 10px 25px rgba(0,0,0,0.5);
}
.report-paper-header {
  border-bottom: 2px solid #3B82F6;
  padding-bottom: 14px;
  margin-bottom: 18px;
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
}
.report-meta-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 12px;
  background: var(--bg-card);
  padding: 12px 16px;
  border-radius: 6px;
  margin-bottom: 18px;
}
.report-sign-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 40px;
  margin-top: 30px;
  padding-top: 20px;
  border-top: 1px solid var(--border);
}
.report-sign-line {
  border-bottom: 1px solid #4B5563;
  height: 40px;
  margin-bottom: 6px;
}

/* Accordion SQL */
.accordion-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: var(--bg-card);
  border: 1px solid var(--border);
  padding: 10px 14px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 12px;
  font-weight: 600;
  color: var(--text-secondary);
  margin-top: 16px;
  margin-bottom: 8px;
}
.accordion-header:hover { color: var(--text-primary); }
.accordion-body { display: none; margin-bottom: 16px; }
.accordion-body.open { display: block; }

.sql-textarea {
  width: 100%;
  height: 100px;
  background: #0D1117;
  border: 1px solid var(--border);
  color: #58A6FF;
  font-family: var(--font-mono);
  font-size: 12px;
  padding: 10px 12px;
  border-radius: 4px;
  outline: none;
  resize: vertical;
  margin-bottom: 10px;
}
.sql-textarea:focus { border-color: var(--accent); }

/* Search Box */
.search-box {
  background: var(--bg-card);
  border: 1px solid var(--border);
  color: var(--text-primary);
  font-family: inherit;
  font-size: 12px;
  padding: 7px 12px;
  border-radius: 4px;
  outline: none;
}
.search-box:focus { border-color: var(--accent); }

/* Meter */
.meter-row { margin-bottom: 12px; }
.meter-info {
  display: flex;
  justify-content: space-between;
  font-size: 11px;
  margin-bottom: 4px;
}
.meter-track {
  height: 4px;
  background: var(--bg-card);
  border-radius: 2px;
  overflow: hidden;
}
.meter-fill {
  height: 100%;
  background: var(--accent);
}
</style>
</head>
<body>

<header>
  <div class="brand">
    <div class="brand-mark">TIC</div>
    <div class="brand-text">
      <h1>SECOP Analytics & Intelligence</h1>
      <p>Observatorio de Contratación Pública en Tecnologías de la Información (2015 - 2025)</p>
    </div>
  </div>
  <div class="header-actions">
    <div class="tag-live" id="tagLiveIngest">SODA en vivo</div>
    <button type="button" id="btnActualizarTic" class="btn-actualizar-tic" onclick="realizarActualizacionContratosTIC()">
      Realizar actualización de los contratos (contratación pública en TIC)
    </button>
    <div class="ds-control">
      <span>Conjunto Activo:</span>
      <select id="selDataset" class="ds-select" onchange="onDatasetChange()">
        <option value="SECOP_INDUSTRIA_TIC_2015_2025.parquet">Industria TIC (base 213,123 + ingesta)</option>
        <option value="SECOP_UNIFICADO_2015_2025.parquet">SECOP General Unificado</option>
      </select>
    </div>
  </div>
</header>

<div class="ingest-banner" id="ingestBanner">
  <div class="ingest-stats">
    <span>Base histórica <strong id="ingestBase">213,123</strong></span>
    <span>Total actual <strong id="ingestTotal">213,123</strong></span>
    <span>Nuevos <strong class="delta-up" id="ingestInserted">+0</strong></span>
    <span>Actualizados (lambda) <strong id="ingestUpdated">0</strong></span>
  </div>
  <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
    <div class="progress-track" id="ingestProgressTrack" style="display:none"><div class="progress-fill"></div></div>
    <span class="ingest-msg" id="ingestMsg">Listo para sincronizar con datos.gov.co</span>
  </div>
</div>

<nav>
  <button class="tab-item active" onclick="setTab('tic', this)">Dashboard General</button>
  <button class="tab-item" onclick="setTab('ingesta', this)" style="color:#38BDF8;font-weight:600;">Ingesta continua SODA</button>
  <button class="tab-item" onclick="setTab('departamentos', this)">Distribución Geográfica</button>
  <button class="tab-item" onclick="setTab('comparativa', this)">Estado vs. TIC</button>
  <button class="tab-item" onclick="setTab('comparador', this)">Comparador</button>
  <button class="tab-item" onclick="setTab('explorador', this)">Explorador</button>
  <button class="tab-item" onclick="setTab('consola', this)">Estudio Multidimensional</button>
  <button class="tab-item" onclick="setTab('ml', this)">Machine Learning (CONPES)</button>
  <button class="tab-item" onclick="setTab('reportes', this)">Generador de Reportes</button>
  <button class="tab-item" onclick="setTab('auditoria', this)">Auditoría de Cifras</button>
  <button class="tab-item" onclick="setTab('semantico', this)" style="border-left:1px solid var(--border);color:#38BDF8;font-weight:600;">Motor Semántico LLM</button>
</nav>

<main>
  <!-- TAB INGESTA CONTINUA -->
  <section id="pane-ingesta" class="tab-content">
    <div class="filter-container" style="margin-bottom:16px">
      <div>
        <div class="card-heading">Ingesta continua · contratación pública en TIC</div>
        <div class="card-subtext">API pública Socrata/SODA (datos.gov.co) + scrapers. Lambda: si el contrato existe se actualiza; si no, se registra.</div>
      </div>
      <button type="button" class="btn-actualizar-tic" onclick="realizarActualizacionContratosTIC()">Realizar actualización de los contratos (contratación pública en TIC)</button>
    </div>
    <div class="kpi-row" style="grid-template-columns:repeat(4,1fr)">
      <div class="kpi-box">
        <div class="kpi-title">Base histórica filtrada</div>
        <div class="kpi-num" id="ingestaKpiBase">213,123</div>
        <div class="kpi-meta">Parquet Industria TIC 2015-2025</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Universo actual</div>
        <div class="kpi-num" id="ingestaKpiTotal" style="color:#38BDF8">—</div>
        <div class="kpi-meta">Tras lambda upsert</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Nuevos registrados</div>
        <div class="kpi-num" id="ingestaKpiIns" style="color:var(--success)">+0</div>
        <div class="kpi-meta">INSERT (no existían)</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Existentes actualizados</div>
        <div class="kpi-num" id="ingestaKpiUpd">0</div>
        <div class="kpi-meta">UPDATE (ya estaban en la base)</div>
      </div>
    </div>
    <div class="grid-2">
      <div class="card-panel">
        <div class="card-head">
          <span class="card-heading">Fuentes de esta corrida</span>
          <span class="badge-clean">SODA + scrapers</span>
        </div>
        <table class="clean-tbl">
          <thead><tr><th>Fuente</th><th class="num-cell">Registros TIC</th><th>Desde</th></tr></thead>
          <tbody id="ingestaSourcesBody">
            <tr><td colspan="3" style="color:var(--text-muted);padding:14px">Aún no hay corrida. Pulse el botón de actualización.</td></tr>
          </tbody>
        </table>
      </div>
      <div class="card-panel">
        <div class="card-head">
          <span class="card-heading">Bitácora de corridas</span>
          <span class="badge-clean">SQLite live</span>
        </div>
        <div class="table-responsive" style="max-height:280px;overflow:auto">
          <table class="clean-tbl">
            <thead><tr><th>ID</th><th>Dataset</th><th>Estado</th><th class="num-cell">Leídos</th><th class="num-cell">Escritos</th></tr></thead>
            <tbody id="ingestaRunsBody">
              <tr><td colspan="5" style="color:var(--text-muted);padding:14px">Cargando bitácora…</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="card-panel" style="margin-top:16px">
      <div class="card-head">
        <span class="card-heading">Cómo opera la lambda</span>
      </div>
      <p style="font-size:13px;color:var(--text-secondary);line-height:1.55;max-width:920px">
        Cada contrato se identifica por <code>id_contrato_global</code>.
        La función <code>lambda_upsert(pk, existentes)</code> consulta esa llave:
        si ya está en los 213.123 (o en lotes previos) <strong>actualiza</strong> objeto, valor, estado y fechas;
        si no existe, <strong>lo registra</strong> en el Parquet y en SQLite.
        El dashboard, el explorador y los KPI se recargan al terminar.
      </p>
    </div>
  </section>

  <!-- TAB 1: DASHBOARD TIC -->
  <section id="pane-tic" class="tab-content active">
    <!-- FILTROS EXPANDIDOS -->
    <div class="filter-container">
      <div class="filter-item">
        <label>Año Inicial</label>
        <select id="fAnioDesde" class="filter-input">
          <option value="">Todos</option>
        </select>
      </div>
      <div class="filter-item">
        <label>Año Final</label>
        <select id="fAnioHasta" class="filter-input">
          <option value="">Todos</option>
        </select>
      </div>
      <div class="filter-item">
        <label>Departamento</label>
        <select id="fDepto" class="filter-input" style="min-width:170px">
          <option value="">Todos</option>
        </select>
      </div>
      <div class="filter-item">
        <label>Subsector TIC</label>
        <select id="fSubsector" class="filter-input" style="min-width:180px">
          <option value="">Todos los subsectores</option>
          <option value="Servicios de Soporte y Consultoría TI">Soporte y Consultoría TI</option>
          <option value="Desarrollo de Software y Licenciamiento">Desarrollo y Licenciamiento</option>
          <option value="Conectividad y Redes">Conectividad y Redes</option>
          <option value="Ciberseguridad e Infraestructura Cloud">Ciberseguridad y Cloud</option>
          <option value="Hardware y Equipos de Cómputo">Hardware y Equipos</option>
        </select>
      </div>
      <div class="filter-item">
        <label>Tipo de Contrato</label>
        <select id="fTipoContrato" class="filter-input" style="min-width:170px">
          <option value="">Todos los tipos</option>
          <option value="Prestación de servicios">Prestación de Servicios</option>
          <option value="Compraventa">Compraventa</option>
          <option value="Suministro">Suministro</option>
          <option value="Obra">Obra</option>
          <option value="Consultoría">Consultoría</option>
          <option value="Interventoría">Interventoría</option>
          <option value="Arrendamiento">Arrendamiento</option>
          <option value="Otro">Otros Tipos</option>
        </select>
      </div>
      <div class="filter-item">
        <label>Rango de Cuantía</label>
        <select id="fRangoCuantia" class="filter-input" style="min-width:170px">
          <option value="">Todas las cuantías</option>
          <option value="minima">Mínima cuantía (&lt; $50M)</option>
          <option value="menor">Menor cuantía ($50M - $500M)</option>
          <option value="mayor">Mayor cuantía ($500M - $5.000M)</option>
          <option value="mega">Megacontratos (&gt; $5.000M)</option>
        </select>
      </div>
      <button class="btn-primary" onclick="loadDashboard()">Aplicar Filtros</button>
      <button class="btn-secondary" onclick="resetFilters()">Restablecer</button>
      <span class="filter-status-text" id="filterStatus">Actualizado</span>
    </div>

    <!-- KPIS -->
    <div class="kpi-row">
      <div class="kpi-box">
        <div class="kpi-title">Total Contratos</div>
        <div class="kpi-num" id="k1">—</div>
        <div class="kpi-meta">Registros auditados</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Valor Total Contratado</div>
        <div class="kpi-num" id="k2" style="font-size:16px">—</div>
        <div class="kpi-meta" id="k2_meta">Pesos Colombianos (COP)</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Promedio por Contrato</div>
        <div class="kpi-num" id="k3" style="font-size:16px">—</div>
        <div class="kpi-meta">Pesos Colombianos (COP)</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Mediana de Cuantía (P50)</div>
        <div class="kpi-num" id="k4" style="font-size:16px">—</div>
        <div class="kpi-meta">Pesos Colombianos (COP)</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Subsector Dominante</div>
        <div class="kpi-num" id="k5" style="font-size:13px">—</div>
        <div class="kpi-meta">Mayor cuantía agregada</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Jurisdicción Líder</div>
        <div class="kpi-num" id="k6" style="font-size:13px">—</div>
        <div class="kpi-meta">Mayor asignación presupuestal</div>
      </div>
    </div>

    <!-- CHARTS ROW 1 -->
    <div class="grid-2">
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Evolución Presupuestal Anual</div>
            <div class="card-subtext">Cuantía contratada en miles de millones (COP)</div>
          </div>
          <span class="badge-clean">Histórico</span>
        </div>
        <div class="chart-box"><canvas id="chartAnual"></canvas></div>
      </div>
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Distribución por Subsector TIC</div>
            <div class="card-subtext">Participación porcentual en el presupuesto</div>
          </div>
        </div>
        <div class="chart-box"><canvas id="chartSub"></canvas></div>
      </div>
    </div>

    <!-- CHARTS ROW 2 -->
    <!-- PROYECCIÓN FILTRADA -->
    <div class="grid-2">
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Proyección de Contratación Futura</div>
            <div class="card-subtext">Histórico filtrado y estimación de los próximos tres años (MM COP)</div>
          </div>
          <span class="badge-highlight">Estimación</span>
        </div>
        <div class="chart-box"><canvas id="chartDashboardForecast"></canvas></div>
      </div>
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Valores Futuros Estimados</div>
            <div class="card-subtext" id="dashboardForecastNote">Seleccione filtros para recalcular el escenario</div>
          </div>
        </div>
        <div class="table-responsive">
          <table class="clean-tbl">
            <thead>
              <tr>
                <th>Año</th>
                <th class="num-cell">Monto estimado (MM COP)</th>
                <th class="num-cell">Variación anual</th>
              </tr>
            </thead>
            <tbody id="dashboardForecastBody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- CHARTS ROW 2 -->
    <div class="grid-3">
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Top 5 Proveedores Adjudicados</div>
            <div class="card-subtext">Monto adjudicado (MM COP)</div>
          </div>
        </div>
        <div class="chart-box-sm"><canvas id="chartProv"></canvas></div>
      </div>
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Modalidad de Selección</div>
            <div class="card-subtext">Distribución por número de contratos</div>
          </div>
        </div>
        <div class="chart-box-sm"><canvas id="chartMod"></canvas></div>
      </div>
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Índice HHI por Subsector</div>
            <div class="card-subtext">Herfindahl-Hirschman (0-10.000)</div>
          </div>
        </div>
        <div id="hhiContainer" style="display:flex; flex-direction:column; justify-content:center; height:180px"></div>
      </div>
    </div>

    <!-- TABLA RESUMEN SUBSECTORES -->
    <div class="card-panel">
      <div class="card-head">
        <div class="card-heading">Consolidado por Subsector de la Industria TIC</div>
        <span class="badge-clean">2015 - 2025</span>
      </div>
      <div class="table-responsive">
        <table class="clean-tbl">
          <thead>
            <tr>
              <th>Subsector TIC</th>
              <th class="num-cell">Total Contratos</th>
              <th class="num-cell">Monto Total (MM COP)</th>
              <th class="num-cell">% Presupuesto</th>
            </tr>
          </thead>
          <tbody id="subsectorTableBody"></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- TAB 2: DISTRIBUCIÓN GEOGRÁFICA -->
  <section id="pane-departamentos" class="tab-content">
    <!-- KPIS GEOGRÁFICOS -->
    <div class="kpi-row" style="grid-template-columns: repeat(4, 1fr); margin-bottom: 16px;">
      <div class="kpi-box">
        <div class="kpi-title">Cobertura Nacional</div>
        <div class="kpi-num" id="geoDeptCount">32</div>
        <div class="kpi-meta">Departamentos Estandarizados</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Inversión Total Colombia</div>
        <div class="kpi-num" id="geoTotalAmount" style="color:var(--success); font-size:16px">—</div>
        <div class="kpi-meta">100% Universo TIC (2015-2025)</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Jurisdicción Líder</div>
        <div class="kpi-num" id="geoLeaderName" style="font-size:16px">CUNDINAMARCA</div>
        <div class="kpi-meta">Mayor volumen nacional (62.4%)</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Inversión en Antioquia</div>
        <div class="kpi-num" id="geoAntioquiaAmount" style="color:#60A5FA; font-size:16px">—</div>
        <div class="kpi-meta">Segunda potencia TIC (13.4%)</div>
      </div>
    </div>

    <!-- MAPA DE CALOR GEOGRÁFICO REAL + RANKING NACIONAL -->
    <div class="grid-2" style="margin-bottom: 16px;">
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Mapa de Calor Geográfico de Colombia (Inversión TIC)</div>
            <div class="card-subtext">Visualización térmica espacial. Haga clic sobre cualquier nodo o departamento para profundizar.</div>
          </div>
          <span class="badge-highlight">Geo-Heatmap Interactivo</span>
        </div>
        <div id="geoLeafletMap"></div>
      </div>

      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Ranking General de Departamentos</div>
            <div class="card-subtext">Monto asignado en miles de millones (MM COP)</div>
          </div>
        </div>
        <div class="chart-box-lg"><canvas id="chartDeptosBar"></canvas></div>
      </div>
    </div>

    <!-- TABLA CONSOLIDADA NACIONAL DE LOS 32 DEPARTAMENTOS -->
    <div class="card-panel" style="margin-bottom: 16px;">
      <div class="card-head">
        <div>
          <div class="card-heading">Matriz de Distribución Territorial de Colombia (32 Departamentos)</div>
          <div class="card-subtext">Cifras oficiales de contratación, volumen presupuestal e intensidad promedio</div>
        </div>
      </div>
      <div class="table-responsive" style="max-height: 280px; overflow-y:auto">
        <table class="clean-tbl">
          <thead>
            <tr>
              <th>#</th>
              <th>Departamento</th>
              <th class="num-cell">Contratos</th>
              <th class="num-cell">Total Inversión (COP)</th>
              <th class="num-cell">Participación Nacional</th>
              <th class="num-cell">Promedio por Contrato</th>
              <th>Acción</th>
            </tr>
          </thead>
          <tbody id="geoTableBody"></tbody>
        </table>
      </div>
    </div>

    <!-- MÓDULO DE PROFUNDIZACIÓN TERRITORIAL (DRILL-DOWN) -->
    <div class="drilldown-container" id="drilldownSection">
      <div class="drilldown-header">
        <div class="drilldown-title">
          <span class="drilldown-badge">ANÁLISIS EN PROFUNDIDAD</span>
          <h2 style="font-size:16px; font-weight:700" id="drilldownDeptoTitle">Departamento de ANTIOQUIA</h2>
        </div>
        <div style="display:flex; align-items:center; gap:10px">
          <label style="font-size:11px; color:var(--text-muted); text-transform:uppercase">Filtrar Territorio:</label>
          <select id="selDrilldownDepto" class="ds-select" style="min-width:200px" onchange="changeDrilldownDepto()">
          </select>
        </div>
      </div>

      <!-- KPIS LOCALES DEL DEPARTAMENTO -->
      <div class="kpi-row" style="grid-template-columns: repeat(4, 1fr); margin-bottom: 16px;">
        <div class="kpi-box" style="background:var(--bg-card)">
          <div class="kpi-title">Contratos en el Territorio</div>
          <div class="kpi-num" id="ddKpiContratos">—</div>
          <div class="kpi-meta">Volumen total auditado</div>
        </div>
        <div class="kpi-box" style="background:var(--bg-card)">
          <div class="kpi-title">Monto Total Asignado</div>
          <div class="kpi-num" id="ddKpiMonto" style="color:var(--success); font-size:16px">—</div>
          <div class="kpi-meta">Pesos Colombianos (COP)</div>
        </div>
        <div class="kpi-box" style="background:var(--bg-card)">
          <div class="kpi-title">Promedio por Contrato</div>
          <div class="kpi-num" id="ddKpiPromedio" style="font-size:16px">—</div>
          <div class="kpi-meta">Pesos Colombianos (COP)</div>
        </div>
        <div class="kpi-box" style="background:var(--bg-card)">
          <div class="kpi-title">Mediana de Cuantía (P50)</div>
          <div class="kpi-num" id="ddKpiMediana" style="font-size:16px">—</div>
          <div class="kpi-meta">Pesos Colombianos (COP)</div>
        </div>
      </div>

      <!-- GRÁFICOS LOCALES: MUNICIPIOS Y ENTIDADES -->
      <div class="grid-equal">
        <div class="card-panel" style="background:var(--bg-card)">
          <div class="card-head">
            <div>
              <div class="card-heading" id="ddChartMunTitle">Top 10 Municipios</div>
              <div class="card-subtext">Monto contratado por municipio (MM COP)</div>
            </div>
          </div>
          <div class="chart-box-sm"><canvas id="chartDdMunicipios"></canvas></div>
        </div>
        <div class="card-panel" style="background:var(--bg-card)">
          <div class="card-head">
            <div>
              <div class="card-heading" id="ddChartEntTitle">Top 10 Entidades Compradoras</div>
              <div class="card-subtext">Principales instituciones estatales en el territorio</div>
            </div>
          </div>
          <div class="chart-box-sm"><canvas id="chartDdEntidades"></canvas></div>
        </div>
      </div>

      <!-- TABLA DETALLADA DE CONTRATOS EN EL DEPARTAMENTO -->
      <div class="card-panel" style="background:var(--bg-card); margin-top:16px">
        <div class="card-head">
          <div>
            <div class="card-heading" id="ddTableTitle">Principales Contratos Tecnológicos en el Territorio</div>
            <div class="card-subtext">Listado de mayor cuantía con trazabilidad completa</div>
          </div>
          <span class="badge-clean">Top 15 Mayor Cuantía</span>
        </div>
        <div class="table-responsive" style="max-height: 380px; overflow-y:auto">
          <table class="clean-tbl">
            <thead>
              <tr>
                <th>ID Contrato</th>
                <th>Entidad Compradora</th>
                <th>Municipio</th>
                <th>Proveedor / Contratista</th>
                <th class="num-cell">Valor Contrato (COP)</th>
                <th>Subsector</th>
                <th>Año</th>
                <th>Objeto del Proceso</th>
              </tr>
            </thead>
            <tbody id="ddContractsBody"></tbody>
          </table>
        </div>
      </div>
    </div>
  </section>

  <!-- TAB 3: TOTAL ESTADO VS TIC -->
  <section id="pane-comparativa" class="tab-content">
    <div class="card-panel" style="margin-bottom:16px">
      <div class="card-head">
        <div>
          <div class="card-heading">Contratación General del Estado Colombiano vs. Industria TIC</div>
          <div class="card-subtext">Comparación de magnitudes presupuestales anuales (2016 - 2025)</div>
        </div>
      </div>
      <div class="chart-box"><canvas id="chartComparativa"></canvas></div>
    </div>

    <div class="grid-2">
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Participación Relativa de la Industria TIC</div>
            <div class="card-subtext">Porcentaje respecto al presupuesto estatal general</div>
          </div>
        </div>
        <div class="chart-box-sm"><canvas id="chartShare"></canvas></div>
      </div>

      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Consolidado Multianual</div>
            <div class="card-subtext">Cifras consolidadas SECOP I y II</div>
          </div>
        </div>
        <div class="table-responsive">
          <table class="clean-tbl">
            <thead>
              <tr>
                <th>Año</th>
                <th class="num-cell">Total Estado (MM)</th>
                <th class="num-cell">Sector TIC (MM)</th>
                <th class="num-cell">% TIC</th>
                <th class="num-cell">Cttos Total</th>
                <th class="num-cell">Cttos TIC</th>
              </tr>
            </thead>
            <tbody id="cmpTableBody"></tbody>
          </table>
        </div>
      </div>
    </div>
  </section>

  <!-- TAB 4: COMPARADOR MULTIDIMENSIONAL (NUEVO MÓDULO POTENCIADO) -->
  <section id="pane-comparador" class="tab-content">
    <!-- PRESETS Y SELECTOR DE DIMENSIÓN -->
    <div class="compare-hero-box">
      <div class="card-head">
        <div>
          <div class="card-heading">Sistema de Comparación Multidimensional (A vs. B)</div>
          <div class="card-subtext">Contraste paramétrico directo entre departamentos, vigencias, tipos de contrato, entidades y proveedores</div>
        </div>
      </div>

      <!-- Presets Rápidos -->
      <div class="studio-pills-bar" style="margin-bottom:12px">
        <button class="studio-pill active" onclick="applyComparePreset('antioquia_valle')">Antioquia vs. Valle del Cauca</button>
        <button class="studio-pill" onclick="applyComparePreset('cundinamarca_antioquia')">Cundinamarca vs. Antioquia</button>
        <button class="studio-pill" onclick="applyComparePreset('anios_pandemia')">2019 (Pre-Pandemia) vs. 2024 (Actual)</button>
        <button class="studio-pill" onclick="applyComparePreset('servicios_compraventa')">Prestación de Servicios vs. Compraventa</button>
        <button class="studio-pill" onclick="applyComparePreset('mintic_sena')">MinTIC vs. SENA</button>
        <button class="studio-pill" onclick="applyComparePreset('soporte_conectividad')">Soporte TI vs. Conectividad</button>
      </div>

      <!-- Selector de Contraste -->
      <div class="compare-select-grid">
        <div class="filter-item">
          <label>Dimensión de Comparación</label>
          <select id="cmpDim" class="ds-select" onchange="onCompareDimChange()">
            <option value="departamento" selected>Departamentos Territoriales</option>
            <option value="anio">Años / Vigencias Fiscales</option>
            <option value="tipo_contrato">Tipos de Contrato</option>
            <option value="entidad">Entidades Compradoras</option>
            <option value="proveedor">Proveedores / Contratistas</option>
            <option value="subsector">Subsectores TIC</option>
          </select>
        </div>

        <div class="filter-item">
          <label id="lblItemA" style="color:#60A5FA; font-weight:700">Elemento A (Referencia)</label>
          <select id="cmpItemA" class="ds-select" style="min-width:220px"></select>
        </div>

        <div class="filter-item">
          <label id="lblItemB" style="color:var(--success); font-weight:700">Elemento B (Contraste)</label>
          <select id="cmpItemB" class="ds-select" style="min-width:220px"></select>
        </div>

        <div class="filter-item">
          <button class="btn-primary" onclick="runComparison()" style="padding:7px 18px">Ejecutar Comparación</button>
        </div>
      </div>
    </div>

    <!-- TARJETAS DE MÉTRICAS COMPARATIVAS LADO A LADO -->
    <div class="compare-kpi-grid">
      <!-- KPI 1: INVERSIÓN TOTAL -->
      <div class="compare-card">
        <div class="compare-card-title">Inversión Presupuestal Total</div>
        <div class="compare-values-row">
          <div class="val-block">
            <span class="val-tag-a" id="tagCmpTotA">A: ANTIOQUIA</span>
            <span class="val-num" id="valCmpTotA">$0</span>
          </div>
          <span class="delta-badge" id="deltaCmpTot">0%</span>
          <div class="val-block" style="text-align:right">
            <span class="val-tag-b" id="tagCmpTotB">B: VALLE</span>
            <span class="val-num" id="valCmpTotB">$0</span>
          </div>
        </div>
      </div>

      <!-- KPI 2: TOTAL CONTRATOS -->
      <div class="compare-card">
        <div class="compare-card-title">Volumen de Contratos</div>
        <div class="compare-values-row">
          <div class="val-block">
            <span class="val-tag-a" id="tagCmpCttA">A</span>
            <span class="val-num" id="valCmpCttA">0</span>
          </div>
          <span class="delta-badge" id="deltaCmpCtt">0%</span>
          <div class="val-block" style="text-align:right">
            <span class="val-tag-b" id="tagCmpCttB">B</span>
            <span class="val-num" id="valCmpCttB">0</span>
          </div>
        </div>
      </div>

      <!-- KPI 3: PROMEDIO POR PROCESO -->
      <div class="compare-card">
        <div class="compare-card-title">Cuantía Promedio por Proceso</div>
        <div class="compare-values-row">
          <div class="val-block">
            <span class="val-tag-a" id="tagCmpAvgA">A</span>
            <span class="val-num" id="valCmpAvgA">$0</span>
          </div>
          <span class="delta-badge" id="deltaCmpAvg">0%</span>
          <div class="val-block" style="text-align:right">
            <span class="val-tag-b" id="tagCmpAvgB">B</span>
            <span class="val-num" id="valCmpAvgB">$0</span>
          </div>
        </div>
      </div>

      <!-- KPI 4: MEDIANA (P50) -->
      <div class="compare-card">
        <div class="compare-card-title">Mediana de Cuantía (P50)</div>
        <div class="compare-values-row">
          <div class="val-block">
            <span class="val-tag-a" id="tagCmpMedA">A</span>
            <span class="val-num" id="valCmpMedA">$0</span>
          </div>
          <span class="delta-badge" id="deltaCmpMed">0%</span>
          <div class="val-block" style="text-align:right">
            <span class="val-tag-b" id="tagCmpMedB">B</span>
            <span class="val-num" id="valCmpMedB">$0</span>
          </div>
        </div>
      </div>

      <!-- KPI 5: PRÓRROGAS -->
      <div class="compare-card">
        <div class="compare-card-title">Prórroga Promedio (Días)</div>
        <div class="compare-values-row">
          <div class="val-block">
            <span class="val-tag-a" id="tagCmpDiasA">A</span>
            <span class="val-num" id="valCmpDiasA">0 d</span>
          </div>
          <span class="delta-badge" id="deltaCmpDias">0 d</span>
          <div class="val-block" style="text-align:right">
            <span class="val-tag-b" id="tagCmpDiasB">B</span>
            <span class="val-num" id="valCmpDiasB">0 d</span>
          </div>
        </div>
      </div>

      <!-- KPI 6: INNOVACIÓN CONPES 4069 -->
      <div class="compare-card">
        <div class="compare-card-title">Intensidad Innovación (CONPES)</div>
        <div class="compare-values-row">
          <div class="val-block">
            <span class="val-tag-a" id="tagCmpConpesA">A</span>
            <span class="val-num" id="valCmpConpesA">0%</span>
          </div>
          <span class="delta-badge" id="deltaCmpConpes">0 pp</span>
          <div class="val-block" style="text-align:right">
            <span class="val-tag-b" id="tagCmpConpesB">B</span>
            <span class="val-num" id="valCmpConpesB">0%</span>
          </div>
        </div>
      </div>
    </div>

    <!-- GRÁFICOS COMPARATIVOS (RADAR MULTIAXIAL + BARRAS PAREADAS) -->
    <div class="grid-equal" style="margin-bottom:16px">
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Perfil Multiaxial de Desempeño (Radar de Eficiencia)</div>
            <div class="card-subtext">5 dimensiones normalizadas (0 - 100) para contraste relativo</div>
          </div>
        </div>
        <div class="chart-box-radar"><canvas id="chartCmpRadar"></canvas></div>
      </div>

      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Distribución Presupuestal por Subsector TIC</div>
            <div class="card-subtext">Monto contratado (MM COP) comparado lado a lado</div>
          </div>
        </div>
        <div class="chart-box-radar"><canvas id="chartCmpBars"></canvas></div>
      </div>
    </div>

    <!-- TABLA DE DETALLE DE LA COMPARACIÓN -->
    <div class="card-panel">
      <div class="card-head">
        <div>
          <div class="card-heading">Matriz de Análisis Diferencial y Brecha</div>
          <div class="card-subtext">Desglose cuantitativo detallado con cálculo de variaciones netas</div>
        </div>
      </div>
      <div class="table-responsive">
        <table class="clean-tbl">
          <thead>
            <tr>
              <th>Dimensión / Métrica Estratégica</th>
              <th class="num-cell" id="thCmpA" style="color:#60A5FA">Elemento A</th>
              <th class="num-cell" id="thCmpB" style="color:var(--success)">Elemento B</th>
              <th class="num-cell">Diferencia Absoluta</th>
              <th class="num-cell">Variación Relativa (%)</th>
              <th>Diagnóstico del Segmento</th>
            </tr>
          </thead>
          <tbody id="cmpMatrixBody"></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- TAB 5: EXPLORADOR -->
  <section id="pane-explorador" class="tab-content">
    <div class="card-panel">
      <div class="card-head" style="flex-wrap:wrap; gap:12px; justify-content:space-between">
        <div style="display:flex; gap:10px; align-items:center; flex:1; min-width:280px">
          <input type="text" id="dtSearch" class="search-box" style="flex:1" placeholder="Buscar contratos TIC (base 213.123 + ingesta continua) por objeto, contratista, entidad, ID o departamento..." onkeydown="if(event.key==='Enter') searchContracts()">
          <button class="btn-primary" onclick="searchContracts()">Buscar</button>
          <button class="btn-secondary" onclick="clearContractSearch()">Limpiar</button>
        </div>
        <div style="display:flex; gap:12px; align-items:center">
          <span id="dtCount" style="font-size:12px; font-weight:600; color:var(--text-primary)">Cargando registros...</span>
          <div style="display:flex; align-items:center; gap:6px">
            <button class="btn-secondary" id="btnPrevPage" onclick="changePage(-1)" style="padding:4px 10px; font-size:12px">Anterior</button>
            <span id="pageIndicator" style="font-size:12px; color:var(--text-muted); font-family:var(--font-mono)">Pág 1 / 1</span>
            <button class="btn-secondary" id="btnNextPage" onclick="changePage(1)" style="padding:4px 10px; font-size:12px">Siguiente</button>
          </div>
          <select id="dtPageSize" class="ds-select" style="padding:4px 8px; font-size:12px" onchange="changePageSize()">
            <option value="50">50 por pág</option>
            <option value="100" selected>100 por pág</option>
            <option value="250">250 por pág</option>
            <option value="500">500 por pág</option>
          </select>
        </div>
      </div>
      <div class="table-responsive" style="max-height: 580px; overflow-y:auto">
        <table class="clean-tbl">
          <thead id="dtHeaders"></thead>
          <tbody id="dtRecords"></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- TAB 6: ESTUDIO ANALÍTICO MULTIDIMENSIONAL -->
  <section id="pane-consola" class="tab-content">
    <div class="card-panel" style="margin-bottom: 16px;">
      <div class="card-head">
        <div>
          <div class="card-heading">Estudio Analítico Multidimensional (OLAP Pivot)</div>
          <div class="card-subtext">Seleccione un reporte preconfigurado o construya una consulta personalizada combinando dimensiones y métricas</div>
        </div>
        <button class="btn-secondary" onclick="exportStudioCSV()" id="btnExportCSV">
          Descargar Datos (CSV)
        </button>
      </div>

      <div class="studio-pills-bar">
        <button class="studio-pill active" id="pill-top_proveedores" onclick="applyStudioPreset('top_proveedores')">Top Proveedores TIC</button>
        <button class="studio-pill" id="pill-top_entidades" onclick="applyStudioPreset('top_entidades')">Top Entidades Compradoras</button>
        <button class="studio-pill" id="pill-megacontratos" onclick="applyStudioPreset('megacontratos')">Megacontratos (&gt; $5.000M)</button>
        <button class="studio-pill" id="pill-prorrogas" onclick="applyStudioPreset('prorrogas')">Riesgo de Prórrogas (&gt;180d)</button>
        <button class="studio-pill" id="pill-tipos_contrato" onclick="applyStudioPreset('tipos_contrato')">Tipos de Contrato</button>
        <button class="studio-pill" id="pill-pyme" onclick="applyStudioPreset('pyme')">Segmento MiPYME vs Grande</button>
        <button class="studio-pill" id="pill-conpes" onclick="applyStudioPreset('conpes')">Iniciativas CONPES 4069 (I+D)</button>
        <button class="studio-pill" id="pill-modalidades" onclick="applyStudioPreset('modalidades')">Modalidades de Selección</button>
      </div>

      <div class="studio-builder-box">
        <div class="filter-item">
          <label>Agrupar por (Dimensión)</label>
          <select id="stDim" class="ds-select">
            <option value="proveedor" selected>Proveedor / Contratista</option>
            <option value="entidad">Entidad Compradora</option>
            <option value="departamento">Departamento</option>
            <option value="municipio">Municipio</option>
            <option value="subsector">Subsector TIC</option>
            <option value="tipo_contrato">Tipo de Contrato</option>
            <option value="modalidad">Modalidad de Selección</option>
            <option value="anio">Año de Contratación</option>
            <option value="pyme">Clasificación PyME</option>
          </select>
        </div>

        <div class="filter-item">
          <label>Métrica de Cálculo</label>
          <select id="stMetric" class="ds-select">
            <option value="sum_monto" selected>Monto Total ($ COP)</option>
            <option value="count_cttos">Número de Contratos (#)</option>
            <option value="avg_monto">Promedio por Contrato ($)</option>
            <option value="median_monto">Mediana de Cuantía ($)</option>
            <option value="avg_dias">Días de Prórroga Promedio</option>
          </select>
        </div>

        <div class="filter-item">
          <label>Filtrar Territorio</label>
          <select id="stDepto" class="ds-select">
            <option value="">Todo el País</option>
          </select>
        </div>

        <div class="filter-item">
          <label>Visualización</label>
          <select id="stChartType" class="ds-select">
            <option value="bar_h" selected>Barras Horizontales</option>
            <option value="bar_v">Barras Verticales</option>
            <option value="doughnut">Rosquilla (Participación)</option>
            <option value="line">Línea Temporal</option>
          </select>
        </div>

        <div class="filter-item">
          <label>Límite</label>
          <select id="stLimit" class="ds-select">
            <option value="10">Top 10</option>
            <option value="15" selected>Top 15</option>
            <option value="30">Top 30</option>
            <option value="50">Top 50</option>
            <option value="100">Top 100</option>
          </select>
        </div>

        <div class="filter-item">
          <button class="btn-primary" onclick="runStudioCustomQuery()" style="padding:7px 16px">Generar Análisis</button>
        </div>
      </div>
    </div>

    <!-- RESULTADOS DEL ANÁLISIS: GRAFICO + METRICAS + TABLA -->
    <div class="grid-2" style="margin-bottom:16px">
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading" id="studioChartTitle">Visualización del Reporte</div>
            <div class="card-subtext" id="studioChartSub">Distribución analítica generada automáticamente</div>
          </div>
        </div>
        <div class="chart-box-lg"><canvas id="chartStudio"></canvas></div>
      </div>

      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Métricas Clave del Reporte</div>
            <div class="card-subtext">Resumen cuantitativo del segmento consultado</div>
          </div>
        </div>
        <div style="display:grid; grid-template-columns: 1fr 1fr; gap:12px; height:180px; align-content:center">
          <div class="kpi-box" style="background:var(--bg-card); padding:12px">
            <div class="kpi-title">Registros Analizados</div>
            <div class="kpi-num" id="studioKpiRows" style="font-size:18px">—</div>
            <div class="kpi-meta">En este reporte</div>
          </div>
          <div class="kpi-box" style="background:var(--bg-card); padding:12px">
            <div class="kpi-title">Monto Representado</div>
            <div class="kpi-num" id="studioKpiMonto" style="font-size:18px; color:var(--success)">—</div>
            <div class="kpi-meta">Miles de Millones COP</div>
          </div>
          <div class="kpi-box" style="background:var(--bg-card); padding:12px">
            <div class="kpi-title">Líder del Segmento</div>
            <div class="kpi-num" id="studioKpiLeader" style="font-size:13px; font-family:var(--font-sans); white-space:nowrap; overflow:hidden; text-overflow:ellipsis">—</div>
            <div class="kpi-meta">Mayor volumen</div>
          </div>
          <div class="kpi-box" style="background:var(--bg-card); padding:12px">
            <div class="kpi-title">Tiempo de Consulta</div>
            <div class="kpi-num" id="studioKpiPerf" style="font-size:18px; color:#60A5FA">&lt; 5 ms</div>
            <div class="kpi-meta">Motor DuckDB</div>
          </div>
        </div>
      </div>
    </div>

    <!-- TABLA DE RESULTADOS -->
    <div class="card-panel">
      <div class="card-head">
        <div>
          <div class="card-heading" id="studioTableTitle">Detalle de Registros</div>
          <div class="card-subtext" id="studioTableSub">Datos tabulados con formato enriquecido</div>
        </div>
      </div>
      <div class="table-responsive" style="max-height: 440px; overflow-y:auto">
        <table class="clean-tbl">
          <thead id="studioTableHead"></thead>
          <tbody id="studioTableBody"></tbody>
        </table>
      </div>
    </div>

    <!-- MODO AVANZADO SQL -->
    <div class="accordion-header" onclick="toggleSqlAccordion()">
      <span>Modo Avanzado: Consola SQL Personalizada (Opcional)</span>
      <span id="accordionArrow">Mostrar Terminal SQL</span>
    </div>
    <div class="accordion-body" id="sqlAccordionBody">
      <div class="card-panel">
        <textarea id="sqlInput" class="sql-textarea">SELECT departamento, count(*) AS cttos, round(sum(valor_contrato)/1e9, 2) AS total_mm FROM datos GROUP BY 1 ORDER BY total_mm DESC LIMIT 10;</textarea>
        <div style="display:flex; justify-content:space-between; align-items:center">
          <button class="btn-primary" onclick="executeUserQuery()">Ejecutar SQL Personalizado</button>
          <span id="queryPerf" style="font-size:11px; color:var(--success)"></span>
        </div>
      </div>
    </div>
  </section>

  <!-- TAB 7: MACHINE LEARNING & CONPES 4069 -->
  <section id="pane-ml" class="tab-content">
    <div class="kpi-row" style="grid-template-columns: repeat(4, 1fr); margin-bottom: 16px;">
      <div class="kpi-box">
        <div class="kpi-title">Universo Clasificado (ML)</div>
        <div class="kpi-num" id="mlTotalCount">—</div>
        <div class="kpi-meta">Contratos con matriz de características</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Inversión CONPES 4069</div>
        <div class="kpi-num" id="mlMontoConpes" style="color:var(--success); font-size:16px">—</div>
        <div class="kpi-meta">Pesos Colombianos en I+D / 4IR</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Proporción Innovación</div>
        <div class="kpi-num" id="mlPctConpes">—</div>
        <div class="kpi-meta">Del total de procesos TIC</div>
      </div>
      <div class="kpi-box">
        <div class="kpi-title">Modelo Predictivo</div>
        <div class="kpi-num" style="font-size:16px; color:#60A5FA">K-Means & IsoForest</div>
        <div class="kpi-meta">Clustering + Detección de Anomalías</div>
      </div>
    </div>

    <!-- 1. CLUSTERS DETALLADOS (K-MEANS) -->
    <div class="card-panel" style="margin-bottom:16px">
      <div class="card-head">
        <div>
          <div class="card-heading">Segmentación de Contratación por Perfiles (K-Means Clustering, k=4)</div>
          <div class="card-subtext">Agrupación no supervisada basada en cuantía, duración, prórrogas y componente de innovación</div>
        </div>
        <span class="badge-clean">Scikit-Learn</span>
      </div>
      <div class="table-responsive">
        <table class="clean-tbl">
          <thead>
            <tr>
              <th>Cluster</th>
              <th>Perfil Operativo / Estratégico</th>
              <th class="num-cell">Contratos</th>
              <th class="num-cell">Monto Total (COP)</th>
              <th class="num-cell">Promedio por Contrato</th>
              <th class="num-cell">Prórroga Promedio (Días)</th>
              <th class="num-cell">% Innovación (CONPES)</th>
            </tr>
          </thead>
          <tbody id="mlClustersBody"></tbody>
        </table>
      </div>
    </div>

    <!-- 2. ANOMALÍAS DETECTADAS (ISOLATION FOREST) -->
    <div class="card-panel" style="margin-bottom:16px">
      <div class="card-head">
        <div>
          <div class="card-heading">Top Anomalías Detectadas (Isolation Forest)</div>
          <div class="card-subtext">Procesos con desviaciones multivariadas extremas en cuantía, prórrogas o modalidad</div>
        </div>
        <span class="badge-clean" style="color:var(--warning); background:rgba(245,158,11,0.12)">Score Anomalía s(x)</span>
      </div>
      <div class="table-responsive" style="max-height: 400px; overflow-y:auto">
        <table class="clean-tbl">
          <thead>
            <tr>
              <th>ID Contrato</th>
              <th>Subsector TIC</th>
              <th>Departamento</th>
              <th>Modalidad</th>
              <th class="num-cell">Valor Contrato (COP)</th>
              <th class="num-cell">Días Adicionados</th>
              <th class="num-cell">Score Anomalía</th>
            </tr>
          </thead>
          <tbody id="mlAnomaliasBody"></tbody>
        </table>
      </div>
    </div>

    <!-- 3. GRÁFICOS ML: PROYECCIÓN PREDICTIVA & DISTRIBUCIÓN DE CLUSTERS -->
    <div class="grid-2" style="margin-bottom: 16px;">
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Proyección Predictiva de Inversión TIC (2016-2025 Real vs. 2026-2027 Pronóstico)</div>
            <div class="card-subtext">Modelo supervisado de tendencia multianual en miles de millones (MM COP)</div>
          </div>
          <span class="badge-clean">Pronóstico Supervisado</span>
        </div>
        <div class="chart-box-sm"><canvas id="chartMlProyeccion"></canvas></div>
        <div class="table-responsive" style="margin-top:12px">
          <table class="clean-tbl">
            <thead>
              <tr>
                <th>Año estimado</th>
                <th class="num-cell">Monto proyectado (MM COP)</th>
                <th class="num-cell">Cambio frente al año anterior</th>
              </tr>
            </thead>
            <tbody id="mlForecastBody"></tbody>
          </table>
        </div>
      </div>

      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Distribución Presupuestal por Perfil (K-Means)</div>
            <div class="card-subtext">Participación porcentual en el monto total contratado</div>
          </div>
        </div>
        <div class="chart-box-sm"><canvas id="chartMlClusters"></canvas></div>
      </div>
    </div>

    <!-- 4. MATRIZ DE CORRELACIÓN DE PEARSON & DESGLOSE CONPES -->
    <div class="grid-2" style="margin-bottom: 16px;">
      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Matriz de Correlación Lineal de Pearson</div>
            <div class="card-subtext">Grado de asociación estadística (r) entre variables clave del pipeline</div>
          </div>
          <span class="badge-clean">Estadística Multivariada</span>
        </div>
        <div class="table-responsive">
          <table class="clean-tbl">
            <thead>
              <tr>
                <th>Variable</th>
                <th class="num-cell">Log(Cuantía)</th>
                <th class="num-cell">Días Prórroga</th>
                <th class="num-cell">Flag Prórroga</th>
                <th class="num-cell">Innovación (CONPES)</th>
              </tr>
            </thead>
            <tbody id="mlCorrBody"></tbody>
          </table>
        </div>
      </div>

      <div class="card-panel">
        <div class="card-head">
          <div>
            <div class="card-heading">Inversión CONPES 4069 por Subsector Tecnológico</div>
            <div class="card-subtext">Asignación presupuestal en proyectos con componentes I+D y 4IR</div>
          </div>
        </div>
        <div class="chart-box-sm"><canvas id="chartMlConpesSub"></canvas></div>
      </div>
    </div>
  </section>

  <!-- TAB 8: GENERADOR DE REPORTES (NUEVO MÓDULO OFICIAL) -->
  <section id="pane-reportes" class="tab-content">
    <!-- CONFIGURADOR DEL REPORTE -->
    <div class="card-panel" style="margin-bottom:16px">
      <div class="card-head">
        <div>
          <div class="card-heading">Generador Oficial de Reportes Ejecutivos e Informes Técnicos</div>
          <div class="card-subtext">Producción automatizada de informes en PDF con sellos de auditoría, tablas y metadatos</div>
        </div>
      </div>

      <div class="studio-builder-box" style="margin-bottom:12px">
        <div class="filter-item">
          <label>Tipo de Informe</label>
          <select id="repTipo" class="ds-select">
            <option value="territorial" selected>Informe Ejecutivo Territorial (Departamental)</option>
            <option value="riesgos">Auditoría de Riesgos, Prórrogas y Anomalías</option>
            <option value="mercado">Estudio de Concentración de Mercado (HHI & Proveedores)</option>
            <option value="conpes">Balance de Innovación Digital (CONPES 4069)</option>
          </select>
        </div>

        <div class="filter-item">
          <label>Territorio / Departamento</label>
          <select id="repDepto" class="ds-select">
            <option value="">Consolidado Nacional (32 Deptos)</option>
          </select>
        </div>

        <div class="filter-item">
          <label>Año Inicial</label>
          <select id="repAnioDesde" class="ds-select">
            <option value="">2015 (Inicio Histórico)</option>
            <option value="2018">2018</option>
            <option value="2020">2020</option>
            <option value="2022">2022</option>
          </select>
        </div>

        <div class="filter-item">
          <label>Año Final</label>
          <select id="repAnioHasta" class="ds-select">
            <option value="">2025 (Cierre Vigente)</option>
            <option value="2024">2024</option>
            <option value="2023">2023</option>
          </select>
        </div>

        <div class="filter-item" style="display:flex; gap:8px">
          <button class="btn-primary" onclick="loadReportPreview()" style="padding:7px 14px">Generar Vista Previa</button>
          <button class="btn-secondary" onclick="downloadOfficialPDF()" style="padding:7px 14px; background:#2563EB; color:#fff; border-color:#2563EB">Descargar PDF Oficial</button>
          <button class="btn-secondary" onclick="window.print()" style="padding:7px 12px">Imprimir</button>
        </div>
      </div>
    </div>

    <!-- VISTA PREVIA DEL REPORTE (HOJA MEMBRETADA) -->
    <div class="report-paper-container" id="reportPreviewArea">
      <div class="report-paper-header">
        <div>
          <span style="font-size:11px; font-weight:700; color:#3B82F6; letter-spacing:0.05em; text-transform:uppercase">Observatorio de Contratación Pública en Tecnologías de la Información</span>
          <h2 style="font-size:18px; font-weight:700; margin-top:4px" id="repDocTitle">Informe Ejecutivo de Desempeño Territorial</h2>
          <p style="font-size:12px; color:var(--text-secondary); margin-top:2px" id="repDocSub">Consolidado Nacional | Vigencia 2015 - 2025</p>
        </div>
        <div style="text-align:right">
          <span class="badge-highlight">Auditoría Oficial DuckDB</span>
          <div style="font-size:11px; color:var(--text-muted); margin-top:6px" id="repDocFecha">Fecha de emisión: Septiembre 2026</div>
        </div>
      </div>

      <div class="report-meta-grid">
        <div>
          <div class="kpi-title">Universo Auditado</div>
          <div style="font-size:16px; font-weight:700; font-family:var(--font-mono)" id="repKpiCttos">—</div>
        </div>
        <div>
          <div class="kpi-title">Inversión Total (COP)</div>
          <div style="font-size:16px; font-weight:700; color:var(--success); font-family:var(--font-mono)" id="repKpiMonto">—</div>
        </div>
        <div>
          <div class="kpi-title">Promedio por Proceso</div>
          <div style="font-size:16px; font-weight:700; font-family:var(--font-mono)" id="repKpiProm">—</div>
        </div>
        <div>
          <div class="kpi-title">Mediana (P50)</div>
          <div style="font-size:16px; font-weight:700; font-family:var(--font-mono)" id="repKpiMed">—</div>
        </div>
      </div>

      <div style="margin-bottom:20px; line-height:1.6; font-size:13px; color:#E2E8F0" id="repDocNarrative">
        Cargando síntesis del informe ejecutivo...
      </div>

      <!-- TABLA 1 REPORTE: SUBSECTORES -->
      <div style="margin-bottom:20px">
        <h3 style="font-size:13px; font-weight:700; margin-bottom:8px; color:#93C5FD">1. Desglose por Subsector Tecnológico</h3>
        <table class="clean-tbl">
          <thead>
            <tr>
              <th>Subsector TIC</th>
              <th class="num-cell">Contratos</th>
              <th class="num-cell">Inversión Total (COP)</th>
              <th class="num-cell">Promedio Proceso</th>
            </tr>
          </thead>
          <tbody id="repTableSub"></tbody>
        </table>
      </div>

      <!-- TABLA 2 REPORTE: ENTIDADES -->
      <div style="margin-bottom:20px">
        <h3 style="font-size:13px; font-weight:700; margin-bottom:8px; color:#93C5FD">2. Principales Entidades Compradoras</h3>
        <table class="clean-tbl">
          <thead>
            <tr>
              <th>Entidad Compradora</th>
              <th class="num-cell">Contratos</th>
              <th class="num-cell">Inversión Total (COP)</th>
            </tr>
          </thead>
          <tbody id="repTableEnt"></tbody>
        </table>
      </div>

      <!-- FIRMAS DE AUDITORÍA -->
      <div class="report-sign-grid">
        <div style="text-align:center">
          <div class="report-sign-line"></div>
          <div style="font-size:11px; font-weight:700; color:#CBD5E1">EQUIPO TÉCNICO DE AUDITORÍA TIC</div>
          <div style="font-size:10px; color:#64748B">Observatorio de Contratación Pública</div>
        </div>
        <div style="text-align:center">
          <div class="report-sign-line"></div>
          <div style="font-size:11px; font-weight:700; color:#CBD5E1">DIRECCIÓN DE INTELIGENCIA DE DATOS</div>
          <div style="font-size:10px; color:#64748B">Validación Columnar DuckDB Engine</div>
        </div>
      </div>
    </div>
  </section>

  <!-- TAB 9: AUDITORÍA Y VERIFICACIÓN -->
  <section id="pane-auditoria" class="tab-content">
    <div style="display:flex; flex-direction:column; gap:16px;">
      
      <!-- Fila 1: Integridad y Arquitectura Técnica -->
      <div class="grid-2">
        <div class="card-panel">
          <div class="card-head">
            <span class="card-heading">Integridad de Archivos Parquet Reales</span>
            <span class="badge-clean">100% Verificado</span>
          </div>
          <table class="clean-tbl">
            <tbody>
              <tr><td>SECOP I — Subconjunto TIC (API f789-7hwg)</td><td class="num-cell">108,700 contratos | $9.599,93 MM COP</td></tr>
              <tr><td>SECOP II — Subconjunto TIC (API jbjy-vk9h)</td><td class="num-cell">104,423 contratos | $30.571,03 MM COP</td></tr>
              <tr><td>Línea Base Nacional Consolidada (2016-2025)</td><td class="num-cell" style="color:var(--accent)">3,100,000+ contratos | $1.488.890 MM COP</td></tr>
              <tr><td>Industria TIC Consolidada (base + ingesta continua)</td><td class="num-cell" style="color:var(--success); font-weight:700" id="auditTicTotal">213,123 contratos | base histórica + SODA</td></tr>
            </tbody>
          </table>
        </div>

        <div class="card-panel">
          <div class="card-head">
            <span class="card-heading">Herramientas Técnicas vs. Conceptos del Reto</span>
            <span class="badge-clean">Stack Enterprise</span>
          </div>
          <table class="clean-tbl">
            <tbody>
              <tr><td>Python 3.14 & DuckDB In-Memory</td><td>Modelado OLAP columnar y álgebra relacional en &lt; 10 ms</td></tr>
              <tr><td>PyArrow & Parquet ZSTD</td><td>Streaming desacoplado, compresión binaria sin CSV pesado</td></tr>
              <tr><td>Scikit-Learn (K-Means & IsoForest)</td><td>Clustering no supervisado y detección de anomalías s(x)</td></tr>
              <tr><td>ReportLab & Chart.js</td><td>Generación de reportes PDF oficiales y visualización multiaxial</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Fila 2: Sanidad y Estadísticos Descriptivos de Valores Financieros -->
      <div class="grid-2">
        <div class="card-panel">
          <div class="card-head">
            <span class="card-heading">Distribución y Sanidad Estadística de Cuantías (COP)</span>
            <span style="font-size:11px; color:#64748B;">Métricas Robustas (Sin Valores Negativos)</span>
          </div>
          <table class="clean-tbl">
            <tbody>
              <tr><td>Valor Mínimo Registrado</td><td class="num-cell">$1.00 COP (Sin valores negativos ni ceros espurios)</td></tr>
              <tr><td>Percentil 25 (P25)</td><td class="num-cell">$15.840.000 COP</td></tr>
              <tr><td>Mediana Real (Percentil 50 - P50)</td><td class="num-cell" style="color:var(--success); font-weight:700;">$42.000.000 COP</td></tr>
              <tr><td>Percentil 75 (P75)</td><td class="num-cell">$95.500.000 COP</td></tr>
              <tr><td>Percentil 99 (P99 - Umbral Megacontratos)</td><td class="num-cell" style="color:var(--warning); font-weight:700;">$1.850.000.000 COP</td></tr>
              <tr><td>Promedio Aritmético</td><td class="num-cell">$137.600.000 COP (Influenciado por outliers)</td></tr>
              <tr><td>Valor Máximo Adjudicado</td><td class="num-cell" style="color:var(--accent); font-weight:700;">$1.09 Billones COP (Conectividad Escolar Nacional)</td></tr>
            </tbody>
          </table>
        </div>

        <div class="card-panel">
          <div class="card-head">
            <span class="card-heading">Auditoría de Calidad, Nulos y Consistencia de Esquema</span>
            <span style="font-size:11px; color:#64748B;" id="auditValidacionLabel">Validación sobre la base histórica 213,123 + ingesta</span>
          </div>
          <table class="clean-tbl">
            <tbody>
              <tr><td>Identificador Global (id_contrato_global)</td><td class="num-cell" style="color:var(--success)">0 nulos (100% Llave Primaria Única)</td></tr>
              <tr><td>Valor del Contrato (valor_contrato)</td><td class="num-cell" style="color:var(--success)">0 nulos (100% Cuantías Válidas &gt; 0)</td></tr>
              <tr><td>Año de Adjudicación (anio_contratacion)</td><td class="num-cell" style="color:var(--success)">0 nulos (Cobertura Continua 2015-2025)</td></tr>
              <tr><td>Jurisdicción Territorial (departamento)</td><td class="num-cell" style="color:var(--success)">0 nulos (Normalizado a 32 Departamentos DANE)</td></tr>
              <tr><td>Modalidad de Selección (modalidad)</td><td class="num-cell" style="color:var(--success)">0 nulos (100% Clasificado por Estatuto 80)</td></tr>
              <tr><td>Taxonomía Tecnológica (subsector_tic)</td><td class="num-cell" style="color:var(--success)">0 nulos (100% Mapeado a las 5 Categorías TIC)</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Fila 3: Clasificación Taxonómica de Campos API y Protocolo de Verificación -->
      <div class="grid-2">
        <div class="card-panel">
          <div class="card-head">
            <span class="card-heading">Estructura Taxonómica de las APIs Oficiales (85 Campos)</span>
            <span style="font-size:11px; color:#64748B;">datos.gov.co</span>
          </div>
          <table class="clean-tbl">
            <thead>
              <tr><th>Categoría del Modelo</th><th>Campos Clave Homologados</th><th>Propósito Analítico</th></tr>
            </thead>
            <tbody>
              <tr><td>Identificación y Enlace</td><td>id_contrato, proceso_compra, url_proceso</td><td>Llaves primarias y trazabilidad oficial</td></tr>
              <tr><td>Entidad Compradora</td><td>nombre_entidad, nit_entidad, orden_gobierno</td><td>Análisis de demanda institucional</td></tr>
              <tr><td>Ubicación Geográfica</td><td>departamento, municipio, codigo_dane</td><td>Distribución territorial y mapas de calor</td></tr>
              <tr><td>Valores y Presupuestos</td><td>valor_contrato, cuantia_contrato, adicion_cop</td><td>Volumetría financiera y dispersión</td></tr>
              <tr><td>Cronograma y Plazos</td><td>fecha_firma, dias_adicionados, plazo_meses</td><td>Detección de prórrogas e índices de atraso</td></tr>
              <tr><td>Clasificación Económica</td><td>codigo_ciiu, codigo_unspsc, tipo_contrato</td><td>Filtro industria TIC y CONPES 4069</td></tr>
            </tbody>
          </table>
        </div>

        <div class="card-panel">
          <div class="card-head">
            <span class="card-heading">Protocolo de Verificación Cruzada en Vivo (APIs Oficiales)</span>
            <span style="font-size:11px; color:#64748B;">Autenticidad de Fuentes</span>
          </div>
          <div style="font-size:11.5px; color:#CBD5E1; line-height:1.55; display:flex; flex-direction:column; gap:10px;">
            <div style="background:#0F172A; border:1px solid #1E293B; padding:10px 12px; border-radius:4px;">
              <div style="font-weight:700; color:#38BDF8; margin-bottom:2px;">1. API SECOP I (Dataset Endpoint: f789-7hwg)</div>
              <div>Validación directa por identificador <code>uid</code> en datos.gov.co. Compara cuantía adjudicada, nombre de entidad y objeto contractual sin discrepancias numéricas.</div>
            </div>
            <div style="background:#0F172A; border:1px solid #1E293B; padding:10px 12px; border-radius:4px;">
              <div style="font-weight:700; color:#38BDF8; margin-bottom:2px;">2. API SECOP II (Dataset Endpoint: jbjy-vk9h)</div>
              <div>Validación en tiempo real por campo <code>id_contrato</code>. Cruce automático de valor de contrato, contratista adjudicado y descripción del proceso.</div>
            </div>
            <div style="background:#0F172A; border:1px solid #1E293B; padding:10px 12px; border-radius:4px;">
              <div style="font-weight:700; color:#10B981; margin-bottom:2px;">3. Reproducibilidad de Auditoría</div>
              <div>Ejecución del script en terminal: <code>python auditoria_integridad_datos.py</code> y <code>python verificacion_cruzada_api_real.py</code>.</div>
            </div>
          </div>
        </div>
      </div>

    </div>
  </section>

  <!-- TAB 10: MOTOR SEMÁNTICO CON LLM (USBMed) -->
  <section id="pane-semantico" class="tab-content">
    <div style="display:flex; flex-direction:column; gap:20px;">

      <!-- Header del Modulo -->
      <div style="background:#111827; border:1px solid #1E293B; border-left:4px solid #0284C7; border-radius:6px; padding:18px 22px;">
        <div style="display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:16px;">
          <div>
            <div style="display:flex; align-items:center; gap:10px; margin-bottom:6px;">
              <span style="background:#0369A1; color:#F0F9FF; font-size:11px; font-weight:700; padding:3px 9px; border-radius:3px; letter-spacing:0.04em; text-transform:uppercase;">Evaluación Técnica Especializada</span>
              <span style="font-size:12px; color:#38BDF8; font-weight:600;">Universidad de San Buenaventura Medellín</span>
            </div>
            <h2 style="font-size:18px; font-weight:700; color:#F8FAFC; margin-bottom:6px; letter-spacing:-0.01em;">
              Análisis Semántico con LLM vs. Perfil de Capacidades Institucionales
            </h2>
            <p style="font-size:12px; color:#94A3B8; max-width:950px; line-height:1.5;">
              Contrasta cualitativamente los objetos contractuales de SECOP frente a los programas de la Facultad de Ingenierías (Ingeniería de Datos y Software, Sistemas Cibernéticos, Multimedia, Sonido, Industrial, Ambiental), el Grupo de Investigación en Modelamiento y Simulación Computacional (GIMSC, Categoría A1 MinCiencias), Geoinformática y el Consultorio Tecnológico Bonaventuriano (CTB).
            </p>
          </div>
          <div style="background:#0B0F19; border:1px solid #1E293B; padding:8px 14px; border-radius:4px; text-align:right;">
            <div style="font-size:10px; text-transform:uppercase; color:#64748B; font-weight:600; letter-spacing:0.04em;">Sedes Académicas</div>
            <div style="font-size:12px; font-weight:600; color:#E2E8F0; margin-top:2px;">Campus San Benito & Bello</div>
          </div>
        </div>
      </div>

      <!-- Barra de Casos Predefinidos y Buscador DuckDB -->
      <div class="card-panel">
        <div style="display:grid; grid-template-columns: 1.1fr 1fr; gap:16px; align-items:center;">
          <div>
            <div style="font-size:11px; font-weight:600; color:#64748B; text-transform:uppercase; letter-spacing:0.03em; margin-bottom:6px;">
              Casos Preconfigurados de Convocatorias Regionales
            </div>
            <select id="selPresetSemantico" class="filter-input" onchange="cambiarPresetSemantico()" style="width:100%; font-size:12px; padding:7px 10px;">
              <option value="preset_ia">Caso 1: Plataforma Inteligente, Analítica y ML (Alcaldía de Medellín - Ing. de Datos y Software / GIMSC)</option>
              <option value="preset_cloud">Caso 2: Arquitectura Cloud, Microservicios y Portales (Gobernación de Antioquia - GIMSC Software)</option>
              <option value="preset_multimedia">Caso 3: Entornos Virtuales, UX y Simulación (Ruta N - Ing. Multimedia / GIMSC)</option>
              <option value="preset_ciber">Caso 4: Sistemas Cibernéticos, IoT y Automatización (Metro de Medellín - GIMSC)</option>
              <option value="preset_hardware">Caso 5: Suministro de Hardware y Obra Civil (Alerta de Brecha de Alcance)</option>
            </select>
          </div>
          <div>
            <div style="font-size:11px; font-weight:600; color:#64748B; text-transform:uppercase; letter-spacing:0.03em; margin-bottom:6px;">
              Búsqueda en Dataset Real de DuckDB
            </div>
            <div style="display:flex; gap:8px;">
              <input type="text" id="inputDuckSearch" class="filter-input" placeholder="Término o entidad (ej. Software, Medellín, IA, Analítica)..." style="flex:1; font-size:12px; padding:7px 10px;">
              <button class="btn-secondary" onclick="buscarContratosEnDuckDB()" style="font-size:11.5px; padding:7px 14px; white-space:nowrap;">Buscar y Cargar</button>
            </div>
            <div id="duckSearchResultCount" style="font-size:11px; color:#94A3B8; margin-top:4px;"></div>
          </div>
        </div>
      </div>

      <!-- Formulario de Evaluación y Cuadro Institucional -->
      <div style="display:grid; grid-template-columns: 1.15fr 0.85fr; gap:20px;">
        
        <!-- Panel Izquierdo: Entrada de Datos -->
        <div class="card-panel">
          <div class="card-head" style="margin-bottom:12px; border-bottom:1px solid #1E293B; padding-bottom:8px;">
            <span class="card-heading">Parámetros del Contrato a Evaluar</span>
            <span style="font-size:11px; color:#64748B;">SECOP I y II</span>
          </div>

          <div style="display:grid; grid-template-columns: 1fr 1fr; gap:12px; margin-bottom:12px;">
            <div class="filter-item">
              <label style="color:#94A3B8; font-size:10.5px;">Identificador del Proceso</label>
              <input type="text" id="semContratoId" class="filter-input" value="CO1.PCONT.4589211">
            </div>
            <div class="filter-item">
              <label style="color:#94A3B8; font-size:10.5px;">Modalidad de Contratación</label>
              <input type="text" id="semModalidad" class="filter-input" value="Licitación Pública">
            </div>
          </div>

          <div class="filter-item" style="margin-bottom:12px;">
            <label style="color:#94A3B8; font-size:10.5px;">Entidad Contratante</label>
            <input type="text" id="semEntidad" class="filter-input" value="ALCALDÍA DE MEDELLÍN - SECRETARÍA DE INNOVACIÓN DIGITAL">
          </div>

          <div class="filter-item" style="margin-bottom:12px;">
            <label style="color:#94A3B8; font-size:10.5px;">Presupuesto Oficial (COP)</label>
            <input type="number" id="semValorCop" class="filter-input" value="1850000000">
          </div>

          <div class="filter-item" style="margin-bottom:14px;">
            <label style="color:#94A3B8; font-size:10.5px;">Objeto Contractual</label>
            <textarea id="semObjeto" class="filter-input" rows="4" style="font-size:12px; line-height:1.45; resize:vertical;">DESARROLLO E IMPLEMENTACIÓN DE UNA PLATAFORMA DIGITAL INTELIGENTE CON MODELOS DE ANALÍTICA PREDICTIVA, MACHINE LEARNING Y CIENCIA DE DATOS PARA LA TOMA DE DECISIONES TERRITORIALES.</textarea>
          </div>

          <div style="background:#0F172A; border:1px solid #1E293B; border-radius:4px; padding:10px 12px; margin-bottom:14px;">
            <div style="font-size:11px; font-weight:600; color:#38BDF8; margin-bottom:3px;">Configuración de Conectividad LLM</div>
            <div style="font-size:11px; color:#94A3B8; margin-bottom:8px; line-height:1.4;">
              El análisis opera de manera autónoma con el <strong>Motor Analítico de Inteligencia Contractual USBMed (NLP Local)</strong>. Opcionalmente puede ingresar una API Key de OpenAI para síntesis extendida.
            </div>
            <input type="password" id="semApiKey" class="filter-input" placeholder="API Key OpenAI (opcional)..." style="width:100%; font-size:11.5px;">
          </div>

          <button id="btnEjecutarSemantico" class="btn-primary" onclick="ejecutarAnalisisSemantico()" style="width:100%; padding:10px; font-size:12.5px; font-weight:600; letter-spacing:0.02em;">
            Ejecutar Evaluación Semántica y Dictamen Institucional
          </button>
        </div>

        <!-- Panel Derecho: Perfil Institucional USBMed -->
        <div class="card-panel" style="display:flex; flex-direction:column; justify-content:space-between;">
          <div>
            <div class="card-head" style="margin-bottom:12px; border-bottom:1px solid #1E293B; padding-bottom:8px;">
              <span class="card-heading">Perfil de Capacidades USBMed</span>
              <span style="font-size:11px; color:#0284C7; font-weight:600;">Facultad de Ingenierías</span>
            </div>

            <div style="margin-bottom:12px;">
              <div style="font-size:10px; font-weight:700; color:#64748B; text-transform:uppercase; letter-spacing:0.04em; margin-bottom:5px;">Sedes Universitarias</div>
              <div style="display:flex; gap:6px; flex-wrap:wrap;">
                <span style="background:#1E293B; color:#CBD5E1; border:1px solid #334155; padding:3px 8px; border-radius:3px; font-size:11px;">Campus San Benito (Medellín)</span>
                <span style="background:#1E293B; color:#CBD5E1; border:1px solid #334155; padding:3px 8px; border-radius:3px; font-size:11px;">Campus Universitario de Bello</span>
              </div>
            </div>

            <div style="margin-bottom:12px;">
              <div style="font-size:10px; font-weight:700; color:#64748B; text-transform:uppercase; letter-spacing:0.04em; margin-bottom:5px;">Programas de Pregrado Acreditados</div>
              <div style="font-size:11.5px; color:#CBD5E1; line-height:1.45; background:#0F172A; border:1px solid #1E293B; padding:6px 10px; border-radius:4px;">
                Ingeniería de Datos y Software • Ingeniería de Sistemas Cibernéticos • Ingeniería Multimedia • Ingeniería de Sonido • Ingeniería Industrial • Ingeniería Ambiental
              </div>
            </div>

            <div style="margin-bottom:12px;">
              <div style="font-size:10px; font-weight:700; color:#64748B; text-transform:uppercase; letter-spacing:0.04em; margin-bottom:5px;">Investigación MinCiencias (GIMSC A1)</div>
              <div style="display:flex; flex-direction:column; gap:4px; font-size:11.5px;">
                <div style="background:#0F172A; border:1px solid #1E293B; padding:5px 8px; border-radius:4px;">
                  <strong style="color:#38BDF8;">GIMSC:</strong> <span style="color:#CBD5E1;">Grupo de Investigación en Modelamiento y Simulación Computacional (MinCiencias A1)</span>
                </div>
                <div style="background:#0F172A; border:1px solid #1E293B; padding:5px 8px; border-radius:4px;">
                  <strong style="color:#38BDF8;">Geoinformática:</strong> <span style="color:#CBD5E1;">Grupo de Geoinformática Aplicada (Gestión Territorial y SIG)</span>
                </div>
              </div>
            </div>

            <div>
              <div style="font-size:10px; font-weight:700; color:#64748B; text-transform:uppercase; letter-spacing:0.04em; margin-bottom:5px;">Unidad de Extensión y Transferencia</div>
              <div style="background:#0F172A; border:1px solid #1E293B; border-left:3px solid #10B981; padding:8px 10px; border-radius:4px; font-size:11.5px; color:#CBD5E1; line-height:1.45;">
                <strong style="color:#10B981;">Consultorio Tecnológico Bonaventuriano (CTB):</strong> Más de 14 años conectando la academia con el Estado y la industria en desarrollo de software, ciencia de datos, acústica, multimedia y proyectos de innovación pública.
              </div>
            </div>
          </div>

          <div style="margin-top:12px; padding-top:8px; border-top:1px solid #1E293B; font-size:10.5px; color:#64748B;">
            Fuente oficial: usbmed.edu.co • data/perfil_universidad.json (Exclusivo capacidades técnicas sin NIT).
          </div>
        </div>

      </div>

      <!-- Panel de Resultados del Análisis Semántico -->
      <div id="semResultPanel" style="display:none;">
        <div class="card-panel" style="border:1px solid #0284C7;">
          
          <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #1E293B; padding-bottom:12px; margin-bottom:16px; flex-wrap:wrap; gap:12px;">
            <div>
              <div style="font-size:10px; text-transform:uppercase; color:#64748B; font-weight:700; letter-spacing:0.04em;">Dictamen de Idoneidad Técnica</div>
              <div id="semConclusion" style="font-size:18px; font-weight:800; margin-top:2px;">—</div>
            </div>
            <div style="display:flex; align-items:center; gap:20px;">
              <div style="text-align:right;">
                <div style="font-size:10px; text-transform:uppercase; color:#64748B; font-weight:600;">Afinidad Semántica</div>
                <div id="semScoreGlobal" style="font-size:24px; font-weight:800; font-family:var(--font-mono); color:#10B981;">0%</div>
              </div>
              <div style="text-align:right; border-left:1px solid #1E293B; padding-left:16px;">
                <div style="font-size:10px; text-transform:uppercase; color:#64748B; font-weight:600;">Motor Ejecutor</div>
                <div id="semEngineTag" style="font-size:11.5px; font-weight:600; color:#FCD34D;">NLP USBMed</div>
              </div>
            </div>
          </div>

          <!-- Alerta de Brechas -->
          <div id="semBrechasAlert" style="display:none; margin-bottom:16px; background:#1C1917; border:1px solid #78350F; border-left:4px solid #D97706; border-radius:4px; padding:10px 14px;"></div>

          <!-- Cuadrícula de Hallazgos y Dictamen Estratégico -->
          <div style="display:grid; grid-template-columns: 1fr 1fr; gap:18px;">
            
            <!-- Columna 1: Desglose Técnico -->
            <div style="background:#0B0F19; border:1px solid #1E293B; border-radius:4px; padding:14px;">
              <div style="font-size:11px; font-weight:700; color:#64748B; text-transform:uppercase; letter-spacing:0.04em; margin-bottom:6px;">Área Tecnológica Principal Identificada</div>
              <div id="semAreaPrincipal" style="font-size:12.5px; font-weight:600; color:#38BDF8; margin-bottom:14px; background:#0F172A; padding:7px 10px; border-radius:3px; border:1px solid #1E293B;">—</div>

              <div style="font-size:11px; font-weight:700; color:#64748B; text-transform:uppercase; letter-spacing:0.04em; margin-bottom:6px;">Grupos de Investigación Relacionados</div>
              <div id="semGruposList" style="display:flex; flex-direction:column; gap:4px; margin-bottom:14px;"></div>

              <div style="font-size:11px; font-weight:700; color:#64748B; text-transform:uppercase; letter-spacing:0.04em; margin-bottom:6px;">Laboratorios y Unidades Especializadas</div>
              <div id="semLabsList" style="display:flex; flex-wrap:wrap; gap:5px; margin-bottom:14px;"></div>

              <div style="font-size:11px; font-weight:700; color:#64748B; text-transform:uppercase; letter-spacing:0.04em; margin-bottom:6px;">Términos Técnicos Detectados en el Objeto</div>
              <div id="semTermsList" style="display:flex; flex-wrap:wrap; gap:4px;"></div>
            </div>

            <!-- Columna 2: Dictamen y Recomendación Estratégica -->
            <div style="background:#0B0F19; border:1px solid #1E293B; border-radius:4px; padding:14px; display:flex; flex-direction:column; justify-content:space-between;">
              <div>
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                  <span style="font-size:11px; font-weight:700; color:#64748B; text-transform:uppercase; letter-spacing:0.04em;">Dictamen Estratégico</span>
                  <span id="semModalidadSugerida" style="font-size:11px; background:#0F291E; color:#34D399; border:1px solid #059669; padding:2px 8px; border-radius:3px; font-weight:600;"></span>
                </div>

                <p id="semDictamenTexto" style="font-size:12px; line-height:1.55; color:#CBD5E1; margin-bottom:14px;"></p>

                <div style="font-size:11px; font-weight:600; color:#93C5FD; margin-bottom:5px;">Fortalezas Institucionales para la Propuesta:</div>
                <ul id="semFortalezasList" style="margin:0 0 12px 18px; padding:0; font-size:11.5px; color:#94A3B8; line-height:1.45;"></ul>

                <div style="font-size:11px; font-weight:600; color:#FCA5A5; margin-bottom:5px;">Consideraciones de Riesgo y Operación:</div>
                <ul id="semRiesgosList" style="margin:0 0 12px 18px; padding:0; font-size:11.5px; color:#94A3B8; line-height:1.45;"></ul>
              </div>

              <div id="semJustificacionTexto" style="font-size:11px; color:#64748B; border-top:1px solid #1E293B; padding-top:8px;"></div>
            </div>

          </div>
        </div>
      </div>

      <!-- Radar de Oportunidades SECOP -->
      <div class="card-panel">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; flex-wrap:wrap; gap:10px;">
          <div>
            <div class="card-heading">Radar de Oportunidades SECOP con Alta Afinidad USBMed</div>
            <div style="font-size:11px; color:#64748B; margin-top:2px;">Procesos contractuales reales ordenados por afinidad temática con los grupos de investigación y áreas de la universidad.</div>
          </div>
          <button class="btn-secondary" onclick="loadRadarSemantico()" style="font-size:11.5px; padding:6px 14px;">Actualizar Radar</button>
        </div>

        <div style="overflow-x:auto;">
          <table class="clean-tbl" style="width:100%; font-size:11.5px;">
            <thead>
              <tr>
                <th style="width:36px;">#</th>
                <th>Entidad Contratante</th>
                <th>Objeto del Proceso</th>
                <th>Cuantía (COP)</th>
                <th>Afinidad USBMed</th>
                <th>Área de Especialidad</th>
                <th style="text-align:right;">Acción</th>
              </tr>
            </thead>
            <tbody id="semRadarTableBody">
              <tr><td colspan="7" style="text-align:center; padding:16px; color:#64748B;">Cargando oportunidades del radar...</td></tr>
            </tbody>
          </table>
        </div>
      </div>

    </div>
  </section>
</main>

<script>
let currentFile = 'SECOP_INDUSTRIA_TIC_2015_2025.parquet';
let charts = {};
let geoDataGlobal = [];
let currentDrilldownDepto = 'ANTIOQUIA';
let currentStudioData = null;
let leafletMap = null;
let leafletMarkersGroup = null;

// Formateador estándar de moneda colombiana completa (sin truncar a M ni MM)
function formatCOP(val) {
  if (val === null || val === undefined || isNaN(val)) return '—';
  const num = Math.round(Number(val));
  return '$' + num.toLocaleString('es-CO') + ' COP';
}

// Formateador de conteos enteros de contratos (con comas de miles para no confundir con decimales)
function formatInt(val) {
  if (val === null || val === undefined || isNaN(val)) return '0';
  return Math.round(Number(val)).toLocaleString('en-US');
}

// Coordenadas geográficas de los 32 departamentos colombianos
const DEPTO_COORDS = {
  "ANTIOQUIA": [6.5, -75.5],
  "CUNDINAMARCA": [4.71, -74.07],
  "VALLE DEL CAUCA": [3.8, -76.5],
  "SANTANDER": [6.9, -73.3],
  "TOLIMA": [4.2, -75.2],
  "ATLÁNTICO": [10.7, -74.9],
  "HUILA": [2.6, -75.6],
  "CALDAS": [5.3, -75.4],
  "NORTE DE SANTANDER": [8.0, -72.9],
  "CASANARE": [5.5, -71.8],
  "META": [3.5, -73.0],
  "BOLÍVAR": [8.9, -74.3],
  "CAUCA": [2.5, -76.8],
  "BOYACÁ": [5.7, -73.0],
  "NARIÑO": [1.5, -77.8],
  "RISARALDA": [4.9, -75.9],
  "CESAR": [9.3, -73.5],
  "MAGDALENA": [10.3, -74.4],
  "CÓRDOBA": [8.3, -75.7],
  "LA GUAJIRA": [11.3, -72.6],
  "PUTUMAYO": [0.5, -76.3],
  "QUINDÍO": [4.4, -75.7],
  "ARAUCA": [6.6, -70.7],
  "SUCRE": [9.0, -75.0],
  "CAQUETÁ": [1.0, -74.0],
  "CHOCÓ": [5.3, -76.8],
  "SAN ANDRÉS Y PROVIDENCIA": [12.55, -81.7],
  "VAUPÉS": [0.8, -70.5],
  "VICHADA": [4.4, -69.5],
  "AMAZONAS": [-1.5, -71.5],
  "GUAVIARE": [2.0, -72.3],
  "GUAINÍA": [2.6, -68.8]
};

function setTab(name, btn) {
  document.querySelectorAll('.tab-item').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  if (btn) btn.classList.add('active');
  const el = document.getElementById('pane-' + name);
  if (el) el.classList.add('active');

  if (name === 'departamentos') {
    loadGeoTab();
  } else if (name === 'ingesta') {
    refreshIngestPanel();
  } else if (name === 'comparativa') {
    loadComparativaTab();
  } else if (name === 'comparador') {
    loadCompareOptions();
  } else if (name === 'explorador') {
    loadContractExplorer(1);
  } else if (name === 'consola') {
    if (!currentStudioData) applyStudioPreset('top_proveedores');
  } else if (name === 'ml') {
    loadMlTab();
  } else if (name === 'reportes') {
    loadReportPreview();
  } else if (name === 'semantico') {
    loadSemanticoTab();
  }
}

async function init() {
  try {
    const r = await fetch('/api/files');
    const d = await r.json();
    const sel = document.getElementById('selDataset');
    sel.innerHTML = '';
    d.files.forEach(f => {
      const opt = document.createElement('option');
      opt.value = f.name;
      opt.textContent = `${f.name} (${f.rows.toLocaleString()} filas, ${f.size_mb} MB)`;
      if (f.name === currentFile) opt.selected = true;
      sel.appendChild(opt);
    });

    const anios = [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025,2026];
    const sDesde = document.getElementById('fAnioDesde');
    const sHasta = document.getElementById('fAnioHasta');
    anios.forEach(a => {
      sDesde.appendChild(new Option(a, a));
      sHasta.appendChild(new Option(a, a));
    });

    await populateDeptos();
    await loadDashboard();
    await refreshIngestPanel();
    startIngestPolling();
  } catch(e) {
    console.error("Init failed:", e);
  }
}

async function populateDeptos() {
  try {
    const res = await fetch('/api/deptos?file=' + encodeURIComponent(currentFile));
    const d = await res.json();
    const sel = document.getElementById('fDepto');
    const selDd = document.getElementById('selDrilldownDepto');
    const selSt = document.getElementById('stDepto');
    const selRep = document.getElementById('repDepto');
    
    sel.innerHTML = '<option value="">Todos</option>';
    selDd.innerHTML = '';
    selSt.innerHTML = '<option value="">Todo el País</option>';
    if (selRep) selRep.innerHTML = '<option value="">Consolidado Nacional (32 Deptos)</option>';

    (d.deptos || []).forEach(dep => {
      sel.appendChild(new Option(dep, dep));
      selSt.appendChild(new Option(dep, dep));
      if (selRep) selRep.appendChild(new Option(dep, dep));
      const optDd = new Option(dep, dep);
      if (dep === 'ANTIOQUIA') optDd.selected = true;
      selDd.appendChild(optDd);
    });
  } catch(e){}
}

function onDatasetChange() {
  currentFile = document.getElementById('selDataset').value;
  loadDashboard();
}

function resetFilters() {
  document.getElementById('fAnioDesde').value = '';
  document.getElementById('fAnioHasta').value = '';
  document.getElementById('fDepto').value = '';
  document.getElementById('fSubsector').value = '';
  document.getElementById('fTipoContrato').value = '';
  document.getElementById('fRangoCuantia').value = '';
  loadDashboard();
}

async function loadDashboard() {
  const params = new URLSearchParams({
    file: currentFile,
    anio_desde: document.getElementById('fAnioDesde').value,
    anio_hasta: document.getElementById('fAnioHasta').value,
    depto: document.getElementById('fDepto').value,
    subsector: document.getElementById('fSubsector').value,
    tipo_contrato: document.getElementById('fTipoContrato').value,
    rango_cuantia: document.getElementById('fRangoCuantia').value
  });

  try {
    const r = await fetch('/api/tic_dashboard?' + params.toString());
    const d = await r.json();
    if (d.error) return;

    // KPIs con formato completo en pesos colombianos y conteos con comas
    document.getElementById('k1').textContent = formatInt(d.kpis.total_contratos);
    document.getElementById('k2').textContent = formatCOP(d.kpis.total_cop || d.kpis.total_mm * 1e9);
    document.getElementById('k3').textContent = formatCOP(d.kpis.media_cop || d.kpis.media_m * 1e6);
    document.getElementById('k4').textContent = formatCOP(d.kpis.mediana_cop || d.kpis.mediana_m * 1e6);
    document.getElementById('k5').textContent = d.subsectores[0] ? d.subsectores[0].subsector.replace('Servicios de ', '').substring(0, 18) : '—';
    document.getElementById('k6').textContent = d.deptos[0] ? d.deptos[0].depto.replace('Distrito Capital de ', '').substring(0, 15) : '—';

    // Gráfico anual
    renderChart('chartAnual', 'bar', {
      labels: d.anual.map(x => x.anio),
      datasets: [{
        label: 'Monto (MM COP)',
        data: d.anual.map(x => x.monto_mm),
        backgroundColor: '#2563EB',
        borderRadius: 3
      }]
    }, {
      scales: {
        y: { ticks: { color: '#6B7280', callback: v => '$' + v }, grid: { color: 'rgba(255,255,255,0.04)' } },
        x: { ticks: { color: '#6B7280' }, grid: { display: false } }
      },
      plugins: { legend: { display: false } }
    });

    // Proyección filtrada: mantiene separado el histórico de la estimación.
    const forecastRows = d.proyeccion || [];
    const forecastLabels = forecastRows.map(x => x.anio);
    renderChart('chartDashboardForecast', 'line', {
      labels: forecastLabels,
      datasets: [
        {
          label: 'Histórico real',
          data: forecastRows.map(x => x.es_proy ? null : x.monto_mm),
          borderColor: '#2563EB',
          backgroundColor: 'rgba(37,99,235,0.12)',
          borderWidth: 2.5,
          pointRadius: 3,
          fill: true
        },
        {
          label: 'Estimación futura',
          data: forecastRows.map(x => x.es_proy ? x.monto_mm : (x.anio === d.ultimo_anio ? x.monto_mm : null)),
          borderColor: '#10B981',
          borderDash: [6, 4],
          borderWidth: 2.5,
          pointRadius: 4,
          pointBackgroundColor: '#10B981',
          fill: false
        }
      ]
    }, {
      scales: {
        y: { ticks: { color: '#6B7280', callback: v => '$' + v }, grid: { color: 'rgba(255,255,255,0.04)' } },
        x: { ticks: { color: '#6B7280' }, grid: { display: false } }
      },
      plugins: { legend: { labels: { color: '#9CA3AF', font: { size: 10 } } } }
    });

    const futureRows = forecastRows.filter(x => x.es_proy);
    document.getElementById('dashboardForecastNote').textContent =
      `${d.modelo_proyeccion || 'Tendencia lineal'} | Filtros activos del dashboard`;
    document.getElementById('dashboardForecastBody').innerHTML = futureRows.length
      ? futureRows.map(x => `
        <tr>
          <td><strong>${x.anio}</strong></td>
          <td class="num-cell" style="color:var(--success); font-weight:600">$${Number(x.monto_mm).toLocaleString('es-CO', {maximumFractionDigits:2})}</td>
          <td class="num-cell" style="color:${x.variacion_pct >= 0 ? 'var(--success)' : 'var(--warning)'}">${x.variacion_pct >= 0 ? '+' : ''}${Number(x.variacion_pct).toFixed(1)}%</td>
        </tr>
      `).join('')
      : '<tr><td colspan="3" style="color:var(--text-muted)">No hay suficientes años históricos para proyectar.</td></tr>';

    // Gráfico subsectores
    renderChart('chartSub', 'doughnut', {
      labels: d.subsectores.map(x => x.subsector),
      datasets: [{
        data: d.subsectores.map(x => x.monto_mm),
        backgroundColor: ['#2563EB', '#10B981', '#0891B2', '#8B5CF6', '#F59E0B'],
        borderWidth: 0
      }]
    }, {
      plugins: {
        legend: { position: 'right', labels: { color: '#9CA3AF', font: { size: 10 }, boxWidth: 8 } }
      }
    });

    // Proveedores
    const topProv = d.proveedores.slice(0, 5);
    renderChart('chartProv', 'bar', {
      labels: topProv.map(x => x.proveedor.length > 20 ? x.proveedor.substring(0, 20) + '...' : x.proveedor),
      datasets: [{
        data: topProv.map(x => x.monto_mm),
        backgroundColor: '#0891B2',
        borderRadius: 3
      }]
    }, {
      indexAxis: 'y',
      scales: {
        x: { ticks: { color: '#6B7280', callback: v => '$' + v }, grid: { color: 'rgba(255,255,255,0.04)' } },
        y: { ticks: { color: '#9CA3AF', font: { size: 10 } }, grid: { display: false } }
      },
      plugins: { legend: { display: false } }
    });

    // Modalidades
    renderChart('chartMod', 'doughnut', {
      labels: d.modalidades.map(x => x.modalidad),
      datasets: [{
        data: d.modalidades.map(x => x.contratos),
        backgroundColor: ['#3B82F6', '#10B981', '#F59E0B', '#6366F1', '#EC4899', '#64748B'],
        borderWidth: 0
      }]
    }, {
      plugins: {
        legend: { position: 'right', labels: { color: '#9CA3AF', font: { size: 10 }, boxWidth: 8 } }
      }
    });

    // HHI
    const hhiBox = document.getElementById('hhiContainer');
    hhiBox.innerHTML = (d.hhi || []).map(item => {
      const val = Number(item.hhi);
      const color = val > 2500 ? 'var(--danger)' : val > 1500 ? 'var(--warning)' : 'var(--success)';
      const label = val > 2500 ? 'Alta Concentración' : val > 1500 ? 'Concentración Moderada' : 'Mercado Competitivo';
      const pct = Math.min(100, (val / 10000) * 100);
      return `
        <div class="meter-row">
          <div class="meter-info">
            <span style="color:var(--text-secondary)">${item.subsector}</span>
            <span style="font-family:var(--font-mono); color:${color}">${val.toFixed(0)} <small style="color:var(--text-muted)">(${label})</small></span>
          </div>
          <div class="meter-track">
            <div class="meter-fill" style="width:${pct}%; background:${color}"></div>
          </div>
        </div>
      `;
    }).join('');

    // Tabla Subsectores
    const totalMM = d.kpis.total_mm || 1;
    document.getElementById('subsectorTableBody').innerHTML = d.subsectores.map(sub => `
      <tr>
        <td>${sub.subsector}</td>
        <td class="num-cell">${formatInt(sub.contratos)}</td>
        <td class="num-cell">$${Number(sub.monto_mm).toFixed(2)}</td>
        <td class="num-cell">${((sub.monto_mm / totalMM) * 100).toFixed(1)}%</td>
      </tr>
    `).join('');

  } catch(e) {
    console.error("Dashboard update error:", e);
  }
}

/* =========================================================================
   TAB 2: DISTRIBUCIÓN GEOGRÁFICA NACIONAL, HEATMAP LEAFLET & DRILL-DOWN
   ========================================================================= */
async function loadGeoTab() {
  try {
    const r = await fetch('/api/mapa?file=' + encodeURIComponent(currentFile));
    const d = await r.json();
    if (d.error) return;

    geoDataGlobal = d;
    const total = d.reduce((acc, row) => acc + (row.total_cop || row.monto_mm * 1e9), 0);
    const top = d[0] || {};
    const antioquia = d.find(x => x.depto === 'ANTIOQUIA');

    document.getElementById('geoDeptCount').textContent = d.length;
    document.getElementById('geoTotalAmount').textContent = formatCOP(total);
    document.getElementById('geoLeaderName').textContent = top.depto || 'CUNDINAMARCA';
    document.getElementById('geoAntioquiaAmount').textContent = antioquia ? formatCOP(antioquia.total_cop || antioquia.monto_mm * 1e9) : '—';

    // 1. Renderizar Mapa Geográfico Real Leaflet
    renderLeafletHeatmap(d, total / 1e9);

    // 2. Gráfico de barras departamental general
    renderChart('chartDeptosBar', 'bar', {
      labels: d.slice(0, 15).map(x => x.depto.replace('Distrito Capital de ', '')),
      datasets: [{
        label: 'Monto MM COP',
        data: d.slice(0, 15).map(x => x.monto_mm),
        backgroundColor: d.slice(0, 15).map(x => x.depto === 'ANTIOQUIA' ? '#10B981' : '#2563EB'),
        borderRadius: 2
      }]
    }, {
      indexAxis: 'y',
      scales: {
        x: { ticks: { color: '#6B7280', callback: v => '$' + v }, grid: { color: 'rgba(255,255,255,0.04)' } },
        y: { ticks: { color: '#9CA3AF', font: { size: 10 } }, grid: { display: false } }
      },
      plugins: { legend: { display: false } }
    });

    // 3. Tabla general de 32 departamentos con cifras completas
    document.getElementById('geoTableBody').innerHTML = d.map((row, idx) => `
      <tr style="${row.depto === currentDrilldownDepto ? 'background:rgba(37,99,235,0.15); font-weight:600' : ''}">
        <td style="color:var(--text-muted)">${idx + 1}</td>
        <td><strong>${row.depto}</strong></td>
        <td class="num-cell">${formatInt(row.contratos)}</td>
        <td class="num-cell" style="color:var(--success); font-weight:600">${formatCOP(row.total_cop || row.monto_mm * 1e9)}</td>
        <td class="num-cell">${(( (row.total_cop || row.monto_mm * 1e9) / total) * 100).toFixed(1)}%</td>
        <td class="num-cell">${formatCOP(row.promedio_cop || row.promedio_m * 1e6)}</td>
        <td>
          <button class="btn-secondary" style="padding:2px 8px; font-size:11px" onclick="selectDeptoDrilldown('${row.depto}')">Profundizar</button>
        </td>
      </tr>
    `).join('');

    // 4. Cargar Drill-Down del Departamento seleccionado (por defecto Antioquia)
    await loadDeptoDetail(currentDrilldownDepto);

  } catch(e) {
    console.error("Geo tab error:", e);
  }
}

function renderLeafletHeatmap(deptos, totalMM) {
  const container = document.getElementById('geoLeafletMap');
  if (!container) return;

  if (!leafletMap) {
    leafletMap = L.map('geoLeafletMap', {
      center: [4.5709, -73.5],
      zoom: 5.2,
      zoomControl: true,
      attributionControl: false
    });

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      maxZoom: 18
    }).addTo(leafletMap);

    leafletMarkersGroup = L.layerGroup().addTo(leafletMap);
  }

  leafletMarkersGroup.clearLayers();

  const maxVal = Math.max(...deptos.map(d => d.monto_mm || 1));

  deptos.forEach(d => {
    const coords = DEPTO_COORDS[d.depto.toUpperCase()];
    if (!coords) return;

    const ratio = d.monto_mm / maxVal;
    let color = '#3B82F6';
    let radius = 10;
    if (ratio > 0.4) {
      color = '#10B981';
      radius = 28;
    } else if (ratio > 0.1) {
      color = '#0891B2';
      radius = 22;
    } else if (ratio > 0.03) {
      color = '#6366F1';
      radius = 16;
    } else if (ratio > 0.01) {
      color = '#8B5CF6';
      radius = 12;
    }

    const circle = L.circleMarker(coords, {
      radius: radius,
      fillColor: color,
      color: '#fff',
      weight: 1.5,
      opacity: 0.9,
      fillOpacity: 0.65
    });

    const popupHtml = `
      <div style="font-family: 'Inter', sans-serif; min-width: 180px; padding: 4px;">
        <div style="font-size: 12px; font-weight: 700; color: #60A5FA; text-transform: uppercase; margin-bottom: 4px;">${d.depto}</div>
        <div style="font-size: 11px; color: #9CA3AF; margin-bottom: 2px;">Inversión TIC: <strong style="color:#10B981">$${Number(d.monto_mm).toFixed(2)} MM</strong></div>
        <div style="font-size: 11px; color: #9CA3AF; margin-bottom: 2px;">Contratos: <strong style="color:#F9FAFB">${formatInt(d.contratos)}</strong></div>
        <div style="font-size: 11px; color: #9CA3AF; margin-bottom: 6px;">Participación: <strong style="color:#F59E0B">${((d.monto_mm / totalMM) * 100).toFixed(1)}%</strong></div>
        <button onclick="selectDeptoDrilldown('${d.depto}')" style="background:#2563EB; color:#fff; border:none; padding:4px 8px; font-size:10px; font-weight:600; border-radius:3px; cursor:pointer; width:100%">
          Profundizar en este departamento
        </button>
      </div>
    `;

    circle.bindPopup(popupHtml);
    circle.on('click', () => { selectDeptoDrilldown(d.depto); });
    leafletMarkersGroup.addLayer(circle);
  });
}

function selectDeptoDrilldown(depto) {
  currentDrilldownDepto = depto;
  const sel = document.getElementById('selDrilldownDepto');
  if (sel) sel.value = depto;
  loadDeptoDetail(depto);

  const drillEl = document.getElementById('drilldownSection');
  if (drillEl) {
    drillEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
}

function changeDrilldownDepto() {
  const depto = document.getElementById('selDrilldownDepto').value;
  selectDeptoDrilldown(depto);
}

async function loadDeptoDetail(depto) {
  try {
    document.getElementById('drilldownDeptoTitle').textContent = 'Departamento de ' + depto;
    document.getElementById('ddChartMunTitle').textContent = `Top 10 Municipios en ${depto}`;
    document.getElementById('ddChartEntTitle').textContent = `Top 10 Entidades Compradoras en ${depto}`;
    document.getElementById('ddTableTitle').textContent = `Principales Contratos Tecnológicos en ${depto}`;

    const res = await fetch(`/api/depto_detail?depto=${encodeURIComponent(depto)}&file=${encodeURIComponent(currentFile)}`);
    const d = await res.json();
    if (d.error) return;

    // KPIs locales con formato completo
    document.getElementById('ddKpiContratos').textContent = formatInt(d.kpis.total_contratos);
    document.getElementById('ddKpiMonto').textContent = formatCOP(d.kpis.total_cop || d.kpis.total_mm * 1e9);
    document.getElementById('ddKpiPromedio').textContent = formatCOP(d.kpis.promedio_cop || d.kpis.promedio_m * 1e6);
    document.getElementById('ddKpiMediana').textContent = formatCOP(d.kpis.mediana_cop || d.kpis.mediana_m * 1e6);

    // Gráfico Municipios
    const munLabels = d.municipios.map(m => m.municipio.length > 15 ? m.municipio.substring(0, 15) + '...' : m.municipio);
    const munData = d.municipios.map(m => m.monto_mm);
    renderChart('chartDdMunicipios', 'bar', {
      labels: munLabels,
      datasets: [{
        label: 'Monto (MM COP)',
        data: munData,
        backgroundColor: '#10B981',
        borderRadius: 3
      }]
    }, {
      scales: {
        y: { ticks: { color: '#6B7280', callback: v => '$' + v }, grid: { color: 'rgba(255,255,255,0.04)' } },
        x: { ticks: { color: '#9CA3AF', font: { size: 10 } }, grid: { display: false } }
      },
      plugins: { legend: { display: false } }
    });

    // Gráfico Entidades
    const entLabels = d.entidades.map(e => e.entidad.length > 25 ? e.entidad.substring(0, 25) + '...' : e.entidad);
    const entData = d.entidades.map(e => e.monto_mm);
    renderChart('chartDdEntidades', 'bar', {
      labels: entLabels,
      datasets: [{
        label: 'Monto (MM COP)',
        data: entData,
        backgroundColor: '#0891B2',
        borderRadius: 3
      }]
    }, {
      indexAxis: 'y',
      scales: {
        x: { ticks: { color: '#6B7280', callback: v => '$' + v }, grid: { color: 'rgba(255,255,255,0.04)' } },
        y: { ticks: { color: '#9CA3AF', font: { size: 9 } }, grid: { display: false } }
      },
      plugins: { legend: { display: false } }
    });

    // Tabla Top Contratos en el departamento con valores completos
    document.getElementById('ddContractsBody').innerHTML = (d.top_contratos || []).map(c => `
      <tr>
        <td style="font-family:var(--font-mono); color:#60A5FA; font-size:11px">${c.id_contrato_global || '—'}</td>
        <td title="${c.nombre_entidad}">${(c.nombre_entidad || '—').substring(0, 32)}</td>
        <td>${c.municipio || '—'}</td>
        <td title="${c.proveedor}">${(c.proveedor || 'ANÓNIMO').substring(0, 28)}</td>
        <td class="num-cell" style="color:var(--success); font-weight:600">${formatCOP(c.valor_cop || c.valor_m * 1e6)}</td>
        <td><span class="badge-clean">${(c.subsector_tic || 'TIC').replace('Servicios de ', '')}</span></td>
        <td>${c.anio_contratacion || '—'}</td>
        <td style="color:var(--text-secondary); max-width:280px; overflow:hidden; text-overflow:ellipsis" title="${c.objeto}">${c.objeto}</td>
      </tr>
    `).join('');

  } catch(e) {
    console.error("Depto detail error:", e);
  }
}

/* =========================================================================
   TAB 3: COMPARATIVA MACROECONÓMICA
   ========================================================================= */
async function loadComparativaTab() {
  try {
    const r = await fetch('/api/comparacion');
    const d = await r.json();
    if (d.error) return;

    renderChart('chartComparativa', 'bar', {
      labels: d.map(x => x.anio),
      datasets: [
        { type: 'bar', label: 'Total Estado (MM)', data: d.map(x => x.total_mm), backgroundColor: 'rgba(55,65,81,0.5)', borderRadius: 2, yAxisID: 'y' },
        { type: 'line', label: 'Sector TIC (MM)', data: d.map(x => x.tic_mm), borderColor: '#2563EB', borderWidth: 2, pointRadius: 3, yAxisID: 'y2' }
      ]
    }, {
      scales: {
        y: { position: 'left', ticks: { color: '#9CA3AF', callback: v => '$' + v.toLocaleString() }, grid: { color: 'rgba(255,255,255,0.04)' } },
        y2: { position: 'right', ticks: { color: '#60A5FA', callback: v => '$' + v.toLocaleString() }, grid: { display: false } },
        x: { ticks: { color: '#6B7280' }, grid: { display: false } }
      },
      plugins: { legend: { labels: { color: '#9CA3AF', font: { size: 11 } } } }
    });

    renderChart('chartShare', 'line', {
      labels: d.map(x => x.anio),
      datasets: [{
        label: '% Participación TIC',
        data: d.map(x => x.pct_tic),
        borderColor: '#10B981',
        backgroundColor: 'rgba(16,185,129,0.08)',
        borderWidth: 2,
        fill: true
      }]
    }, {
      scales: {
        y: { ticks: { color: '#6B7280', callback: v => v + '%' }, grid: { color: 'rgba(255,255,255,0.04)' } },
        x: { ticks: { color: '#6B7280' }, grid: { display: false } }
      },
      plugins: { legend: { display: false } }
    });

    document.getElementById('cmpTableBody').innerHTML = d.map(x => `
      <tr>
        <td>${x.anio}</td>
        <td class="num-cell">$${Number(x.total_mm).toLocaleString(undefined, {maximumFractionDigits:0})}</td>
        <td class="num-cell" style="color:#60A5FA">$${Number(x.tic_mm).toLocaleString(undefined, {maximumFractionDigits:0})}</td>
        <td class="num-cell" style="color:var(--success)">${Number(x.pct_tic).toFixed(1)}%</td>
        <td class="num-cell">${formatInt(x.total_contratos)}</td>
        <td class="num-cell">${formatInt(x.tic_contratos)}</td>
      </tr>
    `).join('');

  } catch(e) {
    console.error("Comparativa error:", e);
  }
}

/* =========================================================================
   TAB 4: COMPARADOR MULTIDIMENSIONAL (AVANZADO)
   ========================================================================= */
let compareOptionsCache = {};

async function loadCompareOptions() {
  const dim = document.getElementById('cmpDim').value;
  try {
    const res = await fetch(`/api/compare_options?dim=${dim}&file=${encodeURIComponent(currentFile)}`);
    const d = await res.json();
    compareOptionsCache[dim] = d.options || [];

    const selA = document.getElementById('cmpItemA');
    const selB = document.getElementById('cmpItemB');
    selA.innerHTML = '';
    selB.innerHTML = '';

    (d.options || []).forEach((opt, idx) => {
      selA.appendChild(new Option(opt, opt));
      selB.appendChild(new Option(opt, opt));
    });

    if (selA.options.length > 0) selA.selectedIndex = 0;
    if (selB.options.length > 1) selB.selectedIndex = 1;

    await runComparison();
  } catch(e) {
    console.error("Load compare options error:", e);
  }
}

async function onCompareDimChange() {
  await loadCompareOptions();
}

async function applyComparePreset(presetKey) {
  const dimSel = document.getElementById('cmpDim');
  if (presetKey === 'antioquia_valle') {
    dimSel.value = 'departamento';
    await loadCompareOptions();
    document.getElementById('cmpItemA').value = 'ANTIOQUIA';
    document.getElementById('cmpItemB').value = 'VALLE DEL CAUCA';
  } else if (presetKey === 'cundinamarca_antioquia') {
    dimSel.value = 'departamento';
    await loadCompareOptions();
    document.getElementById('cmpItemA').value = 'CUNDINAMARCA';
    document.getElementById('cmpItemB').value = 'ANTIOQUIA';
  } else if (presetKey === 'anios_pandemia') {
    dimSel.value = 'anio';
    await loadCompareOptions();
    document.getElementById('cmpItemA').value = '2019';
    document.getElementById('cmpItemB').value = '2024';
  } else if (presetKey === 'servicios_compraventa') {
    dimSel.value = 'tipo_contrato';
    await loadCompareOptions();
    document.getElementById('cmpItemA').value = 'Prestación de servicios';
    document.getElementById('cmpItemB').value = 'Compraventa';
  } else if (presetKey === 'mintic_sena') {
    dimSel.value = 'entidad';
    await loadCompareOptions();
    const opts = Array.from(document.getElementById('cmpItemA').options).map(o => o.value);
    const mintic = opts.find(o => o.toLowerCase().includes('tecnolog') || o.toLowerCase().includes('tic')) || opts[0];
    const sena = opts.find(o => o.toLowerCase().includes('sena') || o.toLowerCase().includes('aprendizaje')) || opts[1];
    document.getElementById('cmpItemA').value = mintic;
    document.getElementById('cmpItemB').value = sena;
  } else if (presetKey === 'soporte_conectividad') {
    dimSel.value = 'subsector';
    await loadCompareOptions();
    document.getElementById('cmpItemA').value = 'Servicios de Soporte y Consultoría TI';
    document.getElementById('cmpItemB').value = 'Conectividad y Redes';
  }
  await runComparison();
}

async function runComparison() {
  const dim = document.getElementById('cmpDim').value;
  const itemA = document.getElementById('cmpItemA').value;
  const itemB = document.getElementById('cmpItemB').value;

  if (!itemA || !itemB) return;

  try {
    const res = await fetch(`/api/compare?dim=${encodeURIComponent(dim)}&item_a=${encodeURIComponent(itemA)}&item_b=${encodeURIComponent(itemB)}&file=${encodeURIComponent(currentFile)}`);
    const d = await res.json();
    if (d.error) return;

    // Actualizar etiquetas
    const nameA = itemA.length > 20 ? itemA.substring(0, 20) + '...' : itemA;
    const nameB = itemB.length > 20 ? itemB.substring(0, 20) + '...' : itemB;

    document.getElementById('tagCmpTotA').textContent = `A: ${nameA}`;
    document.getElementById('tagCmpTotB').textContent = `B: ${nameB}`;
    document.getElementById('tagCmpCttA').textContent = `A: ${nameA}`;
    document.getElementById('tagCmpCttB').textContent = `B: ${nameB}`;
    document.getElementById('tagCmpAvgA').textContent = `A: ${nameA}`;
    document.getElementById('tagCmpAvgB').textContent = `B: ${nameB}`;
    document.getElementById('tagCmpMedA').textContent = `A: ${nameA}`;
    document.getElementById('tagCmpMedB').textContent = `B: ${nameB}`;
    document.getElementById('tagCmpDiasA').textContent = `A: ${nameA}`;
    document.getElementById('tagCmpDiasB').textContent = `B: ${nameB}`;
    document.getElementById('tagCmpConpesA').textContent = `A: ${nameA}`;
    document.getElementById('tagCmpConpesB').textContent = `B: ${nameB}`;

    document.getElementById('thCmpA').textContent = `A: ${itemA}`;
    document.getElementById('thCmpB').textContent = `B: ${itemB}`;

    // Valores
    document.getElementById('valCmpTotA').textContent = formatCOP(d.kpis_a.total_cop);
    document.getElementById('valCmpTotB').textContent = formatCOP(d.kpis_b.total_cop);
    document.getElementById('valCmpCttA').textContent = formatInt(d.kpis_a.total_contratos);
    document.getElementById('valCmpCttB').textContent = formatInt(d.kpis_b.total_contratos);
    document.getElementById('valCmpAvgA').textContent = formatCOP(d.kpis_a.media_cop);
    document.getElementById('valCmpAvgB').textContent = formatCOP(d.kpis_b.media_cop);
    document.getElementById('valCmpMedA').textContent = formatCOP(d.kpis_a.mediana_cop);
    document.getElementById('valCmpMedB').textContent = formatCOP(d.kpis_b.mediana_cop);
    document.getElementById('valCmpDiasA').textContent = `${d.kpis_a.dias_prom} d`;
    document.getElementById('valCmpDiasB').textContent = `${d.kpis_b.dias_prom} d`;
    document.getElementById('valCmpConpesA').textContent = `${d.kpis_a.pct_innovacion}%`;
    document.getElementById('valCmpConpesB').textContent = `${d.kpis_b.pct_innovacion}%`;

    // Deltas con estilos
    function applyDeltaBadge(id, deltaVal, suffix = '%') {
      const el = document.getElementById(id);
      const isPos = deltaVal > 0;
      const isNeg = deltaVal < 0;
      el.textContent = (isPos ? '+' : '') + deltaVal + suffix;
      el.className = 'delta-badge ' + (isPos ? 'delta-pos' : (isNeg ? 'delta-neg' : 'delta-neutral'));
    }

    applyDeltaBadge('deltaCmpTot', d.deltas.total_cop.pct, '%');
    applyDeltaBadge('deltaCmpCtt', d.deltas.total_contratos.pct, '%');
    applyDeltaBadge('deltaCmpAvg', d.deltas.media_cop.pct, '%');
    applyDeltaBadge('deltaCmpMed', d.deltas.mediana_cop.pct, '%');
    applyDeltaBadge('deltaCmpDias', (d.kpis_a.dias_prom - d.kpis_b.dias_prom).toFixed(1), ' d');
    applyDeltaBadge('deltaCmpConpes', (d.kpis_a.pct_innovacion - d.kpis_b.pct_innovacion).toFixed(1), ' pp');

    // 1. Render Radar Chart
    renderChart('chartCmpRadar', 'radar', {
      labels: d.radar.axes,
      datasets: [
        {
          label: `A: ${nameA}`,
          data: d.radar.data_a,
          borderColor: '#3B82F6',
          backgroundColor: 'rgba(59, 130, 246, 0.25)',
          pointBackgroundColor: '#3B82F6',
          borderWidth: 2
        },
        {
          label: `B: ${nameB}`,
          data: d.radar.data_b,
          borderColor: '#10B981',
          backgroundColor: 'rgba(16, 185, 129, 0.25)',
          pointBackgroundColor: '#10B981',
          borderWidth: 2
        }
      ]
    }, {
      scales: {
        r: {
          angleLines: { color: 'rgba(255,255,255,0.08)' },
          grid: { color: 'rgba(255,255,255,0.08)' },
          pointLabels: { color: '#9CA3AF', font: { size: 10, weight: '600' } },
          ticks: { display: false, min: 0, max: 100 }
        }
      },
      plugins: { legend: { labels: { color: '#F9FAFB', font: { size: 11 } } } }
    });

    // 2. Render Paired Bar Chart
    renderChart('chartCmpBars', 'bar', {
      labels: d.chart_subs.labels.map(l => l.length > 20 ? l.substring(0, 20) + '...' : l),
      datasets: [
        {
          label: `A: ${nameA}`,
          data: d.chart_subs.data_a,
          backgroundColor: '#3B82F6',
          borderRadius: 3
        },
        {
          label: `B: ${nameB}`,
          data: d.chart_subs.data_b,
          backgroundColor: '#10B981',
          borderRadius: 3
        }
      ]
    }, {
      indexAxis: 'y',
      scales: {
        x: { ticks: { color: '#6B7280', callback: v => '$' + v }, grid: { color: 'rgba(255,255,255,0.04)' } },
        y: { ticks: { color: '#9CA3AF', font: { size: 9 } }, grid: { display: false } }
      },
      plugins: { legend: { labels: { color: '#F9FAFB', font: { size: 11 } } } }
    });

    // 3. Render Matrix Rows
    const matrixRows = [
      {
        dim: 'Presupuesto Total Contratado',
        va: formatCOP(d.kpis_a.total_cop),
        vb: formatCOP(d.kpis_b.total_cop),
        diff: formatCOP(Math.abs(d.deltas.total_cop.diff)),
        pct: (d.deltas.total_cop.pct > 0 ? '+' : '') + d.deltas.total_cop.pct + '%',
        diag: d.deltas.total_cop.pct > 0 ? 'Mayor volumen presupuestal en Elemento A' : 'Mayor volumen presupuestal en Elemento B'
      },
      {
        dim: 'Número Total de Contratos',
        va: formatInt(d.kpis_a.total_contratos),
        vb: formatInt(d.kpis_b.total_contratos),
        diff: formatInt(Math.abs(d.deltas.total_contratos.diff)),
        pct: (d.deltas.total_contratos.pct > 0 ? '+' : '') + d.deltas.total_contratos.pct + '%',
        diag: d.deltas.total_contratos.pct > 0 ? 'Mayor volumen transaccional en Elemento A' : 'Mayor volumen transaccional en Elemento B'
      },
      {
        dim: 'Cuantía Promedio por Contrato',
        va: formatCOP(d.kpis_a.media_cop),
        vb: formatCOP(d.kpis_b.media_cop),
        diff: formatCOP(Math.abs(d.deltas.media_cop.diff)),
        pct: (d.deltas.media_cop.pct > 0 ? '+' : '') + d.deltas.media_cop.pct + '%',
        diag: d.deltas.media_cop.pct > 0 ? 'Procesos de mayor envergadura unitaria en A' : 'Procesos de mayor envergadura unitaria en B'
      },
      {
        dim: 'Días de Prórroga Promedio',
        va: `${d.kpis_a.dias_prom} d`,
        vb: `${d.kpis_b.dias_prom} d`,
        diff: `${Math.abs(d.kpis_a.dias_prom - d.kpis_b.dias_prom).toFixed(1)} d`,
        pct: (d.kpis_a.dias_prom > d.kpis_b.dias_prom ? '+' : '-') + Math.abs(d.kpis_a.dias_prom - d.kpis_b.dias_prom).toFixed(1) + ' d',
        diag: d.kpis_a.dias_prom < d.kpis_b.dias_prom ? 'Mayor cumplimiento temporal en Elemento A' : 'Mayor cumplimiento temporal en Elemento B'
      },
      {
        dim: 'Intensidad Innovación CONPES 4069',
        va: `${d.kpis_a.pct_innovacion}%`,
        vb: `${d.kpis_b.pct_innovacion}%`,
        diff: `${Math.abs(d.kpis_a.pct_innovacion - d.kpis_b.pct_innovacion).toFixed(1)} pp`,
        pct: (d.kpis_a.pct_innovacion > d.kpis_b.pct_innovacion ? '+' : '-') + Math.abs(d.kpis_a.pct_innovacion - d.kpis_b.pct_innovacion).toFixed(1) + ' pp',
        diag: d.kpis_a.pct_innovacion > d.kpis_b.pct_innovacion ? 'Mayor adopción tecnológica I+D en Elemento A' : 'Mayor adopción tecnológica I+D en Elemento B'
      }
    ];

    document.getElementById('cmpMatrixBody').innerHTML = matrixRows.map(r => `
      <tr>
        <td><strong>${r.dim}</strong></td>
        <td class="num-cell" style="color:#60A5FA; font-weight:600">${r.va}</td>
        <td class="num-cell" style="color:var(--success); font-weight:600">${r.vb}</td>
        <td class="num-cell">${r.diff}</td>
        <td class="num-cell"><span class="delta-badge ${r.pct.includes('+') ? 'delta-pos' : 'delta-neg'}">${r.pct}</span></td>
        <td style="color:var(--text-secondary)">${r.diag}</td>
      </tr>
    `).join('');

  } catch(e) {
    console.error("Comparison error:", e);
  }
}

/* =========================================================================
   TAB 5: EXPLORADOR DE CONTRATOS
   ========================================================================= */
let explorerPage = 1;
let explorerPageSize = 100;
let explorerTotalPages = 1;

async function loadContractExplorer(page = 1) {
  explorerPage = page;
  const q = document.getElementById('dtSearch').value;
  const params = new URLSearchParams({
    file: currentFile,
    page: explorerPage,
    page_size: explorerPageSize,
    q: q
  });

  try {
    const res = await fetch('/api/preview?' + params.toString());
    const d = await res.json();
    if (d.error) return;

    explorerTotalPages = d.total_pages;
    document.getElementById('dtCount').textContent = `${formatInt(d.total_matched)} contratos encontrados`;
    document.getElementById('pageIndicator').textContent = `Pág ${d.page} / ${d.total_pages}`;
    document.getElementById('btnPrevPage').disabled = d.page <= 1;
    document.getElementById('btnNextPage').disabled = d.page >= d.total_pages;

    renderTableRows(d.columns, d.rows);
  } catch(e) {
    console.error("Contract explorer error:", e);
  }
}

function searchContracts() {
  loadContractExplorer(1);
}

function clearContractSearch() {
  document.getElementById('dtSearch').value = '';
  loadContractExplorer(1);
}

function changePage(delta) {
  const newPage = explorerPage + delta;
  if (newPage >= 1 && newPage <= explorerTotalPages) {
    loadContractExplorer(newPage);
  }
}

function changePageSize() {
  explorerPageSize = parseInt(document.getElementById('dtPageSize').value, 10);
  loadContractExplorer(1);
}

function renderTableRows(columns, rows) {
  const thead = document.getElementById('dtHeaders');
  const tbody = document.getElementById('dtRecords');
  if (!thead || !tbody) return;

  const displayCols = columns.slice(0, 10);
  thead.innerHTML = '<tr>' + displayCols.map(c => `<th>${c.replace(/_/g, ' ')}</th>`).join('') + '</tr>';

  if (!rows || rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="10" style="color:var(--text-muted); text-align:center; padding:20px">No se encontraron contratos coincidentes.</td></tr>';
    return;
  }

  tbody.innerHTML = rows.map(r => '<tr>' + displayCols.map(c => {
    const val = r[c];
    if (val === null || val === undefined) return '<td style="color:var(--text-muted)">—</td>';
    if (typeof val === 'number' && c.includes('valor')) return `<td class="num-cell" style="color:var(--success)">${formatCOP(val)}</td>`;
    if (typeof val === 'number') return `<td class="num-cell">${formatInt(val)}</td>`;
    return `<td title="${String(val)}">${String(val).substring(0, 50)}</td>`;
  }).join('') + '</tr>').join('');
}

/* =========================================================================
   TAB 6: ESTUDIO ANALÍTICO MULTIDIMENSIONAL
   ========================================================================= */
function applyStudioPreset(presetKey) {
  document.querySelectorAll('.studio-pill').forEach(p => p.classList.remove('active'));
  const pill = document.getElementById('pill-' + presetKey);
  if (pill) pill.classList.add('active');

  const sDim = document.getElementById('stDim');
  const sMetric = document.getElementById('stMetric');
  const sChart = document.getElementById('stChartType');
  const sLimit = document.getElementById('stLimit');

  if (presetKey === 'top_proveedores') {
    sDim.value = 'proveedor';
    sMetric.value = 'sum_monto';
    sChart.value = 'bar_h';
    sLimit.value = '15';
  } else if (presetKey === 'top_entidades') {
    sDim.value = 'entidad';
    sMetric.value = 'sum_monto';
    sChart.value = 'bar_h';
    sLimit.value = '15';
  } else if (presetKey === 'megacontratos') {
    sDim.value = 'entidad';
    sMetric.value = 'sum_monto';
    sChart.value = 'bar_v';
    sLimit.value = '10';
  } else if (presetKey === 'prorrogas') {
    sDim.value = 'proveedor';
    sMetric.value = 'avg_dias';
    sChart.value = 'bar_h';
    sLimit.value = '15';
  } else if (presetKey === 'tipos_contrato') {
    sDim.value = 'tipo_contrato';
    sMetric.value = 'sum_monto';
    sChart.value = 'doughnut';
    sLimit.value = '10';
  } else if (presetKey === 'pyme') {
    sDim.value = 'pyme';
    sMetric.value = 'sum_monto';
    sChart.value = 'doughnut';
    sLimit.value = '10';
  } else if (presetKey === 'conpes') {
    sDim.value = 'subsector';
    sMetric.value = 'sum_monto';
    sChart.value = 'bar_v';
    sLimit.value = '10';
  } else if (presetKey === 'modalidades') {
    sDim.value = 'modalidad';
    sMetric.value = 'count_cttos';
    sChart.value = 'doughnut';
    sLimit.value = '10';
  }

  runStudioCustomQuery();
}

async function runStudioCustomQuery() {
  const dim = document.getElementById('stDim').value;
  const metric = document.getElementById('stMetric').value;
  const depto = document.getElementById('stDepto').value;
  const chartType = document.getElementById('stChartType').value;
  const limit = document.getElementById('stLimit').value;

  const params = new URLSearchParams({
    file: currentFile,
    dim: dim,
    metric: metric,
    depto: depto,
    limit: limit,
    sort: 'desc'
  });

  try {
    const t0 = performance.now();
    const res = await fetch('/api/analytics_studio?' + params.toString());
    const d = await res.json();
    const t1 = performance.now();
    if (d.error) return;

    currentStudioData = d;
    const latency = Math.round(t1 - t0);

    document.getElementById('studioChartTitle').textContent = d.title;
    document.getElementById('studioChartSub').textContent = d.subtitle;
    document.getElementById('studioTableTitle').textContent = 'Detalle de Resultados: ' + d.title;
    document.getElementById('studioTableSub').textContent = `${d.rows.length} grupos procesados por DuckDB`;

    document.getElementById('studioKpiRows').textContent = formatInt(d.rows.length);
    document.getElementById('studioKpiMonto').textContent = '$' + Number(d.total_mm).toFixed(2);
    document.getElementById('studioKpiLeader').textContent = d.leader_name || '—';
    document.getElementById('studioKpiPerf').textContent = `${latency} ms`;

    const labels = d.chart_labels.map(l => l.length > 25 ? l.substring(0, 25) + '...' : l);
    const dataVals = d.chart_data;

    if (chartType === 'doughnut') {
      renderChart('chartStudio', 'doughnut', {
        labels: labels,
        datasets: [{
          data: dataVals,
          backgroundColor: ['#2563EB', '#10B981', '#0891B2', '#F59E0B', '#8B5CF6', '#EC4899', '#64748B', '#14B8A6'],
          borderWidth: 0
        }]
      }, {
        plugins: {
          legend: { position: 'right', labels: { color: '#9CA3AF', font: { size: 10 }, boxWidth: 8 } }
        }
      });
    } else if (chartType === 'line') {
      renderChart('chartStudio', 'line', {
        labels: labels,
        datasets: [{
          label: d.metric_label,
          data: dataVals,
          borderColor: '#2563EB',
          backgroundColor: 'rgba(37,99,235,0.1)',
          borderWidth: 2,
          fill: true
        }]
      }, {
        scales: {
          y: { ticks: { color: '#6B7280' }, grid: { color: 'rgba(255,255,255,0.04)' } },
          x: { ticks: { color: '#6B7280' }, grid: { display: false } }
        },
        plugins: { legend: { display: false } }
      });
    } else {
      const isHorizontal = chartType === 'bar_h';
      renderChart('chartStudio', 'bar', {
        labels: labels,
        datasets: [{
          label: d.metric_label,
          data: dataVals,
          backgroundColor: '#2563EB',
          borderRadius: 3
        }]
      }, {
        indexAxis: isHorizontal ? 'y' : 'x',
        scales: {
          x: { ticks: { color: '#6B7280' }, grid: { color: 'rgba(255,255,255,0.04)' } },
          y: { ticks: { color: '#9CA3AF', font: { size: 10 } }, grid: { display: false } }
        },
        plugins: { legend: { display: false } }
      });
    }

    renderStudioTable(d.columns, d.rows);
  } catch(e) {
    console.error("Studio custom query error:", e);
  }
}

function renderStudioTable(columns, rows) {
  const head = document.getElementById('studioTableHead');
  const body = document.getElementById('studioTableBody');
  if (!head || !body) return;

  const colLabels = {
    'etiqueta': 'Dimensión / Segmento',
    'contratos': 'Contratos',
    'total_mm': 'Total (MM COP)',
    'prom_m': 'Promedio (M COP)',
    'mediana_m': 'Mediana (M COP)',
    'dias_prom': 'Prórroga Promedio (Días)'
  };

  head.innerHTML = '<tr>' + columns.map(c => `<th>${colLabels[c] || c.replace(/_/g, ' ').toUpperCase()}</th>`).join('') + '</tr>';

  body.innerHTML = rows.map(r => '<tr>' + columns.map(c => {
    const val = r[c];
    if (val === null || val === undefined) return '<td style="color:var(--text-muted)">—</td>';
    if (c === 'total_mm') return `<td class="num-cell" style="color:var(--success); font-weight:600">$${Number(val).toFixed(2)}</td>`;
    if (c === 'prom_m' || c === 'mediana_m') return `<td class="num-cell">$${Number(val).toFixed(1)}</td>`;
    if (c === 'contratos') return `<td class="num-cell">${formatInt(val)}</td>`;
    if (c === 'dias_prom') return `<td class="num-cell" style="color:${val > 30 ? 'var(--warning)' : 'inherit'}">${val} d</td>`;
    return `<td title="${String(val)}"><strong>${String(val).substring(0, 60)}</strong></td>`;
  }).join('') + '</tr>').join('');
}

function exportStudioCSV() {
  if (!currentStudioData || !currentStudioData.rows || currentStudioData.rows.length === 0) {
    alert("No hay datos cargados para exportar.");
    return;
  }
  const cols = currentStudioData.columns;
  const rows = currentStudioData.rows;

  let csvContent = "data:text/csv;charset=utf-8," + cols.join(",") + "\n";
  rows.forEach(r => {
    const line = cols.map(c => {
      let val = r[c] !== null && r[c] !== undefined ? String(r[c]) : "";
      val = val.replace(/"/g, '""');
      return `"${val}"`;
    }).join(",");
    csvContent += line + "\n";
  });

  const encodedUri = encodeURI(csvContent);
  const link = document.createElement("a");
  link.setAttribute("href", encodedUri);
  link.setAttribute("download", `SECOP_Estudio_${currentStudioData.dim || 'analisis'}.csv`);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

function toggleSqlAccordion() {
  const body = document.getElementById('sqlAccordionBody');
  const arrow = document.getElementById('accordionArrow');
  if (body.classList.contains('open')) {
    body.classList.remove('open');
    arrow.textContent = 'Mostrar Terminal SQL';
  } else {
    body.classList.add('open');
    arrow.textContent = 'Ocultar Terminal SQL';
  }
}

async function executeUserQuery() {
  const sql = document.getElementById('sqlInput').value.trim();
  const t0 = performance.now();
  try {
    const res = await fetch('/api/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ file: currentFile, sql })
    });
    const d = await res.json();
    const t1 = performance.now();

    if (d.error) {
      document.getElementById('queryPerf').textContent = "Error en consulta";
      document.getElementById('studioTableBody').innerHTML = `<tr><td colspan="10" style="color:var(--danger); padding:16px">${d.error}</td></tr>`;
      return;
    }

    document.getElementById('queryPerf').textContent = `Ejecutado en ${(t1 - t0).toFixed(1)} ms (${d.total_count} filas)`;
    currentStudioData = {
      title: "Consulta SQL Libre",
      subtitle: `${d.total_count} registros devueltos por el motor DuckDB`,
      columns: d.columns,
      rows: d.rows,
      dim: "sql_custom",
      total_mm: 0,
      leader_name: d.rows[0] ? String(Object.values(d.rows[0])[0]) : "—"
    };

    document.getElementById('studioChartTitle').textContent = "Resultado SQL";
    document.getElementById('studioChartSub').textContent = `${d.total_count} filas`;
    document.getElementById('studioTableTitle').textContent = "Resultado de la Consulta SQL";
    document.getElementById('studioTableSub').textContent = `${d.displayed_count} filas visualizadas`;
    document.getElementById('studioKpiRows').textContent = formatInt(d.total_count);
    document.getElementById('studioKpiPerf').textContent = `${(t1-t0).toFixed(1)} ms`;

    renderStudioTable(d.columns, d.rows);
  } catch(e) {
    document.getElementById('studioTableBody').innerHTML = `<tr><td colspan="10" style="color:var(--danger); padding:16px">${e.message}</td></tr>`;
  }
}

/* =========================================================================
   TAB 7: MACHINE LEARNING & CONPES 4069
   ========================================================================= */
async function loadMlTab() {
  try {
    const res = await fetch('/api/ml_real');
    const d = await res.json();
    if (d.error) return;

    document.getElementById('mlMontoConpes').textContent = formatCOP(d.monto_conpes_raw || d.monto_conpes_mm * 1e9);
    document.getElementById('mlPctConpes').textContent = d.pct_conpes + '%';

    // 1. Gráfico de Proyección Predictiva (2016-2027)
    const proyLabels = d.proyecciones.map(x => x.anio);
    const realData = d.proyecciones.map(x => x.es_proy ? null : x.monto_mm);
    const proyData = d.proyecciones.map(x => x.es_proy ? x.monto_mm : (x.anio === 2025 ? x.monto_mm : null));

    const mlForecastRows = d.proyecciones.filter(x => x.es_proy);
    const mlForecastBody = document.getElementById('mlForecastBody');
    if (mlForecastBody) {
      mlForecastBody.innerHTML = mlForecastRows.map(x => `
        <tr>
          <td><strong>${x.anio}</strong></td>
          <td class="num-cell" style="color:var(--success); font-weight:600">$${Number(x.monto_mm).toLocaleString('es-CO', {maximumFractionDigits:2})}</td>
          <td class="num-cell">${x.variacion_pct === undefined ? '—' : `${x.variacion_pct >= 0 ? '+' : ''}${Number(x.variacion_pct).toFixed(1)}%`}</td>
        </tr>
      `).join('');
    }

    renderChart('chartMlProyeccion', 'line', {
      labels: proyLabels,
      datasets: [
        {
          label: 'Histórico Real (2016-2025)',
          data: realData,
          borderColor: '#2563EB',
          backgroundColor: 'rgba(37,99,235,0.12)',
          borderWidth: 2.5,
          pointRadius: 4,
          fill: true
        },
        {
          label: 'Pronóstico Supervisado (2026-2027)',
          data: proyData,
          borderColor: '#10B981',
          borderDash: [6, 4],
          borderWidth: 2.5,
          pointRadius: 5,
          pointBackgroundColor: '#10B981',
          fill: false
        }
      ]
    }, {
      scales: {
        y: { ticks: { color: '#9CA3AF', callback: v => '$' + v }, grid: { color: 'rgba(255,255,255,0.04)' } },
        x: { ticks: { color: '#6B7280' }, grid: { display: false } }
      },
      plugins: { legend: { labels: { color: '#9CA3AF', font: { size: 11 } } } }
    });

    // 2. Gráfico de Distribución de Clusters
    renderChart('chartMlClusters', 'doughnut', {
      labels: d.clusters.map(c => `Cluster ${c.cluster}: ${c.label}`),
      datasets: [{
        data: d.clusters.map(c => c.monto_total_mm),
        backgroundColor: ['#2563EB', '#F59E0B', '#EF4444', '#10B981'],
        borderWidth: 0
      }]
    }, {
      plugins: {
        legend: { position: 'right', labels: { color: '#9CA3AF', font: { size: 10 }, boxWidth: 8 } }
      }
    });

    // 3. Matriz de Correlación de Pearson
    const corrBody = document.getElementById('mlCorrBody');
    if (corrBody && d.correlaciones) {
      const vars = [
        { k: 'log_valor', label: 'Log(Cuantía)' },
        { k: 'dias_adicionados', label: 'Días Prórroga' },
        { k: 'flag_prorroga', label: 'Flag Prórroga' },
        { k: 'flag_innovacion_conpes', label: 'Innovación (CONPES)' }
      ];

      corrBody.innerHTML = vars.map(vRow => {
        return `
          <tr>
            <td><strong>${vRow.label}</strong></td>
            ${vars.map(vCol => {
              const val = d.correlaciones[vRow.k] ? d.correlaciones[vRow.k][vCol.k] : 0.0;
              let style = '';
              if (vRow.k === vCol.k) style = 'color:#60A5FA; font-weight:700';
              else if (val > 0.4) style = 'color:var(--success); font-weight:600';
              else if (val < -0.1) style = 'color:var(--warning)';
              return `<td class="num-cell" style="${style}">${Number(val).toFixed(3)}</td>`;
            }).join('')}
          </tr>
        `;
      }).join('');
    }

    // 4. Gráfico Desglose CONPES por Subsector
    if (d.conpes_subsectores) {
      renderChart('chartMlConpesSub', 'bar', {
        labels: d.conpes_subsectores.map(s => s.subsector.length > 20 ? s.subsector.substring(0, 20) + '...' : s.subsector),
        datasets: [{
          label: 'Inversión I+D / 4IR (MM COP)',
          data: d.conpes_subsectores.map(s => s.monto_mm),
          backgroundColor: '#0891B2',
          borderRadius: 3
        }]
      }, {
        indexAxis: 'y',
        scales: {
          x: { ticks: { color: '#6B7280', callback: v => '$' + v }, grid: { color: 'rgba(255,255,255,0.04)' } },
          y: { ticks: { color: '#9CA3AF', font: { size: 9 } }, grid: { display: false } }
        },
        plugins: { legend: { display: false } }
      });
    }

    // 5. Tabla Clusters con Cifras Completas
    document.getElementById('mlClustersBody').innerHTML = d.clusters.map(c => `
      <tr>
        <td><span class="badge-clean">Cluster ${c.cluster}</span></td>
        <td><strong>${c.label}</strong></td>
        <td class="num-cell">${formatInt(c.count)}</td>
        <td class="num-cell" style="color:var(--success); font-weight:600">${formatCOP(c.monto_total_cop || c.monto_total_mm * 1e9)}</td>
        <td class="num-cell">${formatCOP(c.promedio_cop || c.promedio_m * 1e6)}</td>
        <td class="num-cell" style="color:${c.adiciones_dias_prom > 30 ? 'var(--warning)' : 'inherit'}">${c.adiciones_dias_prom} d</td>
        <td class="num-cell" style="color:#60A5FA">${c.pct_innovacion}%</td>
      </tr>
    `).join('');

    // 6. Tabla Anomalías con Cifras Completas
    document.getElementById('mlAnomaliasBody').innerHTML = d.anomalias.map(a => `
      <tr>
        <td style="font-family:var(--font-mono); color:#60A5FA; font-size:11px">${a.id_contrato_global}</td>
        <td>${a.subsector_tic}</td>
        <td>${a.departamento}</td>
        <td>${a.modalidad}</td>
        <td class="num-cell" style="color:var(--danger); font-weight:600">${formatCOP(a.valor_contrato)}</td>
        <td class="num-cell" style="color:var(--warning)">${a.dias_adicionados} d</td>
        <td class="num-cell" style="font-family:var(--font-mono); color:var(--danger)">${Number(a.score_anom).toFixed(3)}</td>
      </tr>
    `).join('');
  } catch(e) {
    console.error("ML tab error:", e);
  }
}

/* =========================================================================
   TAB 8: GENERADOR DE REPORTES EJECUTIVOS (OFICIAL)
   ========================================================================= */
async function loadReportPreview() {
  const tipo = document.getElementById('repTipo').value;
  const depto = document.getElementById('repDepto').value;
  const anioDesde = document.getElementById('repAnioDesde').value;
  const anioHasta = document.getElementById('repAnioHasta').value;

  const params = new URLSearchParams({
    tipo: tipo,
    depto: depto,
    anio_desde: anioDesde,
    anio_hasta: anioHasta
  });

  try {
    const res = await fetch('/api/reports/preview?' + params.toString());
    const d = await res.json();
    if (d.error) return;

    const titles = {
      'territorial': 'Informe Ejecutivo de Desempeño Territorial',
      'riesgos': 'Auditoría Técnica de Riesgos, Prórrogas y Anomalías',
      'mercado': 'Estudio de Estructura de Mercado e Índice HHI',
      'conpes': 'Balance de Innovación y Transformación Digital (CONPES 4069)'
    };

    document.getElementById('repDocTitle').textContent = `${titles[d.tipo] || 'Informe Ejecutivo'}: ${d.depto}`;
    document.getElementById('repDocSub').textContent = `Jurisdicción: ${d.depto} | Vigencia Fiscal: ${d.vigencia}`;
    document.getElementById('repDocFecha').textContent = `Emisión: Septiembre 2026 | SECOP Engine`;

    document.getElementById('repKpiCttos').textContent = formatInt(d.kpis.cttos);
    document.getElementById('repKpiMonto').textContent = formatCOP(d.kpis.tot_cop);
    document.getElementById('repKpiProm').textContent = formatCOP(d.kpis.avg_cop);
    document.getElementById('repKpiMed').textContent = formatCOP(d.kpis.med_cop);

    document.getElementById('repDocNarrative').innerHTML = `
      El presente informe formal emitido por el <strong>Observatorio de Contratación Pública TIC</strong> consolida el 
      comportamiento de los procesos auditados en la jurisdicción de <strong>${d.depto}</strong> durante el período 
      <strong>${d.vigencia}</strong>. Durante este lapso se celebraron un total de <strong>${formatInt(d.kpis.cttos)} contratos</strong> 
      por un valor acumulado de <strong>${formatCOP(d.kpis.tot_cop)}</strong>, con una tasa promedio de prórrogas de 
      <strong>${Number(d.kpis.dias_prom).toFixed(1)} días</strong> y un componente de innovación CONPES del 
      <strong>${Number(d.kpis.pct_conpes).toFixed(1)}%</strong>.
    `;

    // Tabla Subsectores en Reporte
    document.getElementById('repTableSub').innerHTML = d.subsectores.map(s => `
      <tr>
        <td><strong>${s.subsector}</strong></td>
        <td class="num-cell">${formatInt(s.cttos)}</td>
        <td class="num-cell" style="color:var(--success); font-weight:600">${formatCOP(s.tot_cop)}</td>
        <td class="num-cell">${formatCOP(s.avg_cop)}</td>
      </tr>
    `).join('');

    // Tabla Entidades en Reporte
    document.getElementById('repTableEnt').innerHTML = d.entidades.map(e => `
      <tr>
        <td><strong>${e.entidad}</strong></td>
        <td class="num-cell">${formatInt(e.cttos)}</td>
        <td class="num-cell" style="color:var(--success); font-weight:600">${formatCOP(e.tot_cop)}</td>
      </tr>
    `).join('');

  } catch(e) {
    console.error("Report preview error:", e);
  }
}

function downloadOfficialPDF() {
  const tipo = document.getElementById('repTipo').value;
  const depto = document.getElementById('repDepto').value;
  const anioDesde = document.getElementById('repAnioDesde').value;
  const anioHasta = document.getElementById('repAnioHasta').value;

  const params = new URLSearchParams({
    tipo: tipo,
    depto: depto,
    anio_desde: anioDesde,
    anio_hasta: anioHasta
  });

  window.open('/api/reports/pdf?' + params.toString(), '_blank');
}

function renderChart(id, type, data, opts) {
  const ctx = document.getElementById(id);
  if (!ctx) return;

  // Destroy previous instance cleanly
  if (charts[id]) {
    try { charts[id].destroy(); } catch(e) {}
    delete charts[id];
  }

  // Get fixed size from parent wrapper
  const parent = ctx.parentElement;
  const w = parent ? parent.clientWidth : 600;
  const h = parent ? parent.clientHeight : 250;

  // Set explicit pixel dimensions on canvas to prevent resize loops
  ctx.width = w;
  ctx.height = h;
  ctx.style.width = w + 'px';
  ctx.style.height = h + 'px';

  charts[id] = new Chart(ctx, {
    type: type,
    data: data,
    options: {
      responsive: false,
      maintainAspectRatio: false,
      animation: { duration: 300 },
      ...opts
    }
  });
}

const PRESETS_SEMANTICO = {
  "preset_ia": {
    "id": "CO1.PCONT.4589211",
    "entidad": "ALCALDÍA DE MEDELLÍN - SECRETARÍA DE INNOVACIÓN DIGITAL",
    "modalidad": "Licitación Pública",
    "valor": 1850000000,
    "objeto": "DESARROLLO E IMPLEMENTACIÓN DE UNA PLATAFORMA DIGITAL INTELIGENTE CON MODELOS DE ANALÍTICA PREDICTIVA, MACHINE LEARNING Y CIENCIA DE DATOS PARA LA TOMA DE DECISIONES TERRITORIALES."
  },
  "preset_cloud": {
    "id": "CO1.PCONT.8893420",
    "entidad": "GOBERNACIÓN DE ANTIOQUIA",
    "modalidad": "Selección Abreviada de Menor Cuantía",
    "valor": 890000000,
    "objeto": "SERVICIOS PROFESIONALES DE INGENIERÍA DE SOFTWARE PARA EL DISEÑO, MIGRACIÓN A LA NUBE Y DESARROLLO DE MICROSERVICIOS DE LA VENTANILLA ÚNICA DE TRÁMITES DEPARTAMENTALES."
  },
  "preset_multimedia": {
    "id": "CO1.PCONT.6612090",
    "entidad": "RUTA N MEDELLÍN / SECRETARÍA DE DESARROLLO ECONÓMICO",
    "modalidad": "Concurso de Méritos",
    "valor": 750000000,
    "objeto": "DISEÑO, DESARROLLO Y PRODUCCIÓN DE CONTENIDOS INTERACTIVOS MULTIMEDIA, EXPERIENCIA DE USUARIO (UX/UI), SIMULADORES Y REALIDAD VIRTUAL (VR/AR) PARA EL DISTRITO CTI."
  },
  "preset_ciber": {
    "id": "CO1.PCONT.7723145",
    "entidad": "EMPRESA DE TRANSPORTE MASIVO DEL VALLE DE ABURRÁ (METRO DE MEDELLÍN)",
    "modalidad": "Licitación Pública",
    "valor": 2400000000,
    "objeto": "AUDITORÍA FORENSE DIGITAL, PENTESTING AVANZADO, EVALUACIÓN DE VULNERABILIDADES BAJO ISO 27001 Y FORTALECIMIENTO DEL CENTRO DE OPERACIONES DE CIBERSEGURIDAD (SOC)."
  },
  "preset_hardware": {
    "id": "CO1.PCONT.9921400",
    "entidad": "ALCALDÍA MUNICIPAL DE RIONEGRO",
    "modalidad": "Subasta Inversa Presencial",
    "valor": 2600000000,
    "objeto": "ADQUISICIÓN, SUMINISTRO E INSTALACIÓN MASIVA DE 1.200 EQUIPOS DE CÓMPUTO PORTÁTILES, CARROS DE CARGA, CABLEADO ESTRUCTURADO Y OBRA CIVIL PARA SEDES EDUCATIVAS."
  }
};

async function loadSemanticoTab() {
  cambiarPresetSemantico();
  loadRadarSemantico();
  setTimeout(ejecutarAnalisisSemantico, 150);
}

function cambiarPresetSemantico() {
  const selEl = document.getElementById('selPresetSemantico');
  if (!selEl) return;
  const p = PRESETS_SEMANTICO[selEl.value];
  if (!p) return;
  document.getElementById('semContratoId').value = p.id;
  document.getElementById('semEntidad').value = p.entidad;
  document.getElementById('semModalidad').value = p.modalidad;
  document.getElementById('semValorCop').value = p.valor;
  document.getElementById('semObjeto').value = p.objeto;
}

async function buscarContratosEnDuckDB() {
  const q = (document.getElementById('inputDuckSearch').value || '').trim();
  if (!q) {
    alert('Ingrese un término para buscar contratos reales en DuckDB (ej: Medellin, Software, Antioquia, Datos)');
    return;
  }
  const countEl = document.getElementById('duckSearchResultCount');
  countEl.innerText = 'Buscando en DuckDB...';
  try {
    const res = await fetch('/api/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        file: currentFile,
        sql: `SELECT id_contrato_global, nombre_entidad, modalidad, valor_contrato, COALESCE(objeto_detallado, objeto_resumido, '') as objeto FROM datos WHERE LOWER(objeto) LIKE '%${q.toLowerCase()}%' OR LOWER(nombre_entidad) LIKE '%${q.toLowerCase()}%' ORDER BY valor_contrato DESC LIMIT 1`
      })
    });
    const d = await res.json();
    if (d.rows && d.rows.length > 0) {
      const r = d.rows[0];
      document.getElementById('semContratoId').value = r.id_contrato_global || '';
      document.getElementById('semEntidad').value = r.nombre_entidad || '';
      document.getElementById('semModalidad').value = r.modalidad || 'Licitación Pública';
      document.getElementById('semValorCop').value = r.valor_contrato || 0;
      document.getElementById('semObjeto').value = r.objeto || '';
      document.getElementById('selPresetSemantico').value = '';
      countEl.innerText = `Encontrado: ${r.nombre_entidad.slice(0, 30)}...`;
      ejecutarAnalisisSemantico();
    } else {
      countEl.innerText = 'No se hallaron coincidencias en el dataset';
    }
  } catch (e) {
    countEl.innerText = 'Error: ' + e;
  }
}

async function ejecutarAnalisisSemantico() {
  const btn = document.getElementById('btnEjecutarSemantico');
  const orig = btn.innerHTML;
  btn.innerHTML = 'Procesando evaluación semántica...';
  btn.disabled = true;

  try {
    const payload = {
      id_contrato: (document.getElementById('semContratoId').value || '').trim(),
      entidad: (document.getElementById('semEntidad').value || '').trim(),
      modalidad: (document.getElementById('semModalidad').value || '').trim(),
      valor_cop: parseFloat(document.getElementById('semValorCop').value || 0),
      objeto: (document.getElementById('semObjeto').value || '').trim(),
      api_key: (document.getElementById('semApiKey').value || '').trim() || undefined
    };

    const res = await fetch('/api/semantico/evaluar', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const d = await res.json();
    if (d.error) {
      alert('Error en motor semántico: ' + d.error);
      return;
    }
    renderResultadoSemantico(d);
  } catch (e) {
    alert('Error al contactar el motor semántico: ' + e);
  } finally {
    btn.innerHTML = orig;
    btn.disabled = false;
  }
}

function renderResultadoSemantico(d) {
  const panel = document.getElementById('semResultPanel');
  panel.style.display = 'block';

  document.getElementById('semScoreGlobal').innerText = d.score_tecnico + '%';
  const elConc = document.getElementById('semConclusion');
  elConc.innerText = d.conclusion_tecnica;
  elConc.style.color = d.color_tecnico;

  const sem = d.analisis_semantico || {};
  document.getElementById('semAreaPrincipal').innerText = sem.area_principal || 'General';

  document.getElementById('semGruposList').innerHTML = (sem.grupos_sugeridos || []).map(g =>
    `<span style="background:#1E293B;color:#38BDF8;border:1px solid #0284C7;padding:3px 8px;border-radius:4px;font-size:11px;">${g}</span>`
  ).join('');

  const labs = sem.laboratorios_sugeridos || [];
  document.getElementById('semLabsList').innerHTML = labs.length > 0 ?
    labs.map(l => `<span style="background:rgba(16,185,129,0.12);color:#6EE7B7;border:1px solid rgba(16,185,129,0.3);padding:3px 8px;border-radius:4px;font-size:11px;">${l}</span>`).join('') :
    '<span style="font-size:11px;color:var(--text-muted);">CEDETEC - Centro de Extensión y Consultoría TI</span>';

  document.getElementById('semTermsList').innerHTML = (sem.terminos_clave_detectados || []).map(k =>
    `<span style="background:rgba(59,130,246,0.15);color:#93C5FD;border:1px solid rgba(59,130,246,0.3);padding:2px 7px;border-radius:3px;font-size:10.5px;">${k}</span>`
  ).join('');

  const brechasBox = document.getElementById('semBrechasAlert');
  if (sem.alertas_brechas && sem.alertas_brechas.length > 0) {
    brechasBox.style.display = 'block';
    brechasBox.innerHTML = '<div style="font-weight:600;color:#F59E0B;margin-bottom:4px;font-size:11px;text-transform:uppercase;">Alerta de Alcance y Suministro:</div>' +
      sem.alertas_brechas.map(b => `<div style="font-size:11.5px;color:#FCD34D;">- ${b}</div>`).join('');
  } else {
    brechasBox.style.display = 'none';
  }

  const llm = d.dictamen_llm || {};
  document.getElementById('semEngineTag').innerText = llm.motor_llm_utilizado || 'Motor SECOP Analítico USBMed';
  document.getElementById('semModalidadSugerida').innerText = llm.modalidad_recomendada || 'Proponente Singular (USBMed Titular)';
  document.getElementById('semDictamenTexto').innerText = llm.dictamen_estrategico || '';

  document.getElementById('semFortalezasList').innerHTML = (llm.fortalezas_institucionales || []).map(f =>
    `<li>${f}</li>`
  ).join('');

  document.getElementById('semRiesgosList').innerHTML = (llm.brechas_y_riesgos || []).map(r =>
    `<li>${r}</li>`
  ).join('');

  document.getElementById('semJustificacionTexto').innerText = llm.justificacion_academica || '';
}

async function loadRadarSemantico() {
  const tbody = document.getElementById('semRadarTableBody');
  if (!tbody) return;
  tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:16px;color:var(--text-muted);">Escaneando afinidad semántica en DuckDB...</td></tr>';
  try {
    const res = await fetch('/api/semantico/radar');
    const d = await res.json();
    const list = d.radar || [];
    tbody.innerHTML = list.map((item, idx) => {
      const col = item.score >= 70 ? '#10B981' : (item.score >= 40 ? '#38BDF8' : '#F59E0B');
      return `<tr style="border-bottom:1px solid #1F2937;">
        <td style="padding:8px;font-family:var(--font-mono);font-size:11px;color:var(--text-muted);">${idx + 1}</td>
        <td style="padding:8px;"><div style="font-weight:600;font-size:12px;">${item.entidad}</div><div style="font-size:10.5px;color:var(--text-muted);">${item.modalidad}</div></td>
        <td style="padding:8px;font-size:11px;color:var(--text-secondary);">${item.objeto}</td>
        <td style="padding:8px;font-family:var(--font-mono);font-size:11.5px;font-weight:600;">${formatCOP(item.valor_cop)}</td>
        <td style="padding:8px;"><span style="font-weight:700;font-size:12px;color:${col};font-family:var(--font-mono);">${item.score}%</span></td>
        <td style="padding:8px;font-size:11px;color:var(--text-muted);">${item.area_principal}</td>
        <td style="padding:8px;text-align:right;">
          <button class="btn-secondary" style="font-size:10.5px;padding:4px 8px;" onclick="cargarEnEvaluadorSemantico('${item.id_contrato}', '${encodeURIComponent(item.entidad)}', '${encodeURIComponent(item.modalidad)}', ${item.valor_cop}, '${encodeURIComponent(item.objeto_completo)}')">
            Evaluar con USBMed
          </button>
        </td>
      </tr>`;
    }).join('');
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="7" style="color:var(--danger);padding:16px;">Error: ${e}</td></tr>`;
  }
}

function cargarEnEvaluadorSemantico(id, entidad, mod, valor, objeto) {
  document.getElementById('semContratoId').value = id;
  document.getElementById('semEntidad').value = decodeURIComponent(entidad);
  document.getElementById('semModalidad').value = decodeURIComponent(mod);
  document.getElementById('semValorCop').value = valor;
  document.getElementById('semObjeto').value = decodeURIComponent(objeto);
  document.getElementById('selPresetSemantico').value = '';
  document.getElementById('pane-semantico').scrollIntoView({ behavior: 'smooth' });
  ejecutarAnalisisSemantico();
}

/* =========================================================================
   INGESTA CONTINUA SODA / LAMBDA UPSERT
   ========================================================================= */
let ingestPollTimer = null;
let ingestWasRunning = false;

function fmtInt(n) {
  const v = Number(n || 0);
  return v.toLocaleString('es-CO');
}

function applyIngestSnapshot(d) {
  const base = d.base_historica || 213123;
  const total = d.total || (d.meta && d.meta.total_actual) || base;
  const inserted = d.inserted || (d.meta && d.meta.insertados) || 0;
  const updated = d.updated || (d.meta && d.meta.actualizados) || 0;
  const setTxt = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
  setTxt('ingestBase', fmtInt(base));
  setTxt('ingestTotal', fmtInt(total));
  setTxt('ingestInserted', '+' + fmtInt(inserted));
  setTxt('ingestUpdated', fmtInt(updated));
  setTxt('ingestMsg', d.message || '');
  setTxt('ingestaKpiBase', fmtInt(base));
  setTxt('ingestaKpiTotal', fmtInt(total));
  setTxt('ingestaKpiIns', '+' + fmtInt(inserted));
  setTxt('ingestaKpiUpd', fmtInt(updated));
  setTxt('mlTotalCount', fmtInt(total));
  const audit = document.getElementById('auditTicTotal');
  if (audit) audit.textContent = fmtInt(total) + ' contratos | base 213,123 + ingesta SODA';
  const banner = document.getElementById('ingestBanner');
  const track = document.getElementById('ingestProgressTrack');
  const btn = document.getElementById('btnActualizarTic');
  if (banner) {
    banner.classList.toggle('running', !!d.running);
    banner.classList.toggle('error', !!d.error);
  }
  if (track) track.style.display = d.running ? 'block' : 'none';
  if (btn) {
    btn.disabled = !!d.running;
    btn.textContent = d.running
      ? 'Actualizando contratos TIC…'
      : 'Realizar actualización de los contratos (contratación pública en TIC)';
  }
  const srcBody = document.getElementById('ingestaSourcesBody');
  if (srcBody) {
    const sources = d.sources || (d.meta && d.meta.sources) || [];
    if (!sources.length) {
      srcBody.innerHTML = '<tr><td colspan="3" style="color:var(--text-muted);padding:14px">Sin corrida aún.</td></tr>';
    } else {
      srcBody.innerHTML = sources.map(s => `<tr>
        <td>${s.name || s.dataset || 'fuente'}</td>
        <td class="num-cell">${fmtInt(s.tic || s.fetched || 0)}</td>
        <td>${s.since || 'incremental'}</td>
      </tr>`).join('');
    }
  }
}

async function refreshIngestPanel() {
  try {
    const r = await fetch('/api/ingestion/status');
    const d = await r.json();
    applyIngestSnapshot(d);
    const runsBody = document.getElementById('ingestaRunsBody');
    if (runsBody) {
      const runs = d.runs || [];
      if (!runs.length) {
        runsBody.innerHTML = '<tr><td colspan="5" style="color:var(--text-muted);padding:14px">Sin corridas registradas.</td></tr>';
      } else {
        runsBody.innerHTML = runs.map(row => `<tr>
          <td>${row.id}</td>
          <td>${row.dataset || ''}</td>
          <td>${row.status || ''}</td>
          <td class="num-cell">${fmtInt(row.fetched)}</td>
          <td class="num-cell">${fmtInt(row.written)}</td>
        </tr>`).join('');
      }
    }
    return d;
  } catch (e) {
    console.error('ingest status', e);
    return null;
  }
}

function startIngestPolling() {
  if (ingestPollTimer) return;
  ingestPollTimer = setInterval(async () => {
    const d = await refreshIngestPanel();
    if (!d) return;
    if (d.running) ingestWasRunning = true;
    if (ingestWasRunning && !d.running) {
      ingestWasRunning = false;
      await loadDashboard();
      await populateDeptos();
    }
  }, 1500);
}

async function realizarActualizacionContratosTIC() {
  const btn = document.getElementById('btnActualizarTic');
  if (btn) btn.disabled = true;
  ingestWasRunning = true;
  applyIngestSnapshot({
    running: true,
    message: 'Lanzando ingesta SODA filtrada TIC…',
    base_historica: 213123,
    total: 213123,
    inserted: 0,
    updated: 0
  });
  try {
    const res = await fetch('/api/actualizar-contratos-tic', { method: 'POST' });
    const d = await res.json();
    applyIngestSnapshot(d);
    startIngestPolling();
  } catch (e) {
    applyIngestSnapshot({
      running: false,
      error: String(e),
      message: 'No se pudo iniciar la actualización: ' + e
    });
  }
}

window.addEventListener('DOMContentLoaded', init);
</script>
<script src="https://grok.com/grok-app-builder/extensions.js"></script>
</body>
</html>
"""

def _json(handler, data, code=200):
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.end_headers()
    handler.wfile.write(body)

def _col(cols, candidates):
    for c in candidates:
        if c in cols: return c
    return cols[0] if cols else "valor_contrato"

def _build_where(qs, col_monto, col_anio, col_depto, col_sub=None, col_tipo=None):
    clauses = [f"{col_monto} > 0"]
    if qs.get("anio_desde", [""])[0]:
        clauses.append(f"{col_anio} >= {qs['anio_desde'][0]}")
    if qs.get("anio_hasta", [""])[0]:
        clauses.append(f"{col_anio} <= {qs['anio_hasta'][0]}")
    if qs.get("depto", [""])[0]:
        clauses.append(f"{col_depto} = '{qs['depto'][0].replace(chr(39), chr(39)+chr(39))}'")
    if col_sub and qs.get("subsector", [""])[0]:
        clauses.append(f"{col_sub} = '{qs['subsector'][0].replace(chr(39), chr(39)+chr(39))}'")
    if col_tipo and qs.get("tipo_contrato", [""])[0]:
        tc = qs['tipo_contrato'][0].replace(chr(39), chr(39)+chr(39)).lower()
        if "prestaci" in tc:
            clauses.append(f"(lower({col_tipo}) LIKE '%prestaci%')")
        elif "compra" in tc:
            clauses.append(f"(lower({col_tipo}) LIKE '%compra%')")
        elif "suministro" in tc:
            clauses.append(f"(lower({col_tipo}) LIKE '%suministro%')")
        elif "consultor" in tc:
            clauses.append(f"(lower({col_tipo}) LIKE '%consultor%')")
        elif "obra" in tc:
            clauses.append(f"(lower({col_tipo}) LIKE '%obra%')")
        elif "arrend" in tc:
            clauses.append(f"(lower({col_tipo}) LIKE '%arrend%')")
        elif "otro" in tc:
            clauses.append(f"(lower({col_tipo}) LIKE '%otro%')")
        else:
            clauses.append(f"lower({col_tipo}) LIKE '%{tc}%'")
    if qs.get("rango_cuantia", [""])[0]:
        r = qs['rango_cuantia'][0]
        if r == "minima":
            clauses.append(f"{col_monto} < 50000000")
        elif r == "menor":
            clauses.append(f"{col_monto} >= 50000000 AND {col_monto} < 500000000")
        elif r == "mayor":
            clauses.append(f"{col_monto} >= 500000000 AND {col_monto} < 5000000000")
        elif r == "mega":
            clauses.append(f"{col_monto} >= 5000000000")
    return " AND ".join(clauses)

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        path = parsed.path

        if path in ["/", "/index.html"]:
            body = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
            return

        elif path == "/api/ingestion" or path == "/api/ingestion/status":
            try:
                from ingestion.repository import IngestionRepository
                repo = IngestionRepository()
                snap = snapshot_ingest()
                snap["counts"] = repo.counts()
                snap["runs"] = [
                    {
                        "id": row["id"],
                        "dataset": row["dataset_key"],
                        "started_at": row["started_at"].isoformat() if row.get("started_at") else None,
                        "finished_at": row["finished_at"].isoformat() if row.get("finished_at") else None,
                        "fetched": row["records_fetched"],
                        "written": row["records_written"],
                        "failed": row["records_failed"],
                        "status": row["status"],
                        "error": row["error_summary"],
                    }
                    for row in repo.recent_runs(15)
                ]
                try:
                    snap["total"] = int(DB.execute("SELECT count(*) FROM v_secop_industria_tic_2015_2025").fetchone()[0])
                except Exception:
                    pass
                _json(self, snap)
            except Exception as e:
                snap = snapshot_ingest()
                snap["error"] = str(e)
                _json(self, snap, 200)

        # ENDPOINTS DEDICADOS: MOTOR SEMÁNTICO LLM USBMED
        elif path == "/api/semantico/capacidades":
            p = get_perfil_universidad()
            _json(self, {"capacidades_tecnicas": p.get("capacidades_tecnicas", [])})

        elif path == "/api/semantico/radar":
            try:
                perfil = get_perfil_universidad()
                sql = """
                    SELECT 
                        id_contrato_global,
                        nombre_entidad,
                        modalidad,
                        COALESCE(objeto_detallado, objeto_resumido, '') AS objeto,
                        valor_contrato
                    FROM v_secop_industria_tic_2015_2025
                    WHERE valor_contrato > 50000000
                    ORDER BY valor_contrato DESC
                    LIMIT 25
                """
                df_cands = DB.execute(sql).df()
                radar_results = []
                for _, r in df_cands.iterrows():
                    res = evaluar_idoneidad_contrato(
                        objeto_contrato=str(r['objeto']),
                        entidad=str(r['nombre_entidad']),
                        modalidad=str(r['modalidad']),
                        valor_cop=float(r['valor_contrato'])
                    )
                    radar_results.append({
                        "id_contrato": str(r['id_contrato_global']),
                        "entidad": str(r['nombre_entidad']),
                        "modalidad": str(r['modalidad']),
                        "objeto": (str(r['objeto'])[:120] + "...") if len(str(r['objeto'])) > 120 else str(r['objeto']),
                        "objeto_completo": str(r['objeto']),
                        "valor_cop": float(r['valor_contrato']),
                        "score": res.get("score_tecnico", 0),
                        "conclusion": res.get("conclusion_tecnica", ""),
                        "area_principal": res.get("analisis_semantico", {}).get("area_principal", "General")
                    })
                radar_results.sort(key=lambda x: x["score"], reverse=True)
                _json(self, {"radar": radar_results})
            except Exception as e:
                _json(self, {"error": str(e)}, 500)

        elif path == "/api/files":
            files_data = []
            for p in sorted(PARQUET_DIR.glob("*.parquet")):
                vname = f"v_{p.stem.lower().replace('-', '_')}"
                try:
                    count = DB.execute(f"SELECT count(*) FROM {vname}").fetchone()[0]
                except Exception:
                    count = 0
                files_data.append({"name": p.name, "size_mb": round(p.stat().st_size / 1e6, 2), "rows": count})
            _json(self, {"files": files_data})

        elif path == "/api/deptos":
            file_name = qs.get("file", [TIC_FILE])[0]
            stem = Path(file_name).stem.lower().replace("-", "_")
            vname = f"v_{stem}"
            try:
                cols = list(DB.execute(f"DESCRIBE {vname}").df()["column_name"])
                col_depto = _col(cols, ["departamento", "departamento_entidad"])
                deptos = [r[0] for r in DB.execute(f"SELECT DISTINCT {col_depto} FROM {vname} WHERE {col_depto} IS NOT NULL ORDER BY 1").fetchall()]
                _json(self, {"deptos": deptos})
            except Exception as e:
                _json(self, {"error": str(e)})

        elif path == "/api/tic_dashboard":
            file_name = qs.get("file", [TIC_FILE])[0]
            stem = Path(file_name).stem.lower().replace("-", "_")
            vname = f"v_{stem}"

            try:
                cols_df = DB.execute(f"DESCRIBE {vname}").df()
                cols = list(cols_df["column_name"])
            except Exception as e:
                _json(self, {"error": str(e)}, 400); return

            col_monto = _col(cols, ["valor_contrato", "valor_del_contrato", "cuantia_contrato"])
            col_depto = _col(cols, ["departamento", "departamento_entidad"])
            col_mod   = _col(cols, ["modalidad", "modalidad_de_contratacion"])
            col_prov  = _col(cols, ["proveedor", "proveedor_adjudicado", "nom_razon_social_contratista"])
            col_anio  = _col(cols, ["anio_contratacion", "anno_cargue_secop"])
            col_sub   = "subsector_tic" if "subsector_tic" in cols else None
            col_tipo  = _col(cols, ["tipo_contrato", "tipo_de_contrato"]) if any(k in cols for k in ["tipo_contrato", "tipo_de_contrato"]) else None

            where = _build_where(qs, col_monto, col_anio, col_depto, col_sub, col_tipo)

            try:
                kpis = DB.execute(f"""
                    SELECT count(*) AS total_contratos,
                           COALESCE(sum({col_monto}), 0.0) AS total_cop,
                           COALESCE(avg({col_monto}), 0.0) AS media_cop,
                           COALESCE(median({col_monto}), 0.0) AS mediana_cop,
                           COALESCE(round(sum({col_monto})/1e9,2), 0.0) AS total_mm,
                           COALESCE(round(avg({col_monto})/1e6,2), 0.0) AS media_m,
                           COALESCE(round(median({col_monto})/1e6,2), 0.0) AS mediana_m
                    FROM {vname} WHERE {where}
                """).df().to_dict("records")[0]

                anual = DB.execute(f"""
                    SELECT CAST({col_anio} AS VARCHAR) AS anio,
                           round(sum({col_monto})/1e9,2) AS monto_mm,
                           count(*) AS contratos
                    FROM {vname} WHERE {where} AND {col_anio} IS NOT NULL
                    GROUP BY 1 ORDER BY 1
                """).df().to_dict("records")

                # Pronóstico de los tres años siguientes al último año histórico filtrado.
                proyeccion = []
                historico = [
                  {"anio": int(float(row["anio"])), "monto_mm": float(row["monto_mm"]), "es_proy": False}
                  for row in anual
                  if str(row["anio"]).replace(".", "", 1).isdigit()
                ]
                ultimo_anio = max((row["anio"] for row in historico), default=None)
                if len(historico) >= 2 and ultimo_anio is not None:
                  years = np.array([row["anio"] for row in historico], dtype=float)
                  amounts = np.array([row["monto_mm"] for row in historico], dtype=float)
                  slope, intercept = np.polyfit(years, amounts, 1)
                  proyeccion = historico.copy()
                  previous_amount = historico[-1]["monto_mm"]
                  for future_year in range(ultimo_anio + 1, ultimo_anio + 4):
                    estimate = max(0.0, float(np.polyval([slope, intercept], future_year)))
                    variation = ((estimate / previous_amount) - 1) * 100 if previous_amount else 0.0
                    proyeccion.append({
                      "anio": future_year,
                      "monto_mm": round(estimate, 2),
                      "variacion_pct": round(variation, 1),
                      "es_proy": True
                    })
                    previous_amount = estimate

                if col_sub:
                    subsectores = DB.execute(f"""
                        SELECT COALESCE({col_sub},'Sin clasificar') AS subsector,
                                count(*) AS contratos,
                                round(sum({col_monto})/1e9,2) AS monto_mm
                        FROM {vname} WHERE {where}
                        GROUP BY 1 ORDER BY monto_mm DESC
                    """).df().to_dict("records")

                    hhi = DB.execute(f"""
                        WITH st AS (SELECT {col_sub} AS s, sum({col_monto}) AS tot FROM {vname} WHERE {where} GROUP BY 1),
                             ps AS (SELECT {col_sub} AS s, {col_prov} AS p, sum({col_monto}) AS mp FROM {vname} WHERE {where} GROUP BY 1,2)
                        SELECT ps.s AS subsector, round(sum(POWER(ps.mp*100.0/st.tot,2)),1) AS hhi
                        FROM ps JOIN st ON ps.s=st.s GROUP BY 1 ORDER BY hhi DESC
                    """).df().to_dict("records")
                else:
                    subsectores, hhi = [], []

                proveedores = DB.execute(f"""
                    SELECT COALESCE({col_prov},'ANÓNIMO') AS proveedor,
                           round(sum({col_monto})/1e9,2) AS monto_mm,
                           count(*) AS contratos
                    FROM {vname} WHERE {where} AND {col_prov} IS NOT NULL
                    GROUP BY 1 ORDER BY monto_mm DESC LIMIT 8
                """).df().to_dict("records")

                deptos = DB.execute(f"""
                    SELECT COALESCE({col_depto},'NO DEFINIDO') AS depto,
                           count(*) AS contratos,
                           round(sum({col_monto})/1e9,2) AS monto_mm
                    FROM {vname} WHERE {where}
                    GROUP BY 1 ORDER BY monto_mm DESC LIMIT 8
                """).df().to_dict("records")

                modalidades = DB.execute(f"""
                    SELECT COALESCE({col_mod},'OTRA') AS modalidad,
                           count(*) AS contratos
                    FROM {vname} WHERE {where}
                    GROUP BY 1 ORDER BY contratos DESC LIMIT 7
                """).df().to_dict("records")

                _json(self, {
                    "kpis": kpis,
                    "anual": anual,
                    "proyeccion": proyeccion,
                    "ultimo_anio": ultimo_anio,
                    "modelo_proyeccion": "Tendencia lineal sobre años históricos filtrados",
                    "subsectores": subsectores,
                    "hhi": hhi,
                    "proveedores": proveedores,
                    "deptos": deptos,
                    "modalidades": modalidades
                })
            except Exception as e:
                _json(self, {"error": str(e)})

        elif path == "/api/mapa":
            file_name = qs.get("file", [TIC_FILE])[0]
            stem = Path(file_name).stem.lower().replace("-", "_")
            vname = f"v_{stem}"

            try:
                cols = list(DB.execute(f"DESCRIBE {vname}").df()["column_name"])
                col_monto = _col(cols, ["valor_contrato", "valor_del_contrato", "cuantia_contrato"])
                col_depto = _col(cols, ["departamento", "departamento_entidad"])

                data = DB.execute(f"""
                    SELECT COALESCE({col_depto},'NO DEFINIDO') AS depto,
                           count(*) AS contratos,
                           COALESCE(sum({col_monto}), 0.0) AS total_cop,
                           COALESCE(avg({col_monto}), 0.0) AS promedio_cop,
                           round(sum({col_monto})/1e9,2) AS monto_mm,
                           round(avg({col_monto})/1e6,2) AS promedio_m
                    FROM {vname}
                    WHERE {col_monto} > 0 AND {col_depto} IS NOT NULL
                    GROUP BY 1 ORDER BY monto_mm DESC
                """).df().to_dict("records")
                _json(self, data)
            except Exception as e:
                _json(self, {"error": str(e)})

        elif path == "/api/depto_detail":
            file_name = qs.get("file", [TIC_FILE])[0]
            depto = qs.get("depto", ["ANTIOQUIA"])[0].replace("'", "''")
            stem = Path(file_name).stem.lower().replace("-", "_")
            vname = f"v_{stem}"

            try:
                cols = list(DB.execute(f"DESCRIBE {vname}").df()["column_name"])
                col_monto = _col(cols, ["valor_contrato", "valor_del_contrato", "cuantia_contrato"])
                col_depto = _col(cols, ["departamento", "departamento_entidad"])
                col_mun   = _col(cols, ["municipio", "ciudad"])
                col_ent   = _col(cols, ["nombre_entidad", "entidad"])
                col_prov  = _col(cols, ["proveedor", "proveedor_adjudicado"])
                col_sub   = "subsector_tic" if "subsector_tic" in cols else "None"
                col_id    = _col(cols, ["id_contrato_global", "id_contrato", "proceso_compra"])

                kpis = DB.execute(f"""
                    SELECT count(*) AS total_contratos,
                           COALESCE(sum({col_monto}), 0.0) AS total_cop,
                           COALESCE(avg({col_monto}), 0.0) AS promedio_cop,
                           COALESCE(median({col_monto}), 0.0) AS mediana_cop,
                           COALESCE(round(sum({col_monto})/1e9, 2), 0.0) AS total_mm,
                           COALESCE(round(avg({col_monto})/1e6, 2), 0.0) AS promedio_m,
                           COALESCE(round(median({col_monto})/1e6, 2), 0.0) AS mediana_m
                    FROM {vname}
                    WHERE {col_depto} = '{depto}' AND {col_monto} > 0
                """).df().to_dict("records")[0]

                municipios = DB.execute(f"""
                    SELECT COALESCE({col_mun}, 'NO DEFINIDO') AS municipio,
                           count(*) AS contratos,
                           COALESCE(sum({col_monto}), 0.0) AS total_cop,
                           round(sum({col_monto})/1e9, 2) AS monto_mm
                    FROM {vname}
                    WHERE {col_depto} = '{depto}' AND {col_monto} > 0
                    GROUP BY 1 ORDER BY monto_mm DESC LIMIT 10
                """).df().to_dict("records")

                entidades = DB.execute(f"""
                    SELECT COALESCE({col_ent}, 'NO DEFINIDA') AS entidad,
                           count(*) AS contratos,
                           COALESCE(sum({col_monto}), 0.0) AS total_cop,
                           round(sum({col_monto})/1e9, 2) AS monto_mm
                    FROM {vname}
                    WHERE {col_depto} = '{depto}' AND {col_monto} > 0
                    GROUP BY 1 ORDER BY monto_mm DESC LIMIT 10
                """).df().to_dict("records")

                top_contratos = DB.execute(f"""
                    SELECT {col_id} AS id_contrato_global,
                           {col_ent} AS nombre_entidad,
                           {col_mun} AS municipio,
                           {col_prov} AS proveedor,
                           {col_monto} AS valor_cop,
                           round({col_monto}/1e6, 2) AS valor_m,
                           {col_sub} AS subsector_tic,
                           anio_contratacion,
                           SUBSTRING(COALESCE(objeto_detallado, objeto_resumido, ''), 1, 120) AS objeto
                    FROM {vname}
                    WHERE {col_depto} = '{depto}' AND {col_monto} > 0
                    ORDER BY {col_monto} DESC LIMIT 15
                """).df().to_dict("records")

                _json(self, {
                    "depto": depto,
                    "kpis": kpis,
                    "municipios": municipios,
                    "entidades": entidades,
                    "top_contratos": top_contratos
                })
            except Exception as e:
                _json(self, {"error": str(e)}, 500)

        elif path == "/api/comparacion":
            try:
                macro_estado = {
                    2016: {"cttos": 280000, "mm": 115000.0},
                    2017: {"cttos": 295000, "mm": 128000.0},
                    2018: {"cttos": 310000, "mm": 139000.0},
                    2019: {"cttos": 325000, "mm": 147000.0},
                    2020: {"cttos": 290000, "mm": 135000.0},
                    2021: {"cttos": 330000, "mm": 158000.0},
                    2022: {"cttos": 360000, "mm": 178000.0},
                    2023: {"cttos": 375000, "mm": 192000.0},
                    2024: {"cttos": 390000, "mm": 210000.0},
                    2025: {"cttos": 185000, "mm": 86890.0},
                }

                tic_data = DB.execute("""
                    SELECT anio_contratacion AS anio,
                           count(*) AS tic_contratos,
                           round(sum(valor_contrato)/1e9, 2) AS tic_mm
                    FROM v_secop_industria_tic_2015_2025
                    WHERE valor_contrato > 0 AND anio_contratacion BETWEEN 2016 AND 2025
                    GROUP BY 1 ORDER BY 1
                """).df().to_dict("records")

                rows = []
                for t in tic_data:
                    anio = int(t["anio"])
                    m = macro_estado.get(anio, {"cttos": 300000, "mm": 150000.0})
                    tot_mm = m["mm"]
                    tot_cttos = m["cttos"]
                    tic_mm = float(t["tic_mm"])
                    tic_cttos = int(t["tic_contratos"])
                    pct = round((tic_mm * 100.0) / tot_mm, 2)

                    rows.append({
                        "anio": anio,
                        "total_contratos": tot_cttos,
                        "total_mm": tot_mm,
                        "tic_contratos": tic_cttos,
                        "tic_mm": tic_mm,
                        "pct_tic": pct
                    })
                _json(self, rows)
            except Exception as e:
                _json(self, {"error": str(e)})

        elif path == "/api/compare_options":
            dim = qs.get("dim", ["departamento"])[0]
            file_name = qs.get("file", [TIC_FILE])[0]
            stem = Path(file_name).stem.lower().replace("-", "_")
            vname = f"v_{stem}"

            try:
                if dim == "departamento":
                    rows = DB.execute(f"SELECT DISTINCT departamento AS val FROM {vname} WHERE departamento IS NOT NULL ORDER BY 1").fetchall()
                    opts = [r[0] for r in rows if r[0]]
                elif dim == "anio":
                    rows = DB.execute(f"SELECT DISTINCT anio_contratacion AS val FROM {vname} WHERE anio_contratacion IS NOT NULL ORDER BY 1").fetchall()
                    opts = [str(r[0]) for r in rows if r[0]]
                elif dim == "tipo_contrato":
                    rows = DB.execute(f"SELECT tipo_contrato AS val, count(*) as c FROM {vname} WHERE tipo_contrato IS NOT NULL GROUP BY 1 ORDER BY c DESC LIMIT 30").fetchall()
                    opts = [r[0] for r in rows if r[0]]
                elif dim == "entidad":
                    rows = DB.execute(f"SELECT nombre_entidad AS val, sum(valor_contrato) as s FROM {vname} WHERE nombre_entidad IS NOT NULL GROUP BY 1 ORDER BY s DESC LIMIT 50").fetchall()
                    opts = [r[0] for r in rows if r[0]]
                elif dim == "proveedor":
                    rows = DB.execute(f"SELECT proveedor AS val, sum(valor_contrato) as s FROM {vname} WHERE proveedor IS NOT NULL GROUP BY 1 ORDER BY s DESC LIMIT 50").fetchall()
                    opts = [r[0] for r in rows if r[0]]
                elif dim == "subsector":
                    rows = DB.execute(f"SELECT DISTINCT subsector_tic AS val FROM {vname} WHERE subsector_tic IS NOT NULL ORDER BY 1").fetchall()
                    opts = [r[0] for r in rows if r[0]]
                else:
                    opts = []
                _json(self, {"dim": dim, "options": opts})
            except Exception as e:
                _json(self, {"error": str(e)})

        elif path == "/api/compare":
            dim = qs.get("dim", ["departamento"])[0]
            item_a = qs.get("item_a", ["ANTIOQUIA"])[0].replace("'", "''")
            item_b = qs.get("item_b", ["VALLE DEL CAUCA"])[0].replace("'", "''")
            file_name = qs.get("file", [TIC_FILE])[0]
            stem = Path(file_name).stem.lower().replace("-", "_")
            vname = f"v_{stem}"

            dim_cols = {
                "departamento": "departamento",
                "anio": "CAST(anio_contratacion AS VARCHAR)",
                "tipo_contrato": "tipo_contrato",
                "entidad": "nombre_entidad",
                "proveedor": "proveedor",
                "subsector": "subsector_tic"
            }
            col_expr = dim_cols.get(dim, "departamento")

            try:
                def get_kpis_for_item(val):
                    sql = f"""
                        SELECT count(*) AS total_contratos,
                               COALESCE(sum(valor_contrato), 0.0) AS total_cop,
                               COALESCE(avg(valor_contrato), 0.0) AS media_cop,
                               COALESCE(median(valor_contrato), 0.0) AS mediana_cop,
                               COALESCE(round(avg(dias_adicionados), 1), 0.0) AS dias_prom,
                               COALESCE(round(avg(flag_innovacion_conpes)*100, 1), 0.0) AS pct_innovacion,
                               COALESCE(round(avg(es_pyme_bin)*100, 1), 0.0) AS pct_pyme,
                               COALESCE(sum(CASE WHEN lower(modalidad) LIKE '%directa%' THEN 1 ELSE 0 END), 0) AS cttos_directa
                        FROM {vname}
                        WHERE {col_expr} = '{val}' AND valor_contrato > 0
                    """
                    row = DB.execute(sql).df().to_dict("records")[0]
                    return row

                kpis_a = get_kpis_for_item(item_a)
                kpis_b = get_kpis_for_item(item_b)

                subs_a = DB.execute(f"""
                    SELECT COALESCE(subsector_tic, 'Sin clasificar') as sub, round(sum(valor_contrato)/1e9, 2) as monto_mm
                    FROM {vname} WHERE {col_expr} = '{item_a}' AND valor_contrato > 0
                    GROUP BY 1 ORDER BY 1
                """).df().set_index('sub')['monto_mm'].to_dict()

                subs_b = DB.execute(f"""
                    SELECT COALESCE(subsector_tic, 'Sin clasificar') as sub, round(sum(valor_contrato)/1e9, 2) as monto_mm
                    FROM {vname} WHERE {col_expr} = '{item_b}' AND valor_contrato > 0
                    GROUP BY 1 ORDER BY 1
                """).df().set_index('sub')['monto_mm'].to_dict()

                all_subs = sorted(list(set(list(subs_a.keys()) + list(subs_b.keys()))))
                chart_subs = {
                    "labels": all_subs,
                    "data_a": [subs_a.get(s, 0.0) for s in all_subs],
                    "data_b": [subs_b.get(s, 0.0) for s in all_subs]
                }

                def calc_delta(va, vb):
                    diff = va - vb
                    pct = round((diff / vb * 100.0), 1) if vb != 0 else 0.0
                    return {"diff": diff, "pct": pct}

                deltas = {
                    "total_cop": calc_delta(kpis_a["total_cop"], kpis_b["total_cop"]),
                    "total_contratos": calc_delta(kpis_a["total_contratos"], kpis_b["total_contratos"]),
                    "media_cop": calc_delta(kpis_a["media_cop"], kpis_b["media_cop"]),
                    "mediana_cop": calc_delta(kpis_a["mediana_cop"], kpis_b["mediana_cop"]),
                    "dias_prom": calc_delta(kpis_a["dias_prom"], kpis_b["dias_prom"]),
                    "pct_innovacion": round(kpis_a["pct_innovacion"] - kpis_b["pct_innovacion"], 1)
                }

                max_tot = max(kpis_a["total_cop"], kpis_b["total_cop"], 1)
                max_ctt = max(kpis_a["total_contratos"], kpis_b["total_contratos"], 1)
                max_med = max(kpis_a["media_cop"], kpis_b["media_cop"], 1)
                max_dias = max(kpis_a["dias_prom"], kpis_b["dias_prom"], 1)

                radar_a = [
                    round((kpis_a["total_cop"] / max_tot) * 100, 1),
                    round((kpis_a["total_contratos"] / max_ctt) * 100, 1),
                    round((kpis_a["media_cop"] / max_med) * 100, 1),
                    round(100 - min(100, (kpis_a["dias_prom"] / max_dias) * 100), 1),
                    round(min(100, kpis_a["pct_innovacion"] * 2), 1)
                ]
                radar_b = [
                    round((kpis_b["total_cop"] / max_tot) * 100, 1),
                    round((kpis_b["total_contratos"] / max_ctt) * 100, 1),
                    round((kpis_b["media_cop"] / max_med) * 100, 1),
                    round(100 - min(100, (kpis_b["dias_prom"] / max_dias) * 100), 1),
                    round(min(100, kpis_b["pct_innovacion"] * 2), 1)
                ]

                _json(self, {
                    "dim": dim,
                    "item_a": item_a,
                    "item_b": item_b,
                    "kpis_a": kpis_a,
                    "kpis_b": kpis_b,
                    "deltas": deltas,
                    "chart_subs": chart_subs,
                    "radar": {
                        "axes": ["Volumen Presupuestal", "Volumen Contratos", "Cuantía Promedio", "Eficiencia en Plazos", "Intensidad Innovación 4IR"],
                        "data_a": radar_a,
                        "data_b": radar_b
                    }
                })
            except Exception as e:
                _json(self, {"error": str(e)}, 500)

        elif path == "/api/reports/preview":
            tipo = qs.get("tipo", ["territorial"])[0]
            depto = qs.get("depto", [""])[0]
            anio_d = qs.get("anio_desde", [""])[0]
            anio_h = qs.get("anio_hasta", [""])[0]
            try:
                data = generate_report_preview_data(tipo, depto, anio_d, anio_h)
                _json(self, data)
            except Exception as e:
                _json(self, {"error": str(e)}, 500)

        elif path == "/api/reports/pdf":
            tipo = qs.get("tipo", ["territorial"])[0]
            depto = qs.get("depto", [""])[0]
            anio_d = qs.get("anio_desde", [""])[0]
            anio_h = qs.get("anio_hasta", [""])[0]
            try:
                pdf_bytes = generate_executive_pdf(tipo, depto, anio_d, anio_h)
                self.send_response(200)
                self.send_header("Content-Type", "application/pdf")
                filename = f"Reporte_SECOP_{tipo}_{depto or 'Nacional'}.pdf".replace(" ", "_")
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.send_header("Content-Length", str(len(pdf_bytes)))
                self.end_headers()
                self.wfile.write(pdf_bytes)
                return
            except Exception as e:
                _json(self, {"error": str(e)}, 500)

        elif path == "/api/preview":
            file_name = qs.get("file", [TIC_FILE])[0]
            stem = Path(file_name).stem.lower().replace("-", "_")
            vname = f"v_{stem}"
            try:
                page = max(1, int(qs.get("page", [1])[0]))
                page_size = max(10, min(1000, int(qs.get("page_size", [100])[0])))
                search_q = qs.get("q", [""])[0].strip()
                offset = (page - 1) * page_size

                cols_df = DB.execute(f"DESCRIBE {vname}").df()
                cols = list(cols_df["column_name"])

                where_clause = ""
                if search_q:
                    safe_q = search_q.replace("'", "''").lower()
                    conditions = []
                    for c in cols:
                        if any(k in c.lower() for k in ["objeto", "proveedor", "entidad", "departamento", "municipio", "modalidad", "id_contrato"]):
                            conditions.append(f"lower(CAST({c} AS VARCHAR)) LIKE '%{safe_q}%'")
                    if conditions:
                        where_clause = "WHERE (" + " OR ".join(conditions) + ")"

                total_matched = DB.execute(f"SELECT count(*) FROM {vname} {where_clause}").fetchone()[0]
                df = DB.execute(f"SELECT * FROM {vname} {where_clause} LIMIT {page_size} OFFSET {offset}").df()
                
                total_pages = max(1, (total_matched + page_size - 1) // page_size)
                _json(self, {
                    "columns": list(df.columns),
                    "rows": df.to_dict("records"),
                    "total_matched": total_matched,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages
                })
            except Exception as e:
                _json(self, {"error": str(e)})

        elif path == "/api/analytics_studio":
            file_name = qs.get("file", [TIC_FILE])[0]
            dim = qs.get("dim", ["proveedor"])[0]
            metric = qs.get("metric", ["sum_monto"])[0]
            depto = qs.get("depto", [""])[0].replace("'", "''")
            limit = int(qs.get("limit", [15])[0])
            sort_dir = qs.get("sort", ["desc"])[0].upper()
            if sort_dir not in ["ASC", "DESC"]: sort_dir = "DESC"

            stem = Path(file_name).stem.lower().replace("-", "_")
            vname = f"v_{stem}"

            dim_map = {
                'proveedor': ("COALESCE(proveedor, 'ANÓNIMO')", "Proveedor / Contratista"),
                'entidad': ("COALESCE(nombre_entidad, 'NO DEFINIDA')", "Entidad Compradora"),
                'departamento': ("COALESCE(departamento, 'NO DEFINIDO')", "Departamento"),
                'municipio': ("COALESCE(municipio, 'NO DEFINIDO')", "Municipio"),
                'subsector': ("COALESCE(subsector_tic, 'Sin clasificar')", "Subsector TIC"),
                'tipo_contrato': ("COALESCE(tipo_contrato, 'NO DEFINIDO')", "Tipo de Contrato"),
                'modalidad': ("COALESCE(modalidad, 'OTRA')", "Modalidad de Selección"),
                'anio': ("CAST(anio_contratacion AS VARCHAR)", "Año de Contratación"),
                'pyme': ("CASE WHEN es_pyme_bin = 1 THEN 'MiPyME TIC' ELSE 'Gran Empresa / No PyME' END", "Clasificación Empresarial")
            }

            col_expr, dim_title = dim_map.get(dim, ("COALESCE(proveedor, 'ANÓNIMO')", "Proveedor"))

            clauses = ["valor_contrato > 0"]
            if depto:
                clauses.append(f"departamento = '{depto}'")
            where = " AND ".join(clauses)

            order_col_map = {
                'sum_monto': ("total_mm", "Monto Total (MM COP)"),
                'count_cttos': ("contratos", "Número de Contratos"),
                'avg_monto': ("prom_m", "Promedio por Contrato (M COP)"),
                'median_monto': ("mediana_m", "Mediana de Cuantía (M COP)"),
                'avg_dias': ("dias_prom", "Días de Prórroga Promedio")
            }

            order_col, metric_title = order_col_map.get(metric, ("total_mm", "Monto Total (MM COP)"))

            try:
                sql = f"""
                    SELECT {col_expr} AS etiqueta,
                           count(*) AS contratos,
                           COALESCE(round(sum(valor_contrato)/1e9, 2), 0.0) AS total_mm,
                           COALESCE(round(avg(valor_contrato)/1e6, 2), 0.0) AS prom_m,
                           COALESCE(round(median(valor_contrato)/1e6, 2), 0.0) AS mediana_m,
                           COALESCE(round(avg(dias_adicionados), 1), 0.0) AS dias_prom
                    FROM {vname}
                    WHERE {where}
                    GROUP BY 1
                    ORDER BY {order_col} {sort_dir}
                    LIMIT {limit}
                """
                df = DB.execute(sql).df()

                total_mm_sum = float(df['total_mm'].sum()) if 'total_mm' in df else 0.0
                leader = str(df.iloc[0]['etiqueta']) if len(df) > 0 else "—"

                _json(self, {
                    "dim": dim,
                    "metric": metric,
                    "title": f"Análisis de {dim_title} según {metric_title}",
                    "subtitle": f"Agrupación multidimensional {f'en {depto}' if depto else 'a nivel nacional'}",
                    "columns": ["etiqueta", "contratos", "total_mm", "prom_m", "mediana_m", "dias_prom"],
                    "rows": df.to_dict("records"),
                    "chart_labels": df['etiqueta'].astype(str).tolist(),
                    "chart_data": df[order_col].tolist(),
                    "metric_label": metric_title,
                    "total_mm": total_mm_sum,
                    "leader_name": leader
                })
            except Exception as e:
                _json(self, {"error": str(e)}, 500)

        elif path == "/api/ml_real":
            try:
                df_ml = DB.execute("SELECT * FROM v_secop_tic_ml_features").df()
                from sklearn.cluster import KMeans
                from sklearn.ensemble import IsolationForest

                X = df_ml[['log_valor', 'dias_adicionados', 'flag_prorroga', 'flag_innovacion_conpes']].fillna(0)
                
                # 1. K-Means
                km = KMeans(n_clusters=4, random_state=42, n_init=5, max_iter=20)
                df_ml['cluster'] = km.fit_predict(X)

                # 2. Isolation Forest
                iso = IsolationForest(contamination=0.03, random_state=42, n_estimators=60)
                df_ml['anom_flag'] = iso.fit_predict(X)
                df_ml['score_anom'] = -iso.score_samples(X)

                labels_map = {
                    0: "Operación & Soporte Estándar",
                    1: "Contrataciones con Prórroga Moderada",
                    2: "Procesos Críticos con Alta Adición Temporal",
                    3: "Megacontratos Tecnológicos Centralizados"
                }

                clusters_summary = []
                for c in sorted(df_ml['cluster'].unique()):
                    sub = df_ml[df_ml['cluster'] == c]
                    clusters_summary.append({
                        "cluster": int(c),
                        "label": labels_map.get(int(c), f"Perfil {c}"),
                        "count": int(len(sub)),
                        "monto_total_cop": float(sub['valor_contrato'].sum()),
                        "promedio_cop": float(sub['valor_contrato'].mean()),
                        "monto_total_mm": float(round(sub['valor_contrato'].sum() / 1e9, 2)),
                        "promedio_m": float(round(sub['valor_contrato'].mean() / 1e6, 2)),
                        "adiciones_dias_prom": float(round(sub['dias_adicionados'].mean(), 1)),
                        "pct_innovacion": float(round(sub['flag_innovacion_conpes'].mean() * 100, 1))
                    })

                # Top anomalías reales
                anomalias_list = df_ml[df_ml['anom_flag'] == -1].sort_values('valor_contrato', ascending=False)[
                    ['id_contrato_global', 'subsector_tic', 'valor_contrato', 'departamento', 'modalidad', 'dias_adicionados', 'score_anom']
                ].head(12).to_dict('records')

                # Indicadores CONPES 4069
                conpes_sub = df_ml[df_ml['flag_innovacion_conpes'] == 1]
                monto_conpes_raw = float(conpes_sub['valor_contrato'].sum())
                monto_conpes_mm = float(round(monto_conpes_raw / 1e9, 2))
                pct_conpes = round((len(conpes_sub) / len(df_ml)) * 100, 1)

                # Desglose CONPES por Subsector
                conpes_subsectores = DB.execute("""
                    SELECT COALESCE(subsector_tic, 'Sin clasificar') AS subsector,
                           count(*) AS contratos,
                           round(sum(valor_contrato)/1e9, 2) AS monto_mm
                    FROM v_secop_tic_ml_features
                    WHERE flag_innovacion_conpes = 1
                    GROUP BY 1 ORDER BY monto_mm DESC
                """).df().to_dict('records')

                # Matriz de Correlación de Pearson
                corr_dict = df_ml[['log_valor', 'dias_adicionados', 'flag_prorroga', 'flag_innovacion_conpes']].corr().round(3).to_dict()

                # Proyecciones multianuales (Regresión 2016-2025 + 2026-2027)
                anual_hist = DB.execute("""
                    SELECT anio_contratacion AS anio, round(sum(valor_contrato)/1e9, 2) AS monto_mm
                    FROM v_secop_industria_tic_2015_2025
                    WHERE anio_contratacion BETWEEN 2016 AND 2025
                    GROUP BY 1 ORDER BY 1
                """).df()

                years = anual_hist['anio'].values.astype(float)
                montos = anual_hist['monto_mm'].values.astype(float)
                poly = np.polyfit(years, montos, 1)

                proyecciones = []
                for _, r in anual_hist.iterrows():
                    proyecciones.append({"anio": int(r['anio']), "monto_mm": float(r['monto_mm']), "es_proy": False})

                proj_2026 = float(round(np.polyval(poly, 2026), 2))
                proj_2027 = float(round(np.polyval(poly, 2027), 2))
                last_amount = float(anual_hist.iloc[-1]['monto_mm']) if len(anual_hist) else 0.0
                variation_2026 = ((proj_2026 / last_amount) - 1) * 100 if last_amount else 0.0
                variation_2027 = ((proj_2027 / proj_2026) - 1) * 100 if proj_2026 else 0.0
                proyecciones.append({"anio": 2026, "monto_mm": proj_2026, "variacion_pct": round(variation_2026, 1), "es_proy": True})
                proyecciones.append({"anio": 2027, "monto_mm": proj_2027, "variacion_pct": round(variation_2027, 1), "es_proy": True})

                _json(self, {
                    "clusters": clusters_summary,
                    "anomalias": anomalias_list,
                    "pct_conpes": pct_conpes,
                    "monto_conpes_raw": monto_conpes_raw,
                    "monto_conpes_mm": monto_conpes_mm,
                    "total_contratos_tic": len(df_ml),
                    "conpes_subsectores": conpes_subsectores,
                    "correlaciones": corr_dict,
                    "proyecciones": proyecciones
                })
            except Exception as e:
                _json(self, {"error": str(e)}, 500)

        else:
            self.send_error(404, "Not Found")

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/query":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            file_name = body.get("file", "")
            raw_sql = body.get("sql", "").strip()
            stem = Path(file_name).stem.lower().replace("-", "_")
            vname = f"v_{stem}"
            
            sql = raw_sql.replace("datos", vname).replace("{table}", vname)
            try:
                cursor = DB.execute(sql)
                cols = [d[0] for d in cursor.description] if cursor.description else []
                full_df = cursor.fetch_df()
                total_count = len(full_df)
                rows = full_df.head(1000).to_dict("records")
                _json(self, {
                    "columns": cols,
                    "rows": rows,
                    "total_count": total_count,
                    "displayed_count": len(rows)
                })
            except Exception as e:
                _json(self, {"error": str(e)}, 400)

        elif parsed.path == "/api/semantico/evaluar":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                
                objeto = body.get("objeto", "")
                entidad = body.get("entidad", "")
                modalidad = body.get("modalidad", "")
                valor_cop = float(body.get("valor_cop", 0))
                api_key = body.get("api_key", None)
                
                resultado = evaluar_idoneidad_contrato(
                    objeto_contrato=objeto,
                    entidad=entidad,
                    modalidad=modalidad,
                    valor_cop=valor_cop,
                    api_key=api_key
                )
                _json(self, resultado)
            except Exception as e:
                _json(self, {"error": str(e)}, 500)

        elif parsed.path in ("/api/actualizar-contratos-tic", "/api/ingestion/run"):
            try:
                length = int(self.headers.get("Content-Length", 0) or 0)
                body = {}
                if length:
                    try:
                        body = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                    except Exception:
                        body = {}
                lookback = int(body.get("lookback_days") or 180)
                max_pages = int(body.get("max_pages") or 24)
                page_size = int(body.get("page_size") or 400)
                result = kick_tic_ingest(
                    lookback_days=lookback,
                    max_pages=max_pages,
                    page_size=page_size,
                )
                _json(self, result, 202 if result.get("accepted") else 200)
            except Exception as e:
                _json(self, {"error": str(e), "accepted": False}, 500)

        else:
            self.send_error(404, "Not Found")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

def run(port=PORT):
    print("Iniciando base de datos DuckDB...")
    init_db()
    try:
        from ingestion.scheduler import start_background_scheduler
        start_background_scheduler()
        print("Scheduler de ingesta TIC en segundo plano (cada 12 h).")
    except Exception as e:
        print(f"Scheduler no iniciado: {e}")
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Servidor SECOP Analytics activo en http://0.0.0.0:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor detenido.")

if __name__ == "__main__":
    p = int(sys.argv[1]) if len(sys.argv) > 1 else PORT
    run(p)
