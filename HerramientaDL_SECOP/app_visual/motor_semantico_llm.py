"""
Motor de Análisis Semántico con LLM y NLP para Evaluación de Contratos SECOP TIC.
Institución: Universidad de San Buenaventura Medellín (USBMed).
Facultad de Ingenierías (Sistemas, Multimedia, Sonido, Electrónica) & CEDETEC.
Compara el objeto contractual con el perfil de capacidades técnicas de la USBMed,
evalúa afinidad semántica y genera dictamen técnico cualitativo y estratégico.
"""
import os
import json
import re
import urllib.request
import urllib.error
from pathlib import Path
from typing import Dict, List, Any, Optional

def get_perfil_universidad() -> Dict[str, Any]:
    base_dir = Path(__file__).resolve().parent.parent
    path_json = base_dir / "data" / "perfil_universidad.json"
    if path_json.exists():
        with open(path_json, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def evaluar_idoneidad_contrato(
    objeto_contrato: str,
    entidad: str = "",
    modalidad: str = "",
    valor_cop: float = 0.0,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    perfil = get_perfil_universidad()
    contrato = {
        "objeto": objeto_contrato,
        "entidad": entidad,
        "modalidad": modalidad,
        "valor": valor_cop
    }
    return evaluar_semantica_pura(contrato, perfil, api_key)

def _normalizar_texto(texto: str) -> str:
    if not texto:
        return ""
    texto = texto.lower()
    reemplazos = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'ü': 'u', 'ñ': 'n'
    }
    for k, v in reemplazos.items():
        texto = texto.replace(k, v)
    texto = re.sub(r'[^a-z0-9\s]', ' ', texto)
    return " ".join(texto.split())

def calcular_afinidad_semantica(
    objeto_contrato: str,
    perfil: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Calcula la afinidad semántica entre el objeto del contrato y las áreas de capacidad de la USBMed.
    """
    texto_norm = _normalizar_texto(objeto_contrato)
    palabras_contrato = set(texto_norm.split())

    areas_evaluadas = []
    coincidencias_totales = []

    capacidades = perfil.get("capacidades_tecnicas", [])
    
    mejor_area = None
    mejor_score = 0.0

    for cap in capacidades:
        area_nombre = cap.get("area", "General")
        palabras_clave = cap.get("palabras_clave", [])
        peso_area = float(cap.get("peso", 1.0))
        
        matches = []
        score_acumulado = 0.0

        for kw in palabras_clave:
            kw_norm = _normalizar_texto(kw)
            if kw_norm in texto_norm:
                matches.append(kw)
                tokens_kw = kw_norm.split()
                if len(tokens_kw) > 1:
                    score_acumulado += 2.5
                else:
                    score_acumulado += 1.2
            elif any(tk in palabras_contrato for tk in kw_norm.split() if len(tk) > 4):
                score_acumulado += 0.45

        area_score = min(100.0, (score_acumulado / 5.0) * 100.0) * peso_area
        area_score = round(min(100.0, area_score), 1)

        info_area = {
            "area": area_nombre,
            "score": area_score,
            "terminos_encontrados": matches[:6],
            "num_coincidencias": len(matches),
            "grupos_investigacion": cap.get("grupos_investigacion", []),
            "laboratorios": cap.get("laboratorios", [])
        }
        areas_evaluadas.append(info_area)
        coincidencias_totales.extend(matches)

        if area_score > mejor_score:
            mejor_score = area_score
            mejor_area = info_area

    areas_evaluadas.sort(key=lambda x: x["score"], reverse=True)

    s1 = areas_evaluadas[0]["score"] if len(areas_evaluadas) > 0 else 0.0
    s2 = areas_evaluadas[1]["score"] if len(areas_evaluadas) > 1 else 0.0
    s3 = areas_evaluadas[2]["score"] if len(areas_evaluadas) > 2 else 0.0
    
    score_global = min(100.0, round((s1 * 0.60) + (s2 * 0.30) + (s3 * 0.10), 1))

    # Detección de componentes de suministro físico ajenos a la academia
    alertas_brechas = []
    if any(term in texto_norm for term in ["suministro de equipo", "compra de computadores", "suministro de computadores", "adquisicion de hardware", "baterias", "ups", "pantallas"]):
        alertas_brechas.append("El pliego exige suministro físico de hardware/computadores masivos (requiere alianza con canal mayorista o integrador comercial).")
    if any(term in texto_norm for term in ["obra civil", "cableado estructurado", "canalizacion", "torres", "mantenimiento locativo", "fibra optica canalizada"]):
        alertas_brechas.append("Exige adecuaciones físicas y obra civil (alcance fuera del objeto académico de la universidad).")
    if any(term in texto_norm for term in ["renovacion de licencias microsoft", "oracle", "sap", "licenciamiento propietario"]):
        alertas_brechas.append("Intermediación de licencias de software propietario extranjero (bajo valor agregado de I+D universitario).")

    if score_global >= 70.0:
        nivel_afinidad = "ALTA IDONEIDAD TÉCNICA"
        color_afinidad = "#10B981"
    elif score_global >= 40.0:
        nivel_afinidad = "IDONEIDAD MEDIA / REQUIERE COMPLEMENTACIÓN"
        color_afinidad = "#38BDF8"
    elif score_global >= 20.0:
        nivel_afinidad = "AFINIDAD PARCIAL O PERIFÉRICA"
        color_afinidad = "#F59E0B"
    else:
        nivel_afinidad = "FUERA DE FOCO ACADÉMICO"
        color_afinidad = "#6B7280"

    return {
        "score_global": score_global,
        "nivel_afinidad": nivel_afinidad,
        "color_afinidad": color_afinidad,
        "area_principal": mejor_area["area"] if mejor_area else "General",
        "grupos_sugeridos": mejor_area["grupos_investigacion"] if mejor_area else [],
        "laboratorios_sugeridos": mejor_area["laboratorios"] if mejor_area else [],
        "terminos_clave_detectados": list(dict.fromkeys(coincidencias_totales))[:10],
        "desglose_areas": areas_evaluadas,
        "alertas_brechas": alertas_brechas
    }

def generar_dictamen_llm(
    objeto_contrato: str,
    entidad: str,
    valor_cop: float,
    modalidad: str,
    perfil: Dict[str, Any],
    afinidad: Dict[str, Any],
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Genera dictamen estratégico del LLM adaptado a la Universidad de San Buenaventura Medellín (USBMed).
    """
    api_key = api_key or os.environ.get("GEMINI_API_KEY")

    if api_key:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
            prompt = f"""
            Eres un consultor experto en contratación pública en Colombia (SECOP, Ley 80 de 1993, Ley 1150) y transferencia tecnológica universitaria.
            Analiza el siguiente proceso contractual para la Universidad de San Buenaventura Medellín (USBMed), cuya Facultad de Ingenierías cuenta con los programas de Ingeniería de Datos y Software, Ingeniería de Sistemas Cibernéticos, Ingeniería Multimedia, Ingeniería de Sonido, Ingeniería Industrial e Ingeniería Ambiental, articulados con el Grupo de Investigación en Modelamiento y Simulación Computacional (GIMSC, Categoría A1 MinCiencias), el Grupo de Geoinformática Aplicada y el Consultorio Tecnológico Bonaventuriano (CTB).

            DATOS DEL CONTRATO:
            - Entidad Contratante: {entidad}
            - Modalidad: {modalidad}
            - Presupuesto Oficial: ${valor_cop:,.0f} COP
            - Objeto: {objeto_contrato}

            ANÁLISIS PREVIO DE CAPACIDADES USBMED:
            - Score de Compatibilidad Técnica: {afinidad['score_global']}% ({afinidad['nivel_afinidad']})
            - Área Principal Detectada: {afinidad['area_principal']}
            - Términos Clave: {', '.join(afinidad['terminos_clave_detectados'])}
            - Grupos MinCiencias USBMed Idóneos: {', '.join(afinidad['grupos_sugeridos'])}
            - Alertas de Suministro/Obra: {', '.join(afinidad['alertas_brechas']) if afinidad['alertas_brechas'] else 'Ninguna'}

            Responde ÚNICAMENTE en formato JSON:
            {{
                "dictamen_estrategico": "Recomendación ejecutiva clara para el Consejo de Facultad / Rectoría de USBMed",
                "fortalezas_institucionales": ["fortaleza técnica 1 de USBMed", "fortaleza 2", "fortaleza 3"],
                "brechas_y_riesgos": ["riesgo o brecha 1", "riesgo 2"],
                "modalidad_recomendada": "Proponente Singular (USBMed) | Unión Temporal con Integrador TIC | Consorcio | Descartar",
                "justificacion_academica": "Párrafo explicando el impacto investigativo, laboratorios de USBMed y valor social agregado"
            }}
            """
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"response_mime_type": "application/json", "temperature": 0.2}
            }
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=12) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                text_resp = data["candidates"][0]["content"]["parts"][0]["text"]
                dictamen = json.loads(text_resp)
                dictamen["motor_llm_utilizado"] = "Google Gemini 1.5 Flash (Cloud API)"
                return dictamen
        except Exception:
            pass

    # MOTOR ANALÍTICO HEURÍSTICO ESPECIALIZADO USBMED
    score = afinidad["score_global"]
    area = afinidad["area_principal"]
    terminos = afinidad["terminos_clave_detectados"]
    alertas = afinidad["alertas_brechas"]
    grupos = afinidad["grupos_sugeridos"]
    labs = afinidad.get("laboratorios_sugeridos", [])

    fortalezas = [
        f"Alineación directa con los programas de Ingeniería y la línea de investigación en '{area}' de la Universidad de San Buenaventura Medellín.",
        f"Capacidad de acreditación de idoneidad y respaldo científico a través de {grupos[0] if grupos else 'GIMSC (Categoría A1 MinCiencias)'}.",
        f"Infraestructura especializada disponible en el {labs[0] if labs else 'Consultorio Tecnológico Bonaventuriano (CTB)'}.",
        f"Dominio comprobado en requerimientos técnicos de: {', '.join(terminos[:4]) if terminos else 'ingeniería de datos, software y modelamiento'}."
    ]

    brechas = []
    if alertas:
        brechas.extend(alertas)
    if valor_cop > 5000000000:
        brechas.append("Cuantía elevada (> $5.000 Millones COP): Exige pólizas de cumplimiento cuantiosas y alto flujo de caja para anticipos.")
    if "Directa" not in modalidad and "Convenio" not in modalidad:
        brechas.append("Proceso licitatorio abierto: Requiere verificar RUP residual y acreditación de contratos anteriores ejecutados por USBMed en Antioquia o a nivel nacional.")
    if not brechas:
        brechas.append("Coordinar dedicación horaria de docentes e ingenieros investigadores para no afectar la carga académica semestral.")

    if score >= 65.0 and len(alertas) == 0:
        mod_rec = "Proponente Singular (USBMed Titular)"
        dictamen_est = (
            f"Oportunidad de alto valor estratégico para la Universidad de San Buenaventura Medellín. El objeto del contrato encaja plenamente con las fortalezas del área de {area}. "
            f"Se aconseja estructurar la propuesta técnica y económica directamente a través del Consultorio Tecnológico Bonaventuriano (CTB) y la Facultad de Ingenierías como proponente único."
        )
    elif score >= 40.0 or len(alertas) > 0:
        mod_rec = "Unión Temporal con Aliado Comercial / Integrador TIC"
        dictamen_est = (
            f"Oportunidad viable bajo la figura de Unión Temporal (60% USBMed - 40% Aliado). La Universidad de San Buenaventura Medellín debe asumir el liderazgo metodológico, desarrollo técnico y consultoría en {area}, "
            "mientras un integrador de la industria asume el suministro de hardware, garantías comerciales y logística física."
        )
    elif score >= 20.0:
        mod_rec = "Consorcio Minoritario o Asesoría Técnica"
        dictamen_est = (
            "El objeto contractual presenta bajo contenido de ingeniería especializada o investigación aplicada. "
            "Se sugiere participar únicamente como asesor o interventor técnico secundario en consorcio minoritario."
        )
    else:
        mod_rec = "Descartar / No Priorizar"
        dictamen_est = (
            "El objeto contractual no tiene pertinencia con las líneas de investigación ni los programas de ingeniería de la Universidad de San Buenaventura Medellín. "
            "Se aconseja orientar los esfuerzos hacia convocatorias de mayor impacto tecnológico y de desarrollo de software."
        )

    justificacion = (
        f"La ejecución de este proyecto en '{entidad}' consolida el posicionamiento de la Universidad de San Buenaventura Medellín como actor clave del ecosistema de innovación de Antioquia y el país, "
        f"permitiendo vincular semilleros de investigación, transferir conocimiento aplicado y fortalecer el fondo de I+D de la Facultad de Ingenierías."
    )

    return {
        "dictamen_estrategico": dictamen_est,
        "fortalezas_institucionales": fortalezas,
        "brechas_y_riesgos": brechas,
        "modalidad_recomendada": mod_rec,
        "justificacion_academica": justificacion,
        "motor_llm_utilizado": "Motor Analítico de Inteligencia Contractual USBMed (NLP Local Engine)"
    }

def evaluar_semantica_pura(
    contrato: Dict[str, Any],
    perfil: Dict[str, Any],
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Evaluación estrictamente técnica y cualitativa para la USBMed:
    Afinidad Semántica (0-100%) + Áreas coincidentes + Brechas + Dictamen LLM.
    """
    objeto = contrato.get("objeto_detallado") or contrato.get("objeto_resumido") or contrato.get("objeto") or ""
    entidad = contrato.get("nombre_entidad") or contrato.get("entidad") or "Entidad Pública"
    valor_cop = float(contrato.get("valor_contrato") or contrato.get("valor") or 0.0)
    modalidad = contrato.get("modalidad", "No Especificada")

    afinidad = calcular_afinidad_semantica(objeto, perfil)
    dictamen_llm = generar_dictamen_llm(
        objeto_contrato=objeto,
        entidad=entidad,
        valor_cop=valor_cop,
        modalidad=modalidad,
        perfil=perfil,
        afinidad=afinidad,
        api_key=api_key
    )

    score_tec = afinidad["score_global"]
    if score_tec >= 70.0:
        conclusion = "ALTA IDONEIDAD TÉCNICA"
        color_tec = "#10B981"
    elif score_tec >= 40.0:
        conclusion = "IDONEIDAD MEDIA / REQUIERE COMPLEMENTACIÓN"
        color_tec = "#38BDF8"
    else:
        conclusion = "BAJA IDONEIDAD / FUERA DE LÍNEAS PRIORITARIAS"
        color_tec = "#EF4444"

    return {
        "contrato_id": contrato.get("id_contrato_global"),
        "entidad": entidad,
        "departamento": contrato.get("departamento"),
        "objeto": objeto,
        "valor_contrato": valor_cop,
        "modalidad": modalidad,
        "score_tecnico": score_tec,
        "conclusion_tecnica": conclusion,
        "color_tecnico": color_tec,
        "analisis_semantico": afinidad,
        "dictamen_llm": dictamen_llm
    }
