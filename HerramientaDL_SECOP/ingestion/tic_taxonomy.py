"""Taxonomía TIC compartida: SoQL, clasificación de subsector y filtro de cuantía."""
from __future__ import annotations

TIC_TERMS = (
    "software",
    "tecnolog",
    "informatic",
    "comput",
    "conectividad",
    "telecomunicac",
    "cibersegur",
    "licenciamiento",
    "internet",
    "servidor",
    "datacenter",
    "data center",
    "hardware",
    "redes",
)

TIC_EXCLUDE = (
    "arrastre",
    "grua",
    "cárcel",
    "carcel",
    "emprestito",
    "emprstito",
)

MIN_VALOR = 100_000
MAX_VALOR = 500_000_000_000
ANIO_MIN = 2015
ANIO_MAX = 2026


def _or_likes(column: str, terms: tuple[str, ...] = TIC_TERMS) -> str:
    parts = [f"lower({column}) like '%{term}%'" for term in terms]
    return "(" + " OR ".join(parts) + ")"


def tic_where_secop_ii() -> str:
    """Filtro SoQL sobre contratos electrónicos SECOP II (jbjy-vk9h)."""
    objeto = _or_likes("objeto_del_contrato")
    desc = _or_likes("descripcion_del_proceso")
    return (
        f"({objeto} OR {desc} OR codigo_de_categoria_principal like '43%' "
        f"OR codigo_de_categoria_principal like '8111%')"
    )


def tic_where_secop_i() -> str:
    """Filtro SoQL sobre SECOP I (f789-7hwg)."""
    objeto = _or_likes("objeto_a_contratar")
    detalle = _or_likes("detalle_del_objeto_a_contratar")
    return f"({objeto} OR {detalle})"


def tic_where_procesos() -> str:
    """Filtro SoQL sobre procesos de contratación SECOP II (p6dx-8zbt)."""
    nombre = _or_likes("nombre_del_procedimiento")
    desc = _or_likes("descripcion_del_procedimiento")
    return f"({nombre} OR {desc})"


def es_tic_texto(*textos: str) -> bool:
    blob = " ".join(t or "" for t in textos).lower()
    if not blob.strip():
        return False
    if any(ex in blob for ex in TIC_EXCLUDE):
        return False
    return any(term in blob for term in TIC_TERMS)


def clasificar_subsector(*textos: str) -> str:
    blob = " ".join(t or "" for t in textos).lower()
    if any(k in blob for k in ("software", "licenci", "desarrollo de aplicac", "aplicacion")):
        return "Desarrollo de Software y Licenciamiento"
    if any(k in blob for k in ("conectividad", "internet", "telecomunicac", "redes")):
        return "Conectividad y Redes"
    if any(k in blob for k in ("cibersegur", "seguridad inform", "cloud", "nube")):
        return "Ciberseguridad e Infraestructura Cloud"
    if any(k in blob for k in ("computad", "servidor", "hardware", "equipos de c", "datacenter")):
        return "Hardware y Equipos de Cómputo"
    return "Servicios de Soporte y Consultoría TI"


def flag_innovacion(*textos: str) -> int:
    blob = " ".join(t or "" for t in textos).lower()
    keys = (
        "innovaci",
        "investigaci",
        "ciencia",
        "inteligencia artificial",
        "analitica",
        "analítica",
        "machine learning",
        "big data",
        "transformacion digital",
        "transformación digital",
    )
    return 1 if any(k in blob for k in keys) else 0
