"""
Función lambda de upsert sobre la base TIC.

Si el contrato ya existe (misma llave primaria) se consulta y se actualiza.
Si no existe, se registra (inserta). Nunca duplica el id_contrato_global.
"""
from __future__ import annotations

from typing import Callable, Iterable, Mapping, MutableMapping, Sequence

# Lambda pedida: decide si la fila se actualiza o se registra.
lambda_upsert: Callable[[str, Mapping[str, object]], str] = (
    lambda pk, existentes: "update" if pk in existentes else "insert"
)


def aplicar_lambda_upsert(
    incoming: Iterable[dict],
    existentes: MutableMapping[str, dict],
    pk_field: str = "id_contrato_global",
) -> dict:
    """Aplica lambda_upsert fila a fila y muta `existentes` in-place.

    Returns:
        {inserted, updated, skipped, actions: [{pk, action}]}
    """
    inserted = 0
    updated = 0
    skipped = 0
    actions: list[dict] = []
    for rec in incoming:
        pk = str(rec.get(pk_field) or "").strip()
        if not pk:
            skipped += 1
            continue
        action = lambda_upsert(pk, existentes)
        if action == "update":
            prev = existentes[pk]
            merged = {**prev, **{k: v for k, v in rec.items() if v not in (None, "")}}
            existentes[pk] = merged
            updated += 1
        else:
            existentes[pk] = rec
            inserted += 1
        actions.append({"pk": pk, "action": action})
    return {
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "actions": actions,
    }


UNIFIED_COLUMNS: Sequence[str] = (
    "sistema_origen",
    "id_contrato_global",
    "proceso_compra",
    "nit_entidad",
    "nombre_entidad",
    "orden_gobierno",
    "departamento",
    "municipio",
    "modalidad",
    "tipo_contrato",
    "estado_contrato",
    "documento_proveedor",
    "proveedor",
    "es_pyme_bin",
    "fecha_firma",
    "fecha_inicio",
    "fecha_fin",
    "anio_contratacion",
    "valor_contrato",
    "valor_facturado",
    "valor_pagado",
    "dias_adicionados",
    "objeto_resumido",
    "objeto_detallado",
    "subsector_tic",
    "flag_innovacion_conpes",
)


def duckdb_lambda_merge_sql(parquet_path: str, dest_path: str) -> str:
    """SQL equivalente a la lambda: incoming gana si el id ya existía (UPDATE),
    si no se inserta. Deduplica por id_contrato_global."""
    cols = ", ".join(UNIFIED_COLUMNS)
    src = parquet_path.replace("\\", "/")
    dst = dest_path.replace("\\", "/")
    return f"""
    COPY (
        SELECT {cols}
        FROM (
            SELECT {cols}, 1 AS _src FROM incoming_tic
            UNION ALL
            SELECT {cols}, 0 AS _src FROM read_parquet('{src}')
        )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id_contrato_global
            ORDER BY _src DESC, fecha_firma DESC
        ) = 1
    ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
