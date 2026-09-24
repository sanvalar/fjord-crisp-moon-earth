"""
Módulo de descarga y paginación resiliente para la API de SECOP II.
Descarga los datos por bloques ordenados por :id utilizando SoQL, guarda checkpoints
y permite pausar y continuar sin duplicación ni pérdida de progreso.
"""
import json
import logging
import time
from pathlib import Path
from secop_ii.config import CHECKPOINTS_DIR, CHUNK_SIZE, FECHA_MIN, FECHA_MAX
from secop_ii.api import SecopAPIClient

logger = logging.getLogger("secop_ii.downloader")

class SecopDownloader:
    def __init__(self, client: SecopAPIClient = None, checkpoint_file: Path = None):
        self.client = client or SecopAPIClient()
        self.checkpoint_file = checkpoint_file or (CHECKPOINTS_DIR / "downloader_checkpoint.json")
        self.state = self._load_checkpoint()

    def _load_checkpoint(self):
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    logger.info(f"Checkpoint recuperado: {data}")
                    return data
            except Exception as e:
                logger.warning(f"No se pudo leer checkpoint anterior: {e}")
        return {
            "last_offset": 0,
            "total_downloaded": 0,
            "completed": False,
            "blocks_downloaded": 0
        }

    def save_checkpoint(self, offset: int, total_downloaded: int, completed: bool = False):
        self.state["last_offset"] = offset
        self.state["total_downloaded"] = total_downloaded
        self.state["completed"] = completed
        self.state["blocks_downloaded"] += 1
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2)
        logger.debug(f"Checkpoint guardado: offset={offset}, total={total_downloaded}")

    def reset_checkpoint(self):
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        self.state = {
            "last_offset": 0,
            "total_downloaded": 0,
            "completed": False,
            "blocks_downloaded": 0
        }
        logger.info("Checkpoint reiniciado.")

    def stream_batches(self, chunk_size: int = CHUNK_SIZE, max_records: int = None, 
                       fecha_inicio: str = FECHA_MIN, fecha_fin: str = FECHA_MAX,
                       resume: bool = True):
        """
        Generador que entrega bloques de registros sucesivamente desde la API.
        Usa paginación estable con ORDER BY :id LIMIT chunk_size OFFSET offset.
        """
        if not resume:
            self.reset_checkpoint()

        offset = self.state["last_offset"]
        total_retrieved = self.state["total_downloaded"]
        
        # Clausula de filtro temporal
        where_conditions = []
        if fecha_inicio:
            where_conditions.append(f"fecha_de_firma >= '{fecha_inicio}'")
        if fecha_fin:
            where_conditions.append(f"fecha_de_firma <= '{fecha_fin}'")
        where_clause = " AND ".join(where_conditions) if where_conditions else ""

        logger.info(f"Iniciando descarga en bloques. Offset inicial: {offset}, chunk_size: {chunk_size}, filtro: [{where_clause}]")

        while True:
            # Control de límite de prueba
            actual_limit = chunk_size
            if max_records is not None:
                remaining = max_records - total_retrieved
                if remaining <= 0:
                    logger.info(f"Alcanzado límite solicitado de {max_records} registros.")
                    break
                if remaining < actual_limit:
                    actual_limit = remaining

            query = f"SELECT * "
            if where_clause:
                query += f"WHERE {where_clause} "
            query += f"ORDER BY :id LIMIT {actual_limit} OFFSET {offset}"

            logger.info(f"Descargando bloque: offset={offset}, limit={actual_limit}...")
            start_t = time.time()
            try:
                batch = self.client.execute_query(query)
            except Exception as e:
                logger.error(f"Fallo en bloque offset={offset}: {e}. El progreso hasta el momento queda resguardado.")
                raise

            elapsed = time.time() - start_t
            records_count = len(batch) if isinstance(batch, list) else 0

            if records_count == 0:
                logger.info(f"Fin de datos alcanzado (respuesta vacía en offset {offset}).")
                self.save_checkpoint(offset=offset, total_downloaded=total_retrieved, completed=True)
                break

            total_retrieved += records_count
            offset += records_count
            self.save_checkpoint(offset=offset, total_downloaded=total_retrieved, completed=False)

            logger.info(f"Bloque recibido: {records_count} filas en {elapsed:.2f}s ({records_count/max(elapsed, 0.001):.1f} filas/s). Total acumulado: {total_retrieved}")
            
            yield batch

            if max_records and total_retrieved >= max_records:
                logger.info(f"Descarga de muestra completada con éxito ({total_retrieved} registros).")
                break
