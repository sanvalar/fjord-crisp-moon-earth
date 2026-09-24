"""
Downloader paginado por bloques con checkpoints para SECOP I (f789-7hwg).
Filtra por anno_cargue_secop (2015 - 2025) y pagina de forma determinista por :id.
"""
import json
import logging
import time
from pathlib import Path
from secop_i.config import CHECKPOINTS_DIR, CHUNK_SIZE, ANNO_MIN, ANNO_MAX
from secop_i.api import SecopIAPIClient

logger = logging.getLogger("secop_i.downloader")

class SecopIDownloader:
    def __init__(self, client: SecopIAPIClient = None, checkpoint_file: Path = None):
        self.client = client or SecopIAPIClient()
        self.checkpoint_file = checkpoint_file or (CHECKPOINTS_DIR / "downloader_secop_i_checkpoint.json")
        self.state = self._load_checkpoint()

    def _load_checkpoint(self):
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"No se pudo leer checkpoint SECOP I: {e}")
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

    def reset_checkpoint(self):
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        self.state = {
            "last_offset": 0,
            "total_downloaded": 0,
            "completed": False,
            "blocks_downloaded": 0
        }

    def stream_batches(self, chunk_size: int = CHUNK_SIZE, max_records: int = None,
                       anno_min: int = ANNO_MIN, anno_max: int = ANNO_MAX,
                       resume: bool = True):
        if not resume:
            self.reset_checkpoint()

        offset = self.state["last_offset"]
        total_retrieved = self.state["total_downloaded"]

        where_clause = f"anno_cargue_secop >= {anno_min} AND anno_cargue_secop <= {anno_max}"
        logger.info(f"Iniciando descarga SECOP I. Offset: {offset}, chunk_size: {chunk_size}, filtro: [{where_clause}]")

        while True:
            actual_limit = chunk_size
            if max_records is not None:
                remaining = max_records - total_retrieved
                if remaining <= 0:
                    break
                if remaining < actual_limit:
                    actual_limit = remaining

            query = f"SELECT * WHERE {where_clause} ORDER BY :id LIMIT {actual_limit} OFFSET {offset}"

            logger.info(f"Descargando bloque SECOP I: offset={offset}, limit={actual_limit}...")
            start_t = time.time()
            batch = self.client.execute_query(query)
            elapsed = time.time() - start_t

            records_count = len(batch) if isinstance(batch, list) else 0

            if records_count == 0:
                logger.info(f"Fin de datos SECOP I alcanzado en offset {offset}.")
                self.save_checkpoint(offset=offset, total_downloaded=total_retrieved, completed=True)
                break

            total_retrieved += records_count
            offset += records_count
            self.save_checkpoint(offset=offset, total_downloaded=total_retrieved, completed=False)

            logger.info(f"Bloque SECOP I: {records_count} filas en {elapsed:.2f}s. Acumulado: {total_retrieved:,}")
            yield batch

            if max_records and total_retrieved >= max_records:
                break
