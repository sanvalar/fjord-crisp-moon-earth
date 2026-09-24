"""
Módulo de comunicación HTTP resiliente con la API de Datos Abiertos de Colombia.
Incluye reintentos con backoff exponencial, headers de navegador y manejo robusto de errores.
"""
import time
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from secop_ii.config import (
    API_BASE_URL, DATASET_METADATA_URL, REQUEST_TIMEOUT, 
    MAX_RETRIES, BACKOFF_FACTOR
)

logger = logging.getLogger("secop_ii.api")

class SecopAPIClient:
    def __init__(self, timeout=REQUEST_TIMEOUT, max_retries=MAX_RETRIES):
        self.timeout = timeout
        self.session = requests.Session()
        
        # Configuración de reintentos automáticos para códigos 429 (Too Many Requests), 500, 502, 503, 504
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        self.session.headers.update({
            "User-Agent": "SecopII-DataPipeline/1.0 (Research; Python/Requests)",
            "Accept": "application/json"
        })

    def fetch_dataset_metadata(self):
        """Obtiene la metadata oficial del dataset (columnas, tipos, nombres legibles)."""
        logger.info(f"Consultando metadata oficial desde {DATASET_METADATA_URL}...")
        try:
            resp = self.session.get(DATASET_METADATA_URL, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al obtener metadata del dataset: {e}")
            raise

    def execute_query(self, soql_query: str):
        """
        Ejecuta una consulta SoQL sobre el endpoint query.json con reintentos y manejo de errores.
        """
        params = {"query": soql_query}
        logger.debug(f"Ejecutando SoQL: {soql_query}")
        
        try:
            response = self.session.get(API_BASE_URL, params=params, timeout=self.timeout)
            if response.status_code == 400:
                logger.error(f"Error 400 Bad Request. Consulta enviada: {soql_query}. Respuesta: {response.text[:300]}")
                response.raise_for_status()
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error en petición HTTP tras reintentos: {e}")
            raise

    def get_sample_rows(self, limit: int = 5):
        """Obtiene un número pequeño de filas para inspeccionar columnas reales y tipos."""
        query = f"SELECT * LIMIT {limit}"
        return self.execute_query(query)

    def get_total_count(self, where_clause: str = None):
        """Obtiene el conteo total de registros opcionalmente filtrado."""
        query = "SELECT COUNT(*) as total"
        if where_clause:
            query += f" WHERE {where_clause}"
        data = self.execute_query(query)
        if data and isinstance(data, list) and len(data) > 0:
            return int(data[0].get("total", 0))
        return 0
