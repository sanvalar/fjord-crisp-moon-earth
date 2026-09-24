"""
Cliente HTTP resiliente para la API de SECOP I (f789-7hwg).
Manejo de reintentos, backoff exponencial y consultas SoQL.
"""
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from secop_i.config import (
    API_SECOP_I_URL, DATASET_I_METADATA_URL, REQUEST_TIMEOUT,
    MAX_RETRIES, BACKOFF_FACTOR
)

logger = logging.getLogger("secop_i.api")

class SecopIAPIClient:
    def __init__(self, timeout=REQUEST_TIMEOUT, max_retries=MAX_RETRIES):
        self.timeout = timeout
        self.session = requests.Session()
        
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
            "User-Agent": "SecopI-DataPipeline/1.0 (Research; Python/Requests)",
            "Accept": "application/json"
        })

    def fetch_dataset_metadata(self):
        """Descarga metadatos oficiales del dataset f789-7hwg."""
        logger.info(f"Consultando metadata de SECOP I desde {DATASET_I_METADATA_URL}...")
        try:
            resp = self.session.get(DATASET_I_METADATA_URL, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al obtener metadata de SECOP I: {e}")
            raise

    def execute_query(self, soql_query: str):
        """Ejecuta una consulta SoQL sobre el endpoint query.json de SECOP I."""
        params = {"query": soql_query}
        try:
            response = self.session.get(API_SECOP_I_URL, params=params, timeout=self.timeout)
            if response.status_code == 400:
                logger.error(f"Error 400 Bad Request en SECOP I. Consulta: {soql_query}. Resp: {response.text[:300]}")
                response.raise_for_status()
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error en petición HTTP SECOP I: {e}")
            raise

    def get_sample_rows(self, limit: int = 5):
        query = f"SELECT * LIMIT {limit}"
        return self.execute_query(query)
