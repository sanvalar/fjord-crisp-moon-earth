"""Cliente HTTP con reintentos, backoff exponencial y clasificación de 403."""
from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional

import requests

logger = logging.getLogger("ingestion.http")

RATE_LIMIT_MARKERS = (
    "rate limit",
    "rate-limit",
    "too many",
    "throttl",
    "quota",
    "try again later",
    "over capacity",
    "exceeded",
)
PERMISSION_MARKERS = (
    "not authorized",
    "unauthorized",
    "access denied",
    "permission denied",
    "invalid app token",
    "forbidden for this user",
)


class HttpClientError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, body: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class RateLimitError(HttpClientError):
    def __init__(self, message: str, retry_after: Optional[float] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class PermissionDeniedError(HttpClientError):
    pass


@dataclass
class HttpResponse:
    status_code: int
    payload: Any
    headers: Mapping[str, str]
    url: str


def classify_403(response: requests.Response) -> str:
    """Devuelve 'rate_limit' o 'permission' para un HTTP 403."""
    text = (response.text or "").lower()
    headers = {k.lower(): str(v) for k, v in response.headers.items()}
    if headers.get("retry-after") or headers.get("x-ratelimit-remaining") == "0":
        return "rate_limit"
    if any(marker in text for marker in PERMISSION_MARKERS):
        return "permission"
    if any(marker in text for marker in RATE_LIMIT_MARKERS):
        return "rate_limit"
    # En la API pública de Socrata un 403 vacío suele ser cupo, no ACL.
    return "rate_limit"


def _retry_after_seconds(response: requests.Response, fallback: float) -> float:
    raw = response.headers.get("Retry-After") or response.headers.get("retry-after")
    if raw:
        try:
            return max(float(raw), 1.0)
        except ValueError:
            pass
    return fallback


class ResilientHttpClient:
    def __init__(
        self,
        timeout: int,
        max_retries: int,
        backoff_factor: float,
        app_token: str = "",
        user_agent: str = "SECOP-Ingestion/1.0",
        session: Optional[requests.Session] = None,
        sleeper=time.sleep,
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.session = session or requests.Session()
        self.sleeper = sleeper
        headers = {
            "User-Agent": user_agent,
            "Accept": "application/json, text/html;q=0.8",
        }
        if app_token:
            headers["X-App-Token"] = app_token
        self.session.headers.update(headers)

    def get_json(self, url: str, params: Optional[dict] = None) -> HttpResponse:
        response = self._request("GET", url, params=params)
        try:
            payload = response.json()
        except ValueError as exc:
            raise HttpClientError(
                f"Respuesta no JSON desde {url}",
                status_code=response.status_code,
                body=response.text[:500],
            ) from exc
        return HttpResponse(
            status_code=response.status_code,
            payload=payload,
            headers=dict(response.headers),
            url=response.url,
        )

    def get_text(self, url: str, params: Optional[dict] = None) -> HttpResponse:
        response = self._request("GET", url, params=params)
        return HttpResponse(
            status_code=response.status_code,
            payload=response.text,
            headers=dict(response.headers),
            url=response.url,
        )

    def _request(self, method: str, url: str, params: Optional[dict] = None) -> requests.Response:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.request(
                    method, url, params=params, timeout=self.timeout
                )
            except requests.RequestException as exc:
                last_error = HttpClientError(f"Error de red: {exc}")
                wait = self._backoff(attempt)
                logger.warning(
                    "Fallo de red intento %s/%s url=%s wait=%.1fs error=%s",
                    attempt,
                    self.max_retries,
                    url,
                    wait,
                    exc,
                )
                if attempt >= self.max_retries:
                    break
                self.sleeper(wait)
                continue

            if response.status_code == 403:
                kind = classify_403(response)
                if kind == "permission":
                    raise PermissionDeniedError(
                        "403 de permisos (no se reintenta como cupo)",
                        status_code=403,
                        body=response.text[:500],
                    )
                wait = _retry_after_seconds(response, self._backoff(attempt))
                last_error = RateLimitError(
                    "403 interpretado como limitación de frecuencia",
                    retry_after=wait,
                    status_code=403,
                    body=response.text[:500],
                )
                logger.warning(
                    "403 rate-limit intento %s/%s url=%s wait=%.1fs",
                    attempt,
                    self.max_retries,
                    url,
                    wait,
                )
                if attempt >= self.max_retries:
                    break
                self.sleeper(wait)
                continue

            if response.status_code == 429:
                wait = _retry_after_seconds(response, self._backoff(attempt))
                last_error = RateLimitError(
                    "429 Too Many Requests",
                    retry_after=wait,
                    status_code=429,
                    body=response.text[:500],
                )
                logger.warning(
                    "429 intento %s/%s url=%s wait=%.1fs",
                    attempt,
                    self.max_retries,
                    url,
                    wait,
                )
                if attempt >= self.max_retries:
                    break
                self.sleeper(wait)
                continue

            if response.status_code >= 500:
                wait = self._backoff(attempt)
                last_error = HttpClientError(
                    f"Error de servidor {response.status_code}",
                    status_code=response.status_code,
                    body=response.text[:500],
                )
                logger.warning(
                    "HTTP %s intento %s/%s url=%s wait=%.1fs",
                    response.status_code,
                    attempt,
                    self.max_retries,
                    url,
                    wait,
                )
                if attempt >= self.max_retries:
                    break
                self.sleeper(wait)
                continue

            if response.status_code >= 400:
                raise HttpClientError(
                    f"HTTP {response.status_code} en {url}",
                    status_code=response.status_code,
                    body=response.text[:500],
                )
            return response

        raise last_error or HttpClientError(f"Fallo HTTP persistente en {url}")

    def _backoff(self, attempt: int) -> float:
        base = self.backoff_factor ** attempt
        jitter = random.uniform(0, 0.4 * base)
        return min(base + jitter, 120.0)
