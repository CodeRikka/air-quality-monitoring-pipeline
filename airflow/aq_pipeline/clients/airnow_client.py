import os
import time
from typing import Any

import requests
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class AirNowClient:
    BASE_URL = "https://www.airnowapi.org/aq/data"
    RETRY_STATUS_CODES = (408, 429, 500, 502, 503, 504)

    def __init__(self) -> None:
        self.api_key = os.environ["AIRNOW_API_KEY"]
        self.request_timeout_seconds = int(os.getenv("AIRNOW_REQUEST_TIMEOUT_SECONDS", "60"))
        self.min_request_interval_seconds = float(os.getenv("AIRNOW_MIN_REQUEST_INTERVAL_SECONDS", "1"))
        self.max_retries = int(os.getenv("AIRNOW_MAX_RETRIES", "3"))
        self.retry_backoff_factor = float(os.getenv("AIRNOW_RETRY_BACKOFF_FACTOR", "1.0"))
        self._last_request_monotonic: float | None = None

        retry = Retry(
            total=self.max_retries,
            read=self.max_retries,
            connect=self.max_retries,
            status=self.max_retries,
            backoff_factor=self.retry_backoff_factor,
            status_forcelist=self.RETRY_STATUS_CODES,
            allowed_methods={"GET"},
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session = requests.Session()
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def get_observations(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        query = {
            "API_KEY": self.api_key,
            **params,
        }
        self._apply_rate_limit()
        response = self._session.get(self.BASE_URL, params=query, timeout=self.request_timeout_seconds)
        self._last_request_monotonic = time.monotonic()
        response.raise_for_status()
        return response.json()

    def get_hourly_file(self, source_url: str) -> Response:
        self._apply_rate_limit()
        response = self._session.get(source_url, timeout=self.request_timeout_seconds)
        self._last_request_monotonic = time.monotonic()
        return response

    def _apply_rate_limit(self) -> None:
        if self.min_request_interval_seconds <= 0:
            return

        now = time.monotonic()
        if self._last_request_monotonic is None:
            return

        elapsed = now - self._last_request_monotonic
        remaining = self.min_request_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)
