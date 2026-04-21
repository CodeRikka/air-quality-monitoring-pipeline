import os
import time
import logging
from collections.abc import Sequence
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class AQSClient:
    BASE_URL = "https://aqs.epa.gov/data/api"
    RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
    MAX_PARAMS_PER_REQUEST = 5

    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)
        self.email = os.environ["EPA_AQS_EMAIL"]
        self.api_key = os.environ["EPA_AQS_KEY"]
        self.request_timeout_seconds = int(os.getenv("AQS_REQUEST_TIMEOUT_SECONDS", "60"))
        self.min_request_interval_seconds = float(os.getenv("AQS_MIN_REQUEST_INTERVAL_SECONDS", "5"))
        self.max_retries = int(os.getenv("AQS_MAX_RETRIES", "3"))
        self.retry_backoff_factor = float(os.getenv("AQS_RETRY_BACKOFF_FACTOR", "1.0"))
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

    def get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        query = {
            "email": self.email,
            "key": self.api_key,
            **params,
        }
        url = f"{self.BASE_URL}/{endpoint}"

        self._apply_rate_limit()
        request_start = time.monotonic()
        self._logger.info(
            "AQS request start: endpoint=%s params=%s timeout_seconds=%s",
            endpoint,
            params,
            self.request_timeout_seconds,
        )
        try:
            response = self._session.get(url, params=query, timeout=self.request_timeout_seconds)
            self._last_request_monotonic = time.monotonic()
            response.raise_for_status()
        except Exception:
            elapsed = time.monotonic() - request_start
            self._logger.exception(
                "AQS request failed: endpoint=%s params=%s elapsed_seconds=%.2f",
                endpoint,
                params,
                elapsed,
            )
            raise

        payload = response.json()
        self._raise_if_aqs_response_failed(endpoint=endpoint, params=query, payload=payload)
        elapsed = time.monotonic() - request_start
        self._logger.info(
            "AQS request success: endpoint=%s elapsed_seconds=%.2f",
            endpoint,
            elapsed,
        )
        return payload

    def _format_param_value(self, param: str | Sequence[str]) -> str:
        if isinstance(param, str):
            cleaned = [value.strip() for value in param.split(",") if value.strip()]
        else:
            cleaned = [str(value).strip() for value in param if str(value).strip()]
        unique_values = list(dict.fromkeys(cleaned))
        if not unique_values:
            raise ValueError("AQS param cannot be empty.")
        if len(unique_values) > self.MAX_PARAMS_PER_REQUEST:
            raise ValueError(
                f"AQS allows up to {self.MAX_PARAMS_PER_REQUEST} params per request, got {len(unique_values)}."
            )
        return ",".join(unique_values)

    def get_states(self) -> dict[str, Any]:
        return self.get("list/states", {})

    def get_counties_by_state(self, state: str) -> dict[str, Any]:
        return self.get("list/countiesByState", {"state": state})

    def get_sites_by_county(self, state: str, county: str) -> dict[str, Any]:
        return self.get("list/sitesByCounty", {"state": state, "county": county})

    def get_parameter_classes(self) -> dict[str, Any]:
        return self.get("list/classes", {})

    def get_parameters_by_class(self, parameter_class: str) -> dict[str, Any]:
        return self.get("list/parametersByClass", {"pc": parameter_class})

    def get_monitors_by_county(
        self,
        *,
        state: str,
        county: str,
        bdate: str,
        edate: str,
        param: str | Sequence[str] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "state": state,
            "county": county,
            "bdate": bdate,
            "edate": edate,
        }
        if param:
            params["param"] = self._format_param_value(param)
        return self.get("monitors/byCounty", params)

    def get_monitors_by_state(
        self,
        *,
        state: str,
        bdate: str,
        edate: str,
        param: str | Sequence[str] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "state": state,
            "bdate": bdate,
            "edate": edate,
        }
        if param:
            params["param"] = self._format_param_value(param)
        return self.get("monitors/byState", params)

    def get_sample_data_by_county(
        self,
        *,
        param: str | Sequence[str],
        bdate: str,
        edate: str,
        state: str,
        county: str,
        cbdate: str | None = None,
        cedate: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "param": self._format_param_value(param),
            "bdate": bdate,
            "edate": edate,
            "state": state,
            "county": county,
        }
        if cbdate and cedate:
            params["cbdate"] = cbdate
            params["cedate"] = cedate
        return self.get("sampleData/byCounty", params)

    def get_sample_data_by_state(
        self,
        *,
        param: str | Sequence[str],
        bdate: str,
        edate: str,
        state: str,
        cbdate: str | None = None,
        cedate: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "param": self._format_param_value(param),
            "bdate": bdate,
            "edate": edate,
            "state": state,
        }
        if cbdate and cedate:
            params["cbdate"] = cbdate
            params["cedate"] = cedate
        return self.get("sampleData/byState", params)

    def get_daily_data_by_county(
        self,
        *,
        param: str | Sequence[str],
        bdate: str,
        edate: str,
        state: str,
        county: str,
        cbdate: str | None = None,
        cedate: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "param": self._format_param_value(param),
            "bdate": bdate,
            "edate": edate,
            "state": state,
            "county": county,
        }
        if cbdate and cedate:
            params["cbdate"] = cbdate
            params["cedate"] = cedate
        return self.get("dailyData/byCounty", params)

    def get_daily_data_by_state(
        self,
        *,
        param: str | Sequence[str],
        bdate: str,
        edate: str,
        state: str,
        cbdate: str | None = None,
        cedate: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "param": self._format_param_value(param),
            "bdate": bdate,
            "edate": edate,
            "state": state,
        }
        if cbdate and cedate:
            params["cbdate"] = cbdate
            params["cedate"] = cedate
        return self.get("dailyData/byState", params)

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

    @staticmethod
    def _raise_if_aqs_response_failed(endpoint: str, params: dict[str, Any], payload: dict[str, Any]) -> None:
        header = payload.get("Header")
        if not header:
            return

        header_items = header if isinstance(header, list) else [header]
        for item in header_items:
            if not isinstance(item, dict):
                continue
            status = str(item.get("status", "")).strip().lower()
            if status in {"", "success", "ok"}:
                continue
            error_message = (
                item.get("error")
                or item.get("message")
                or item.get("status")
                or "Unknown AQS response failure"
            )
            # AQS commonly returns this as a non-success status for valid empty-result queries.
            # Treat it as non-fatal so bootstrap/reconcile can continue across sparse county/param combos.
            if "no data matched your selection" in str(error_message).strip().lower():
                return
            raise RuntimeError(
                f"AQS request failed for endpoint '{endpoint}' with params {params}: {error_message}"
            )
