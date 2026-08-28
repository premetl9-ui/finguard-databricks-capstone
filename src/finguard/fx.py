from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import requests


ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"


class AlphaVantageError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExchangeRate:
    from_currency: str
    to_currency: str
    rate: Decimal
    provider_timestamp: datetime
    raw: dict[str, Any]


class AlphaVantageClient:
    def __init__(self, api_key: str, timeout_seconds: int = 15, max_retries: int = 3):
        if not api_key:
            raise ValueError("Alpha Vantage API key is required")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.session = requests.Session()

    def _get(self, params: dict[str, str]) -> dict[str, Any]:
        params = {**params, "apikey": self.api_key}
        last_error: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(
                    ALPHA_VANTAGE_URL,
                    params=params,
                    timeout=self.timeout_seconds,
                )
                if response.status_code == 429 or 500 <= response.status_code < 600:
                    raise requests.HTTPError(
                        f"temporary Alpha Vantage HTTP {response.status_code}", response=response
                    )
                response.raise_for_status()
                payload = response.json()

                if "Error Message" in payload:
                    raise AlphaVantageError(payload["Error Message"])
                if "Note" in payload or "Information" in payload:
                    # Alpha Vantage commonly reports throttling/quota status in JSON.
                    message = payload.get("Note") or payload.get("Information")
                    raise requests.HTTPError(str(message))
                return payload
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                delay = (2**attempt) + random.uniform(0.0, 0.5)
                time.sleep(delay)

        raise AlphaVantageError(f"Alpha Vantage request failed after retries: {last_error}")

    def current_rate(self, from_currency: str, to_currency: str) -> ExchangeRate:
        payload = self._get(
            {
                "function": "CURRENCY_EXCHANGE_RATE",
                "from_currency": from_currency.upper(),
                "to_currency": to_currency.upper(),
            }
        )
        item = payload.get("Realtime Currency Exchange Rate")
        if not item:
            raise AlphaVantageError("Missing Realtime Currency Exchange Rate in response")

        refreshed = item.get("6. Last Refreshed")
        timestamp = (
            datetime.strptime(refreshed, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            if refreshed
            else datetime.now(timezone.utc)
        )
        return ExchangeRate(
            from_currency=item.get("1. From_Currency Code", from_currency).upper(),
            to_currency=item.get("3. To_Currency Code", to_currency).upper(),
            rate=Decimal(item["5. Exchange Rate"]),
            provider_timestamp=timestamp,
            raw=payload,
        )

    def daily_rates(
        self, from_currency: str, to_currency: str, outputsize: str = "compact"
    ) -> dict[str, Any]:
        return self._get(
            {
                "function": "FX_DAILY",
                "from_symbol": from_currency.upper(),
                "to_symbol": to_currency.upper(),
                "outputsize": outputsize,
            }
        )
