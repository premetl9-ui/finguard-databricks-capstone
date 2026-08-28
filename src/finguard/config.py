from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Settings:
    catalog: str = "finguard"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    operations_schema: str = "operations"
    cdc_schema: str = "lakebase_cdc"
    home_currency: str = "USD"
    alpha_vantage_api_key: str | None = None
    alert_threshold: int = 60
    critical_threshold: int = 80

    @property
    def bronze_transactions(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.bronze_transactions"

    @property
    def bronze_fx_api(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.bronze_fx_api"

    @property
    def silver_transactions(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.silver_transactions"

    @property
    def silver_fx_rates(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.silver_fx_rates"

    @property
    def gold_transaction_risk(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.gold_transaction_risk"


def _read_yaml(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_settings(path: str | Path | None = None) -> Settings:
    data = _read_yaml(path)
    project = data.get("project", {})
    transactions = data.get("transactions", {})
    risk = data.get("risk", {})

    return Settings(
        catalog=os.getenv("FINGUARD_CATALOG", project.get("catalog", "finguard")),
        bronze_schema=project.get("bronze_schema", "bronze"),
        silver_schema=project.get("silver_schema", "silver"),
        gold_schema=project.get("gold_schema", "gold"),
        operations_schema=project.get("operations_schema", "operations"),
        cdc_schema=project.get("cdc_schema", "lakebase_cdc"),
        home_currency=os.getenv(
            "FINGUARD_HOME_CURRENCY", transactions.get("home_currency", "USD")
        ).upper(),
        alpha_vantage_api_key=os.getenv("ALPHA_VANTAGE_API_KEY"),
        alert_threshold=int(os.getenv("FINGUARD_ALERT_THRESHOLD", risk.get("alert_threshold", 60))),
        critical_threshold=int(
            os.getenv("FINGUARD_CRITICAL_THRESHOLD", risk.get("critical_threshold", 80))
        ),
    )
