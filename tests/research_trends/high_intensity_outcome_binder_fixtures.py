from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from high_intensity_event_logger_fixtures import (
    build_high_intensity_event_logger_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_forward_observe_event_logger import (
    run_high_intensity_risk_cap_forward_observe_event_logger,
)

__all__ = [
    "build_high_intensity_outcome_binder_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_outcome_binder_fixture(tmp_path: Path) -> dict[str, Path]:
    upstream = build_high_intensity_event_logger_fixture(tmp_path)
    event_logger_dir = tmp_path / "event_logger"
    run_high_intensity_risk_cap_forward_observe_event_logger(
        threshold_selection_dir=upstream["threshold_selection_dir"],
        forward_observe_plan_dir=upstream["forward_observe_plan_dir"],
        dynamic_dry_run_dir=upstream["dynamic_dry_run_dir"],
        dynamic_diagnostics_dir=upstream["dynamic_diagnostics_dir"],
        readiness_dir=upstream["readiness_dir"],
        timestamp_remediation_dir=upstream["timestamp_remediation_dir"],
        output_dir=event_logger_dir,
        docs_root=tmp_path / "event_logger_docs",
    )
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    _write_prices(prices_path)
    _write_rates(rates_path)
    return {
        **upstream,
        "event_logger_dir": event_logger_dir,
        "prices_path": prices_path,
        "rates_path": rates_path,
    }


def _write_prices(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    current = date(2023, 1, 2)
    business_index = 0
    while current <= date(2023, 3, 10):
        if current.weekday() < 5:
            close = 100.0 + business_index * 0.5
            rows.append(
                {
                    "date": current.isoformat(),
                    "ticker": "QQQ",
                    "symbol": "QQQ",
                    "open": close,
                    "high": close + 1.0,
                    "low": close - 1.0,
                    "close": close,
                    "adj_close": close,
                    "volume": 1000000,
                    "source": "fixture",
                    "updated_at": "",
                    "source_symbol": "QQQ",
                    "canonical_symbol": "QQQ",
                }
            )
            business_index += 1
        current += timedelta(days=1)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_rates(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"date": "2023-03-10", "series": "DUMMY_RATE", "value": "1.0"},
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "series", "value"])
        writer.writeheader()
        writer.writerows(rows)
