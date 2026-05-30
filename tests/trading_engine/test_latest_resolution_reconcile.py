from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.trading_engine.data_registry_consistency import (
    build_data_registry_consistency_payload,
)
from ai_trading_system.trading_engine.price_cache_reconcile import run_price_cache_reconcile
from trading_engine.test_price_cache_reconcile import _reconcile_fixture


def test_latest_resolution_ok_after_reconcile(tmp_path: Path, monkeypatch) -> None:
    fixture = _reconcile_fixture(tmp_path, monkeypatch)

    run_price_cache_reconcile(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )
    payload = build_data_registry_consistency_payload(
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )

    assert payload["metadata"]["status"] == "OK"
    assert payload["latest_resolution"]["status"] == "OK"
    assert payload["latest_resolution"]["resolved_market_data_date"] == (
        payload["latest_resolution"]["resolved_backtest_manifest_date"]
    )
    registry = {item["canonical_symbol"]: item for item in payload["asset_registry"]}
    assert registry["BRK.B"]["diagnosis"] == "BRK.B OK via BRK-B."
