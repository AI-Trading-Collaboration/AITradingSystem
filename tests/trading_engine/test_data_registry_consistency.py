from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from ai_trading_system.trading_engine.data_registry_consistency import (
    build_data_registry_consistency_payload,
    latest_valid_backtest_manifest_context,
    reconcile_price_cache_plan,
    run_data_registry_consistency,
    validate_backtest_manifest_consistency,
)
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture


def test_data_registry_detects_repaired_manifest_cache_mismatch(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=20, min_history_days=8)
    prices_path = tmp_path / "data" / "prices_daily.csv"
    _write_backtest_manifest(
        tmp_path,
        fixture["as_of"],
        prices_path=prices_path,
        assets=("QQQ", "GOOGL", "BRK.B", "SGOV"),
    )

    payload = build_data_registry_consistency_payload(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
    )

    registry = {item["canonical_symbol"]: item for item in payload["asset_registry"]}
    assert payload["metadata"]["status"] == "FAILED"
    assert registry["QQQ"]["error_code"] == "OK"
    assert registry["GOOGL"]["error_code"] == "MANIFEST_PRICE_CACHE_MISMATCH"
    assert registry["BRK.B"]["source_symbol"] == "BRK-B"
    assert registry["BRK.B"]["mapping_status"] == "OK"
    assert registry["SGOV"]["error_code"] == "MANIFEST_PRICE_CACHE_MISMATCH"
    assert payload["path_consistency"]["status"] == "OK"


def test_data_registry_latest_resolution_ignores_invalid_newer_manifest(
    tmp_path: Path,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=20, min_history_days=8)
    valid_date = fixture["as_of"] - timedelta(days=1)
    prices_path = tmp_path / "data" / "prices_daily.csv"
    _write_backtest_manifest(
        tmp_path,
        valid_date,
        prices_path=prices_path,
        assets=("QQQ", "NVDA"),
    )
    _write_backtest_manifest(
        tmp_path,
        fixture["as_of"],
        prices_path=prices_path,
        assets=("QQQ", "GOOGL"),
        status="FAILED",
    )

    context = latest_valid_backtest_manifest_context(
        output_root=fixture["output_dir"],
        as_of=fixture["as_of"],
        expected_prices_path=prices_path,
    )
    payload = build_data_registry_consistency_payload(
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
    )

    assert context.manifest_date == valid_date
    assert payload["latest_resolution"]["resolved_backtest_manifest_date"] == (
        valid_date.isoformat()
    )
    assert payload["latest_resolution"]["status"] == "MISMATCH"


def test_data_registry_report_and_reconcile_plan_are_safety_bounded(
    tmp_path: Path,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=20, min_history_days=8)
    prices_path = tmp_path / "data" / "prices_daily.csv"
    _write_backtest_manifest(
        tmp_path,
        fixture["as_of"],
        prices_path=prices_path,
        assets=("QQQ", "GOOGL"),
    )

    run = run_data_registry_consistency(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
    )
    validation = validate_backtest_manifest_consistency(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
    )
    dry_run_plan = reconcile_price_cache_plan(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        dry_run=True,
    )

    assert run.json_path.exists()
    assert run.markdown_path.exists()
    assert "Data Registry Consistency Report" in run.markdown_path.read_text(encoding="utf-8")
    assert validation["status"] == "FAILED"
    assert dry_run_plan["status"] == "DRY_RUN"
    assert dry_run_plan["production_effect"] == "none"
    assert dry_run_plan["auto_promotion"] is False


def _write_backtest_manifest(
    tmp_path: Path,
    as_of: date,
    *,
    prices_path: Path,
    assets: tuple[str, ...],
    status: str = "OK",
) -> Path:
    path = tmp_path / "artifacts" / "backtest_snapshots" / as_of.isoformat()
    path.mkdir(parents=True, exist_ok=True)
    manifest_path = path / "backtest_input_manifest.json"
    payload = {
        "schema_version": 1,
        "report_type": "backtest_input_manifest",
        "snapshot_id": f"backtest-input-{as_of.isoformat()}",
        "generated_at": "2026-01-20T00:00:00+00:00",
        "status": status,
        "production_effect": "none",
        "assets": list(assets),
        "symbol_mapping": {
            "BRK.B": {"canonical_symbol": "BRK.B", "source_symbol": "BRK-B"}
        },
        "date_range": {"start": "2026-01-01", "end": as_of.isoformat()},
        "price_data_files": [str(prices_path)],
        "signal_snapshot_files": [],
        "data_quality_report": str(
            tmp_path / "artifacts" / "data_quality" / as_of.isoformat() / "diagnostics.json"
        ),
        "config_hash": "test",
        "code_version": "test",
    }
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if prices_path.exists():
        frame = pd.read_csv(prices_path)
        frame.to_csv(prices_path, index=False)
    return manifest_path
