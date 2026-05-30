from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    run_backtest_input_diagnostics,
)
from ai_trading_system.trading_engine.portfolio_sensitivity import (
    build_portfolio_sensitivity_payload,
    run_portfolio_sensitivity,
)
from ai_trading_system.trading_engine.signal_snapshots import run_signal_snapshot_build
from trading_engine.test_data_registry_consistency import _write_backtest_manifest
from trading_engine.test_portfolio_sensitivity import _write_portfolio_sensitivity_config
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture


def test_portfolio_sensitivity_blocks_on_manifest_cache_mismatch(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=40, min_history_days=12)
    prices_path = tmp_path / "data" / "prices_daily.csv"
    _write_backtest_manifest(
        tmp_path,
        fixture["as_of"],
        prices_path=prices_path,
        assets=("QQQ", "NVDA", "GOOGL"),
    )
    config_path = _write_portfolio_sensitivity_config(tmp_path, fixture["config_path"])

    payload = build_portfolio_sensitivity_payload(
        as_of=fixture["as_of"],
        profile_names=("baseline_v0_1",),
        config_path=config_path,
    )

    assert payload["metadata"]["status"] == "FAILED"
    assert payload["data_gate"]["status"] == "FAILED"
    assert payload["data_gate"]["error_code"] == "MANIFEST_PRICE_CACHE_MISMATCH"
    assert payload["promotion_impact"]["can_support_candidate_promotion"] is False
    assert payload["safety"]["production_parameters_modified"] is False


def test_portfolio_sensitivity_uses_latest_valid_backtest_manifest(
    tmp_path: Path,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=80, min_history_days=20)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    diagnostics = run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )
    config_path = _write_portfolio_sensitivity_config(tmp_path, fixture["config_path"])

    run = run_portfolio_sensitivity(
        profile_names=("baseline_v0_1",),
        config_path=config_path,
    )

    assert run.as_of == fixture["as_of"]
    assert run.payload["data_gate"]["status"] == "OK"
    assert run.payload["data_gate"]["source"] == "backtest_input_manifest"
    assert run.payload["data_gate"]["manifest"] == str(diagnostics.manifest_path)
