from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.trading_engine.portfolio_sensitivity import run_portfolio_sensitivity
from ai_trading_system.trading_engine.price_cache_reconcile import run_price_cache_reconcile
from ai_trading_system.trading_engine.signal_snapshots import run_signal_snapshot_build
from trading_engine.test_portfolio_sensitivity import _write_portfolio_sensitivity_config
from trading_engine.test_price_cache_reconcile import _reconcile_fixture


def test_portfolio_sensitivity_data_gate_passes_after_reconcile(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = _reconcile_fixture(tmp_path, monkeypatch)
    run_price_cache_reconcile(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    config_path = _write_portfolio_sensitivity_config(tmp_path, fixture["config_path"])

    run = run_portfolio_sensitivity(
        profile_names=("baseline_v0_1",),
        config_path=config_path,
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )

    assert run.payload["data_gate"]["status"] == "OK"
    assert run.payload["data_gate"]["latest_resolution_status"] == "OK"
    assert run.payload["metadata"]["status"] == "LIMITED"
    assert run.payload["metadata"]["production_effect"] == "none"
    assert run.payload["promotion_impact"]["can_support_candidate_promotion"] is False
