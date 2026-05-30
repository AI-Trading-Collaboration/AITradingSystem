from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.trading_engine.price_cache_reconcile import refresh_backtest_manifest
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture


def test_refresh_backtest_manifest_dry_run_does_not_write(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=20, min_history_days=8)
    target = (
        fixture["output_dir"]
        / "backtest_snapshots"
        / fixture["as_of"].isoformat()
        / "backtest_input_manifest.json"
    )

    run = refresh_backtest_manifest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        dry_run=True,
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )

    assert run.payload["metadata"]["status"] == "DRY_RUN"
    assert run.payload["would_write_manifest"] == str(target)
    assert not target.exists()


def test_refresh_backtest_manifest_writes_manifest(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=20, min_history_days=8)

    run = refresh_backtest_manifest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )

    assert run.payload["metadata"]["status"] in {"OK", "FAILED"}
    assert run.payload["production_effect"] == "none"
    assert run.payload["manual_review_required"] is True
    assert run.payload["auto_promotion"] is False
    assert run.diagnostic_run is not None
    assert run.diagnostic_run.manifest_path.exists()
