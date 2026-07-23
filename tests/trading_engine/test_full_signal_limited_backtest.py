from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.signal_snapshots import run_signal_snapshot_build
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture


def test_full_signal_backtest_limited_uses_signal_snapshot_scores(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    baseline_text = fixture["baseline_path"].read_text(encoding="utf-8")
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )

    run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        dry_run=True,
    )

    assert run.payload["metadata"]["backtest_mode"] == "full_signal_backtest_limited"
    assert run.payload["data_quality"]["signal_snapshots_status"] == "LIMITED"
    assert run.payload["data_quality"]["can_run_shadow_backtest"] is True
    assert run.payload["data_quality"]["can_promote_candidate"] is False
    assert run.payload["promotion_constraints"]["max_promotion_status"] == "watch"
    assert run.payload["promotion_constraints"]["allow_candidate"] is False
    assert run.payload["promotion_decision"]["status"] in {"watch", "rejected"}
    assert "limited" in run.payload["promotion_decision"]["reason"].lower()

    score_calculation = run.payload["score_calculation"]
    assert score_calculation["mode"] == "full_signal_backtest_limited"
    assert score_calculation["signal_snapshot_status"] == "LIMITED"
    assert "trend_momentum" in score_calculation["price_derived_signals"]
    assert "earnings_quality" in score_calculation["fallback_signals"]
    assert run.payload["score_attribution"]["row_count"] > 0
    assert run.payload["score_attribution"]["rows"]
    assert "trend_momentum" in run.payload["parameter_contribution_summary"]
    assert fixture["baseline_path"].read_text(encoding="utf-8") == baseline_text
