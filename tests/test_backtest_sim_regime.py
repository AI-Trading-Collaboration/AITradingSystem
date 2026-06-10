from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_regime_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    REGIME_BUCKETS,
    validate_backtest_sim_regime_artifact,
)


def test_backtest_sim_regime_review_uses_known_regime_buckets(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_regime_fixture(tmp_path, monkeypatch)
    regime = fixture["regime"]
    metrics = regime["variant_regime_metrics"]

    assert regime["manifest"]["status"] == "PASS"
    assert metrics
    assert {row["regime"] for row in metrics} <= REGIME_BUCKETS
    assert regime["manifest"]["broker_action_taken"] is False

    validation = validate_backtest_sim_regime_artifact(
        regime_review_id=regime["regime_review_id"],
        output_dir=fixture["regime_dir"],
    )
    assert validation["status"] == "PASS"
