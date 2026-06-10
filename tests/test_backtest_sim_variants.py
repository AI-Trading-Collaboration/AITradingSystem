from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_variant_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    BACKTEST_SIM_VARIANTS,
    validate_backtest_sim_variants_artifact,
)


def test_backtest_sim_variants_apply_configured_adjustment_limits(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    variants = fixture["variants"]
    rows = variants["variant_rows"]

    assert set(variants["manifest"]["variants_generated"]) == set(BACKTEST_SIM_VARIANTS)
    assert variants["manifest"]["broker_action_taken"] is False
    assert {row["variant"] for row in rows} == set(BACKTEST_SIM_VARIANTS)
    limited = [row for row in rows if row["variant"] == "limited_adjustment"]
    assert limited
    assert max(row["turnover"] for row in limited) <= 0.10
    assert all(row["production_effect"] == "none" for row in rows)

    validation = validate_backtest_sim_variants_artifact(
        variant_set_id=variants["variant_set_id"],
        output_dir=fixture["variant_dir"],
    )
    assert validation["status"] == "PASS"
