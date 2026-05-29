from __future__ import annotations

from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    run_backtest_input_diagnostics,
)
from trading_engine.test_backtest_input_diagnostics import _write_fixture


def test_repair_dry_run_generates_actionable_plan_for_missing_assets(tmp_path) -> None:
    fixture = _write_fixture(tmp_path, missing_assets=("BRK.B", "SGOV"))

    run = run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        generated_at=fixture["generated_at"],
    )

    repair_plan = run.payload["repair_plan"]
    first_step = repair_plan["steps"][0]
    assert repair_plan["status"] == "AVAILABLE"
    assert first_step["action"] == "download_missing_price_history"
    assert first_step["assets"] == ["BRK.B", "SGOV"]
    assert first_step["required"] is True
    assert first_step["date_range"]["start"]
    assert first_step["date_range"]["end"] == fixture["as_of"].isoformat()
