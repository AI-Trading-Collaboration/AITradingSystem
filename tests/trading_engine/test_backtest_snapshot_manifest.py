from __future__ import annotations

from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    REQUIRED_SIGNAL_SNAPSHOTS,
    run_backtest_input_diagnostics,
)
from trading_engine.test_backtest_input_diagnostics import _write_fixture


def test_backtest_input_snapshot_manifest_records_reproducibility_inputs(tmp_path) -> None:
    fixture = _write_fixture(tmp_path)

    run = run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        generated_at=fixture["generated_at"],
    )

    manifest = run.manifest
    assert run.manifest_path.exists()
    assert manifest["report_type"] == "backtest_input_manifest"
    assert manifest["snapshot_id"] == f"backtest-input-{fixture['as_of'].isoformat()}"
    assert manifest["status"] == "OK"
    assert manifest["assets"]
    assert manifest["price_data_files"]
    assert len(manifest["signal_snapshot_files"]) == len(REQUIRED_SIGNAL_SNAPSHOTS)
    assert manifest["data_quality_report"] == str(run.json_path)
    assert manifest["config_hash"]
    assert manifest["code_version"]
