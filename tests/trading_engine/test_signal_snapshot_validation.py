from __future__ import annotations

import json
from pathlib import Path

from ai_trading_system.trading_engine.signal_snapshots import (
    load_signal_snapshot_payload,
    report_alias_paths,
    run_signal_snapshot_build,
    validate_signal_snapshot_payload,
    write_signal_snapshot_report_alias,
)
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture


def test_signal_snapshot_validation_accepts_limited_full_snapshot(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=220, min_history_days=20)
    run = run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )

    payload = load_signal_snapshot_payload(run.json_path)

    assert validate_signal_snapshot_payload(payload) == []
    assert payload["overall_quality"]["status"] == "LIMITED"
    assert payload["overall_quality"]["missing_signal_count"] == 0


def test_signal_snapshot_validation_rejects_missing_required_signal(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=80, min_history_days=20)
    run = run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    payload = json.loads(run.json_path.read_text(encoding="utf-8"))
    payload["signals"].pop("event_risk")

    issues = validate_signal_snapshot_payload(payload)

    assert "missing required signals: event_risk" in issues


def test_signal_snapshot_report_alias_writes_json_and_markdown(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=80, min_history_days=20)
    run = run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    reports_dir = tmp_path / "outputs" / "reports"

    json_path, markdown_path = write_signal_snapshot_report_alias(
        run.payload,
        reports_dir,
        fixture["as_of"],
    )

    assert (json_path, markdown_path) == report_alias_paths(reports_dir, fixture["as_of"])
    assert json_path.exists()
    assert markdown_path.exists()
    assert "Signal Snapshot" in markdown_path.read_text(encoding="utf-8")
