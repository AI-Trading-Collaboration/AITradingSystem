from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_backtest_sim_helpers import (
    run_calibration_fixture,
    run_forward_bridge_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DynamicV3BacktestSimulationError,
    validate_backtest_sim_forward_bridge_artifact,
)


def test_backtest_sim_forward_bridge_requires_forward_confirmation(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    bridge = fixture["bridge"]
    targets = bridge["forward_confirmation_targets"]["targets"]

    assert bridge["manifest"]["status"] == "PASS"
    assert bridge["manifest"]["next_action"] == "continue_forward_tracking"
    assert targets
    assert targets[0]["required_forward_events"] == 2
    assert targets[0]["windows"] == [1, 5, 10, 20]
    assert targets[0]["success_criteria"]["win_rate_vs_no_trade_min"] == 0.55
    assert targets[0]["tracking_status"] == "TRACKING_REQUIRED"
    assert bridge["manifest"]["bridge_semantics"] == "TRACKING_PLAN_ONLY"
    assert bridge["manifest"]["target_count"] == 2
    assert bridge["input_snapshot"]["calibration_validation"]["status"] == "PASS"
    assert bridge["manifest"]["broker_action_allowed"] is False
    assert bridge["manifest"]["production_effect"] == "none"
    assert (bridge["bridge_dir"] / "reader_brief_section.md").exists()

    validation = validate_backtest_sim_forward_bridge_artifact(
        bridge_id=bridge["bridge_id"],
        output_dir=fixture["bridge_dir"],
    )
    assert validation["status"] == "PASS"


def test_forward_bridge_rejects_naive_cutoff_without_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_calibration_fixture(tmp_path, monkeypatch)

    with pytest.raises(DynamicV3BacktestSimulationError, match="timezone-aware"):
        sim.run_backtest_sim_forward_bridge(
            calibration_pack_id=fixture["calibration"]["calibration_pack_id"],
            calibration_dir=fixture["calibration_dir"],
            output_dir=fixture["bridge_dir"],
            generated_at=datetime(2026, 7, 31, 9),
        )

    assert not fixture["bridge_dir"].exists()


def test_forward_bridge_rejects_invalid_calibration_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_calibration_fixture(tmp_path, monkeypatch)
    source_report = (
        fixture["calibration"]["calibration_pack_dir"]
        / "backtest_sim_calibration_report.md"
    )
    source_report.write_text(
        source_report.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8"
    )

    with pytest.raises(DynamicV3BacktestSimulationError, match="calibration validation"):
        sim.run_backtest_sim_forward_bridge(
            calibration_pack_id=fixture["calibration"]["calibration_pack_id"],
            calibration_dir=fixture["calibration_dir"],
            output_dir=fixture["bridge_dir"],
            generated_at=datetime(2026, 7, 31, 9, tzinfo=UTC),
        )

    assert not fixture["bridge_dir"].exists()


def test_forward_bridge_rejects_future_calibration_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_calibration_fixture(tmp_path, monkeypatch)

    with pytest.raises(DynamicV3BacktestSimulationError, match="after bridge cutoff"):
        sim.run_backtest_sim_forward_bridge(
            calibration_pack_id=fixture["calibration"]["calibration_pack_id"],
            calibration_dir=fixture["calibration_dir"],
            output_dir=fixture["bridge_dir"],
            generated_at=datetime(2026, 7, 31, 7, 59, tzinfo=UTC),
        )

    assert not fixture["bridge_dir"].exists()


@pytest.mark.parametrize(
    ("relative_path", "is_json"),
    [
        ("sim_forward_bridge_manifest.json", True),
        ("forward_confirmation_targets.json", True),
        ("weekly_review_questions.json", True),
        ("forward_bridge_input_snapshot.json", True),
        ("sim_forward_bridge_report.md", False),
        ("reader_brief_section.md", False),
    ],
)
def test_forward_bridge_validator_rejects_output_tamper(
    tmp_path: Path,
    monkeypatch: Any,
    relative_path: str,
    is_json: bool,
) -> None:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    bridge = fixture["bridge"]
    target = bridge["bridge_dir"] / relative_path
    if is_json:
        payload = json.loads(target.read_text(encoding="utf-8"))
        payload["tampered"] = True
        target.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    else:
        target.write_text(target.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")

    validation = validate_backtest_sim_forward_bridge_artifact(
        bridge_id=bridge["bridge_id"], output_dir=fixture["bridge_dir"]
    )
    assert validation["status"] == "FAIL"


def test_forward_bridge_validator_rejects_live_calibration_drift(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    bridge = fixture["bridge"]
    source_report = (
        fixture["calibration"]["calibration_pack_dir"]
        / "backtest_sim_calibration_report.md"
    )
    source_report.write_text(
        source_report.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8"
    )

    validation = validate_backtest_sim_forward_bridge_artifact(
        bridge_id=bridge["bridge_id"], output_dir=fixture["bridge_dir"]
    )
    assert validation["status"] == "FAIL"
