from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from dynamic_v3_confirmation_cycle_helpers import cycle_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    DynamicV3ConfirmationOperationsError,
    build_confirmation_dashboard,
    build_rule_review_queue,
    validate_confirmation_dashboard_artifact,
)
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


@with_artifact_validation_session
def test_confirmation_dashboard_summarizes_target_and_pressure_progress(
    tmp_path: Path,
) -> None:
    fixture = cycle_fixture(tmp_path)
    queue = build_rule_review_queue(
        cycle_id=fixture["cycle"]["cycle_id"],
        output_dir=tmp_path / "rule_review_queue",
        cycle_dir=fixture["cycle_dir"],
        journal_path=fixture["journal_path"],
        generated_at=datetime(2026, 7, 31, 20, tzinfo=UTC),
    )

    dashboard = build_confirmation_dashboard(
        week_ending=date(2026, 8, 2),
        weekly_cycle_id="",
        progress_id=fixture["progress"]["progress_id"],
        evaluation_id=fixture["evaluation"]["evaluation_id"],
        cycle_id=fixture["cycle"]["cycle_id"],
        queue_id=queue["queue_id"],
        output_dir=tmp_path / "confirmation_dashboard",
        progress_dir=fixture["progress_dir"],
        evaluation_dir=fixture["evaluation_dir"],
        rule_cycle_dir=fixture["cycle_dir"],
        queue_dir=tmp_path / "rule_review_queue",
        pressure_tag_dir=tmp_path / "pressure_regime_tag",
        generated_at=datetime(2026, 7, 31, 21, tzinfo=UTC),
    )

    targets = {row["target_id"]: row for row in dashboard["target_status_table"]["targets"]}
    pressure = dashboard["pressure_sample_dashboard"]
    summary = dashboard["confirmation_dashboard_summary"]
    assert summary["targets_total"] == 1
    assert summary["ready_for_evaluation"] == 0
    assert targets["limited_adjustment_vs_no_trade"]["progress_pct"] == 0.0
    assert pressure["defensive_validation_status"] == "INSUFFICIENT_PRESSURE_EVENTS"
    assert pressure["pressure_samples"]["tech_drawdown"] == 0
    assert summary["policy_change_allowed"] is False
    assert (
        validate_confirmation_dashboard_artifact(
            dashboard_id=dashboard["dashboard_id"],
            output_dir=tmp_path / "confirmation_dashboard",
        )["status"]
        == "PASS"
    )

    target_path = Path(dashboard["dashboard_dir"]) / "target_status_table.json"
    target_payload = json.loads(target_path.read_text(encoding="utf-8"))
    target_payload["targets"][0]["available_forward_events"] = 999
    target_path.write_text(
        json.dumps(target_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    assert (
        validate_confirmation_dashboard_artifact(
            dashboard_id=dashboard["dashboard_id"],
            output_dir=tmp_path / "confirmation_dashboard",
        )["status"]
        == "FAIL"
    )

    invalid_pressure_root = tmp_path / "invalid_pressure"
    invalid_pressure_dir = invalid_pressure_root / "invalid-pressure"
    invalid_pressure_dir.mkdir(parents=True)
    (invalid_pressure_dir / "pressure_regime_manifest.json").write_text(
        json.dumps(
            {
                "tag_id": "invalid-pressure",
                "generated_at": "2026-07-31T20:30:00+00:00",
                "production_effect": "none",
                "broker_action_allowed": False,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    invalid_output = tmp_path / "invalid_dashboard_output"
    with pytest.raises(DynamicV3ConfirmationOperationsError, match="validation failed"):
        build_confirmation_dashboard(
            week_ending=date(2026, 8, 2),
            weekly_cycle_id="",
            progress_id=fixture["progress"]["progress_id"],
            evaluation_id=fixture["evaluation"]["evaluation_id"],
            cycle_id=fixture["cycle"]["cycle_id"],
            queue_id=queue["queue_id"],
            output_dir=invalid_output,
            progress_dir=fixture["progress_dir"],
            evaluation_dir=fixture["evaluation_dir"],
            rule_cycle_dir=fixture["cycle_dir"],
            queue_dir=tmp_path / "rule_review_queue",
            pressure_tag_dir=invalid_pressure_root,
            generated_at=datetime(2026, 7, 31, 22, tzinfo=UTC),
        )
    assert not invalid_output.exists()
