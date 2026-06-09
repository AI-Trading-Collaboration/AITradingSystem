from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_outcome_loop_helpers import run_rolling_refresh_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def test_forward_outcome_decision_keeps_policy_and_broker_disabled_when_not_ready(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    run_rolling_refresh_fixture(tmp_path, monkeypatch)
    accumulation.run_evidence_trend(
        output_dir=tmp_path / "evidence_trend",
        rolling_refresh_dir=tmp_path / "rolling_evidence_refresh",
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    result = accumulation.run_forward_outcome_decision(
        week_ending=date(2026, 6, 14),
        output_dir=tmp_path / "forward_outcome_decision",
        outcome_update_dir=tmp_path / "outcome_update",
        rolling_refresh_dir=tmp_path / "rolling_evidence_refresh",
        evidence_trend_dir=tmp_path / "evidence_trend",
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    matrix = result["forward_go_no_go_matrix"]
    actions = result["forward_next_actions"]["next_actions"]
    assert matrix["recommended_action"] == "continue_tracking"
    assert matrix["rule_calibration_readiness"] == "NOT_READY"
    assert matrix["broker_action_allowed"] is False
    assert matrix["production_effect"] == "none"
    assert any(row["action"] == "do_not_change_policy" for row in actions)
    assert any(row.get("target_date") == "2026-06-21" for row in actions)
    assert (
        accumulation.validate_forward_outcome_decision_artifact(
            decision_id=result["decision_id"],
            output_dir=tmp_path / "forward_outcome_decision",
        )["status"]
        == "PASS"
    )
