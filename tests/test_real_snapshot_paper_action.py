from __future__ import annotations

from pathlib import Path

from real_snapshot_helpers import real_snapshot_paper_action_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    validate_real_snapshot_paper_action,
)


def test_real_snapshot_paper_action_monitor_is_no_action(tmp_path: Path) -> None:
    fixture = real_snapshot_paper_action_fixture(tmp_path, decision="monitor")
    action = fixture["paper_action"]["paper_action_from_real_snapshot"]
    validation = validate_real_snapshot_paper_action(
        paper_action_id=fixture["paper_action"]["paper_action_id"],
        output_dir=tmp_path / "real_snapshot_paper_action",
    )

    assert validation["status"] == "PASS"
    assert action["action_type"] == "no_action"
    assert all(delta == 0.0 for delta in action["applied_paper_deltas"].values())
    assert action["broker_action_taken"] is False
    assert action["order_ticket_generated"] is False


def test_real_snapshot_paper_adjustment_applies_paper_only_deltas(tmp_path: Path) -> None:
    fixture = real_snapshot_paper_action_fixture(
        tmp_path,
        decision="paper_adjustment_review_only",
    )
    action = fixture["paper_action"]["paper_action_from_real_snapshot"]
    paper_state = fixture["paper_action"]["paper_state_after_action"]
    validation = validate_real_snapshot_paper_action(
        paper_action_id=fixture["paper_action"]["paper_action_id"],
        output_dir=tmp_path / "real_snapshot_paper_action",
    )

    assert validation["status"] == "PASS"
    assert action["action_type"] == "paper_only"
    assert any(delta != 0.0 for delta in action["applied_paper_deltas"].values())
    assert paper_state["real_snapshot_mutated"] is False
    assert paper_state["weight_sum"] == 1.0
    assert action["broker_action_taken"] is False
    assert action["order_ticket_generated"] is False
