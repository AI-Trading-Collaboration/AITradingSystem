from __future__ import annotations

import json
from pathlib import Path

import pytest
from real_snapshot_helpers import (
    real_snapshot_dry_run_fixture,
    real_snapshot_paper_action_fixture,
)

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    RealSnapshotError,
    apply_real_snapshot_paper_action,
    create_real_execution_owner_review,
    validate_real_snapshot_paper_action,
)


def test_real_snapshot_paper_action_monitor_is_no_action(tmp_path: Path) -> None:
    fixture = real_snapshot_paper_action_fixture(tmp_path, decision="monitor")
    action = fixture["paper_action"]["paper_action_from_real_snapshot"]
    validation = validate_real_snapshot_paper_action(
        paper_action_id=fixture["paper_action"]["paper_action_id"],
        **_validation_roots(tmp_path),
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
        **_validation_roots(tmp_path),
    )

    assert validation["status"] == "PASS"
    assert action["action_type"] == "paper_only"
    assert any(delta != 0.0 for delta in action["applied_paper_deltas"].values())
    assert paper_state["real_snapshot_mutated"] is False
    assert paper_state["weight_sum"] == 1.0
    assert action["broker_action_taken"] is False
    assert action["order_ticket_generated"] is False


def test_real_snapshot_paper_action_rejects_pending_owner_review_before_write(
    tmp_path: Path,
) -> None:
    fixture = real_snapshot_dry_run_fixture(tmp_path)
    owner_review = create_real_execution_owner_review(
        dry_run_id=fixture["dry_run"]["dry_run_id"],
        dry_run_dir=tmp_path / "real_snapshot_dry_run",
        output_dir=tmp_path / "real_execution_owner_review",
    )
    paper_action_dir = tmp_path / "real_snapshot_paper_action"

    with pytest.raises(
        RealSnapshotError,
        match="recorded non-pending owner decision",
    ):
        apply_real_snapshot_paper_action(
            owner_review_id=owner_review["review_id"],
            owner_review_dir=tmp_path / "real_execution_owner_review",
            dry_run_dir=tmp_path / "real_snapshot_dry_run",
            manual_snapshot_dir=tmp_path / "manual_portfolio_snapshot",
            drift_dir=tmp_path / "position_drift",
            guardrail_dir=tmp_path / "execution_guardrails",
            output_dir=paper_action_dir,
        )

    assert not paper_action_dir.exists()


def test_real_snapshot_paper_action_validator_detects_output_and_source_tampering(
    tmp_path: Path,
) -> None:
    (tmp_path / "output_tamper").mkdir()
    output_fixture = real_snapshot_paper_action_fixture(
        tmp_path / "output_tamper",
        decision="paper_adjustment_review_only",
    )
    action_path = (
        output_fixture["paper_action"]["paper_action_dir"]
        / "paper_action_from_real_snapshot.json"
    )
    action = json.loads(action_path.read_text(encoding="utf-8"))
    changed_symbol = next(iter(action["applied_paper_deltas"]))
    action["applied_paper_deltas"][changed_symbol] += 0.01
    action_path.write_text(
        json.dumps(action, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    output_validation = validate_real_snapshot_paper_action(
        paper_action_id=output_fixture["paper_action"]["paper_action_id"],
        **_validation_roots(tmp_path / "output_tamper"),
    )

    (tmp_path / "source_tamper").mkdir()
    source_fixture = real_snapshot_paper_action_fixture(
        tmp_path / "source_tamper",
        decision="paper_adjustment_review_only",
    )
    guardrail_path = (
        tmp_path
        / "source_tamper"
        / "execution_guardrails"
        / source_fixture["paper_action"]["paper_action_from_real_snapshot"]["guardrail_id"]
        / "proposed_adjustment_checks.jsonl"
    )
    rows = [json.loads(line) for line in guardrail_path.read_text(encoding="utf-8").splitlines()]
    rows[0]["capped_delta"] = float(rows[0].get("capped_delta", 0.0)) + 0.01
    guardrail_path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    source_validation = validate_real_snapshot_paper_action(
        paper_action_id=source_fixture["paper_action"]["paper_action_id"],
        **_validation_roots(tmp_path / "source_tamper"),
    )

    assert output_validation["status"] == "FAIL"
    assert "action_content_derived" in _failed_check_ids(output_validation)
    assert source_validation["status"] == "FAIL"
    assert "source_checksums_match" in _failed_check_ids(source_validation)


def _validation_roots(root: Path) -> dict[str, Path]:
    return {
        "output_dir": root / "real_snapshot_paper_action",
        "owner_review_dir": root / "real_execution_owner_review",
        "dry_run_dir": root / "real_snapshot_dry_run",
        "manual_snapshot_dir": root / "manual_portfolio_snapshot",
        "drift_dir": root / "position_drift",
        "guardrail_dir": root / "execution_guardrails",
    }


def _failed_check_ids(payload: dict[str, object]) -> set[str]:
    checks = payload["checks"]
    assert isinstance(checks, list)
    return {
        str(row["check_id"])
        for row in checks
        if isinstance(row, dict) and row.get("passed") is not True
    }
