from __future__ import annotations

import copy
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_outcome_loop_helpers import run_rolling_refresh_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def test_forward_outcome_decision_keeps_policy_and_broker_disabled_when_not_ready(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = run_rolling_refresh_fixture(tmp_path, monkeypatch)
    trend = accumulation.run_evidence_trend(
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
        outcome_update_id=fixture["update"]["outcome_update_id"],
        refresh_id=fixture["refresh"]["refresh_id"],
        trend_id=trend["trend_id"],
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    matrix = result["forward_go_no_go_matrix"]
    actions = result["forward_next_actions"]["next_actions"]
    assert matrix["recommended_action"] == "continue_tracking"
    assert matrix["evidence_status"] == "COMPLETE"
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


def test_forward_outcome_decision_missing_sources_are_not_no_due_windows(
    tmp_path: Path,
) -> None:
    result = accumulation.run_forward_outcome_decision(
        week_ending=date(2026, 6, 14),
        output_dir=tmp_path / "forward_outcome_decision",
        outcome_update_dir=tmp_path / "outcome_update",
        rolling_refresh_dir=tmp_path / "rolling_evidence_refresh",
        evidence_trend_dir=tmp_path / "evidence_trend",
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    matrix = result["forward_go_no_go_matrix"]
    assert matrix["evidence_status"] == "INCOMPLETE"
    assert matrix["outcome_update_status"] == "MISSING"
    assert matrix["forward_available"] is None
    assert matrix["forward_pending"] is None
    assert matrix["recommended_action"] == "wait_for_more_outcomes"
    assert matrix["rule_calibration_readiness"] == "NOT_READY"
    assert accumulation.validate_forward_outcome_decision_artifact(
        decision_id=result["decision_id"],
        output_dir=tmp_path / "forward_outcome_decision",
    )["status"] == "PASS"


def test_forward_outcome_decision_blocks_invalid_source_before_output(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = run_rolling_refresh_fixture(tmp_path, monkeypatch)
    report = fixture["update"]["outcome_update_dir"] / "outcome_update_report.md"
    report.write_text(report.read_text(encoding="utf-8") + "tampered\n", encoding="utf-8")

    with pytest.raises(
        accumulation.DynamicV3OutcomeAccumulationError,
        match="outcome_update validation must PASS",
    ):
        accumulation.run_forward_outcome_decision(
            week_ending=date(2026, 6, 14),
            output_dir=tmp_path / "forward_outcome_decision",
            outcome_update_dir=tmp_path / "outcome_update",
            rolling_refresh_dir=tmp_path / "rolling_evidence_refresh",
            evidence_trend_dir=tmp_path / "evidence_trend",
            generated_at=datetime(2026, 6, 10, tzinfo=UTC),
        )
    assert not (tmp_path / "forward_outcome_decision").exists()


def test_forward_outcome_decision_ready_and_risk_routes_use_reviewed_policy(
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
    snapshot = copy.deepcopy(result["source_snapshot"])
    policy = snapshot["policy"]
    files = snapshot["sources"]["evidence_trend"]["selected"]["source_bundle"]["files"]
    latest = files["evidence_trend_timeseries.jsonl"]["content"][-1]
    latest.update(
        {
            "forward_available": 20,
            "limited_vs_notrade_confidence": "HIGH",
            "consensus_target_risk": "PASS",
        }
    )
    summary = files["confidence_trend_summary.json"]["content"]
    summary["trend_status"] = "STABLE"

    ready = accumulation._forward_go_no_go_matrix(  # noqa: SLF001
        week_ending=date(2026, 6, 14), snapshot=snapshot, policy=policy
    )
    assert ready["rule_calibration_readiness"] == "READY"
    assert ready["recommended_action"] == "review_rule_calibration_evidence"
    ready_actions = accumulation._forward_next_actions(ready, policy)  # noqa: SLF001
    assert {row["action"] for row in ready_actions["next_actions"]} >= {
        "review_rule_calibration_evidence",
        "do_not_auto_apply_policy",
    }

    summary["trend_status"] = "DETERIORATING"
    risk_review = accumulation._forward_go_no_go_matrix(  # noqa: SLF001
        week_ending=date(2026, 6, 14), snapshot=snapshot, policy=policy
    )
    assert risk_review["rule_calibration_readiness"] == "NOT_READY"
    assert risk_review["recommended_action"] == "manual_review"


@pytest.mark.parametrize(
    ("target", "field", "value"),
    [
        ("forward_go_no_go_matrix.json", "recommended_action", "TAMPERED"),
        ("forward_decision_source_snapshot.json", "generated_at", "2020-01-01T00:00:00+00:00"),
    ],
)
def test_forward_outcome_decision_validator_rejects_output_and_snapshot_tamper(
    tmp_path: Path,
    monkeypatch: Any,
    target: str,
    field: str,
    value: str,
) -> None:
    result, _ = _run_decision_fixture(tmp_path, monkeypatch)
    path = result["decision_dir"] / target
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload[field] = value
    _write_json(path, payload)

    assert accumulation.validate_forward_outcome_decision_artifact(
        decision_id=result["decision_id"],
        output_dir=tmp_path / "forward_outcome_decision",
    )["status"] == "FAIL"


def test_forward_outcome_decision_validator_rejects_policy_and_live_source_tamper(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    policy_path = tmp_path / "forward_decision_policy.yaml"
    policy_path.write_text(
        accumulation.DEFAULT_FORWARD_OUTCOME_DECISION_POLICY_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    result, fixture = _run_decision_fixture(tmp_path, monkeypatch, policy_path=policy_path)
    policy_path.write_text(policy_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    assert accumulation.validate_forward_outcome_decision_artifact(
        decision_id=result["decision_id"],
        output_dir=tmp_path / "forward_outcome_decision",
    )["status"] == "FAIL"

    policy_path.write_text(
        accumulation.DEFAULT_FORWARD_OUTCOME_DECISION_POLICY_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    trend_report = fixture["trend"]["trend_dir"] / "evidence_trend_report.md"
    trend_report.write_text(
        trend_report.read_text(encoding="utf-8") + "tampered\n",
        encoding="utf-8",
    )
    assert accumulation.validate_forward_outcome_decision_artifact(
        decision_id=result["decision_id"],
        output_dir=tmp_path / "forward_outcome_decision",
    )["status"] == "FAIL"


def _run_decision_fixture(
    tmp_path: Path,
    monkeypatch: Any,
    *,
    policy_path: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    fixture = run_rolling_refresh_fixture(tmp_path, monkeypatch)
    trend = accumulation.run_evidence_trend(
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
        policy_path=policy_path or accumulation.DEFAULT_FORWARD_OUTCOME_DECISION_POLICY_PATH,
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )
    return result, {**fixture, "trend": trend}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
