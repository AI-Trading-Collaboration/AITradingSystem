from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.owner_decision_audit_log import (
    AUDIT_LOG_BLOCKED,
    AUDIT_LOG_PASS,
    OwnerDecisionAuditLogError,
    append_owner_decision_record,
    build_owner_decision_audit_log_payload,
    latest_owner_decision_audit_log_json_path,
    render_owner_decision_audit_log_markdown,
    validate_owner_decision_audit_log_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_owner_decision_audit_log_append_and_report(tmp_path: Path) -> None:
    log_path = tmp_path / "owner_decision_audit_log.jsonl"
    record = append_owner_decision_record(
        _owner_decision_record(),
        log_path=log_path,
        appended_at=datetime(2026, 5, 4, 13, 5, tzinfo=UTC),
    )
    payload = build_owner_decision_audit_log_payload(as_of=RUN_DATE, log_path=log_path)
    validation = validate_owner_decision_audit_log_payload(payload)
    markdown = render_owner_decision_audit_log_markdown(payload)

    assert log_path.read_text(encoding="utf-8").count("\n") == 1
    assert record["decision_id"] == "owner-decision-1"
    assert payload["audit_log_status"] == AUDIT_LOG_PASS
    assert payload["summary"]["included_record_count"] == 1
    assert payload["summary"]["latest_owner_action"] == "hold"
    assert payload["monthly_review_pack_inputs"]["input_status"] == "AVAILABLE"
    assert payload["promotion_board_inputs"]["input_status"] == "AVAILABLE"
    assert validation["validation_status"] == "PASS"
    assert "Owner Decision Audit Log" in markdown


def test_trading_392_owner_hold_decision_fixture_is_governance_only(
    tmp_path: Path,
) -> None:
    decision_path = (
        PROJECT_ROOT
        / "docs"
        / "decisions"
        / "TRADING-392_owner_hold_decision_2026-06-17.json"
    )
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    log_path = tmp_path / "owner_decision_audit_log.jsonl"

    record = append_owner_decision_record(
        decision,
        log_path=log_path,
        source_record_path=decision_path,
        appended_at=datetime(2026, 6, 17, 0, 5, tzinfo=UTC),
    )
    payload = build_owner_decision_audit_log_payload(
        as_of=date(2026, 6, 17),
        log_path=log_path,
    )
    validation = validate_owner_decision_audit_log_payload(payload)

    assert record["decision_id"] == "TRADING-392_owner_hold_2026-06-17"
    assert record["owner_action"] == "hold"
    assert record["safety_status"] == "SAFETY_PASS_WITH_WARNINGS"
    assert record["candidate_state_mutated"] is False
    assert record["paper_shadow_state_mutated"] is False
    assert record["official_target_weights_generated"] is False
    assert record["broker_action_taken"] is False
    assert record["order_ticket_generated"] is False
    assert payload["audit_log_status"] == AUDIT_LOG_PASS
    assert payload["summary"]["latest_owner_action"] == "hold"
    assert payload["summary"]["latest_decision_id"] == (
        "TRADING-392_owner_hold_2026-06-17"
    )
    assert validation["validation_status"] == "PASS"


def test_owner_decision_audit_log_rejects_duplicate_decision_id(tmp_path: Path) -> None:
    log_path = tmp_path / "owner_decision_audit_log.jsonl"
    append_owner_decision_record(_owner_decision_record(), log_path=log_path)

    with pytest.raises(OwnerDecisionAuditLogError, match="duplicate owner decision_id"):
        append_owner_decision_record(_owner_decision_record(), log_path=log_path)


def test_owner_decision_audit_log_blocks_invalid_safety_pairing(tmp_path: Path) -> None:
    log_path = tmp_path / "owner_decision_audit_log.jsonl"
    record = _owner_decision_record()
    record["owner_action"] = "continue_shadow"
    record["safety_status"] = "SAFETY_BLOCKED"

    with pytest.raises(OwnerDecisionAuditLogError, match="safety_blocked_cannot_continue_shadow"):
        append_owner_decision_record(record, log_path=log_path)


def test_owner_decision_audit_log_accepts_decision_stage_actions(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "owner_decision_audit_log.jsonl"
    record = _owner_decision_record()
    record["owner_action"] = "keep_hold"
    append_owner_decision_record(record, log_path=log_path)

    blocked_record = _owner_decision_record()
    blocked_record["decision_id"] = "owner-decision-approve"
    blocked_record["owner_action"] = "approve_resume_normal_shadow"
    blocked_record["safety_status"] = "SAFETY_BLOCKED"

    with pytest.raises(OwnerDecisionAuditLogError, match="safety_blocked_cannot_continue_shadow"):
        append_owner_decision_record(blocked_record, log_path=log_path)


def test_owner_decision_audit_log_validation_blocks_duplicate_jsonl(tmp_path: Path) -> None:
    log_path = tmp_path / "owner_decision_audit_log.jsonl"
    normalized = append_owner_decision_record(_owner_decision_record(), log_path=log_path)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(normalized, ensure_ascii=False, sort_keys=True) + "\n")

    payload = build_owner_decision_audit_log_payload(as_of=RUN_DATE, log_path=log_path)
    validation = validate_owner_decision_audit_log_payload(payload)

    assert payload["audit_log_status"] == AUDIT_LOG_BLOCKED
    assert payload["summary"]["duplicate_decision_id_count"] == 1
    assert validation["validation_status"] == "FAIL"


def test_owner_decision_audit_log_cli_writes_report_and_validation(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    log_path = tmp_path / "data" / "governance" / "owner_decision_audit_log.jsonl"
    decision_path = tmp_path / "owner_decision.json"
    decision_path.write_text(
        json.dumps(_owner_decision_record(), ensure_ascii=False),
        encoding="utf-8",
    )
    runner = CliRunner()

    append_result = runner.invoke(
        app,
        [
            "reports",
            "owner-decision-audit-log",
            "append",
            "--decision-json-path",
            str(decision_path),
            "--log-path",
            str(log_path),
        ],
    )
    assert append_result.exit_code == 0, append_result.output

    report_result = runner.invoke(
        app,
        [
            "reports",
            "owner-decision-audit-log",
            "report",
            "--as-of",
            RUN_DATE.isoformat(),
            "--log-path",
            str(log_path),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert report_result.exit_code == 0, report_result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "owner-decision-audit-log",
            "validate",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    report_json = reports_dir / "owner_decision_audit_log_2026-05-04.json"
    validation_json = reports_dir / "owner_decision_audit_log_validation_2026-05-04.json"
    assert latest_owner_decision_audit_log_json_path(reports_dir) == report_json
    assert json.loads(report_json.read_text(encoding="utf-8"))["audit_log_status"] == (
        AUDIT_LOG_PASS
    )
    validation_payload = json.loads(validation_json.read_text(encoding="utf-8"))
    assert validation_payload["validation_status"] == "PASS"
    assert validation_payload["input_artifacts"]["owner_decision_audit_log_report"] == str(
        report_json
    )


def test_reader_brief_owner_decision_audit_log_summary_reads_report_index(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "owner_decision_audit_log.jsonl"
    append_owner_decision_record(_owner_decision_record(), log_path=log_path)
    payload = build_owner_decision_audit_log_payload(as_of=RUN_DATE, log_path=log_path)
    validation = validate_owner_decision_audit_log_payload(payload)
    report_path = tmp_path / "owner_decision_audit_log_2026-05-04.json"
    validation_path = tmp_path / "owner_decision_audit_log_validation_2026-05-04.json"
    report_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")

    summary = reader_brief._owner_decision_audit_log_summary(
        {
            "reports": [
                {
                    "report_id": "owner_decision_audit_log",
                    "latest_artifact_path": str(report_path),
                },
                {
                    "report_id": "owner_decision_audit_log_validation",
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["audit_log_status"] == AUDIT_LOG_PASS
    assert summary["validation_status"] == "PASS"
    assert summary["record_count"] == 1
    assert summary["latest_decision_id"] == "owner-decision-1"
    assert summary["monthly_review_pack_input"] == "AVAILABLE"
    assert summary["promotion_board_input"] == "AVAILABLE"


def _owner_decision_record() -> dict[str, object]:
    return {
        "decision_id": "owner-decision-1",
        "timestamp": "2026-05-04T13:00:00+00:00",
        "candidate_id": "median_plus_regime_mismatch_filter",
        "input_artifacts": [
            {
                "artifact_id": "owner_review_template_v2",
                "artifact_path": "outputs/reports/owner_review_template_v2_2026-05-04.json",
                "artifact_type": "report",
            },
            {
                "artifact_id": "research_safety_boundary_audit",
                "artifact_path": "outputs/reports/research_safety_boundary_audit_2026-05-04.json",
                "artifact_type": "report",
            },
        ],
        "owner_action": "hold",
        "reason_summary": "Forward sample remains limited; hold until next review.",
        "safety_status": "SAFETY_PASS_WITH_WARNINGS",
        "next_action": "review_next_weekly_paper_shadow_package",
        "production_effect": "none",
    }
