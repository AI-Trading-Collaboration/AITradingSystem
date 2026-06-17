from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from test_decision_stage_review import (
    _blocked_recovery_pack,
    _report_quality_gate_payload,
    _write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import decision_stage_review as decision_stage
from ai_trading_system.reports import return_to_research_reset as return_reset

RUN_DATE = date(2026, 6, 17)


def test_return_to_research_reset_appends_owner_decision_once(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    decision_dir = tmp_path / "docs" / "decisions"
    log_path = tmp_path / "data" / "governance" / "owner_decision_audit_log.jsonl"
    _write_decision_stage_inputs(reports_dir, tmp_path)

    payloads = return_reset.build_return_to_research_reset_payloads(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        decision_source_dir=decision_dir,
        owner_decision_log_path=log_path,
        append_owner_decision=True,
    )
    validation = return_reset.validate_return_to_research_governance_snapshot_payload(
        payloads[return_reset.GOVERNANCE_SNAPSHOT_REPORT_TYPE]
    )

    owner = payloads[return_reset.OWNER_DECISION_RECORD_REPORT_TYPE]
    transition = payloads[return_reset.CANDIDATE_TRANSITION_PACK_REPORT_TYPE]
    archived = payloads[return_reset.ARCHIVED_CANDIDATE_STATUS_REPORT_TYPE]
    snapshot = payloads[return_reset.GOVERNANCE_SNAPSHOT_REPORT_TYPE]

    assert owner["status"] == "OWNER_RETURN_TO_RESEARCH_DECISION_RECORDED"
    assert owner["summary"]["append_status"] == "OWNER_DECISION_APPENDED"
    assert owner["summary"]["owner_action"] == "return_to_research"
    assert transition["summary"]["transition_status"] == "RETURN_TO_RESEARCH_CONFIRMED"
    assert archived["summary"]["candidate_status"] == "RETURNED_TO_RESEARCH"
    assert archived["summary"]["candidate_rejected"] is False
    assert snapshot["status"] == "RETURN_TO_RESEARCH_COMPLETE"
    assert snapshot["summary"]["normal_paper_shadow_active"] is False
    assert snapshot["summary"]["extended_shadow_allowed"] is False
    assert snapshot["summary"]["live_trading_allowed"] is False
    assert snapshot["summary"]["candidate_rejected"] is False
    assert validation["validation_status"] == "PASS"

    log_lines = [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(log_lines) == 1
    assert log_lines[0]["owner_action"] == "return_to_research"

    second = return_reset.build_return_to_research_reset_payloads(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        decision_source_dir=decision_dir,
        owner_decision_log_path=log_path,
        append_owner_decision=True,
    )
    second_owner = second[return_reset.OWNER_DECISION_RECORD_REPORT_TYPE]
    assert second_owner["summary"]["append_status"] == "OWNER_DECISION_ALREADY_RECORDED"
    assert len(log_path.read_text(encoding="utf-8").splitlines()) == 1


def test_return_to_research_reset_cli_writes_batch_and_validation(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    decision_dir = tmp_path / "docs" / "decisions"
    log_path = tmp_path / "data" / "governance" / "owner_decision_audit_log.jsonl"
    _write_decision_stage_inputs(reports_dir, tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "return-to-research-reset",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--decision-source-dir",
            str(decision_dir),
            "--owner-decision-log-path",
            str(log_path),
        ],
    )
    assert result.exit_code == 0, result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-return-to-research-governance-snapshot",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    expected_types = (
        *return_reset.RESET_REPORT_TYPES,
        return_reset.GOVERNANCE_SNAPSHOT_VALIDATION_REPORT_TYPE,
    )
    for report_type in expected_types:
        assert return_reset.default_return_to_research_json_path(
            report_type,
            reports_dir,
            RUN_DATE,
        ).exists()
        assert return_reset.default_return_to_research_markdown_path(
            report_type,
            reports_dir,
            RUN_DATE,
        ).exists()

    snapshot = json.loads(
        return_reset.default_return_to_research_json_path(
            return_reset.GOVERNANCE_SNAPSHOT_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ).read_text(encoding="utf-8")
    )
    validation = json.loads(
        return_reset.default_return_to_research_json_path(
            return_reset.GOVERNANCE_SNAPSHOT_VALIDATION_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ).read_text(encoding="utf-8")
    )
    assert snapshot["summary"]["return_to_research_status"] == "RETURN_TO_RESEARCH_COMPLETE"
    assert validation["validation_status"] == "PASS"


def _write_decision_stage_inputs(reports_dir: Path, project_root: Path) -> None:
    recovery_pack, report_index = _blocked_recovery_pack(project_root)
    _write_json(reports_dir / "report_index_2026-06-17.json", report_index)
    _write_json(
        reports_dir / "research_governance_recovery_pack_2026-06-17.json",
        recovery_pack,
    )
    _write_json(
        reports_dir / "report_quality_gate_2026-06-17.json",
        _report_quality_gate_payload(),
    )
    payloads = decision_stage.build_decision_stage_review_payloads(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=project_root,
        owner_decision_log_path=(
            project_root / "data" / "governance" / "upstream_owner_decision_audit_log.jsonl"
        ),
    )
    for report_type, payload in payloads.items():
        decision_stage.write_decision_stage_json(
            payload,
            decision_stage.default_decision_stage_json_path(
                report_type,
                reports_dir,
                RUN_DATE,
            ),
        )
