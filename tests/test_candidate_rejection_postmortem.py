from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.candidate_rejection_postmortem import (
    NO_POSTMORTEM_RECORD_PROVIDED,
    POSTMORTEM_INVALID,
    POSTMORTEM_VALID,
    REQUIRED_SECTION_IDS,
    TEMPLATE_BLOCKED,
    TEMPLATE_READY,
    build_candidate_rejection_postmortem_payload,
    validate_candidate_rejection_postmortem_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_candidate_rejection_postmortem_template_ready_without_record(
    tmp_path: Path,
) -> None:
    payload = build_candidate_rejection_postmortem_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    validation = validate_candidate_rejection_postmortem_payload(payload)

    assert payload["template_status"] == TEMPLATE_READY
    assert payload["summary"]["postmortem_record_provided"] is False
    assert payload["summary"]["filled_postmortem_status"] == NO_POSTMORTEM_RECORD_PROVIDED
    assert set(payload["blank_postmortem_template"]) >= set(REQUIRED_SECTION_IDS)
    assert validation["validation_status"] == "PASS"


def test_candidate_rejection_postmortem_blocks_invalid_filled_record(
    tmp_path: Path,
) -> None:
    postmortem_path = tmp_path / "invalid_postmortem.json"
    invalid = _valid_postmortem_record()
    invalid.pop("lessons_learned")
    invalid["candidate_state_mutated"] = True
    postmortem_path.write_text(json.dumps(invalid, ensure_ascii=False), encoding="utf-8")

    payload = build_candidate_rejection_postmortem_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        postmortem_json_path=postmortem_path,
        project_root=tmp_path,
    )
    validation = validate_candidate_rejection_postmortem_payload(payload)

    assert payload["template_status"] == TEMPLATE_BLOCKED
    assert payload["summary"]["filled_postmortem_status"] == POSTMORTEM_INVALID
    assert validation["validation_status"] == "FAIL"
    assert any(
        issue["issue_id"] == "filled_postmortem_valid_if_provided"
        for issue in validation["blocking_issues"]
    )


def test_candidate_rejection_postmortem_validates_filled_record(
    tmp_path: Path,
) -> None:
    postmortem_path = tmp_path / "valid_postmortem.json"
    postmortem_path.write_text(
        json.dumps(_valid_postmortem_record(), ensure_ascii=False),
        encoding="utf-8",
    )

    payload = build_candidate_rejection_postmortem_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        postmortem_json_path=postmortem_path,
        project_root=tmp_path,
    )
    validation = validate_candidate_rejection_postmortem_payload(payload)

    assert payload["template_status"] == TEMPLATE_READY
    assert payload["summary"]["filled_postmortem_status"] == POSTMORTEM_VALID
    assert payload["summary"]["failed_evidence_gate_count"] == 1
    assert payload["summary"]["can_revisit"] is True
    assert validation["validation_status"] == "PASS"


def test_candidate_rejection_postmortem_cli_writes_report_and_validation(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    report_index = _report_index_payload(tmp_path)
    report_index_path = reports_dir / "report_index_2026-05-04.json"
    report_index_path.parent.mkdir(parents=True, exist_ok=True)
    report_index_path.write_text(json.dumps(report_index, ensure_ascii=False), encoding="utf-8")
    runner = CliRunner()

    report_result = runner.invoke(
        app,
        [
            "reports",
            "candidate-rejection-postmortem-template",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--report-index-path",
            str(report_index_path),
            "--project-root",
            str(tmp_path),
        ],
    )
    assert report_result.exit_code == 0, report_result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-candidate-rejection-postmortem-template",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    report_payload = json.loads(
        (reports_dir / "candidate_rejection_postmortem_template_2026-05-04.json").read_text(
            encoding="utf-8"
        )
    )
    validation_payload = json.loads(
        (
            reports_dir / "candidate_rejection_postmortem_template_validation_2026-05-04.json"
        ).read_text(encoding="utf-8")
    )
    assert report_payload["template_status"] == TEMPLATE_READY
    assert validation_payload["validation_status"] == "PASS"
    assert validation_payload["input_artifacts"][
        "candidate_rejection_postmortem_template"
    ].endswith("candidate_rejection_postmortem_template_2026-05-04.json")


def test_reader_brief_candidate_rejection_postmortem_summary(tmp_path: Path) -> None:
    payload = build_candidate_rejection_postmortem_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    validation = validate_candidate_rejection_postmortem_payload(payload)
    report_path = tmp_path / "candidate_rejection_postmortem_template_2026-05-04.json"
    validation_path = (
        tmp_path / "candidate_rejection_postmortem_template_validation_2026-05-04.json"
    )
    report_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")

    summary = reader_brief._candidate_rejection_postmortem_summary(
        {
            "reports": [
                {
                    "report_id": "candidate_rejection_postmortem_template",
                    "latest_artifact_path": str(report_path),
                },
                {
                    "report_id": "candidate_rejection_postmortem_template_validation",
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["template_status"] == TEMPLATE_READY
    assert summary["validation_status"] == "PASS"
    assert summary["required_section_count"] == len(REQUIRED_SECTION_IDS)
    assert summary["production_effect"] == "none"


def _report_index_payload(tmp_path: Path) -> dict[str, object]:
    reports: list[dict[str, object]] = []
    source_payloads = {
        "paper_shadow_promotion_board": {
            "report_type": "paper_shadow_promotion_board",
            "board_decision": "REJECT",
            "candidate_id": "candidate_a",
            "summary": {"candidate_id": "candidate_a"},
            "production_effect": "none",
        },
        "owner_decision_audit_log": {
            "report_type": "owner_decision_audit_log",
            "audit_log_status": "AUDIT_LOG_PASS",
            "candidate_id": "candidate_a",
            "production_effect": "none",
        },
        "research_monthly_review_pack": {
            "report_type": "research_monthly_review_pack",
            "monthly_review_status": "MONTHLY_REVIEW_READY_WITH_WARNINGS",
            "candidate_id": "candidate_a",
            "production_effect": "none",
        },
        "research_safety_boundary_audit": {
            "report_type": "research_safety_boundary_audit",
            "safety_status": "SAFETY_PASS_WITH_WARNINGS",
            "production_effect": "none",
        },
    }
    for report_id, payload in source_payloads.items():
        source_path = tmp_path / "sources" / report_id / f"{report_id}.json"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        reports.append(
            {
                "report_id": report_id,
                "latest_artifact_path": str(source_path),
                "artifact_status": "AVAILABLE",
                "freshness_status": "FRESH",
                "production_effect": "none",
            }
        )
    return {
        "schema_version": 1,
        "report_type": "report_index",
        "as_of": RUN_DATE.isoformat(),
        "reports": reports,
        "production_effect": "none",
    }


def _valid_postmortem_record() -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "candidate_rejection_postmortem_record",
        "as_of": RUN_DATE.isoformat(),
        "production_effect": "none",
        "candidate_summary": {
            "candidate_id": "candidate_a",
            "candidate_name": "Candidate A",
            "rejection_decision_source": "paper_shadow_promotion_board",
        },
        "reason_for_rejection": {
            "primary_reason": "Cost sensitivity and benchmark evidence failed.",
            "rejection_category": "insufficient_evidence",
            "decision_reference": "paper_shadow_promotion_board_2026-05-04.json",
        },
        "failed_evidence_gates": [
            {
                "gate_id": "cost_sensitivity",
                "status": "FAILED",
                "reason": "Insufficient cost inputs.",
                "source_artifact": "cost_sensitivity_review.json",
            }
        ],
        "failed_stress_scenarios": [],
        "data_quality_issues": [],
        "safety_boundary_issues": [],
        "revisit_assessment": {
            "can_revisit": True,
            "revisit_condition": "Revisit after owner provides cost and benchmark evidence.",
            "owner_review_required": True,
        },
        "lessons_learned": [
            {
                "lesson": "Require numeric cost evidence before board review.",
                "follow_up_task": "TRADING-follow-up",
            }
        ],
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
    }
