from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.owner_review_template_v2 import (
    OWNER_ACTIONS,
    PASS_STATUS,
    build_owner_review_template_v2_payload,
    latest_owner_review_template_v2_json_path,
    render_owner_review_template_v2_markdown,
    validate_owner_review_template_v2_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_owner_review_template_v2_declares_required_contract() -> None:
    payload = build_owner_review_template_v2_payload(as_of=RUN_DATE)
    validation = validate_owner_review_template_v2_payload(payload)
    markdown = render_owner_review_template_v2_markdown(payload)

    field_ids = {field["field_id"] for field in payload["required_fields"]}
    assert payload["template_status"] == "TEMPLATE_READY"
    assert payload["production_effect"] == "none"
    assert payload["summary"]["required_field_count"] == 9
    assert set(OWNER_ACTIONS) == {
        option["owner_action"] for option in payload["owner_action_options"]
    }
    assert "candidate_id" in field_ids
    assert "linked_input_artifacts" in field_ids
    assert validation["validation_status"] == PASS_STATUS
    assert "Owner Review Template V2" in markdown


def test_filled_owner_review_record_validation_passes() -> None:
    payload = build_owner_review_template_v2_payload(as_of=RUN_DATE)
    validation = validate_owner_review_template_v2_payload(
        payload,
        review_record=_filled_review_record(),
    )

    assert validation["validation_status"] == PASS_STATUS
    assert validation["summary"]["review_record_provided"] is True
    assert validation["review_record_validation"]["failed_check_count"] == 0


def test_trading_393_owner_review_fixture_validates_as_hold() -> None:
    review_path = (
        PROJECT_ROOT
        / "docs"
        / "owner_reviews"
        / "TRADING-393_owner_review_template_v2_2026-06-17.json"
    )
    review = json.loads(review_path.read_text(encoding="utf-8"))
    payload = build_owner_review_template_v2_payload(as_of=date(2026, 6, 17))

    validation = validate_owner_review_template_v2_payload(
        payload,
        review_record=review,
        review_record_path=review_path,
    )

    assert review["final_owner_action"] == "hold"
    assert review["safety_status"] == "SAFETY_PASS_WITH_WARNINGS"
    assert review["official_target_weights_generated"] is False
    assert review["broker_action_taken"] is False
    assert review["order_ticket_generated"] is False
    assert review["live_trading_allowed"] is False
    assert validation["validation_status"] == PASS_STATUS
    assert validation["summary"]["review_record_provided"] is True
    assert validation["review_record_validation"]["failed_check_count"] == 0


def test_filled_owner_review_record_blocks_missing_required_field() -> None:
    payload = build_owner_review_template_v2_payload(as_of=RUN_DATE)
    record = _filled_review_record()
    record.pop("main_reason_to_reject")

    validation = validate_owner_review_template_v2_payload(payload, review_record=record)

    assert validation["validation_status"] == "FAIL"
    assert any(
        issue["issue_id"] == "filled_review_required_field_main_reason_to_reject"
        for issue in validation["blocking_issues"]
    )


def test_filled_owner_review_record_blocks_safety_blocked_continuation() -> None:
    payload = build_owner_review_template_v2_payload(as_of=RUN_DATE)
    record = _filled_review_record()
    record["safety_status"] = "SAFETY_BLOCKED"
    record["final_owner_action"] = "continue_shadow"

    validation = validate_owner_review_template_v2_payload(payload, review_record=record)

    assert validation["validation_status"] == "FAIL"
    assert any(
        issue["issue_id"] == "safety_blocked_cannot_continue_shadow"
        for issue in validation["blocking_issues"]
    )


def test_owner_review_template_v2_cli_writes_report_and_validation(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    template_json = reports_dir / "owner_review_template_v2_2026-05-04.json"
    template_md = reports_dir / "owner_review_template_v2_2026-05-04.md"
    validation_json = reports_dir / "owner_review_template_v2_validation_2026-05-04.json"
    validation_md = reports_dir / "owner_review_template_v2_validation_2026-05-04.md"

    runner = CliRunner()
    run = runner.invoke(
        app,
        [
            "reports",
            "owner-review-template-v2",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert run.exit_code == 0, run.output

    validation = runner.invoke(
        app,
        [
            "reports",
            "validate-owner-review-template-v2",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )

    assert validation.exit_code == 0, validation.output
    assert template_md.exists()
    assert validation_md.exists()
    assert latest_owner_review_template_v2_json_path(reports_dir) == template_json
    assert json.loads(template_json.read_text(encoding="utf-8"))["template_status"] == (
        "TEMPLATE_READY"
    )
    validation_payload = json.loads(validation_json.read_text(encoding="utf-8"))
    assert validation_payload["validation_status"] == PASS_STATUS
    assert validation_payload["input_artifacts"]["owner_review_template_v2"] == str(
        template_json
    )


def test_owner_review_template_v2_cli_validates_filled_review(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    review_path = tmp_path / "filled_review.json"
    review_path.write_text(
        json.dumps(_filled_review_record(), ensure_ascii=False),
        encoding="utf-8",
    )

    runner = CliRunner()
    run = runner.invoke(
        app,
        [
            "reports",
            "owner-review-template-v2",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert run.exit_code == 0, run.output

    validation = runner.invoke(
        app,
        [
            "reports",
            "validate-owner-review-template-v2",
            "--latest",
            "--reports-dir",
            str(reports_dir),
            "--review-json-path",
            str(review_path),
        ],
    )

    assert validation.exit_code == 0, validation.output
    validation_payload = json.loads(
        (reports_dir / "owner_review_template_v2_validation_2026-05-04.json").read_text(
            encoding="utf-8"
        )
    )
    assert validation_payload["summary"]["review_record_provided"] is True
    assert validation_payload["input_artifacts"]["filled_owner_review"] == str(review_path)


def test_reader_brief_owner_review_template_v2_summary_reads_report_index(tmp_path: Path) -> None:
    payload = build_owner_review_template_v2_payload(as_of=RUN_DATE)
    validation = validate_owner_review_template_v2_payload(payload)
    template_path = tmp_path / "owner_review_template_v2_2026-05-04.json"
    validation_path = tmp_path / "owner_review_template_v2_validation_2026-05-04.json"
    template_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")

    summary = reader_brief._owner_review_template_v2_summary(
        {
            "reports": [
                {
                    "report_id": "owner_review_template_v2",
                    "latest_artifact_path": str(template_path),
                },
                {
                    "report_id": "owner_review_template_v2_validation",
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["template_status"] == "TEMPLATE_READY"
    assert summary["validation_status"] == PASS_STATUS
    assert summary["required_field_count"] == 9
    assert summary["owner_action_count"] == 8


def _filled_review_record() -> dict[str, object]:
    return {
        "candidate_id": "median_plus_regime_mismatch_filter",
        "evidence_interpretation": "Evidence supports continued observation with warnings.",
        "main_reason_to_continue": "Weekly review did not introduce a new blocker.",
        "main_reason_to_reject": "Reject if safety or data governance blocks reappear.",
        "uncertainty": "Forward sample remains limited.",
        "required_follow_up": "Review next weekly paper-shadow package.",
        "final_owner_action": "hold",
        "linked_input_artifacts": [
            {
                "artifact_id": "shadow_continuation_readiness",
                "artifact_path": "outputs/reports/example.json",
            }
        ],
        "safety_status": "SAFETY_PASS_WITH_WARNINGS",
        "production_effect": "none",
    }
