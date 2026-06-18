from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from test_return_to_research_reset import _write_decision_stage_inputs
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import next_research_cycle as next_cycle
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports import return_to_research_reset as return_reset

RUN_DATE = date(2026, 6, 17)


def test_next_research_cycle_builds_fail_closed_research_chain(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_return_to_research_inputs(reports_dir, tmp_path)

    payloads = next_cycle.build_next_research_cycle_payloads(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        data_quality_gate={
            "status": "PASS",
            "passed": True,
            "error_count": 0,
            "warning_count": 0,
            "report_path": str(reports_dir / "data_quality_2026-06-17.md"),
        },
    )

    assert payloads[next_cycle.INTAKE_REPORT_TYPE]["status"] == (
        "NEXT_RESEARCH_CYCLE_INTAKE_READY"
    )
    frozen = payloads[next_cycle.FROZEN_SPEC_REPORT_TYPE]
    assert frozen["summary"]["paper_shadow_eligible"] is False
    assert frozen["summary"]["market_regime"] == "ai_after_chatgpt"

    backfill = payloads[next_cycle.BACKFILL_REPORT_TYPE]
    assert backfill["status"] == "CANDIDATE_BACKFILL_NEEDS_EXECUTABLE_BINDING"
    assert backfill["summary"]["data_quality_status"] == "PASS"
    assert backfill["summary"]["official_target_weights_generated"] is False
    assert "executable_candidate_signal_binding" in backfill["missing_data_list"]

    gate = payloads[next_cycle.RESEARCH_GATE_REPORT_TYPE]
    assert gate["status"] == "NEEDS_MORE_EVIDENCE"
    assert gate["summary"]["paper_shadow_activation_allowed"] is False

    snapshot = payloads[next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE]
    assert snapshot["status"] == "NEXT_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE"
    assert snapshot["summary"]["live_trading_allowed"] is False
    assert snapshot["summary"]["broker_order_allowed"] is False

    for report_type, payload in payloads.items():
        validation = next_cycle.validate_next_research_cycle_payload(
            payload,
            expected_report_type=report_type,
        )
        assert validation["status"] == "PASS", report_type
        assert payload["safety_boundary"]["paper_shadow_candidate_created"] is False
        assert payload["safety_boundary"]["official_target_weights_generated"] is False
        assert payload["production_effect"] == "none"


def test_next_research_cycle_cli_writes_intake_freeze_and_validations(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_return_to_research_inputs(reports_dir, tmp_path)
    runner = CliRunner()

    intake_result = runner.invoke(
        app,
        [
            "reports",
            "next-research-cycle-intake",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert intake_result.exit_code == 0, intake_result.output

    freeze_result = runner.invoke(
        app,
        [
            "reports",
            "next-candidate-spec-freeze",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert freeze_result.exit_code == 0, freeze_result.output

    frozen_path = next_cycle.default_next_research_cycle_json_path(
        next_cycle.FROZEN_SPEC_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    frozen_payload = json.loads(frozen_path.read_text(encoding="utf-8"))
    backfill_payload = next_cycle.build_next_candidate_backfill_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        frozen_spec_payload=frozen_payload,
        data_quality_gate={"status": "PASS", "passed": True, "report_path": "dq.md"},
    )
    next_cycle.write_next_research_cycle_json(
        backfill_payload,
        next_cycle.default_next_research_cycle_json_path(
            next_cycle.BACKFILL_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    next_cycle.write_next_research_cycle_markdown(
        backfill_payload,
        next_cycle.default_next_research_cycle_markdown_path(
            next_cycle.BACKFILL_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-next-candidate-backfill",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    for report_type in (
        next_cycle.INTAKE_REPORT_TYPE,
        next_cycle.FROZEN_SPEC_REPORT_TYPE,
        next_cycle.BACKFILL_REPORT_TYPE,
        f"{next_cycle.BACKFILL_REPORT_TYPE}{next_cycle.VALIDATION_SUFFIX}",
    ):
        assert next_cycle.default_next_research_cycle_json_path(
            report_type,
            reports_dir,
            RUN_DATE,
        ).exists()


def test_reader_brief_summarizes_next_research_cycle_snapshot(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_return_to_research_inputs(reports_dir, tmp_path)
    payloads = next_cycle.build_next_research_cycle_payloads(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        data_quality_gate={
            "status": "PASS",
            "passed": True,
            "error_count": 0,
            "warning_count": 0,
            "report_path": str(reports_dir / "data_quality_2026-06-17.md"),
        },
    )
    snapshot = payloads[next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE]
    validation = next_cycle.validate_next_research_cycle_payload(
        snapshot,
        expected_report_type=next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE,
    )
    snapshot_path = next_cycle.write_next_research_cycle_json(
        snapshot,
        next_cycle.default_next_research_cycle_json_path(
            next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    validation_path = next_cycle.write_next_research_cycle_json(
        validation,
        next_cycle.default_next_research_cycle_json_path(
            f"{next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE}{next_cycle.VALIDATION_SUFFIX}",
            reports_dir,
            RUN_DATE,
        ),
    )
    report_index = {
        "reports": [
            {
                "report_id": next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE,
                "latest_artifact_path": str(snapshot_path),
            },
            {
                "report_id": (
                    f"{next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE}"
                    f"{next_cycle.VALIDATION_SUFFIX}"
                ),
                "latest_artifact_path": str(validation_path),
            },
        ]
    }

    summary = reader_brief._next_research_cycle_snapshot_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["status"] == "NEXT_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE"
    assert summary["research_gate_decision"] == "NEEDS_MORE_EVIDENCE"
    assert summary["validation_status"] == "PASS"
    assert summary["paper_shadow_activation_allowed"] is False
    assert summary["live_trading_allowed"] is False
    assert summary["official_target_weights_generated"] is False
    assert summary["broker_order_allowed"] is False


def _write_return_to_research_inputs(reports_dir: Path, project_root: Path) -> None:
    decision_dir = project_root / "docs" / "decisions"
    log_path = project_root / "data" / "governance" / "owner_decision_audit_log.jsonl"
    _write_decision_stage_inputs(reports_dir, project_root)
    payloads = return_reset.build_return_to_research_reset_payloads(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        decision_source_dir=decision_dir,
        owner_decision_log_path=log_path,
        append_owner_decision=True,
    )
    for report_type, payload in payloads.items():
        return_reset.write_return_to_research_json(
            payload,
            return_reset.default_return_to_research_json_path(
                report_type,
                reports_dir,
                RUN_DATE,
            ),
        )
