from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import assert_research_safe
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import (
    dynamic_v3_normal_paper_shadow_resumption_gate as gate,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st


def test_normal_paper_shadow_resumption_gate_allows_clean_manual_review(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_recovery(
        monkeypatch,
        signal_status="OK",
        evidence_status="FRESH",
        readiness_status="READY_TO_CONTINUE",
        health_status="HEALTHY",
    )

    result = gate.run_normal_paper_shadow_resumption_gate(
        as_of=date(2024, 4, 22),
        owner_action="continue_normal_shadow",
        manual_owner_review_completed=True,
        output_dir=tmp_path / "gate",
        generated_at=datetime(2024, 4, 22, 1, tzinfo=UTC),
    )
    report = result["normal_paper_shadow_resumption_gate_report"]

    assert report["normal_paper_shadow_resumption_gate_status"] == (
        "RESUME_NORMAL_SHADOW_ALLOWED"
    )
    assert report["normal_paper_shadow_may_resume"] is True
    assert report["owner_action"] == "continue_normal_shadow"
    assert report["extended_shadow_allowed"] is False
    assert report["live_trading_allowed"] is False
    assert result["normal_paper_shadow_resumption_gate_validation"]["status"] == "PASS"
    assert "normal_paper_shadow_resumption_gate_status" in result["reader_brief_section"]
    assert_research_safe(result["manifest"])


def test_normal_paper_shadow_resumption_gate_preserves_warnings(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_recovery(
        monkeypatch,
        signal_status="WARNING",
        evidence_status="ACCEPTABLE",
        readiness_status="MANUAL_REVIEW_REQUIRED",
        health_status="MANUAL_REVIEW_REQUIRED",
        warning_reasons=["signal_input_completeness:warning"],
    )

    result = gate.run_normal_paper_shadow_resumption_gate(
        as_of=date(2024, 4, 22),
        owner_action="continue_normal_shadow",
        manual_owner_review_completed=True,
        output_dir=tmp_path / "gate",
        generated_at=datetime(2024, 4, 22, 2, tzinfo=UTC),
    )
    report = result["normal_paper_shadow_resumption_gate_report"]

    assert report["normal_paper_shadow_resumption_gate_status"] == (
        "RESUME_NORMAL_SHADOW_WITH_WARNINGS"
    )
    assert report["normal_paper_shadow_may_resume"] is True
    assert "signal_input_completeness_not_blocking" in report["warning_reasons"]
    assert "canonical_health_not_blocked" in report["warning_reasons"]
    assert "readiness_health_recovery:signal_input_completeness:warning" in report[
        "warning_reasons"
    ]
    assert result["normal_paper_shadow_resumption_gate_validation"]["status"] == "PASS"


def test_normal_paper_shadow_resumption_gate_hold_blocks_resumption(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_recovery(
        monkeypatch,
        signal_status="OK",
        evidence_status="FRESH",
        readiness_status="READY_TO_CONTINUE",
        health_status="HEALTHY",
    )

    result = gate.run_normal_paper_shadow_resumption_gate(
        as_of=date(2024, 4, 22),
        owner_action="hold",
        manual_owner_review_completed=True,
        output_dir=tmp_path / "gate",
        generated_at=datetime(2024, 4, 22, 3, tzinfo=UTC),
    )
    report = result["normal_paper_shadow_resumption_gate_report"]

    assert report["normal_paper_shadow_resumption_gate_status"] == (
        "RESUME_NORMAL_SHADOW_BLOCKED"
    )
    assert report["normal_paper_shadow_may_resume"] is False
    assert report["owner_action_is_safe_non_promotion"] is True
    assert "owner_action:hold" in report["blocking_reasons"]
    assert result["normal_paper_shadow_resumption_gate_validation"]["status"] == "PASS"


def test_normal_paper_shadow_resumption_gate_blocks_promotion_like_owner_action(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_recovery(
        monkeypatch,
        signal_status="OK",
        evidence_status="FRESH",
        readiness_status="READY_TO_CONTINUE",
        health_status="HEALTHY",
    )

    result = gate.run_normal_paper_shadow_resumption_gate(
        as_of=date(2024, 4, 22),
        owner_action="enter_extended_shadow",
        manual_owner_review_completed=True,
        output_dir=tmp_path / "gate",
        generated_at=datetime(2024, 4, 22, 4, tzinfo=UTC),
    )
    report = result["normal_paper_shadow_resumption_gate_report"]

    assert report["normal_paper_shadow_resumption_gate_status"] == (
        "RESUME_NORMAL_SHADOW_BLOCKED"
    )
    assert report["owner_action_is_safe_non_promotion"] is False
    assert "owner_action_safe_non_promotion" in report["blocking_reasons"]
    assert "owner_action:not_allowed_for_normal_resumption" in report["blocking_reasons"]
    assert report["extended_shadow_allowed"] is False


def test_normal_paper_shadow_resumption_gate_cli_run_report_and_validate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_recovery(
        monkeypatch,
        signal_status="OK",
        evidence_status="FRESH",
        readiness_status="READY_TO_CONTINUE",
        health_status="HEALTHY",
    )
    output_dir = tmp_path / "gate_cli"

    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "normal-paper-shadow-resumption-gate",
            "run",
            "--as-of",
            "2024-04-22",
            "--owner-action",
            "continue_normal_shadow",
            "--manual-owner-review-completed",
            "--output-dir",
            str(output_dir),
        ],
    )
    assert run.exit_code == 0
    assert "normal_paper_shadow_resumption_gate_status=RESUME_NORMAL_SHADOW_ALLOWED" in (
        run.output
    )
    assert "extended_shadow_allowed=false" in run.output
    gate_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("gate_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "normal-paper-shadow-resumption-gate",
            "report",
            "--gate-id",
            gate_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "normal_paper_shadow_may_resume=True" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-normal-paper-shadow-resumption-gate",
            "--gate-id",
            gate_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def _patch_recovery(
    monkeypatch,
    *,
    signal_status: str,
    evidence_status: str,
    readiness_status: str,
    health_status: str,
    warning_reasons: list[str] | None = None,
) -> None:
    warning_reasons = warning_reasons or []

    def fake_recovery_payload(**kwargs):
        recovery_id = "readiness-health-recovery-test"
        source_statuses = {
            "signal_input_status": signal_status,
            "evidence_freshness_status": evidence_status,
            "evidence_safe_to_continue_shadow": evidence_status in {"FRESH", "ACCEPTABLE"},
            "shadow_continuation_readiness": readiness_status,
            "shadow_continuation_safe_to_continue_shadow": readiness_status
            in {"READY_TO_CONTINUE", "READY_WITH_WARNINGS"},
            "shadow_continuation_manual_review_required": readiness_status
            == "MANUAL_REVIEW_REQUIRED",
            "paper_shadow_health_status": health_status,
            "paper_shadow_health_safe_to_continue_shadow": health_status
            in {"HEALTHY", "HEALTHY_WITH_WARNINGS"},
        }
        report = {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_readiness_health_recovery_report",
            "recovery_id": recovery_id,
            "readiness_health_recovery_status": "MANUAL_REVIEW_REQUIRED"
            if warning_reasons
            else "PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION",
            "source_statuses": source_statuses,
            "source_validations": {
                "evidence_staleness_monitor": "PASS",
                "shadow_continuation_readiness": "PASS",
                "paper_shadow_health": "PASS",
            },
            "blocking_reasons": [],
            "warning_reasons": warning_reasons,
            "production_effect": "none",
            **st.SYSTEM_TARGET_SAFETY,
        }
        return {
            "schema_version": st.SCHEMA_VERSION,
            "recovery_id": recovery_id,
            "readiness_health_recovery_report_path": f"/tmp/{recovery_id}.json",
            "readiness_health_recovery_validation": _validation(recovery_id),
            "readiness_health_recovery_report": report,
            **st.SYSTEM_TARGET_SAFETY,
        }

    monkeypatch.setattr(
        gate.recovery,
        "readiness_health_recovery_report_payload",
        fake_recovery_payload,
    )


def _validation(artifact_id: str) -> dict[str, object]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "test_validation",
        "artifact_id": artifact_id,
        "status": "PASS",
        "failed_check_count": 0,
        **st.SYSTEM_TARGET_SAFETY,
    }
