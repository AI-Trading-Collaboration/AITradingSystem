from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import assert_research_safe
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import (
    dynamic_v3_readiness_health_recovery as recovery,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st


def test_readiness_health_recovery_allows_clean_normal_observation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_chain(
        monkeypatch,
        signal_status="OK",
        evidence_status="FRESH",
        readiness_status="READY_TO_CONTINUE",
        health_status="HEALTHY",
    )

    result = recovery.run_readiness_health_recovery_chain(
        as_of=date(2024, 4, 22),
        output_dir=tmp_path / "readiness_health_recovery",
        generated_at=datetime(2024, 4, 22, 1, tzinfo=UTC),
    )
    report = result["readiness_health_recovery_report"]

    assert report["readiness_health_recovery_status"] == (
        "PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION"
    )
    assert report["normal_paper_shadow_may_resume"] is True
    assert report["manual_review_required"] is False
    assert report["promotion_board_allowed"] is False
    assert report["extended_shadow_allowed"] is False
    assert result["readiness_health_recovery_validation"]["status"] == "PASS"
    assert "readiness_health_recovery_status" in result["reader_brief_section"]
    assert_research_safe(result["manifest"])


def test_readiness_health_recovery_fail_closes_blocked_health(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_chain(
        monkeypatch,
        signal_status="OK",
        evidence_status="FRESH",
        readiness_status="READY_TO_CONTINUE",
        health_status="BLOCKED_DATA",
        health_blockers=["cache_catalog:blocking"],
    )

    result = recovery.run_readiness_health_recovery_chain(
        as_of=date(2024, 4, 22),
        output_dir=tmp_path / "readiness_health_recovery",
        generated_at=datetime(2024, 4, 22, 2, tzinfo=UTC),
    )
    report = result["readiness_health_recovery_report"]

    assert report["readiness_health_recovery_status"] == "PAPER_SHADOW_STILL_BLOCKED"
    assert report["normal_paper_shadow_may_resume"] is False
    assert report["hard_stop_triggered"] is True
    assert "paper_shadow_health:blocked_data" in report["blocking_reasons"]
    assert "paper_shadow_health:cache_catalog:blocking" in report["blocking_reasons"]
    assert result["readiness_health_recovery_validation"]["status"] == "PASS"


def test_readiness_health_recovery_keeps_warnings_in_manual_review(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_chain(
        monkeypatch,
        signal_status="WARNING",
        evidence_status="ACCEPTABLE",
        readiness_status="READY_WITH_WARNINGS",
        readiness_manual_review=True,
        health_status="HEALTHY_WITH_WARNINGS",
        health_warnings=["signal_input_completeness:warning"],
    )

    result = recovery.run_readiness_health_recovery_chain(
        as_of=date(2024, 4, 22),
        output_dir=tmp_path / "readiness_health_recovery",
        generated_at=datetime(2024, 4, 22, 3, tzinfo=UTC),
    )
    report = result["readiness_health_recovery_report"]

    assert report["readiness_health_recovery_status"] == "MANUAL_REVIEW_REQUIRED"
    assert report["normal_paper_shadow_may_resume"] is False
    assert report["hard_stop_triggered"] is False
    assert "signal_input_completeness:warning" in report["warning_reasons"]
    assert "paper_shadow_health:healthy_with_warnings" in report["warning_reasons"]
    assert result["readiness_health_recovery_validation"]["status"] == "PASS"


def test_readiness_health_recovery_cli_run_report_and_validate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_chain(
        monkeypatch,
        signal_status="OK",
        evidence_status="FRESH",
        readiness_status="READY_TO_CONTINUE",
        health_status="HEALTHY",
    )
    output_dir = tmp_path / "readiness_health_recovery_cli"

    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "readiness-health-recovery",
            "run",
            "--as-of",
            "2024-04-22",
            "--output-dir",
            str(output_dir),
        ],
    )
    assert run.exit_code == 0
    expected_status = (
        "readiness_health_recovery_status="
        "PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION"
    )
    assert expected_status in run.output
    assert "promotion_board_allowed=false" in run.output
    recovery_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("recovery_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "readiness-health-recovery",
            "report",
            "--recovery-id",
            recovery_id,
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
            "validate-readiness-health-recovery",
            "--recovery-id",
            recovery_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def _patch_chain(
    monkeypatch,
    *,
    signal_status: str,
    evidence_status: str,
    readiness_status: str,
    health_status: str,
    readiness_manual_review: bool = False,
    health_blockers: list[str] | None = None,
    health_warnings: list[str] | None = None,
) -> None:
    health_blockers = health_blockers or []
    health_warnings = health_warnings or []

    def fake_staleness(**kwargs):
        monitor_id = "evidence-staleness-test"
        return {
            "monitor_id": monitor_id,
            "manifest": {
                "evidence_staleness_report_path": f"/tmp/{monitor_id}.json",
                **st.SYSTEM_TARGET_SAFETY,
            },
            "evidence_staleness_report": {
                "schema_version": st.SCHEMA_VERSION,
                "report_type": "etf_dynamic_v3_evidence_staleness_report",
                "monitor_id": monitor_id,
                "evidence_freshness_status": evidence_status,
                "safe_to_continue_shadow": evidence_status in {"FRESH", "ACCEPTABLE"},
                "blocking_artifacts": ["price_data"] if evidence_status == "BLOCKING" else [],
                "missing_artifacts": [],
                "stale_artifacts": ["paper_shadow_weekly_review"]
                if evidence_status == "ACCEPTABLE"
                else [],
                "signal_input_status": signal_status,
                "production_effect": "none",
                **st.SYSTEM_TARGET_SAFETY,
            },
            "evidence_staleness_validation": _validation(monitor_id),
        }

    def fake_readiness(**kwargs):
        readiness_id = "shadow-readiness-test"
        blocked = readiness_status.startswith("BLOCKED_")
        return {
            "readiness_id": readiness_id,
            "manifest": {
                "shadow_continuation_readiness_report_path": f"/tmp/{readiness_id}.json",
                **st.SYSTEM_TARGET_SAFETY,
            },
            "shadow_continuation_readiness_report": {
                "schema_version": st.SCHEMA_VERSION,
                "report_type": "etf_dynamic_v3_shadow_continuation_readiness_report",
                "readiness_id": readiness_id,
                "shadow_continuation_readiness": readiness_status,
                "safe_to_continue_shadow": readiness_status in {
                    "READY_TO_CONTINUE",
                    "READY_WITH_WARNINGS",
                },
                "missing_artifacts": ["paper_shadow_daily"] if blocked else [],
                "blocking_artifacts": [],
                "stale_artifacts": [],
                "manual_review_required": readiness_manual_review,
                "signal_input_status": signal_status,
                "production_effect": "none",
                **st.SYSTEM_TARGET_SAFETY,
            },
            "shadow_continuation_readiness_validation": _validation(readiness_id),
        }

    def fake_health(**kwargs):
        health_id = "paper-shadow-health-test"
        return {
            "health_id": health_id,
            "manifest": {
                "paper_shadow_health_report_path": f"/tmp/{health_id}.json",
                **st.SYSTEM_TARGET_SAFETY,
            },
            "paper_shadow_health_report": {
                "schema_version": st.SCHEMA_VERSION,
                "report_type": "etf_dynamic_v3_paper_shadow_health_report",
                "health_id": health_id,
                "paper_shadow_health_status": health_status,
                "safe_to_continue_shadow": health_status in {
                    "HEALTHY",
                    "HEALTHY_WITH_WARNINGS",
                },
                "signal_input_status": signal_status,
                "blocking_reasons": health_blockers,
                "warnings": health_warnings,
                "production_effect": "none",
                **st.SYSTEM_TARGET_SAFETY,
            },
            "paper_shadow_health_validation": _validation(health_id),
        }

    monkeypatch.setattr(recovery.readiness, "run_evidence_staleness_monitor", fake_staleness)
    monkeypatch.setattr(
        recovery.readiness,
        "run_shadow_continuation_readiness_report",
        fake_readiness,
    )
    monkeypatch.setattr(recovery.health, "run_paper_shadow_health_report", fake_health)


def _validation(artifact_id: str) -> dict[str, object]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "test_validation",
        "artifact_id": artifact_id,
        "status": "PASS",
        "failed_check_count": 0,
        **st.SYSTEM_TARGET_SAFETY,
    }
