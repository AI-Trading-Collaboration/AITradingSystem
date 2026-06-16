from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_filtered_candidate_readiness_helpers import assert_research_safe
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import (
    dynamic_v3_shadow_decision_comparison as comparison,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.reports import reader_brief


def test_shadow_decision_comparison_classifies_blocked_change(
    tmp_path: Path,
) -> None:
    fixture = _comparison_fixture(tmp_path)

    result = comparison.run_shadow_decision_comparison(
        as_of=date(2026, 6, 16),
        current_readiness_id=fixture["current_readiness_id"],
        readiness_dir=fixture["readiness_dir"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_dir=fixture["health_dir"],
        output_dir=fixture["comparison_dir"],
        generated_at=datetime(2026, 6, 16, 5, tzinfo=UTC),
    )
    report = result["shadow_decision_comparison_report"]

    assert report["shadow_decision_comparison_status"] == (
        "DECISION_COMPARISON_WITH_WARNINGS"
    )
    assert report["previous_readiness_id"] == fixture["previous_readiness_id"]
    assert report["decision_changed"] is True
    assert report["change_classification"] == "BLOCKED"
    assert "signal_input_completeness" in report["change_reason"]
    assert report["previous_state"]["safe_to_continue_shadow"] is True
    assert report["current_state"]["safe_to_continue_shadow"] is False
    assert report["current_state"]["signal_input_completeness"] == "BLOCKING"
    assert result["shadow_decision_comparison_validation"]["status"] == "PASS"
    assert "shadow_decision_comparison_status" in result["reader_brief_section"]
    assert_research_safe(report)
    assert report["shadow_decision_comparison_only"] is True
    assert report["decision_mutated"] is False
    assert report["data_downloaded_by_comparison"] is False


def test_shadow_decision_comparison_fails_closed_without_previous(
    tmp_path: Path,
) -> None:
    fixture = _comparison_fixture(tmp_path)

    result = comparison.run_shadow_decision_comparison(
        as_of=date(2026, 6, 16),
        current_readiness_id=fixture["current_readiness_id"],
        previous_readiness_id="missing-previous-readiness",
        readiness_dir=fixture["readiness_dir"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_dir=fixture["health_dir"],
        output_dir=tmp_path / "missing_previous_comparison",
        generated_at=datetime(2026, 6, 16, 5, tzinfo=UTC),
    )
    report = result["shadow_decision_comparison_report"]

    assert report["shadow_decision_comparison_status"] == (
        "BLOCKED_MISSING_PREVIOUS_DECISION"
    )
    assert report["change_classification"] == "BLOCKED"
    assert report["decision_changed"] is True
    assert report["previous_state"]["readiness_status"] == "MISSING"
    assert result["shadow_decision_comparison_validation"]["status"] == "PASS"


def test_shadow_decision_comparison_classifies_recovered_change(
    tmp_path: Path,
) -> None:
    fixture = _comparison_fixture(tmp_path)
    recovered_id = "shadow-continuation-readiness-recovered"
    _write_readiness_artifact(
        fixture["readiness_dir"],
        recovered_id,
        weekly_id=fixture["weekly_id"],
        generated_at="2026-06-17T04:00:00+00:00",
        readiness_status="READY_TO_CONTINUE",
        safe_to_continue=True,
        signal_status="OK",
        blocking_artifacts=[],
    )

    result = comparison.run_shadow_decision_comparison(
        as_of=date(2026, 6, 17),
        current_readiness_id=recovered_id,
        previous_readiness_id=fixture["current_readiness_id"],
        readiness_dir=fixture["readiness_dir"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_dir=fixture["health_dir"],
        output_dir=tmp_path / "recovered_comparison",
        generated_at=datetime(2026, 6, 17, 5, tzinfo=UTC),
    )
    report = result["shadow_decision_comparison_report"]

    assert report["decision_changed"] is True
    assert report["change_classification"] == "RECOVERED"
    assert report["recommended_owner_action"] == "confirm_recovery_before_resuming_shadow"
    assert report["previous_state"]["safe_to_continue_shadow"] is False
    assert report["current_state"]["safe_to_continue_shadow"] is True
    assert result["shadow_decision_comparison_validation"]["status"] == "PASS"


def test_shadow_decision_comparison_cli_run_report_and_validate(
    tmp_path: Path,
) -> None:
    fixture = _comparison_fixture(tmp_path)
    output_dir = tmp_path / "shadow_decision_comparison_cli"

    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "shadow-decision-comparison",
            "run",
            "--as-of",
            "2026-06-16",
            "--current-readiness-id",
            fixture["current_readiness_id"],
            "--readiness-dir",
            str(fixture["readiness_dir"]),
            "--weekly-review-dir",
            str(fixture["weekly_dir"]),
            "--paper-shadow-health-dir",
            str(fixture["health_dir"]),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert run.exit_code == 0
    assert "shadow_decision_comparison_status=DECISION_COMPARISON_WITH_WARNINGS" in (
        run.output
    )
    assert "change_classification=BLOCKED" in run.output
    assert "decision_changed=True" in run.output
    assert "validation_status=PASS" in run.output
    comparison_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("comparison_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "shadow-decision-comparison",
            "report",
            "--comparison-id",
            comparison_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "recommended_owner_action=hold_shadow_until_blockers_resolved" in (
        report.output
    )

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-shadow-decision-comparison",
            "--comparison-id",
            comparison_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def test_reader_brief_summarizes_shadow_decision_comparison(
    tmp_path: Path,
) -> None:
    fixture = _comparison_fixture(tmp_path)
    result = comparison.run_shadow_decision_comparison(
        as_of=date(2026, 6, 16),
        current_readiness_id=fixture["current_readiness_id"],
        readiness_dir=fixture["readiness_dir"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_dir=fixture["health_dir"],
        output_dir=fixture["comparison_dir"],
        generated_at=datetime(2026, 6, 16, 5, tzinfo=UTC),
    )
    leaderboard_path = _write_minimal_leaderboard(tmp_path)
    manifest_path = (
        result["comparison_dir"] / "shadow_decision_comparison_manifest.json"
    )
    report_index = {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_parameter_sweep_leaderboard",
                "latest_artifact_path": str(leaderboard_path),
            },
            {
                "report_id": "etf_dynamic_v3_shadow_decision_comparison",
                "latest_artifact_path": str(manifest_path),
            },
        ]
    }

    summary = reader_brief._etf_dynamic_v3_parameter_research_summary(report_index)

    assert summary["shadow_decision_comparison_id"] == result["comparison_id"]
    assert summary["shadow_decision_comparison_status"] == (
        "DECISION_COMPARISON_WITH_WARNINGS"
    )
    assert summary["shadow_decision_changed"] is True
    assert summary["shadow_decision_change_classification"] == "BLOCKED"
    assert summary["shadow_decision_current_state"] == "BLOCKED_STALE_DATA"
    assert summary["shadow_decision_previous_state"] == "READY_TO_CONTINUE"
    assert summary["shadow_decision_validation_status"] == "PASS"
    assert summary["shadow_decision_comparison"] == str(manifest_path)


def _comparison_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = {
        "current_readiness_id": "shadow-continuation-readiness-current",
        "previous_readiness_id": "shadow-continuation-readiness-previous",
        "weekly_id": "paper-shadow-weekly-comparison-test",
        "readiness_dir": tmp_path / "shadow_continuation_readiness",
        "weekly_dir": tmp_path / "paper_shadow_weekly_review",
        "health_dir": tmp_path / "paper_shadow_health",
        "comparison_dir": tmp_path / "shadow_decision_comparison",
    }
    _write_weekly_artifact(fixture["weekly_dir"], fixture["weekly_id"])
    _write_readiness_artifact(
        fixture["readiness_dir"],
        fixture["previous_readiness_id"],
        weekly_id=fixture["weekly_id"],
        generated_at="2026-06-15T04:00:00+00:00",
        readiness_status="READY_TO_CONTINUE",
        safe_to_continue=True,
        signal_status="OK",
        blocking_artifacts=[],
    )
    _write_readiness_artifact(
        fixture["readiness_dir"],
        fixture["current_readiness_id"],
        weekly_id=fixture["weekly_id"],
        generated_at="2026-06-16T04:00:00+00:00",
        readiness_status="BLOCKED_STALE_DATA",
        safe_to_continue=False,
        signal_status="BLOCKING",
        blocking_artifacts=["signal_input_completeness"],
    )
    _write_health_artifact(
        fixture["health_dir"],
        "paper-shadow-health-comparison-test",
        readiness_id=fixture["current_readiness_id"],
        signal_status="BLOCKING",
    )
    return fixture


def _write_readiness_artifact(
    root: Path,
    readiness_id: str,
    *,
    weekly_id: str,
    generated_at: str,
    readiness_status: str,
    safe_to_continue: bool,
    signal_status: str,
    blocking_artifacts: list[str],
) -> None:
    artifact_dir = root / readiness_id
    artifact_dir.mkdir(parents=True)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_continuation_readiness_report",
        "readiness_id": readiness_id,
        "candidate": "median_plus_regime_mismatch_filter",
        "as_of": "2026-06-16",
        "generated_at": generated_at,
        "shadow_continuation_readiness": readiness_status,
        "safe_to_continue_shadow": safe_to_continue,
        "missing_artifacts": [],
        "blocking_artifacts": blocking_artifacts,
        "stale_artifacts": [],
        "coverage_status": "PASS",
        "fallback_status": "PRIMARY_OK",
        "fallback_policy_summary": {"fallback_status": "PRIMARY_OK"},
        "signal_input_status": signal_status,
        "signal_input_completeness_summary": {
            "signal_input_status": signal_status,
            "blocking_count": 1 if signal_status == "BLOCKING" else 0,
        },
        "manual_review_required": not safe_to_continue,
        "next_required_action": (
            "stop_paper_shadow_until_signal_inputs_are_restored"
            if signal_status == "BLOCKING"
            else "continue_shadow_with_owner_review"
        ),
        "data_validation_status": "PASS",
        "source_artifacts": {
            "paper_shadow_weekly_review": {
                "source_id": "paper_shadow_weekly_review",
                "exists": True,
                "artifact_id": weekly_id,
                "status": "CONTINUE",
                "validation_status": "PASS",
                "summary": {
                    "weekly_review_id": weekly_id,
                    "weekly_decision": "CONTINUE",
                },
            },
            "paper_shadow_drift_monitor": {
                "source_id": "paper_shadow_drift_monitor",
                "exists": True,
                "artifact_id": "paper-shadow-drift-comparison-test",
                "status": "NONE",
                "validation_status": "PASS",
                "summary": {"drift_status": "NONE"},
            },
            "signal_input_completeness": {
                "source_id": "signal_input_completeness",
                "exists": True,
                "artifact_id": "signal-input-comparison-test",
                "status": signal_status,
                "validation_status": "PASS",
                "summary": {"signal_input_status": signal_status},
            },
        },
        "safety_boundary_status": "PASS",
        **comparison.SHADOW_DECISION_COMPARISON_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_continuation_readiness_manifest",
        "readiness_id": readiness_id,
        "candidate": "median_plus_regime_mismatch_filter",
        "as_of": "2026-06-16",
        "generated_at": generated_at,
        "status": "PASS" if safe_to_continue else "MANUAL_REVIEW_REQUIRED",
        "shadow_continuation_readiness": readiness_status,
        "safe_to_continue_shadow": safe_to_continue,
        "shadow_continuation_readiness_manifest_path": str(
            artifact_dir / "shadow_continuation_readiness_manifest.json"
        ),
        "shadow_continuation_readiness_report_path": str(
            artifact_dir / "shadow_continuation_readiness_report.json"
        ),
        "shadow_continuation_readiness_markdown_path": str(
            artifact_dir / "shadow_continuation_readiness_report.md"
        ),
        "reader_brief_section_path": str(artifact_dir / "reader_brief_section.md"),
        "validation_path": str(
            artifact_dir / "shadow_continuation_readiness_validation.json"
        ),
        **comparison.SHADOW_DECISION_COMPARISON_SAFETY,
    }
    validation = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_continuation_readiness_validation",
        "artifact_id": readiness_id,
        "status": "PASS",
        "failed_check_count": 0,
        "checks": [],
        "production_effect": "none",
    }
    _write_json(artifact_dir / "shadow_continuation_readiness_manifest.json", manifest)
    _write_json(artifact_dir / "shadow_continuation_readiness_report.json", report)
    (artifact_dir / "shadow_continuation_readiness_report.md").write_text(
        "# Readiness\n",
        encoding="utf-8",
    )
    (artifact_dir / "reader_brief_section.md").write_text(
        "shadow_continuation_readiness\n",
        encoding="utf-8",
    )
    _write_json(artifact_dir / "shadow_continuation_readiness_validation.json", validation)


def _write_weekly_artifact(root: Path, weekly_id: str) -> None:
    artifact_dir = root / weekly_id
    artifact_dir.mkdir(parents=True)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_weekly_manifest",
        "weekly_review_id": weekly_id,
        "candidate": "median_plus_regime_mismatch_filter",
        "week_start": "2026-06-08",
        "week_end": "2026-06-12",
        "status": "PASS",
        "weekly_decision": "CONTINUE",
        "paper_shadow_weekly_manifest_path": str(
            artifact_dir / "paper_shadow_weekly_manifest.json"
        ),
        "paper_shadow_weekly_review_path": str(
            artifact_dir / "paper_shadow_weekly_review.json"
        ),
        "paper_shadow_weekly_report_path": str(
            artifact_dir / "paper_shadow_weekly_report.md"
        ),
        "reader_brief_section_path": str(artifact_dir / "reader_brief_section.md"),
        "validation_path": str(artifact_dir / "paper_shadow_weekly_validation.json"),
        **comparison.SHADOW_DECISION_COMPARISON_SAFETY,
    }
    review = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_weekly_review",
        "weekly_review_id": weekly_id,
        "candidate": "median_plus_regime_mismatch_filter",
        "week_start": "2026-06-08",
        "week_end": "2026-06-12",
        "weekly_decision": "CONTINUE",
        "coverage_status": "PASS",
        "coverage_classification": "FULL_WEEK_REVIEW",
        "coverage_safe_for_continuation": True,
        "signal_input_status": "OK",
        "summary": {
            "drift_severity_trend": {
                "max_severity": "NONE",
                "sequence": ["NONE"],
            },
        },
        **comparison.SHADOW_DECISION_COMPARISON_SAFETY,
    }
    validation = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_weekly_validation",
        "artifact_id": weekly_id,
        "status": "PASS",
        "failed_check_count": 0,
        "checks": [],
        "production_effect": "none",
    }
    _write_json(artifact_dir / "paper_shadow_weekly_manifest.json", manifest)
    _write_json(artifact_dir / "paper_shadow_weekly_review.json", review)
    (artifact_dir / "paper_shadow_weekly_report.md").write_text(
        "# Weekly\n",
        encoding="utf-8",
    )
    (artifact_dir / "reader_brief_section.md").write_text(
        "paper_shadow_weekly_review\n",
        encoding="utf-8",
    )
    _write_json(artifact_dir / "paper_shadow_weekly_validation.json", validation)


def _write_health_artifact(
    root: Path,
    health_id: str,
    *,
    readiness_id: str,
    signal_status: str,
) -> None:
    artifact_dir = root / health_id
    artifact_dir.mkdir(parents=True)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_health_report",
        "health_id": health_id,
        "as_of": "2026-06-16",
        "generated_at": "2026-06-16T04:30:00+00:00",
        "paper_shadow_health_status": "BLOCKED_SIGNAL_INPUTS",
        "safe_to_continue_shadow": False,
        "signal_input_status": signal_status,
        "fallback_status": "PRIMARY_OK",
        "drift_status": "NONE",
        "source_artifacts": {
            "shadow_continuation_readiness": {
                "source_id": "shadow_continuation_readiness",
                "exists": True,
                "artifact_id": readiness_id,
                "status": "BLOCKED_STALE_DATA",
                "validation_status": "PASS",
                "summary": {"readiness_id": readiness_id},
            }
        },
        "blocking_reasons": ["signal_input_completeness:blocking"],
        "warnings": [],
        "next_required_action": "stop_paper_shadow_until_signal_inputs_are_restored",
        **comparison.SHADOW_DECISION_COMPARISON_SAFETY,
    }
    _write_json(artifact_dir / "paper_shadow_health_report.json", report)


def _write_minimal_leaderboard(tmp_path: Path) -> Path:
    path = tmp_path / "leaderboard_manifest.json"
    _write_json(
        path,
        {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_parameter_sweep_leaderboard",
            "run_id": "leaderboard-test",
            "generated_at": "2026-06-16T00:00:00+00:00",
            "production_effect": "none",
        },
    )
    return path


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
