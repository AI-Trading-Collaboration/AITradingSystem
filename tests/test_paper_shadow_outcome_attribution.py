from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_filtered_candidate_readiness_helpers import assert_research_safe
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import (
    dynamic_v3_paper_shadow_outcome_attribution as attribution,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.reports import reader_brief


def test_outcome_attribution_classifies_weekly_and_health_drivers(
    tmp_path: Path,
) -> None:
    fixture = _outcome_fixture(tmp_path)

    result = attribution.run_paper_shadow_outcome_attribution(
        as_of=date(2026, 6, 16),
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_id=fixture["health_id"],
        paper_shadow_health_dir=fixture["health_dir"],
        output_dir=fixture["outcome_dir"],
        generated_at=datetime(2026, 6, 16, 4, tzinfo=UTC),
    )
    report = result["paper_shadow_outcome_attribution_report"]

    assert report["paper_shadow_outcome_attribution_status"] == (
        "OUTCOME_ATTRIBUTION_COMPLETE"
    )
    assert report["dominant_driver"] == "signal_input_incompleteness"
    assert report["dominant_confidence"] == "HIGH"
    assert report["active_driver_count"] == 9
    assert report["unknown_driver_count"] == 0
    assert result["paper_shadow_outcome_attribution_validation"]["status"] == "PASS"
    assert "paper_shadow_outcome_attribution_status" in result["reader_brief_section"]
    assert_research_safe(report)
    assert report["outcome_attribution_only"] is True
    assert report["weekly_decision_mutated"] is False
    assert report["data_downloaded_by_attribution"] is False


def test_outcome_attribution_fails_closed_without_weekly_review(
    tmp_path: Path,
) -> None:
    fixture = _outcome_fixture(tmp_path)

    result = attribution.run_paper_shadow_outcome_attribution(
        as_of=date(2026, 6, 16),
        weekly_review_id="missing-weekly-review",
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_id=fixture["health_id"],
        paper_shadow_health_dir=fixture["health_dir"],
        output_dir=tmp_path / "missing_weekly_outcome",
        generated_at=datetime(2026, 6, 16, 4, tzinfo=UTC),
    )
    report = result["paper_shadow_outcome_attribution_report"]

    assert report["paper_shadow_outcome_attribution_status"] == (
        "BLOCKED_MISSING_WEEKLY_REVIEW"
    )
    assert report["dominant_driver"] == "UNKNOWN"
    assert "paper_shadow_weekly_review:missing" in report["blocking_reasons"]
    assert report["source_artifacts"]["paper_shadow_weekly_review"]["exists"] is False
    assert result["paper_shadow_outcome_attribution_validation"]["status"] == "PASS"


def test_outcome_attribution_cli_run_report_and_validate(tmp_path: Path) -> None:
    fixture = _outcome_fixture(tmp_path)
    output_dir = tmp_path / "paper_shadow_outcome_attribution_cli"

    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "paper-shadow-outcome-attribution",
            "run",
            "--as-of",
            "2026-06-16",
            "--weekly-review-id",
            fixture["weekly_id"],
            "--weekly-review-dir",
            str(fixture["weekly_dir"]),
            "--paper-shadow-health-id",
            fixture["health_id"],
            "--paper-shadow-health-dir",
            str(fixture["health_dir"]),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert run.exit_code == 0
    assert "paper_shadow_outcome_attribution_status=OUTCOME_ATTRIBUTION_COMPLETE" in (
        run.output
    )
    assert "dominant_driver=signal_input_incompleteness" in run.output
    assert "validation_status=PASS" in run.output
    attribution_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("attribution_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "paper-shadow-outcome-attribution",
            "report",
            "--attribution-id",
            attribution_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "dominant_confidence=HIGH" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-paper-shadow-outcome-attribution",
            "--attribution-id",
            attribution_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def test_reader_brief_summarizes_outcome_attribution(tmp_path: Path) -> None:
    fixture = _outcome_fixture(tmp_path)
    result = attribution.run_paper_shadow_outcome_attribution(
        as_of=date(2026, 6, 16),
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_id=fixture["health_id"],
        paper_shadow_health_dir=fixture["health_dir"],
        output_dir=fixture["outcome_dir"],
        generated_at=datetime(2026, 6, 16, 4, tzinfo=UTC),
    )
    leaderboard_path = _write_minimal_leaderboard(tmp_path)
    manifest_path = (
        result["attribution_dir"] / "paper_shadow_outcome_attribution_manifest.json"
    )
    report_index = {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_parameter_sweep_leaderboard",
                "latest_artifact_path": str(leaderboard_path),
            },
            {
                "report_id": "etf_dynamic_v3_paper_shadow_outcome_attribution",
                "latest_artifact_path": str(manifest_path),
            },
        ]
    }

    summary = reader_brief._etf_dynamic_v3_parameter_research_summary(report_index)

    assert summary["paper_shadow_outcome_attribution_id"] == result["attribution_id"]
    assert summary["paper_shadow_outcome_attribution_status"] == (
        "OUTCOME_ATTRIBUTION_COMPLETE"
    )
    assert summary["paper_shadow_outcome_dominant_driver"] == (
        "signal_input_incompleteness"
    )
    assert summary["paper_shadow_outcome_validation_status"] == "PASS"
    assert summary["paper_shadow_outcome_attribution"] == str(manifest_path)


def _outcome_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = {
        "weekly_id": "paper-shadow-weekly-outcome-test",
        "health_id": "paper-shadow-health-outcome-test",
        "weekly_dir": tmp_path / "paper_shadow_weekly_review",
        "health_dir": tmp_path / "paper_shadow_health",
        "outcome_dir": tmp_path / "paper_shadow_outcome_attribution",
    }
    _write_weekly_artifact(fixture["weekly_dir"], fixture["weekly_id"])
    _write_health_artifact(fixture["health_dir"], fixture["health_id"])
    return fixture


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
        "weekly_decision": "WATCH",
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
        **attribution.OUTCOME_ATTRIBUTION_SAFETY,
    }
    review = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_weekly_review",
        "weekly_review_id": weekly_id,
        "candidate": "median_plus_regime_mismatch_filter",
        "week_start": "2026-06-08",
        "week_end": "2026-06-12",
        "weekly_decision": "WATCH",
        "coverage_status": "MANUAL_REVIEW_REQUIRED",
        "coverage_classification": "PARTIAL_ARTIFACT_WINDOW_REVIEW",
        "coverage_safe_for_continuation": False,
        "signal_input_status": "BLOCKING",
        "source_ledger_final_decision": "WATCH",
        "manual_coverage_override": True,
        "summary": {
            "coverage_status": "MANUAL_REVIEW_REQUIRED",
            "coverage_classification": "PARTIAL_ARTIFACT_WINDOW_REVIEW",
            "missing_input_artifacts": ["price_data"],
            "drift_severity_trend": {
                "max_severity": "WARNING",
                "sequence": ["NONE", "WARNING"],
            },
            "signal_stability": "CHANGED",
            "drawdown_behavior": "DRAWDOWN_EXPANDED",
        },
        "daily_observations": [
            {
                "as_of": "2026-06-11",
                "signal_output": "CONTINUE",
                "risk_off_risk_on_state": "normal",
                "benchmark_comparison": "OUTPERFORMED_QQQ",
            },
            {
                "as_of": "2026-06-12",
                "signal_output": "WATCH",
                "risk_off_risk_on_state": "risk_off",
                "benchmark_comparison": "UNDERPERFORMED_QQQ",
            },
        ],
        **attribution.OUTCOME_ATTRIBUTION_SAFETY,
    }
    validation = {
        "schema_version": st.SCHEMA_VERSION,
        "artifact_id": weekly_id,
        "status": "PASS",
        "failed_check_count": 0,
        "checks": [],
        **attribution.OUTCOME_ATTRIBUTION_SAFETY,
    }
    _write_json(artifact_dir / "paper_shadow_weekly_manifest.json", manifest)
    _write_json(artifact_dir / "paper_shadow_weekly_review.json", review)
    _write_json(artifact_dir / "paper_shadow_weekly_validation.json", validation)
    (artifact_dir / "paper_shadow_weekly_report.md").write_text("# weekly\n", encoding="utf-8")
    (artifact_dir / "reader_brief_section.md").write_text(
        "- paper_shadow_weekly_review_id: test\n",
        encoding="utf-8",
    )


def _write_health_artifact(root: Path, health_id: str) -> None:
    artifact_dir = root / health_id
    artifact_dir.mkdir(parents=True)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_health_manifest",
        "health_id": health_id,
        "paper_shadow_health_status": "BLOCKED_SIGNAL_INPUTS",
        "paper_shadow_health_manifest_path": str(
            artifact_dir / "paper_shadow_health_manifest.json"
        ),
        "paper_shadow_health_report_path": str(
            artifact_dir / "paper_shadow_health_report.json"
        ),
        "paper_shadow_health_markdown_path": str(
            artifact_dir / "paper_shadow_health_report.md"
        ),
        "reader_brief_section_path": str(artifact_dir / "reader_brief_section.md"),
        "validation_path": str(artifact_dir / "paper_shadow_health_validation.json"),
        **attribution.OUTCOME_ATTRIBUTION_SAFETY,
    }
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_health_report",
        "health_id": health_id,
        "paper_shadow_health_status": "BLOCKED_SIGNAL_INPUTS",
        "safe_to_continue_shadow": False,
        "data_freshness_status": "BLOCKING",
        "signal_input_status": "BLOCKING",
        "fallback_status": "FALLBACK_USED",
        "cache_integrity_status": "OK",
        "weekly_review_coverage_status": "MANUAL_REVIEW_REQUIRED",
        "drift_status": "WARNING",
        "blocking_reasons": ["signal_input_completeness:blocking"],
        "warnings": ["data_stale_warning"],
        "next_required_action": "stop_paper_shadow_until_signal_inputs_are_restored",
        **attribution.OUTCOME_ATTRIBUTION_SAFETY,
    }
    validation = {
        "schema_version": st.SCHEMA_VERSION,
        "artifact_id": health_id,
        "status": "PASS",
        "failed_check_count": 0,
        "checks": [],
        **attribution.OUTCOME_ATTRIBUTION_SAFETY,
    }
    _write_json(artifact_dir / "paper_shadow_health_manifest.json", manifest)
    _write_json(artifact_dir / "paper_shadow_health_report.json", report)
    _write_json(artifact_dir / "paper_shadow_health_validation.json", validation)
    (artifact_dir / "paper_shadow_health_report.md").write_text("# health\n", encoding="utf-8")
    (artifact_dir / "reader_brief_section.md").write_text(
        "- paper_shadow_health_status: BLOCKED_SIGNAL_INPUTS\n",
        encoding="utf-8",
    )


def _write_minimal_leaderboard(tmp_path: Path) -> Path:
    path = tmp_path / "leaderboard" / "leaderboard.json"
    safety = {
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_state_mutated": False,
        "baseline_config_mutated": False,
        "official_target_weights_mutated": False,
        "automatic_candidate_promotion": False,
        "auto_enrollment_without_owner_approval": False,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
        "production_candidate_generated": False,
    }
    _write_json(
        path,
        {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_parameter_sweep_leaderboard",
            "status": "PASS",
            "evaluator_mode": "tiny_fixture_proxy",
            "metrics_source": "fixture",
            "not_for_investment_decision": True,
            "summary_sentence": "fixture leaderboard",
            "candidate_count": 1,
            "top_eligible_candidates": [
                {
                    "candidate": "median_plus_regime_mismatch_filter",
                    "gate": "observe_only",
                    "score": 0.1,
                }
            ],
            "most_common_reject_reasons": [],
            "safety": safety,
            "production_candidate_generated": False,
        },
    )
    return path


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
