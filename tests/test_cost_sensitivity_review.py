from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_filtered_candidate_readiness_helpers import assert_research_safe
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import dynamic_v3_cost_sensitivity as cost
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st


def test_cost_sensitivity_review_all_scenarios_meaningful(tmp_path: Path) -> None:
    fixture = _cost_fixture(
        tmp_path,
        metrics={
            "metrics_id": "candidate-metrics-strong",
            "candidate": "median_plus_regime_mismatch_filter",
            "candidate_lineage_id": "filtered-candidate-lineage-2026-06-15",
            "source_variant": "median_plus_regime_mismatch_filter",
            "window_start": "2026-06-08",
            "window_end": "2026-06-15",
            "generated_at": "2026-06-15T23:00:00+00:00",
            "validation_status": "PASS",
            "evidence_status": "VALIDATED_DATED_METRICS",
            "outcome_mode": "PAPER_SHADOW_OBSERVED",
            "metric_source": "validated_paper_shadow_candidate_metrics",
            "turnover": 1.0,
            "gross_performance": 0.08,
            "baseline_performance": 0.06,
        },
    )

    result = cost.run_cost_sensitivity_review(
        as_of=date(2026, 6, 15),
        candidate_metrics_path=fixture["metrics_path"],
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_id=fixture["health_id"],
        paper_shadow_health_dir=fixture["health_dir"],
        output_dir=fixture["cost_dir"],
        generated_at=datetime(2026, 6, 16, 1, tzinfo=UTC),
    )
    review = result["cost_sensitivity_review"]
    validation = result["cost_sensitivity_validation"]
    high = _scenario(review, "high")

    assert review["cost_sensitivity_status"] == "MEANINGFUL_ALL_SCENARIOS"
    assert review["gross_improvement_proxy"] == 0.02
    assert high["cost_drag"] == 0.0025
    assert high["net_improvement_proxy"] == 0.0175
    assert high["improvement_remains_meaningful"] is True
    assert review["promotion_board_inputs"]["high_cost_improvement_meaningful"] is True
    assert validation["status"] == "PASS"
    assert result["input_snapshot"]["schema_version"] == (
        cost.COST_SENSITIVITY_INPUT_SCHEMA
    )
    assert review["evidence_status"] == "VALIDATED_DATED_METRICS"
    assert "cost_sensitivity_status" in result["reader_brief_section"]
    assert_research_safe(review)
    assert review["execution_model_ready"] is False
    assert review["broker_action_allowed"] is False


def test_cost_sensitivity_review_fails_closed_without_numeric_metrics(
    tmp_path: Path,
) -> None:
    fixture = _cost_fixture(tmp_path, metrics=None)

    result = cost.run_cost_sensitivity_review(
        as_of=date(2026, 6, 15),
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_id=fixture["health_id"],
        paper_shadow_health_dir=fixture["health_dir"],
        output_dir=fixture["cost_dir"],
        generated_at=datetime(2026, 6, 16, 1, tzinfo=UTC),
    )
    review = result["cost_sensitivity_review"]

    assert review["cost_sensitivity_status"] == "INSUFFICIENT_COST_INPUTS"
    assert "candidate_metrics:insufficient_cost_inputs" in review["blocking_reasons"]
    assert _scenario(review, "medium")["classification"] == "INSUFFICIENT_INPUTS"
    assert result["cost_sensitivity_validation"]["status"] == "PASS"


def test_cost_sensitivity_cli_run_report_and_validate(tmp_path: Path) -> None:
    fixture = _cost_fixture(
        tmp_path,
        metrics={
            "metrics_id": "candidate-metrics-fragile",
            "candidate": "median_plus_regime_mismatch_filter",
            "candidate_lineage_id": "filtered-candidate-lineage-2026-06-15",
            "source_variant": "median_plus_regime_mismatch_filter",
            "window_start": "2026-06-08",
            "window_end": "2026-06-15",
            "generated_at": "2026-06-15T23:00:00+00:00",
            "validation_status": "PASS",
            "evidence_status": "VALIDATED_DATED_METRICS",
            "outcome_mode": "PAPER_SHADOW_OBSERVED",
            "metric_source": "validated_paper_shadow_candidate_metrics",
            "turnover": 1.0,
            "gross_performance": 0.013,
            "baseline_performance": 0.01,
        },
    )
    output_dir = tmp_path / "cost_sensitivity_cli"

    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "cost-sensitivity-review",
            "run",
            "--as-of",
            "2026-06-15",
            "--candidate-metrics-path",
            str(fixture["metrics_path"]),
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
    assert "cost_sensitivity_status=NOT_MEANINGFUL_UNDER_COSTS" in run.output
    assert "high_cost_improvement_meaningful=False" in run.output
    assert "validation_status=PASS" in run.output
    review_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("review_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "cost-sensitivity-review",
            "report",
            "--review-id",
            review_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "cost_sensitivity_status=NOT_MEANINGFUL_UNDER_COSTS" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-cost-sensitivity-review",
            "--review-id",
            review_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def test_cost_sensitivity_validation_fails_closed_on_source_and_view_tamper(
    tmp_path: Path,
) -> None:
    fixture = _cost_fixture(
        tmp_path,
        metrics={
            "metrics_id": "candidate-metrics-tamper",
            "candidate": "median_plus_regime_mismatch_filter",
            "candidate_lineage_id": "filtered-candidate-lineage-2026-06-15",
            "source_variant": "median_plus_regime_mismatch_filter",
            "window_start": "2026-06-08",
            "window_end": "2026-06-15",
            "generated_at": "2026-06-15T23:00:00+00:00",
            "validation_status": "PASS",
            "evidence_status": "VALIDATED_DATED_METRICS",
            "outcome_mode": "PAPER_SHADOW_OBSERVED",
            "metric_source": "validated_paper_shadow_candidate_metrics",
            "turnover": 1.0,
            "gross_performance": 0.08,
            "baseline_performance": 0.06,
        },
    )
    result = cost.run_cost_sensitivity_review(
        as_of=date(2026, 6, 15),
        candidate_metrics_path=fixture["metrics_path"],
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_id=fixture["health_id"],
        paper_shadow_health_dir=fixture["health_dir"],
        output_dir=fixture["cost_dir"],
        generated_at=datetime(2026, 6, 16, 1, tzinfo=UTC),
    )
    review_id = result["review_id"]
    fixture["metrics_path"].write_text("{}\n", encoding="utf-8")
    assert cost.validate_cost_sensitivity_artifact(
        review_id=review_id,
        output_dir=fixture["cost_dir"],
        write_output=False,
    )["status"] == "FAIL"
    report_path = fixture["cost_dir"] / review_id / "cost_sensitivity_report.md"
    report_path.write_text("tampered\n", encoding="utf-8")
    assert cost.validate_cost_sensitivity_artifact(
        review_id=review_id,
        output_dir=fixture["cost_dir"],
        write_output=False,
    )["status"] == "FAIL"


def _cost_fixture(tmp_path: Path, *, metrics: dict[str, Any] | None) -> dict[str, Any]:
    fixture = {
        "weekly_id": "paper-shadow-weekly-cost-test",
        "health_id": "paper-shadow-health-cost-test",
        "weekly_dir": tmp_path / "paper_shadow_weekly_review",
        "health_dir": tmp_path / "paper_shadow_health",
        "cost_dir": tmp_path / "cost_sensitivity",
    }
    _write_weekly_artifact(fixture["weekly_dir"], fixture["weekly_id"])
    _write_health_artifact(fixture["health_dir"], fixture["health_id"])
    if metrics is not None:
        fixture["metrics_path"] = tmp_path / "candidate_metrics.json"
        _write_json(fixture["metrics_path"], {**metrics, **cost.COST_SENSITIVITY_SAFETY})
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
        "weekly_decision": "CONTINUE",
        "generated_at": "2026-06-12T23:00:00+00:00",
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
        **cost.COST_SENSITIVITY_SAFETY,
    }
    review = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_weekly_review",
        "weekly_review_id": weekly_id,
        "candidate": "median_plus_regime_mismatch_filter",
        "candidate_lineage_id": "filtered-candidate-lineage-2026-06-15",
        "week_start": "2026-06-08",
        "week_end": "2026-06-12",
        "generated_at": "2026-06-12T23:00:00+00:00",
        "weekly_decision": "CONTINUE",
        "coverage_status": "PASS",
        "coverage_classification": "FULL_WEEK_REVIEW",
        "summary": {
            "turnover_behavior": "STABLE",
            "benchmark_comparison_proxy": "STABLE",
        },
        **cost.COST_SENSITIVITY_SAFETY,
    }
    validation = {
        "schema_version": st.SCHEMA_VERSION,
        "artifact_id": weekly_id,
        "status": "PASS",
        "failed_check_count": 0,
        "checks": [],
        **cost.COST_SENSITIVITY_SAFETY,
    }
    _write_json(artifact_dir / "paper_shadow_weekly_manifest.json", manifest)
    _write_json(artifact_dir / "paper_shadow_weekly_review.json", review)
    _write_json(artifact_dir / "paper_shadow_weekly_validation.json", validation)
    (artifact_dir / "paper_shadow_weekly_report.md").write_text("# weekly\n", encoding="utf-8")
    (artifact_dir / "reader_brief_section.md").write_text(
        "- paper_shadow_weekly_review_id: test\n",
        encoding="utf-8",
    )
    st._write_latest_pointer(
        "latest_paper_shadow_weekly_review",
        weekly_id,
        artifact_dir / "paper_shadow_weekly_manifest.json",
    )


def _write_health_artifact(root: Path, health_id: str) -> None:
    artifact_dir = root / health_id
    artifact_dir.mkdir(parents=True)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_health_manifest",
        "health_id": health_id,
        "paper_shadow_health_status": "HEALTHY",
        "generated_at": "2026-06-15T22:00:00+00:00",
        "paper_shadow_health_manifest_path": str(
            artifact_dir / "paper_shadow_health_manifest.json"
        ),
        "paper_shadow_health_report_path": str(artifact_dir / "paper_shadow_health_report.json"),
        "paper_shadow_health_markdown_path": str(artifact_dir / "paper_shadow_health_report.md"),
        "reader_brief_section_path": str(artifact_dir / "reader_brief_section.md"),
        "validation_path": str(artifact_dir / "paper_shadow_health_validation.json"),
        **cost.COST_SENSITIVITY_SAFETY,
    }
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_health_report",
        "health_id": health_id,
        "candidate": "median_plus_regime_mismatch_filter",
        "candidate_lineage_id": "filtered-candidate-lineage-2026-06-15",
        "as_of": "2026-06-15",
        "generated_at": "2026-06-15T22:00:00+00:00",
        "paper_shadow_health_status": "HEALTHY",
        "safe_to_continue_shadow": True,
        "signal_input_status": "OK",
        "blocking_reasons": [],
        "warnings": [],
        **cost.COST_SENSITIVITY_SAFETY,
    }
    validation = {
        "schema_version": st.SCHEMA_VERSION,
        "artifact_id": health_id,
        "status": "PASS",
        "failed_check_count": 0,
        "checks": [],
        **cost.COST_SENSITIVITY_SAFETY,
    }
    _write_json(artifact_dir / "paper_shadow_health_manifest.json", manifest)
    _write_json(artifact_dir / "paper_shadow_health_report.json", report)
    _write_json(artifact_dir / "paper_shadow_health_validation.json", validation)
    (artifact_dir / "paper_shadow_health_report.md").write_text("# health\n", encoding="utf-8")
    (artifact_dir / "reader_brief_section.md").write_text(
        "- paper_shadow_health_status: HEALTHY\n",
        encoding="utf-8",
    )
    st._write_latest_pointer(
        "latest_paper_shadow_health",
        health_id,
        artifact_dir / "paper_shadow_health_manifest.json",
    )


def _scenario(review: dict[str, Any], scenario_id: str) -> dict[str, Any]:
    return next(
        row for row in review["scenario_results"] if row["scenario_id"] == scenario_id
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
