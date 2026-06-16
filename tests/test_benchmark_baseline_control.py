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
    dynamic_v3_benchmark_baseline_control as baseline,
)
from ai_trading_system.etf_portfolio import dynamic_v3_cost_sensitivity as cost
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.reports import reader_brief


def test_benchmark_baseline_control_all_baselines_outperformed(tmp_path: Path) -> None:
    fixture = _baseline_fixture(tmp_path)

    result = baseline.run_benchmark_baseline_control_pack(
        as_of=date(2026, 6, 15),
        candidate_metrics_path=fixture["candidate_metrics_path"],
        baseline_metrics_path=_baseline_metrics_path(tmp_path, "strong", 0.05),
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        cost_sensitivity_review_id=fixture["cost_id"],
        cost_sensitivity_dir=fixture["cost_dir"],
        output_dir=fixture["baseline_dir"],
        generated_at=datetime(2026, 6, 16, 2, tzinfo=UTC),
    )
    pack = result["benchmark_baseline_control_pack"]
    summary = pack["comparison_summary"]

    assert pack["benchmark_baseline_status"] == "CANDIDATE_OUTPERFORMS_BASELINES"
    assert pack["baseline_count"] == 5
    assert summary["outperformed_baseline_count"] == 5
    assert summary["insufficient_metric_baseline_count"] == 0
    assert all(row["asset_universe"] for row in pack["baselines"])
    assert all(row["rebalancing_assumption"] for row in pack["baselines"])
    assert all(row["cost_assumption"] for row in pack["baselines"])
    assert pack["monthly_review_pack_inputs"]["source_cost_sensitivity_review_id"] == fixture[
        "cost_id"
    ]
    assert result["benchmark_baseline_validation"]["status"] == "PASS"
    assert "benchmark_baseline_status" in result["reader_brief_section"]
    assert_research_safe(pack)
    assert pack["execution_model_ready"] is False
    assert pack["broker_action_allowed"] is False


def test_benchmark_baseline_control_fails_closed_without_numeric_metrics(
    tmp_path: Path,
) -> None:
    fixture = _baseline_fixture(tmp_path)

    result = baseline.run_benchmark_baseline_control_pack(
        as_of=date(2026, 6, 15),
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        cost_sensitivity_review_id=fixture["cost_id"],
        cost_sensitivity_dir=fixture["cost_dir"],
        output_dir=fixture["baseline_dir"],
        generated_at=datetime(2026, 6, 16, 2, tzinfo=UTC),
    )
    pack = result["benchmark_baseline_control_pack"]

    assert pack["benchmark_baseline_status"] == "INSUFFICIENT_BASELINE_METRICS"
    assert "candidate_metrics:insufficient_metrics" in pack["blocking_reasons"]
    assert "baseline_metrics:insufficient_metrics" in pack["blocking_reasons"]
    assert pack["comparison_summary"]["insufficient_metric_baseline_count"] == 5
    assert result["benchmark_baseline_validation"]["status"] == "PASS"


def test_benchmark_baseline_cli_run_report_and_validate(tmp_path: Path) -> None:
    fixture = _baseline_fixture(tmp_path)
    output_dir = tmp_path / "benchmark_baseline_cli"

    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "benchmark-baseline-control",
            "run",
            "--as-of",
            "2026-06-15",
            "--candidate-metrics-path",
            str(fixture["candidate_metrics_path"]),
            "--baseline-metrics-path",
            str(_mixed_baseline_metrics_path(tmp_path)),
            "--weekly-review-id",
            fixture["weekly_id"],
            "--weekly-review-dir",
            str(fixture["weekly_dir"]),
            "--cost-sensitivity-review-id",
            fixture["cost_id"],
            "--cost-sensitivity-dir",
            str(fixture["cost_dir"]),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert run.exit_code == 0
    assert "benchmark_baseline_status=MIXED_BASELINE_RESULT" in run.output
    assert "outperformed_baseline_count=2" in run.output
    assert "validation_status=PASS" in run.output
    control_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("control_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "benchmark-baseline-control",
            "report",
            "--control-id",
            control_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "benchmark_baseline_status=MIXED_BASELINE_RESULT" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-benchmark-baseline-control",
            "--control-id",
            control_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def test_reader_brief_summarizes_benchmark_baseline_control(tmp_path: Path) -> None:
    fixture = _baseline_fixture(tmp_path)
    result = baseline.run_benchmark_baseline_control_pack(
        as_of=date(2026, 6, 15),
        candidate_metrics_path=fixture["candidate_metrics_path"],
        baseline_metrics_path=_baseline_metrics_path(tmp_path, "brief", 0.05),
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        cost_sensitivity_review_id=fixture["cost_id"],
        cost_sensitivity_dir=fixture["cost_dir"],
        output_dir=fixture["baseline_dir"],
        generated_at=datetime(2026, 6, 16, 2, tzinfo=UTC),
    )
    leaderboard_path = _write_minimal_leaderboard(tmp_path)
    manifest_path = result["control_dir"] / "benchmark_baseline_manifest.json"
    report_index = {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_parameter_sweep_leaderboard",
                "latest_artifact_path": str(leaderboard_path),
            },
            {
                "report_id": "etf_dynamic_v3_benchmark_baseline_control",
                "latest_artifact_path": str(manifest_path),
            },
        ]
    }

    summary = reader_brief._etf_dynamic_v3_parameter_research_summary(report_index)

    assert summary["benchmark_baseline_control_id"] == result["control_id"]
    assert summary["benchmark_baseline_status"] == "CANDIDATE_OUTPERFORMS_BASELINES"
    assert summary["benchmark_baseline_count"] == 5
    assert summary["benchmark_baseline_validation_status"] == "PASS"
    assert summary["benchmark_baseline_control"] == str(manifest_path)


def _baseline_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = {
        "weekly_id": "paper-shadow-weekly-baseline-test",
        "health_id": "paper-shadow-health-baseline-test",
        "weekly_dir": tmp_path / "paper_shadow_weekly_review",
        "health_dir": tmp_path / "paper_shadow_health",
        "cost_dir": tmp_path / "cost_sensitivity",
        "baseline_dir": tmp_path / "benchmark_baseline",
    }
    _write_weekly_artifact(fixture["weekly_dir"], fixture["weekly_id"])
    _write_health_artifact(fixture["health_dir"], fixture["health_id"])
    candidate_metrics_path = tmp_path / "candidate_metrics.json"
    _write_json(
        candidate_metrics_path,
        {
            "metrics_id": "candidate-baseline-metrics",
            "candidate": "median_plus_regime_mismatch_filter",
            "as_of": "2026-06-15",
            "turnover": 1.0,
            "gross_performance_proxy": 0.08,
            "net_performance_proxy": 0.08,
            "baseline_performance_proxy": 0.06,
            **baseline.BENCHMARK_BASELINE_SAFETY,
        },
    )
    cost_result = cost.run_cost_sensitivity_review(
        as_of=date(2026, 6, 15),
        candidate_metrics_path=candidate_metrics_path,
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_id=fixture["health_id"],
        paper_shadow_health_dir=fixture["health_dir"],
        output_dir=fixture["cost_dir"],
        generated_at=datetime(2026, 6, 16, 1, tzinfo=UTC),
    )
    fixture["cost_id"] = cost_result["review_id"]
    fixture["candidate_metrics_path"] = candidate_metrics_path
    return fixture


def _baseline_metrics_path(tmp_path: Path, suffix: str, value: float) -> Path:
    path = tmp_path / f"baseline_metrics_{suffix}.json"
    _write_json(
        path,
        {
            "metrics_id": f"baseline-metrics-{suffix}",
            "as_of": "2026-06-15",
            "baselines": [
                {"baseline_id": baseline_id, "net_performance_proxy": value}
                for baseline_id in baseline.REQUIRED_BASELINE_IDS
            ],
            **baseline.BENCHMARK_BASELINE_SAFETY,
        },
    )
    return path


def _mixed_baseline_metrics_path(tmp_path: Path) -> Path:
    path = tmp_path / "baseline_metrics_mixed.json"
    _write_json(
        path,
        {
            "metrics_id": "baseline-metrics-mixed",
            "as_of": "2026-06-15",
            "baselines": [
                {"baseline_id": "static_allocation", "net_performance_proxy": 0.078},
                {"baseline_id": "no_trade", "net_performance_proxy": 0.06},
                {"baseline_id": "qqq_only", "net_performance_proxy": 0.081},
                {"baseline_id": "spy_only", "net_performance_proxy": 0.03},
                {"baseline_id": "equal_weight_etf", "net_performance_proxy": 0.08},
            ],
            **baseline.BENCHMARK_BASELINE_SAFETY,
        },
    )
    return path


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
        **baseline.BENCHMARK_BASELINE_SAFETY,
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
        **baseline.BENCHMARK_BASELINE_SAFETY,
    }
    validation = {
        "schema_version": st.SCHEMA_VERSION,
        "artifact_id": weekly_id,
        "status": "PASS",
        "failed_check_count": 0,
        "checks": [],
        **baseline.BENCHMARK_BASELINE_SAFETY,
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
        "paper_shadow_health_manifest_path": str(
            artifact_dir / "paper_shadow_health_manifest.json"
        ),
        "paper_shadow_health_report_path": str(artifact_dir / "paper_shadow_health_report.json"),
        "paper_shadow_health_markdown_path": str(artifact_dir / "paper_shadow_health_report.md"),
        "reader_brief_section_path": str(artifact_dir / "reader_brief_section.md"),
        "validation_path": str(artifact_dir / "paper_shadow_health_validation.json"),
        **baseline.BENCHMARK_BASELINE_SAFETY,
    }
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_health_report",
        "health_id": health_id,
        "paper_shadow_health_status": "HEALTHY",
        "safe_to_continue_shadow": True,
        "signal_input_status": "OK",
        "blocking_reasons": [],
        "warnings": [],
        **baseline.BENCHMARK_BASELINE_SAFETY,
    }
    validation = {
        "schema_version": st.SCHEMA_VERSION,
        "artifact_id": health_id,
        "status": "PASS",
        "failed_check_count": 0,
        "checks": [],
        **baseline.BENCHMARK_BASELINE_SAFETY,
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
