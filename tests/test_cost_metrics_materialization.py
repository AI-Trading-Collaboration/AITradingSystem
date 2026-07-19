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
    dynamic_v3_cost_metrics_materialization as materialization,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st


def test_cost_metrics_materialization_simulation_stays_insufficient(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_sim_outcome(monkeypatch)
    fixture = _source_fixture(tmp_path)

    result = materialization.run_cost_metrics_materialization(
        as_of=date(2026, 6, 17),
        source_variant="limited_adjustment",
        sim_outcome_id="sim-outcome-test",
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_id=fixture["health_id"],
        paper_shadow_health_dir=fixture["health_dir"],
        cost_sensitivity_output_dir=fixture["cost_dir"],
        output_dir=fixture["materialization_dir"],
        generated_at=datetime(2026, 6, 17, 1, tzinfo=UTC),
    )
    report = result["cost_metrics_materialization_report"]

    assert report["cost_metrics_materialization_status"] == "INSUFFICIENT_COST_INPUTS"
    assert report["source_variant"] == "limited_adjustment"
    assert report["materialized_metrics"]["turnover"] is None
    assert report["materialized_metrics"]["gross_performance_proxy"] is None
    assert report["materialized_metrics"]["gross_improvement_proxy"] is None
    assert report["materialized_metrics"]["drawdown_proxy"] is None
    assert report["materialized_metrics"]["trade_rotation_count"] is None
    assert report["materialized_metrics"]["evidence_status"] == "INSUFFICIENT_DATA"
    assert report["candidate_to_source_mapping"]["mapping_accepted"] is False
    assert report["cost_sensitivity_status"] == "INSUFFICIENT_COST_INPUTS"
    assert report["net_performance_proxy_by_scenario"]["high"] is None
    assert result["cost_metrics_materialization_validation"]["status"] == "PASS"
    assert result["input_snapshot"]["schema_version"] == (
        materialization.COST_METRICS_INPUT_SCHEMA
    )
    assert "cost_metrics_materialization_status" in result["reader_brief_section"]
    assert Path(report["candidate_metrics_path"]).exists()
    assert_research_safe(report)


def test_cost_metrics_materialization_insufficient_when_variant_missing(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_sim_outcome(monkeypatch, include_candidate=False)
    fixture = _source_fixture(tmp_path)

    result = materialization.run_cost_metrics_materialization(
        as_of=date(2026, 6, 17),
        source_variant="limited_adjustment",
        sim_outcome_id="sim-outcome-test",
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_id=fixture["health_id"],
        paper_shadow_health_dir=fixture["health_dir"],
        cost_sensitivity_output_dir=fixture["cost_dir"],
        output_dir=fixture["materialization_dir"],
        generated_at=datetime(2026, 6, 17, 2, tzinfo=UTC),
    )
    report = result["cost_metrics_materialization_report"]

    assert report["cost_metrics_materialization_status"] == "INSUFFICIENT_COST_INPUTS"
    assert "turnover:missing" in report["blocking_reasons"]
    assert "source_variant_row_missing" in report["warnings"]
    assert report["cost_sensitivity_status"] == "INSUFFICIENT_COST_INPUTS"
    assert result["cost_metrics_materialization_validation"]["status"] == "PASS"


def test_cost_metrics_materialization_cli_run_report_and_validate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_sim_outcome(monkeypatch)
    fixture = _source_fixture(tmp_path)

    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "cost-sensitivity-metrics-materialization",
            "run",
            "--as-of",
            "2026-06-17",
            "--weekly-review-id",
            fixture["weekly_id"],
            "--weekly-review-dir",
            str(fixture["weekly_dir"]),
            "--paper-shadow-health-id",
            fixture["health_id"],
            "--paper-shadow-health-dir",
            str(fixture["health_dir"]),
            "--cost-sensitivity-output-dir",
            str(fixture["cost_dir"]),
            "--output-dir",
            str(fixture["materialization_dir"]),
        ],
    )
    assert run.exit_code == 0
    assert "cost_metrics_materialization_status=INSUFFICIENT_COST_INPUTS" in run.output
    assert "cost_sensitivity_status=INSUFFICIENT_COST_INPUTS" in run.output
    materialization_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("materialization_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "cost-sensitivity-metrics-materialization",
            "report",
            "--materialization-id",
            materialization_id,
            "--output-dir",
            str(fixture["materialization_dir"]),
        ],
    )
    assert report.exit_code == 0
    assert "turnover=None" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-cost-sensitivity-metrics-materialization",
            "--materialization-id",
            materialization_id,
            "--output-dir",
            str(fixture["materialization_dir"]),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def test_cost_metrics_materialization_validation_fails_closed_on_live_and_view_tamper(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_sim_outcome(monkeypatch)
    fixture = _source_fixture(tmp_path)
    result = materialization.run_cost_metrics_materialization(
        as_of=date(2026, 6, 17),
        source_variant="limited_adjustment",
        sim_outcome_id="sim-outcome-test",
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        paper_shadow_health_id=fixture["health_id"],
        paper_shadow_health_dir=fixture["health_dir"],
        cost_sensitivity_output_dir=fixture["cost_dir"],
        output_dir=fixture["materialization_dir"],
        generated_at=datetime(2026, 6, 17, 3, tzinfo=UTC),
    )
    materialization_id = result["materialization_id"]
    _patch_sim_outcome(monkeypatch, include_candidate=False)
    assert materialization.validate_cost_metrics_materialization_artifact(
        materialization_id=materialization_id,
        output_dir=fixture["materialization_dir"],
        write_output=False,
    )["status"] == "FAIL"
    report_path = (
        fixture["materialization_dir"]
        / materialization_id
        / "cost_metrics_materialization_report.md"
    )
    report_path.write_text("tampered\n", encoding="utf-8")
    assert materialization.validate_cost_metrics_materialization_artifact(
        materialization_id=materialization_id,
        output_dir=fixture["materialization_dir"],
        write_output=False,
    )["status"] == "FAIL"


def _patch_sim_outcome(monkeypatch, *, include_candidate: bool = True) -> None:
    def fake_payload(**kwargs):
        rows: list[dict[str, Any]] = [
            {
                "variant": "no_trade",
                "avg_5d_return": 0.005236,
                "avg_relative_to_no_trade_5d": 0.0,
                "avg_turnover": 0.0,
                "avg_max_drawdown_20d": -0.036261,
                "event_count": 185,
                "available_count": 730,
                "win_rate_vs_no_trade_5d": 0.0,
            }
        ]
        if include_candidate:
            rows.insert(
                0,
                {
                    "variant": "limited_adjustment",
                    "avg_5d_return": 0.00638,
                    "avg_relative_to_no_trade_5d": 0.001144,
                    "avg_turnover": 0.005945,
                    "avg_max_drawdown_20d": -0.042935,
                    "event_count": 185,
                    "available_count": 730,
                    "win_rate_vs_no_trade_5d": 0.597826,
                },
            )
        return {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_backtest_sim_outcome_manifest",
            "sim_outcome_id": "sim-outcome-test",
            "sim_outcome_manifest_path": "/tmp/sim_outcome_manifest.json",
            "simulated_variant_summary": {
                "schema_version": st.SCHEMA_VERSION,
                "report_type": "etf_dynamic_v3_backtest_sim_variant_summary",
                "outcome_mode": "BACKTEST_SIMULATION",
                "summary": rows,
                **st.SYSTEM_TARGET_SAFETY,
            },
            "production_effect": "none",
            **st.SYSTEM_TARGET_SAFETY,
        }

    monkeypatch.setattr(
        materialization.sim,
        "backtest_sim_outcome_report_payload",
        fake_payload,
    )


def _source_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = {
        "weekly_id": "paper-shadow-weekly-cost-materialization-test",
        "health_id": "paper-shadow-health-cost-materialization-test",
        "weekly_dir": tmp_path / "paper_shadow_weekly_review",
        "health_dir": tmp_path / "paper_shadow_health",
        "cost_dir": tmp_path / "cost_sensitivity",
        "materialization_dir": tmp_path / "cost_metrics_materialization",
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
        **st.SYSTEM_TARGET_SAFETY,
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
        **st.SYSTEM_TARGET_SAFETY,
    }
    validation = _validation(weekly_id)
    _write_json(artifact_dir / "paper_shadow_weekly_manifest.json", manifest)
    _write_json(artifact_dir / "paper_shadow_weekly_review.json", review)
    _write_json(artifact_dir / "paper_shadow_weekly_validation.json", validation)
    (artifact_dir / "paper_shadow_weekly_report.md").write_text("# weekly\n", encoding="utf-8")
    (artifact_dir / "reader_brief_section.md").write_text(
        "- paper_shadow_weekly_review_id: test\n",
        encoding="utf-8",
    )
    _write_json(root / "latest_paper_shadow_weekly_review.json", manifest)


def _write_health_artifact(root: Path, health_id: str) -> None:
    artifact_dir = root / health_id
    artifact_dir.mkdir(parents=True)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_health_manifest",
        "health_id": health_id,
        "paper_shadow_health_manifest_path": str(
            artifact_dir / "paper_shadow_health_manifest.json"
        ),
        "paper_shadow_health_report_path": str(artifact_dir / "paper_shadow_health_report.json"),
        "paper_shadow_health_markdown_path": str(artifact_dir / "paper_shadow_health_report.md"),
        "reader_brief_section_path": str(artifact_dir / "reader_brief_section.md"),
        "validation_path": str(artifact_dir / "paper_shadow_health_validation.json"),
        **st.SYSTEM_TARGET_SAFETY,
    }
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_health_report",
        "health_id": health_id,
        "paper_shadow_health_status": "HEALTHY",
        "safe_to_continue_shadow": True,
        "signal_input_status": "OK",
        "production_effect": "none",
        **st.SYSTEM_TARGET_SAFETY,
    }
    validation = _validation(health_id)
    _write_json(artifact_dir / "paper_shadow_health_manifest.json", manifest)
    _write_json(artifact_dir / "paper_shadow_health_report.json", report)
    _write_json(artifact_dir / "paper_shadow_health_validation.json", validation)
    (artifact_dir / "paper_shadow_health_report.md").write_text("# health\n", encoding="utf-8")
    (artifact_dir / "reader_brief_section.md").write_text(
        "- paper_shadow_health_id: test\n",
        encoding="utf-8",
    )
    _write_json(root / "latest_paper_shadow_health.json", manifest)


def _validation(artifact_id: str) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "artifact_id": artifact_id,
        "status": "PASS",
        "failed_check_count": 0,
        "checks": [],
        **st.SYSTEM_TARGET_SAFETY,
    }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
