from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.parameter_review import (
    PARAMETER_REVIEW_EVIDENCE_SCHEMA_VERSION,
    ParameterReviewError,
    build_parameter_review_aggregation,
    build_parameter_review_evidence_record,
    parameter_review_evidence_to_json,
    validate_parameter_review_evidence_record,
)


def test_parameter_review_schema_validates_complete_evidence_record() -> None:
    record = _evidence_record()

    assert record["schema_version"] == PARAMETER_REVIEW_EVIDENCE_SCHEMA_VERSION
    assert record["candidate_id"] == "candidate_ai_growth"
    assert record["safety"]["production_effect"] == "none"
    assert validate_parameter_review_evidence_record(record) == []
    assert json.loads(parameter_review_evidence_to_json(record))["candidate_id"] == (
        "candidate_ai_growth"
    )


def test_parameter_review_schema_missing_candidate_id_fails() -> None:
    record = _evidence_record()
    record["candidate_id"] = ""

    with pytest.raises(ParameterReviewError, match="candidate_id"):
        validate_parameter_review_evidence_record(record)


def test_parameter_review_schema_missing_safety_fields_fails() -> None:
    record = _evidence_record()
    del record["safety"]["manual_review_required"]

    with pytest.raises(ParameterReviewError, match="manual_review_required"):
        validate_parameter_review_evidence_record(record)


def test_parameter_review_schema_unsafe_production_effect_fails() -> None:
    record = _evidence_record()
    record["production_effect"] = "update_target_weights"

    with pytest.raises(ParameterReviewError, match="production_effect"):
        validate_parameter_review_evidence_record(record)


def test_parameter_review_schema_invalid_date_range_fails() -> None:
    record = _evidence_record()
    record["review_start_date"] = "2026-06-02"
    record["review_end_date"] = "2026-06-01"

    with pytest.raises(ParameterReviewError, match="review_start_date"):
        validate_parameter_review_evidence_record(record)


def test_parameter_review_metrics_allow_null_with_reason() -> None:
    record = _evidence_record()
    record["metrics"]["excess_return_vs_SPY"] = None
    record["metrics"]["metric_null_reasons"]["excess_return_vs_SPY"] = (
        "SPY benchmark unavailable in forward dashboard."
    )

    assert validate_parameter_review_evidence_record(record) == []

    del record["metrics"]["metric_null_reasons"]["excess_return_vs_SPY"]
    with pytest.raises(ParameterReviewError, match="excess_return_vs_SPY"):
        validate_parameter_review_evidence_record(record)


def test_parameter_review_evidence_source_links_are_required() -> None:
    record = _evidence_record()
    record["evidence_sources"] = []

    with pytest.raises(ParameterReviewError, match="evidence source link"):
        validate_parameter_review_evidence_record(record)


def test_parameter_review_aggregator_loads_forward_dashboard(tmp_path) -> None:
    context = _aggregation_context(tmp_path)

    payload = build_parameter_review_aggregation(
        as_of=date(2026, 6, 1),
        report_index_payload=context["report_index"],
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert payload["status"] == "available"
    assert payload["evidence_record_count"] == 1
    record = payload["evidence_records"][0]
    assert record["candidate_id"] == "unit_run:base_ai_growth"
    assert record["source_run_id"] == "unit_run"
    assert record["metrics"]["excess_return_vs_baseline"] == 0.02
    assert any(
        source["source_type"] == "forward_dashboard" for source in record["evidence_sources"]
    )


def test_parameter_review_aggregator_loads_weekly_review_and_decision_journal(tmp_path) -> None:
    context = _aggregation_context(tmp_path)

    payload = build_parameter_review_aggregation(
        as_of=date(2026, 6, 1),
        report_index_payload=context["report_index"],
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )
    record = payload["evidence_records"][0]

    assert record["weekly_review_links"][0]["review_id"] == "weekly-review-1"
    assert record["journal_links"][0]["decision_id"] == "decision-1"
    assert record["metrics"]["manual_review_count"] == 1
    assert record["metrics"]["accepted_review_count"] == 1


def test_parameter_review_aggregator_missing_optional_source_is_handled(tmp_path) -> None:
    context = _aggregation_context(tmp_path, include_watchlist=False)

    payload = build_parameter_review_aggregation(
        as_of=date(2026, 6, 1),
        report_index_payload=context["report_index"],
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert payload["status"] == "available"
    assert any(
        source["report_id"] == "etf_forward_watchlist" and source["status"] == "missing_data"
        for source in payload["source_reports"]
    )
    assert payload["evidence_records"][0]["production_effect"] == "none"


def test_parameter_review_aggregator_missing_required_source_returns_needs_more_data(
    tmp_path,
) -> None:
    context = _aggregation_context(tmp_path, include_forward_dashboard=False)

    payload = build_parameter_review_aggregation(
        as_of=date(2026, 6, 1),
        report_index_payload=context["report_index"],
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert payload["status"] == "needs_more_data"
    assert payload["reason"] == "INSUFFICIENT_FORWARD_EVIDENCE"
    assert payload["evidence_records"] == []
    assert payload["missing_required_sources"][0]["report_id"] == "etf_forward_dashboard"


def test_parameter_review_aggregator_preserves_source_paths_and_safety(tmp_path) -> None:
    context = _aggregation_context(tmp_path)

    payload = build_parameter_review_aggregation(
        as_of=date(2026, 6, 1),
        report_index_payload=context["report_index"],
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    paths = {
        source["report_id"]: source["source_report_path"]
        for source in payload["source_reports"]
    }
    assert paths["etf_forward_dashboard"].endswith("forward_dashboard_2026-06-01.json")
    assert payload["observe_only"] is True
    assert payload["candidate_only"] is True
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["manual_review_required"] is True


def test_parameter_review_cli_aggregate_writes_outputs(tmp_path) -> None:
    context = _aggregation_context(tmp_path)
    report_index_path = tmp_path / "report_index_2026-06-01.json"
    _write_json(report_index_path, context["report_index"])
    output_dir = tmp_path / "parameter_review"

    result = CliRunner().invoke(
        etf_app,
        [
            "parameter-review",
            "aggregate",
            "--as-of",
            "2026-06-01",
            "--report-index-path",
            str(report_index_path),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "parameter_review_evidence_2026-06-01.json").exists()
    assert (output_dir / "parameter_review_evidence_2026-06-01.md").exists()
    assert "production_effect=none" in result.output


def _evidence_record(
    *,
    review_start_date: date = date(2026, 5, 1),
    review_end_date: date = date(2026, 6, 1),
) -> dict[str, object]:
    return build_parameter_review_evidence_record(
        candidate_id="candidate_ai_growth",
        experiment_id="base_ai_growth",
        source_pack_id="etf_calibration_v1",
        source_run_id="run_20260601",
        baseline_config_hash="baseline_hash",
        candidate_config_hash="candidate_hash",
        review_start_date=review_start_date,
        review_end_date=review_end_date,
        forward_days=22,
        evidence_sources=[
            _source("forward_dashboard", "forward_dashboard_2026-06-01.json"),
            _source("weekly_review", "weekly_review_2026-06-01.json"),
            _source("decision_journal", "decision_journal_2026-06-01.json"),
            _source("experiment_report", "comparison_report.json"),
            _source("candidate_gate", "candidate_selection_report.json"),
            _source("validation_gate", "forward_validation_2026-06-01.json"),
        ],
        metrics={
            "return_since_enrollment": 0.04,
            "excess_return_vs_baseline": 0.02,
            "excess_return_vs_QQQ": 0.01,
            "excess_return_vs_SPY": 0.03,
            "max_drawdown_since_enrollment": -0.05,
            "drawdown_delta_vs_baseline": 0.01,
            "turnover_since_enrollment": 0.2,
            "turnover_delta_vs_baseline": 0.03,
            "constraint_hit_rate": 0.0,
            "regime_transition_count": 1,
            "weight_stability_score": 0.82,
            "data_coverage_ratio": 1.0,
            "manual_review_count": 2,
            "accepted_review_count": 1,
            "rejected_review_count": 0,
            "deferred_review_count": 1,
            "metric_null_reasons": {},
        },
        journal_links=[
            {
                "decision_id": "decision-1",
                "source_report_path": "decision_journal_2026-06-01.json",
            }
        ],
        weekly_review_links=[
            {
                "review_id": "weekly-review-1",
                "source_report_path": "weekly_review_2026-06-01.json",
            }
        ],
        validation_status={"status": "available", "gates": [{"status": "PASS"}]},
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
        review_id="review-1",
        parameter_review_id="parameter-review-1",
    )


def _source(source_type: str, path: str) -> dict[str, str]:
    return {
        "source_type": source_type,
        "source_module": source_type,
        "source_report_path": path,
        "source_metric": "fixture",
        "time_window": "2026-05-01/2026-06-01",
        "reason_code": "FIXTURE",
    }


def _aggregation_context(
    tmp_path,
    *,
    include_forward_dashboard: bool = True,
    include_watchlist: bool = False,
) -> dict[str, object]:
    records = []
    if include_forward_dashboard:
        forward_path = tmp_path / "forward_dashboard_2026-06-01.json"
        _write_json(forward_path, _forward_dashboard_payload())
        records.append(_report_index_record("etf_forward_dashboard", forward_path))
    weekly_path = tmp_path / "weekly_review_2026-06-01.json"
    journal_path = tmp_path / "decision_journal_2026-06-01.json"
    comparison_path = tmp_path / "comparison_report.json"
    selection_path = tmp_path / "candidate_selection_report.json"
    validation_path = tmp_path / "forward_validation_2026-06-01.json"
    _write_json(weekly_path, _weekly_review_payload())
    _write_json(journal_path, _decision_journal_report_payload())
    _write_json(comparison_path, _experiment_comparison_payload())
    _write_json(selection_path, _candidate_selection_payload())
    _write_json(validation_path, _validation_payload("etf_forward_validation"))
    records.extend(
        [
            _report_index_record("etf_weekly_review", weekly_path),
            _report_index_record("etf_decision_journal_report", journal_path),
            _report_index_record("etf_experiment_comparison", comparison_path),
            _report_index_record("etf_experiment_candidate_selection", selection_path),
            _report_index_record("etf_forward_validation", validation_path),
        ]
    )
    if include_watchlist:
        watchlist_path = tmp_path / "forward_watchlist_2026-06-01.json"
        _write_json(watchlist_path, {"report_type": "etf_forward_watchlist", "status": "PASS"})
        records.append(_report_index_record("etf_forward_watchlist", watchlist_path))
    return {"report_index": {"status": "PASS", "reports": records}}


def _report_index_record(report_id: str, path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "title": report_id,
        "latest_artifact_path": str(path),
        "artifact_status": "PASS",
        "freshness_status": "FRESH",
        "artifact_date": "2026-06-01",
    }


def _forward_dashboard_payload() -> dict[str, object]:
    return {
        "schema_version": "etf_forward_dashboard_v1",
        "report_type": "etf_forward_dashboard",
        "status": "AVAILABLE",
        "as_of": "2026-06-01",
        "baseline_config_hash": "baseline_hash",
        "candidate_summary_table": [
            {
                "shadow_id": "shadow-1",
                "candidate_id": "unit_run:base_ai_growth",
                "experiment_id": "base_ai_growth",
                "status": "active",
                "days_since_enrollment": 22,
                "return_since_enrollment": 0.04,
                "excess_return_vs_baseline": 0.02,
                "excess_return_vs_QQQ": 0.01,
                "excess_return_vs_SPY": 0.03,
                "excess_return_vs_SMH": -0.01,
                "max_drawdown_since_enrollment": -0.05,
                "turnover_since_enrollment": 0.2,
                "constraint_hits_since_enrollment": 0,
                "last_evaluated_date": "2026-06-01",
                "metric_null_reasons": {},
            }
        ],
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _weekly_review_payload() -> dict[str, object]:
    return {
        "schema_version": "etf_weekly_review_v1",
        "report_type": "etf_weekly_review",
        "review_id": "weekly-review-1",
        "sections": {
            "shadow_candidate_review": {
                "source_report_path": "weekly_review_2026-06-01.json",
                "active_shadow_candidates": [
                    {
                        "candidate_id": "unit_run:base_ai_growth",
                        "experiment_id": "base_ai_growth",
                        "recommended_observation_action": "continue_shadow",
                    }
                ],
            }
        },
        "manual_review_actions": [],
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _decision_journal_report_payload() -> dict[str, object]:
    return {
        "schema_version": "etf_portfolio_decision_journal_report_v1",
        "report_type": "etf_portfolio_decision_journal_report",
        "source_journal_path": "journal.json",
        "entries": [
            {
                "decision_id": "decision-1",
                "linked_candidate": "unit_run:base_ai_growth",
                "decision_status": "accept_recommendation",
                "confidence": 0.8,
                "source_weekly_review": "weekly_review_2026-06-01.json",
                "linked_report": "decision_journal_2026-06-01.json",
            }
        ],
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _experiment_comparison_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "etf_experiment_comparison",
        "run_metadata": {
            "run_id": "unit_run",
            "pack_id": "etf_calibration_v1",
            "config_hash": "baseline_hash",
        },
        "metrics_table": [{"experiment_id": "base_ai_growth", "total_return": 0.1}],
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _candidate_selection_payload() -> dict[str, object]:
    return {
        "schema_version": "etf_experiment_candidate_selection_v1",
        "report_type": "etf_experiment_candidate_selection",
        "run_metadata": {
            "run_id": "unit_run",
            "pack_id": "etf_calibration_v1",
            "config_hash": "baseline_hash",
        },
        "candidates": [
            {
                "candidate_id": "unit_run:base_ai_growth",
                "experiment_id": "base_ai_growth",
                "source_run_id": "unit_run",
                "config_hash": "candidate_hash",
                "selection_status": "eligible_for_shadow",
            }
        ],
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _validation_payload(report_type: str) -> dict[str, object]:
    return {
        "schema_version": f"{report_type}_v1",
        "report_type": report_type,
        "status": "PASS",
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _write_json(path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
