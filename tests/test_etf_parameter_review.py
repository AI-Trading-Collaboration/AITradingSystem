from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

from ai_trading_system.etf_portfolio.parameter_review import (
    PARAMETER_REVIEW_EVIDENCE_SCHEMA_VERSION,
    ParameterReviewError,
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
