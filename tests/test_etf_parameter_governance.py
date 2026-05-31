from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.etf_portfolio.governance import (
    GOVERNANCE_SUMMARY_SCHEMA_KEYS,
    evaluate_parameter_candidate,
    load_parameter_governance_policy,
    write_parameter_governance_summary,
)
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle


def test_etf_parameter_governance_blocks_missing_benchmark_comparison() -> None:
    summary = _summary({**_valid_candidate(), "benchmark_comparison": {}})

    assert summary["promotion_status"] == "BLOCKED"
    assert "benchmark_comparison_missing" in summary["promotion_blockers"]


def test_etf_parameter_governance_blocks_small_sample_size() -> None:
    candidate = _valid_candidate()
    candidate["sample_period"]["trading_days"] = 20

    summary = _summary(candidate)

    assert summary["promotion_status"] == "BLOCKED"
    assert "sample_size_too_small" in summary["promotion_blockers"]


def test_etf_parameter_governance_blocks_high_turnover() -> None:
    candidate = _valid_candidate()
    candidate["turnover_comparison"]["candidate_avg_turnover"] = 0.45

    summary = _summary(candidate)

    assert summary["promotion_status"] == "BLOCKED"
    assert "turnover_too_high" in summary["promotion_blockers"]


def test_etf_parameter_governance_blocks_no_lookahead_failure() -> None:
    candidate = _valid_candidate()
    candidate["no_lookahead_validation"] = {"status": "FAIL"}

    summary = _summary(candidate)

    assert summary["promotion_status"] == "BLOCKED"
    assert "no_lookahead_not_passed" in summary["promotion_blockers"]


def test_etf_parameter_governance_blocks_p2_live_self_promotion() -> None:
    candidate = _valid_candidate()
    candidate["candidate_source"] = "p2_weight_optimizer"
    candidate["requested_production_effect"] = "production_weights"

    summary = _summary(candidate)

    assert summary["promotion_status"] == "BLOCKED"
    assert "p2_live_self_promotion_blocked" in summary["promotion_blockers"]
    assert "production_effect_requested" in summary["promotion_blockers"]
    assert summary["production_effect"] == "none"


def test_etf_parameter_governance_allows_only_manual_review_when_all_gates_pass() -> None:
    summary = _summary(_valid_candidate())

    assert summary["promotion_status"] == "ELIGIBLE_FOR_MANUAL_REVIEW"
    assert summary["promotion_blockers"] == []
    assert summary["manual_review_required"] is True
    assert summary["production_effect"] == "none"
    assert summary["current_model_state"] == "production_baseline"
    assert summary["candidate_model_state"] == "shadow"


def test_etf_parameter_governance_summary_schema_is_stable(tmp_path: Path) -> None:
    summary = _summary(_valid_candidate())
    json_path = tmp_path / "parameter_governance.json"
    md_path = tmp_path / "parameter_governance.md"

    write_parameter_governance_summary(summary, json_path=json_path, markdown_path=md_path)

    assert tuple(summary) == GOVERNANCE_SUMMARY_SCHEMA_KEYS
    assert json.loads(json_path.read_text(encoding="utf-8"))["report_type"] == (
        "etf_parameter_governance"
    )
    assert "Promotion Status: ELIGIBLE_FOR_MANUAL_REVIEW" in md_path.read_text(
        encoding="utf-8"
    )


def _summary(candidate: dict[str, object]) -> dict[str, object]:
    config = load_etf_config_bundle()
    policy = load_parameter_governance_policy()
    return evaluate_parameter_candidate(
        config=config,
        policy=policy,
        candidate=candidate,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )


def _valid_candidate() -> dict[str, object]:
    return {
        "candidate_model_version": "0.2.0-candidate",
        "candidate_model_state": "shadow",
        "candidate_source": "p1_experiment",
        "tests_passed": True,
        "shadow_mode": True,
        "manual_review_required": True,
        "production_effect": "none",
        "sample_period": {
            "start_date": "2023-01-03",
            "end_date": "2026-05-29",
            "trading_days": 760,
        },
        "benchmark_comparison": {
            "primary_benchmark_id": "B001",
            "strategy_total_return": 0.42,
            "benchmark_total_return": 0.31,
            "excess_return": 0.11,
        },
        "turnover_comparison": {
            "candidate_avg_turnover": 0.12,
            "baseline_avg_turnover": 0.10,
            "threshold": 0.30,
        },
        "drawdown_comparison": {
            "candidate_max_drawdown": -0.10,
            "baseline_max_drawdown": -0.13,
        },
        "risk_comparison": {"risk_increase_only": False},
        "no_lookahead_validation": {"status": "PASS"},
    }
