from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest
import yaml

from ai_trading_system.shadow.lineage import sha256_file
from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.portfolio_candidate_review import (
    decide_portfolio_candidate,
    run_portfolio_candidate_review,
)
from ai_trading_system.trading_engine.portfolio_candidate_tracking import (
    run_portfolio_candidate_tracking,
    write_portfolio_candidate_tracking_summary,
)
from ai_trading_system.trading_engine.portfolio_tracking_review import (
    load_portfolio_tracking_review_config,
    run_portfolio_tracking_review,
    validate_portfolio_tracking_review_payload,
)
from trading_engine.test_portfolio_candidate_review import _review_fixture
from trading_engine.test_portfolio_candidate_tracking import (
    _write_portfolio_candidate_tracking_config,
)


def test_default_portfolio_tracking_review_config_loads() -> None:
    config = load_portfolio_tracking_review_config()

    assert config["production_effect"] == "none"
    assert config["manual_review_required"] is True
    assert config["auto_promotion"] is False
    assert set(config["performance_review_recommendation"]) >= {
        "continue_tracking",
        "watch",
        "pause_tracking",
        "retire_candidate",
        "needs_more_data",
        "eligible_for_extended_review",
    }
    assert config["tracking_guardrails"]["min_tracking_days_for_positive_recommendation"] == 5
    assert config["safety"]["production_write_allowed"] is False


def test_no_active_candidate_blocks_tracking_review(tmp_path: Path) -> None:
    config_path = _write_portfolio_tracking_review_config(tmp_path)

    run = run_portfolio_tracking_review(
        as_of=_date(2026, 1, 20),
        config_path=config_path,
    )

    assert run.payload["metadata"]["status"] == "BLOCKED"
    assert run.payload["recommendation"]["status"] == "pause_tracking"
    assert run.payload["recommendation"]["reason"] == "no_active_shadow_candidate"
    assert validate_portfolio_tracking_review_payload(run.payload) == []


def test_active_candidate_with_one_day_needs_more_data(tmp_path: Path) -> None:
    fixture, _, tracking_review_config = _tracking_review_fixture(tmp_path)
    baseline_before = sha256_file(fixture["baseline_path"])

    run = run_portfolio_tracking_review(
        as_of=fixture["as_of"],
        config_path=tracking_review_config,
    )

    assert run.json_path.exists()
    assert run.markdown_path.exists()
    assert validate_portfolio_tracking_review_payload(run.payload) == []
    assert run.payload["metadata"]["status"] == "LIMITED"
    assert run.payload["candidate"]["tracking_status"] == "active_tracking"
    assert run.payload["candidate"]["tracking_days"] == 1
    assert run.payload["recommendation"]["status"] == "needs_more_data"
    assert run.payload["promotion_impact"]["can_support_candidate_promotion"] is False
    assert sha256_file(fixture["baseline_path"]) == baseline_before


def test_active_candidate_with_five_days_continues_tracking(tmp_path: Path) -> None:
    fixture, tracking_run, tracking_review_config = _tracking_review_fixture(tmp_path)
    _clone_tracking_days(tracking_run.payload, tracking_run.json_path.parent.parent, count=5)

    run = run_portfolio_tracking_review(
        as_of=fixture["as_of"] + timedelta(days=4),
        config_path=tracking_review_config,
        window="5d",
    )

    assert run.payload["metadata"]["status"] == "OK"
    assert run.payload["candidate"]["tracking_days"] == 5
    assert run.payload["tracking_window"]["window"] == "rolling_5d"
    assert run.payload["recommendation"]["status"] == "continue_tracking"
    assert run.payload["risk_guardrails"]["status"] == "PASS"


def test_active_candidate_with_twenty_positive_days_is_extended_review_eligible(
    tmp_path: Path,
) -> None:
    fixture, tracking_run, tracking_review_config = _tracking_review_fixture(tmp_path)
    _clone_tracking_days(
        tracking_run.payload,
        tracking_run.json_path.parent.parent,
        count=20,
        candidate_return_step=0.002,
        baseline_return_step=0.001,
        candidate_drawdown=-0.01,
        baseline_drawdown=-0.02,
        candidate_rebalance=7,
        baseline_rebalance=3,
        candidate_signal_score=0.9,
        baseline_signal_score=0.7,
    )

    run = run_portfolio_tracking_review(
        as_of=fixture["as_of"] + timedelta(days=19),
        config_path=tracking_review_config,
        window="since-start",
    )

    assert run.payload["metadata"]["status"] == "OK"
    assert run.payload["candidate"]["tracking_days"] == 20
    assert run.payload["signal_transmission_review"]["signal_to_weight_improved"] is True
    assert run.payload["performance_review"]["relative_performance"]["excess_return"] > 0
    assert run.payload["recommendation"]["status"] == "eligible_for_extended_review"
    assert run.payload["promotion_impact"]["can_support_candidate_promotion"] is False


def test_underperformance_with_drawdown_breach_retires_candidate(tmp_path: Path) -> None:
    fixture, tracking_run, tracking_review_config = _tracking_review_fixture(tmp_path)
    _clone_tracking_days(
        tracking_run.payload,
        tracking_run.json_path.parent.parent,
        count=5,
        candidate_return_step=-0.003,
        baseline_return_step=0.001,
        candidate_drawdown=-0.08,
        baseline_drawdown=-0.02,
    )

    run = run_portfolio_tracking_review(
        as_of=fixture["as_of"] + timedelta(days=4),
        config_path=tracking_review_config,
    )

    assert run.payload["metadata"]["status"] == "OK"
    assert "max_drawdown_worse_than_baseline_limit" in run.payload["risk_guardrails"]["warnings"]
    assert run.payload["recommendation"]["status"] == "retire_candidate"


def test_freshness_stale_blocks_tracking_review(tmp_path: Path) -> None:
    fixture, tracking_run, tracking_review_config = _tracking_review_fixture(tmp_path)
    payload = json.loads(tracking_run.json_path.read_text(encoding="utf-8"))
    payload["market_data_freshness"]["status"] = "STALE"
    tracking_run.json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    run = run_portfolio_tracking_review(
        as_of=fixture["as_of"],
        config_path=tracking_review_config,
    )

    assert run.payload["metadata"]["status"] == "BLOCKED"
    assert "freshness_status_stale" in run.payload["risk_guardrails"]["hard_rejections"]
    assert run.payload["recommendation"]["status"] == "pause_tracking"


def test_tracking_not_active_blocks_tracking_review(tmp_path: Path) -> None:
    fixture, tracking_run, tracking_review_config = _tracking_review_fixture(tmp_path)
    payload = json.loads(tracking_run.json_path.read_text(encoding="utf-8"))
    payload["candidate"]["tracking_status"] = "degraded_tracking"
    tracking_run.json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    run = run_portfolio_tracking_review(
        as_of=fixture["as_of"],
        config_path=tracking_review_config,
    )

    assert run.payload["metadata"]["status"] == "BLOCKED"
    assert "tracking_status_not_active" in run.payload["risk_guardrails"]["hard_rejections"]
    assert run.payload["recommendation"]["status"] == "pause_tracking"


def test_shadow_backtest_references_tracking_review_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _, tracking_review_config = _tracking_review_fixture(tmp_path)
    review_run = run_portfolio_tracking_review(
        as_of=fixture["as_of"],
        config_path=tracking_review_config,
    )
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)

    shadow_run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        dry_run=True,
    )

    decision = shadow_run.payload["promotion_decision"]
    assert decision["status"] == "rejected"
    assert decision["portfolio_tracking_review_recommendation"] == "needs_more_data"
    assert decision["supporting_artifacts"]["portfolio_tracking_review"] == str(
        review_run.json_path
    )


def _tracking_review_fixture(tmp_path: Path) -> tuple[dict[str, object], object, Path]:
    fixture, _, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reviewer="manual",
        reason="Continue shadow tracking.",
        config_path=review_config,
    )
    tracking_run = run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )
    tracking_review_config = _write_portfolio_tracking_review_config(
        tmp_path,
        production_path=fixture["baseline_path"],
    )
    return fixture, tracking_run, tracking_review_config


def _clone_tracking_days(
    payload: dict[str, object],
    tracking_root: Path,
    *,
    count: int,
    candidate_return_step: float = 0.0,
    baseline_return_step: float = 0.0,
    candidate_drawdown: float | None = None,
    baseline_drawdown: float | None = None,
    candidate_rebalance: int | None = None,
    baseline_rebalance: int | None = None,
    candidate_signal_score: float | None = None,
    baseline_signal_score: float | None = None,
) -> None:
    base_date = _date_from_payload(payload)
    for index in range(count):
        item = json.loads(json.dumps(payload))
        item["metadata"]["run_id"] = (
            "portfolio-candidate-tracking-" f"{(base_date + timedelta(days=index)).isoformat()}"
        )
        item["metadata"]["status"] = "OK"
        item["candidate"]["tracking_status"] = "active_tracking"
        item["date_resolution"]["tracking_date"] = (base_date + timedelta(days=index)).isoformat()
        item["date_resolution"]["effective_data_date"] = (
            base_date + timedelta(days=index)
        ).isoformat()
        item["date_resolution"]["latest_market_data_date"] = (
            base_date + timedelta(days=index)
        ).isoformat()
        raw = item["tracking_metrics"]["raw_observed_metrics"]
        baseline = raw["baseline"]
        candidate = raw["candidate"]
        baseline["cumulative_return"] = round(index * baseline_return_step, 6)
        candidate["cumulative_return"] = round(index * candidate_return_step, 6)
        if baseline_drawdown is not None:
            baseline["drawdown"] = baseline_drawdown
            baseline["max_drawdown"] = baseline_drawdown
        if candidate_drawdown is not None:
            candidate["drawdown"] = candidate_drawdown
            candidate["max_drawdown"] = candidate_drawdown
        if baseline_rebalance is not None:
            baseline["rebalance_count"] = baseline_rebalance
        if candidate_rebalance is not None:
            candidate["rebalance_count"] = candidate_rebalance
        if baseline_signal_score is not None:
            baseline["signal_transmission_score"] = baseline_signal_score
        if candidate_signal_score is not None:
            candidate["signal_transmission_score"] = candidate_signal_score
        item["tracking_metrics"]["baseline"]["daily_return"] = baseline_return_step
        item["tracking_metrics"]["candidate"]["daily_return"] = candidate_return_step
        output_dir = tracking_root / (base_date + timedelta(days=index)).isoformat()
        write_portfolio_candidate_tracking_summary(
            item,
            output_dir / "portfolio_candidate_tracking_summary.json",
            output_dir / "portfolio_candidate_tracking_summary.md",
        )


def _date_from_payload(payload: dict[str, object]):
    return _date_from_text(payload["date_resolution"]["tracking_date"])  # type: ignore[index]


def _date_from_text(value: object):
    from datetime import date

    return date.fromisoformat(str(value))


def _date(year: int, month: int, day: int):
    from datetime import date

    return date(year, month, day)


def _write_portfolio_tracking_review_config(
    tmp_path: Path,
    production_path: object | None = None,
) -> Path:
    config_path = tmp_path / "config" / "portfolio" / "portfolio_tracking_review.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "version": "portfolio-tracking-review-test",
                "owner": "tests",
                "status": "pilot",
                "production_effect": "none",
                "manual_review_required": True,
                "auto_promotion": False,
                "observe_only": True,
                "rationale": "test tracking review",
                "intended_effect": "test tracking review",
                "validation_evidence": "unit tests",
                "review_condition": "test review",
                "input": {
                    "portfolio_candidate_tracking_dir": str(
                        tmp_path / "artifacts" / "portfolio_candidate_tracking"
                    ),
                    "active_shadow_candidates_path": str(
                        tmp_path
                        / "artifacts"
                        / "portfolio_candidate_tracking"
                        / "state"
                        / "active_shadow_candidates.json"
                    ),
                    "portfolio_candidate_reviews_dir": str(
                        tmp_path / "artifacts" / "portfolio_candidate_reviews"
                    ),
                    "portfolio_candidates_dir": str(
                        tmp_path / "artifacts" / "portfolio_candidates"
                    ),
                    "market_data_freshness_dir": str(tmp_path / "artifacts" / "data_freshness"),
                    "market_data_refresh_dir": str(tmp_path / "artifacts" / "data_refresh"),
                    "backtest_snapshot_dir": str(tmp_path / "artifacts" / "backtest_snapshots"),
                    "production_parameters_path": str(
                        production_path
                        or tmp_path / "config" / "parameters" / "production" / "current.yaml"
                    ),
                },
                "output": {
                    "portfolio_tracking_reviews_dir": str(
                        tmp_path / "artifacts" / "portfolio_tracking_reviews"
                    ),
                    "report_alias_dir": str(tmp_path / "outputs" / "reports"),
                    "dry_run_dir": str(
                        tmp_path / "outputs" / "dry_runs" / "portfolio_tracking_reviews"
                    ),
                },
                "review_windows": [
                    "latest_day",
                    "rolling_5d",
                    "rolling_20d",
                    "since_tracking_start",
                ],
                "default_window": "since_tracking_start",
                "performance_review_recommendation": [
                    "continue_tracking",
                    "watch",
                    "pause_tracking",
                    "retire_candidate",
                    "needs_more_data",
                    "eligible_for_extended_review",
                ],
                "tracking_guardrails": {
                    "max_drawdown_worse_than_baseline_limit": 0.02,
                    "turnover_relative_increase_limit": 0.30,
                    "cost_drag_relative_increase_limit": 0.20,
                    "min_tracking_days_for_positive_recommendation": 5,
                    "min_tracking_days_for_extended_review": 20,
                },
                "hard_rejection": [
                    "data_gate_not_ok",
                    "freshness_status_stale",
                    "tracking_status_not_active",
                    "production_effect_not_none",
                    "auto_promotion_true",
                    "production_config_modified",
                ],
                "safety": {
                    "production_write_allowed": False,
                    "candidate_promotion_enabled": False,
                    "candidate_production_promotion_allowed": False,
                    "data_quality_gate_lowered": False,
                },
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return config_path
