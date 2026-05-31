from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.parameters.weight_tuning_failure import (
    build_weight_tuning_failure_payload,
    write_weight_tuning_failure_summary,
)
from ai_trading_system.trading_engine.portfolio_turnover_attribution import (
    build_portfolio_turnover_attribution_payload,
    run_portfolio_turnover_attribution,
    validate_portfolio_turnover_attribution_payload,
    write_portfolio_turnover_attribution_report,
)
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture
from trading_engine.test_weight_tuning_failure_attribution import (
    _candidate,
    write_weight_tuning_failure_fixture,
)


def test_missing_weight_tuning_failure_summary_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = date(2026, 5, 28)
    summary_path, _ = write_weight_tuning_failure_fixture(
        tmp_path,
        as_of=as_of,
        candidates=[_turnover_candidate("wt-0001")],
    )
    monkeypatch.setattr(
        "ai_trading_system.trading_engine.portfolio_turnover_attribution."
        "latest_weight_tuning_failure_path_on_or_before",
        lambda *_args, **_kwargs: None,
    )

    run = run_portfolio_turnover_attribution(
        weight_tuning_path=summary_path,
        output_root=tmp_path / "artifacts" / "portfolio_turnover_attribution",
    )

    assert run.payload["metadata"]["status"] == "BLOCKED"
    assert run.payload["metadata"]["reason"] == "missing_weight_tuning_failure_summary"
    assert run.payload["root_cause"]["category"] == "insufficient_details"
    assert validate_portfolio_turnover_attribution_payload(run.payload) == []


def test_missing_candidate_turnover_details_is_limited(tmp_path: Path) -> None:
    as_of = date(2026, 5, 28)
    weak_candidate = _candidate(
        "wt-0001",
        ["turnover_guardrail_failed"],
        {},
    )
    summary_path, candidates_path = write_weight_tuning_failure_fixture(
        tmp_path,
        as_of=as_of,
        candidates=[weak_candidate],
    )
    failure_path = _write_failure_artifact(summary_path, candidates_path, tmp_path)

    payload = _build_payload(summary_path, candidates_path, failure_path, tmp_path)

    assert payload["metadata"]["status"] == "LIMITED"
    assert payload["metadata"]["reason"] == "insufficient_candidate_turnover_details"
    assert payload["diagnostic_quality"]["candidate_level_turnover_details_available"] is False
    assert validate_portfolio_turnover_attribution_payload(payload) == []


def test_turnover_attribution_extracts_candidate_asset_window_and_near_miss(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 28)
    summary_path, candidates_path = write_weight_tuning_failure_fixture(
        tmp_path,
        as_of=as_of,
        candidates=[
            _turnover_candidate(
                "wt-0001",
                asset_rows=[
                    _asset("NVDA", 0.18, 0.35, 14, 0.026),
                    _asset("SMH", 0.12, 0.23, 11, 0.021),
                ],
                rebalance={
                    "rebalance_days": 28,
                    "baseline_rebalance_days": 14,
                    "extra_rebalance_days": 14,
                    "avg_assets_changed_per_rebalance": 4.2,
                    "max_assets_changed_per_rebalance": 7,
                    "small_trade_ratio": 0.46,
                },
            )
        ],
    )
    failure_path = _write_failure_artifact(summary_path, candidates_path, tmp_path)

    payload = _build_payload(summary_path, candidates_path, failure_path, tmp_path)

    assert payload["metadata"]["status"] == "TURNOVER_FAILURE_EXPLAINED"
    assert payload["candidate_turnover_summary"]["total_failed_by_turnover"] == 1
    assert payload["candidate_turnover_attribution"][0]["candidate_id"] == "wt-0001"
    assert payload["asset_turnover_contribution"][0]["symbol"] == "NVDA"
    assert payload["walk_forward_turnover"][0]["window_id"] == "wf-1"
    assert payload["rebalance_attribution"]["extra_rebalance_days"] == 14
    assert payload["near_miss_turnover_analysis"][0]["candidate_id"] == "wt-0001"
    assert payload["promotion_impact"]["can_support_candidate_promotion"] is False
    assert payload["safety"]["production_config_modified"] is False


def test_turnover_root_cause_classification(tmp_path: Path) -> None:
    cases = [
        (
            _turnover_candidate(
                "wt-rebalance",
                l1_distance=0.10,
                rebalance={
                    "rebalance_days": 10,
                    "baseline_rebalance_days": 4,
                    "extra_rebalance_days": 6,
                    "small_trade_ratio": 0.70,
                },
            ),
            "rebalance_threshold_too_low",
        ),
        (
            _turnover_candidate(
                "wt-score",
                l1_distance=0.10,
                score_volatility={
                    "mean_abs_score_change": 0.04,
                    "score_change_asset_day_ratio": 0.75,
                },
                rebalance={
                    "rebalance_days": 10,
                    "baseline_rebalance_days": 4,
                    "extra_rebalance_days": 6,
                    "small_trade_ratio": 0.10,
                },
            ),
            "score_volatility_too_high",
        ),
        (
            _turnover_candidate("wt-l1", l1_distance=0.35),
            "weight_search_too_aggressive",
        ),
        (
            _turnover_candidate(
                "wt-asset",
                l1_distance=0.10,
                asset_rows=[_asset("NVDA", 0.50, 0.60, 8, 0.02)],
            ),
            "asset_rotation_too_frequent",
        ),
        (
            _turnover_candidate(
                "wt-cost",
                l1_distance=0.10,
                relative_overrides={
                    "turnover_delta": 0.005,
                    "turnover_relative_increase": 0.32,
                    "cost_drag_delta": 0.004,
                },
            ),
            "cost_model_too_punitive",
        ),
    ]
    as_of = date(2026, 5, 28)
    for candidate, expected in cases:
        case_root = tmp_path / expected
        summary_path, candidates_path = write_weight_tuning_failure_fixture(
            case_root,
            as_of=as_of,
            candidates=[candidate],
        )
        failure_path = _write_failure_artifact(summary_path, candidates_path, case_root)

        payload = _build_payload(summary_path, candidates_path, failure_path, case_root)

        assert payload["root_cause"]["category"] == expected
        assert validate_portfolio_turnover_attribution_payload(payload) == []


def test_shadow_backtest_references_portfolio_turnover_attribution_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    as_of = fixture["as_of"]
    assert isinstance(as_of, date)
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)
    artifact_path = write_portfolio_turnover_attribution_artifact(tmp_path, as_of=as_of)

    run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=as_of,
        config_path=fixture["config_path"],
        dry_run=True,
    )

    decision = run.payload["promotion_decision"]
    assert decision["supporting_artifacts"]["portfolio_turnover_attribution"] == str(
        artifact_path
    )
    assert decision["status"] == "rejected"
    assert run.payload["metadata"]["production_effect"] == "none"
    assert "portfolio_turnover_attribution_root_cause" in decision


def write_portfolio_turnover_attribution_artifact(
    tmp_path: Path,
    *,
    as_of: date,
    root_cause: str | None = None,
) -> Path:
    summary_path, candidates_path = write_weight_tuning_failure_fixture(
        tmp_path,
        as_of=as_of,
        candidates=[_turnover_candidate("wt-0001", l1_distance=0.35)],
    )
    failure_path = _write_failure_artifact(summary_path, candidates_path, tmp_path)
    payload = _build_payload(summary_path, candidates_path, failure_path, tmp_path)
    if root_cause is not None:
        payload["root_cause"]["category"] = root_cause
    artifact_dir = tmp_path / "artifacts" / "portfolio_turnover_attribution" / as_of.isoformat()
    json_path = artifact_dir / "portfolio_turnover_attribution_summary.json"
    markdown_path = artifact_dir / "portfolio_turnover_attribution_summary.md"
    write_portfolio_turnover_attribution_report(payload, json_path, markdown_path)
    return json_path


def _build_payload(
    summary_path: Path,
    candidates_path: Path,
    failure_path: Path,
    tmp_path: Path,
) -> dict[str, Any]:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.setdefault("inputs", {})["signal_snapshot"] = str(
        tmp_path / "missing_signal_snapshot.json"
    )
    return build_portfolio_turnover_attribution_payload(
        summary,
        json.loads(candidates_path.read_text(encoding="utf-8")),
        json.loads(failure_path.read_text(encoding="utf-8")),
        weight_tuning_path=summary_path,
        candidates_path=candidates_path,
        failure_path=failure_path,
        output_root=tmp_path / "artifacts" / "portfolio_turnover_attribution",
        generated_at=datetime(2026, 5, 31, tzinfo=UTC),
    )


def _write_failure_artifact(
    summary_path: Path,
    candidates_path: Path,
    tmp_path: Path,
) -> Path:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    candidates = json.loads(candidates_path.read_text(encoding="utf-8"))
    payload = build_weight_tuning_failure_payload(
        summary,
        candidates,
        summary_path=summary_path,
        candidates_path=candidates_path,
        output_root=tmp_path / "artifacts" / "weight_tuning_failure",
        generated_at=datetime(2026, 5, 31, tzinfo=UTC),
    )
    as_of = date.fromisoformat(str(summary.get("as_of")))
    artifact_dir = tmp_path / "artifacts" / "weight_tuning_failure" / as_of.isoformat()
    json_path = artifact_dir / "weight_tuning_failure_summary.json"
    markdown_path = artifact_dir / "weight_tuning_failure_summary.md"
    write_weight_tuning_failure_summary(payload, json_path, markdown_path)
    return json_path


def _turnover_candidate(
    candidate_id: str,
    *,
    l1_distance: float = 0.12,
    asset_rows: list[dict[str, Any]] | None = None,
    rebalance: dict[str, Any] | None = None,
    score_volatility: dict[str, Any] | None = None,
    relative_overrides: dict[str, float] | None = None,
) -> dict[str, Any]:
    relative = {
        "baseline_turnover": 0.31,
        "candidate_turnover": 0.52,
        "turnover_delta": 0.21,
        "turnover_relative_increase": 0.68,
        "cost_drag_delta": 0.006,
        "annualized_return_delta": 0.02,
        "sharpe_ratio_delta": 0.08,
        "max_drawdown_delta": 0.0,
        "non_worse_walk_forward_ratio": 1.0,
    }
    relative.update(relative_overrides or {})
    candidate = _candidate(
        candidate_id,
        ["turnover_guardrail_failed"],
        relative,
        objective_score=0.08,
        windows=[
            {
                "window_id": "wf-1",
                "status": "worse",
                "baseline_metrics": {"turnover": 0.20},
                "candidate_metrics": {"turnover": 0.40},
                "relative_metrics": {
                    "baseline_turnover": 0.20,
                    "candidate_turnover": 0.40,
                    "turnover_delta": 0.20,
                    "cost_drag_delta": 0.002,
                },
                "objective_breakdown": {"objective_score": 0.03},
            }
        ],
    )
    candidate["metrics"] = {
        "annualized_return": 0.17,
        "sharpe_ratio": 1.18,
        "turnover": relative.get("candidate_turnover", 0.52),
        "estimated_cost_drag": 0.012,
    }
    candidate["l1_distance_from_baseline"] = l1_distance
    if asset_rows is not None:
        candidate["asset_turnover_contribution"] = asset_rows
    if rebalance is not None:
        candidate["rebalance_attribution"] = rebalance
    if score_volatility is not None:
        candidate["score_volatility"] = score_volatility
    return candidate


def _asset(
    symbol: str,
    contribution: float,
    share: float,
    rebalance_count: int,
    avg_change: float,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "turnover_contribution": contribution,
        "turnover_share": share,
        "rebalance_count": rebalance_count,
        "avg_weight_change": avg_change,
    }
