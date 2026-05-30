from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.parameters.weight_tuning import write_weight_tuning_summary
from ai_trading_system.trading_engine.parameters.weight_tuning_failure import (
    build_weight_tuning_failure_payload,
    run_weight_tuning_failure_attribution,
    validate_weight_tuning_failure_payload,
    write_weight_tuning_failure_summary,
)
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture
from trading_engine.weight_tuning_helpers import (
    RESTRICTED_SHADOW_WEIGHTS,
    sample_weight_tuning_payload,
)


def test_missing_weight_tuning_summary_blocks(tmp_path: Path) -> None:
    as_of = date(2026, 5, 28)

    run = run_weight_tuning_failure_attribution(
        as_of=as_of,
        summary_path=tmp_path / "missing.json",
        output_root=tmp_path / "artifacts" / "weight_tuning_failure",
    )

    assert run.payload["metadata"]["status"] == "BLOCKED"
    assert run.payload["metadata"]["reason"] == "missing_weight_tuning_summary"
    assert run.payload["root_cause"]["category"] == "data_insufficient"
    assert run.json_path.exists()
    assert validate_weight_tuning_failure_payload(run.payload) == []


def test_missing_candidate_file_blocks(tmp_path: Path) -> None:
    as_of = date(2026, 5, 28)
    summary_path = _write_weight_tuning_summary_only(tmp_path, as_of)

    run = run_weight_tuning_failure_attribution(
        summary_path=summary_path,
        output_root=tmp_path / "artifacts" / "weight_tuning_failure",
    )

    assert run.payload["metadata"]["status"] == "BLOCKED"
    assert run.payload["metadata"]["reason"] == "missing_weight_tuning_candidates"
    assert run.payload["tuning_result"]["result"] == "NO_CANDIDATE"
    assert validate_weight_tuning_failure_payload(run.payload) == []


def test_failure_attribution_ranks_turnover_and_extracts_near_miss(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 28)
    summary_path, candidates_path = write_weight_tuning_failure_fixture(
        tmp_path,
        as_of=as_of,
        candidates=[
            _candidate(
                "wt-0001",
                ["turnover_guardrail_failed"],
                {
                    "annualized_return_delta": 0.02,
                    "sharpe_ratio_delta": 0.08,
                    "max_drawdown_delta": 0.0,
                    "turnover_relative_increase": 0.34,
                    "non_worse_walk_forward_ratio": 1.0,
                },
                objective_score=0.08,
            ),
            _candidate(
                "wt-0002",
                ["turnover_guardrail_failed"],
                {
                    "annualized_return_delta": 0.01,
                    "sharpe_ratio_delta": 0.03,
                    "max_drawdown_delta": 0.0,
                    "turnover_relative_increase": 0.38,
                    "non_worse_walk_forward_ratio": 1.0,
                },
                objective_score=0.04,
            ),
        ],
    )

    payload = build_weight_tuning_failure_payload(
        json.loads(summary_path.read_text(encoding="utf-8")),
        json.loads(candidates_path.read_text(encoding="utf-8")),
        summary_path=summary_path,
        candidates_path=candidates_path,
        output_root=tmp_path / "artifacts" / "weight_tuning_failure",
        generated_at=datetime(2026, 5, 30, tzinfo=UTC),
    )

    assert payload["metadata"]["status"] == "NO_CANDIDATE_EXPLAINED"
    assert payload["candidate_rejection_summary"]["total_candidates"] == 2
    assert payload["candidate_rejection_summary"]["rejected_by_guardrails"] == 2
    assert payload["failure_ranking"][0]["reason"] == "turnover_guardrail_failed"
    assert payload["root_cause"]["category"] == "portfolio_turnover_too_high"
    assert payload["near_miss_candidates"][0]["candidate_id"] == "wt-0001"
    assert "turnover_relative_increase" in payload["near_miss_candidates"][0]["distance_to_pass"]
    assert validate_weight_tuning_failure_payload(payload) == []


def test_failure_attribution_classifies_drawdown_no_alpha_walk_forward_and_search(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 28)
    cases = [
        (
            "drawdown",
            [
                _candidate(
                    "wt-0001",
                    ["drawdown_guardrail_failed"],
                    {
                        "annualized_return_delta": 0.02,
                        "sharpe_ratio_delta": 0.03,
                        "max_drawdown_delta": -0.04,
                        "non_worse_walk_forward_ratio": 1.0,
                    },
                )
            ],
            "drawdown_control_insufficient",
        ),
        (
            "no_alpha",
            [
                _candidate(
                    "wt-0001",
                    [],
                    {
                        "annualized_return_delta": -0.01,
                        "sharpe_ratio_delta": -0.02,
                        "max_drawdown_delta": 0.0,
                        "non_worse_walk_forward_ratio": 1.0,
                    },
                    guardrail_status="PASS",
                    objective_score=-0.03,
                )
            ],
            "no_alpha_detected",
        ),
        (
            "walk_forward",
            [
                _candidate(
                    "wt-0001",
                    ["walk_forward_guardrail_failed"],
                    {
                        "annualized_return_delta": 0.02,
                        "sharpe_ratio_delta": 0.04,
                        "max_drawdown_delta": 0.0,
                        "non_worse_walk_forward_ratio": 0.25,
                    },
                    windows=[
                        {
                            "window_id": "wf-1",
                            "status": "worse",
                            "relative_metrics": {
                                "annualized_return_delta": -0.02,
                                "sharpe_ratio_delta": -0.04,
                            },
                        }
                    ],
                )
            ],
            "walk_forward_unstable",
        ),
        ("search", [], "search_space_too_narrow"),
    ]
    for suffix, candidates, expected in cases:
        summary_path, candidates_path = write_weight_tuning_failure_fixture(
            tmp_path / suffix,
            as_of=as_of,
            candidates=candidates,
            signal_quality_status="OK" if suffix == "no_alpha" else "LIMITED",
        )
        payload = build_weight_tuning_failure_payload(
            json.loads(summary_path.read_text(encoding="utf-8")),
            json.loads(candidates_path.read_text(encoding="utf-8")),
            summary_path=summary_path,
            candidates_path=candidates_path,
            output_root=tmp_path / suffix / "artifacts" / "weight_tuning_failure",
        )

        assert payload["root_cause"]["category"] == expected
        assert payload["promotion_impact"]["can_support_candidate_promotion"] is False
        assert payload["safety"]["production_config_modified"] is False


def test_shadow_backtest_references_weight_tuning_failure_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    as_of = fixture["as_of"]
    assert isinstance(as_of, date)
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)
    failure_path = write_weight_tuning_failure_artifact(tmp_path, as_of=as_of)

    run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=as_of,
        config_path=fixture["config_path"],
        dry_run=True,
    )

    decision = run.payload["promotion_decision"]
    assert decision["supporting_artifacts"]["weight_tuning_failure"] == str(failure_path)
    assert decision["status"] == "rejected"
    assert run.payload["metadata"]["production_effect"] == "none"
    assert "portfolio_turnover_too_high" in decision["reason"]


def write_weight_tuning_failure_fixture(
    tmp_path: Path,
    *,
    as_of: date,
    candidates: list[dict[str, Any]],
    signal_quality_status: str = "LIMITED",
) -> tuple[Path, Path]:
    artifact_dir = tmp_path / "artifacts" / "weight_tuning" / as_of.isoformat()
    summary_path = artifact_dir / "weight_tuning_summary.json"
    markdown_path = artifact_dir / "weight_tuning_summary.md"
    candidates_path = artifact_dir / "weight_tuning_candidates.json"
    payload = sample_weight_tuning_payload(as_of=as_of)
    payload["signal_quality"]["status"] = signal_quality_status
    payload["output_artifacts"]["weight_tuning_candidates"] = str(candidates_path)
    payload["search"]["candidates_evaluated"] = len(candidates)
    payload["search"]["candidates_generated"] = len(candidates)
    payload["search"]["candidates_rejected_by_guardrails"] = len(
        [item for item in candidates if item.get("guardrail_status") != "PASS"]
    )
    if not candidates:
        payload["search"]["candidates_rejected_by_constraints"] = 12
    if candidates:
        payload["candidate_ranking"] = candidates
        payload["recommended_candidate"] = candidates[0]
    write_weight_tuning_summary(payload, summary_path, markdown_path)
    candidates_path.write_text(
        json.dumps(
            {
                "schema_version": payload["schema_version"],
                "report_type": "weight_tuning_candidates",
                "metadata": payload["metadata"],
                "summary_artifact": str(summary_path),
                "candidate_count": len(candidates),
                "candidates": candidates,
                "safety": payload["safety"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return summary_path, candidates_path


def write_weight_tuning_failure_artifact(
    tmp_path: Path,
    *,
    as_of: date,
    root_cause: str = "portfolio_turnover_too_high",
) -> Path:
    summary_path, candidates_path = write_weight_tuning_failure_fixture(
        tmp_path,
        as_of=as_of,
        candidates=[
            _candidate(
                "wt-0001",
                ["turnover_guardrail_failed"],
                {
                    "annualized_return_delta": 0.02,
                    "sharpe_ratio_delta": 0.08,
                    "max_drawdown_delta": 0.0,
                    "turnover_relative_increase": 0.34,
                    "non_worse_walk_forward_ratio": 1.0,
                },
            )
        ],
    )
    payload = build_weight_tuning_failure_payload(
        json.loads(summary_path.read_text(encoding="utf-8")),
        json.loads(candidates_path.read_text(encoding="utf-8")),
        summary_path=summary_path,
        candidates_path=candidates_path,
        output_root=tmp_path / "artifacts" / "weight_tuning_failure",
    )
    payload["root_cause"]["category"] = root_cause
    artifact_dir = tmp_path / "artifacts" / "weight_tuning_failure" / as_of.isoformat()
    json_path = artifact_dir / "weight_tuning_failure_summary.json"
    markdown_path = artifact_dir / "weight_tuning_failure_summary.md"
    write_weight_tuning_failure_summary(payload, json_path, markdown_path)
    return json_path


def _write_weight_tuning_summary_only(tmp_path: Path, as_of: date) -> Path:
    artifact_dir = tmp_path / "artifacts" / "weight_tuning" / as_of.isoformat()
    summary_path = artifact_dir / "weight_tuning_summary.json"
    payload = sample_weight_tuning_payload(as_of=as_of)
    payload["output_artifacts"]["weight_tuning_candidates"] = str(
        artifact_dir / "weight_tuning_candidates.json"
    )
    write_weight_tuning_summary(payload, summary_path, artifact_dir / "weight_tuning_summary.md")
    return summary_path


def _candidate(
    candidate_id: str,
    rejection_reasons: list[str],
    relative_metrics: dict[str, float],
    *,
    guardrail_status: str = "FAIL",
    objective_score: float = 0.01,
    windows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    hard_rejections = [
        {
            "turnover_guardrail_failed": "turnover_increase_more_than_limit",
            "drawdown_guardrail_failed": "max_drawdown_worse_than_baseline_by_more_than_limit",
            "return_guardrail_failed": "annualized_return_underperformance_more_than_limit",
            "walk_forward_guardrail_failed": "non_worse_walk_forward_ratio_below_minimum",
        }.get(reason, reason)
        for reason in rejection_reasons
    ]
    return {
        "candidate_id": candidate_id,
        "weights": dict(RESTRICTED_SHADOW_WEIGHTS),
        "constraint_status": "PASS",
        "guardrail_status": guardrail_status,
        "status": "rejected",
        "rejection_reasons": rejection_reasons,
        "metrics": {"annualized_return": 0.17, "sharpe_ratio": 1.18},
        "relative_metrics": relative_metrics,
        "objective_breakdown": {"objective_score": objective_score},
        "guardrails": {
            "status": guardrail_status,
            "hard_rejections": hard_rejections,
            "turnover_relative_increase": relative_metrics.get("turnover_relative_increase", 0.0),
            "validation_window_count": len(windows or []),
        },
        "walk_forward_windows": windows or [],
        "l1_distance_from_baseline": 0.12,
        "fallback_signals_free_tuned": False,
    }
