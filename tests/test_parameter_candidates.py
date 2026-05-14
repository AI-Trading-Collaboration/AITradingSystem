from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.parameter_candidates import (
    build_parameter_candidate_ledger,
    render_parameter_candidate_report,
)


def test_parameter_candidate_ledger_records_trials_and_candidates(
    tmp_path: Path,
) -> None:
    replay_path = _write_parameter_replay_summary(tmp_path)

    ledger = build_parameter_candidate_ledger(
        parameter_replay_summary_path=replay_path,
        as_of=date(2026, 4, 10),
        generated_at=datetime.fromisoformat("2026-04-10T12:00:00+00:00"),
    )
    markdown = render_parameter_candidate_report(ledger, tmp_path / "ledger.json")

    assert ledger.status == "PASS"
    assert ledger.trial_count == 4
    assert ledger.candidate_count == 3
    assert ledger.ready_for_forward_shadow_count == 1
    assert ledger.material_risk_review_count == 1
    assert ledger.needs_policy_count == 0
    assert "参数候选台账" in markdown
    assert "parameter_candidate:2026-04-01_2026-04-10:weight_perturb_trend_up_20pct" in markdown
    assert "READY_FOR_FORWARD_SHADOW" in markdown
    assert "MATERIAL_RISK_REVIEW" in markdown
    assert "eligible_for_multi_objective_gate" in markdown


def test_parameter_candidate_cli_writes_ledger_and_report(tmp_path: Path) -> None:
    replay_path = _write_parameter_replay_summary(tmp_path)
    ledger_path = tmp_path / "parameter_candidates.json"
    report_path = tmp_path / "parameter_candidates.md"

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "build-parameter-candidates",
            "--parameter-replay-summary-path",
            str(replay_path),
            "--as-of",
            "2026-04-10",
            "--output-path",
            str(ledger_path),
            "--report-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "参数候选状态：PASS" in result.output
    assert "Candidate 数：3" in result.output
    payload = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert payload["report_type"] == "parameter_candidate_ledger"
    assert payload["production_effect"] == "none"
    assert payload["trial_count"] == 4
    assert payload["candidate_count"] == 3
    assert payload["ready_for_forward_shadow_count"] == 1
    assert payload["material_risk_review_count"] == 1
    assert report_path.exists()


def test_parameter_candidate_blocks_random_baseline_failure(tmp_path: Path) -> None:
    replay_path = _write_parameter_replay_summary(tmp_path)
    payload = json.loads(replay_path.read_text(encoding="utf-8"))
    payload["robustness_evidence"]["same_turnover_random_strategy"].update(
        {
            "random_beats_count": 4,
            "dynamic_strategy_percentile": 0.60,
        }
    )
    replay_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    ledger = build_parameter_candidate_ledger(
        parameter_replay_summary_path=replay_path,
        as_of=date(2026, 4, 10),
    )

    trend_candidate = next(
        candidate
        for candidate in ledger.candidates
        if candidate.source_scenario_id == "weight_perturb_trend_up_20pct"
    )
    assert trend_candidate.recommendation_status == "BLOCKED_BY_RANDOM_BASELINE"
    assert "random_baseline_failed" in trend_candidate.veto_reasons
    assert ledger.blocked_count >= 1


def test_parameter_candidate_blocks_component_coverage_failure(tmp_path: Path) -> None:
    replay_path = _write_parameter_replay_summary(tmp_path)
    payload = json.loads(replay_path.read_text(encoding="utf-8"))
    payload["robustness_evidence"]["coverage"].update(
        {
            "minimum_component_coverage": 0.50,
            "minimum_average_component_coverage": 0.85,
            "maximum_placeholder_share": 0.25,
            "blocking_components": ["valuation"],
            "blocked": True,
        }
    )
    replay_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    ledger = build_parameter_candidate_ledger(
        parameter_replay_summary_path=replay_path,
        as_of=date(2026, 4, 10),
    )

    trend_candidate = next(
        candidate
        for candidate in ledger.candidates
        if candidate.source_scenario_id == "weight_perturb_trend_up_20pct"
    )
    assert trend_candidate.recommendation_status == "BLOCKED_BY_DATA"
    assert "data_component_coverage_blocked" in trend_candidate.veto_reasons
    assert "data_coverage_below_threshold" in trend_candidate.veto_reasons
    assert "data_placeholder_share_exceeded" in trend_candidate.veto_reasons
    assert trend_candidate.coverage_blocking_components == ("valuation",)


def test_parameter_candidate_requires_bootstrap_ci_support(tmp_path: Path) -> None:
    replay_path = _write_parameter_replay_summary(tmp_path)
    payload = json.loads(replay_path.read_text(encoding="utf-8"))
    for scenario in payload["scenarios"]:
        if scenario["scenario_id"] == "weight_perturb_trend_up_20pct":
            scenario["return_delta_bootstrap_ci_low"] = -0.01
            scenario["return_delta_bootstrap_ci_high"] = 0.08
    replay_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    ledger = build_parameter_candidate_ledger(
        parameter_replay_summary_path=replay_path,
        as_of=date(2026, 4, 10),
    )

    trend_candidate = next(
        candidate
        for candidate in ledger.candidates
        if candidate.source_scenario_id == "weight_perturb_trend_up_20pct"
    )
    assert trend_candidate.recommendation_status == "READY_FOR_AS_IF_REPLAY"
    assert "statistical_bootstrap_ci_crosses_threshold" in trend_candidate.veto_reasons


def test_parameter_candidate_requires_independent_sample_windows(tmp_path: Path) -> None:
    replay_path = _write_parameter_replay_summary(tmp_path)
    payload = json.loads(replay_path.read_text(encoding="utf-8"))
    payload["materiality_policy"]["candidate_min_independent_windows"] = 4
    payload["robustness_evidence"]["sample_independence"].update(
        {
            "effective_independent_windows": 1,
            "min_required_independent_windows": 4,
            "blocked": True,
        }
    )
    replay_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    ledger = build_parameter_candidate_ledger(
        parameter_replay_summary_path=replay_path,
        as_of=date(2026, 4, 10),
    )

    trend_candidate = next(
        candidate
        for candidate in ledger.candidates
        if candidate.source_scenario_id == "weight_perturb_trend_up_20pct"
    )
    assert trend_candidate.recommendation_status == "READY_FOR_AS_IF_REPLAY"
    assert "sample_independence_insufficient" in trend_candidate.veto_reasons


def test_parameter_candidate_risk_reviews_when_architecture_baseline_wins(
    tmp_path: Path,
) -> None:
    replay_path = _write_parameter_replay_summary(tmp_path)
    payload = json.loads(replay_path.read_text(encoding="utf-8"))
    payload["robustness_evidence"]["score_architecture_baseline"].update(
        {
            "best_total_return": 0.14,
            "best_delta_vs_base": 0.04,
            "base_beats_best_score_architecture_baseline": False,
        }
    )
    replay_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    ledger = build_parameter_candidate_ledger(
        parameter_replay_summary_path=replay_path,
        as_of=date(2026, 4, 10),
    )

    trend_candidate = next(
        candidate
        for candidate in ledger.candidates
        if candidate.source_scenario_id == "weight_perturb_trend_up_20pct"
    )
    assert trend_candidate.recommendation_status == "RISK_REVIEW"
    assert "score_architecture_baseline_not_beaten" in trend_candidate.veto_reasons


def _write_parameter_replay_summary(tmp_path: Path) -> Path:
    path = tmp_path / "parameter_replay_2026-04-10.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "feedback_parameter_replay",
                "production_effect": "none",
                "status": "PASS_WITH_LIMITATIONS",
                "as_of": "2026-04-10",
                "requested_start": "2026-04-01",
                "requested_end": "2026-04-10",
                "source_summary_path": "outputs/backtests/backtest_robustness.json",
                "materiality_policy": {
                    "weight_perturbation_material_total_return_delta_abs": 0.05,
                    "candidate_max_drawdown_worsening": 0.02,
                    "candidate_random_baseline_min_percentile": 0.90,
                    "candidate_max_random_beats_share": 0.10,
                    "candidate_min_oos_total_return": 0.0,
                    "candidate_blocking_data_credibility_grades": ["C"],
                    "candidate_min_component_coverage": 0.90,
                    "candidate_max_placeholder_share": 0.0,
                    "candidate_blocking_component_source_types": [
                        "insufficient_data",
                        "placeholder",
                    ],
                    "candidate_require_bootstrap_ci": True,
                    "candidate_min_bootstrap_ci_lower_total_return_delta": 0.0,
                    "candidate_label_horizon_days": 20,
                    "candidate_embargo_days": 5,
                    "candidate_min_independent_windows": 1,
                },
                "robustness_evidence": {
                    "data_quality": {
                        "status": "PASS",
                        "passed": True,
                        "data_credibility_grade": "B",
                        "remaining_gaps": [],
                    },
                    "coverage": {
                        "available": True,
                        "sample_count": 8,
                        "min_required_component_coverage": 0.90,
                        "max_allowed_placeholder_share": 0.0,
                        "blocking_source_types": ["insufficient_data", "placeholder"],
                        "minimum_component_coverage": 1.0,
                        "minimum_average_component_coverage": 1.0,
                        "maximum_placeholder_share": 0.0,
                        "blocking_components": [],
                        "blocked": False,
                    },
                    "sample_independence": {
                        "available": True,
                        "signal_count": 30,
                        "calendar_span_days": 45,
                        "label_horizon_days": 20,
                        "embargo_days": 5,
                        "effective_window_days": 25,
                        "effective_independent_windows": 1,
                        "min_required_independent_windows": 1,
                        "blocked": False,
                    },
                    "same_turnover_random_strategy": {
                        "available": True,
                        "random_path_count": 10,
                        "random_beats_count": 0,
                        "dynamic_strategy_percentile": 1.0,
                        "min_required_percentile": 0.90,
                        "max_random_beats_share": 0.10,
                    },
                    "out_of_sample_validation": {
                        "available": True,
                        "in_sample_total_return": 0.12,
                        "out_of_sample_total_return": 0.08,
                        "oos_vs_insample_degradation": 0.04,
                        "blocked": False,
                        "min_oos_total_return": 0.0,
                        "material_degradation_threshold": 0.05,
                    },
                    "signal_family_baseline": {
                        "available": True,
                        "best_scenario_id": "trend_only_baseline",
                        "best_total_return": 0.09,
                        "best_delta_vs_base": -0.01,
                        "base_beats_best_signal_family_baseline": True,
                    },
                    "score_architecture_baseline": {
                        "available": True,
                        "best_scenario_id": "alpha_only_score_baseline",
                        "best_total_return": 0.09,
                        "best_delta_vs_base": -0.01,
                        "base_beats_best_score_architecture_baseline": True,
                    },
                },
                "market_regime": {
                    "regime_id": "ai_after_chatgpt",
                    "name": "ChatGPT 后 AI 主线行情",
                },
                "warnings": [],
                "scenarios": [
                    {
                        "scenario_id": "weight_perturb_trend_up_20pct",
                        "label": "trend 权重上调 20%",
                        "category": "module_weight_perturbation",
                        "status": "PASS_WITH_LIMITATIONS",
                        "total_return_delta_vs_base": 0.06,
                        "max_drawdown_delta_vs_base": 0.01,
                        "turnover": 0.8,
                        "return_delta_bootstrap_ci_low": 0.02,
                        "return_delta_bootstrap_ci_high": 0.09,
                        "material_total_return_delta": True,
                        "skipped_reason": None,
                        "description": "测试 trend 权重上调。",
                    },
                    {
                        "scenario_id": "rebalance_every_5d",
                        "label": "每 5 个交易日再平衡",
                        "category": "rebalance_frequency",
                        "status": "PASS_WITH_LIMITATIONS",
                        "total_return_delta_vs_base": 0.01,
                        "max_drawdown_delta_vs_base": 0.005,
                        "turnover": 0.3,
                        "material_total_return_delta": False,
                        "skipped_reason": None,
                        "description": "测试再平衡频率。",
                    },
                    {
                        "scenario_id": "shifted_start",
                        "label": "起点后移",
                        "category": "window",
                        "status": "PASS_WITH_LIMITATIONS",
                        "total_return_delta_vs_base": -0.07,
                        "max_drawdown_delta_vs_base": 0.02,
                        "turnover": 0.5,
                        "material_total_return_delta": True,
                        "skipped_reason": None,
                        "description": "测试起点敏感性。",
                    },
                    {
                        "scenario_id": "late_window_skipped",
                        "label": "起点后移",
                        "category": "window",
                        "status": "SKIPPED",
                        "total_return_delta_vs_base": None,
                        "max_drawdown_delta_vs_base": None,
                        "turnover": None,
                        "material_total_return_delta": None,
                        "skipped_reason": "样本不足。",
                        "description": "测试起点敏感性。",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path
