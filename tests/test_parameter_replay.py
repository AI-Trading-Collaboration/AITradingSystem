from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.parameter_replay import (
    build_parameter_replay_report,
    render_parameter_replay_report,
    write_parameter_replay_summary,
)


def test_parameter_replay_report_summarizes_robustness_scenarios(
    tmp_path: Path,
) -> None:
    robustness_path = _write_robustness_summary(tmp_path)

    report = build_parameter_replay_report(
        robustness_summary_path=robustness_path,
        as_of=date(2026, 4, 10),
        generated_at=datetime.fromisoformat("2026-04-10T12:00:00+00:00"),
    )
    markdown = render_parameter_replay_report(report)

    assert report.status == "PASS"
    assert report.scenario_count == 8
    assert report.material_delta_count == 1
    assert report.robustness_evidence["same_turnover_random_strategy"][
        "dynamic_strategy_percentile"
    ] == 1.0
    assert report.robustness_evidence["out_of_sample_validation"]["blocked"] is False
    assert report.robustness_evidence["statistical"]["deflated_sharpe_proxy"][
        "available"
    ] is True
    assert report.robustness_evidence["statistical"]["pbo_proxy"]["value"] == 0.0
    assert "参数 as-if replay 收益变化报告" in markdown
    assert "production_effect：none" in markdown
    assert "weight_perturb_trend_up_20pct" in markdown
    assert "+6.0%" in markdown
    assert "任何参数进入 production 前仍需 replay/shadow" in markdown


def test_parameter_replay_cli_writes_markdown_and_summary(tmp_path: Path) -> None:
    robustness_path = _write_robustness_summary(tmp_path)
    report_path = tmp_path / "parameter_replay.md"
    summary_path = tmp_path / "parameter_replay.json"

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "build-parameter-replay",
            "--robustness-summary-path",
            str(robustness_path),
            "--as-of",
            "2026-04-10",
            "--output-path",
            str(report_path),
            "--summary-output-path",
            str(summary_path),
        ],
    )

    assert result.exit_code == 0
    assert "参数 replay 状态：PASS" in result.output
    assert "Material delta：1" in result.output
    assert report_path.exists()
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["report_type"] == "feedback_parameter_replay"
    assert summary["production_effect"] == "none"
    assert summary["scenario_count"] == 8
    assert summary["material_delta_count"] == 1
    assert summary["robustness_evidence"]["signal_family_baseline"][
        "base_beats_best_signal_family_baseline"
    ] is True


def test_parameter_replay_summary_writer(tmp_path: Path) -> None:
    robustness_path = _write_robustness_summary(tmp_path)
    report = build_parameter_replay_report(
        robustness_summary_path=robustness_path,
        as_of=date(2026, 4, 10),
    )
    output_path = write_parameter_replay_summary(report, tmp_path / "summary.json")

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["source_summary_path"] == str(robustness_path)
    assert payload["completed_scenario_count"] == 8


def test_parameter_replay_uses_current_policy_for_legacy_summary(
    tmp_path: Path,
) -> None:
    robustness_path = _write_robustness_summary(tmp_path, include_policy=False)

    report = build_parameter_replay_report(
        robustness_summary_path=robustness_path,
        as_of=date(2026, 4, 10),
    )

    assert report.status == "PASS_WITH_LIMITATIONS"
    assert report.material_delta_count == 1
    assert report.materiality_policy is not None
    assert report.materiality_policy["source"] == "current_backtest_validation_policy"
    assert any("缺少 materiality policy" in warning for warning in report.warnings)


def _write_robustness_summary(tmp_path: Path, *, include_policy: bool = True) -> Path:
    path = tmp_path / "backtest_robustness_2026-04-01_2026-04-10.json"
    payload = {
        "schema_version": 1,
        "report_type": "backtest_robustness",
        "production_effect": "none",
        "status": "PASS_WITH_LIMITATIONS",
        "requested_start": "2026-04-01",
        "requested_end": "2026-04-10",
        "first_signal_date": "2026-04-01",
        "last_signal_date": "2026-04-10",
        "data_quality_status": "PASS",
        "data_credibility_grade": "B",
        "coverage_evidence": {
            "available": True,
            "sample_count": 8,
            "min_required_component_coverage": 0.9,
            "max_allowed_placeholder_share": 0.0,
            "blocking_source_types": ["insufficient_data", "placeholder"],
            "minimum_component_coverage": 1.0,
            "minimum_average_component_coverage": 1.0,
            "maximum_placeholder_share": 0.0,
            "blocking_components": [],
            "blocked": False,
            "components": {
                "trend": {
                    "observations": 8,
                    "min_coverage": 1.0,
                    "average_coverage": 1.0,
                    "placeholder_count": 0,
                    "insufficient_data_count": 0,
                    "placeholder_share": 0.0,
                    "blocking_source_count": 0,
                    "source_type_counts": {"hard_data": 8},
                    "blocked": False,
                }
            },
        },
        "market_regime": {
            "regime_id": "ai_after_chatgpt",
            "name": "ChatGPT 后 AI 主线行情",
            "start_date": "2022-12-01",
            "anchor_date": "2022-11-30",
            "anchor_event": "ChatGPT 公开发布",
        },
        "base_dynamic": {
            "total_return": 0.10,
            "cagr": 1.20,
            "max_drawdown": -0.05,
            "sharpe": 1.5,
            "turnover": 0.7,
        },
        "remaining_gaps": [],
        "scenarios": [
            {
                "scenario_id": "weight_perturb_trend_up_20pct",
                "label": "trend 权重上调 20%",
                "category": "module_weight_perturbation",
                "status": "PASS_WITH_LIMITATIONS",
                "total_return": 0.16,
                "total_return_delta_vs_base": 0.06,
                "max_drawdown": -0.04,
                "sharpe": 1.8,
                "turnover": 0.8,
                "return_delta_bootstrap_ci_95": {
                    "low": 0.02,
                    "high": 0.09,
                    "daily_return_count": 8,
                    "method": "paired_block_bootstrap_total_return_delta",
                },
                "skipped_reason": None,
                "description": "测试 trend 上调。",
            },
            {
                "scenario_id": "weight_perturb_trend_down_20pct",
                "label": "trend 权重下调 20%",
                "category": "module_weight_perturbation",
                "status": "PASS_WITH_LIMITATIONS",
                "total_return": 0.08,
                "total_return_delta_vs_base": -0.02,
                "max_drawdown": -0.055,
                "sharpe": 1.1,
                "turnover": 0.6,
                "skipped_reason": None,
                "description": "测试 trend 下调。",
            },
            {
                "scenario_id": "rebalance_every_5d",
                "label": "每 5 个交易日再平衡",
                "category": "rebalance_frequency",
                "status": "PASS_WITH_LIMITATIONS",
                "total_return": 0.11,
                "total_return_delta_vs_base": 0.01,
                "max_drawdown": -0.045,
                "sharpe": 1.6,
                "turnover": 0.3,
                "skipped_reason": None,
                "description": "测试再平衡频率。",
            },
            {
                "scenario_id": "same_turnover_random_seed_42",
                "label": "同换手率随机策略",
                "category": "same_turnover_random_strategy",
                "status": "PASS_WITH_LIMITATIONS",
                "total_return": 0.02,
                "total_return_delta_vs_base": -0.08,
                "max_drawdown": -0.08,
                "sharpe": 0.4,
                "turnover": 0.7,
                "skipped_reason": None,
                "description": "非参数候选，作为随机基线证据。",
            },
            {
                "scenario_id": "same_turnover_random_seed_43",
                "label": "同换手率随机策略",
                "category": "same_turnover_random_strategy",
                "status": "PASS_WITH_LIMITATIONS",
                "total_return": 0.04,
                "total_return_delta_vs_base": -0.06,
                "max_drawdown": -0.07,
                "sharpe": 0.6,
                "turnover": 0.7,
                "skipped_reason": None,
                "description": "非参数候选，作为随机基线证据。",
            },
            {
                "scenario_id": "trend_only_baseline",
                "label": "趋势-only 基线",
                "category": "signal_family_baseline",
                "status": "PASS_WITH_LIMITATIONS",
                "total_return": 0.09,
                "total_return_delta_vs_base": -0.01,
                "max_drawdown": -0.06,
                "sharpe": 1.3,
                "turnover": 0.4,
                "skipped_reason": None,
                "description": "趋势信号族基线。",
            },
            {
                "scenario_id": "in_sample_window",
                "label": "in-sample 窗口",
                "category": "out_of_sample_validation",
                "status": "PASS_WITH_LIMITATIONS",
                "total_return": 0.10,
                "total_return_delta_vs_base": 0.0,
                "max_drawdown": -0.05,
                "sharpe": 1.4,
                "turnover": 0.5,
                "skipped_reason": None,
                "description": "样本内窗口。",
            },
            {
                "scenario_id": "out_of_sample_holdout",
                "label": "out-of-sample holdout",
                "category": "out_of_sample_validation",
                "status": "PASS_WITH_LIMITATIONS",
                "total_return": 0.08,
                "total_return_delta_vs_base": -0.02,
                "max_drawdown": -0.04,
                "sharpe": 1.2,
                "turnover": 0.4,
                "skipped_reason": None,
                "description": "样本外 holdout。",
            },
        ],
    }
    if include_policy:
        payload["policy"] = {
            "weight_perturbation_material_total_return_delta_abs": 0.05,
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
        }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path
