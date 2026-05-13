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
    assert report.scenario_count == 3
    assert report.material_delta_count == 1
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
    assert summary["scenario_count"] == 3
    assert summary["material_delta_count"] == 1


def test_parameter_replay_summary_writer(tmp_path: Path) -> None:
    robustness_path = _write_robustness_summary(tmp_path)
    report = build_parameter_replay_report(
        robustness_summary_path=robustness_path,
        as_of=date(2026, 4, 10),
    )
    output_path = write_parameter_replay_summary(report, tmp_path / "summary.json")

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["source_summary_path"] == str(robustness_path)
    assert payload["completed_scenario_count"] == 3


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
                "description": "非参数候选，报告不纳入参数 replay。",
            },
        ],
    }
    if include_policy:
        payload["policy"] = {"weight_perturbation_material_total_return_delta_abs": 0.05}
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path
