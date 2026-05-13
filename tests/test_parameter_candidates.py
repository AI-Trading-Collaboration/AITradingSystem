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

    assert ledger.status == "PASS_WITH_LIMITATIONS"
    assert ledger.trial_count == 4
    assert ledger.candidate_count == 3
    assert ledger.ready_for_owner_review_count == 1
    assert ledger.material_risk_review_count == 1
    assert ledger.needs_policy_count == 1
    assert "参数候选台账" in markdown
    assert "parameter_candidate:2026-04-01_2026-04-10:weight_perturb_trend_up_20pct" in markdown
    assert "READY_FOR_OWNER_REVIEW" in markdown
    assert "MATERIAL_RISK_REVIEW" in markdown
    assert "NEEDS_MATERIALITY_POLICY" in markdown


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
    assert "参数候选状态：PASS_WITH_LIMITATIONS" in result.output
    assert "Candidate 数：3" in result.output
    payload = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert payload["report_type"] == "parameter_candidate_ledger"
    assert payload["production_effect"] == "none"
    assert payload["trial_count"] == 4
    assert payload["candidate_count"] == 3
    assert payload["material_risk_review_count"] == 1
    assert report_path.exists()


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
                        "material_total_return_delta": None,
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
