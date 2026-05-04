from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.periodic_investment_review import (
    build_periodic_investment_review_report,
    render_periodic_investment_review_report,
)


def test_periodic_investment_review_renders_weekly_sections(tmp_path: Path) -> None:
    inputs = _write_periodic_review_inputs(tmp_path)

    report = build_periodic_investment_review_report(
        period="weekly",
        as_of=date(2026, 5, 4),
        since=date(2026, 4, 28),
        scores_path=inputs["scores"],
        decision_snapshot_path=inputs["snapshots"],
        outcomes_path=inputs["outcomes"],
        learning_queue_path=inputs["learning"],
        rule_experiment_path=inputs["experiments"],
    )
    markdown = render_periodic_investment_review_report(report)

    assert report.status == "PASS"
    assert "# AI 产业链周报投资复盘" in markdown
    assert "本期结论是否变化" in markdown
    assert "改变判断的前三个证据" in markdown
    assert "下周最重要的观察事件" in markdown
    assert "daily_trace.json" in markdown
    assert "production_effect=none" in markdown


def test_reports_investment_review_cli_writes_monthly_report(tmp_path: Path) -> None:
    inputs = _write_periodic_review_inputs(tmp_path)
    output_path = tmp_path / "monthly_review.md"

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "investment-review",
            "--period",
            "monthly",
            "--as-of",
            "2026-05-04",
            "--since",
            "2026-05-01",
            "--scores-path",
            str(inputs["scores"]),
            "--decision-snapshot-path",
            str(inputs["snapshots"]),
            "--outcomes-path",
            str(inputs["outcomes"]),
            "--learning-queue-path",
            str(inputs["learning"]),
            "--rule-experiment-path",
            str(inputs["experiments"]),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    text = output_path.read_text(encoding="utf-8")
    assert "# AI 产业链月报投资复盘" in text
    assert "本月系统校准和规则学习" in text
    assert "Rule experiments" in text
    assert "投资复盘状态：PASS" in result.output


def _write_periodic_review_inputs(tmp_path: Path) -> dict[str, Path]:
    scores_path = tmp_path / "scores_daily.csv"
    pd.DataFrame(
        [
            {
                "as_of": "2026-05-01",
                "component": "overall",
                "score": 68.0,
                "confidence": 62.0,
                "final_risk_asset_ai_min": 0.4,
                "final_risk_asset_ai_max": 0.6,
                "total_asset_ai_min": 0.24,
                "total_asset_ai_max": 0.48,
                "triggered_position_gates": "valuation:40%",
                "confidence_reasons": "估值限制",
            },
            {
                "as_of": "2026-05-04",
                "component": "overall",
                "score": 73.0,
                "confidence": 66.0,
                "final_risk_asset_ai_min": 0.4,
                "final_risk_asset_ai_max": 0.4,
                "total_asset_ai_min": 0.24,
                "total_asset_ai_max": 0.32,
                "triggered_position_gates": "valuation:40%; thesis:70%",
                "confidence_reasons": "低置信度模块：policy_geopolitics",
            },
        ]
    ).to_csv(scores_path, index=False)

    snapshots_dir = tmp_path / "snapshots"
    snapshots_dir.mkdir()
    belief_start = tmp_path / "belief_start.json"
    belief_end = tmp_path / "belief_end.json"
    belief_start.write_text(
        json.dumps(_belief_state("节点覆盖 10 个。", "Risk stable"), ensure_ascii=False),
        encoding="utf-8",
    )
    belief_end.write_text(
        json.dumps(_belief_state("节点覆盖 12 个。", "Risk watch 1"), ensure_ascii=False),
        encoding="utf-8",
    )
    _write_snapshot(snapshots_dir, date(2026, 5, 1), belief_start, trend_score=60)
    _write_snapshot(snapshots_dir, date(2026, 5, 4), belief_end, trend_score=72)

    outcomes_path = tmp_path / "decision_outcomes.csv"
    pd.DataFrame(
        [
            {
                "signal_date": "2026-05-01",
                "outcome_status": "AVAILABLE",
                "ai_proxy_return": 0.04,
                "ai_proxy_max_drawdown": -0.02,
            },
            {
                "signal_date": "2026-05-04",
                "outcome_status": "PENDING",
            },
        ]
    ).to_csv(outcomes_path, index=False)

    learning_path = tmp_path / "learning.json"
    learning_path.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "created_at": "2026-05-02",
                        "rule_candidate_required": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    experiments_path = tmp_path / "experiments.json"
    experiments_path.write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "replay_plan": {"status": "NOT_RUN"},
                        "forward_shadow_plan": {"status": "PENDING"},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return {
        "scores": scores_path,
        "snapshots": snapshots_dir,
        "outcomes": outcomes_path,
        "learning": learning_path,
        "experiments": experiments_path,
    }


def _write_snapshot(
    snapshots_dir: Path,
    signal_date: date,
    belief_state_path: Path,
    *,
    trend_score: float,
) -> None:
    snapshot = {
        "signal_date": signal_date.isoformat(),
        "scores": {
            "components": [
                {"component": "trend", "score": trend_score},
                {"component": "valuation", "score": 45.0},
            ]
        },
        "belief_state_ref": {"path": str(belief_state_path), "read_only": True},
        "trace": {
            "overall_claim_id": f"daily_score:{signal_date.isoformat()}:overall_position",
            "trace_bundle_path": "daily_trace.json",
        },
    }
    path = snapshots_dir / f"decision_snapshot_{signal_date.isoformat()}.json"
    path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")


def _belief_state(industry_summary: str, risk_summary: str) -> dict[str, object]:
    return {
        "industry_chain_state": {"summary": industry_summary},
        "confidence": {"reasons": ["低置信度模块：policy_geopolitics"]},
        "thesis_state": {"summary": "Thesis warning 1"},
        "risk_state": {"summary": risk_summary},
        "valuation_state": {"summary": "估值偏贵"},
    }
