from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.market_feedback_optimization import (
    build_market_feedback_optimization_report,
    render_market_feedback_optimization_report,
)


def test_market_feedback_optimization_report_summarizes_readiness(
    tmp_path: Path,
) -> None:
    paths = _write_feedback_optimization_artifacts(tmp_path)

    report = build_market_feedback_optimization_report(
        as_of=date(2026, 4, 10),
        since=date(2026, 4, 1),
        replay_start=date(2022, 12, 1),
        replay_end=date(2026, 4, 10),
        data_quality_report_path=paths["data_quality"],
        decision_outcomes_path=paths["decision_outcomes"],
        prediction_outcomes_path=paths["prediction_outcomes"],
        causal_chain_path=paths["causal"],
        learning_queue_path=paths["learning"],
        rule_experiment_path=paths["rule_experiments"],
        shadow_maturity_report_path=paths["shadow_maturity"],
        calibration_overlay_path=paths["overlay"],
        effective_weights_path=paths["effective_weights"],
        sample_policy_path=paths["sample_policy"],
    )
    markdown = render_market_feedback_optimization_report(report)

    assert report.status == "PASS_WITH_LIMITATIONS"
    assert report.readiness == "READY_FOR_REPLAY_OR_SHADOW_REVIEW"
    assert "市场阶段：ai_after_chatgpt" in markdown
    assert "样本政策：feedback_sample_policy_test" in markdown
    assert "as-if 回放窗口：2022-12-01 至 2026-04-10" in markdown
    assert "生产影响：none" in markdown
    assert "## 执行频次" in markdown
    assert (
        "Decision outcome 可用样本：5 / reporting/pilot/diagnostic/promotion=1/5/30/60"
        in markdown
    )
    assert "候选规则数：1" in markdown
    assert "Approved overlay 数：1" in markdown


def test_market_feedback_optimization_cli_writes_report(tmp_path: Path) -> None:
    paths = _write_feedback_optimization_artifacts(tmp_path)
    output_path = tmp_path / "market_feedback_optimization.md"

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "optimize-market-feedback",
            "--as-of",
            "2026-04-10",
            "--since",
            "2026-04-01",
            "--replay-start",
            "2022-12-01",
            "--replay-end",
            "2026-04-10",
            "--data-quality-report-path",
            str(paths["data_quality"]),
            "--decision-outcomes-path",
            str(paths["decision_outcomes"]),
            "--prediction-outcomes-path",
            str(paths["prediction_outcomes"]),
            "--causal-chain-path",
            str(paths["causal"]),
            "--learning-queue-path",
            str(paths["learning"]),
            "--rule-experiment-path",
            str(paths["rule_experiments"]),
            "--shadow-maturity-report-path",
            str(paths["shadow_maturity"]),
            "--calibration-overlay-path",
            str(paths["overlay"]),
            "--effective-weights-path",
            str(paths["effective_weights"]),
            "--sample-policy-path",
            str(paths["sample_policy"]),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "市场反馈优化状态：PASS_WITH_LIMITATIONS" in result.output
    assert "Readiness：READY_FOR_REPLAY_OR_SHADOW_REVIEW" in result.output
    assert output_path.exists()
    assert "市场反馈优化闭环报告" in output_path.read_text(encoding="utf-8")


def _write_feedback_optimization_artifacts(tmp_path: Path) -> dict[str, Path]:
    sample_policy_path = tmp_path / "feedback_sample_policy.yaml"
    sample_policy_path.write_text(
        "\n".join(
            [
                "version: feedback_sample_policy_test",
                "status: pilot",
                "market_regime_id: ai_after_chatgpt",
                "review_after_reports: 8",
                "decision_outcomes:",
                "  reporting_floor: 1",
                "  pilot_floor: 5",
                "  diagnostic_floor: 30",
                "  promotion_floor: 60",
                "prediction_outcomes:",
                "  reporting_floor: 1",
                "  pilot_floor: 2",
                "  diagnostic_floor: 30",
                "  promotion_floor: 30",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    data_quality_path = tmp_path / "data_quality_2026-04-10.md"
    data_quality_path.write_text("# 数据质量报告\n\n- 状态：PASS\n", encoding="utf-8")

    decision_outcomes_path = tmp_path / "decision_outcomes.csv"
    pd.DataFrame(
        [
            {
                "snapshot_id": "decision_snapshot:2026-04-02",
                "signal_date": "2026-04-06",
                "horizon_days": 5,
                "outcome_status": "AVAILABLE",
            },
            {
                "snapshot_id": "decision_snapshot:2026-04-03",
                "signal_date": "2026-04-07",
                "horizon_days": 5,
                "outcome_status": "AVAILABLE",
            },
            {
                "snapshot_id": "decision_snapshot:2026-04-04",
                "signal_date": "2026-04-08",
                "horizon_days": 5,
                "outcome_status": "AVAILABLE",
            },
            {
                "snapshot_id": "decision_snapshot:2026-04-05",
                "signal_date": "2026-04-09",
                "horizon_days": 5,
                "outcome_status": "AVAILABLE",
            },
            {
                "snapshot_id": "decision_snapshot:2026-04-06",
                "signal_date": "2026-04-10",
                "horizon_days": 5,
                "outcome_status": "AVAILABLE",
            },
        ]
    ).to_csv(decision_outcomes_path, index=False)

    prediction_outcomes_path = tmp_path / "prediction_outcomes.csv"
    pd.DataFrame(
        [
            {
                "prediction_id": "prediction:shadow",
                "candidate_id": "rule_experiment:2026-04-02_overall_position",
                "production_effect": "none",
                "decision_date": "2026-04-09",
                "outcome_status": "AVAILABLE",
            },
            {
                "prediction_id": "prediction:shadow:2",
                "candidate_id": "rule_experiment:2026-04-02_overall_position",
                "production_effect": "none",
                "decision_date": "2026-04-10",
                "outcome_status": "AVAILABLE",
            }
        ]
    ).to_csv(prediction_outcomes_path, index=False)

    causal_path = tmp_path / "decision_causal_chains.json"
    causal_path.write_text(
        json.dumps({"schema_version": 1, "chains": [{"chain_id": "chain:1"}]}),
        encoding="utf-8",
    )

    learning_path = tmp_path / "decision_learning_queue.json"
    learning_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "items": [
                    {
                        "review_id": "learning_review:1",
                        "attribution_category": "rule_issue",
                        "rule_candidate_required": True,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    rule_experiments_path = tmp_path / "rule_experiments.json"
    rule_experiments_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "candidates": [
                    {
                        "candidate_id": "rule_experiment:2026-04-02_overall_position",
                        "replay_plan": {"status": "NOT_RUN"},
                        "forward_shadow_plan": {"status": "PENDING"},
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    shadow_maturity_path = tmp_path / "shadow_maturity_2026-04-10.md"
    shadow_maturity_path.write_text(
        "# Shadow 样本成熟度\n\n- 状态：PASS_WITH_LIMITATIONS\n",
        encoding="utf-8",
    )

    overlay_path = tmp_path / "approved_calibration_overlay.json"
    overlay_path.write_text(
        json.dumps(
            {
                "overlays": [
                    {"overlay_id": "overlay:approved", "status": "approved_soft"},
                    {"overlay_id": "overlay:candidate", "status": "candidate"},
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    effective_weights_path = tmp_path / "current_effective_weights.json"
    effective_weights_path.write_text(
        json.dumps({"matched_overlays": ["overlay:approved"]}),
        encoding="utf-8",
    )

    return {
        "data_quality": data_quality_path,
        "decision_outcomes": decision_outcomes_path,
        "prediction_outcomes": prediction_outcomes_path,
        "causal": causal_path,
        "learning": learning_path,
        "rule_experiments": rule_experiments_path,
        "shadow_maturity": shadow_maturity_path,
        "overlay": overlay_path,
        "effective_weights": effective_weights_path,
        "sample_policy": sample_policy_path,
    }
