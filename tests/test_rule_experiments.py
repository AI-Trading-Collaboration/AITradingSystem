from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.rule_experiments import (
    build_rule_experiment_ledger,
    lookup_rule_experiment,
    render_rule_experiment_report,
)


def test_build_rule_experiment_ledger_only_uses_required_candidates() -> None:
    ledger = build_rule_experiment_ledger(
        learning_items=(
            _learning_item("2026-04-02", rule_candidate_required=True),
            _learning_item("2026-04-03", rule_candidate_required=False),
            _learning_item(
                "2026-04-04",
                rule_candidate_required=True,
                attribution_category="sample_limited",
            ),
        ),
        generated_at=datetime(2026, 4, 10, tzinfo=UTC),
        replay_start=date(2022, 12, 1),
        replay_end=date(2026, 4, 10),
        shadow_start=date(2026, 4, 10),
        shadow_days=30,
    )
    markdown = render_rule_experiment_report(
        ledger,
        ledger_path=Path("rule_experiments.json"),
    )

    assert ledger.candidate_count == 1
    assert ledger.source_learning_item_count == 3
    candidate = ledger.candidates[0]
    assert candidate["candidate_id"] == "rule_experiment:2026-04-02_overall_position"
    assert candidate["production_effect"] == "none"
    assert candidate["approved_for_production"] is False
    assert candidate["replay_plan"]["status"] == "NOT_RUN"
    assert candidate["replay_plan"]["start_date"] == "2022-12-01"
    assert candidate["forward_shadow_plan"]["status"] == "PENDING"
    assert candidate["forward_shadow_plan"]["end_date"] == "2026-05-10"
    assert "未完成 replay、shadow 和 GOV-001 批准前" in markdown


def test_feedback_rule_experiment_cli_writes_and_looks_up_candidate(tmp_path: Path) -> None:
    learning_path = tmp_path / "decision_learning_queue.json"
    output_path = tmp_path / "rule_experiments.json"
    report_path = tmp_path / "rule_experiments.md"
    learning_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "items": [_learning_item("2026-04-02", rule_candidate_required=True)],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "build-rule-experiments",
            "--learning-queue-path",
            str(learning_path),
            "--output-path",
            str(output_path),
            "--report-path",
            str(report_path),
            "--as-of",
            "2026-04-10",
            "--shadow-days",
            "20",
        ],
    )

    assert result.exit_code == 0
    assert "候选规则实验台账已生成" in result.output
    candidate = lookup_rule_experiment(
        output_path,
        "rule_experiment:2026-04-02_overall_position",
    )
    assert candidate["governance"]["approval_status"] == "NOT_SUBMITTED"
    assert report_path.exists()
    lookup = CliRunner().invoke(
        app,
        [
            "feedback",
            "lookup-rule-experiment",
            "--input-path",
            str(output_path),
            "--id",
            "rule_experiment:2026-04-02_overall_position",
        ],
    )
    assert lookup.exit_code == 0
    assert "Production effect：none" in lookup.output


def _learning_item(
    signal_date: str,
    *,
    rule_candidate_required: bool,
    attribution_category: str = "rule_issue",
) -> dict[str, object]:
    return {
        "review_id": f"learning_review:{signal_date}_overall_position",
        "chain_id": f"decision_causal_chain:{signal_date}:overall_position",
        "signal_date": signal_date,
        "market_regime": {"regime_id": "ai_after_chatgpt"},
        "linked_decision_snapshot": f"decision_snapshot:{signal_date}",
        "linked_evidence_ids": [f"evidence:{signal_date}:overall"],
        "triggered_gate_ids": ["valuation"],
        "affected_modules": [
            {
                "component": "valuation",
                "score": 40.0,
                "confidence": 0.7,
                "source_type": "hard_data",
            }
        ],
        "outcome_direction": "failure",
        "attribution_category": attribution_category,
        "reason": "失败样本已触发 position gate，需要复核 gate 阈值。",
        "next_step": "评估是否生成 rule_candidate，并通过 replay/shadow 验证。",
        "rule_candidate_required": rule_candidate_required,
        "available_window_count": 1,
        "sample_limited": attribution_category == "sample_limited",
        "outcome_summary": {"mean_ai_proxy_return": -0.03},
    }
