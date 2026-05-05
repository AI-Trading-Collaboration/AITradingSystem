from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.feedback_loop_review import (
    build_feedback_loop_review_report,
    render_feedback_loop_review_report,
)


def test_build_feedback_loop_review_report_covers_feedback_artifacts(
    tmp_path: Path,
) -> None:
    paths = _write_feedback_artifacts(tmp_path)

    report = build_feedback_loop_review_report(
        as_of=date(2026, 4, 10),
        since=date(2026, 4, 1),
        evidence_path=paths["evidence"],
        decision_snapshot_path=paths["snapshots"],
        outcomes_path=paths["outcomes"],
        prediction_outcomes_path=paths["prediction_outcomes"],
        causal_chain_path=paths["causal"],
        learning_queue_path=paths["learning"],
        rule_experiment_path=paths["rule_experiments_missing"],
        task_register_path=paths["task_register"],
    )
    markdown = render_feedback_loop_review_report(report)

    assert report.status == "PASS_WITH_LIMITATIONS"
    assert "市场阶段：ai_after_chatgpt" in markdown
    assert "## 新证据" in markdown
    assert "## Outcome 与校准" in markdown
    assert "## Prediction / Shadow Outcome" in markdown
    assert "## 因果链" in markdown
    assert "## 学习队列" in markdown
    assert "## Task Register" in markdown
    assert "rule_issue=1" in markdown
    assert "Blocked tasks：SOURCE-001" in markdown
    assert "EXPERIMENT-001 / GOV-001 尚未实现" in markdown


def test_feedback_loop_review_cli_writes_report(tmp_path: Path) -> None:
    paths = _write_feedback_artifacts(tmp_path)
    report_path = tmp_path / "feedback_loop_review.md"

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "loop-review",
            "--evidence-path",
            str(paths["evidence"]),
            "--decision-snapshot-path",
            str(paths["snapshots"]),
            "--outcomes-path",
            str(paths["outcomes"]),
            "--prediction-outcomes-path",
            str(paths["prediction_outcomes"]),
            "--causal-chain-path",
            str(paths["causal"]),
            "--learning-queue-path",
            str(paths["learning"]),
            "--rule-experiment-path",
            str(paths["rule_experiments_missing"]),
            "--task-register-path",
            str(paths["task_register"]),
            "--as-of",
            "2026-04-10",
            "--since",
            "2026-04-01",
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "反馈闭环复核状态：PASS_WITH_LIMITATIONS" in result.output
    assert report_path.exists()
    assert "反馈闭环周期复核报告" in report_path.read_text(encoding="utf-8")


def test_feedback_loop_review_reads_rule_experiment_ledger(tmp_path: Path) -> None:
    paths = _write_feedback_artifacts(tmp_path)
    paths["rule_experiments"].write_text(
        json.dumps(
            {
                "schema_version": 1,
                "candidate_count": 1,
                "pending_replay_count": 1,
                "pending_shadow_count": 1,
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

    report = build_feedback_loop_review_report(
        as_of=date(2026, 4, 10),
        since=date(2026, 4, 1),
        evidence_path=paths["evidence"],
        decision_snapshot_path=paths["snapshots"],
        outcomes_path=paths["outcomes"],
        prediction_outcomes_path=paths["prediction_outcomes"],
        causal_chain_path=paths["causal"],
        learning_queue_path=paths["learning"],
        rule_experiment_path=paths["rule_experiments"],
        task_register_path=paths["task_register"],
    )
    markdown = render_feedback_loop_review_report(report)

    assert "CONNECTED_PENDING_VALIDATION" in markdown
    assert "候选规则数：1" in markdown
    assert "未运行 replay：1" in markdown
    assert "Challenger 分组数：1" in markdown


def _write_feedback_artifacts(tmp_path: Path) -> dict[str, Path]:
    evidence_dir = tmp_path / "market_evidence"
    evidence_dir.mkdir()
    snapshot_dir = tmp_path / "decision_snapshots"
    snapshot_dir.mkdir()
    (snapshot_dir / "decision_snapshot_2026-04-02.json").write_text(
        json.dumps(
            {
                "snapshot_id": "decision_snapshot:2026-04-02",
                "signal_date": "2026-04-02",
            }
        ),
        encoding="utf-8",
    )
    outcomes_path = tmp_path / "decision_outcomes.csv"
    pd.DataFrame(
        [
            {
                "snapshot_id": "decision_snapshot:2026-04-02",
                "horizon_days": 5,
                "outcome_status": "AVAILABLE",
            },
            {
                "snapshot_id": "decision_snapshot:2026-04-02",
                "horizon_days": 20,
                "outcome_status": "PENDING",
            },
        ]
    ).to_csv(outcomes_path, index=False)
    prediction_outcomes_path = tmp_path / "prediction_outcomes.csv"
    pd.DataFrame(
        [
            {
                "prediction_id": "prediction:shadow",
                "candidate_id": "rule_experiment:2026-04-02_overall_position",
                "production_effect": "none",
                "outcome_status": "AVAILABLE",
            }
        ]
    ).to_csv(prediction_outcomes_path, index=False)
    causal_path = tmp_path / "decision_causal_chains.json"
    causal_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "chains": [{"chain_id": "decision_causal_chain:2026-04-02"}],
            }
        ),
        encoding="utf-8",
    )
    learning_path = tmp_path / "decision_learning_queue.json"
    learning_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "items": [
                    {
                        "review_id": "learning_review:2026-04-02",
                        "attribution_category": "rule_issue",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    rule_experiments_path = tmp_path / "rule_experiments.json"
    task_register_path = tmp_path / "task_register.md"
    task_register_path.write_text(
        "\n".join(
            [
                "|ID|领域|优先级|状态|下一责任方|阻塞或下一步|验收标准|备注|",
                "|---|---|---|---|---|---|---|---|",
                "|SOURCE-001|数据源|P1|BLOCKED_OWNER_INPUT|owner|等待输入|完成| |",
                "|LEARNING-001|反馈|P1|DONE|系统|已完成|完成| |",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "evidence": evidence_dir,
        "snapshots": snapshot_dir,
        "outcomes": outcomes_path,
        "prediction_outcomes": prediction_outcomes_path,
        "causal": causal_path,
        "learning": learning_path,
        "rule_experiments": rule_experiments_path,
        "rule_experiments_missing": tmp_path / "missing_rule_experiments.json",
        "task_register": task_register_path,
    }
