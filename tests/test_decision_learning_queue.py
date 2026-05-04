from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.decision_learning_queue import (
    build_decision_learning_queue,
    lookup_decision_learning_item,
    render_decision_learning_queue_report,
)


def test_build_decision_learning_queue_classifies_failure_and_sample_limit() -> None:
    chains = (
        _chain(
            "2026-04-02",
            triggered_gate=True,
            outcome_return=-0.03,
            hit=False,
        ),
        _chain(
            "2026-04-03",
            triggered_gate=False,
            outcome_return=None,
            hit=None,
        ),
        _chain(
            "2026-04-04",
            triggered_gate=False,
            outcome_return=0.04,
            hit=True,
        ),
    )

    ledger = build_decision_learning_queue(
        chains=chains,
        generated_at=datetime(2026, 4, 10, tzinfo=UTC),
    )

    failure = ledger.items[0]
    limited = ledger.items[1]
    success = ledger.items[2]
    assert failure["attribution_category"] == "rule_issue"
    assert failure["outcome_direction"] == "failure"
    assert failure["rule_candidate_required"] is True
    assert limited["attribution_category"] == "sample_limited"
    assert limited["rule_candidate_required"] is False
    assert success["outcome_direction"] == "success"
    assert success["rule_candidate_required"] is False

    markdown = render_decision_learning_queue_report(
        ledger,
        ledger_path=Path("decision_learning_queue.json"),
    )
    assert "样本不足策略" in markdown
    assert "sample_limited" in markdown
    assert "rule_issue" in markdown


def test_build_decision_learning_queue_prioritizes_data_quality_issue() -> None:
    ledger = build_decision_learning_queue(
        chains=(
            _chain(
                "2026-04-02",
                triggered_gate=True,
                outcome_return=-0.03,
                hit=False,
                quality_status="FAIL",
            ),
        )
    )

    item = ledger.items[0]
    assert item["attribution_category"] == "data_issue"
    assert item["rule_candidate_required"] is False


def test_feedback_learning_queue_cli_writes_and_looks_up_item(tmp_path: Path) -> None:
    causal_path = tmp_path / "decision_causal_chains.json"
    queue_path = tmp_path / "decision_learning_queue.json"
    report_path = tmp_path / "decision_learning_queue.md"
    causal_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "chains": [
                    _chain(
                        "2026-04-02",
                        triggered_gate=True,
                        outcome_return=-0.03,
                        hit=False,
                    )
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "build-learning-queue",
            "--causal-chain-path",
            str(causal_path),
            "--output-path",
            str(queue_path),
            "--report-path",
            str(report_path),
            "--as-of",
            "2026-04-10",
        ],
    )

    assert result.exit_code == 0
    item = lookup_decision_learning_item(
        queue_path,
        "learning_review:2026-04-02_overall_position",
    )
    assert item["attribution_category"] == "rule_issue"
    assert report_path.exists()
    lookup = CliRunner().invoke(
        app,
        [
            "feedback",
            "lookup-learning",
            "--input-path",
            str(queue_path),
            "--id",
            "learning_review:2026-04-02_overall_position",
        ],
    )
    assert lookup.exit_code == 0
    assert "归因分类" in lookup.output


def _chain(
    signal_date: str,
    *,
    triggered_gate: bool,
    outcome_return: float | None,
    hit: bool | None,
    quality_status: str = "PASS",
) -> dict[str, object]:
    windows = []
    if outcome_return is not None:
        windows.append(
            {
                "horizon_days": 5,
                "outcome_status": "AVAILABLE",
                "ai_proxy_return": outcome_return,
                "hit": hit,
            }
        )
    return {
        "schema_version": 1,
        "chain_id": f"decision_causal_chain:{signal_date}:overall_position",
        "signal_date": signal_date,
        "market_regime": {"regime_id": "ai_after_chatgpt"},
        "signal_time_context": {
            "linked_decision_snapshot": f"decision_snapshot:{signal_date}",
            "linked_evidence_ids": [f"evidence:{signal_date}:overall"],
            "quality": {
                "market_data_status": quality_status,
                "market_data_error_count": 0 if quality_status == "PASS" else 1,
                "market_data_warning_count": 0,
            },
            "affected_modules": [
                {
                    "component": "trend",
                    "score": 62.0,
                    "confidence": 0.8,
                    "source_type": "hard_data",
                }
            ],
            "triggered_gates": (
                [
                    {
                        "gate_id": "valuation",
                        "label": "估值拥挤",
                        "reason": "valuation gate",
                    }
                ]
                if triggered_gate
                else []
            ),
        },
        "post_signal_observations": {
            "append_only": True,
            "linked_outcome_windows": windows,
            "linked_rule_candidate": None,
        },
    }
