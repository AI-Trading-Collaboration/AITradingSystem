from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.decision_causal_chains import (
    build_decision_causal_chain_ledger,
    lookup_decision_causal_chain,
    render_decision_causal_chain_report,
)


def test_build_decision_causal_chain_keeps_outcomes_post_signal(
    tmp_path: Path,
) -> None:
    trace_path = _write_trace_bundle(tmp_path)
    snapshots = (
        _snapshot("2026-04-01", 70.0, 80.0, 0.60, trace_path),
        _snapshot("2026-04-02", 62.0, 65.0, 0.45, trace_path),
    )
    outcomes = pd.DataFrame(
        [
            {
                "snapshot_id": "decision_snapshot:2026-04-02",
                "horizon_days": 5,
                "outcome_status": "AVAILABLE",
                "outcome_end_date": "2026-04-09",
                "ai_proxy_return": -0.03,
                "ai_proxy_max_drawdown": -0.05,
                "ai_proxy_realized_volatility": 0.30,
                "hit": False,
            }
        ]
    )

    ledger = build_decision_causal_chain_ledger(
        snapshots=snapshots,
        outcomes=outcomes,
        generated_at=datetime(2026, 4, 10, tzinfo=UTC),
    )

    chain = ledger.chains[1]
    signal_context = chain["signal_time_context"]
    post_signal = chain["post_signal_observations"]
    assert chain["chain_id"] == "decision_causal_chain:2026-04-02:overall_position"
    assert signal_context["score_delta"] == -8.0
    assert signal_context["confidence_delta"] == -15.0
    assert signal_context["position_delta"]["final_max_delta"] == pytest.approx(-0.15)
    assert signal_context["linked_evidence_ids"] == [
        "evidence:daily_score:2026-04-02:position"
    ]
    assert "linked_outcome_windows" not in signal_context
    assert post_signal["append_only"] is True
    assert post_signal["linked_outcome_windows"][0]["horizon_days"] == 5

    markdown = render_decision_causal_chain_report(
        ledger,
        ledger_path=tmp_path / "decision_causal_chains.json",
    )

    assert "signal_time_context" in markdown
    assert "post_signal_observations" in markdown
    assert "PASS / 错误 0 / 警告 1" in markdown
    assert "decision_causal_chain:2026-04-02:overall_position" in markdown


def test_feedback_causal_chain_cli_writes_and_looks_up_chain(tmp_path: Path) -> None:
    trace_path = _write_trace_bundle(tmp_path)
    snapshot_dir = tmp_path / "decision_snapshots"
    snapshot_dir.mkdir()
    for snapshot in (
        _snapshot("2026-04-01", 70.0, 80.0, 0.60, trace_path),
        _snapshot("2026-04-02", 62.0, 65.0, 0.45, trace_path),
    ):
        signal_date = snapshot["signal_date"]
        (snapshot_dir / f"decision_snapshot_{signal_date}.json").write_text(
            json.dumps(snapshot, ensure_ascii=False),
            encoding="utf-8",
        )
    outcomes_path = tmp_path / "decision_outcomes.csv"
    pd.DataFrame(
        [
            {
                "snapshot_id": "decision_snapshot:2026-04-02",
                "horizon_days": 1,
                "outcome_status": "AVAILABLE",
                "ai_proxy_return": 0.01,
            }
        ]
    ).to_csv(outcomes_path, index=False)
    ledger_path = tmp_path / "decision_causal_chains.json"
    report_path = tmp_path / "decision_causal_chains.md"

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "build-causal-chain",
            "--decision-snapshot-path",
            str(snapshot_dir),
            "--outcomes-path",
            str(outcomes_path),
            "--output-path",
            str(ledger_path),
            "--report-path",
            str(report_path),
            "--as-of",
            "2026-04-10",
        ],
    )

    assert result.exit_code == 0
    chain = lookup_decision_causal_chain(
        ledger_path,
        "decision_causal_chain:2026-04-02:overall_position",
    )
    assert chain["post_signal_observations"]["linked_outcome_windows"]
    assert report_path.exists()
    lookup = CliRunner().invoke(
        app,
        [
            "feedback",
            "lookup-chain",
            "--input-path",
            str(ledger_path),
            "--id",
            "decision_causal_chain:2026-04-02:overall_position",
        ],
    )
    assert lookup.exit_code == 0
    assert "触发 gate" in lookup.output


def _write_trace_bundle(tmp_path: Path) -> Path:
    trace_path = tmp_path / "daily_score_trace.json"
    trace_path.write_text(
        json.dumps(
            {
                "claims": [
                    {
                        "claim_id": "daily_score:2026-04-02:overall_position",
                        "evidence_ids": [
                            "evidence:daily_score:2026-04-02:position",
                        ],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return trace_path


def _snapshot(
    signal_date: str,
    score: float,
    confidence: float,
    final_max: float,
    trace_path: Path,
) -> dict[str, object]:
    return {
        "snapshot_id": f"decision_snapshot:{signal_date}",
        "signal_date": signal_date,
        "market_regime": {"regime_id": "ai_after_chatgpt"},
        "scores": {
            "overall_score": score,
            "confidence_score": confidence,
            "confidence_level": "high" if confidence >= 75 else "medium",
            "components": [
                {
                    "component": "trend",
                    "score": score,
                    "confidence": confidence / 100,
                    "source_type": "hard_data",
                    "reason": "trend test",
                },
                {
                    "component": "valuation",
                    "score": 42.0,
                    "confidence": 0.70,
                    "source_type": "manual_input",
                    "reason": "valuation crowded",
                },
            ],
        },
        "positions": {
            "final_risk_asset_ai_band": {
                "min_position": 0.20,
                "max_position": final_max,
                "label": "仓位受限",
            },
            "position_gates": [
                {
                    "gate_id": "score_model",
                    "label": "评分模型仓位",
                    "triggered": True,
                    "max_position": 0.80,
                    "reason": "score model",
                },
                {
                    "gate_id": "valuation",
                    "label": "估值拥挤",
                    "triggered": final_max < 0.60,
                    "max_position": final_max,
                    "reason": "valuation gate",
                },
            ],
        },
        "trace": {
            "trace_bundle_path": str(trace_path),
            "overall_claim_id": "daily_score:2026-04-02:overall_position",
        },
        "quality": {
            "market_data_status": "PASS",
            "market_data_error_count": 0,
            "market_data_warning_count": 1,
        },
        "belief_state_ref": {"path": "belief_state.json", "production_effect": "none"},
    }
