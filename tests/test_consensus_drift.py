from __future__ import annotations

import json
from pathlib import Path

from dynamic_v3_position_readiness_helpers import position_advisory_config

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    run_consensus_drift,
    validate_consensus_drift_artifact,
)


def test_consensus_drift_high_disagreement(tmp_path: Path) -> None:
    monitor_dir = tmp_path / "shadow_monitor_runs" / "monitor-high"
    monitor_dir.mkdir(parents=True)
    (monitor_dir / "shadow_monitor_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "monitor_run_id": "monitor-high",
                "shadow_shortlist_id": "shadow-high",
                "as_of": "2026-06-07",
                "status": "PASS",
                "candidate_count": 2,
                "broker_action_allowed": False,
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    rows = [
        {"candidate_id": "candidate-a", "target_weights": {"QQQ": 0.90, "CASH": 0.10}},
        {"candidate_id": "candidate-b", "target_weights": {"QQQ": 0.10, "CASH": 0.90}},
    ]
    (monitor_dir / "shadow_candidate_daily_results.jsonl").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )

    result = run_consensus_drift(
        shadow_monitor_run_id="monitor-high",
        config_path=position_advisory_config(tmp_path),
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
    )

    assert result["summary"]["disagreement_status"] == "HIGH_DISAGREEMENT"
    assert result["summary"]["position_advisory_implication"] == "manual_review_required"
    assert (
        validate_consensus_drift_artifact(
            drift_id=result["drift_id"],
            output_dir=tmp_path / "consensus_drift",
        )["status"]
        == "PASS"
    )
