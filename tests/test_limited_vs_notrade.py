from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def test_limited_vs_notrade_marks_insufficient_data_and_does_not_apply_policy(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(accumulation, "DEFAULT_LATEST_POINTER_DIR", tmp_path / "latest")
    repair_dir = tmp_path / "backfill_repair" / "repair-1"
    repair_dir.mkdir(parents=True, exist_ok=True)
    (repair_dir / "backfill_repair_manifest.json").write_text(
        json.dumps({"repair_id": "repair-1", "broker_action_allowed": False}, sort_keys=True),
        encoding="utf-8",
    )
    rows = [
        {
            "replay_event_id": "event-1",
            "variant": "limited_adjustment",
            "window_days": 5,
            "return": 0.0,
            "max_drawdown": 0.0,
            "turnover": 0.1,
            "outcome_status": "PENDING",
        },
        {
            "replay_event_id": "event-1",
            "variant": "no_trade",
            "window_days": 5,
            "return": 0.0,
            "max_drawdown": 0.0,
            "turnover": 0.0,
            "outcome_status": "PENDING",
        },
    ]
    (repair_dir / "repaired_outcome_windows.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )

    result = accumulation.run_limited_vs_notrade_evaluation(
        output_dir=tmp_path / "limited_vs_notrade",
        repair_dir=tmp_path / "backfill_repair",
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    metrics = result["window_comparison_metrics"]
    assert result["manifest"]["available_count"] == 0
    assert metrics["overall_recommendation"] == "insufficient_data"
    assert all(row["confidence"] == "INSUFFICIENT_DATA" for row in metrics["by_window"])
    assert result["manifest"]["auto_policy_apply"] is False
    assert (
        accumulation.validate_limited_vs_notrade_artifact(
            focus_id=result["focus_id"],
            output_dir=tmp_path / "limited_vs_notrade",
        )["status"]
        == "PASS"
    )

