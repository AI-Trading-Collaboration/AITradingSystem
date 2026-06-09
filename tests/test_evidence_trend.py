from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_outcome_loop_helpers import run_rolling_refresh_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def test_evidence_trend_marks_single_refresh_as_insufficient_history(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    run_rolling_refresh_fixture(tmp_path, monkeypatch)

    result = accumulation.run_evidence_trend(
        output_dir=tmp_path / "evidence_trend",
        rolling_refresh_dir=tmp_path / "rolling_evidence_refresh",
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    summary = result["confidence_trend_summary"]
    assert summary["trend_status"] == "INSUFFICIENT_HISTORY"
    assert summary["confidence_change"] == "NO_CHANGE"
    assert summary["next_action"] == "continue_tracking"
    assert (
        accumulation.validate_evidence_trend_artifact(
            trend_id=result["trend_id"],
            output_dir=tmp_path / "evidence_trend",
        )["status"]
        == "PASS"
    )
