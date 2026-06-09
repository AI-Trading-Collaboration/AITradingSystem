from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_paper_tracking_helpers import write_daily_advisory

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def test_consensus_risk_requires_samples_before_pass_and_forbids_default_execution(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(accumulation, "DEFAULT_LATEST_POINTER_DIR", tmp_path / "latest")
    write_daily_advisory(tmp_path, daily_advisory_id="daily-1", as_of="2026-06-08")

    result = accumulation.run_consensus_risk_review(
        output_dir=tmp_path / "consensus_risk",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        historical_replay_dir=tmp_path / "historical_replay",
        backfill_dir=tmp_path / "backfilled_outcome",
        repair_dir=tmp_path / "backfill_repair",
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    assert result["manifest"]["consensus_target_risk"] == "INSUFFICIENT_DATA"
    assert result["manifest"]["consensus_target_default_execution_recommended"] is False
    assert result["consensus_exposure_summary"]["sample_count"] == 1
    assert (
        accumulation.validate_consensus_risk_artifact(
            risk_id=result["risk_id"],
            output_dir=tmp_path / "consensus_risk",
        )["status"]
        == "PASS"
    )
