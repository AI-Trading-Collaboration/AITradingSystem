from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dynamic_v3_paper_tracking_helpers import (
    write_daily_advisory,
    write_market_cache,
    write_owner_review,
)

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import SCHEMA_VERSION


def test_replay_sample_expansion_classifies_pit_and_excludes_unsafe(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(accumulation, "DEFAULT_LATEST_POINTER_DIR", tmp_path / "latest")
    write_daily_advisory(tmp_path, daily_advisory_id="safe", as_of="2026-06-08")
    write_owner_review(tmp_path, daily_advisory_id="safe", as_of="2026-06-08")
    unsafe_dir = tmp_path / "position_advisory_daily" / "unsafe"
    unsafe_dir.mkdir(parents=True, exist_ok=True)
    (unsafe_dir / "daily_advisory_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "daily_advisory_id": "unsafe",
                "as_of": "2026-06-09",
                "generated_at": datetime(2026, 6, 9, tzinfo=UTC).isoformat(),
                "status": "PASS",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    prices_path, _ = write_market_cache(tmp_path / "market_cache", start="2026-06-08")
    prices = pd.read_csv(prices_path)
    prices["symbol"] = prices["ticker"]
    prices.to_csv(prices_path, index=False)

    result = accumulation.run_replay_sample_expansion(
        start=datetime(2026, 6, 1, tzinfo=UTC).date(),
        end=datetime(2026, 6, 30, tzinfo=UTC).date(),
        output_dir=tmp_path / "replay_sample_expansion",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        consensus_drift_dir=tmp_path / "consensus_drift",
        owner_review_dir=tmp_path / "owner_review_journal",
        replay_inventory_dir=tmp_path / "replay_inventory",
        prices_path=prices_path,
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    summary = result["pit_classification_summary"]
    unsafe = next(
        row for row in result["expanded_replay_events"] if row["daily_advisory_id"] == "unsafe"
    )
    assert summary["total_expanded_events"] == 2
    assert summary["pit_safe_count"] == 1
    assert summary["pit_unsafe_count"] == 1
    assert unsafe["replay_eligibility"] == "INELIGIBLE"
    assert (
        accumulation.validate_replay_sample_expansion_artifact(
            expansion_id=result["expansion_id"],
            output_dir=tmp_path / "replay_sample_expansion",
        )["status"]
        == "PASS"
    )
