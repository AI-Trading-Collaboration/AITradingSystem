from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_historical_replay_helpers import (
    build_replay_review_chain,
    prepare_replay_test_environment,
)

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    run_replay_diagnosis,
    validate_replay_diagnosis_artifact,
)


def test_replay_diagnosis_classifies_pending_reasons_and_counts_paper_events(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    chain = build_replay_review_chain(
        paths,
        backfill_generated_at=datetime(2026, 6, 3, tzinfo=UTC),
    )

    diagnosis = run_replay_diagnosis(
        inventory_id=chain["inventory"]["inventory_id"],
        replay_id=chain["replay"]["replay_id"],
        backfill_id=chain["backfill"]["backfill_id"],
        sim_id=chain["sim"]["sim_id"],
        review_id=chain["review"]["review_id"],
        inventory_dir=paths["inventory_dir"],
        replay_dir=paths["historical_replay_dir"],
        backfill_dir=paths["backfill_dir"],
        sim_dir=paths["paper_sim_dir"],
        review_dir=paths["performance_review_dir"],
        output_dir=paths["diagnosis_dir"],
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )

    coverage = diagnosis["coverage_breakdown"]
    reasons = {
        row["reason"]: row for row in diagnosis["pending_reason_summary"]["pending_reasons"]
    }
    assert coverage["inventory"]["pit_safe"] == 2
    assert coverage["inventory"]["pit_warning"] == 0
    assert coverage["inventory"]["pit_unsafe"] == 0
    assert coverage["replay"]["replayed_events"] == 2
    assert coverage["backfill"]["available_windows"] == 0
    assert coverage["backfill"]["pending_windows"] > 0
    assert coverage["paper_sim"]["event_count"] == len(chain["sim"]["state_history"])
    assert reasons["future_window_not_reached"]["blocking"] is False
    assert reasons["no_available_outcome_windows"]["blocking"] is True
    assert reasons["review_waiting_for_backfill"]["recommended_action"] == (
        "complete_backfill_before_review"
    )

    validation = validate_replay_diagnosis_artifact(
        diagnosis_id=diagnosis["diagnosis_id"],
        output_dir=paths["diagnosis_dir"],
    )
    assert validation["status"] == "PASS"
