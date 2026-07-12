from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_historical_replay_helpers import (
    build_replay_review_chain,
    prepare_replay_test_environment,
)

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    run_backfill_repair,
    run_replay_diagnosis,
    validate_backfill_repair_artifact,
)


def test_backfill_repair_recomputes_available_windows_without_future_decision_leakage(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    chain = build_replay_review_chain(
        paths,
        backfill_generated_at=datetime(2026, 6, 30, tzinfo=UTC),
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

    repair = run_backfill_repair(
        backfill_id=chain["backfill"]["backfill_id"],
        diagnosis_id=diagnosis["diagnosis_id"],
        backfill_dir=paths["backfill_dir"],
        diagnosis_dir=paths["diagnosis_dir"],
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["backfill_repair_dir"],
        prices_path=paths["prices_path"],
        rates_path=paths["rates_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )

    delta = repair["backfill_availability_delta"]
    assert delta["repaired_count"] > 0
    assert delta["after"]["available"] > delta["before"]["available"]
    assert delta["still_pending_count"] < delta["before"]["pending"]
    assert repair["manifest"]["future_data_used_in_decision"] is False
    assert all(
        action["future_data_used_in_decision"] is False for action in repair["repair_actions"]
    )

    validation = validate_backfill_repair_artifact(
        repair_id=repair["repair_id"],
        output_dir=paths["backfill_repair_dir"],
    )
    assert validation["status"] == "PASS"
