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
    run_variant_comparison,
    validate_variant_comparison_artifact,
)


def test_variant_comparison_outputs_pairwise_metrics_for_validated_replay_chain(
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

    insufficient = run_variant_comparison(
        backfill_id=chain["backfill"]["backfill_id"],
        backfill_dir=paths["backfill_dir"],
        output_dir=paths["variant_comparison_dir"],
        generated_at=datetime(2026, 7, 20, tzinfo=UTC),
    )
    assert insufficient["variant_rank_summary"]["recommendation_confidence"] == "LOW"

    comparison = run_variant_comparison(
        backfill_id=chain["backfill"]["backfill_id"],
        repair_id=repair["repair_id"],
        backfill_dir=paths["backfill_dir"],
        repair_dir=paths["backfill_repair_dir"],
        output_dir=paths["variant_comparison_dir"],
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )

    pair = next(
        row
        for row in comparison["variant_pairwise_comparison"]["comparisons"]
        if {row["variant_a"], row["variant_b"]} == {"limited_adjustment", "no_trade"}
        and row["window_days"] == 5
    )
    assert pair["event_count"] > 0
    assert pair["conclusion"] in {
        "variant_a_better",
        "variant_b_better",
        "mixed",
        "insufficient_data",
    }
    assert comparison["variant_rank_summary"]["best_variant"] != "MISSING"
    assert comparison["variant_rank_summary"]["recommendation_confidence"] == "LOW"

    validation = validate_variant_comparison_artifact(
        comparison_id=comparison["comparison_id"],
        output_dir=paths["variant_comparison_dir"],
    )
    assert validation["status"] == "PASS"
