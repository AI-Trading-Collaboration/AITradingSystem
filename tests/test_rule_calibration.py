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
    run_rule_calibration,
    run_variant_comparison,
    validate_rule_calibration_artifact,
)


def test_rule_calibration_generates_manual_only_policy_proposals(
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
    comparison = run_variant_comparison(
        backfill_id=chain["backfill"]["backfill_id"],
        repair_id=repair["repair_id"],
        backfill_dir=paths["backfill_dir"],
        repair_dir=paths["backfill_repair_dir"],
        output_dir=paths["variant_comparison_dir"],
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )

    calibration = run_rule_calibration(
        comparison_id=comparison["comparison_id"],
        comparison_dir=paths["variant_comparison_dir"],
        output_dir=paths["rule_calibration_dir"],
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )

    proposals = calibration["proposed_policy_adjustments"]["proposals"]
    safety = calibration["calibration_safety_checks"]
    assert proposals
    assert all(proposal["auto_apply"] is False for proposal in proposals)
    assert all(proposal["requires_owner_approval"] is True for proposal in proposals)
    assert safety["auto_apply"] is False
    assert safety["broker_action_allowed"] is False
    assert safety["owner_approval_required"] is True
    assert calibration["manifest"]["baseline_config_mutated"] is False

    validation = validate_rule_calibration_artifact(
        calibration_id=calibration["calibration_id"],
        output_dir=paths["rule_calibration_dir"],
    )
    assert validation["status"] == "PASS"
