from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_historical_replay_helpers import (
    build_replay_review_chain,
    prepare_replay_test_environment,
    report_index_for_dynamic_v3,
)

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    run_backfill_repair,
    run_replay_diagnosis,
    run_replay_forward_bridge,
    run_rule_calibration,
    run_variant_comparison,
    validate_replay_forward_bridge_artifact,
)
from ai_trading_system.reports import reader_brief


def test_replay_forward_bridge_feeds_reader_brief_without_policy_or_broker_effect(
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

    bridge = run_replay_forward_bridge(
        diagnosis_id=diagnosis["diagnosis_id"],
        comparison_id=comparison["comparison_id"],
        calibration_id=calibration["calibration_id"],
        diagnosis_dir=paths["diagnosis_dir"],
        comparison_dir=paths["variant_comparison_dir"],
        calibration_dir=paths["rule_calibration_dir"],
        output_dir=paths["replay_forward_bridge_dir"],
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )

    focus_items = bridge["forward_tracking_focus"]["focus_items"]
    assert focus_items
    assert focus_items[0]["required_future_events"] == 10
    assert bridge["manifest"]["broker_action_allowed"] is False
    assert bridge["manifest"]["baseline_config_mutated"] is False
    assert "Dynamic Rescue Replay-to-Forward Bridge" in (
        bridge["bridge_dir"] / "reader_brief_section.md"
    ).read_text(encoding="utf-8")

    report_index = report_index_for_dynamic_v3(paths, {**chain, "bridge": bridge})
    summary = reader_brief._etf_dynamic_v3_parameter_research_summary(report_index)
    assert summary["replay_forward_bridge_status"] == bridge["manifest"]["status"]
    assert summary["replay_forward_focus"] == focus_items[0]["item"]
    assert summary["replay_forward_next_action"] == bridge["manifest"]["next_action"]
    assert summary["production_candidate_generated"] is False
    assert summary["automatic_candidate_promotion"] is False

    replay_only_index = {
        "reports": [
            row
            for row in report_index["reports"]
            if row["report_id"] != "etf_dynamic_v3_parameter_sweep_leaderboard"
        ],
    }
    replay_only_summary = reader_brief._etf_dynamic_v3_parameter_research_summary(
        replay_only_index
    )
    assert replay_only_summary["replay_forward_bridge_status"] == bridge["manifest"]["status"]
    assert replay_only_summary["replay_forward_focus"] == focus_items[0]["item"]
    assert replay_only_summary["replay_forward_next_action"] == bridge["manifest"]["next_action"]

    validation = validate_replay_forward_bridge_artifact(
        bridge_id=bridge["bridge_id"],
        output_dir=paths["replay_forward_bridge_dir"],
    )
    assert validation["status"] == "PASS"
