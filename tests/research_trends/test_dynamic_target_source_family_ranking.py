from __future__ import annotations

from dynamic_target_source_remediation_fixtures import alignment_row, pit_row, source_row

from ai_trading_system.dynamic_target_baseline_source_remediation import (
    build_dynamic_target_source_family_ranking,
)


def test_source_family_ranking_prioritizes_target_exposure_semantics() -> None:
    inventory = [
        source_row(source_id="dynamic", source_type="dynamic_strategy_target_exposure"),
        source_row(
            source_id="unknown",
            source_type="unknown_candidate_source",
            target_exposure=False,
        ),
    ]
    rows = build_dynamic_target_source_family_ranking(
        inventory_rows=inventory,
        pit_rows=[pit_row("dynamic"), pit_row("unknown")],
        source_gap_rows=[],
        risk_alignment_rows=[alignment_row("dynamic"), alignment_row("unknown")],
        market_alignment_rows=[alignment_row("dynamic"), alignment_row("unknown")],
    )

    assert rows[0]["source_family"] == "dynamic_strategy_target_exposure"
    assert rows[0]["ranking_label"] in {
        "TOP_REMEDIATION_CANDIDATE",
        "REMEDIABLE_WITH_PIT_CAVEAT",
    }
    unknown = next(row for row in rows if row["source_family"] == "unknown_candidate_source")
    assert unknown["ranking_label"] == "NOT_REMEDIABLE"
    assert rows[0]["promotion_allowed"] is False
    assert rows[0]["broker_action"] == "none"
