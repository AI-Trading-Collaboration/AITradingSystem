from __future__ import annotations

from ai_trading_system.breadth_participation_feasibility_audit import (
    HORIZONS,
    USAGE_ROLES,
    build_candidate_family_design_sketch,
    build_candidate_signal_concept_matrix,
)


def test_breadth_candidate_family_design_sketch_is_generated() -> None:
    sketch = build_candidate_family_design_sketch(
        target_etfs=["QQQ", "SPY", "SMH"],
        target_assets=["QQQ", "SPY", "SMH"],
    )

    assert sketch["candidate_family"] == "breadth_participation"
    assert sketch["potential_candidate_ids"]
    assert sketch["broker_action"] == "none"


def test_breadth_candidate_signal_concepts_are_non_empty() -> None:
    rows = build_candidate_signal_concept_matrix(target_assets=["QQQ", "SPY", "SMH"])

    assert rows
    assert any(row["signal_name"] == "trend_fragility_score" for row in rows)


def test_breadth_candidate_signal_usage_roles_are_legal() -> None:
    rows = build_candidate_signal_concept_matrix(target_assets=["QQQ", "SPY", "SMH"])

    assert {row["usage_role"] for row in rows} <= USAGE_ROLES


def test_breadth_candidate_signal_horizons_are_legal() -> None:
    rows = build_candidate_signal_concept_matrix(target_assets=["QQQ", "SPY", "SMH"])

    for row in rows:
        assert set(row["horizons"]) <= set(HORIZONS)
        assert "1d" not in row["horizons"]
        assert row["broker_action"] == "none"
