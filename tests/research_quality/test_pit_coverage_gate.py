from __future__ import annotations

from ai_trading_system.research_quality.pit_coverage_gate import (
    build_pit_blocker_summary,
    evaluate_pit_gate,
)
from ai_trading_system.research_quality.pit_coverage_matrix import build_pit_coverage_matrix
from ai_trading_system.research_quality.pit_input_registry import (
    DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    load_pit_input_registry,
)


def test_dynamic_strategy_pit_gate_blocks_current_phase() -> None:
    registry = load_pit_input_registry(DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH)
    matrix = build_pit_coverage_matrix(registry)
    rows = matrix["pit_coverage_matrix"]

    gate = evaluate_pit_gate(rows)

    assert gate["candidate_search_allowed"] is False
    assert gate["research_only_observation_allowed"] is False
    assert gate["paper_shadow_allowed"] is False
    assert gate["production_allowed"] is False
    assert gate["blockers"] == [
        "BLOCKING_GAP_GROWTH_TILT_ENGINE",
        "BLOCKING_GAP_VALID_UNTIL_WINDOW",
    ]
    assert "policy-derived safety gate" in gate["policy_note"]
    assert "not a statistically calibrated empirical threshold" in gate["policy_note"]
    assert "VALID_UNTIL_WINDOW_NOT_GROUNDED" in gate["research_only_observation"][
        "reasons"
    ]
    assert "CURRENT_PHASE_PRODUCTION_DISABLED" in gate["production"]["reasons"]


def test_dynamic_strategy_pit_blocker_summary_preserves_blocking_inputs() -> None:
    registry = load_pit_input_registry(DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH)
    matrix = build_pit_coverage_matrix(registry)
    gate = evaluate_pit_gate(matrix["pit_coverage_matrix"])

    summary = build_pit_blocker_summary(matrix["pit_coverage_matrix"], gate)

    assert summary["blocking_gaps"] == ["growth_tilt_engine", "valid_until_window"]
    assert summary["candidate_search_allowed"] is False
    assert summary["research_only_observation_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    for input_id in ("growth_tilt_engine", "valid_until_window"):
        detail = summary["blocking_gap_details"][input_id]
        assert detail["candidate_search_blocker"] is True
        assert detail["observation_blocker"] is True
        assert detail["paper_shadow_blocker"] is True
        assert detail["production_blocker"] is True
