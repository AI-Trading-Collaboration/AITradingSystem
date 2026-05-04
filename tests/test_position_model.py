from __future__ import annotations

import pytest

from ai_trading_system.scoring.position_model import (
    ModuleScore,
    PositionGate,
    WeightedScoreModel,
)


def test_weighted_score_maps_to_position_band() -> None:
    model = WeightedScoreModel()

    recommendation = model.recommend(
        [
            ModuleScore("trend", score=80, weight=25, reason="trend strong"),
            ModuleScore("macro", score=60, weight=15, reason="macro neutral"),
        ]
    )

    assert recommendation.total_score == 72.5
    assert recommendation.min_position == 0.6
    assert recommendation.max_position == 0.8
    assert recommendation.risk_asset_ai_band.min_position == 0.6
    assert recommendation.risk_asset_ai_band.max_position == 0.8
    assert recommendation.label == "偏重仓"


def test_recommendation_includes_total_asset_exposure() -> None:
    model = WeightedScoreModel()

    recommendation = model.recommend(
        [ModuleScore("trend", score=80, weight=25, reason="trend strong")],
        total_risk_asset_min=0.6,
        total_risk_asset_max=0.8,
    )

    assert recommendation.risk_asset_ai_band.min_position == 0.8
    assert recommendation.risk_asset_ai_band.max_position == 1.0
    assert recommendation.total_asset_ai_band.min_position == 0.48
    assert recommendation.total_asset_ai_band.max_position == 0.8


def test_position_gate_caps_final_position_without_changing_model_band() -> None:
    model = WeightedScoreModel()

    recommendation = model.recommend(
        [ModuleScore("trend", score=85, weight=25, reason="trend strong")],
        position_gates=(
            PositionGate(
                gate_id="risk_events",
                label="风险事件",
                source="risk_event_occurrences",
                max_position=0.25,
                triggered=True,
                reason="L3 risk event",
            ),
        ),
    )

    assert recommendation.model_risk_asset_ai_band.min_position == 0.8
    assert recommendation.model_risk_asset_ai_band.max_position == 1.0
    assert recommendation.risk_asset_ai_band.min_position == 0.25
    assert recommendation.risk_asset_ai_band.max_position == 0.25
    assert recommendation.label == "重仓/仓位受限"
    assert recommendation.triggered_position_gates[-1].gate_id == "risk_events"


def test_invalid_score_is_rejected() -> None:
    model = WeightedScoreModel()

    with pytest.raises(ValueError, match="score must be between 0 and 100"):
        model.recommend([ModuleScore("trend", score=101, weight=25, reason="bad input")])


def test_invalid_total_asset_range_is_rejected() -> None:
    model = WeightedScoreModel()

    with pytest.raises(ValueError, match="position range"):
        model.recommend(
            [ModuleScore("trend", score=80, weight=25, reason="trend strong")],
            total_risk_asset_min=0.9,
            total_risk_asset_max=0.6,
        )
