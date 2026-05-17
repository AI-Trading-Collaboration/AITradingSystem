from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ai_trading_system.trading_engine.intent_builder import (
    build_order_intent_from_candidate,
)
from ai_trading_system.trading_engine.schemas import (
    AssetType,
    OrderIntentCandidate,
    OrderSide,
)


def test_blocked_candidate_cannot_convert_to_order_intent() -> None:
    candidate = _candidate(blocked=True, blocked_by=["manual_approval_required"])

    with pytest.raises(RuntimeError, match="blocked OrderIntentCandidate"):
        build_order_intent_from_candidate(candidate, mode="paper")


def test_real_mode_candidate_cannot_convert_to_order_intent() -> None:
    candidate = _candidate(blocked=False, mode="real")

    with pytest.raises(RuntimeError, match="mode must be paper"):
        build_order_intent_from_candidate(candidate, mode="paper")

    paper_candidate = _candidate(blocked=False)
    with pytest.raises(RuntimeError, match="allowed only in paper mode"):
        build_order_intent_from_candidate(paper_candidate, mode="real")


def test_unblocked_paper_candidate_converts_to_order_intent() -> None:
    candidate = _candidate(blocked=False)

    intent = build_order_intent_from_candidate(candidate, mode="paper")

    assert intent.symbol == "TSM"
    assert intent.target_notional_usd == 1000.0
    assert intent.limit_price == 100.0
    assert intent.metadata["source_candidate_id"] == "candidate_1"
    assert intent.metadata["production_effect"] == "none"


def test_unblocked_candidate_requires_order_fields() -> None:
    candidate = _candidate(blocked=False, symbol=None)

    with pytest.raises(ValueError, match="missing required fields"):
        build_order_intent_from_candidate(candidate, mode="paper")


def _candidate(**overrides: object) -> OrderIntentCandidate:
    values = {
        "candidate_id": "candidate_1",
        "created_at": datetime(2026, 5, 17, 14, 0, tzinfo=UTC),
        "strategy_id": "candidate_adapter_test",
        "strategy_version": "v1",
        "run_id": "run_2026_05_17",
        "symbol": "TSM",
        "asset_type": AssetType.STOCK,
        "side": OrderSide.BUY,
        "target_notional_usd": 1000.0,
        "limit_price": 100.0,
        "confidence": 0.75,
        "score_snapshot_id": "score_snapshot_1",
        "blocked": False,
        "blocked_by": [],
        "mode": "paper",
        "production_effect": "none",
    }
    values.update(overrides)
    return OrderIntentCandidate.model_validate(values)
