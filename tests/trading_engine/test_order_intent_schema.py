from __future__ import annotations

from pydantic import ValidationError

from ai_trading_system.trading_engine.schemas import (
    AssetType,
    OrderIntent,
    OrderSide,
    OrderType,
    TimeInForce,
)


def test_order_intent_accepts_valid_limit_order() -> None:
    intent = _intent()

    assert intent.symbol == "TSM"
    assert intent.side == OrderSide.BUY
    assert intent.requested_notional_usd == 1000.0


def test_order_intent_rejects_invalid_side() -> None:
    with pytest_raises_validation_error():
        _intent(side="HOLD")


def test_order_intent_rejects_invalid_order_type() -> None:
    with pytest_raises_validation_error():
        _intent(order_type="MARKET")


def test_order_intent_rejects_confidence_outside_range() -> None:
    with pytest_raises_validation_error():
        _intent(confidence=1.2)


def test_order_intent_requires_quantity_or_notional() -> None:
    with pytest_raises_validation_error():
        _intent(target_notional_usd=None, target_quantity=None)


def _intent(**overrides: object) -> OrderIntent:
    values = {
        "strategy_id": "schema_test_strategy",
        "strategy_version": "v1",
        "run_id": "run_2026_05_17",
        "symbol": "tsm",
        "asset_type": AssetType.STOCK,
        "side": OrderSide.BUY,
        "order_type": OrderType.LIMIT,
        "time_in_force": TimeInForce.DAY,
        "target_notional_usd": 1000.0,
        "limit_price": 185.0,
        "confidence": 0.75,
        "score_snapshot_id": "score_snapshot_1",
    }
    values.update(overrides)
    return OrderIntent.model_validate(values)


class pytest_raises_validation_error:
    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> bool:
        assert exc_type is not None
        assert issubclass(exc_type, ValidationError)
        return True
