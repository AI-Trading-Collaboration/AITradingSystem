from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.audit import JsonlAuditLogger
from ai_trading_system.trading_engine.audit.jsonl import (
    read_jsonl,
    replay_intent_audit_trace,
)
from ai_trading_system.trading_engine.config import load_trading_engine_config
from ai_trading_system.trading_engine.execution import ExecutionService, PaperBroker
from ai_trading_system.trading_engine.portfolio import PaperPortfolio
from ai_trading_system.trading_engine.risk import PreTradeRiskChecker
from ai_trading_system.trading_engine.schemas import (
    AssetType,
    MarketContext,
    MarketSnapshot,
    OrderIntent,
    OrderSide,
    OrderType,
    TimeInForce,
)


def test_audit_replay_links_approved_filled_intent(tmp_path: Path) -> None:
    audit_root = tmp_path / "audit"
    as_of = date(2026, 5, 17)
    filled_intent = _intent(intent_id="intent_filled", symbol="TSM")
    open_intent = _intent(intent_id="intent_open", symbol="NVDA", limit_price=100.0)
    rejected_intent = _intent(
        intent_id="intent_rejected",
        symbol="INTC",
        confidence=0.50,
    )
    service = _service(audit_root)

    service.execute(filled_intent, market_context=MarketContext(as_of=as_of))
    service.execute(open_intent, market_context=MarketContext(as_of=as_of))
    service.execute(rejected_intent, market_context=MarketContext(as_of=as_of))
    service.process_market_snapshot(
        [
            _snapshot(symbol="TSM", open_price=99.5, low=99.0, high=101.0, last=101.0),
            _snapshot(symbol="NVDA", open_price=105.0, low=101.0, high=106.0, last=105.0),
        ]
    )

    trace = replay_intent_audit_trace(audit_root, filled_intent.intent_id, as_of=as_of)

    assert len(trace["order_intent_log"]) == 1
    assert len(trace["risk_check_log"]) == 1
    assert trace["risk_check_log"][0]["payload"]["approved"] is True
    assert len(trace["order_log"]) == 1
    assert len(trace["fill_log"]) == 1
    assert len(trace["portfolio_snapshot"]) == 1
    execution_statuses = [
        record["payload"]["status"] for record in trace["execution_report_log"]
    ]
    assert execution_statuses == ["SUBMITTED", "FILLED"]

    order_payload = trace["order_log"][0]["payload"]
    fill_payload = trace["fill_log"][0]["payload"]
    assert order_payload["intent_id"] == filled_intent.intent_id
    assert fill_payload["intent_id"] == filled_intent.intent_id
    assert fill_payload["broker_order_id"] == order_payload["broker_order_id"]
    assert filled_intent.intent_id in trace["portfolio_snapshot"][0][
        "related_intent_ids"
    ]


def test_audit_replay_keeps_rejected_intent_without_broker_order_or_fill(
    tmp_path: Path,
) -> None:
    audit_root = tmp_path / "audit"
    as_of = date(2026, 5, 17)
    rejected_intent = _intent(
        intent_id="intent_rejected",
        symbol="INTC",
        confidence=0.50,
    )
    service = _service(audit_root)

    service.execute(rejected_intent, market_context=MarketContext(as_of=as_of))

    trace = replay_intent_audit_trace(audit_root, rejected_intent.intent_id, as_of=as_of)

    assert len(trace["order_intent_log"]) == 1
    assert len(trace["risk_check_log"]) == 1
    assert trace["risk_check_log"][0]["payload"]["approved"] is False
    assert trace["order_log"] == []
    assert trace["fill_log"] == []
    assert trace["portfolio_snapshot"] == []
    assert [record["payload"]["status"] for record in trace["execution_report_log"]] == [
        "REJECTED"
    ]


def test_audit_replay_keeps_open_intent_without_fill(tmp_path: Path) -> None:
    audit_root = tmp_path / "audit"
    as_of = date(2026, 5, 17)
    open_intent = _intent(intent_id="intent_open", symbol="NVDA", limit_price=100.0)
    service = _service(audit_root)

    service.execute(open_intent, market_context=MarketContext(as_of=as_of))
    service.process_market_snapshot(
        _snapshot(symbol="NVDA", open_price=105.0, low=101.0, high=106.0, last=105.0)
    )

    trace = replay_intent_audit_trace(audit_root, open_intent.intent_id, as_of=as_of)

    assert len(trace["order_intent_log"]) == 1
    assert trace["risk_check_log"][0]["payload"]["approved"] is True
    assert len(trace["order_log"]) == 1
    assert trace["fill_log"] == []
    assert [record["payload"]["status"] for record in trace["execution_report_log"]] == [
        "SUBMITTED"
    ]
    assert open_intent.intent_id in trace["portfolio_snapshot"][0]["related_intent_ids"]


def test_audit_records_have_required_lineage_fields(tmp_path: Path) -> None:
    audit_root = tmp_path / "audit"
    as_of = date(2026, 5, 17)
    filled_intent = _intent(intent_id="intent_filled", symbol="TSM")
    rejected_intent = _intent(
        intent_id="intent_rejected",
        symbol="INTC",
        confidence=0.50,
    )
    service = _service(audit_root)

    service.execute(filled_intent, market_context=MarketContext(as_of=as_of))
    service.execute(rejected_intent, market_context=MarketContext(as_of=as_of))
    service.process_market_snapshot(
        _snapshot(symbol="TSM", open_price=99.5, low=99.0, high=101.0, last=101.0)
    )

    records = _all_audit_records(audit_root)
    assert records
    for record in records:
        assert record["timestamp"]
        assert record["run_id"] == "run_2026_05_17"
        assert record["strategy_id"] == "audit_integrity_test"
        assert record["schema_version"] == "1.0"
        assert record["source_object_id"]
        assert "intent_id" in record or "related_intent_ids" in record
        assert isinstance(record["payload"], dict)


def _service(audit_root: Path) -> ExecutionService:
    config = load_trading_engine_config()
    return ExecutionService(
        risk_checker=PreTradeRiskChecker(config),
        broker=PaperBroker(portfolio=PaperPortfolio(100000.0)),
        audit_logger=JsonlAuditLogger(audit_root),
        config=config,
    )


def _intent(**overrides: object) -> OrderIntent:
    values = {
        "intent_id": "intent_1",
        "created_at": datetime(2026, 5, 17, 14, 0, tzinfo=UTC),
        "strategy_id": "audit_integrity_test",
        "strategy_version": "v1",
        "run_id": "run_2026_05_17",
        "symbol": "TSM",
        "asset_type": AssetType.STOCK,
        "side": OrderSide.BUY,
        "order_type": OrderType.LIMIT,
        "time_in_force": TimeInForce.DAY,
        "target_notional_usd": 1000.0,
        "limit_price": 100.0,
        "confidence": 0.75,
        "score_snapshot_id": "score_snapshot_1",
    }
    values.update(overrides)
    return OrderIntent.model_validate(values)


def _snapshot(
    *,
    symbol: str,
    open_price: float,
    low: float,
    high: float,
    last: float,
) -> MarketSnapshot:
    return MarketSnapshot(
        symbol=symbol,
        timestamp=datetime(2026, 5, 17, 20, 0, tzinfo=UTC),
        open=open_price,
        high=high,
        low=low,
        last=last,
    )


def _all_audit_records(audit_root: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(audit_root.rglob("*.jsonl")):
        records.extend(read_jsonl(path))
    return records
