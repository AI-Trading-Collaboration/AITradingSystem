from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from ai_trading_system.trading_engine.schemas.broker_order import BrokerOrder
from ai_trading_system.trading_engine.schemas.execution_report import ExecutionReport
from ai_trading_system.trading_engine.schemas.order_intent import OrderIntent
from ai_trading_system.trading_engine.schemas.portfolio_state import PortfolioState
from ai_trading_system.trading_engine.schemas.risk_result import RiskCheckResult

AUDIT_STREAMS = (
    "order_intent_log",
    "risk_check_log",
    "order_log",
    "execution_report_log",
    "fill_log",
    "portfolio_snapshot",
)


class JsonlAuditLogger:
    def __init__(self, root_dir: Path | str) -> None:
        self.root_dir = Path(root_dir)

    def log_order_intent(self, order_intent: OrderIntent) -> Path:
        return self._write(
            stream="order_intent_log",
            as_of=order_intent.created_at.date(),
            run_id=order_intent.run_id,
            strategy_id=order_intent.strategy_id,
            schema_version=order_intent.schema_version,
            source_object_id=order_intent.intent_id,
            intent_id=order_intent.intent_id,
            payload=order_intent,
        )

    def log_risk_result(
        self,
        risk_result: RiskCheckResult,
        *,
        order_intent: OrderIntent,
    ) -> Path:
        return self._write(
            stream="risk_check_log",
            as_of=order_intent.created_at.date(),
            run_id=order_intent.run_id,
            strategy_id=order_intent.strategy_id,
            schema_version=order_intent.schema_version,
            source_object_id=risk_result.intent_id,
            intent_id=order_intent.intent_id,
            payload=risk_result,
        )

    def log_order(self, broker_order: BrokerOrder, *, order_intent: OrderIntent) -> Path:
        return self._write(
            stream="order_log",
            as_of=order_intent.created_at.date(),
            run_id=order_intent.run_id,
            strategy_id=order_intent.strategy_id,
            schema_version=order_intent.schema_version,
            source_object_id=broker_order.broker_order_id,
            intent_id=order_intent.intent_id,
            payload=broker_order,
        )

    def log_execution_report(
        self,
        execution_report: ExecutionReport,
        *,
        order_intent: OrderIntent,
    ) -> Path:
        return self._write(
            stream="execution_report_log",
            as_of=order_intent.created_at.date(),
            run_id=order_intent.run_id,
            strategy_id=order_intent.strategy_id,
            schema_version=order_intent.schema_version,
            source_object_id=execution_report.broker_order_id or execution_report.intent_id,
            intent_id=order_intent.intent_id,
            payload=execution_report,
        )

    def log_fill(
        self,
        execution_report: ExecutionReport,
        *,
        order_intent: OrderIntent,
    ) -> Path:
        return self._write(
            stream="fill_log",
            as_of=order_intent.created_at.date(),
            run_id=order_intent.run_id,
            strategy_id=order_intent.strategy_id,
            schema_version=order_intent.schema_version,
            source_object_id=execution_report.broker_order_id or execution_report.intent_id,
            intent_id=order_intent.intent_id,
            payload=execution_report,
        )

    def log_portfolio_snapshot(
        self,
        portfolio_state: PortfolioState,
        *,
        run_id: str,
        strategy_id: str,
        schema_version: str = "1.0",
        related_intent_ids: tuple[str, ...] = (),
    ) -> Path:
        return self._write(
            stream="portfolio_snapshot",
            as_of=portfolio_state.as_of.date(),
            run_id=run_id,
            strategy_id=strategy_id,
            schema_version=schema_version,
            source_object_id=f"portfolio_snapshot:{portfolio_state.as_of.isoformat()}",
            intent_id=related_intent_ids[0] if len(related_intent_ids) == 1 else None,
            related_intent_ids=related_intent_ids,
            payload=portfolio_state,
        )

    def _write(
        self,
        *,
        stream: str,
        as_of: date,
        run_id: str,
        strategy_id: str,
        schema_version: str,
        source_object_id: str,
        intent_id: str | None = None,
        related_intent_ids: tuple[str, ...] = (),
        payload: BaseModel,
    ) -> Path:
        path = self.root_dir / stream / f"{as_of.isoformat()}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "stream": stream,
            "timestamp": datetime.now(UTC).isoformat(),
            "run_id": run_id,
            "strategy_id": strategy_id,
            "schema_version": schema_version,
            "source_object_id": source_object_id,
            "payload": payload.model_dump(mode="json"),
        }
        if intent_id is not None:
            record["intent_id"] = intent_id
        if related_intent_ids:
            record["related_intent_ids"] = list(related_intent_ids)
        with path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        return path


def read_jsonl(path: Path | str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                loaded = json.loads(line)
                if isinstance(loaded, dict):
                    records.append(loaded)
    return records


def replay_intent_audit_trace(
    root_dir: Path | str,
    intent_id: str,
    *,
    as_of: date | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Read all audit records related to one intent_id, grouped by stream."""
    root = Path(root_dir)
    trace: dict[str, list[dict[str, Any]]] = {stream: [] for stream in AUDIT_STREAMS}
    for stream in AUDIT_STREAMS:
        for path in _stream_paths(root / stream, as_of=as_of):
            for record in read_jsonl(path):
                if _record_matches_intent(record, intent_id):
                    trace[stream].append(record)
    return trace


def _stream_paths(stream_dir: Path, *, as_of: date | None) -> tuple[Path, ...]:
    if as_of is not None:
        path = stream_dir / f"{as_of.isoformat()}.jsonl"
        return (path,) if path.exists() else ()
    if not stream_dir.exists():
        return ()
    return tuple(sorted(stream_dir.glob("*.jsonl")))


def _record_matches_intent(record: dict[str, Any], intent_id: str) -> bool:
    if record.get("intent_id") == intent_id:
        return True
    related_intent_ids = record.get("related_intent_ids")
    if isinstance(related_intent_ids, list) and intent_id in related_intent_ids:
        return True
    if record.get("source_object_id") == intent_id:
        return True
    payload = record.get("payload")
    if isinstance(payload, dict):
        return payload.get("intent_id") == intent_id or payload.get("client_order_id") == intent_id
    return False
