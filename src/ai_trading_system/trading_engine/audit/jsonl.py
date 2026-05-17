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
            payload=broker_order,
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
            payload=execution_report,
        )

    def log_portfolio_snapshot(
        self,
        portfolio_state: PortfolioState,
        *,
        run_id: str,
        strategy_id: str,
        schema_version: str = "1.0",
    ) -> Path:
        return self._write(
            stream="portfolio_snapshot",
            as_of=portfolio_state.as_of.date(),
            run_id=run_id,
            strategy_id=strategy_id,
            schema_version=schema_version,
            source_object_id=f"portfolio_snapshot:{portfolio_state.as_of.isoformat()}",
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
        payload: BaseModel,
    ) -> Path:
        path = self.root_dir / stream / f"{as_of.isoformat()}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "timestamp": datetime.now(UTC).isoformat(),
            "run_id": run_id,
            "strategy_id": strategy_id,
            "schema_version": schema_version,
            "source_object_id": source_object_id,
            "payload": payload.model_dump(mode="json"),
        }
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
