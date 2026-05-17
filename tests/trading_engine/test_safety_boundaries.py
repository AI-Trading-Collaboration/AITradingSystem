from __future__ import annotations

import ast
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol, cast

import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.brokers import AlpacaAdapterStub, IbkrAdapterStub
from ai_trading_system.trading_engine.config import load_trading_engine_config
from ai_trading_system.trading_engine.execution import ExecutionService, PaperBroker
from ai_trading_system.trading_engine.reports import (
    build_trading_daily_report,
    render_trading_daily_report,
)
from ai_trading_system.trading_engine.schemas import (
    AssetType,
    OrderIntent,
    OrderSide,
    OrderType,
    PortfolioState,
    TimeInForce,
)


class SubmitOrderStub(Protocol):
    def submit_order(self, order_intent: OrderIntent) -> object: ...


def test_default_trading_engine_config_keeps_real_trading_disabled() -> None:
    config = load_trading_engine_config()

    assert config.trading.mode == "paper"
    assert config.trading.real_trading_enabled is False


def test_execution_service_rejects_real_trading_enabled_config() -> None:
    config = load_trading_engine_config()
    real_enabled_config = config.model_copy(
        update={"trading": config.trading.model_copy(update={"real_trading_enabled": True})}
    )

    with pytest.raises(RuntimeError, match="real_trading_enabled must remain false"):
        ExecutionService(config=real_enabled_config)


def test_execution_service_only_accepts_paper_broker() -> None:
    with pytest.raises(RuntimeError, match="only supports PaperBroker"):
        ExecutionService(
            broker=cast(PaperBroker, IbkrAdapterStub()),
            config=load_trading_engine_config(),
        )


@pytest.mark.parametrize(
    ("adapter", "expected_message"),
    [
        (IbkrAdapterStub(), "IBKR adapter stub cannot submit orders"),
        (AlpacaAdapterStub(), "Alpaca adapter stub cannot submit orders"),
    ],
)
def test_real_broker_adapter_stubs_cannot_submit_orders(
    adapter: SubmitOrderStub,
    expected_message: str,
) -> None:
    with pytest.raises(RuntimeError, match=expected_message):
        adapter.submit_order(_intent())


def test_non_trading_engine_modules_do_not_import_broker_adapters() -> None:
    violations: list[str] = []
    source_root = PROJECT_ROOT / "src" / "ai_trading_system"
    for path in source_root.rglob("*.py"):
        if _is_in_trading_engine(path):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("ai_trading_system.trading_engine.brokers"):
                        violations.append(f"{path}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith("ai_trading_system.trading_engine.brokers"):
                    imported = ", ".join(alias.name for alias in node.names)
                    violations.append(f"{path}: from {module} import {imported}")

    assert violations == []


def test_trading_daily_report_contains_production_effect_none() -> None:
    report = build_trading_daily_report(
        as_of=date(2026, 5, 17),
        order_intents=[],
        risk_results=[],
        submitted_orders=[],
        execution_reports=[],
        portfolio_state=PortfolioState(
            as_of=datetime(2026, 5, 17, 21, 0, tzinfo=UTC),
            cash_usd=100000.0,
            equity_value_usd=100000.0,
            gross_exposure_usd=0.0,
            net_exposure_usd=0.0,
        ),
        audit_root=PROJECT_ROOT / "data" / "trading_engine" / "audit",
    )

    assert report.production_effect == "none"
    assert "production_effect=none" in render_trading_daily_report(report)


def test_trading_engine_tests_do_not_read_real_broker_environment() -> None:
    forbidden_fragments = (
        "os." + "environ",
        "os." + "getenv",
        "environ" + "[",
        "getenv" + "(",
        "IBKR" + "_",
        "ALPACA" + "_",
        "API" + "_KEY",
    )
    violations = []
    for path in (PROJECT_ROOT / "tests" / "trading_engine").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for fragment in forbidden_fragments:
            if fragment in text:
                violations.append(f"{path}: {fragment}")

    assert violations == []


def _is_in_trading_engine(path: Path) -> bool:
    try:
        path.relative_to(PROJECT_ROOT / "src" / "ai_trading_system" / "trading_engine")
    except ValueError:
        return False
    return True


def _intent() -> OrderIntent:
    return OrderIntent(
        created_at=datetime(2026, 5, 17, 14, 0, tzinfo=UTC),
        strategy_id="safety_boundary_test",
        strategy_version="v1",
        run_id="run_2026_05_17",
        symbol="TSM",
        asset_type=AssetType.STOCK,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.DAY,
        target_notional_usd=1000.0,
        limit_price=100.0,
        confidence=0.75,
        score_snapshot_id="score_snapshot_1",
    )
