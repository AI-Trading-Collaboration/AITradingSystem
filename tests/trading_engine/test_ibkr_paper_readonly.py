from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.brokers.ibkr_readonly import (
    IBKRPaperReadOnlyAdapter,
    IBKRPaperReadOnlyConfig,
    IBKRPaperReadOnlyReconciliation,
    _ensure_asyncio_event_loop,
    load_ibkr_paper_readonly_config,
    mask_account_id,
)
from ai_trading_system.trading_engine.portfolio.paper_portfolio import PaperPortfolio
from ai_trading_system.trading_engine.schemas.order_intent import (
    AssetType,
    OrderIntent,
    OrderSide,
    OrderType,
    TimeInForce,
)
from scripts.run_ibkr_paper_readonly_snapshot import run_snapshot


def test_default_config_keeps_ibkr_paper_readonly_disabled() -> None:
    config = load_ibkr_paper_readonly_config(PROJECT_ROOT / "config" / "ibkr_paper_readonly.yaml")

    assert config.enabled is False
    assert config.trading_mode == "paper"
    assert config.readonly is True
    assert config.production_effect == "none"


def test_submit_order_raises_clear_readonly_error() -> None:
    adapter = IBKRPaperReadOnlyAdapter(config=_enabled_config(), client=MockClient())

    with pytest.raises(RuntimeError, match="cannot submit_order"):
        adapter.submit_order(_intent())


def test_trading_mode_non_paper_fails_closed_before_network() -> None:
    client = MockClient()
    adapter = IBKRPaperReadOnlyAdapter(
        config=_enabled_config(trading_mode="real"),
        client=client,
    )

    with pytest.raises(RuntimeError, match="trading_mode must be paper"):
        adapter.connect()

    assert client.connect_calls == 0


def test_readonly_false_fails_closed_before_network() -> None:
    client = MockClient()
    adapter = IBKRPaperReadOnlyAdapter(
        config=_enabled_config(readonly=False),
        client=client,
    )

    with pytest.raises(RuntimeError, match="readonly must be true"):
        adapter.connect()

    assert client.connect_calls == 0


def test_account_id_masking_hides_full_account_id() -> None:
    masked = mask_account_id("DUP1234567")

    assert masked == "DUP***4567"
    assert "DUP1234567" not in masked


def test_snapshot_json_keeps_production_effect_none_and_masks_account(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path, _enabled_config().model_dump())
    output_dir = tmp_path / "reports"
    client = MockClient()

    payload = run_snapshot(
        as_of=date(2026, 5, 17),
        config_path=config_path,
        output_dir=output_dir,
        client=client,
    )

    json_path = output_dir / "ibkr_paper_account_snapshot_2026-05-17.json"
    json_text = json_path.read_text(encoding="utf-8")
    snapshot = json.loads(json_text)
    assert payload["production_effect"] == "none"
    assert payload["readonly"] is True
    assert snapshot["production_effect"] == "none"
    assert snapshot["readonly"] is True
    assert snapshot["account_id_masked"] == "DUP***4567"
    assert "DUP1234567" not in json_text
    assert snapshot["connection_status"]["status"] == "CONNECTED"
    assert snapshot["contract_details_sample"]["symbol"] == "NVDA"
    assert client.connect_calls == 1


def test_snapshot_blocks_non_paper_account_without_network(tmp_path: Path) -> None:
    config = _enabled_config(account_id="U1234567")
    config_path = _write_config(tmp_path, config.model_dump())
    client = MockClient()

    payload = run_snapshot(
        as_of=date(2026, 5, 17),
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    assert payload["snapshot_status"] == "BLOCK"
    assert payload["connection_status"]["status"] == "BLOCKED_ACCOUNT_ID"
    assert client.connect_calls == 0


def test_disabled_snapshot_does_not_call_real_broker_network(tmp_path: Path) -> None:
    config = _enabled_config(enabled=False)
    config_path = _write_config(tmp_path, config.model_dump())
    client = MockClient()

    payload = run_snapshot(
        as_of=date(2026, 5, 17),
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    assert payload["connection_status"]["status"] == "DISABLED"
    assert client.connect_calls == 0


def test_reconciliation_scaffold_compares_symbol_quantity_and_cash() -> None:
    portfolio = PaperPortfolio(100000.0)
    portfolio.apply_fill(
        symbol="NVDA",
        side=OrderSide.BUY,
        quantity=3,
        price=100.0,
        fees=0.0,
    )

    result = IBKRPaperReadOnlyReconciliation().reconcile(
        account_summary=[{"tag": "TotalCashValue", "value": "99700", "currency": "USD"}],
        positions=[{"contract": {"symbol": "NVDA"}, "position": 3}],
        local_portfolio=portfolio,
    )

    assert result.status == "PASS"
    assert result.cash_summary_present is True
    assert result.compared_positions == 1


def test_reconciliation_accepts_ib_insync_sequence_rows() -> None:
    portfolio = PaperPortfolio(100000.0)
    portfolio.apply_fill(
        symbol="NVDA",
        side=OrderSide.BUY,
        quantity=3,
        price=100.0,
        fees=0.0,
    )

    result = IBKRPaperReadOnlyReconciliation().reconcile(
        account_summary=[["DUP***4567", "TotalCashValue", "99700", "USD", ""]],
        positions=[["DUP***4567", {"symbol": "NVDA"}, 3, 100.0]],
        local_portfolio=portfolio,
    )

    assert result.status == "PASS"
    assert result.cash_summary_present is True
    assert result.compared_positions == 1


def test_source_does_not_read_broker_credentials() -> None:
    forbidden_fragments = (
        "os." + "environ",
        "os." + "getenv",
        "environ" + "[",
        "get" + "env(",
    )
    source_paths = [
        PROJECT_ROOT
        / "src"
        / "ai_trading_system"
        / "trading_engine"
        / "brokers"
        / "ibkr_readonly.py",
        PROJECT_ROOT / "scripts" / "run_ibkr_paper_readonly_snapshot.py",
    ]
    violations: list[str] = []
    for path in source_paths:
        text = path.read_text(encoding="utf-8")
        for fragment in forbidden_fragments:
            if fragment in text:
                violations.append(f"{path}: {fragment}")

    assert violations == []


def test_ib_insync_client_bootstrap_creates_missing_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_loop = object()
    calls: list[object] = []

    def raise_missing_loop() -> None:
        raise RuntimeError("There is no current event loop in thread 'MainThread'.")

    monkeypatch.setattr(
        "ai_trading_system.trading_engine.brokers.ibkr_readonly.asyncio.get_event_loop",
        raise_missing_loop,
    )
    monkeypatch.setattr(
        "ai_trading_system.trading_engine.brokers.ibkr_readonly.asyncio.new_event_loop",
        lambda: created_loop,
    )
    monkeypatch.setattr(
        "ai_trading_system.trading_engine.brokers.ibkr_readonly.asyncio.set_event_loop",
        calls.append,
    )

    _ensure_asyncio_event_loop()

    assert calls == [created_loop]


class MockClient:
    def __init__(self) -> None:
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.connected = False

    def connect(
        self,
        host: str,
        port: int,
        clientId: int,
        readonly: bool,
        account: str,
    ) -> None:
        self.connect_calls += 1
        assert host == "127.0.0.1"
        assert port == 7497
        assert clientId == 19009
        assert readonly is True
        assert account == "DUP1234567"
        self.connected = True

    def isConnected(self) -> bool:
        return self.connected

    def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.connected = False

    def accountSummary(self, account: str) -> list[dict[str, Any]]:
        assert account == "DUP1234567"
        return [
            {
                "account": "DUP1234567",
                "tag": "TotalCashValue",
                "value": "100000",
                "currency": "USD",
            }
        ]

    def positions(self) -> list[dict[str, Any]]:
        return [
            {
                "account": "DUP1234567",
                "contract": {"symbol": "NVDA"},
                "position": 0,
                "avgCost": 0,
            }
        ]

    def openOrders(self) -> list[dict[str, Any]]:
        return []

    def executions(self) -> list[dict[str, Any]]:
        return [
            {
                "account": "DUP1234567",
                "execution_id": "paper_exec_1",
                "symbol": "NVDA",
                "shares": 0,
            }
        ]

    def reqContractDetails(self, contract: Any) -> list[dict[str, Any]]:
        symbol = contract["symbol"] if isinstance(contract, dict) else contract.symbol
        return [{"contract": {"symbol": symbol}, "minTick": 0.01}]


def _enabled_config(**overrides: Any) -> IBKRPaperReadOnlyConfig:
    values: dict[str, Any] = {
        "enabled": True,
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 19009,
        "account_id": "DUP1234567",
        "trading_mode": "paper",
        "readonly": True,
        "production_effect": "none",
    }
    values.update(overrides)
    return IBKRPaperReadOnlyConfig.model_validate(values)


def _write_config(tmp_path: Path, values: dict[str, Any]) -> Path:
    path = tmp_path / "ibkr_paper_readonly.yaml"
    path.write_text(yaml.safe_dump(values, sort_keys=False), encoding="utf-8")
    return path


def _intent() -> OrderIntent:
    return OrderIntent(
        strategy_id="ibkr_readonly_test",
        strategy_version="v1",
        run_id="run_2026_05_17",
        symbol="NVDA",
        asset_type=AssetType.STOCK,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.DAY,
        target_notional_usd=1000.0,
        limit_price=100.0,
        confidence=0.75,
        score_snapshot_id="score_snapshot_1",
    )
