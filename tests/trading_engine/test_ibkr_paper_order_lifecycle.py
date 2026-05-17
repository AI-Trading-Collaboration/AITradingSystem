from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.brokers.ibkr_paper_order import (
    IBKRPaperOrderConfig,
    IBKRPaperOrderLifecycleAdapter,
    load_ibkr_paper_order_config,
)
from scripts.run_ibkr_paper_order_lifecycle import run_order_lifecycle

LOCAL_VALIDATION_REPORT = (
    PROJECT_ROOT / "docs" / "reviews" / "ibkr_paper_order_lifecycle_local_validation_2026-05-17.md"
)
SANITIZED_LIFECYCLE_FIXTURE = (
    PROJECT_ROOT / "tests" / "fixtures" / "ibkr_paper_order_lifecycle_sanitized.json"
)
FORBIDDEN_PRODUCTION_SEMANTICS = (
    "READY_FOR_LIVE",
    "SHOULD_TRADE",
    "PROMOTE_TO_PRODUCTION",
)
SECRET_PATTERNS = (
    r"sk-[A-Za-z0-9]{8,}",
    r"api[_ -]?key",
    r"password",
    r"token",
)


def test_default_config_keeps_ibkr_paper_order_lifecycle_disabled() -> None:
    config = load_ibkr_paper_order_config(PROJECT_ROOT / "config" / "ibkr_paper_order.yaml")

    assert config.paper_order_lifecycle_enabled is False
    assert config.trading_mode == "paper"
    assert config.production_effect == "none"
    assert config.max_quantity == 1


def test_non_paper_mode_fails_closed_before_network() -> None:
    client = MockOrderClient()
    adapter = IBKRPaperOrderLifecycleAdapter(
        config=_enabled_config(trading_mode="live"),
        client=client,
    )

    with pytest.raises(RuntimeError, match="trading_mode must be paper"):
        adapter.connect()

    assert client.connect_calls == 0


def test_non_dup_account_fails_closed_before_network() -> None:
    client = MockOrderClient()
    adapter = IBKRPaperOrderLifecycleAdapter(
        config=_enabled_config(account_id="U1234567"),
        client=client,
    )

    with pytest.raises(RuntimeError, match="account_id must start with DUP"):
        adapter.connect()

    assert client.connect_calls == 0


def test_production_effect_non_none_fails_closed_before_network() -> None:
    client = MockOrderClient()
    adapter = IBKRPaperOrderLifecycleAdapter(
        config=_enabled_config(production_effect="orders"),
        client=client,
    )

    with pytest.raises(RuntimeError, match="production_effect must be none"):
        adapter.connect()

    assert client.connect_calls == 0


def test_lifecycle_disabled_fails_closed_before_network() -> None:
    client = MockOrderClient()
    adapter = IBKRPaperOrderLifecycleAdapter(
        config=_enabled_config(paper_order_lifecycle_enabled=False),
        client=client,
    )

    with pytest.raises(RuntimeError, match="paper_order_lifecycle_enabled must be true"):
        adapter.connect()

    assert client.connect_calls == 0


def test_market_order_is_rejected_without_submit(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path, order_type="MKT")

    assert payload["lifecycle_status"] == "BLOCK"
    assert _issue_codes(payload) == ["order_type"]
    assert client.place_order_calls == 0
    assert payload["production_effect"] == "none"


def test_option_order_is_rejected_without_submit(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path, asset_type="option")

    assert payload["lifecycle_status"] == "BLOCK"
    assert _issue_codes(payload) == ["asset_type"]
    assert client.place_order_calls == 0


def test_gtc_order_is_rejected_without_submit(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path, time_in_force="GTC")

    assert payload["lifecycle_status"] == "BLOCK"
    assert _issue_codes(payload) == ["time_in_force"]
    assert client.place_order_calls == 0


def test_quantity_above_cap_is_rejected_without_submit(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path, quantity=2)

    assert payload["lifecycle_status"] == "BLOCK"
    assert _issue_codes(payload) == ["quantity"]
    assert client.place_order_calls == 0


def test_symbol_outside_whitelist_is_rejected_without_submit(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path, symbol="MSFT")

    assert payload["lifecycle_status"] == "BLOCK"
    assert _issue_codes(payload) == ["symbol_whitelist"]
    assert client.place_order_calls == 0


def test_short_sell_is_rejected_without_submit(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, _enabled_config().model_dump())
    client = MockOrderClient(position_quantity=0)

    payload = run_order_lifecycle(
        as_of=date(2026, 5, 17),
        config_path=config_path,
        output_dir=tmp_path / "reports",
        client=client,
        symbol="NVDA",
        side="SELL",
        quantity=1,
        limit_price=1000.0,
    )

    assert payload["lifecycle_status"] == "BLOCK"
    assert _issue_codes(payload) == ["no_short_sell"]
    assert client.place_order_calls == 0


def test_explicit_margin_order_is_rejected_without_submit(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path, margin=True)

    assert payload["lifecycle_status"] == "BLOCK"
    assert _issue_codes(payload) == ["margin"]
    assert client.place_order_calls == 0


def test_submit_open_cancel_cancelled_normal_path(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path)

    assert payload["lifecycle_status"] == "PASS"
    assert payload["connection_status"]["status"] == "CONNECTED"
    assert payload["account_id_masked"] == "DUP***4567"
    assert payload["submitted_order"]["symbol"] == "NVDA"
    assert payload["submitted_order"]["order_type"] == "LIMIT"
    assert payload["submitted_order"]["time_in_force"] == "DAY"
    assert payload["broker_order_id"] == "101"
    assert [event["status"] for event in payload["order_status_events"]] == [
        "Submitted",
        "Cancelled",
    ]
    assert payload["open_order_seen"] is True
    assert payload["cancel_requested"] is True
    assert payload["final_order_status"] == "Cancelled"
    assert payload["cancelled_confirmed"] is True
    assert payload["fills_seen"] is False
    assert client.connect_calls == 1
    assert client.place_order_calls == 1
    assert client.cancel_order_calls == 1


def test_submit_then_status_exception_still_generates_error_report(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, _enabled_config().model_dump())
    output_dir = tmp_path / "reports"
    client = MockOrderClient(raise_on_wait=True)

    payload = run_order_lifecycle(
        as_of=date(2026, 5, 17),
        config_path=config_path,
        output_dir=output_dir,
        client=client,
        symbol="NVDA",
        side="BUY",
        quantity=1,
        limit_price=10.0,
    )

    assert payload["lifecycle_status"] == "ERROR"
    assert payload["broker_order_id"] == "101"
    assert payload["cancel_requested"] is True
    assert client.place_order_calls == 1
    assert client.cancel_order_calls == 1
    assert (output_dir / "ibkr_paper_order_lifecycle_2026-05-17.json").exists()
    assert (output_dir / "ibkr_paper_order_lifecycle_2026-05-17.md").exists()


def test_reference_price_unavailable_returns_limited_without_submit(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, _enabled_config().model_dump())
    client = MockOrderClient(reference_price=None)

    payload = run_order_lifecycle(
        as_of=date(2026, 5, 17),
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
        symbol="NVDA",
        side="BUY",
        quantity=1,
        limit_price=None,
    )

    assert payload["lifecycle_status"] == "LIMITED"
    assert _issue_codes(payload) == ["reference_price_unavailable"]
    assert client.place_order_calls == 0


def test_account_id_is_masked_in_json_and_markdown(tmp_path: Path) -> None:
    payload, _client = _run_with_mock(tmp_path)
    json_path = Path(payload["output_paths"]["json"])
    markdown_path = Path(payload["output_paths"]["markdown"])
    json_text = json_path.read_text(encoding="utf-8")
    markdown_text = markdown_path.read_text(encoding="utf-8")

    assert payload["account_id_masked"] == "DUP***4567"
    assert "DUP1234567" not in json_text
    assert "DUP1234567" not in markdown_text
    assert re.search(r"\bDUP\d{5,}\b", json_text, flags=re.IGNORECASE) is None
    assert re.search(r"\bDUP\d{5,}\b", markdown_text, flags=re.IGNORECASE) is None


def test_source_does_not_read_broker_api_key_or_secrets() -> None:
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
        / "ibkr_paper_order.py",
        PROJECT_ROOT / "scripts" / "run_ibkr_paper_order_lifecycle.py",
    ]
    violations: list[str] = []
    for path in source_paths:
        text = path.read_text(encoding="utf-8")
        for fragment in forbidden_fragments:
            if fragment in text:
                violations.append(f"{path}: {fragment}")

    assert violations == []


def test_disabled_report_does_not_call_live_broker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_config(
        tmp_path,
        _enabled_config(paper_order_lifecycle_enabled=False).model_dump(),
    )

    def fail_if_called() -> None:
        raise AssertionError("live broker client factory must not be called")

    monkeypatch.setattr(
        "ai_trading_system.trading_engine.brokers.ibkr_paper_order._create_ibkr_client",
        fail_if_called,
    )

    payload = run_order_lifecycle(
        as_of=date(2026, 5, 17),
        config_path=config_path,
        output_dir=tmp_path,
        symbol="NVDA",
        side="BUY",
        quantity=1,
        limit_price=10.0,
    )

    assert payload["lifecycle_status"] == "BLOCK"
    assert payload["connection_status"]["status"] == "NOT_RUN"


def test_sanitized_lifecycle_fixture_omits_full_dup_account_and_order_id() -> None:
    text = SANITIZED_LIFECYCLE_FIXTURE.read_text(encoding="utf-8")
    payload = json.loads(text)

    assert payload["connection"]["account_id_masked"] == "DUP***0000"
    assert re.search(r"\bDUP\d{5,}\b", text, flags=re.IGNORECASE) is None
    assert re.search(r'"broker_order_id"\s*:\s*"\d+', text) is None
    assert payload["submitted_order"]["broker_order_id_redaction"].startswith("[REDACTED")
    assert payload["production_effect"] == "none"
    assert payload["paper_only"] is True


def test_sanitized_lifecycle_fixture_contains_no_secret_values() -> None:
    text = SANITIZED_LIFECYCLE_FIXTURE.read_text(encoding="utf-8")

    for pattern in SECRET_PATTERNS:
        assert re.search(pattern, text, flags=re.IGNORECASE) is None


def test_local_validation_report_records_production_effect_none() -> None:
    text = LOCAL_VALIDATION_REPORT.read_text(encoding="utf-8")

    assert "production_effect=none" in text


def test_local_validation_report_contains_no_secret_values() -> None:
    text = LOCAL_VALIDATION_REPORT.read_text(encoding="utf-8")

    for pattern in SECRET_PATTERNS:
        assert re.search(pattern, text, flags=re.IGNORECASE) is None


def test_local_validation_report_is_explicitly_paper_only() -> None:
    text = LOCAL_VALIDATION_REPORT.read_text(encoding="utf-8")

    assert "paper-only=true" in text
    assert "TWS Paper Trading" in text
    assert "IB Gateway Paper Trading" in text
    assert "daily-run" in text
    assert "replay" in text
    assert "dashboard" in text


def test_local_validation_report_avoids_live_trade_promotion_semantics() -> None:
    text = LOCAL_VALIDATION_REPORT.read_text(encoding="utf-8")

    for phrase in FORBIDDEN_PRODUCTION_SEMANTICS:
        assert phrase not in text


class MockOrder:
    def __init__(self, order_id: int) -> None:
        self.orderId = order_id
        self.permId = order_id + 1000
        self.action = "BUY"
        self.orderType = "LMT"
        self.totalQuantity = 1
        self.lmtPrice = 10.0
        self.tif = "DAY"


class MockOrderStatus:
    def __init__(self, status: str = "Submitted") -> None:
        self.status = status
        self.filled = 0
        self.remaining = 1
        self.avgFillPrice = 0.0


class MockTrade:
    def __init__(self, order_id: int = 101) -> None:
        self.order = MockOrder(order_id)
        self.orderStatus = MockOrderStatus()
        self.fills: list[dict[str, Any]] = []


class MockOrderClient:
    def __init__(
        self,
        *,
        raise_on_wait: bool = False,
        reference_price: float | None = 500.0,
        position_quantity: int = 1,
    ) -> None:
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.place_order_calls = 0
        self.cancel_order_calls = 0
        self.wait_calls = 0
        self.connected = False
        self.raise_on_wait = raise_on_wait
        self.reference_price = reference_price
        self.position_quantity = position_quantity
        self.trade: MockTrade | None = None

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
        assert clientId == 19010
        assert readonly is False
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
                "position": self.position_quantity,
                "avgCost": 20,
            }
        ]

    def get_reference_price(self, symbol: str) -> float | None:
        assert symbol == "NVDA"
        return self.reference_price

    def placeOrder(self, contract: Any, order: Any) -> MockTrade:
        self.place_order_calls += 1
        symbol = contract["symbol"] if isinstance(contract, dict) else contract.symbol
        assert symbol == "NVDA"
        action = order["action"] if isinstance(order, dict) else order.action
        tif = order["tif"] if isinstance(order, dict) else order.tif
        order_type = order["orderType"] if isinstance(order, dict) else order.orderType
        assert action == "BUY"
        assert tif == "DAY"
        assert order_type in {"LMT", "LIMIT"}
        self.trade = MockTrade()
        return self.trade

    def waitOnUpdate(self, timeout: float) -> None:
        assert timeout >= 0
        self.wait_calls += 1
        if self.raise_on_wait:
            raise RuntimeError("paper order status stream failed")

    def openTrades(self) -> list[dict[str, Any]]:
        if self.trade is None or self.trade.orderStatus.status == "Cancelled":
            return []
        return [{"order": {"orderId": self.trade.order.orderId}, "status": "Submitted"}]

    def cancelOrder(self, order: Any) -> None:
        self.cancel_order_calls += 1
        assert order is not None
        if self.trade is not None:
            self.trade.orderStatus.status = "Cancelled"
            self.trade.orderStatus.remaining = 1


def _enabled_config(**overrides: Any) -> IBKRPaperOrderConfig:
    values: dict[str, Any] = {
        "paper_order_lifecycle_enabled": True,
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 19010,
        "account_id": "DUP1234567",
        "trading_mode": "paper",
        "production_effect": "none",
        "allowed_symbols": ["NVDA", "AAPL", "TSM"],
        "max_quantity": 1,
        "far_from_market_pct": 0.5,
        "status_timeout_seconds": 0.0,
        "cancel_timeout_seconds": 0.25,
    }
    values.update(overrides)
    return IBKRPaperOrderConfig.model_validate(values)


def _write_config(tmp_path: Path, values: dict[str, Any]) -> Path:
    path = tmp_path / "ibkr_paper_order.yaml"
    path.write_text(yaml.safe_dump(values, sort_keys=False), encoding="utf-8")
    return path


def _run_with_mock(tmp_path: Path, **overrides: Any) -> tuple[dict[str, Any], MockOrderClient]:
    config_path = _write_config(tmp_path, _enabled_config().model_dump())
    client = MockOrderClient()
    payload = run_order_lifecycle(
        as_of=date(2026, 5, 17),
        config_path=config_path,
        output_dir=tmp_path / "reports",
        client=client,
        symbol=overrides.pop("symbol", "NVDA"),
        side=overrides.pop("side", "BUY"),
        quantity=overrides.pop("quantity", 1),
        limit_price=overrides.pop("limit_price", 10.0),
        asset_type=overrides.pop("asset_type", "stock"),
        order_type=overrides.pop("order_type", "LIMIT"),
        time_in_force=overrides.pop("time_in_force", "DAY"),
        **overrides,
    )
    return payload, client


def _issue_codes(payload: dict[str, Any]) -> list[str]:
    return [issue["code"] for issue in payload["issues"]]
