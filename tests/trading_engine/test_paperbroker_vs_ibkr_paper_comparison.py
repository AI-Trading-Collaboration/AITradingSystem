from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.brokers.ibkr_paper_order import IBKRPaperOrderConfig
from ai_trading_system.trading_engine.brokers.paperbroker_ibkr_comparison import (
    DEFAULT_RECOMMENDATIONS,
    run_paperbroker_vs_ibkr_paper_comparison,
)

SOURCE_PATHS = [
    PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "trading_engine"
    / "brokers"
    / "paperbroker_ibkr_comparison.py",
    PROJECT_ROOT / "scripts" / "run_paperbroker_vs_ibkr_paper_comparison.py",
]


def test_non_paper_mode_fails_closed_before_network(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path, config_overrides={"trading_mode": "live"})

    assert payload["comparison_status"] == "BLOCK"
    assert _issue_codes(payload) == ["trading_mode"]
    assert client.connect_calls == 0
    assert payload["production_effect"] == "none"


def test_non_dup_account_fails_closed_before_network(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path, config_overrides={"account_id": "U1234567"})

    assert payload["comparison_status"] == "BLOCK"
    assert _issue_codes(payload) == ["account_id"]
    assert client.connect_calls == 0


def test_comparison_disabled_fails_closed_before_network(tmp_path: Path) -> None:
    payload, client = _run_with_mock(
        tmp_path,
        config_overrides={"ibkr_paper_comparison_enabled": False},
    )

    assert payload["comparison_status"] == "BLOCK"
    assert _issue_codes(payload) == ["ibkr_paper_comparison_enabled"]
    assert client.connect_calls == 0


def test_production_effect_non_none_fails_closed_before_network(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path, config_overrides={"production_effect": "orders"})

    assert payload["comparison_status"] == "BLOCK"
    assert _issue_codes(payload) == ["production_effect"]
    assert payload["production_effect"] == "none"
    assert payload["configured_production_effect"] == "orders"
    assert client.connect_calls == 0


@pytest.mark.parametrize(
    ("override", "issue_code"),
    [
        ({"order_type": "MKT"}, "order_type"),
        ({"asset_type": "option"}, "asset_type"),
        ({"time_in_force": "GTC"}, "time_in_force"),
        ({"quantity": 2}, "quantity"),
        ({"side": "SELL"}, "side"),
        ({"margin": True}, "margin"),
        ({"bracket_order": True}, "bracket_order"),
        ({"stop_price": 9.0}, "stop_order"),
        ({"algo_strategy": "Adaptive"}, "algo_order"),
    ],
)
def test_forbidden_order_shapes_are_rejected_before_connect(
    tmp_path: Path,
    override: dict[str, Any],
    issue_code: str,
) -> None:
    payload, client = _run_with_mock(tmp_path, **override)

    assert payload["comparison_status"] == "BLOCK"
    assert _issue_codes(payload) == [issue_code]
    assert client.connect_calls == 0
    assert client.place_order_calls == 0


def test_local_accepted_and_ibkr_cancelled_normal_path(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path)

    assert payload["comparison_status"] == "PASS"
    assert payload["comparison_mode"] == "diagnostic_only"
    assert payload["production_effect"] == "none"
    assert payload["local"]["local_order_status"] == "SUBMITTED"
    assert payload["local"]["local_open_order_seen"] is True
    assert payload["local"]["local_fill_seen"] is False
    assert payload["local"]["local_cancel_result"] == "CANCELLED"
    assert payload["local"]["local_final_status"] == "CANCELLED"
    assert payload["local"]["local_reconciliation_status"] == "PASS"
    assert payload["ibkr"]["broker_order_id"] == "[REDACTED_BROKER_ORDER_ID:len=3]"
    assert [event["status"] for event in payload["ibkr"]["order_status_events"]] == [
        "Submitted",
        "Cancelled",
    ]
    assert payload["ibkr"]["open_order_seen"] is True
    assert payload["ibkr"]["cancel_requested"] is True
    assert payload["ibkr"]["final_status"] == "Cancelled"
    assert payload["ibkr"]["fills_seen"] is False
    assert payload["ibkr"]["ibkr_reconciliation_status"] == "PASS"
    assert payload["diff"]["status_match"] is True
    assert payload["diff"]["fill_match"] is True
    assert payload["diff"]["cancel_match"] is True
    assert payload["diff"]["lifecycle_event_gap"] == []
    assert payload["difference_labels"] == []
    assert payload["recommendations"] == DEFAULT_RECOMMENDATIONS
    assert client.connect_calls == 1
    assert client.place_order_calls == 1
    assert client.cancel_order_calls == 1


def test_ibkr_rejected_outputs_broker_rejected_difference(tmp_path: Path) -> None:
    payload, _client = _run_with_mock(
        tmp_path,
        client=MockComparisonClient(initial_status="Rejected"),
    )

    assert payload["comparison_status"] == "DIAGNOSTIC_DIFFERENCE"
    assert payload["ibkr"]["final_status"] == "Rejected"
    assert payload["diff"]["ibkr_rejected_but_local_accepted"] is True
    assert "BROKER_REJECTED" in payload["difference_labels"]
    assert "EXPECTED_DIFFERENCE" in payload["difference_labels"]


def test_local_filled_but_ibkr_not_filled_marks_local_sim_too_optimistic(
    tmp_path: Path,
) -> None:
    payload, _client = _run_with_mock(
        tmp_path,
        local_snapshot={"open": 9.5, "high": 11.0, "low": 9.0, "last": 10.0},
        local_price_source="provided_test_touching_limit_snapshot",
    )

    assert payload["comparison_status"] == "DIAGNOSTIC_DIFFERENCE"
    assert payload["local"]["local_fill_seen"] is True
    assert payload["ibkr"]["fills_seen"] is False
    assert payload["diff"]["local_filled_but_ibkr_not_filled"] is True
    assert payload["diff"]["local_price_source"] == "provided_test_touching_limit_snapshot"
    assert "LOCAL_SIM_TOO_OPTIMISTIC" in payload["difference_labels"]


def test_account_id_and_broker_order_id_are_redacted_in_outputs(tmp_path: Path) -> None:
    payload, _client = _run_with_mock(tmp_path)
    json_text = Path(payload["output_paths"]["json"]).read_text(encoding="utf-8")
    markdown_text = Path(payload["output_paths"]["markdown"]).read_text(encoding="utf-8")

    assert payload["account_id_masked"] == "DUP***4567"
    assert "DUP1234567" not in json_text
    assert "DUP1234567" not in markdown_text
    assert re.search(r"\bDUP\d{5,}\b", json_text, flags=re.IGNORECASE) is None
    assert re.search(r"\bDUP\d{5,}\b", markdown_text, flags=re.IGNORECASE) is None
    assert re.search(r'"broker_order_id"\s*:\s*"\d+', json_text) is None
    assert re.search(r"broker_order_id：`\d+`", markdown_text) is None
    assert '"broker_order_id": "101"' not in json_text
    assert "broker_order_id：`101`" not in markdown_text


def test_intent_fixture_input_is_supported(tmp_path: Path) -> None:
    fixture_path = tmp_path / "order_intent.json"
    fixture_path.write_text(
        json.dumps(
            {
                "order_intent": {
                    "symbol": "AAPL",
                    "asset_type": "stock",
                    "side": "BUY",
                    "order_type": "LIMIT",
                    "time_in_force": "DAY",
                    "target_quantity": 1,
                    "limit_price": 10.0,
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    payload, _client = _run_with_mock(
        tmp_path,
        intent_fixture_path=fixture_path,
        client=MockComparisonClient(expected_symbol="AAPL"),
        symbol="NVDA",
    )

    assert payload["comparison_status"] == "PASS"
    assert payload["requested_order"]["input_source"] == "order_intent_fixture"
    assert payload["order_intent"]["symbol"] == "AAPL"


def test_source_does_not_read_api_key_or_environment_secrets() -> None:
    forbidden_fragments = (
        "os." + "environ",
        "os." + "getenv",
        "environ" + "[",
        "get" + "env(",
    )
    violations: list[str] = []
    for path in SOURCE_PATHS:
        text = path.read_text(encoding="utf-8")
        for fragment in forbidden_fragments:
            if fragment in text:
                violations.append(f"{path}: {fragment}")

    assert violations == []


def test_source_does_not_trigger_daily_replay_dashboard_or_live_paths() -> None:
    forbidden_fragments = (
        "run_paper_trading_replay",
        "run_paper_trading_from_candidates",
        "from ai_trading_system.trading_engine.reports.paper_signal_quality",
        "from ai_trading_system.trading_engine.reports.shadow_parameter_impact",
        "daily_task_dashboard",
        "evidence_dashboard",
        "Alpaca",
        "alpaca",
        "real_trading_enabled",
    )
    violations: list[str] = []
    for path in SOURCE_PATHS:
        text = path.read_text(encoding="utf-8")
        for fragment in forbidden_fragments:
            if fragment in text:
                violations.append(f"{path}: {fragment}")

    assert violations == []


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
    def __init__(self, order_id: int, status: str) -> None:
        self.order = MockOrder(order_id)
        self.orderStatus = MockOrderStatus(status)
        self.fills: list[dict[str, Any]] = []


class MockComparisonClient:
    def __init__(
        self,
        *,
        expected_symbol: str = "NVDA",
        initial_status: str = "Submitted",
        order_id: int = 101,
    ) -> None:
        self.expected_symbol = expected_symbol
        self.initial_status = initial_status
        self.order_id = order_id
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.place_order_calls = 0
        self.cancel_order_calls = 0
        self.connected = False
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
        return []

    def placeOrder(self, contract: Any, order: Any) -> MockTrade:
        self.place_order_calls += 1
        symbol = contract["symbol"] if isinstance(contract, dict) else contract.symbol
        assert symbol == self.expected_symbol
        action = order["action"] if isinstance(order, dict) else order.action
        tif = order["tif"] if isinstance(order, dict) else order.tif
        order_type = order["orderType"] if isinstance(order, dict) else order.orderType
        assert action == "BUY"
        assert tif == "DAY"
        assert order_type in {"LMT", "LIMIT"}
        self.trade = MockTrade(self.order_id, self.initial_status)
        return self.trade

    def waitOnUpdate(self, timeout: float) -> None:
        assert timeout >= 0

    def openTrades(self) -> list[dict[str, Any]]:
        if self.trade is None:
            return []
        if self.trade.orderStatus.status != "Submitted":
            return []
        return [{"order": {"orderId": self.trade.order.orderId}, "status": "Submitted"}]

    def cancelOrder(self, order: Any) -> None:
        self.cancel_order_calls += 1
        assert order is not None
        if self.trade is not None and self.trade.orderStatus.status == "Submitted":
            self.trade.orderStatus.status = "Cancelled"
            self.trade.orderStatus.remaining = 1


def _enabled_config(**overrides: Any) -> IBKRPaperOrderConfig:
    values: dict[str, Any] = {
        "paper_order_lifecycle_enabled": True,
        "ibkr_paper_comparison_enabled": True,
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
        "cancel_timeout_seconds": 0.0,
    }
    values.update(overrides)
    return IBKRPaperOrderConfig.model_validate(values)


def _write_config(tmp_path: Path, values: dict[str, Any]) -> Path:
    path = tmp_path / "ibkr_paper_order.yaml"
    path.write_text(yaml.safe_dump(values, sort_keys=False), encoding="utf-8")
    return path


def _run_with_mock(
    tmp_path: Path,
    *,
    config_overrides: dict[str, Any] | None = None,
    client: MockComparisonClient | None = None,
    **overrides: Any,
) -> tuple[dict[str, Any], MockComparisonClient]:
    config_path = _write_config(
        tmp_path,
        _enabled_config(**(config_overrides or {})).model_dump(),
    )
    mock_client = client or MockComparisonClient()
    payload = run_paperbroker_vs_ibkr_paper_comparison(
        as_of=date(2026, 5, 17),
        config_path=config_path,
        output_dir=tmp_path / "reports",
        client=mock_client,
        symbol=overrides.pop("symbol", "NVDA"),
        side=overrides.pop("side", "BUY"),
        quantity=overrides.pop("quantity", 1),
        limit_price=overrides.pop("limit_price", 10.0),
        asset_type=overrides.pop("asset_type", "stock"),
        order_type=overrides.pop("order_type", "LIMIT"),
        time_in_force=overrides.pop("time_in_force", "DAY"),
        **overrides,
    )
    return payload, mock_client


def _issue_codes(payload: dict[str, Any]) -> list[str]:
    return [issue["code"] for issue in payload["issues"]]
