from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.brokers.ibkr_paper_controlled_fill import (
    ControlledFillConfig,
    _assert_no_sensitive_output,
    load_controlled_fill_config,
    run_ibkr_paper_controlled_fill_test,
)

SOURCE_PATHS = [
    PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "trading_engine"
    / "brokers"
    / "ibkr_paper_controlled_fill.py",
    PROJECT_ROOT / "scripts" / "run_ibkr_paper_controlled_fill_test.py",
]
CONTROLLED_FILL_NO_FILL_FIXTURE = (
    PROJECT_ROOT / "tests" / "fixtures" / "ibkr_paper_controlled_fill_no_fill_sanitized.json"
)
CONTROLLED_FILL_LOCAL_VALIDATION_REVIEW = (
    PROJECT_ROOT / "docs" / "reviews" / "ibkr_paper_controlled_fill_local_validation_2026-05-17.md"
)
CONTROLLED_FILL_RUNBOOK = (
    PROJECT_ROOT / "docs" / "runbooks" / "ibkr_paper_controlled_fill_local_validation.md"
)
FORBIDDEN_PRODUCTION_SEMANTICS = (
    "READY_FOR_LIVE",
    "SHOULD_TRADE",
    "PROMOTE_TO_PRODUCTION",
)
SENSITIVE_VALUE_PATTERNS = (
    r"sk-[A-Za-z0-9]{8,}",
    r"(?i)authorization:\s*bearer",
    r"(?i)\"api[_ -]?key\"\s*:",
    r"(?i)\"password\"\s*:",
    r"(?i)\"token\"\s*:",
    r"(?i)session[_ -]?cookie\s*[:=]",
)


def test_default_controlled_fill_config_fails_closed() -> None:
    config = load_controlled_fill_config(
        PROJECT_ROOT / "config" / "ibkr_paper_controlled_fill.yaml"
    )

    assert config.controlled_fill_enabled is False
    assert config.trading_mode == "paper"
    assert config.production_effect == "none"
    assert config.max_quantity == 1
    assert config.allowed_symbols == ["NVDA", "AAPL", "TSM"]
    assert config.allow_market_order is False
    assert config.allow_sell is False
    assert config.allow_option is False
    assert config.allow_margin is False
    assert config.allow_short is False
    assert config.require_manual_limit_price is True


def test_controlled_fill_disabled_fails_closed_before_network(tmp_path: Path) -> None:
    payload, client = _run_with_mock(
        tmp_path,
        config_overrides={"controlled_fill_enabled": False},
    )

    assert payload["test_status"] == "BLOCK"
    assert _issue_codes(payload) == ["controlled_fill_enabled"]
    assert client.connect_calls == 0
    assert client.place_order_calls == 0


def test_non_paper_mode_fails_closed_before_network(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path, config_overrides={"trading_mode": "live"})

    assert payload["test_status"] == "BLOCK"
    assert _issue_codes(payload) == ["trading_mode"]
    assert client.connect_calls == 0


def test_non_dup_account_fails_closed_before_network(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path, config_overrides={"account_id": "U1234567"})

    assert payload["test_status"] == "BLOCK"
    assert _issue_codes(payload) == ["account_id"]
    assert client.connect_calls == 0


def test_production_effect_non_none_fails_closed_before_network(tmp_path: Path) -> None:
    payload, client = _run_with_mock(
        tmp_path,
        config_overrides={"production_effect": "orders"},
    )

    assert payload["test_status"] == "BLOCK"
    assert _issue_codes(payload) == ["production_effect"]
    assert payload["production_effect"] == "none"
    assert payload["configured_production_effect"] == "orders"
    assert client.connect_calls == 0


@pytest.mark.parametrize(
    ("override", "issue_code"),
    [
        ({"side": "SELL"}, "side"),
        ({"order_type": "MARKET"}, "order_type"),
        ({"quantity": 2}, "quantity"),
    ],
)
def test_forbidden_order_inputs_are_rejected_before_connect(
    tmp_path: Path,
    override: dict[str, Any],
    issue_code: str,
) -> None:
    payload, client = _run_with_mock(tmp_path, **override)

    assert payload["test_status"] == "BLOCK"
    assert _issue_codes(payload) == [issue_code]
    assert client.connect_calls == 0
    assert client.place_order_calls == 0


def test_fill_seen_true_outputs_fill_report_and_local_comparison(tmp_path: Path) -> None:
    payload, client = _run_with_mock(
        tmp_path,
        client=MockControlledFillClient(initial_status="Filled", include_fill=True),
        market_snapshot={
            "open": 401.25,
            "high": 402.0,
            "low": 401.0,
            "last": 401.5,
        },
        limit_price=401.25,
    )

    assert payload["test_status"] == "PASS"
    assert payload["production_effect"] == "none"
    assert payload["trading_mode"] == "paper"
    assert payload["symbol"] == "NVDA"
    assert payload["side"] == "BUY"
    assert payload["quantity"] == 1
    assert payload["limit_price"] == 401.25
    assert payload["fill_seen"] is True
    assert payload["fill_quantity"] == 1.0
    assert payload["avg_fill_price"] == 401.25
    assert payload["fill_time"] == "2026-05-17T14:31:00Z"
    assert payload["commission_report_seen"] is True
    assert payload["cancel_requested"] is False
    assert payload["final_order_status"] == "Filled"
    assert payload["paperbroker_comparison"]["local_fill_seen"] is True
    assert payload["paperbroker_comparison"]["local_avg_fill_price"] == 401.25
    assert payload["paperbroker_comparison"]["ibkr_fill_seen"] is True
    assert payload["paperbroker_comparison"]["ibkr_avg_fill_price"] == 401.25
    assert payload["paperbroker_comparison"]["fill_price_diff"] == 0.0
    assert payload["paperbroker_comparison"]["fill_match_status"] == "EXACT_MATCH"
    assert client.connect_calls == 1
    assert client.place_order_calls == 1
    assert client.cancel_order_calls == 0
    assert Path(payload["output_paths"]["json"]).exists()
    assert Path(payload["output_paths"]["markdown"]).exists()


def test_fill_seen_false_cancels_and_outputs_limited(tmp_path: Path) -> None:
    payload, client = _run_with_mock(tmp_path)

    assert payload["test_status"] == "LIMITED"
    assert payload["fill_seen"] is False
    assert payload["fill_quantity"] == 0.0
    assert payload["cancel_requested"] is True
    assert payload["final_order_status"] == "Cancelled"
    assert payload["paperbroker_comparison"]["fill_match_status"] == "NOT_RUN"
    assert payload["production_surface_impact"]["paperbroker_fill_model"] == "none"
    assert "fill_not_seen_timeout" in _issue_codes(payload)
    assert client.cancel_order_calls == 1
    assert Path(payload["output_paths"]["json"]).exists()
    assert Path(payload["output_paths"]["markdown"]).exists()


def test_missing_market_snapshot_marks_insufficient_market_data(tmp_path: Path) -> None:
    missing_prices_path = tmp_path / "missing_prices_daily.csv"
    payload, _client = _run_with_mock(
        tmp_path,
        client=MockControlledFillClient(initial_status="Filled", include_fill=True),
        limit_price=401.25,
        prices_path=missing_prices_path,
    )

    assert payload["fill_seen"] is True
    assert payload["test_status"] == "LIMITED"
    assert payload["paperbroker_comparison"]["fill_match_status"] == "INSUFFICIENT_MARKET_DATA"
    assert payload["paperbroker_comparison"]["market_snapshot_source"] == (
        "missing_reliable_market_snapshot"
    )
    assert "insufficient_market_data" in _issue_codes(payload)


def test_account_id_and_broker_order_id_are_redacted_in_outputs(tmp_path: Path) -> None:
    payload, _client = _run_with_mock(
        tmp_path,
        client=MockControlledFillClient(initial_status="Filled", include_fill=True),
        market_snapshot={
            "open": 401.25,
            "high": 402.0,
            "low": 401.0,
            "last": 401.5,
        },
        limit_price=401.25,
    )
    json_text = Path(payload["output_paths"]["json"]).read_text(encoding="utf-8")
    markdown_text = Path(payload["output_paths"]["markdown"]).read_text(encoding="utf-8")

    assert payload["account_id_masked"] == "DUP***4567"
    assert payload["broker_order_id"] == "[REDACTED_BROKER_ORDER_ID:len=3]"
    assert "DUP1234567" not in json_text
    assert "DUP1234567" not in markdown_text
    assert re.search(r"\bDUP\d{5,}\b", json_text, flags=re.IGNORECASE) is None
    assert re.search(r"\bDUP\d{5,}\b", markdown_text, flags=re.IGNORECASE) is None
    assert re.search(r'"broker_order_id"\s*:\s*"\d+', json_text) is None
    assert re.search(r'"orderId"\s*:\s*101', json_text) is None
    assert "broker_order_id：`101`" not in markdown_text


def test_short_broker_order_id_does_not_conflict_with_non_sensitive_numbers(
    tmp_path: Path,
) -> None:
    payload, _client = _run_with_mock(
        tmp_path,
        client=MockControlledFillClient(initial_status="Filled", include_fill=True, order_id=1),
        market_snapshot={
            "open": 401.25,
            "high": 402.0,
            "low": 401.0,
            "last": 401.5,
        },
        limit_price=401.25,
    )
    json_text = Path(payload["output_paths"]["json"]).read_text(encoding="utf-8")
    markdown_text = Path(payload["output_paths"]["markdown"]).read_text(encoding="utf-8")

    assert payload["broker_order_id"] == "[REDACTED_BROKER_ORDER_ID:len=1]"
    assert payload["as_of"] == "2026-05-17"
    assert '"as_of": "2026-05-17"' in json_text
    assert '"quantity": 1' in json_text
    assert '"broker_order_id": "[REDACTED_BROKER_ORDER_ID:len=1]"' in json_text
    assert re.search(r'"broker_order_id"\s*:\s*"?1"?', json_text) is None
    assert re.search(r'"orderId"\s*:\s*1\b', json_text) is None
    assert "broker_order_id：`1`" not in markdown_text


def test_sensitive_output_check_keeps_short_broker_id_contextual() -> None:
    _assert_no_sensitive_output(
        '{"quantity": 1, "as_of": "2026-05-17", '
        '"broker_order_id": "[REDACTED_BROKER_ORDER_ID:len=1]"}',
        account_id="DUP1234567",
        raw_broker_order_ids=["1"],
    )

    with pytest.raises(RuntimeError, match="unredacted broker order id"):
        _assert_no_sensitive_output(
            '{"broker_order_id": 1}',
            account_id="DUP1234567",
            raw_broker_order_ids=["1"],
        )


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


def test_source_does_not_trigger_daily_run_replay_or_dashboard() -> None:
    forbidden_fragments = (
        "run_paper_trading_replay",
        "run_paper_trading_from_candidates",
        "daily_task_dashboard",
        "evidence_dashboard",
        "from ai_trading_system.trading_engine.reports.paper_signal_quality",
        "from ai_trading_system.trading_engine.reports.shadow_parameter_impact",
    )
    violations: list[str] = []
    for path in SOURCE_PATHS:
        text = path.read_text(encoding="utf-8")
        for fragment in forbidden_fragments:
            if fragment in text:
                violations.append(f"{path}: {fragment}")

    assert violations == []


def test_sanitized_controlled_fill_no_fill_fixture_records_limited_result() -> None:
    payload = json.loads(CONTROLLED_FILL_NO_FILL_FIXTURE.read_text(encoding="utf-8"))

    assert payload["test_status"] == "LIMITED"
    assert payload["connection_status"]["status"] == "CONNECTED"
    assert payload["paper_only"] is True
    assert payload["diagnostic_only"] is True
    assert payload["manual_cli_only"] is True
    assert payload["production_effect"] == "none"
    assert payload["symbol"] == "NVDA"
    assert payload["side"] == "BUY"
    assert payload["quantity"] == 1
    assert payload["limit_price"] == 224.5
    assert payload["order_status_path"] == [
        "PendingSubmit",
        "PreSubmitted",
        "PendingCancel",
        "Cancelled",
    ]
    assert payload["fill_seen"] is False
    assert payload["fill_quantity"] == 0.0
    assert payload["commission_report_seen"] is False
    assert payload["cancel_requested"] is True
    assert payload["final_order_status"] == "Cancelled"
    assert _issue_codes(payload) == ["fill_not_seen_timeout"]
    assert payload["calibration_observation"] == {
        "classification": "NO_FILL_LIFECYCLE_VALIDATED",
        "fill_tested": False,
        "fill_model_validated": False,
        "paperbroker_fill_model_changed": False,
    }
    assert payload["production_surface_impact"]["paperbroker_fill_model"] == "none"


def test_sanitized_controlled_fill_fixture_omits_sensitive_identifiers() -> None:
    text = CONTROLLED_FILL_NO_FILL_FIXTURE.read_text(encoding="utf-8")
    payload = json.loads(text)

    assert payload["account_id_masked"] == "DUP***0000"
    assert payload["broker_order_id"] == "[REDACTED_BROKER_ORDER_ID]"
    assert payload["submitted_order"]["perm_id"] == "[REDACTED_PERM_ID]"
    assert re.search(r"\bDUP\d{5,}\b", text, flags=re.IGNORECASE) is None
    assert re.search(r'"broker_order_id"\s*:\s*"\d+', text) is None
    assert re.search(r'"perm_id"\s*:\s*"\d+', text) is None
    assert re.search(r"\borderId\b.*\d+", text) is None
    assert re.search(r"\bpermId\b.*\d+", text) is None
    assert re.search(r"\bexecId\b", text) is None
    for pattern in SENSITIVE_VALUE_PATTERNS:
        assert re.search(pattern, text, flags=re.IGNORECASE) is None


def test_controlled_fill_local_validation_review_records_safe_no_fill_result() -> None:
    text = CONTROLLED_FILL_LOCAL_VALIDATION_REVIEW.read_text(encoding="utf-8")

    assert "test_status=LIMITED" in text
    assert "connection_status=CONNECTED" in text
    assert "paper-only=true" in text
    assert "diagnostic-only=true" in text
    assert "manual_cli_only=true" in text
    assert "production_effect=none" in text
    assert "production_surface_impact=none" in text
    assert "fill_model_validated=false" in text
    assert "symbol|`NVDA`" in text
    assert "side|`BUY`" in text
    assert "quantity|`1`" in text
    assert "limit_price|`224.50`" in text
    assert "PendingSubmit -> PreSubmitted -> PendingCancel -> Cancelled" in text
    assert "fill_seen=false" in text
    assert "fill_quantity=0" in text
    assert "commission_report_seen=false" in text
    assert "cancel_requested=true" in text
    assert "final_order_status=Cancelled" in text
    assert "issue=fill_not_seen_timeout" in text
    assert "fill model remains unvalidated" in text
    assert "不影响 daily-run、replay、dashboard" in text
    assert "不修改 `PaperBroker` fill model" in text
    assert re.search(r"\bDUP\d{5,}\b", text, flags=re.IGNORECASE) is None
    for pattern in SENSITIVE_VALUE_PATTERNS:
        assert re.search(pattern, text, flags=re.IGNORECASE) is None


def test_controlled_fill_runbook_documents_no_fill_semantics() -> None:
    text = CONTROLLED_FILL_RUNBOOK.read_text(encoding="utf-8")

    assert "非美股正常交易时段" in text
    assert "`BUY LIMIT DAY` 订单可能停留在 `PreSubmitted`" in text
    assert "`limit_price` 高于 TWS / Gateway 当时显示的 ask" in text
    assert "NO_FILL_LIFECYCLE_VALIDATED" in text
    assert "`fill_tested=false`" in text
    assert "`LIFECYCLE_ALIGNED_FILL_UNTESTED`" in text
    assert "只能证明 submit/open/cancel 链路可用，不能证明 fill model" in text
    assert "不把单个 no-fill 样本用于放宽或收紧本地 fill simulation" in text
    assert "美股正常交易时段重新运行 near-ask `BUY LIMIT`" in text


def test_controlled_fill_artifacts_avoid_live_trade_promotion_semantics() -> None:
    texts = [
        CONTROLLED_FILL_NO_FILL_FIXTURE.read_text(encoding="utf-8"),
        CONTROLLED_FILL_LOCAL_VALIDATION_REVIEW.read_text(encoding="utf-8"),
        CONTROLLED_FILL_RUNBOOK.read_text(encoding="utf-8"),
    ]

    for text in texts:
        for phrase in FORBIDDEN_PRODUCTION_SEMANTICS:
            assert phrase not in text


class MockOrder:
    def __init__(self, order_id: int) -> None:
        self.orderId = order_id
        self.permId = order_id + 1000
        self.action = "BUY"
        self.orderType = "LMT"
        self.totalQuantity = 1
        self.lmtPrice = 401.25
        self.tif = "DAY"


class MockOrderStatus:
    def __init__(self, status: str = "Submitted") -> None:
        self.status = status
        self.filled = 1 if status == "Filled" else 0
        self.remaining = 0 if status == "Filled" else 1
        self.avgFillPrice = 401.25 if status == "Filled" else 0.0


class MockTrade:
    def __init__(
        self,
        *,
        order_id: int,
        status: str,
        include_fill: bool,
    ) -> None:
        self.order = MockOrder(order_id)
        self.orderStatus = MockOrderStatus(status)
        self.fills: list[dict[str, Any]] = []
        if include_fill:
            self.fills.append(
                {
                    "execution": {
                        "orderId": order_id,
                        "shares": 1,
                        "price": 401.25,
                        "time": "2026-05-17T14:31:00Z",
                    },
                    "commissionReport": {
                        "commission": 1.0,
                        "currency": "USD",
                    },
                }
            )


class MockControlledFillClient:
    def __init__(
        self,
        *,
        initial_status: str = "Submitted",
        include_fill: bool = False,
        order_id: int = 101,
    ) -> None:
        self.initial_status = initial_status
        self.include_fill = include_fill
        self.order_id = order_id
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.place_order_calls = 0
        self.cancel_order_calls = 0
        self.wait_calls = 0
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
        assert clientId == 19013
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
        assert symbol == "NVDA"
        action = order["action"] if isinstance(order, dict) else order.action
        tif = order["tif"] if isinstance(order, dict) else order.tif
        order_type = order["orderType"] if isinstance(order, dict) else order.orderType
        quantity = order["totalQuantity"] if isinstance(order, dict) else order.totalQuantity
        assert action == "BUY"
        assert tif == "DAY"
        assert order_type in {"LMT", "LIMIT"}
        assert quantity == 1
        self.trade = MockTrade(
            order_id=self.order_id,
            status=self.initial_status,
            include_fill=self.include_fill,
        )
        return self.trade

    def waitOnUpdate(self, timeout: float) -> None:
        assert timeout >= 0
        self.wait_calls += 1

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


def _enabled_config(**overrides: Any) -> ControlledFillConfig:
    values: dict[str, Any] = {
        "controlled_fill_enabled": True,
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 19013,
        "account_id": "DUP1234567",
        "trading_mode": "paper",
        "production_effect": "none",
        "allowed_symbols": ["NVDA", "AAPL", "TSM"],
        "max_quantity": 1,
        "allow_market_order": False,
        "allow_sell": False,
        "allow_option": False,
        "allow_margin": False,
        "allow_short": False,
        "require_manual_limit_price": True,
        "status_timeout_seconds": 0.0,
        "cancel_timeout_seconds": 0.0,
    }
    values.update(overrides)
    return ControlledFillConfig.model_validate(values)


def _write_config(tmp_path: Path, values: dict[str, Any]) -> Path:
    path = tmp_path / "ibkr_paper_controlled_fill.yaml"
    path.write_text(yaml.safe_dump(values, sort_keys=False), encoding="utf-8")
    return path


def _run_with_mock(
    tmp_path: Path,
    *,
    config_overrides: dict[str, Any] | None = None,
    client: MockControlledFillClient | None = None,
    **overrides: Any,
) -> tuple[dict[str, Any], MockControlledFillClient]:
    config_path = _write_config(
        tmp_path,
        _enabled_config(**(config_overrides or {})).model_dump(),
    )
    mock_client = client or MockControlledFillClient()
    payload = run_ibkr_paper_controlled_fill_test(
        as_of=date(2026, 5, 17),
        config_path=config_path,
        output_dir=tmp_path / "reports",
        client=mock_client,
        symbol=overrides.pop("symbol", "NVDA"),
        side=overrides.pop("side", "BUY"),
        quantity=overrides.pop("quantity", 1),
        limit_price=overrides.pop("limit_price", 401.25),
        asset_type=overrides.pop("asset_type", "stock"),
        order_type=overrides.pop("order_type", "LIMIT"),
        time_in_force=overrides.pop("time_in_force", "DAY"),
        **overrides,
    )
    return payload, mock_client


def _issue_codes(payload: dict[str, Any]) -> list[str]:
    return [issue["code"] for issue in payload["issues"]]
