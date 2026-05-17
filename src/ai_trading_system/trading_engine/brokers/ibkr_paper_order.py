from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.brokers.ibkr_readonly import (
    _create_ibkr_client,
    _ensure_asyncio_event_loop,
    _extract_position_quantities,
    _to_jsonable,
    mask_account_id,
    sanitize_ibkr_payload,
)

DEFAULT_IBKR_PAPER_ORDER_CONFIG_PATH = PROJECT_ROOT / "config" / "ibkr_paper_order.yaml"
ORDER_LIFECYCLE_PRODUCTION_EFFECT = "none"
DEFAULT_ALLOWED_SYMBOLS = ("NVDA", "AAPL", "TSM")
OPEN_ORDER_STATUSES = {"ApiPending", "PendingSubmit", "PreSubmitted", "Submitted"}
CANCELLED_STATUSES = {"ApiCancelled", "Cancelled", "Inactive"}
FINAL_ORDER_STATUSES = CANCELLED_STATUSES | {"Filled"}
ACCOUNT_CASH_TAGS = {
    "availablefunds",
    "cashbalance",
    "settledcash",
    "totalcashbalance",
    "totalcashvalue",
}


class LifecycleStatus(StrEnum):
    PASS = "PASS"
    LIMITED = "LIMITED"
    BLOCK = "BLOCK"
    ERROR = "ERROR"


class SafetyCheck(BaseModel):
    name: str = Field(min_length=1)
    status: Literal["PASS", "BLOCK", "LIMITED"]
    message: str = Field(min_length=1)
    details: dict[str, Any] = Field(default_factory=dict)


class LifecycleIssue(BaseModel):
    code: str = Field(min_length=1)
    severity: Literal["LIMITED", "BLOCK", "ERROR"]
    message: str = Field(min_length=1)
    details: dict[str, Any] = Field(default_factory=dict)


class IBKRPaperOrderConfig(BaseModel):
    paper_order_lifecycle_enabled: bool = False
    ibkr_paper_comparison_enabled: bool = False
    host: str = "127.0.0.1"
    port: int = Field(default=7497, ge=1, le=65535)
    client_id: int = Field(default=19010, ge=0)
    account_id: str = ""
    trading_mode: str = "paper"
    production_effect: str = ORDER_LIFECYCLE_PRODUCTION_EFFECT
    allowed_symbols: list[str] = Field(
        default_factory=lambda: list(DEFAULT_ALLOWED_SYMBOLS),
        min_length=1,
    )
    max_quantity: int = Field(default=1, ge=1, le=10)
    far_from_market_pct: float = Field(default=0.5, gt=0, lt=1)
    status_timeout_seconds: float = Field(default=10.0, ge=0.0, le=120.0)
    cancel_timeout_seconds: float = Field(default=10.0, ge=0.0, le=120.0)
    exchange: str = "SMART"
    currency: str = "USD"

    @model_validator(mode="after")
    def normalize_config(self) -> Self:
        self.host = self.host.strip()
        self.account_id = self.account_id.strip()
        self.trading_mode = self.trading_mode.strip().lower()
        self.production_effect = self.production_effect.strip().lower()
        self.allowed_symbols = [symbol.strip().upper() for symbol in self.allowed_symbols]
        self.exchange = self.exchange.strip().upper()
        self.currency = self.currency.strip().upper()
        return self

    def assert_lifecycle_settings(self) -> None:
        if self.trading_mode != "paper":
            raise RuntimeError("IBKR Paper order lifecycle fail closed: trading_mode must be paper")
        if self.production_effect != ORDER_LIFECYCLE_PRODUCTION_EFFECT:
            raise RuntimeError(
                "IBKR Paper order lifecycle fail closed: production_effect must be none"
            )
        if not self.paper_order_lifecycle_enabled:
            raise RuntimeError(
                "IBKR Paper order lifecycle fail closed: "
                "paper_order_lifecycle_enabled must be true"
            )
        if not self.account_id.upper().startswith("DUP"):
            raise RuntimeError(
                "IBKR Paper order lifecycle fail closed: account_id must start with DUP"
            )


class IBKRPaperOrderRequest(BaseModel):
    symbol: str = Field(min_length=1, pattern=r"^[A-Za-z0-9.^-]+$")
    side: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    limit_price: float | None = Field(default=None, gt=0)
    asset_type: str = "stock"
    order_type: str = "LIMIT"
    time_in_force: str = "DAY"
    margin: bool = False
    bracket_order: bool = False
    stop_price: float | None = Field(default=None, gt=0)
    trailing_amount: float | None = Field(default=None, gt=0)
    algo_strategy: str | None = None
    parent_order_id: str | None = None
    oca_group: str | None = None

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("side", "order_type", "time_in_force")
    @classmethod
    def normalize_upper_token(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("asset_type")
    @classmethod
    def normalize_asset_type(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("algo_strategy", "parent_order_id", "oca_group")
    @classmethod
    def normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    def with_limit_price(self, limit_price: float) -> IBKRPaperOrderRequest:
        return self.model_copy(update={"limit_price": limit_price})


@dataclass(frozen=True)
class SubmittedPaperOrder:
    trade: Any
    broker_order_id: str | None
    submitted_order: dict[str, Any]


class IBKRPaperOrderLifecycleAdapter:
    def __init__(
        self,
        *,
        config: IBKRPaperOrderConfig,
        client: Any | None = None,
    ) -> None:
        self.config = config
        self._client = client
        self._connected = False

    def connect(self) -> dict[str, Any]:
        self.config.assert_lifecycle_settings()
        client = self._client or _create_ibkr_client()
        self._client = client
        try:
            client.connect(
                self.config.host,
                self.config.port,
                clientId=self.config.client_id,
                readonly=False,
                account=self.config.account_id,
            )
        except TypeError:
            client.connect(self.config.host, self.config.port, self.config.client_id)
        self._connected = _client_connected(client)
        return {
            "status": "CONNECTED" if self._connected else "UNKNOWN",
            "connected": self._connected,
            "host": self.config.host,
            "port": self.config.port,
            "client_id": self.config.client_id,
            "trading_mode": self.config.trading_mode,
            "production_effect": ORDER_LIFECYCLE_PRODUCTION_EFFECT,
            "account_id_masked": mask_account_id(self.config.account_id),
        }

    def disconnect(self) -> None:
        if self._client is None:
            self._connected = False
            return
        disconnect = getattr(self._client, "disconnect", None)
        if callable(disconnect):
            disconnect()
        self._connected = False

    def resolve_limit_price(self, request: IBKRPaperOrderRequest) -> float | None:
        if request.limit_price is not None:
            return request.limit_price
        reference_price = self.get_reference_price(request.symbol)
        if reference_price is None or reference_price <= 0:
            return None
        if request.side == "BUY":
            return round(reference_price * (1 - self.config.far_from_market_pct), 2)
        return round(reference_price * (1 + self.config.far_from_market_pct), 2)

    def validate_order_request(self, request: IBKRPaperOrderRequest) -> list[SafetyCheck]:
        checks = self._static_safety_checks(request)
        if _has_blocking_check(checks):
            return checks

        if request.side == "SELL":
            available_quantity = self.get_position_quantity(request.symbol)
            if available_quantity is None:
                checks.append(
                    _block(
                        "no_short_sell",
                        "SELL requires confirmed long quantity; position data unavailable",
                        {"symbol": request.symbol},
                    )
                )
            elif available_quantity < request.quantity:
                checks.append(
                    _block(
                        "no_short_sell",
                        "SELL would exceed confirmed long quantity",
                        {
                            "symbol": request.symbol,
                            "available_quantity": available_quantity,
                            "requested_quantity": request.quantity,
                        },
                    )
                )
            else:
                checks.append(
                    _pass(
                        "no_short_sell",
                        "SELL quantity is covered by confirmed long quantity",
                        {
                            "symbol": request.symbol,
                            "available_quantity": available_quantity,
                            "requested_quantity": request.quantity,
                        },
                    )
                )

        if request.side == "BUY":
            available_cash = self.get_available_cash()
            notional = float(request.quantity) * float(request.limit_price or 0)
            if available_cash is None:
                checks.append(
                    _block(
                        "no_margin",
                        (
                            "BUY requires cash confirmation so the lifecycle test "
                            "cannot rely on margin"
                        ),
                    )
                )
            elif available_cash < notional:
                checks.append(
                    _block(
                        "no_margin",
                        "BUY notional exceeds confirmed cash and could rely on margin",
                        {"available_cash": available_cash, "order_notional": notional},
                    )
                )
            else:
                checks.append(
                    _pass(
                        "no_margin",
                        "BUY notional is covered by confirmed cash",
                        {"available_cash": available_cash, "order_notional": notional},
                    )
                )

        return checks

    def submit_order(self, request: IBKRPaperOrderRequest) -> SubmittedPaperOrder:
        client = self._require_client()
        contract = _stock_contract(
            request.symbol,
            exchange=self.config.exchange,
            currency=self.config.currency,
        )
        order = _limit_order(request, account_id=self.config.account_id)
        trade = client.placeOrder(contract, order)
        broker_order_id = _broker_order_id(trade)
        submitted_order = _submitted_order_payload(
            request=request,
            broker_order_id=broker_order_id,
            account_id=self.config.account_id,
        )
        return SubmittedPaperOrder(
            trade=trade,
            broker_order_id=broker_order_id,
            submitted_order=submitted_order,
        )

    def wait_for_order_events(self, trade: Any, *, timeout_seconds: float) -> list[dict[str, Any]]:
        client = self._require_client()
        events: list[dict[str, Any]] = []
        deadline = time.monotonic() + timeout_seconds
        while True:
            _append_event_if_new(events, _status_event_from_trade(trade))
            if timeout_seconds <= 0:
                break
            if time.monotonic() >= deadline:
                break
            wait_on_update = getattr(client, "waitOnUpdate", None)
            if callable(wait_on_update):
                wait_on_update(timeout=min(0.25, max(deadline - time.monotonic(), 0)))
            else:
                sleep = getattr(client, "sleep", None)
                if callable(sleep):
                    sleep(min(0.25, max(deadline - time.monotonic(), 0)))
                else:
                    time.sleep(min(0.25, max(deadline - time.monotonic(), 0)))
            latest_status = _final_status(events)
            if latest_status in FINAL_ORDER_STATUSES:
                break
        return sanitize_ibkr_payload(events, account_id=self.config.account_id)

    def list_open_orders(self) -> list[Any]:
        client = self._require_client()
        for method_name in ("openTrades", "openOrders", "reqOpenOrders", "reqAllOpenOrders"):
            method = getattr(client, method_name, None)
            if callable(method):
                raw = method()
                return sanitize_ibkr_payload(raw, account_id=self.config.account_id)
        return []

    def cancel_order(self, submitted_order: SubmittedPaperOrder) -> Any:
        client = self._require_client()
        cancel = getattr(client, "cancelOrder", None)
        if not callable(cancel):
            raise RuntimeError("IBKR client does not expose cancelOrder")
        raw_order = _trade_order(submitted_order.trade)
        return cancel(raw_order if raw_order is not None else submitted_order.trade)

    def get_fills(self, trade: Any) -> list[Any]:
        fills = _extract_fills(trade)
        return sanitize_ibkr_payload(fills, account_id=self.config.account_id)

    def get_contract_details(self, symbol: str) -> Any:
        client = self._require_client()
        direct = getattr(client, "get_contract_details", None)
        if callable(direct):
            return sanitize_ibkr_payload(direct(symbol), account_id=self.config.account_id)

        req_contract_details = getattr(client, "reqContractDetails", None)
        if not callable(req_contract_details):
            return None
        contract = _stock_contract(
            symbol,
            exchange=self.config.exchange,
            currency=self.config.currency,
        )
        try:
            raw_details = req_contract_details(contract)
        except TypeError:
            raw_details = req_contract_details(contract, [])
        return sanitize_ibkr_payload(raw_details, account_id=self.config.account_id)

    def get_position_quantity(self, symbol: str) -> float | None:
        client = self._require_client()
        for method_name in ("positions", "reqPositions"):
            method = getattr(client, method_name, None)
            if callable(method):
                raw_positions = method()
                positions = _extract_position_quantities(raw_positions)
                return positions.get(symbol.upper(), 0.0)
        return None

    def get_available_cash(self) -> float | None:
        client = self._require_client()
        for method_name in ("accountSummary", "reqAccountSummary"):
            method = getattr(client, method_name, None)
            if not callable(method):
                continue
            try:
                raw_summary = method(account=self.config.account_id)
            except TypeError:
                try:
                    raw_summary = method(self.config.account_id)
                except TypeError:
                    raw_summary = method()
            cash = _extract_available_cash(raw_summary)
            if cash is not None:
                return cash
        return None

    def get_reference_price(self, symbol: str) -> float | None:
        client = self._require_client()
        direct = getattr(client, "get_reference_price", None)
        if callable(direct):
            return _positive_float_or_none(direct(symbol))
        contract = _stock_contract(
            symbol, exchange=self.config.exchange, currency=self.config.currency
        )
        req_market_data = getattr(client, "reqMktData", None)
        if not callable(req_market_data):
            return None
        try:
            ticker = req_market_data(contract, "", False, False)
        except TypeError:
            ticker = req_market_data(contract)
        sleep = getattr(client, "sleep", None)
        if callable(sleep):
            sleep(1)
        price = _market_price_from_ticker(ticker)
        cancel_market_data = getattr(client, "cancelMktData", None)
        if callable(cancel_market_data):
            try:
                cancel_market_data(contract)
            except TypeError:
                pass
        return price

    def _static_safety_checks(self, request: IBKRPaperOrderRequest) -> list[SafetyCheck]:
        checks = [
            _pass("trading_mode", "trading_mode is paper"),
            _pass("production_effect", "production_effect is none"),
            _pass("account_id", "account_id starts with DUP"),
        ]
        if request.asset_type != "stock":
            checks.append(
                _block(
                    "asset_type",
                    "only stock orders are allowed",
                    {"asset_type": request.asset_type},
                )
            )
        else:
            checks.append(_pass("asset_type", "asset_type is stock"))

        if request.order_type != "LIMIT":
            checks.append(
                _block(
                    "order_type",
                    "only LIMIT orders are allowed; market orders are forbidden",
                    {"order_type": request.order_type},
                )
            )
        else:
            checks.append(_pass("order_type", "order_type is LIMIT"))

        if request.time_in_force != "DAY":
            checks.append(
                _block(
                    "time_in_force",
                    "only DAY time_in_force is allowed; GTC is forbidden",
                    {"time_in_force": request.time_in_force},
                )
            )
        else:
            checks.append(_pass("time_in_force", "time_in_force is DAY"))

        if request.side not in {"BUY", "SELL"}:
            checks.append(_block("side", "side must be BUY or SELL", {"side": request.side}))
        else:
            checks.append(_pass("side", "side is BUY or SELL"))

        if request.symbol not in self.config.allowed_symbols:
            checks.append(
                _block(
                    "symbol_whitelist",
                    "symbol is not in the configured Paper order whitelist",
                    {
                        "symbol": request.symbol,
                        "allowed_symbols": self.config.allowed_symbols,
                    },
                )
            )
        else:
            checks.append(_pass("symbol_whitelist", "symbol is in the configured whitelist"))

        if request.quantity > self.config.max_quantity:
            checks.append(
                _block(
                    "quantity",
                    "quantity exceeds the Paper lifecycle safety cap",
                    {
                        "quantity": request.quantity,
                        "max_quantity": self.config.max_quantity,
                    },
                )
            )
        else:
            checks.append(_pass("quantity", "quantity is within the Paper lifecycle safety cap"))

        if request.limit_price is None:
            checks.append(_block("limit_price", "LIMIT order requires a positive limit_price"))
        else:
            checks.append(_pass("limit_price", "limit_price is positive"))

        if request.margin:
            checks.append(_block("margin", "explicit margin order semantics are forbidden"))
        else:
            checks.append(_pass("margin", "no explicit margin order semantics"))

        if request.bracket_order:
            checks.append(_block("bracket_order", "bracket orders are forbidden"))
        else:
            checks.append(_pass("bracket_order", "not a bracket order"))

        if request.stop_price is not None:
            checks.append(_block("stop_order", "stop orders are forbidden"))
        else:
            checks.append(_pass("stop_order", "not a stop order"))

        if request.trailing_amount is not None:
            checks.append(_block("trailing_order", "trailing orders are forbidden"))
        else:
            checks.append(_pass("trailing_order", "not a trailing order"))

        if request.algo_strategy is not None:
            checks.append(
                _block(
                    "algo_order",
                    "algo orders are forbidden",
                    {"algo_strategy": request.algo_strategy},
                )
            )
        else:
            checks.append(_pass("algo_order", "not an algo order"))

        if request.parent_order_id is not None or request.oca_group is not None:
            checks.append(
                _block(
                    "linked_order",
                    "parent/child and OCA order links are forbidden",
                    {
                        "parent_order_id": request.parent_order_id,
                        "oca_group": request.oca_group,
                    },
                )
            )
        else:
            checks.append(_pass("linked_order", "no parent/child or OCA linkage"))

        return checks

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("IBKR Paper order lifecycle adapter is not connected")
        return self._client


def load_ibkr_paper_order_config(
    path: Path | str = DEFAULT_IBKR_PAPER_ORDER_CONFIG_PATH,
) -> IBKRPaperOrderConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}
    return IBKRPaperOrderConfig.model_validate(raw)


def masked_order_account_id(config: IBKRPaperOrderConfig | None) -> str:
    return mask_account_id(config.account_id if config is not None else "")


def order_events_summary(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    statuses = [str(event.get("status")) for event in events if event.get("status")]
    final_status = statuses[-1] if statuses else "UNKNOWN"
    return {
        "open_order_seen": any(status in OPEN_ORDER_STATUSES for status in statuses),
        "final_order_status": final_status,
        "cancelled_confirmed": final_status in CANCELLED_STATUSES,
    }


def lifecycle_status_from_result(
    *,
    open_order_seen: bool,
    cancelled_confirmed: bool,
    fills_seen: bool,
    issues: Sequence[LifecycleIssue],
) -> LifecycleStatus:
    if any(issue.severity == LifecycleStatus.ERROR.value for issue in issues):
        return LifecycleStatus.ERROR
    if any(issue.severity == LifecycleStatus.BLOCK.value for issue in issues):
        return LifecycleStatus.BLOCK
    if not cancelled_confirmed or not open_order_seen or fills_seen or issues:
        return LifecycleStatus.LIMITED
    return LifecycleStatus.PASS


def safety_checks_to_issues(checks: Sequence[SafetyCheck]) -> list[LifecycleIssue]:
    return [
        LifecycleIssue(
            code=check.name,
            severity="BLOCK" if check.status == "BLOCK" else "LIMITED",
            message=check.message,
            details=check.details,
        )
        for check in checks
        if check.status != "PASS"
    ]


def _stock_contract(symbol: str, *, exchange: str, currency: str) -> Any:
    _ensure_asyncio_event_loop()
    try:
        from ib_insync import Stock
    except ImportError:
        return {
            "symbol": symbol.upper(),
            "secType": "STK",
            "exchange": exchange,
            "currency": currency,
        }
    return Stock(symbol.upper(), exchange, currency)


def _limit_order(request: IBKRPaperOrderRequest, *, account_id: str) -> Any:
    _ensure_asyncio_event_loop()
    try:
        from ib_insync import LimitOrder
    except ImportError:
        return {
            "action": request.side,
            "totalQuantity": request.quantity,
            "orderType": "LMT",
            "lmtPrice": request.limit_price,
            "tif": "DAY",
            "account": account_id,
            "transmit": True,
            "outsideRth": False,
        }
    order = LimitOrder(
        request.side,
        request.quantity,
        float(request.limit_price or 0),
        tif="DAY",
        account=account_id,
    )
    order.transmit = True
    order.outsideRth = False
    return order


def _submitted_order_payload(
    *,
    request: IBKRPaperOrderRequest,
    broker_order_id: str | None,
    account_id: str,
) -> dict[str, Any]:
    payload = {
        "symbol": request.symbol,
        "asset_type": request.asset_type,
        "side": request.side,
        "order_type": request.order_type,
        "time_in_force": request.time_in_force,
        "quantity": request.quantity,
        "limit_price": request.limit_price,
        "broker_order_id": broker_order_id,
        "account_id_masked": mask_account_id(account_id),
        "submitted_at": datetime.now(tz=UTC).isoformat(),
        "production_effect": ORDER_LIFECYCLE_PRODUCTION_EFFECT,
    }
    return sanitize_ibkr_payload(payload, account_id=account_id)


def _client_connected(client: Any) -> bool:
    is_connected = getattr(client, "isConnected", None)
    if callable(is_connected):
        return bool(is_connected())
    return True


def _has_blocking_check(checks: Sequence[SafetyCheck]) -> bool:
    return any(check.status == "BLOCK" for check in checks)


def _pass(name: str, message: str, details: dict[str, Any] | None = None) -> SafetyCheck:
    return SafetyCheck(name=name, status="PASS", message=message, details=details or {})


def _block(name: str, message: str, details: dict[str, Any] | None = None) -> SafetyCheck:
    return SafetyCheck(name=name, status="BLOCK", message=message, details=details or {})


def _append_event_if_new(events: list[dict[str, Any]], event: dict[str, Any] | None) -> None:
    if event is None:
        return
    comparable = {key: value for key, value in event.items() if key != "observed_at"}
    for existing in events:
        existing_comparable = {
            key: value for key, value in existing.items() if key != "observed_at"
        }
        if existing_comparable == comparable:
            return
    events.append(event)


def _status_event_from_trade(trade: Any) -> dict[str, Any] | None:
    status_payload = _extract_order_status_payload(trade)
    if not status_payload:
        return None
    status = _string_from_keys(status_payload, ("status", "Status"))
    if not status:
        return None
    return {
        "observed_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "filled": _float_from_keys(status_payload, ("filled", "filledQuantity")),
        "remaining": _float_from_keys(status_payload, ("remaining", "remainingQuantity")),
        "avg_fill_price": _float_from_keys(status_payload, ("avgFillPrice", "avg_fill_price")),
        "broker_order_id": _broker_order_id(trade),
    }


def _extract_order_status_payload(trade: Any) -> dict[str, Any]:
    jsonable = _to_jsonable(trade)
    if isinstance(jsonable, Mapping):
        for key in ("orderStatus", "order_status", "status"):
            value = jsonable.get(key)
            if isinstance(value, Mapping):
                return dict(value)
        if "status" in jsonable:
            return dict(jsonable)
    status_obj = getattr(trade, "orderStatus", None)
    if status_obj is not None:
        status_json = _to_jsonable(status_obj)
        if isinstance(status_json, Mapping):
            return dict(status_json)
    return {}


def _broker_order_id(trade: Any) -> str | None:
    jsonable = _to_jsonable(trade)
    candidates: list[Any] = []
    if isinstance(jsonable, Mapping):
        candidates.extend(
            [
                jsonable.get("broker_order_id"),
                jsonable.get("orderId"),
                jsonable.get("order_id"),
                jsonable.get("permId"),
            ]
        )
        for key in ("order", "orderStatus", "order_status"):
            value = jsonable.get(key)
            if isinstance(value, Mapping):
                candidates.extend(
                    [
                        value.get("orderId"),
                        value.get("order_id"),
                        value.get("permId"),
                        value.get("perm_id"),
                    ]
                )
    order = getattr(trade, "order", None)
    if order is not None:
        candidates.extend([getattr(order, "orderId", None), getattr(order, "permId", None)])
    status = getattr(trade, "orderStatus", None)
    if status is not None:
        candidates.extend([getattr(status, "orderId", None), getattr(status, "permId", None)])
    for candidate in candidates:
        if candidate not in (None, "", 0):
            return str(candidate)
    return None


def _trade_order(trade: Any) -> Any | None:
    if isinstance(trade, Mapping):
        order = trade.get("order")
        return order if order is not None else trade
    return getattr(trade, "order", None)


def _extract_fills(trade: Any) -> list[Any]:
    jsonable = _to_jsonable(trade)
    if isinstance(jsonable, Mapping):
        raw_fills = jsonable.get("fills") or jsonable.get("executions") or []
        if isinstance(raw_fills, list):
            return raw_fills
        return [raw_fills] if raw_fills else []
    raw_fills = getattr(trade, "fills", [])
    if isinstance(raw_fills, list):
        return raw_fills
    return [raw_fills] if raw_fills else []


def _final_status(events: Sequence[Mapping[str, Any]]) -> str:
    for event in reversed(events):
        status = event.get("status")
        if status:
            return str(status)
    return "UNKNOWN"


def _extract_available_cash(account_summary: Any) -> float | None:
    jsonable = _to_jsonable(account_summary)
    return _cash_from_jsonable(jsonable)


def _cash_from_jsonable(value: Any) -> float | None:
    if isinstance(value, Mapping):
        tag = str(value.get("tag") or value.get("Tag") or "").lower().replace("_", "")
        if tag in ACCOUNT_CASH_TAGS:
            cash = _positive_float_or_none(value.get("value") or value.get("Value"))
            if cash is not None:
                return cash
        for key, item in value.items():
            normalized_key = str(key).lower().replace("_", "")
            if normalized_key in ACCOUNT_CASH_TAGS:
                cash = _positive_float_or_none(item)
                if cash is not None:
                    return cash
            nested_cash = _cash_from_jsonable(item)
            if nested_cash is not None:
                return nested_cash
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        if len(value) >= 3:
            tag = str(value[1]).lower().replace("_", "")
            if tag in ACCOUNT_CASH_TAGS:
                cash = _positive_float_or_none(value[2])
                if cash is not None:
                    return cash
        for item in value:
            nested_cash = _cash_from_jsonable(item)
            if nested_cash is not None:
                return nested_cash
    return None


def _market_price_from_ticker(ticker: Any) -> float | None:
    market_price = getattr(ticker, "marketPrice", None)
    if callable(market_price):
        price = _positive_float_or_none(market_price())
        if price is not None:
            return price
    jsonable = _to_jsonable(ticker)
    if isinstance(jsonable, Mapping):
        for key in ("last", "close", "marketPrice", "bid", "ask"):
            price = _positive_float_or_none(jsonable.get(key))
            if price is not None:
                return price
        bid = _positive_float_or_none(jsonable.get("bid"))
        ask = _positive_float_or_none(jsonable.get("ask"))
        if bid is not None and ask is not None:
            return (bid + ask) / 2
    return None


def _positive_float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _string_from_keys(payload: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _float_from_keys(payload: Mapping[str, Any], keys: Sequence[str]) -> float | None:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return None
