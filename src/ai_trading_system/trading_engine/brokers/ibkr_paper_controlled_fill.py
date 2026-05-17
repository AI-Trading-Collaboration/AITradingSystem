from __future__ import annotations

import csv
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.brokers.ibkr_paper_order import (
    DEFAULT_ALLOWED_SYMBOLS,
    IBKRPaperOrderConfig,
    IBKRPaperOrderLifecycleAdapter,
    IBKRPaperOrderRequest,
    LifecycleIssue,
    SafetyCheck,
    order_events_summary,
    safety_checks_to_issues,
)
from ai_trading_system.trading_engine.brokers.ibkr_readonly import (
    mask_account_id,
    sanitize_ibkr_payload,
)
from ai_trading_system.trading_engine.execution.paper_broker import PaperBroker
from ai_trading_system.trading_engine.market_data.market_session_guard import (
    FALLBACK_UNKNOWN_SOURCE,
    MarketSessionGuardResult,
    MarketSessionStatus,
    evaluate_market_session,
)
from ai_trading_system.trading_engine.schemas.market import MarketSnapshot
from ai_trading_system.trading_engine.schemas.order_intent import (
    AssetType,
    OrderIntent,
    OrderSide,
    OrderType,
    TimeInForce,
)

CONTROLLED_FILL_REPORT_TYPE = "ibkr_paper_controlled_fill"
CONTROLLED_FILL_PRODUCTION_EFFECT = "none"
DEFAULT_IBKR_PAPER_CONTROLLED_FILL_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "ibkr_paper_controlled_fill.yaml"
)
DEFAULT_CONTROLLED_FILL_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "reports"
_BROKER_ORDER_ID_KEYS = {
    "broker_order_id",
    "orderid",
    "order_id",
    "permid",
    "perm_id",
}
_CONTEXTUAL_BROKER_ORDER_ID_LABEL = (
    r"(?:broker[\s_-]*order[\s_-]*id|order[\s_-]*id|orderid|perm[\s_-]*id|permid)"
)
_JSON_BROKER_ORDER_ID_KEY = r"(?:broker_order_id|orderId|order_id|permId|perm_id)"
_MIN_GLOBAL_STRING_BROKER_ID_LENGTH = 4
DEFAULT_CONTROLLED_FILL_EXCHANGE_TIMEZONE = "US/Eastern"
_SESSION_GUARD_ISSUE_CODES = {
    MarketSessionStatus.CLOSED: "market_session_closed",
    MarketSessionStatus.OUTSIDE_RTH: "outside_regular_trading_hours",
    MarketSessionStatus.UNKNOWN: "market_session_unknown",
}


class ControlledFillStatus(StrEnum):
    PASS = "PASS"
    LIMITED = "LIMITED"
    BLOCK = "BLOCK"
    ERROR = "ERROR"


class ControlledFillConfig(BaseModel):
    controlled_fill_enabled: bool = False
    host: str = "127.0.0.1"
    port: int = Field(default=7497, ge=1, le=65535)
    client_id: int = Field(default=19013, ge=0)
    account_id: str = ""
    trading_mode: str = "paper"
    production_effect: str = CONTROLLED_FILL_PRODUCTION_EFFECT
    max_quantity: int = Field(default=1, ge=1, le=1)
    allowed_symbols: list[str] = Field(
        default_factory=lambda: list(DEFAULT_ALLOWED_SYMBOLS),
        min_length=1,
    )
    allow_market_order: bool = False
    allow_sell: bool = False
    allow_option: bool = False
    allow_margin: bool = False
    allow_short: bool = False
    require_manual_limit_price: bool = True
    status_timeout_seconds: float = Field(default=30.0, ge=0.0, le=300.0)
    cancel_timeout_seconds: float = Field(default=10.0, ge=0.0, le=120.0)
    exchange: str = "SMART"
    currency: str = "USD"
    exchange_timezone: str = DEFAULT_CONTROLLED_FILL_EXCHANGE_TIMEZONE

    @model_validator(mode="after")
    def normalize_config(self) -> Self:
        self.host = self.host.strip()
        self.account_id = self.account_id.strip()
        self.trading_mode = self.trading_mode.strip().lower()
        self.production_effect = self.production_effect.strip().lower()
        self.allowed_symbols = [symbol.strip().upper() for symbol in self.allowed_symbols]
        self.exchange = self.exchange.strip().upper()
        self.currency = self.currency.strip().upper()
        self.exchange_timezone = self.exchange_timezone.strip() or (
            DEFAULT_CONTROLLED_FILL_EXCHANGE_TIMEZONE
        )
        return self


def load_controlled_fill_config(
    path: Path | str = DEFAULT_IBKR_PAPER_CONTROLLED_FILL_CONFIG_PATH,
) -> ControlledFillConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}
    return ControlledFillConfig.model_validate(raw)


def run_ibkr_paper_controlled_fill_test(
    *,
    as_of: date,
    config_path: Path | str = DEFAULT_IBKR_PAPER_CONTROLLED_FILL_CONFIG_PATH,
    output_dir: Path | str = DEFAULT_CONTROLLED_FILL_OUTPUT_DIR,
    client: Any | None = None,
    symbol: str = "NVDA",
    side: str = "BUY",
    quantity: int = 1,
    limit_price: float | None = None,
    asset_type: str = "stock",
    order_type: str = "LIMIT",
    time_in_force: str = "DAY",
    margin: bool = False,
    market_snapshot: MarketSnapshot | Mapping[str, Any] | None = None,
    prices_path: Path | str | None = None,
    allow_outside_rth_diagnostic: bool = False,
    session_as_of: datetime | None = None,
) -> dict[str, Any]:
    config: ControlledFillConfig | None = None
    adapter: IBKRPaperOrderLifecycleAdapter | None = None
    submitted_order: Any | None = None
    raw_broker_order_ids: list[str] = []
    stage = "load_config"
    payload = _base_payload(as_of=as_of, config_path=Path(config_path))
    payload["outside_rth_override"] = bool(allow_outside_rth_diagnostic)

    try:
        config = load_controlled_fill_config(config_path)
        _merge_config_metadata(payload, config)

        request = IBKRPaperOrderRequest(
            symbol=symbol,
            side=side,
            quantity=quantity,
            limit_price=limit_price,
            asset_type=asset_type,
            order_type=order_type,
            time_in_force=time_in_force,
            margin=margin,
        )
        payload.update(
            {
                "symbol": request.symbol,
                "side": request.side,
                "quantity": request.quantity,
                "limit_price": request.limit_price,
                "requested_order": _requested_order_payload(request),
            }
        )

        stage = "config_safety"
        initial_checks = _config_safety_checks(config) + _order_input_safety_checks(
            request,
            config=config,
        )
        _merge_safety_checks(payload, initial_checks)
        initial_issues = safety_checks_to_issues(initial_checks)
        if initial_issues:
            for issue in initial_issues:
                _add_issue(payload, issue)
            payload["test_status"] = ControlledFillStatus.BLOCK.value
            return _write_outputs(
                payload=payload,
                as_of=as_of,
                output_dir=Path(output_dir),
                config=config,
                raw_broker_order_ids=raw_broker_order_ids,
            )

        stage = "connect"
        adapter = IBKRPaperOrderLifecycleAdapter(
            config=_order_config_from_controlled_fill(config),
            client=client,
        )
        payload["connection_status"] = adapter.connect()

        stage = "order_safety"
        broker_checks = adapter.validate_order_request(request)
        _merge_safety_checks(payload, broker_checks)
        broker_issues = safety_checks_to_issues(broker_checks)
        if broker_issues:
            for issue in broker_issues:
                _add_issue(payload, issue)
            payload["test_status"] = ControlledFillStatus.BLOCK.value
            return _write_outputs(
                payload=payload,
                as_of=as_of,
                output_dir=Path(output_dir),
                config=config,
                raw_broker_order_ids=raw_broker_order_ids,
            )

        stage = "market_session_guard"
        session_result = _market_session_guard_result(
            adapter=adapter,
            request=request,
            config=config,
            as_of=session_as_of or datetime.now(tz=UTC),
        )
        _merge_market_session_guard(payload, session_result)
        if not session_result.can_submit_controlled_fill:
            issue_code = _session_guard_issue_code(session_result.market_session_status)
            override_allowed = (
                allow_outside_rth_diagnostic
                and session_result.market_session_status == MarketSessionStatus.OUTSIDE_RTH
            )
            _add_issue(
                payload,
                LifecycleIssue(
                    code=issue_code,
                    severity="LIMITED" if override_allowed else "BLOCK",
                    message=(
                        "outside_rth_override=true; fill interpretation is limited"
                        if override_allowed
                        else "Controlled fill submission blocked by market session guard."
                    ),
                    details={
                        "market_session_status": session_result.market_session_status.value,
                        "session_guard_reason": session_result.reason,
                        "outside_rth_override": bool(allow_outside_rth_diagnostic),
                    },
                ),
            )
            if override_allowed:
                payload["controlled_fill_submission"] = (
                    "submitted_with_outside_rth_diagnostic_override"
                )
                payload["fill_interpretation"] = "limited"
            else:
                payload["controlled_fill_submission"] = "blocked_by_session_guard"
                payload["test_status"] = ControlledFillStatus.BLOCK.value
                return _write_outputs(
                    payload=payload,
                    as_of=as_of,
                    output_dir=Path(output_dir),
                    config=config,
                    raw_broker_order_ids=raw_broker_order_ids,
                )
        else:
            payload["controlled_fill_submission"] = "submitted"
            payload["fill_interpretation"] = "regular_session"

        order_intent = _order_intent_from_request(request, as_of=as_of)
        payload["order_intent"] = order_intent.model_dump(mode="json")

        stage = "submit_order"
        submitted_order = adapter.submit_order(request)
        if submitted_order.broker_order_id:
            raw_broker_order_ids.append(submitted_order.broker_order_id)
        payload["broker_order_id"] = _redacted_broker_order_id(submitted_order.broker_order_id)
        payload["submitted_order"] = _redact_broker_payload(
            submitted_order.submitted_order,
            raw_broker_order_ids=raw_broker_order_ids,
            account_id=config.account_id,
        )

        stage = "wait_fill_status"
        _merge_order_events(
            payload,
            adapter.wait_for_order_events(
                submitted_order.trade,
                timeout_seconds=config.status_timeout_seconds,
            ),
            raw_broker_order_ids=raw_broker_order_ids,
            account_id=config.account_id,
        )
        open_orders = _redact_broker_payload(
            adapter.list_open_orders(),
            raw_broker_order_ids=raw_broker_order_ids,
            account_id=config.account_id,
        )
        payload["open_order_seen"] = _open_order_seen(
            order_status_events=payload["order_status_events"],
            open_orders=open_orders,
        )
        payload["open_orders_seen_payload"] = open_orders
        _merge_fills(
            payload,
            adapter.get_fills(submitted_order.trade),
            raw_broker_order_ids=raw_broker_order_ids,
            account_id=config.account_id,
        )
        _summarize_order_and_fills(payload)

        if not payload["fill_seen"]:
            _add_issue(
                payload,
                LifecycleIssue(
                    code="fill_not_seen_timeout",
                    severity="LIMITED",
                    message="IBKR Paper order did not fill within the controlled fill timeout.",
                    details={"timeout_seconds": config.status_timeout_seconds},
                ),
            )
            stage = "cancel_order"
            payload["cancel_requested"] = True
            adapter.cancel_order(submitted_order)

            stage = "wait_cancel_status"
            _merge_order_events(
                payload,
                adapter.wait_for_order_events(
                    submitted_order.trade,
                    timeout_seconds=config.cancel_timeout_seconds,
                ),
                raw_broker_order_ids=raw_broker_order_ids,
                account_id=config.account_id,
            )
            _merge_fills(
                payload,
                adapter.get_fills(submitted_order.trade),
                raw_broker_order_ids=raw_broker_order_ids,
                account_id=config.account_id,
            )
            _summarize_order_and_fills(payload)
            if not _cancelled_confirmed(payload):
                _add_issue(
                    payload,
                    LifecycleIssue(
                        code="cancel_not_confirmed",
                        severity="LIMITED",
                        message="Cancel was requested but final order status is not cancelled.",
                        details={"final_order_status": payload["final_order_status"]},
                    ),
                )

        if payload["fill_seen"]:
            stage = "local_paperbroker_comparison"
            _run_local_fill_comparison(
                payload,
                order_intent=order_intent,
                as_of=as_of,
                market_snapshot=market_snapshot,
                prices_path=Path(prices_path) if prices_path is not None else None,
            )

        payload["test_status"] = _test_status_from_payload(payload)
    except Exception as exc:
        if adapter is not None and submitted_order is not None and not payload["cancel_requested"]:
            payload["cancel_requested"] = _try_cancel_after_error(adapter, submitted_order, payload)
        severity: Literal["BLOCK", "ERROR"] = (
            "BLOCK"
            if stage in {"load_config", "config_safety", "order_safety", "market_session_guard"}
            else "ERROR"
        )
        _add_issue(
            payload,
            LifecycleIssue(
                code=f"{stage}_failed",
                severity=severity,
                message=sanitize_ibkr_payload(
                    str(exc), account_id=config.account_id if config else ""
                ),
                details={"error_type": type(exc).__name__, "stage": stage},
            ),
        )
        if payload["connection_status"].get("status") == "NOT_RUN" and stage != "config_safety":
            payload["connection_status"] = {
                "status": "ERROR",
                "connected": False,
                "error_type": type(exc).__name__,
                "production_effect": CONTROLLED_FILL_PRODUCTION_EFFECT,
                "trading_mode": payload["trading_mode"],
                "account_id_masked": payload["account_id_masked"],
            }
        payload["test_status"] = severity
    finally:
        if adapter is not None:
            adapter.disconnect()

    return _write_outputs(
        payload=payload,
        as_of=as_of,
        output_dir=Path(output_dir),
        config=config,
        raw_broker_order_ids=raw_broker_order_ids,
    )


def render_controlled_fill_markdown(payload: dict[str, Any]) -> str:
    comparison = payload.get("paperbroker_comparison", {})
    lines = [
        "# IBKR Paper Controlled Fill Test",
        "",
        f"- 日期：{payload['as_of']}",
        f"- test_status：{payload['test_status']}",
        f"- production_effect={payload['production_effect']}",
        f"- trading_mode={payload['trading_mode']}",
        f"- account_id_masked：`{payload['account_id_masked']}`",
        f"- manual_cli_only：{str(payload.get('manual_cli_only')).lower()}",
        f"- controlled_fill_submission：{payload.get('controlled_fill_submission')}",
        f"- outside_rth_override：{str(payload.get('outside_rth_override')).lower()}",
        f"- fill_interpretation：{payload.get('fill_interpretation')}",
        (
            "- 安全边界：这是 IBKR Paper controlled fill test，不是实盘交易，"
            "不是 production 自动交易；成交也只是 Paper account 模拟成交，"
            "不得用一次 fill 结果修改 PaperBroker。"
        ),
        "",
        "## Order",
        "",
        _json_block(payload.get("submitted_order") or payload.get("requested_order", {})),
        "",
        "## Market Session Guard",
        "",
        f"- market_session_status：{payload.get('market_session_status')}",
        f"- exchange_timezone：{payload.get('exchange_timezone')}",
        f"- trading_hours_source：{payload.get('trading_hours_source')}",
        f"- liquid_hours_source：{payload.get('liquid_hours_source')}",
        f"- session_guard_reason：{payload.get('session_guard_reason')}",
        (
            "- 解释：非 regular session 下 `PreSubmitted` / no-fill 不等于 fill failure；"
            "不应归因于 `PaperBroker` fill model。"
        ),
        "",
        "## IBKR Paper Fill",
        "",
        f"- broker_order_id：`{payload.get('broker_order_id') or 'missing'}`",
        f"- open_order_seen：{str(payload.get('open_order_seen')).lower()}",
        f"- fill_seen：{str(payload.get('fill_seen')).lower()}",
        f"- fill_quantity：{payload.get('fill_quantity')}",
        f"- avg_fill_price：{payload.get('avg_fill_price')}",
        f"- fill_time：{payload.get('fill_time')}",
        f"- commission_report_seen：{str(payload.get('commission_report_seen')).lower()}",
        f"- cancel_requested：{str(payload.get('cancel_requested')).lower()}",
        f"- final_order_status：{payload.get('final_order_status')}",
        "",
        "## Local PaperBroker Comparison",
        "",
        f"- fill_match_status：{comparison.get('fill_match_status')}",
        f"- local_fill_seen：{comparison.get('local_fill_seen')}",
        f"- local_avg_fill_price：{comparison.get('local_avg_fill_price')}",
        f"- ibkr_fill_seen：{comparison.get('ibkr_fill_seen')}",
        f"- ibkr_avg_fill_price：{comparison.get('ibkr_avg_fill_price')}",
        f"- fill_price_diff：{comparison.get('fill_price_diff')}",
        f"- market_snapshot_source：{comparison.get('market_snapshot_source')}",
        "",
        "## Order Status Events",
        "",
        _json_block(payload.get("order_status_events", [])),
        "",
        "## Fills",
        "",
        _json_block(payload.get("fills", [])),
        "",
        "## Safety Checks",
        "",
        _json_block(payload.get("safety_checks", [])),
        "",
        "## Issues",
        "",
        _json_block(payload.get("issues", [])),
    ]
    return "\n".join(lines).rstrip() + "\n"


def default_controlled_fill_json_path(reports_dir: Path, as_of: date) -> Path:
    return reports_dir / f"{CONTROLLED_FILL_REPORT_TYPE}_{as_of.isoformat()}.json"


def _base_payload(*, as_of: date, config_path: Path) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "report_type": CONTROLLED_FILL_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "config_path": str(config_path),
        "test_status": ControlledFillStatus.BLOCK.value,
        "production_effect": CONTROLLED_FILL_PRODUCTION_EFFECT,
        "configured_production_effect": "unknown",
        "trading_mode": "unknown",
        "controlled_fill_enabled": False,
        "manual_cli_only": True,
        "market_session_status": MarketSessionStatus.UNKNOWN.value,
        "can_submit_controlled_fill": False,
        "market_session_source": FALLBACK_UNKNOWN_SOURCE,
        "exchange_timezone": DEFAULT_CONTROLLED_FILL_EXCHANGE_TIMEZONE,
        "trading_hours_source": FALLBACK_UNKNOWN_SOURCE,
        "liquid_hours_source": FALLBACK_UNKNOWN_SOURCE,
        "controlled_fill_submission": "not_evaluated",
        "outside_rth_override": False,
        "session_guard_reason": "not_evaluated",
        "session_guard_checked_at": None,
        "fill_interpretation": "not_evaluated",
        "production_surface_impact": {
            "daily_run": "none",
            "dashboard": "none",
            "replay": "none",
            "paper_signal_quality": "none",
            "shadow_impact": "none",
            "paperbroker_fill_model": "none",
            "trading_advice": "none",
        },
        "connection_status": {"status": "NOT_RUN", "connected": False},
        "account_id_masked": "missing",
        "broker_order_id": None,
        "symbol": "",
        "side": "",
        "quantity": None,
        "limit_price": None,
        "requested_order": {},
        "submitted_order": {},
        "order_intent": {},
        "order_status_events": [],
        "open_order_seen": False,
        "open_orders_seen_payload": [],
        "fill_seen": False,
        "fill_quantity": 0.0,
        "avg_fill_price": None,
        "fill_time": None,
        "fills": [],
        "cancel_requested": False,
        "final_order_status": "UNKNOWN",
        "commission_report_seen": False,
        "paperbroker_comparison": _empty_paperbroker_comparison(),
        "safety_checks": [],
        "issues": [],
    }


def _merge_config_metadata(payload: dict[str, Any], config: ControlledFillConfig) -> None:
    payload.update(
        {
            "configured_production_effect": config.production_effect,
            "trading_mode": config.trading_mode,
            "controlled_fill_enabled": config.controlled_fill_enabled,
            "account_id_masked": mask_account_id(config.account_id),
            "exchange_timezone": config.exchange_timezone,
        }
    )


def _config_safety_checks(config: ControlledFillConfig) -> list[SafetyCheck]:
    checks: list[SafetyCheck] = []
    checks.append(
        _pass("controlled_fill_enabled", "controlled fill is explicitly enabled")
        if config.controlled_fill_enabled
        else _block("controlled_fill_enabled", "controlled_fill_enabled must be true")
    )
    checks.append(
        _pass("trading_mode", "trading_mode is paper")
        if config.trading_mode == "paper"
        else _block("trading_mode", "trading_mode must be paper")
    )
    checks.append(
        _pass("production_effect", "production_effect is none")
        if config.production_effect == CONTROLLED_FILL_PRODUCTION_EFFECT
        else _block("production_effect", "production_effect must be none")
    )
    checks.append(
        _pass("account_id", "account_id starts with DUP")
        if config.account_id.upper().startswith("DUP")
        else _block("account_id", "account_id must start with DUP")
    )
    checks.append(
        _pass("max_quantity", "max_quantity is capped at 1")
        if config.max_quantity == 1
        else _block("max_quantity", "max_quantity must equal 1")
    )
    for field_name in (
        "allow_market_order",
        "allow_sell",
        "allow_option",
        "allow_margin",
        "allow_short",
    ):
        value = bool(getattr(config, field_name))
        checks.append(
            _block(field_name, f"{field_name} must remain false")
            if value
            else _pass(field_name, f"{field_name} is false")
        )
    checks.append(
        _pass("require_manual_limit_price", "manual limit price is required")
        if config.require_manual_limit_price
        else _block("require_manual_limit_price", "require_manual_limit_price must be true")
    )
    return checks


def _order_input_safety_checks(
    request: IBKRPaperOrderRequest,
    *,
    config: ControlledFillConfig,
) -> list[SafetyCheck]:
    checks: list[SafetyCheck] = []
    checks.append(
        _pass("asset_type", "asset_type is stock")
        if request.asset_type == "stock"
        else _block("asset_type", "only stock orders are allowed")
    )
    checks.append(
        _pass("order_type", "order_type is LIMIT")
        if request.order_type == "LIMIT"
        else _block("order_type", "only LIMIT orders are allowed; market orders are forbidden")
    )
    checks.append(
        _pass("time_in_force", "time_in_force is DAY")
        if request.time_in_force == "DAY"
        else _block("time_in_force", "only DAY time_in_force is allowed")
    )
    checks.append(
        _pass("side", "side is BUY")
        if request.side == "BUY"
        else _block("side", "only BUY is allowed; SELL/short is forbidden")
    )
    checks.append(
        _pass("symbol_whitelist", "symbol is in the configured whitelist")
        if request.symbol in set(config.allowed_symbols)
        else _block(
            "symbol_whitelist",
            "symbol is not in the configured controlled fill whitelist",
            {"symbol": request.symbol, "allowed_symbols": config.allowed_symbols},
        )
    )
    checks.append(
        _pass("quantity", "quantity equals the controlled fill cap")
        if request.quantity == 1 and request.quantity <= config.max_quantity
        else _block(
            "quantity",
            "quantity must equal 1 for the first controlled fill version",
            {"quantity": request.quantity, "max_quantity": config.max_quantity},
        )
    )
    checks.append(
        _pass("limit_price", "manual limit_price is positive")
        if request.limit_price is not None and request.limit_price > 0
        else _block("limit_price", "manual positive limit_price is required")
    )
    checks.append(
        _block("margin", "margin semantics are forbidden")
        if request.margin
        else _pass("margin", "no explicit margin semantics")
    )
    return checks


def _order_config_from_controlled_fill(config: ControlledFillConfig) -> IBKRPaperOrderConfig:
    return IBKRPaperOrderConfig(
        paper_order_lifecycle_enabled=True,
        ibkr_paper_comparison_enabled=False,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        trading_mode=config.trading_mode,
        production_effect=config.production_effect,
        allowed_symbols=list(config.allowed_symbols),
        max_quantity=config.max_quantity,
        status_timeout_seconds=config.status_timeout_seconds,
        cancel_timeout_seconds=config.cancel_timeout_seconds,
        exchange=config.exchange,
        currency=config.currency,
    )


def _requested_order_payload(request: IBKRPaperOrderRequest) -> dict[str, Any]:
    return {
        "symbol": request.symbol,
        "asset_type": request.asset_type,
        "side": request.side,
        "order_type": request.order_type,
        "time_in_force": request.time_in_force,
        "quantity": request.quantity,
        "limit_price": request.limit_price,
        "production_effect": CONTROLLED_FILL_PRODUCTION_EFFECT,
    }


def _market_session_guard_result(
    *,
    adapter: IBKRPaperOrderLifecycleAdapter,
    request: IBKRPaperOrderRequest,
    config: ControlledFillConfig,
    as_of: datetime,
) -> MarketSessionGuardResult:
    try:
        contract_details = adapter.get_contract_details(request.symbol)
    except Exception as exc:
        return _unknown_market_session_result(
            symbol=request.symbol,
            as_of=as_of,
            exchange_timezone=config.exchange_timezone,
            reason=(
                "IBKR contract details request failed: "
                f"{sanitize_ibkr_payload(str(exc), account_id=config.account_id)}"
            ),
        )

    first_detail = _first_contract_detail(contract_details)
    trading_hours = _contract_detail_value(first_detail, "tradingHours", "trading_hours")
    liquid_hours = _contract_detail_value(first_detail, "liquidHours", "liquid_hours")
    exchange_timezone = (
        _contract_detail_value(first_detail, "timeZoneId", "timeZoneID", "time_zone_id")
        or config.exchange_timezone
    )
    return evaluate_market_session(
        symbol=request.symbol,
        as_of=as_of,
        trading_hours=trading_hours,
        liquid_hours=liquid_hours,
        exchange_timezone=exchange_timezone,
    )


def _unknown_market_session_result(
    *,
    symbol: str,
    as_of: datetime,
    exchange_timezone: str,
    reason: str,
) -> MarketSessionGuardResult:
    return MarketSessionGuardResult(
        symbol=symbol,
        checked_at=as_of.isoformat(),
        market_session_status=MarketSessionStatus.UNKNOWN,
        can_submit_controlled_fill=False,
        reason=reason,
        source=FALLBACK_UNKNOWN_SOURCE,
        exchange_timezone=exchange_timezone,
        trading_hours_source=FALLBACK_UNKNOWN_SOURCE,
        liquid_hours_source=FALLBACK_UNKNOWN_SOURCE,
    )


def _first_contract_detail(contract_details: Any) -> Any:
    if isinstance(contract_details, Sequence) and not isinstance(
        contract_details, str | bytes | bytearray
    ):
        return contract_details[0] if contract_details else None
    return contract_details


def _contract_detail_value(contract_details: Any, *names: str) -> str | None:
    if contract_details is None:
        return None
    if isinstance(contract_details, Mapping):
        normalized = {str(key).lower(): value for key, value in contract_details.items()}
        for name in names:
            value = contract_details.get(name)
            if isinstance(value, str) and value.strip():
                return value.strip()
            value = normalized.get(name.lower())
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None
    for name in names:
        value = getattr(contract_details, name, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _merge_market_session_guard(
    payload: dict[str, Any],
    session_result: MarketSessionGuardResult,
) -> None:
    session_payload = session_result.as_payload()
    payload.update(
        {
            "market_session_status": session_payload["market_session_status"],
            "can_submit_controlled_fill": session_payload["can_submit_controlled_fill"],
            "market_session_source": session_payload["source"],
            "exchange_timezone": session_payload["exchange_timezone"],
            "trading_hours_source": session_payload["trading_hours_source"],
            "liquid_hours_source": session_payload["liquid_hours_source"],
            "session_guard_reason": session_payload["reason"],
            "session_guard_checked_at": session_payload["checked_at"],
        }
    )


def _session_guard_issue_code(status: MarketSessionStatus) -> str:
    return _SESSION_GUARD_ISSUE_CODES.get(status, "market_session_unknown")


def _order_intent_from_request(request: IBKRPaperOrderRequest, *, as_of: date) -> OrderIntent:
    return OrderIntent(
        intent_id=f"trading-013-{as_of.isoformat()}-{request.symbol}",
        created_at=datetime.now(tz=UTC),
        strategy_id="ibkr_paper_controlled_fill_test",
        strategy_version="trading-013-v1",
        run_id=f"trading-013-{as_of.isoformat()}",
        symbol=request.symbol,
        asset_type=AssetType.STOCK,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.DAY,
        target_quantity=1,
        limit_price=float(request.limit_price or 0),
        confidence=0.0,
        score_snapshot_id=f"controlled_fill_{as_of.isoformat()}",
        reason_codes=["diagnostic_only", "ibkr_paper_controlled_fill_test"],
        metadata={
            "controlled_fill_test": True,
            "production_effect": CONTROLLED_FILL_PRODUCTION_EFFECT,
        },
    )


def _merge_safety_checks(payload: dict[str, Any], checks: Sequence[SafetyCheck]) -> None:
    existing = {
        (check["name"], check["status"], check["message"])
        for check in payload["safety_checks"]
        if isinstance(check, Mapping)
    }
    for check in checks:
        check_payload = check.model_dump(mode="json")
        key = (check_payload["name"], check_payload["status"], check_payload["message"])
        if key not in existing:
            payload["safety_checks"].append(check_payload)
            existing.add(key)


def _merge_order_events(
    payload: dict[str, Any],
    events: list[dict[str, Any]],
    *,
    raw_broker_order_ids: Sequence[str],
    account_id: str,
) -> None:
    for event in events:
        redacted = _redact_broker_payload(
            event,
            raw_broker_order_ids=raw_broker_order_ids,
            account_id=account_id,
        )
        comparable = {key: value for key, value in redacted.items() if key != "observed_at"}
        if any(
            {key: value for key, value in existing.items() if key != "observed_at"} == comparable
            for existing in payload["order_status_events"]
        ):
            continue
        payload["order_status_events"].append(redacted)


def _merge_fills(
    payload: dict[str, Any],
    fills: list[Any],
    *,
    raw_broker_order_ids: Sequence[str],
    account_id: str,
) -> None:
    redacted_fills = _redact_broker_payload(
        fills,
        raw_broker_order_ids=raw_broker_order_ids,
        account_id=account_id,
    )
    if isinstance(redacted_fills, list):
        payload["fills"] = redacted_fills
    else:
        payload["fills"] = [redacted_fills] if redacted_fills else []


def _summarize_order_and_fills(payload: dict[str, Any]) -> None:
    order_summary = order_events_summary(payload["order_status_events"])
    payload["open_order_seen"] = bool(
        payload["open_order_seen"] or order_summary["open_order_seen"]
    )
    payload["final_order_status"] = order_summary["final_order_status"]

    fill_summary = _fill_summary(
        fills=payload["fills"],
        order_status_events=payload["order_status_events"],
    )
    payload.update(fill_summary)


def _fill_summary(
    *,
    fills: Sequence[Any],
    order_status_events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    total_quantity = 0.0
    weighted_notional = 0.0
    fill_time: str | None = None
    commission_report_seen = False

    for fill in fills:
        jsonable = fill if isinstance(fill, Mapping) else {}
        quantity = _first_positive_float(
            jsonable,
            (
                "shares",
                "qty",
                "quantity",
                "filled",
                "execution.shares",
                "execution.qty",
                "execution.cumQty",
            ),
        )
        price = _first_positive_float(
            jsonable,
            ("price", "avgPrice", "execution.price", "execution.avgPrice"),
        )
        if quantity is not None:
            total_quantity += quantity
            if price is not None:
                weighted_notional += quantity * price
        if fill_time is None:
            fill_time = _first_string(jsonable, ("time", "execution.time", "execution.execTime"))
        if _has_commission_report(jsonable):
            commission_report_seen = True

    status_fill_quantity = 0.0
    status_avg_price: float | None = None
    for event in order_status_events:
        status_filled = _positive_float_or_none(event.get("filled"))
        if status_filled is not None:
            status_fill_quantity = max(status_fill_quantity, status_filled)
        status_price = _positive_float_or_none(event.get("avg_fill_price"))
        if status_price is not None:
            status_avg_price = status_price

    if total_quantity <= 0 < status_fill_quantity:
        total_quantity = status_fill_quantity
        if status_avg_price is not None:
            weighted_notional = status_fill_quantity * status_avg_price

    avg_price = None
    if total_quantity > 0 and weighted_notional > 0:
        avg_price = round(weighted_notional / total_quantity, 6)
    elif status_avg_price is not None:
        avg_price = status_avg_price

    status_filled = any(
        str(event.get("status", "")).upper() == "FILLED" for event in order_status_events
    )
    fill_seen = bool(total_quantity > 0 or fills or status_filled)
    return {
        "fill_seen": fill_seen,
        "fill_quantity": total_quantity,
        "avg_fill_price": avg_price,
        "fill_time": fill_time,
        "commission_report_seen": commission_report_seen,
    }


def _run_local_fill_comparison(
    payload: dict[str, Any],
    *,
    order_intent: OrderIntent,
    as_of: date,
    market_snapshot: MarketSnapshot | Mapping[str, Any] | None,
    prices_path: Path | None,
) -> None:
    snapshot, source = _resolve_reliable_market_snapshot(
        order_intent.symbol,
        as_of=as_of,
        provided_snapshot=market_snapshot,
        prices_path=prices_path,
    )
    ibkr_avg_fill_price = payload.get("avg_fill_price")
    if snapshot is None:
        payload["paperbroker_comparison"] = {
            **_empty_paperbroker_comparison(),
            "ibkr_fill_seen": payload["fill_seen"],
            "ibkr_avg_fill_price": ibkr_avg_fill_price,
            "market_snapshot_source": source,
            "fill_match_status": "INSUFFICIENT_MARKET_DATA",
        }
        _add_issue(
            payload,
            LifecycleIssue(
                code="insufficient_market_data",
                severity="LIMITED",
                message=(
                    "Reliable MarketSnapshot is unavailable; local PaperBroker comparison "
                    "was skipped."
                ),
            ),
        )
        return

    broker = PaperBroker()
    order = broker.submit_order(order_intent)
    reports = broker.process_market_snapshot(snapshot)
    filled_report = next((report for report in reports if report.filled_quantity > 0), None)
    local_fill_seen = filled_report is not None
    local_avg_fill_price = None if filled_report is None else filled_report.avg_fill_price
    fill_price_diff = None
    if ibkr_avg_fill_price is not None and local_avg_fill_price is not None:
        fill_price_diff = round(float(local_avg_fill_price) - float(ibkr_avg_fill_price), 6)
    payload["paperbroker_comparison"] = {
        "local_fill_seen": local_fill_seen,
        "local_avg_fill_price": local_avg_fill_price,
        "ibkr_fill_seen": payload["fill_seen"],
        "ibkr_avg_fill_price": ibkr_avg_fill_price,
        "fill_price_diff": fill_price_diff,
        "fill_match_status": _fill_match_status(
            local_fill_seen=local_fill_seen,
            ibkr_fill_seen=payload["fill_seen"],
            fill_price_diff=fill_price_diff,
        ),
        "market_snapshot_source": source,
        "market_snapshot": snapshot.model_dump(mode="json"),
        "local_broker_order_id": order.broker_order_id,
        "local_execution_reports": [report.model_dump(mode="json") for report in reports],
    }


def _resolve_reliable_market_snapshot(
    symbol: str,
    *,
    as_of: date,
    provided_snapshot: MarketSnapshot | Mapping[str, Any] | None,
    prices_path: Path | None,
) -> tuple[MarketSnapshot | None, str]:
    if isinstance(provided_snapshot, MarketSnapshot):
        return provided_snapshot, "provided_market_snapshot"
    if isinstance(provided_snapshot, Mapping):
        return (
            MarketSnapshot.model_validate({**provided_snapshot, "symbol": symbol}),
            "provided_market_snapshot",
        )
    historical = _historical_market_snapshot(
        symbol,
        as_of=as_of,
        prices_path=prices_path or PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    )
    if historical is not None:
        return historical, "historical_ohlc"
    return None, "missing_reliable_market_snapshot"


def _historical_market_snapshot(
    symbol: str,
    *,
    as_of: date,
    prices_path: Path,
) -> MarketSnapshot | None:
    if not prices_path.exists():
        return None
    target_symbol = symbol.upper()
    try:
        with prices_path.open("r", encoding="utf-8", newline="") as file:
            for row in csv.DictReader(file):
                row_symbol = str(row.get("ticker") or row.get("symbol") or "").upper()
                if row_symbol != target_symbol or str(row.get("date") or "") != as_of.isoformat():
                    continue
                open_price = _positive_float_or_none(row.get("open"))
                high_price = _positive_float_or_none(row.get("high"))
                low_price = _positive_float_or_none(row.get("low"))
                close_price = _positive_float_or_none(row.get("close"))
                if None in {open_price, high_price, low_price, close_price}:
                    continue
                return MarketSnapshot(
                    symbol=target_symbol,
                    timestamp=datetime.combine(as_of, time(20, 0), tzinfo=UTC),
                    open=float(open_price),
                    high=float(high_price),
                    low=float(low_price),
                    last=float(close_price),
                )
    except OSError:
        return None
    return None


def _fill_match_status(
    *,
    local_fill_seen: bool,
    ibkr_fill_seen: bool,
    fill_price_diff: float | None,
) -> str:
    if local_fill_seen != ibkr_fill_seen:
        return "FILL_OBSERVATION_DIFFERENCE"
    if fill_price_diff is None:
        return "OBSERVE_ONLY"
    if fill_price_diff == 0:
        return "EXACT_MATCH"
    return "PRICE_DIFFERENCE"


def _write_outputs(
    *,
    payload: dict[str, Any],
    as_of: date,
    output_dir: Path,
    config: ControlledFillConfig | None,
    raw_broker_order_ids: Sequence[str],
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = default_controlled_fill_json_path(output_dir, as_of)
    markdown_path = json_path.with_suffix(".md")
    payload["output_paths"] = {"json": str(json_path), "markdown": str(markdown_path)}
    sanitized_payload = sanitize_ibkr_payload(
        _redact_broker_payload(
            payload,
            raw_broker_order_ids=raw_broker_order_ids,
            account_id=config.account_id if config else "",
        ),
        account_id=config.account_id if config else "",
    )
    sanitized_payload["production_effect"] = CONTROLLED_FILL_PRODUCTION_EFFECT
    json_text = json.dumps(sanitized_payload, ensure_ascii=False, indent=2)
    markdown_text = render_controlled_fill_markdown(sanitized_payload)
    _assert_no_sensitive_output(
        json_text,
        account_id=config.account_id if config else "",
        raw_broker_order_ids=raw_broker_order_ids,
    )
    _assert_no_sensitive_output(
        markdown_text,
        account_id=config.account_id if config else "",
        raw_broker_order_ids=raw_broker_order_ids,
    )
    json_path.write_text(json_text + "\n", encoding="utf-8")
    markdown_path.write_text(markdown_text, encoding="utf-8")
    return sanitized_payload


def _test_status_from_payload(payload: Mapping[str, Any]) -> str:
    issues = payload.get("issues", [])
    if any(isinstance(issue, Mapping) and issue.get("severity") == "ERROR" for issue in issues):
        return ControlledFillStatus.ERROR.value
    if any(isinstance(issue, Mapping) and issue.get("severity") == "BLOCK" for issue in issues):
        return ControlledFillStatus.BLOCK.value
    if issues or not payload.get("fill_seen"):
        return ControlledFillStatus.LIMITED.value
    return ControlledFillStatus.PASS.value


def _open_order_seen(
    *,
    order_status_events: list[dict[str, Any]],
    open_orders: Any,
) -> bool:
    summary = order_events_summary(order_status_events)
    return bool(summary["open_order_seen"] or open_orders)


def _cancelled_confirmed(payload: Mapping[str, Any]) -> bool:
    status = str(payload.get("final_order_status") or "").upper()
    return status in {"APICANCELLED", "CANCELLED", "INACTIVE"}


def _try_cancel_after_error(
    adapter: IBKRPaperOrderLifecycleAdapter,
    submitted_order: Any,
    payload: dict[str, Any],
) -> bool:
    try:
        adapter.cancel_order(submitted_order)
    except Exception as exc:
        _add_issue(
            payload,
            LifecycleIssue(
                code="cancel_after_error_failed",
                severity="ERROR",
                message=sanitize_ibkr_payload(str(exc), account_id=""),
                details={"error_type": type(exc).__name__},
            ),
        )
        return False
    return True


def _add_issue(payload: dict[str, Any], issue: LifecycleIssue) -> None:
    issue_payload = issue.model_dump(mode="json")
    if issue_payload not in payload["issues"]:
        payload["issues"].append(issue_payload)


def _redact_broker_payload(
    value: Any,
    *,
    raw_broker_order_ids: Sequence[str],
    account_id: str,
) -> Any:
    sanitized = sanitize_ibkr_payload(value, account_id=account_id)
    return _redact_broker_value(sanitized, raw_broker_order_ids=raw_broker_order_ids)


def _redact_broker_value(value: Any, *, raw_broker_order_ids: Sequence[str]) -> Any:
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = str(key).lower().replace("-", "_")
            if normalized_key in _BROKER_ORDER_ID_KEYS:
                redacted[str(key)] = _redacted_broker_order_id(item)
            else:
                redacted[str(key)] = _redact_broker_value(
                    item,
                    raw_broker_order_ids=raw_broker_order_ids,
                )
        return redacted
    if isinstance(value, list):
        return [
            _redact_broker_value(item, raw_broker_order_ids=raw_broker_order_ids) for item in value
        ]
    if isinstance(value, str):
        return _redact_broker_order_ids_in_text(value, raw_broker_order_ids=raw_broker_order_ids)
    return value


def _redact_broker_order_ids_in_text(
    text: str,
    *,
    raw_broker_order_ids: Sequence[str],
) -> str:
    redacted = text
    for raw_id in raw_broker_order_ids:
        raw_id_text = str(raw_id or "")
        if not raw_id_text:
            continue
        replacement = _redacted_broker_order_id(raw_id_text) or ""
        redacted = _contextual_broker_order_id_pattern(raw_id_text).sub(
            lambda match, replacement=replacement: (
                f"{match.group('prefix')}{replacement}{match.group('suffix')}"
            ),
            redacted,
        )
        if _allow_global_string_broker_id_scan(raw_id_text):
            redacted = re.sub(
                rf"(?<![\w.-]){re.escape(raw_id_text)}(?![\w.-])",
                replacement,
                redacted,
            )
    return redacted


def _allow_global_string_broker_id_scan(raw_id: str) -> bool:
    return len(raw_id) >= _MIN_GLOBAL_STRING_BROKER_ID_LENGTH and not raw_id.isdigit()


def _contextual_broker_order_id_pattern(raw_id: str) -> re.Pattern[str]:
    escaped = re.escape(raw_id)
    return re.compile(
        rf"(?P<prefix>"
        rf"(?:\"{_JSON_BROKER_ORDER_ID_KEY}\"\s*:\s*[`'\"]?)"
        rf"|(?:\b{_CONTEXTUAL_BROKER_ORDER_ID_LABEL}\b\s*(?:[:=：#]|is)?\s*[`'\"]?)"
        rf")"
        rf"{escaped}"
        rf"(?P<suffix>[`'\"]?)"
        rf"(?=$|[\s,.;)\]}}])",
        flags=re.IGNORECASE,
    )


def _redacted_broker_order_id(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, str) and value.startswith("[REDACTED_BROKER_ORDER_ID:"):
        return value
    return f"[REDACTED_BROKER_ORDER_ID:len={len(str(value))}]"


def _empty_paperbroker_comparison() -> dict[str, Any]:
    return {
        "local_fill_seen": None,
        "local_avg_fill_price": None,
        "ibkr_fill_seen": None,
        "ibkr_avg_fill_price": None,
        "fill_price_diff": None,
        "fill_match_status": "NOT_RUN",
        "market_snapshot_source": "not_run",
    }


def _first_positive_float(payload: Mapping[str, Any], keys: Sequence[str]) -> float | None:
    for key in keys:
        value = _nested_value(payload, key)
        parsed = _positive_float_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _first_string(payload: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    for key in keys:
        value = _nested_value(payload, key)
        if value not in (None, ""):
            return str(value)
    return None


def _nested_value(payload: Mapping[str, Any], dotted_key: str) -> Any:
    value: Any = payload
    for part in dotted_key.split("."):
        if not isinstance(value, Mapping):
            return None
        value = value.get(part)
    return value


def _has_commission_report(payload: Mapping[str, Any]) -> bool:
    if "commissionReport" in payload or "commission_report" in payload:
        return True
    commission = _first_positive_float(
        payload,
        ("commission", "commissionReport.commission", "commission_report.commission"),
    )
    return commission is not None


def _positive_float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _pass(name: str, message: str, details: dict[str, Any] | None = None) -> SafetyCheck:
    return SafetyCheck(name=name, status="PASS", message=message, details=details or {})


def _block(name: str, message: str, details: dict[str, Any] | None = None) -> SafetyCheck:
    return SafetyCheck(name=name, status="BLOCK", message=message, details=details or {})


def _json_block(value: Any) -> str:
    return "```json\n" + json.dumps(value, ensure_ascii=False, indent=2) + "\n```"


def _assert_no_sensitive_output(
    text: str,
    *,
    account_id: str,
    raw_broker_order_ids: Sequence[str],
) -> None:
    if account_id and account_id in text:
        raise RuntimeError("controlled fill output contains an unmasked account id")
    if re.search(r"\b(?:DUP?|U)\d{5,}\b", text, flags=re.IGNORECASE):
        raise RuntimeError("controlled fill output contains an unmasked account id")
    for raw_id in raw_broker_order_ids:
        raw_id_text = str(raw_id or "")
        if not raw_id_text:
            continue
        context_pattern = _contextual_broker_order_id_pattern(raw_id_text)
        if context_pattern.search(text):
            raise RuntimeError("controlled fill output contains an unredacted broker order id")
        if _allow_global_string_broker_id_scan(raw_id_text) and re.search(
            rf"(?<![\w.-]){re.escape(raw_id_text)}(?![\w.-])",
            text,
        ):
            raise RuntimeError("controlled fill output contains an unredacted broker order id")


__all__ = [
    "CONTROLLED_FILL_PRODUCTION_EFFECT",
    "CONTROLLED_FILL_REPORT_TYPE",
    "ControlledFillConfig",
    "ControlledFillStatus",
    "DEFAULT_IBKR_PAPER_CONTROLLED_FILL_CONFIG_PATH",
    "default_controlled_fill_json_path",
    "load_controlled_fill_config",
    "render_controlled_fill_markdown",
    "run_ibkr_paper_controlled_fill_test",
]
