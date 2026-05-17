from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.brokers.ibkr_paper_order import (
    DEFAULT_IBKR_PAPER_ORDER_CONFIG_PATH,
    IBKRPaperOrderConfig,
    IBKRPaperOrderLifecycleAdapter,
    IBKRPaperOrderRequest,
    LifecycleIssue,
    load_ibkr_paper_order_config,
    order_events_summary,
    safety_checks_to_issues,
)
from ai_trading_system.trading_engine.brokers.ibkr_readonly import (
    mask_account_id,
    sanitize_ibkr_payload,
)
from ai_trading_system.trading_engine.execution.paper_broker import PaperBroker
from ai_trading_system.trading_engine.schemas.broker_order import OrderStatus
from ai_trading_system.trading_engine.schemas.market import MarketSnapshot
from ai_trading_system.trading_engine.schemas.order_intent import (
    AssetType,
    OrderIntent,
    OrderSide,
    OrderType,
    TimeInForce,
)

COMPARISON_REPORT_TYPE = "paperbroker_vs_ibkr_paper_comparison"
COMPARISON_PRODUCTION_EFFECT = "none"
COMPARISON_MODE = "diagnostic_only"
DEFAULT_COMPARISON_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "reports"
DEFAULT_RECOMMENDATIONS = [
    "consider stricter fill simulation",
    "require historical bid/ask",
    "mark synthetic snapshot replay as LIMITED",
    "add broker_rejection_reason mapping",
]
ALLOWED_DIFFERENCE_LABELS = {
    "EXPECTED_DIFFERENCE",
    "LOCAL_SIM_TOO_OPTIMISTIC",
    "BROKER_REJECTED",
    "INSUFFICIENT_MARKET_DATA",
    "CANCEL_TIMING_DIFFERENCE",
}
_FINAL_CANCELLED = {"CANCELLED", "APICANCELLED"}
_BROKER_REJECTED = {"REJECTED", "INACTIVE"}


def run_paperbroker_vs_ibkr_paper_comparison(
    *,
    as_of: date,
    config_path: Path | str = DEFAULT_IBKR_PAPER_ORDER_CONFIG_PATH,
    output_dir: Path | str = DEFAULT_COMPARISON_OUTPUT_DIR,
    client: Any | None = None,
    intent_fixture_path: Path | str | None = None,
    symbol: str = "NVDA",
    side: str = "BUY",
    quantity: int = 1,
    limit_price: float | None = None,
    asset_type: str = "stock",
    order_type: str = "LIMIT",
    time_in_force: str = "DAY",
    margin: bool = False,
    bracket_order: bool = False,
    stop_price: float | None = None,
    trailing_amount: float | None = None,
    algo_strategy: str | None = None,
    parent_order_id: str | None = None,
    oca_group: str | None = None,
    local_snapshot: MarketSnapshot | Mapping[str, Any] | None = None,
    local_price_source: str | None = None,
) -> dict[str, Any]:
    config: IBKRPaperOrderConfig | None = None
    adapter: IBKRPaperOrderLifecycleAdapter | None = None
    submitted_order: Any | None = None
    raw_broker_order_ids: list[str] = []
    stage = "load_config"
    payload = _base_payload(as_of=as_of, config_path=Path(config_path))

    try:
        config = load_ibkr_paper_order_config(config_path)
        _merge_config_metadata(payload, config)

        raw_request = _comparison_input_payload(
            intent_fixture_path=intent_fixture_path,
            symbol=symbol,
            side=side,
            quantity=quantity,
            limit_price=limit_price,
            asset_type=asset_type,
            order_type=order_type,
            time_in_force=time_in_force,
            margin=margin,
            bracket_order=bracket_order,
            stop_price=stop_price,
            trailing_amount=trailing_amount,
            algo_strategy=algo_strategy,
            parent_order_id=parent_order_id,
            oca_group=oca_group,
        )
        payload["requested_order"] = _public_requested_order(raw_request)

        stage = "config_safety"
        config_issues = _config_safety_issues(config)
        order_issues = _order_input_issues(raw_request, allowed_symbols=config.allowed_symbols)
        for issue in config_issues + order_issues:
            _add_issue(payload, issue)
        if config_issues or order_issues:
            payload["comparison_status"] = "BLOCK"
            return _write_comparison_outputs(
                payload=payload,
                as_of=as_of,
                output_dir=Path(output_dir),
                config=config,
                raw_broker_order_ids=raw_broker_order_ids,
            )

        order_intent = _order_intent_from_input(raw_request, as_of=as_of)
        payload["order_intent"] = order_intent.model_dump(mode="json")

        stage = "local_paperbroker"
        local_result = _run_local_paperbroker(
            order_intent,
            local_snapshot=local_snapshot,
            local_price_source=local_price_source,
        )
        payload["local"] = local_result

        stage = "connect"
        adapter = IBKRPaperOrderLifecycleAdapter(config=config, client=client)
        payload["ibkr"]["connection_status"] = adapter.connect()

        stage = "ibkr_order_safety"
        request = IBKRPaperOrderRequest(
            symbol=order_intent.symbol,
            side=order_intent.side.value,
            quantity=int(order_intent.target_quantity or 0),
            limit_price=order_intent.limit_price,
            asset_type=order_intent.asset_type.value,
            order_type=order_intent.order_type.value,
            time_in_force=order_intent.time_in_force.value,
            margin=margin,
            bracket_order=bracket_order,
            stop_price=stop_price,
            trailing_amount=trailing_amount,
            algo_strategy=algo_strategy,
            parent_order_id=parent_order_id,
            oca_group=oca_group,
        )
        safety_checks = adapter.validate_order_request(request)
        payload["safety_checks"] = [check.model_dump(mode="json") for check in safety_checks]
        blocking_issues = safety_checks_to_issues(safety_checks)
        if blocking_issues:
            for issue in blocking_issues:
                _add_issue(payload, issue)
            payload["comparison_status"] = "BLOCK"
            return _finalize_and_write(
                payload=payload,
                as_of=as_of,
                output_dir=Path(output_dir),
                config=config,
                raw_broker_order_ids=raw_broker_order_ids,
            )

        stage = "submit_ibkr_order"
        submitted_order = adapter.submit_order(request)
        if submitted_order.broker_order_id:
            raw_broker_order_ids.append(submitted_order.broker_order_id)
        payload["ibkr"]["broker_order_id"] = _redacted_broker_order_id(
            submitted_order.broker_order_id
        )
        payload["ibkr"]["submitted_order"] = _redact_broker_payload(
            submitted_order.submitted_order,
            raw_broker_order_ids=raw_broker_order_ids,
            account_id=config.account_id,
        )

        stage = "wait_ibkr_open_status"
        _merge_ibkr_order_events(
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
        payload["ibkr"]["open_orders_seen_payload"] = open_orders
        payload["ibkr"]["open_order_seen"] = _open_order_seen(
            order_status_events=payload["ibkr"]["order_status_events"],
            open_orders=open_orders,
        )

        stage = "cancel_ibkr_order"
        payload["ibkr"]["cancel_requested"] = True
        adapter.cancel_order(submitted_order)

        stage = "wait_ibkr_cancel_status"
        _merge_ibkr_order_events(
            payload,
            adapter.wait_for_order_events(
                submitted_order.trade,
                timeout_seconds=config.cancel_timeout_seconds,
            ),
            raw_broker_order_ids=raw_broker_order_ids,
            account_id=config.account_id,
        )
        fills = _redact_broker_payload(
            adapter.get_fills(submitted_order.trade),
            raw_broker_order_ids=raw_broker_order_ids,
            account_id=config.account_id,
        )
        payload["ibkr"]["fills"] = fills
        payload["ibkr"]["fills_seen"] = bool(fills)
        _summarize_ibkr_lifecycle(payload)
        _finalize_comparison(payload)
    except Exception as exc:
        if adapter is not None and submitted_order is not None:
            _try_cancel_ibkr_after_error(adapter, submitted_order, payload)
        severity = "BLOCK" if stage in {"load_config", "config_safety"} else "ERROR"
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
        payload["comparison_status"] = severity
    finally:
        if adapter is not None:
            adapter.disconnect()

    return _finalize_and_write(
        payload=payload,
        as_of=as_of,
        output_dir=Path(output_dir),
        config=config,
        raw_broker_order_ids=raw_broker_order_ids,
    )


def render_paperbroker_vs_ibkr_markdown(payload: dict[str, Any]) -> str:
    diff = payload.get("diff", {})
    local = payload.get("local", {})
    ibkr = payload.get("ibkr", {})
    lines = [
        "# PaperBroker vs IBKR Paper Comparison",
        "",
        f"- 日期：{payload['as_of']}",
        f"- comparison_status：{payload['comparison_status']}",
        f"- comparison_mode={payload['comparison_mode']}",
        f"- production_effect={payload['production_effect']}",
        (
            "- 安全边界：本报告只做 diagnostic comparison；不影响 daily-run、"
            "dashboard production conclusion、paper signal quality、shadow impact、"
            "参数晋级或交易建议。"
        ),
        "",
        "## OrderIntent",
        "",
        _json_block(payload.get("order_intent") or payload.get("requested_order", {})),
        "",
        "## Local PaperBroker",
        "",
        f"- local_order_status：{local.get('local_order_status')}",
        f"- local_open_order_seen：{str(local.get('local_open_order_seen')).lower()}",
        f"- local_fill_seen：{str(local.get('local_fill_seen')).lower()}",
        f"- local_avg_fill_price：{local.get('local_avg_fill_price')}",
        f"- local_cancel_result：{local.get('local_cancel_result')}",
        f"- local_final_status：{local.get('local_final_status')}",
        f"- local_reconciliation_status：{local.get('local_reconciliation_status')}",
        "",
        "## IBKR Paper",
        "",
        f"- broker_order_id：`{ibkr.get('broker_order_id') or 'missing'}`",
        f"- openOrder seen：{str(ibkr.get('open_order_seen')).lower()}",
        f"- cancel requested：{str(ibkr.get('cancel_requested')).lower()}",
        f"- final status：{ibkr.get('final_status')}",
        f"- fills seen：{str(ibkr.get('fills_seen')).lower()}",
        f"- ibkr_reconciliation_status：{ibkr.get('ibkr_reconciliation_status')}",
        "",
        "## Diff",
        "",
        f"- status_match：{str(diff.get('status_match')).lower()}",
        f"- fill_match：{str(diff.get('fill_match')).lower()}",
        f"- cancel_match：{str(diff.get('cancel_match')).lower()}",
        (
            "- local_filled_but_ibkr_not_filled："
            f"{str(diff.get('local_filled_but_ibkr_not_filled')).lower()}"
        ),
        (
            "- ibkr_rejected_but_local_accepted："
            f"{str(diff.get('ibkr_rejected_but_local_accepted')).lower()}"
        ),
        f"- local_price_source：{diff.get('local_price_source')}",
        (
            "- ibkr_reference_price_available："
            f"{str(diff.get('ibkr_reference_price_available')).lower()}"
        ),
        f"- lifecycle_event_gap：{', '.join(diff.get('lifecycle_event_gap', [])) or 'none'}",
        f"- difference_labels：{', '.join(payload.get('difference_labels', [])) or 'none'}",
        "",
        "## Recommendations",
        "",
        *[f"- {recommendation}" for recommendation in payload.get("recommendations", [])],
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


def default_comparison_json_path(reports_dir: Path, as_of: date) -> Path:
    return reports_dir / f"{COMPARISON_REPORT_TYPE}_{as_of.isoformat()}.json"


def _base_payload(*, as_of: date, config_path: Path) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "report_type": COMPARISON_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "config_path": str(config_path),
        "comparison_status": "BLOCK",
        "comparison_mode": COMPARISON_MODE,
        "production_effect": COMPARISON_PRODUCTION_EFFECT,
        "trading_mode": "unknown",
        "configured_production_effect": "unknown",
        "paper_order_lifecycle_enabled": False,
        "ibkr_paper_comparison_enabled": False,
        "account_id_masked": "missing",
        "manual_cli_only": True,
        "production_surface_impact": {
            "daily_run": "none",
            "dashboard_production_conclusion": "none",
            "paper_signal_quality": "none",
            "shadow_impact": "none",
            "parameter_promotion": "none",
            "trading_advice": "none",
        },
        "requested_order": {},
        "order_intent": {},
        "local": _empty_local_result(),
        "ibkr": _empty_ibkr_result(),
        "diff": _empty_diff(),
        "difference_labels": [],
        "recommendations": list(DEFAULT_RECOMMENDATIONS),
        "safety_checks": [],
        "issues": [],
    }


def _merge_config_metadata(payload: dict[str, Any], config: IBKRPaperOrderConfig) -> None:
    payload.update(
        {
            "trading_mode": config.trading_mode,
            "configured_production_effect": config.production_effect,
            "paper_order_lifecycle_enabled": config.paper_order_lifecycle_enabled,
            "ibkr_paper_comparison_enabled": config.ibkr_paper_comparison_enabled,
            "account_id_masked": mask_account_id(config.account_id),
        }
    )


def _comparison_input_payload(
    *,
    intent_fixture_path: Path | str | None,
    symbol: str,
    side: str,
    quantity: int,
    limit_price: float | None,
    asset_type: str,
    order_type: str,
    time_in_force: str,
    margin: bool,
    bracket_order: bool,
    stop_price: float | None,
    trailing_amount: float | None,
    algo_strategy: str | None,
    parent_order_id: str | None,
    oca_group: str | None,
) -> dict[str, Any]:
    if intent_fixture_path is not None:
        raw = json.loads(Path(intent_fixture_path).read_text(encoding="utf-8"))
        if isinstance(raw, Mapping) and isinstance(raw.get("order_intent"), Mapping):
            raw = raw["order_intent"]
        if not isinstance(raw, Mapping):
            raise ValueError("OrderIntent fixture must contain a JSON object")
        quantity_value = raw.get("target_quantity", raw.get("quantity", quantity))
        return {
            "input_source": "order_intent_fixture",
            "fixture_path": str(intent_fixture_path),
            "symbol": str(raw.get("symbol", symbol)),
            "side": str(raw.get("side", side)),
            "quantity": int(quantity_value) if quantity_value is not None else 0,
            "limit_price": raw.get("limit_price", limit_price),
            "asset_type": str(raw.get("asset_type", asset_type)),
            "order_type": str(raw.get("order_type", order_type)),
            "time_in_force": str(raw.get("time_in_force", time_in_force)),
            "margin": bool(raw.get("margin", margin)),
            "bracket_order": bool(raw.get("bracket_order", bracket_order)),
            "stop_price": raw.get("stop_price", stop_price),
            "trailing_amount": raw.get("trailing_amount", trailing_amount),
            "algo_strategy": raw.get("algo_strategy", algo_strategy),
            "parent_order_id": raw.get("parent_order_id", parent_order_id),
            "oca_group": raw.get("oca_group", oca_group),
        }
    return {
        "input_source": "cli_parameters",
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "limit_price": limit_price,
        "asset_type": asset_type,
        "order_type": order_type,
        "time_in_force": time_in_force,
        "margin": margin,
        "bracket_order": bracket_order,
        "stop_price": stop_price,
        "trailing_amount": trailing_amount,
        "algo_strategy": algo_strategy,
        "parent_order_id": parent_order_id,
        "oca_group": oca_group,
    }


def _public_requested_order(raw_request: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "input_source": raw_request.get("input_source"),
        "symbol": str(raw_request.get("symbol", "")).strip().upper(),
        "asset_type": str(raw_request.get("asset_type", "")).strip().lower(),
        "side": str(raw_request.get("side", "")).strip().upper(),
        "order_type": str(raw_request.get("order_type", "")).strip().upper(),
        "time_in_force": str(raw_request.get("time_in_force", "")).strip().upper(),
        "quantity": raw_request.get("quantity"),
        "limit_price": raw_request.get("limit_price"),
        "production_effect": COMPARISON_PRODUCTION_EFFECT,
    }


def _config_safety_issues(config: IBKRPaperOrderConfig) -> list[LifecycleIssue]:
    issues: list[LifecycleIssue] = []
    if config.trading_mode != "paper":
        issues.append(_block_issue("trading_mode", "trading_mode must be paper"))
    if config.production_effect != COMPARISON_PRODUCTION_EFFECT:
        issues.append(_block_issue("production_effect", "production_effect must be none"))
    if not config.paper_order_lifecycle_enabled:
        issues.append(
            _block_issue(
                "paper_order_lifecycle_enabled",
                "paper_order_lifecycle_enabled must be true",
            )
        )
    if not config.ibkr_paper_comparison_enabled:
        issues.append(
            _block_issue(
                "ibkr_paper_comparison_enabled",
                "ibkr_paper_comparison_enabled must be true",
            )
        )
    if not config.account_id.upper().startswith("DUP"):
        issues.append(_block_issue("account_id", "account_id must start with DUP"))
    return issues


def _order_input_issues(
    raw_request: Mapping[str, Any],
    *,
    allowed_symbols: Sequence[str],
) -> list[LifecycleIssue]:
    issues: list[LifecycleIssue] = []
    request = _public_requested_order(raw_request)
    if request["asset_type"] != "stock":
        issues.append(_block_issue("asset_type", "only stock orders are allowed"))
    if request["order_type"] != "LIMIT":
        issues.append(_block_issue("order_type", "only LIMIT orders are allowed"))
    if request["time_in_force"] != "DAY":
        issues.append(_block_issue("time_in_force", "only DAY time_in_force is allowed"))
    if request["side"] != "BUY":
        issues.append(_block_issue("side", "only BUY is allowed; short/SELL is forbidden"))
    if request["symbol"] not in {symbol.upper() for symbol in allowed_symbols}:
        issues.append(
            _block_issue(
                "symbol_whitelist",
                "symbol is not in the configured comparison whitelist",
            )
        )
    try:
        parsed_quantity = int(request["quantity"])
    except (TypeError, ValueError):
        parsed_quantity = 0
    if parsed_quantity != 1:
        issues.append(_block_issue("quantity", "quantity must equal 1"))
    try:
        parsed_limit_price = float(request["limit_price"])
    except (TypeError, ValueError):
        parsed_limit_price = 0.0
    if parsed_limit_price <= 0:
        issues.append(_block_issue("limit_price", "positive LIMIT price is required"))
    if bool(raw_request.get("margin")):
        issues.append(_block_issue("margin", "margin semantics are forbidden"))
    if bool(raw_request.get("bracket_order")):
        issues.append(_block_issue("bracket_order", "bracket orders are forbidden"))
    if raw_request.get("stop_price") is not None:
        issues.append(_block_issue("stop_order", "stop orders are forbidden"))
    if raw_request.get("trailing_amount") is not None:
        issues.append(_block_issue("trailing_order", "trailing orders are forbidden"))
    if raw_request.get("algo_strategy") is not None:
        issues.append(_block_issue("algo_order", "algo orders are forbidden"))
    if raw_request.get("parent_order_id") is not None or raw_request.get("oca_group") is not None:
        issues.append(_block_issue("linked_order", "linked orders are forbidden"))
    return issues


def _order_intent_from_input(raw_request: Mapping[str, Any], *, as_of: date) -> OrderIntent:
    request = _public_requested_order(raw_request)
    return OrderIntent(
        intent_id=f"trading-011-{as_of.isoformat()}-{request['symbol']}",
        created_at=datetime.now(tz=UTC),
        strategy_id="paperbroker_vs_ibkr_paper_comparison",
        strategy_version="trading-011-v1",
        run_id=f"trading-011-{as_of.isoformat()}",
        symbol=str(request["symbol"]),
        asset_type=AssetType.STOCK,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.DAY,
        target_quantity=1,
        limit_price=float(request["limit_price"]),
        confidence=0.0,
        score_snapshot_id=f"diagnostic_only_{as_of.isoformat()}",
        reason_codes=["diagnostic_only", "paperbroker_vs_ibkr_paper_comparison"],
        metadata={
            "comparison_mode": COMPARISON_MODE,
            "production_effect": COMPARISON_PRODUCTION_EFFECT,
            "input_source": request["input_source"],
        },
    )


def _run_local_paperbroker(
    order_intent: OrderIntent,
    *,
    local_snapshot: MarketSnapshot | Mapping[str, Any] | None,
    local_price_source: str | None,
) -> dict[str, Any]:
    broker = PaperBroker()
    order = broker.submit_order(order_intent)
    open_order_seen = any(
        open_order.broker_order_id == order.broker_order_id
        for open_order in broker.list_open_orders()
    )
    snapshot, price_source = _local_market_snapshot(
        order_intent,
        local_snapshot=local_snapshot,
        local_price_source=local_price_source,
    )
    reports = broker.process_market_snapshot(snapshot)
    filled_report = next((report for report in reports if report.filled_quantity > 0), None)
    fill_seen = filled_report is not None
    cancel_result = "NOT_REQUESTED_FILLED" if fill_seen else "UNKNOWN"
    cancel_error: str | None = None
    if not fill_seen:
        try:
            cancelled_order = broker.cancel_order(order.broker_order_id)
            cancel_result = cancelled_order.status.value
        except Exception as exc:
            cancel_result = "ERROR"
            cancel_error = str(exc)

    final_order = broker.get_order(order.broker_order_id)
    reconciliation_status = "PASS" if cancel_error is None else "LIMITED"
    return {
        "local_order_status": order.status.value,
        "local_open_order_seen": open_order_seen,
        "local_fill_seen": fill_seen,
        "local_avg_fill_price": None if filled_report is None else filled_report.avg_fill_price,
        "local_cancel_result": cancel_result,
        "local_final_status": final_order.status.value,
        "local_reconciliation_status": reconciliation_status,
        "local_price_source": price_source,
        "market_snapshot": snapshot.model_dump(mode="json"),
        "execution_reports": [report.model_dump(mode="json") for report in reports],
        "cancel_error": cancel_error,
    }


def _local_market_snapshot(
    order_intent: OrderIntent,
    *,
    local_snapshot: MarketSnapshot | Mapping[str, Any] | None,
    local_price_source: str | None,
) -> tuple[MarketSnapshot, str]:
    if isinstance(local_snapshot, MarketSnapshot):
        return local_snapshot, local_price_source or "provided_local_snapshot"
    if isinstance(local_snapshot, Mapping):
        return (
            MarketSnapshot.model_validate({**local_snapshot, "symbol": order_intent.symbol}),
            local_price_source or "provided_local_snapshot",
        )
    reference = max(order_intent.limit_price * 2.0, order_intent.limit_price + 1.0)
    return (
        MarketSnapshot(
            symbol=order_intent.symbol,
            open=reference,
            high=reference * 1.01,
            low=reference,
            last=reference,
        ),
        "synthetic_far_from_market_snapshot",
    )


def _merge_ibkr_order_events(
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
            for existing in payload["ibkr"]["order_status_events"]
        ):
            continue
        payload["ibkr"]["order_status_events"].append(redacted)


def _open_order_seen(
    *,
    order_status_events: list[dict[str, Any]],
    open_orders: Any,
) -> bool:
    summary = order_events_summary(order_status_events)
    return bool(summary["open_order_seen"] or open_orders)


def _summarize_ibkr_lifecycle(payload: dict[str, Any]) -> None:
    summary = order_events_summary(payload["ibkr"]["order_status_events"])
    payload["ibkr"]["open_order_seen"] = bool(
        payload["ibkr"]["open_order_seen"] or summary["open_order_seen"]
    )
    payload["ibkr"]["final_status"] = summary["final_order_status"]
    payload["ibkr"]["cancelled_confirmed"] = summary["cancelled_confirmed"]
    payload["ibkr"]["ibkr_reconciliation_status"] = _ibkr_reconciliation_status(payload["ibkr"])


def _ibkr_reconciliation_status(ibkr: Mapping[str, Any]) -> str:
    final_status = _normalize_broker_status(str(ibkr.get("final_status", "UNKNOWN")))
    if final_status == "REJECTED":
        return "LIMITED"
    if ibkr.get("cancel_requested") and final_status == "CANCELLED":
        return "PASS"
    if ibkr.get("fills_seen"):
        return "LIMITED"
    return "LIMITED"


def _finalize_comparison(payload: dict[str, Any]) -> None:
    diff = _comparison_diff(payload["local"], payload["ibkr"])
    labels = _difference_labels(diff=diff, ibkr=payload["ibkr"])
    payload["diff"] = diff
    payload["difference_labels"] = labels
    payload["comparison_status"] = "DIAGNOSTIC_DIFFERENCE" if labels else "PASS"


def _finalize_and_write(
    *,
    payload: dict[str, Any],
    as_of: date,
    output_dir: Path,
    config: IBKRPaperOrderConfig | None,
    raw_broker_order_ids: Sequence[str],
) -> dict[str, Any]:
    return _write_comparison_outputs(
        payload=payload,
        as_of=as_of,
        output_dir=output_dir,
        config=config,
        raw_broker_order_ids=raw_broker_order_ids,
    )


def _comparison_diff(local: Mapping[str, Any], ibkr: Mapping[str, Any]) -> dict[str, Any]:
    local_final = _normalize_broker_status(str(local.get("local_final_status", "UNKNOWN")))
    ibkr_final = _normalize_broker_status(str(ibkr.get("final_status", "UNKNOWN")))
    local_cancelled = str(local.get("local_cancel_result")) == OrderStatus.CANCELLED.value
    ibkr_cancelled = bool(ibkr.get("cancelled_confirmed"))
    local_fill_seen = bool(local.get("local_fill_seen"))
    ibkr_fill_seen = bool(ibkr.get("fills_seen"))
    gaps: list[str] = []
    if local_final != ibkr_final:
        gaps.append("status_mismatch")
    if local_fill_seen != ibkr_fill_seen:
        gaps.append("fill_mismatch")
    if local_cancelled != ibkr_cancelled:
        gaps.append("cancel_mismatch")
    if not local.get("local_open_order_seen"):
        gaps.append("local_open_order_not_seen")
    if not ibkr.get("open_order_seen"):
        gaps.append("ibkr_open_order_not_seen")
    return {
        "status_match": local_final == ibkr_final,
        "fill_match": local_fill_seen == ibkr_fill_seen,
        "cancel_match": local_cancelled == ibkr_cancelled,
        "local_filled_but_ibkr_not_filled": local_fill_seen and not ibkr_fill_seen,
        "ibkr_rejected_but_local_accepted": (
            str(local.get("local_order_status")) == OrderStatus.SUBMITTED.value
            and ibkr_final == "REJECTED"
        ),
        "local_price_source": local.get("local_price_source"),
        "ibkr_reference_price_available": ibkr.get("ibkr_reference_price_available"),
        "lifecycle_event_gap": gaps,
    }


def _difference_labels(*, diff: Mapping[str, Any], ibkr: Mapping[str, Any]) -> list[str]:
    labels: list[str] = []
    if diff.get("lifecycle_event_gap"):
        labels.append("EXPECTED_DIFFERENCE")
    if diff.get("local_filled_but_ibkr_not_filled"):
        labels.append("LOCAL_SIM_TOO_OPTIMISTIC")
    if diff.get("ibkr_rejected_but_local_accepted"):
        labels.append("BROKER_REJECTED")
    if ibkr.get("ibkr_reference_price_available") is False:
        labels.append("INSUFFICIENT_MARKET_DATA")
    if not diff.get("cancel_match") and ibkr.get("cancel_requested"):
        labels.append("CANCEL_TIMING_DIFFERENCE")
    return [label for label in labels if label in ALLOWED_DIFFERENCE_LABELS]


def _normalize_broker_status(status: str) -> str:
    normalized = status.strip().upper()
    if normalized in _FINAL_CANCELLED:
        return "CANCELLED"
    if normalized in _BROKER_REJECTED:
        return "REJECTED"
    if normalized in {"SUBMITTED", "PRESUBMITTED", "PENDINGSUBMIT", "APIPENDING"}:
        return "OPEN"
    if normalized == "FILLED":
        return "FILLED"
    return normalized or "UNKNOWN"


def _try_cancel_ibkr_after_error(
    adapter: IBKRPaperOrderLifecycleAdapter,
    submitted_order: Any,
    payload: dict[str, Any],
) -> None:
    if payload["ibkr"].get("cancel_requested"):
        return
    payload["ibkr"]["cancel_requested"] = True
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


def _write_comparison_outputs(
    *,
    payload: dict[str, Any],
    as_of: date,
    output_dir: Path,
    config: IBKRPaperOrderConfig | None,
    raw_broker_order_ids: Sequence[str],
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = default_comparison_json_path(output_dir, as_of)
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
    sanitized_payload["production_effect"] = COMPARISON_PRODUCTION_EFFECT
    json_text = json.dumps(sanitized_payload, ensure_ascii=False, indent=2)
    markdown_text = render_paperbroker_vs_ibkr_markdown(sanitized_payload)
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
            if normalized_key in {
                "broker_order_id",
                "orderid",
                "order_id",
                "permid",
                "perm_id",
            }:
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
        redacted = value
        for raw_id in raw_broker_order_ids:
            if raw_id:
                redacted = redacted.replace(raw_id, _redacted_broker_order_id(raw_id))
        return redacted
    return value


def _redacted_broker_order_id(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, str) and value.startswith("[REDACTED_BROKER_ORDER_ID:"):
        return value
    return f"[REDACTED_BROKER_ORDER_ID:len={len(str(value))}]"


def _empty_local_result() -> dict[str, Any]:
    return {
        "local_order_status": "NOT_RUN",
        "local_open_order_seen": False,
        "local_fill_seen": False,
        "local_avg_fill_price": None,
        "local_cancel_result": "NOT_RUN",
        "local_final_status": "NOT_RUN",
        "local_reconciliation_status": "NOT_RUN",
        "local_price_source": "not_run",
    }


def _empty_ibkr_result() -> dict[str, Any]:
    return {
        "connection_status": {"status": "NOT_RUN", "connected": False},
        "broker_order_id": None,
        "submitted_order": {},
        "open_order_seen": False,
        "open_orders_seen_payload": [],
        "order_status_events": [],
        "cancel_requested": False,
        "final_status": "UNKNOWN",
        "cancelled_confirmed": False,
        "fills_seen": False,
        "fills": [],
        "ibkr_reconciliation_status": "NOT_RUN",
        "ibkr_reference_price_available": None,
    }


def _empty_diff() -> dict[str, Any]:
    return {
        "status_match": False,
        "fill_match": False,
        "cancel_match": False,
        "local_filled_but_ibkr_not_filled": False,
        "ibkr_rejected_but_local_accepted": False,
        "local_price_source": "not_run",
        "ibkr_reference_price_available": None,
        "lifecycle_event_gap": [],
    }


def _add_issue(payload: dict[str, Any], issue: LifecycleIssue) -> None:
    issue_payload = issue.model_dump(mode="json")
    if issue_payload not in payload["issues"]:
        payload["issues"].append(issue_payload)


def _block_issue(code: str, message: str) -> LifecycleIssue:
    return LifecycleIssue(code=code, severity="BLOCK", message=message)


def _json_block(value: Any) -> str:
    return "```json\n" + json.dumps(value, ensure_ascii=False, indent=2) + "\n```"


def _assert_no_sensitive_output(
    text: str,
    *,
    account_id: str,
    raw_broker_order_ids: Sequence[str],
) -> None:
    if account_id and account_id in text:
        raise RuntimeError("comparison output contains an unmasked account id")
    if re.search(r"\b(?:DUP?|U)\d{5,}\b", text, flags=re.IGNORECASE):
        raise RuntimeError("comparison output contains an unmasked account id")
    for raw_id in raw_broker_order_ids:
        if raw_id and re.search(rf"\b{re.escape(raw_id)}\b", text):
            raise RuntimeError("comparison output contains an unredacted broker order id")


__all__ = [
    "COMPARISON_MODE",
    "COMPARISON_PRODUCTION_EFFECT",
    "COMPARISON_REPORT_TYPE",
    "DEFAULT_RECOMMENDATIONS",
    "default_comparison_json_path",
    "render_paperbroker_vs_ibkr_markdown",
    "run_paperbroker_vs_ibkr_paper_comparison",
]
