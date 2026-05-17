from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.brokers.ibkr_paper_order import (  # noqa: E402
    DEFAULT_IBKR_PAPER_ORDER_CONFIG_PATH,
    IBKRPaperOrderConfig,
    IBKRPaperOrderLifecycleAdapter,
    IBKRPaperOrderRequest,
    LifecycleIssue,
    LifecycleStatus,
    SafetyCheck,
    lifecycle_status_from_result,
    load_ibkr_paper_order_config,
    masked_order_account_id,
    order_events_summary,
    safety_checks_to_issues,
)
from ai_trading_system.trading_engine.brokers.ibkr_readonly import (  # noqa: E402
    sanitize_ibkr_payload,
)


def run_order_lifecycle(
    *,
    as_of: date,
    config_path: Path | str = DEFAULT_IBKR_PAPER_ORDER_CONFIG_PATH,
    output_dir: Path | str = REPO_ROOT / "outputs" / "reports",
    client: Any | None = None,
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
) -> dict[str, Any]:
    config: IBKRPaperOrderConfig | None = None
    adapter: IBKRPaperOrderLifecycleAdapter | None = None
    submitted_order: Any | None = None
    stage = "load_config"
    payload = _base_payload(as_of=as_of, config_path=Path(config_path))

    try:
        config = load_ibkr_paper_order_config(config_path)
        payload.update(
            {
                "account_id_masked": masked_order_account_id(config),
                "production_effect": config.production_effect,
                "trading_mode": config.trading_mode,
                "paper_order_lifecycle_enabled": config.paper_order_lifecycle_enabled,
            }
        )
        request = IBKRPaperOrderRequest(
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
        payload["requested_order"] = request.model_dump(mode="json")

        stage = "config_safety"
        config.assert_lifecycle_settings()
        adapter = IBKRPaperOrderLifecycleAdapter(config=config, client=client)

        stage = "connect"
        payload["connection_status"] = adapter.connect()

        stage = "resolve_limit_price"
        resolved_limit_price = adapter.resolve_limit_price(request)
        if resolved_limit_price is None:
            _add_safety_check(
                payload,
                SafetyCheck(
                    name="reference_price",
                    status="LIMITED",
                    message=(
                        "reference price is unavailable and no manual limit price was provided; "
                        "order was not submitted"
                    ),
                    details={"symbol": request.symbol},
                ),
            )
            _add_issue(
                payload,
                LifecycleIssue(
                    code="reference_price_unavailable",
                    severity="LIMITED",
                    message="无法取得参考价且未手动传入 limit price，已停止提交订单。",
                    details={"symbol": request.symbol},
                ),
            )
            payload["lifecycle_status"] = LifecycleStatus.LIMITED.value
            return _write_outputs(
                payload=payload, as_of=as_of, output_dir=Path(output_dir), config=config
            )

        request = request.with_limit_price(resolved_limit_price)
        payload["submitted_order"] = _requested_submitted_order(request)

        stage = "order_safety"
        safety_checks = adapter.validate_order_request(request)
        payload["safety_checks"] = [check.model_dump(mode="json") for check in safety_checks]
        blocking_issues = safety_checks_to_issues(safety_checks)
        if blocking_issues:
            for issue in blocking_issues:
                _add_issue(payload, issue)
            payload["lifecycle_status"] = LifecycleStatus.BLOCK.value
            return _write_outputs(
                payload=payload, as_of=as_of, output_dir=Path(output_dir), config=config
            )

        stage = "submit_order"
        submitted_order = adapter.submit_order(request)
        payload["submitted_order"] = submitted_order.submitted_order
        payload["broker_order_id"] = submitted_order.broker_order_id

        stage = "wait_open_status"
        _merge_order_events(
            payload,
            adapter.wait_for_order_events(
                submitted_order.trade,
                timeout_seconds=config.status_timeout_seconds,
            ),
        )
        open_orders = adapter.list_open_orders()
        payload["open_order_seen"] = _open_order_seen(
            order_status_events=payload["order_status_events"],
            open_orders=open_orders,
            broker_order_id=submitted_order.broker_order_id,
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
        )
        fills = adapter.get_fills(submitted_order.trade)
        payload["fills_seen"] = bool(fills)
        payload["fills"] = fills

        summary = order_events_summary(payload["order_status_events"])
        payload["open_order_seen"] = bool(payload["open_order_seen"] or summary["open_order_seen"])
        payload["final_order_status"] = summary["final_order_status"]
        payload["cancelled_confirmed"] = summary["cancelled_confirmed"]
        _add_lifecycle_limitations(payload)
        payload["lifecycle_status"] = lifecycle_status_from_result(
            open_order_seen=payload["open_order_seen"],
            cancelled_confirmed=payload["cancelled_confirmed"],
            fills_seen=payload["fills_seen"],
            issues=[LifecycleIssue.model_validate(issue) for issue in payload["issues"]],
        ).value
    except Exception as exc:
        if submitted_order is not None and adapter is not None and not payload["cancel_requested"]:
            payload["cancel_requested"] = _try_cancel_after_error(adapter, submitted_order, payload)
        severity = "BLOCK" if stage in {"load_config", "config_safety", "order_safety"} else "ERROR"
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
                "production_effect": "none",
                "trading_mode": payload["trading_mode"],
                "account_id_masked": payload["account_id_masked"],
            }
        payload["lifecycle_status"] = (
            LifecycleStatus.BLOCK.value if severity == "BLOCK" else LifecycleStatus.ERROR.value
        )
    finally:
        if adapter is not None:
            adapter.disconnect()

    return _write_outputs(payload=payload, as_of=as_of, output_dir=Path(output_dir), config=config)


def render_markdown_lifecycle(payload: dict[str, Any]) -> str:
    lines = [
        "# IBKR Paper Order Lifecycle Report",
        "",
        f"- 日期：{payload['as_of']}",
        f"- Lifecycle status：{payload['lifecycle_status']}",
        f"- Connection status：{payload['connection_status'].get('status', 'UNKNOWN')}",
        f"- Account id：`{payload['account_id_masked']}`",
        f"- production_effect={payload['production_effect']}",
        f"- trading_mode={payload['trading_mode']}",
        (
            "- 安全边界：本报告只验证 IBKR Paper submit/status/cancel/report 生命周期；"
            "不是实盘交易，不接入 production 自动交易，不影响 production 仓位建议。"
        ),
        "",
        "## Submitted Order",
        "",
        _json_block(payload.get("submitted_order", {})),
        "",
        "## Lifecycle Summary",
        "",
        f"- broker_order_id：`{payload.get('broker_order_id') or 'missing'}`",
        f"- open_order_seen：{str(payload.get('open_order_seen')).lower()}",
        f"- cancel_requested：{str(payload.get('cancel_requested')).lower()}",
        f"- final_order_status：{payload.get('final_order_status')}",
        f"- cancelled_confirmed：{str(payload.get('cancelled_confirmed')).lower()}",
        f"- fills_seen：{str(payload.get('fills_seen')).lower()}",
        "",
        "## Order Status Events",
        "",
        _json_block(payload.get("order_status_events", [])),
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


def _base_payload(*, as_of: date, config_path: Path) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "report_type": "ibkr_paper_order_lifecycle",
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "config_path": str(config_path),
        "lifecycle_status": LifecycleStatus.BLOCK.value,
        "connection_status": {"status": "NOT_RUN", "connected": False},
        "account_id_masked": "missing",
        "production_effect": "none",
        "trading_mode": "unknown",
        "paper_order_lifecycle_enabled": False,
        "requested_order": {},
        "submitted_order": {},
        "broker_order_id": None,
        "order_status_events": [],
        "open_order_seen": False,
        "cancel_requested": False,
        "final_order_status": "UNKNOWN",
        "cancelled_confirmed": False,
        "fills_seen": False,
        "fills": [],
        "safety_checks": [],
        "issues": [],
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    as_of: date,
    output_dir: Path,
    config: IBKRPaperOrderConfig | None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"ibkr_paper_order_lifecycle_{as_of.isoformat()}.json"
    markdown_path = output_dir / f"ibkr_paper_order_lifecycle_{as_of.isoformat()}.md"
    payload["output_paths"] = {"json": str(json_path), "markdown": str(markdown_path)}
    sanitized_payload = sanitize_ibkr_payload(
        payload,
        account_id=config.account_id if config is not None else "",
    )
    json_text = json.dumps(sanitized_payload, ensure_ascii=False, indent=2)
    markdown_text = render_markdown_lifecycle(sanitized_payload)
    _assert_no_obvious_unmasked_account_id(json_text)
    _assert_no_obvious_unmasked_account_id(markdown_text)
    json_path.write_text(json_text + "\n", encoding="utf-8")
    markdown_path.write_text(markdown_text, encoding="utf-8")
    return sanitized_payload


def _requested_submitted_order(request: IBKRPaperOrderRequest) -> dict[str, Any]:
    return {
        "symbol": request.symbol,
        "asset_type": request.asset_type,
        "side": request.side,
        "order_type": request.order_type,
        "time_in_force": request.time_in_force,
        "quantity": request.quantity,
        "limit_price": request.limit_price,
        "production_effect": "none",
    }


def _merge_order_events(payload: dict[str, Any], events: list[dict[str, Any]]) -> None:
    for event in events:
        comparable = {key: value for key, value in event.items() if key != "observed_at"}
        already_seen = False
        for existing in payload["order_status_events"]:
            existing_comparable = {
                key: value for key, value in existing.items() if key != "observed_at"
            }
            if existing_comparable == comparable:
                already_seen = True
                break
        if not already_seen:
            payload["order_status_events"].append(event)


def _open_order_seen(
    *,
    order_status_events: list[dict[str, Any]],
    open_orders: list[Any],
    broker_order_id: str | None,
) -> bool:
    if any(
        event.get("status") in {"ApiPending", "PendingSubmit", "PreSubmitted", "Submitted"}
        for event in order_status_events
    ):
        return True
    if broker_order_id is None:
        return bool(open_orders)
    return broker_order_id in json.dumps(open_orders, ensure_ascii=False)


def _add_lifecycle_limitations(payload: dict[str, Any]) -> None:
    if not payload["open_order_seen"]:
        _add_issue(
            payload,
            LifecycleIssue(
                code="open_order_not_seen",
                severity="LIMITED",
                message="未观察到 openOrder/orderStatus 的 open/submitted 状态。",
            ),
        )
    if payload["cancel_requested"] and not payload["cancelled_confirmed"]:
        _add_issue(
            payload,
            LifecycleIssue(
                code="cancel_not_confirmed",
                severity="LIMITED",
                message="已请求 cancel，但未确认 Cancelled/Inactive final status。",
                details={"final_order_status": payload["final_order_status"]},
            ),
        )
    if payload["fills_seen"]:
        _add_issue(
            payload,
            LifecycleIssue(
                code="fills_seen",
                severity="LIMITED",
                message="Paper lifecycle order 出现 fills；该结果只能作为受限样本复核。",
            ),
        )


def _try_cancel_after_error(
    adapter: IBKRPaperOrderLifecycleAdapter,
    submitted_order: Any,
    payload: dict[str, Any],
) -> bool:
    try:
        adapter.cancel_order(submitted_order)
    except Exception as cancel_exc:
        _add_issue(
            payload,
            LifecycleIssue(
                code="cancel_after_error_failed",
                severity="ERROR",
                message=sanitize_ibkr_payload(str(cancel_exc), account_id=""),
                details={"error_type": type(cancel_exc).__name__},
            ),
        )
        return False
    return True


def _add_safety_check(payload: dict[str, Any], check: SafetyCheck) -> None:
    payload["safety_checks"].append(check.model_dump(mode="json"))


def _add_issue(payload: dict[str, Any], issue: LifecycleIssue) -> None:
    issue_payload = issue.model_dump(mode="json")
    if issue_payload not in payload["issues"]:
        payload["issues"].append(issue_payload)


def _json_block(value: Any) -> str:
    return "```json\n" + json.dumps(value, ensure_ascii=False, indent=2) + "\n```"


def _assert_no_obvious_unmasked_account_id(text: str) -> None:
    if any(fragment in text.upper() for fragment in ("DUP000000", "DU000000")):
        return
    if re.search(r"\b(?:DUP?|U)\d{5,}\b", text, flags=re.IGNORECASE):
        raise RuntimeError("IBKR lifecycle output contains an unmasked account id")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an IBKR Paper order lifecycle test.")
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Lifecycle report date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--config-path",
        "--config",
        dest="config_path",
        default=str(DEFAULT_IBKR_PAPER_ORDER_CONFIG_PATH),
        help="Path to IBKR Paper order lifecycle YAML config.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "reports"),
        help="Directory for lifecycle JSON and Markdown outputs.",
    )
    parser.add_argument("--symbol", default="NVDA", help="Whitelisted stock symbol.")
    parser.add_argument("--side", default="BUY", choices=["BUY", "SELL"], help="Order side.")
    parser.add_argument("--quantity", type=int, default=1, help="Tiny paper lifecycle quantity.")
    parser.add_argument(
        "--limit-price",
        "--dry-limit-price",
        dest="limit_price",
        type=float,
        default=None,
        help="Manual far-from-market LIMIT price for lifecycle testing.",
    )
    args = parser.parse_args()
    payload = run_order_lifecycle(
        as_of=date.fromisoformat(args.date),
        config_path=Path(args.config_path),
        output_dir=Path(args.output_dir),
        symbol=args.symbol,
        side=args.side,
        quantity=args.quantity,
        limit_price=args.limit_price,
    )
    print(f"Lifecycle status：{payload['lifecycle_status']}")
    print(f"Connection status：{payload['connection_status'].get('status', 'UNKNOWN')}")
    print(f"Broker order id：{payload.get('broker_order_id') or 'missing'}")
    print(f"Cancelled confirmed：{payload['cancelled_confirmed']}")
    print(f"JSON：{payload['output_paths']['json']}")
    print(f"Markdown：{payload['output_paths']['markdown']}")


if __name__ == "__main__":
    main()
