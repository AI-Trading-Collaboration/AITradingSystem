from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
SCRIPTS_ROOT = REPO_ROOT / "scripts"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

PAPER_TRADING_REPLAY_SCHEMA_VERSION = 1
REPLAY_MODE_DAILY_INDEPENDENT = "daily_independent"
REPLAY_MODE_CONTINUOUS_PORTFOLIO = "continuous_portfolio"
REPLAY_MODE_CHOICES = ("daily-independent", "continuous-portfolio")
DAY_ORDER_EXPIRATION_POLICY = (
    "DAY orders expire at the end of the replay day; open DAY orders are not " "carried forward."
)
UNSUPPORTED_ORDER_POLICY = (
    "GTC and other non-DAY time_in_force candidates are rejected before order "
    "submission in continuous-portfolio replay."
)

COUNT_FIELDS = (
    "candidate_count",
    "blocked_candidates",
    "generated_intents",
    "approved",
    "rejected",
    "submitted",
    "filled",
    "open",
    "cancelled",
)
PNL_FIELDS = ("realized_pnl", "unrealized_pnl")
GROUP_COUNT_FIELDS = (
    "candidate_count",
    "blocked_candidates",
    "generated_intents",
    "approved",
    "rejected",
    "submitted",
    "filled",
    "open",
    "cancelled",
    "conversion_errors",
)


def run_paper_trading_replay(
    *,
    start: date,
    end: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    audit_root: Path = REPO_ROOT / "data" / "trading_engine" / "audit",
    trading_daily_report_dir: Path = REPO_ROOT / "reports" / "trading_daily",
    project_root: Path = REPO_ROOT,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    mode: str = "daily-independent",
    prices_path: Path | None = None,
) -> dict[str, Any]:
    from run_paper_trading_from_candidates import run_from_candidates

    if end < start:
        raise ValueError("end must be on or after start")

    replay_mode = _normalize_replay_mode(mode)
    reports_dir.mkdir(parents=True, exist_ok=True)
    output_json_path = output_json_path or _default_replay_json_path(
        reports_dir,
        start,
        end,
        replay_mode=replay_mode,
    )
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    if replay_mode == REPLAY_MODE_CONTINUOUS_PORTFOLIO:
        return _run_continuous_portfolio_replay(
            start=start,
            end=end,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            reports_dir=reports_dir,
            audit_root=audit_root,
            trading_daily_report_dir=trading_daily_report_dir,
            project_root=project_root,
            prices_path=prices_path,
        )

    totals = _zero_totals()
    distributions: dict[str, dict[str, int]] = {
        "daily_status": {},
        "reconciliation_status": {},
        "market_snapshot_source": {},
    }
    aggregations = _empty_aggregations()
    daily_results: list[dict[str, Any]] = []

    for as_of in _date_range(start, end):
        candidates_path = reports_dir / f"order_intent_candidates_{as_of.isoformat()}.json"
        summary_output_path = reports_dir / f"paper_trading_summary_{as_of.isoformat()}.json"
        candidate_file_preexisting = candidates_path.exists()
        try:
            summary = run_from_candidates(
                as_of=as_of,
                candidates_path=candidates_path,
                audit_root=audit_root,
                report_dir=trading_daily_report_dir,
                summary_output_path=summary_output_path,
                project_root=project_root,
                ensure_upstream_artifacts=not candidate_file_preexisting,
                prices_path=prices_path,
            )
            record = _daily_result_from_summary(
                as_of=as_of,
                candidates_path=candidates_path,
                summary=summary,
                candidate_file_preexisting=candidate_file_preexisting,
                summary_output_path=summary_output_path,
            )
            _add_daily_totals(totals, record)
            _increment(distributions["daily_status"], str(record["status"]))
            _increment(
                distributions["reconciliation_status"],
                str(record["reconciliation_status"]),
            )
            _increment(
                distributions["market_snapshot_source"],
                str(record["market_snapshot_source"]),
            )
            _add_candidate_aggregations(
                aggregations,
                _candidate_records(summary.get("candidate_records")),
            )
        except Exception as exc:  # noqa: BLE001 - replay must record per-day failures.
            record = _daily_error_result(
                as_of=as_of,
                candidates_path=candidates_path,
                summary_output_path=summary_output_path,
                candidate_file_preexisting=candidate_file_preexisting,
                error=str(exc),
            )
            _increment(distributions["daily_status"], "ERROR")
            _increment(distributions["reconciliation_status"], "ERROR")
            _increment(distributions["market_snapshot_source"], "ERROR")
        daily_results.append(record)

    quality_flags = _quality_flags(daily_results)
    payload: dict[str, Any] = {
        "schema_version": PAPER_TRADING_REPLAY_SCHEMA_VERSION,
        "report_type": "paper_trading_replay",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "market_regime": "ai_after_chatgpt",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "date_count": len(daily_results),
        "status": _overall_status(daily_results, quality_flags=quality_flags),
        "production_effect": "none",
        "replay_mode": replay_mode,
        "portfolio_carry_forward": False,
        "order_expiration_policy": (
            "daily-independent resets the paper portfolio every replay day; open "
            "orders are not carried across days."
        ),
        "unsupported_order_policy": UNSUPPORTED_ORDER_POLICY,
        "continuous_metrics_available": False,
        "expired_day_orders": 0,
        "rejected_gtc_orders": 0,
        "carried_positions_count": 0,
        "final_cash": None,
        "final_equity": None,
        "final_positions": [],
        "max_position_concentration": 0.0,
        "max_drawdown_pct": 0.0,
        "implementation_status": "IMPLEMENTED",
        "execution_boundary": {
            "mode": "paper",
            "paper_only": True,
            "real_broker_allowed": False,
            "broker_api_allowed": False,
            "api_key_read": False,
            "production_position_effect": "none",
        },
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
            "reports_dir": str(reports_dir),
            "audit_root": str(audit_root),
            "trading_daily_report_dir": str(trading_daily_report_dir),
        },
        "totals": totals,
        "distributions": {key: dict(sorted(value.items())) for key, value in distributions.items()},
        "aggregations": _serialize_aggregations(aggregations),
        "quality_flags": quality_flags,
        "daily_results": daily_results,
        "notes": [
            "本 replay 只验证 paper trading daily flow，不代表实盘收益。",
            "当前 replay_mode=daily_independent：逐日独立模拟，不是连续组合收益。",
            "replay 不读取 broker API key，不调用真实 broker，不改变 production 仓位建议。",
            "缺失候选或上游输入时只生成 limited artifacts，不补造投资结论。",
        ],
    }

    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    output_md_path.parent.mkdir(parents=True, exist_ok=True)
    output_md_path.write_text(render_paper_trading_replay_report(payload), encoding="utf-8")
    return payload


def _run_continuous_portfolio_replay(
    *,
    start: date,
    end: date,
    output_json_path: Path,
    output_md_path: Path,
    reports_dir: Path,
    audit_root: Path,
    trading_daily_report_dir: Path,
    project_root: Path,
    prices_path: Path | None,
) -> dict[str, Any]:
    from run_paper_trading_from_candidates import (
        _ensure_upstream_candidate_artifacts,
        _update_candidate_replay_records,
    )

    from ai_trading_system.trading_engine.audit import JsonlAuditLogger
    from ai_trading_system.trading_engine.config import load_trading_engine_config
    from ai_trading_system.trading_engine.execution import ExecutionService, PaperBroker
    from ai_trading_system.trading_engine.intent_builder import (
        build_order_intent_from_candidate,
    )
    from ai_trading_system.trading_engine.market_snapshot_provider import (
        HistoricalPriceMarketSnapshotProvider,
    )
    from ai_trading_system.trading_engine.portfolio import PaperPortfolio
    from ai_trading_system.trading_engine.portfolio.reconciliation import (
        reconcile_portfolio_from_execution_reports,
    )
    from ai_trading_system.trading_engine.risk import PreTradeRiskChecker
    from ai_trading_system.trading_engine.schemas import MarketContext

    reports_dir.mkdir(parents=True, exist_ok=True)
    audit_root.mkdir(parents=True, exist_ok=True)
    config = load_trading_engine_config()
    portfolio = PaperPortfolio(config.execution.default_initial_cash_usd)
    broker = PaperBroker(portfolio=portfolio, execution_settings=config.execution)
    audit_logger = JsonlAuditLogger(audit_root)
    service = ExecutionService(
        risk_checker=PreTradeRiskChecker(config),
        broker=broker,
        audit_logger=audit_logger,
        config=config,
    )
    snapshot_provider = HistoricalPriceMarketSnapshotProvider(
        prices_path=prices_path or project_root / "data" / "raw" / "prices_daily.csv",
    )
    run_id = f"continuous_portfolio_replay:{start.isoformat()}:{end.isoformat()}"

    totals = _zero_totals()
    distributions: dict[str, dict[str, int]] = {
        "daily_status": {},
        "reconciliation_status": {},
        "market_snapshot_source": {},
    }
    aggregations = _empty_aggregations()
    daily_results: list[dict[str, Any]] = []
    portfolio_snapshots: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    daily_equity: dict[str, float] = {}
    daily_cash: dict[str, float] = {}
    daily_exposure: dict[str, float] = {}
    daily_realized_pnl: dict[str, float] = {}
    daily_unrealized_pnl: dict[str, float] = {}
    cumulative_realized_pnl: dict[str, float] = {}
    cumulative_unrealized_pnl: dict[str, float] = {}

    previous_realized_pnl = 0.0
    previous_unrealized_pnl = 0.0
    peak_equity: float | None = None
    max_drawdown = {"amount_usd": 0.0, "percent": 0.0}
    exposure_peak = 0.0
    position_concentration_peak = 0.0

    for as_of in _date_range(start, end):
        candidates_path = reports_dir / f"order_intent_candidates_{as_of.isoformat()}.json"
        candidate_file_preexisting = candidates_path.exists()
        risk_start = len(service.risk_results)
        order_start = len(service.submitted_orders)
        report_start = len(service.execution_reports)
        try:
            if not candidate_file_preexisting:
                _ensure_upstream_candidate_artifacts(
                    as_of=as_of,
                    candidates_path=candidates_path,
                    project_root=project_root,
                )
            payload = _read_json_object(candidates_path)
            candidates, candidate_records = _load_continuous_candidates(
                payload,
                as_of=as_of,
            )
            candidate_record_by_id = {
                str(record["candidate_id"]): record for record in candidate_records
            }

            approved_candidates = []
            intents = []
            for candidate in candidates:
                record = candidate_record_by_id[candidate.candidate_id]
                try:
                    intent = build_order_intent_from_candidate(candidate, mode="paper")
                except (RuntimeError, ValueError) as exc:
                    record["conversion_error"] = str(exc)
                    continue
                record["generated_intent"] = True
                record["intent_id"] = intent.intent_id
                approved_candidates.append(candidate)
                intents.append(intent)

            market_snapshot_source_counts = _empty_market_snapshot_source_counts()
            snapshots = []
            for candidate in approved_candidates:
                if candidate.symbol is None or candidate.limit_price is None:
                    continue
                resolution = snapshot_provider.resolve(candidate, as_of=as_of)
                snapshots.append(resolution.snapshot)
                market_snapshot_source_counts[resolution.source] += 1
                candidate_record_by_id[candidate.candidate_id][
                    "market_snapshot_source"
                ] = resolution.source

            prices = _snapshot_prices(snapshots)
            market_context = MarketContext(as_of=as_of, prices=prices)
            for intent in intents:
                service.execute(intent, market_context=market_context)

            if snapshots:
                service.process_market_snapshot(snapshots)
            snapshot_time = max(
                (snapshot.timestamp for snapshot in snapshots),
                default=datetime.combine(as_of, time(20, 0), tzinfo=UTC),
            )
            service.expire_day_orders(completed_at=snapshot_time)
            final_orders = [
                broker.get_order(order.broker_order_id) for order in service.submitted_orders
            ]
            final_orders_day = [
                broker.get_order(order.broker_order_id)
                for order in service.submitted_orders[order_start:]
            ]
            portfolio_state = service.get_portfolio_state(
                prices=prices,
                as_of=snapshot_time,
            )
            reconciliation_result = reconcile_portfolio_from_execution_reports(
                execution_reports=service.execution_reports,
                submitted_orders=final_orders,
                actual_portfolio=portfolio_state,
                initial_cash_usd=config.execution.default_initial_cash_usd,
                prices=prices,
                as_of=snapshot_time,
            )
            risk_results_day = service.risk_results[risk_start:]
            execution_reports_day = service.execution_reports[report_start:]
            _update_candidate_replay_records(
                candidate_records=candidate_records,
                risk_results=risk_results_day,
                submitted_orders=final_orders_day,
                execution_reports=execution_reports_day,
            )
            related_intent_ids = tuple(intent.intent_id for intent in intents)
            audit_logger.log_portfolio_snapshot(
                portfolio_state,
                run_id=run_id,
                strategy_id="continuous_portfolio_replay",
                related_intent_ids=related_intent_ids,
            )

            day_key = as_of.isoformat()
            snapshot_record = _portfolio_snapshot_record(
                as_of=as_of,
                portfolio_state=portfolio_state,
            )
            current_unrealized_pnl = _portfolio_unrealized_pnl(portfolio_state)
            realized_delta = portfolio_state.realized_pnl_usd - previous_realized_pnl
            unrealized_delta = current_unrealized_pnl - previous_unrealized_pnl
            previous_realized_pnl = portfolio_state.realized_pnl_usd
            previous_unrealized_pnl = current_unrealized_pnl
            peak_equity, drawdown = _drawdown_point(
                equity=portfolio_state.equity_value_usd,
                peak_equity=peak_equity,
            )
            if drawdown["amount_usd"] < max_drawdown["amount_usd"]:
                max_drawdown = drawdown
            exposure_peak = max(exposure_peak, portfolio_state.gross_exposure_usd)
            position_concentration_peak = max(
                position_concentration_peak,
                _position_concentration(snapshot_record),
            )

            record = _continuous_daily_result(
                as_of=as_of,
                candidates_path=candidates_path,
                candidate_file_preexisting=candidate_file_preexisting,
                candidate_records=candidate_records,
                risk_results=risk_results_day,
                final_orders_day=final_orders_day,
                realized_pnl=realized_delta,
                unrealized_pnl=unrealized_delta,
                reconciliation_status=reconciliation_result.status.value,
                market_snapshot_source_counts=market_snapshot_source_counts,
                portfolio_snapshot=snapshot_record,
            )
            portfolio_snapshots.append(snapshot_record)
            equity_curve.append(
                {
                    "date": day_key,
                    "equity": portfolio_state.equity_value_usd,
                    "drawdown": drawdown["amount_usd"],
                    "drawdown_percent": drawdown["percent"],
                }
            )
            daily_equity[day_key] = portfolio_state.equity_value_usd
            daily_cash[day_key] = portfolio_state.cash_usd
            daily_exposure[day_key] = portfolio_state.gross_exposure_usd
            daily_realized_pnl[day_key] = realized_delta
            daily_unrealized_pnl[day_key] = unrealized_delta
            cumulative_realized_pnl[day_key] = portfolio_state.realized_pnl_usd
            cumulative_unrealized_pnl[day_key] = current_unrealized_pnl
            _add_candidate_aggregations(aggregations, tuple(candidate_records))
        except Exception as exc:  # noqa: BLE001 - replay records per-day failures.
            snapshot_time = datetime.combine(as_of, time(20, 0), tzinfo=UTC)
            portfolio_state = service.get_portfolio_state(as_of=snapshot_time)
            snapshot_record = _portfolio_snapshot_record(
                as_of=as_of,
                portfolio_state=portfolio_state,
            )
            record = _continuous_daily_error_result(
                as_of=as_of,
                candidates_path=candidates_path,
                candidate_file_preexisting=candidate_file_preexisting,
                portfolio_snapshot=snapshot_record,
                error=str(exc),
            )
            portfolio_snapshots.append(snapshot_record)

        _add_daily_totals(totals, record)
        _increment(distributions["daily_status"], str(record["status"]))
        _increment(
            distributions["reconciliation_status"],
            str(record["reconciliation_status"]),
        )
        _increment(
            distributions["market_snapshot_source"],
            str(record["market_snapshot_source"]),
        )
        daily_results.append(record)

    quality_flags = _quality_flags(daily_results)
    final_snapshot = _mapping(portfolio_snapshots[-1]) if portfolio_snapshots else {}
    final_positions = list(_list_mappings(final_snapshot.get("positions")))
    max_drawdown_pct = _float_value(max_drawdown.get("percent"))
    payload_out: dict[str, Any] = {
        "schema_version": PAPER_TRADING_REPLAY_SCHEMA_VERSION,
        "report_type": "paper_trading_replay",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "market_regime": "ai_after_chatgpt",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "date_count": len(daily_results),
        "status": _overall_status(daily_results, quality_flags=quality_flags),
        "production_effect": "none",
        "replay_mode": REPLAY_MODE_CONTINUOUS_PORTFOLIO,
        "portfolio_carry_forward": True,
        "order_expiration_policy": DAY_ORDER_EXPIRATION_POLICY,
        "unsupported_order_policy": UNSUPPORTED_ORDER_POLICY,
        "continuous_metrics_available": True,
        "expired_day_orders": sum(_int_value(record.get("expired")) for record in daily_results),
        "rejected_gtc_orders": sum(
            _int_value(record.get("rejected_gtc_orders")) for record in daily_results
        ),
        "carried_positions_count": len(final_positions),
        "final_cash": final_snapshot.get("cash"),
        "final_equity": final_snapshot.get("equity"),
        "final_positions": final_positions,
        "max_position_concentration": position_concentration_peak,
        "max_drawdown_pct": max_drawdown_pct,
        "implementation_status": "IMPLEMENTED",
        "execution_boundary": {
            "mode": "paper",
            "paper_only": True,
            "real_broker_allowed": False,
            "broker_api_allowed": False,
            "api_key_read": False,
            "production_position_effect": "none",
        },
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
            "reports_dir": str(reports_dir),
            "audit_root": str(audit_root),
            "trading_daily_report_dir": str(trading_daily_report_dir),
        },
        "totals": totals,
        "distributions": {key: dict(sorted(value.items())) for key, value in distributions.items()},
        "aggregations": _serialize_aggregations(aggregations),
        "quality_flags": quality_flags,
        "daily_results": daily_results,
        "portfolio_snapshots": portfolio_snapshots,
        "equity_curve": equity_curve,
        "daily_equity": daily_equity,
        "daily_cash": daily_cash,
        "daily_exposure": daily_exposure,
        "daily_realized_pnl": daily_realized_pnl,
        "daily_unrealized_pnl": daily_unrealized_pnl,
        "cumulative_realized_pnl": cumulative_realized_pnl,
        "cumulative_unrealized_pnl": cumulative_unrealized_pnl,
        "max_drawdown": max_drawdown,
        "exposure_peak": exposure_peak,
        "position_concentration_peak": position_concentration_peak,
        "notes": [
            "continuous-portfolio 是 paper-only 连续组合模拟。",
            "它不是实盘交易，也不是真实 broker 成交。",
            "production_effect=none；replay 不读取 broker API key。",
            "daily-independent 每天重置 portfolio；continuous-portfolio 结转持仓和 cash。",
            "DAY order 当日未成交即过期；本阶段不支持 GTC。",
        ],
    }

    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(
        json.dumps(payload_out, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    output_md_path.parent.mkdir(parents=True, exist_ok=True)
    output_md_path.write_text(
        render_paper_trading_replay_report(payload_out),
        encoding="utf-8",
    )
    return payload_out


def render_paper_trading_replay_report(payload: dict[str, Any]) -> str:
    totals = _mapping(payload.get("totals"))
    quality_flags = _mapping(payload.get("quality_flags"))
    replay_mode = _string(payload.get("replay_mode"))
    is_continuous = replay_mode == REPLAY_MODE_CONTINUOUS_PORTFOLIO
    mode_boundary = (
        "- 边界：continuous-portfolio 是 paper-only 连续组合模拟；"
        "不是实盘交易，不是真实账户收益，不是真实 broker 成交；"
        "不包含完整税费/滑点模拟，不能作为实盘上线依据；"
        "production_effect=none；不读取 broker API key。"
        if is_continuous
        else (
            "- 边界：paper-only 复盘；不读取 broker API key；不调用真实 broker；"
            "不改变 production 仓位建议。"
        )
    )
    mode_difference = (
        "- 与 daily-independent 的区别：daily-independent 每天重置 paper portfolio；"
        "continuous-portfolio 从 start date 初始化一次，并跨日结转持仓、cash 和 PnL。"
        if is_continuous
        else (
            "- daily-independent = 每天重新初始化组合；当前默认 replay 是逐日独立模拟，"
            "不是连续组合收益；不会结转前一日持仓、cash 或 open order。"
        )
    )
    lines = [
        "# Paper Trading Replay Summary",
        "",
        f"- 日期范围：{payload.get('start')} 到 {payload.get('end')}",
        "- 市场阶段：`ai_after_chatgpt`",
        f"- 状态：{payload.get('status')}",
        f"- replay_mode={replay_mode}",
        f"- portfolio_carry_forward={_format_bool(payload.get('portfolio_carry_forward'))}",
        "- production_effect=none",
        "- continuous_metrics_available="
        f"{_format_bool(payload.get('continuous_metrics_available'))}",
        f"- order_expiration_policy={payload.get('order_expiration_policy', 'missing')}",
        f"- unsupported_order_policy={payload.get('unsupported_order_policy', 'missing')}",
        mode_boundary,
        mode_difference,
        "",
        "## 汇总",
        "",
        "| 指标 | 数值 |",
        "|---|---:|",
    ]
    for field in (*COUNT_FIELDS, *PNL_FIELDS):
        lines.append(f"| {field} | {_format_value(totals.get(field))} |")

    if is_continuous:
        lines.extend(_render_continuous_metrics_section(payload))

    lines.extend(
        [
            "",
            "## Replay Quality Flags",
            "",
            "| Flag | Count |",
            "|---|---:|",
        ]
    )
    for field in (
        "synthetic_snapshot_days",
        "missing_candidate_days",
        "limited_upstream_days",
        "error_days",
        "empty_candidate_days",
    ):
        lines.append(f"| {field} | {quality_flags.get(field, 0)} |")
    lines.append(
        "| synthetic_snapshot_count | "
        f"{_synthetic_snapshot_count(_list_mappings(payload.get('daily_results')))} |"
    )

    lines.extend(
        [
            "",
            "## Reconciliation Status 分布",
            "",
            "| Status | Count |",
            "|---|---:|",
        ]
    )
    for key, count in _mapping(
        _mapping(payload.get("distributions")).get("reconciliation_status")
    ).items():
        lines.append(f"| {key} | {count} |")

    lines.extend(_render_group_section(payload, "by_symbol", "按 Symbol 聚合"))
    lines.extend(_render_group_section(payload, "by_strategy_id", "按 Strategy 聚合"))
    lines.extend(_render_group_section(payload, "by_reason_code", "按 Reason Code 聚合"))
    lines.extend(_render_group_section(payload, "by_blocked_by", "按 Blocked By 聚合"))

    lines.extend(_render_daily_results_section(payload, is_continuous=is_continuous))
    return "\n".join(lines).rstrip() + "\n"


def _render_continuous_metrics_section(payload: dict[str, Any]) -> list[str]:
    max_drawdown = _mapping(payload.get("max_drawdown"))
    final_positions = _list_mappings(payload.get("final_positions"))
    lines = [
        "",
        "## Continuous Portfolio Metrics",
        "",
        "| 指标 | 数值 |",
        "|---|---:|",
        f"| max_drawdown_usd | {_format_value(max_drawdown.get('amount_usd', 0.0))} |",
        f"| max_drawdown_percent | {_format_percent(max_drawdown.get('percent'))} |",
        f"| max_drawdown_pct | {_format_percent(payload.get('max_drawdown_pct'))} |",
        f"| exposure_peak | {_format_value(payload.get('exposure_peak'))} |",
        "| position_concentration_peak | "
        f"{_format_percent(payload.get('position_concentration_peak'))} |",
        "| max_position_concentration | "
        f"{_format_percent(payload.get('max_position_concentration'))} |",
        f"| expired_day_orders | {payload.get('expired_day_orders', 0)} |",
        f"| rejected_gtc_orders | {payload.get('rejected_gtc_orders', 0)} |",
        "",
        "## Final Portfolio Summary",
        "",
        "| 指标 | 数值 |",
        "|---|---:|",
        f"| final_cash | {_format_value(payload.get('final_cash'))} |",
        f"| final_equity | {_format_value(payload.get('final_equity'))} |",
        f"| final_positions_count | {len(final_positions)} |",
        f"| carried_positions_count | {payload.get('carried_positions_count', 0)} |",
        "",
        "## Final Positions",
        "",
        "| Symbol | Quantity | Avg cost | Market price | Market value | Unrealized PnL |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for position in final_positions:
        lines.append(
            "| "
            f"{position.get('symbol')} | "
            f"{position.get('quantity')} | "
            f"{_format_value(position.get('avg_cost'))} | "
            f"{_format_value(position.get('market_price'))} | "
            f"{_format_value(position.get('market_value'))} | "
            f"{_format_value(position.get('unrealized_pnl'))} |"
        )
    if not final_positions:
        lines.append("| none | 0 | 0.00 | 0.00 | 0.00 | 0.00 |")
    lines.extend(
        [
            "",
            "## Equity Curve",
            "",
            "| Date | Equity | Cash | Exposure | Realized PnL | Unrealized PnL | Drawdown |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    daily_cash = _mapping(payload.get("daily_cash"))
    daily_exposure = _mapping(payload.get("daily_exposure"))
    cumulative_realized = _mapping(payload.get("cumulative_realized_pnl"))
    cumulative_unrealized = _mapping(payload.get("cumulative_unrealized_pnl"))
    for point in _list_mappings(payload.get("equity_curve")):
        day_key = _string(point.get("date"))
        lines.append(
            "| "
            f"{day_key} | "
            f"{_format_value(point.get('equity'))} | "
            f"{_format_value(daily_cash.get(day_key))} | "
            f"{_format_value(daily_exposure.get(day_key))} | "
            f"{_format_value(cumulative_realized.get(day_key))} | "
            f"{_format_value(cumulative_unrealized.get(day_key))} | "
            f"{_format_value(point.get('drawdown'))} |"
        )
    if not _list_mappings(payload.get("equity_curve")):
        lines.append("| none | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |")
    return lines


def _render_daily_results_section(
    payload: dict[str, Any],
    *,
    is_continuous: bool,
) -> list[str]:
    lines = [
        "",
        "## 每日结果",
        "",
    ]
    if is_continuous:
        lines.extend(
            [
                "| Date | Status | Candidates | Intents | Submitted | "
                "Filled/Open/Expired | Reconciliation | Equity | Cash | Exposure |",
                "|---|---|---:|---:|---:|---:|---|---:|---:|---:|",
            ]
        )
        for record in _list_mappings(payload.get("daily_results")):
            snapshot = _mapping(record.get("portfolio_snapshot"))
            filled_open_expired = (
                f"{record.get('filled', 0)}/"
                f"{record.get('open', 0)}/"
                f"{record.get('expired', 0)}"
            )
            lines.append(
                "| "
                f"{record.get('as_of')} | "
                f"{record.get('status')} | "
                f"{record.get('candidate_count')} | "
                f"{record.get('generated_intents')} | "
                f"{record.get('submitted')} | "
                f"{filled_open_expired} | "
                f"{record.get('reconciliation_status')} | "
                f"{_format_value(snapshot.get('equity'))} | "
                f"{_format_value(snapshot.get('cash'))} | "
                f"{_format_value(snapshot.get('exposure'))} |"
            )
        return lines

    lines.extend(
        [
            "| Date | Status | Candidates | Intents | Submitted | "
            "Filled/Open/Cancelled | Reconciliation | Snapshot Source | Limited upstream |",
            "|---|---|---:|---:|---:|---:|---|---|---|",
        ]
    )
    for record in _list_mappings(payload.get("daily_results")):
        filled_open_cancelled = (
            f"{record.get('filled', 0)}/"
            f"{record.get('open', 0)}/"
            f"{record.get('cancelled', 0)}"
        )
        lines.append(
            "| "
            f"{record.get('as_of')} | "
            f"{record.get('status')} | "
            f"{record.get('candidate_count')} | "
            f"{record.get('generated_intents')} | "
            f"{record.get('submitted')} | "
            f"{filled_open_cancelled} | "
            f"{record.get('reconciliation_status')} | "
            f"{record.get('market_snapshot_source')} | "
            f"{record.get('limited_upstream_generated')} |"
        )
    return lines


def _render_group_section(
    payload: dict[str, Any],
    group_key: str,
    title: str,
) -> list[str]:
    rows = _list_mappings(_mapping(payload.get("aggregations")).get(group_key))
    lines = [
        "",
        f"## {title}",
        "",
        "| Key | Candidates | Blocked | Intents | Approved/Rejected | Submitted | "
        "Filled/Open/Cancelled | Conversion Errors |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    if not rows:
        lines.append("| none | 0 | 0 | 0 | 0/0 | 0 | 0/0/0 | 0 |")
        return lines
    for row in rows:
        approved_rejected = f"{row.get('approved', 0)}/{row.get('rejected', 0)}"
        filled_open_cancelled = (
            f"{row.get('filled', 0)}/" f"{row.get('open', 0)}/" f"{row.get('cancelled', 0)}"
        )
        lines.append(
            "| "
            f"{row.get('key')} | "
            f"{row.get('candidate_count', 0)} | "
            f"{row.get('blocked_candidates', 0)} | "
            f"{row.get('generated_intents', 0)} | "
            f"{approved_rejected} | "
            f"{row.get('submitted', 0)} | "
            f"{filled_open_cancelled} | "
            f"{row.get('conversion_errors', 0)} |"
        )
    return lines


def _default_replay_json_path(
    reports_dir: Path,
    start: date,
    end: date,
    *,
    replay_mode: str,
) -> Path:
    suffix = f"{start.isoformat()}_{end.isoformat()}"
    return reports_dir / f"paper_trading_replay_{suffix}.json"


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _zero_totals() -> dict[str, int | float]:
    return {
        **{field: 0 for field in COUNT_FIELDS},
        **{field: 0.0 for field in PNL_FIELDS},
    }


def _empty_aggregations() -> dict[str, dict[str, dict[str, int]]]:
    return {
        "by_symbol": {},
        "by_strategy_id": {},
        "by_reason_code": {},
        "by_blocked_by": {},
    }


def _daily_result_from_summary(
    *,
    as_of: date,
    candidates_path: Path,
    summary: dict[str, Any],
    candidate_file_preexisting: bool,
    summary_output_path: Path,
) -> dict[str, Any]:
    market_snapshot_source_counts = _market_snapshot_source_counts(
        summary.get("market_snapshot_source_counts")
    )
    record: dict[str, Any] = {
        "as_of": as_of.isoformat(),
        "status": _string(summary.get("status")) or "UNKNOWN",
        "production_effect": _string(summary.get("production_effect")) or "none",
        "candidate_file_preexisting": candidate_file_preexisting,
        "limited_upstream_generated": not candidate_file_preexisting,
        "candidates_path": str(candidates_path),
        "summary_output_path": str(summary_output_path),
        "report_path": str(summary.get("report_path") or ""),
        "audit_log_path": str(summary.get("audit_log_path") or ""),
        "reconciliation_status": _string(summary.get("reconciliation_status")) or "MISSING",
        "market_snapshot_source": _string(summary.get("market_snapshot_source")) or "none",
        "market_snapshot_source_counts": market_snapshot_source_counts,
    }
    for field in COUNT_FIELDS:
        record[field] = _int_value(summary.get(field))
    for field in PNL_FIELDS:
        record[field] = _float_value(summary.get(field))
    return record


def _daily_error_result(
    *,
    as_of: date,
    candidates_path: Path,
    summary_output_path: Path,
    candidate_file_preexisting: bool,
    error: str,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "as_of": as_of.isoformat(),
        "status": "ERROR",
        "production_effect": "none",
        "candidate_file_preexisting": candidate_file_preexisting,
        "limited_upstream_generated": not candidate_file_preexisting,
        "candidates_path": str(candidates_path),
        "summary_output_path": str(summary_output_path),
        "report_path": "",
        "audit_log_path": "",
        "reconciliation_status": "ERROR",
        "market_snapshot_source": "ERROR",
        "market_snapshot_source_counts": _empty_market_snapshot_source_counts(),
        "error": error,
    }
    for field in COUNT_FIELDS:
        record[field] = 0
    for field in PNL_FIELDS:
        record[field] = 0.0
    return record


def _continuous_daily_result(
    *,
    as_of: date,
    candidates_path: Path,
    candidate_file_preexisting: bool,
    candidate_records: list[dict[str, Any]],
    risk_results: list[Any],
    final_orders_day: list[Any],
    realized_pnl: float,
    unrealized_pnl: float,
    reconciliation_status: str,
    market_snapshot_source_counts: dict[str, int],
    portfolio_snapshot: dict[str, Any],
) -> dict[str, Any]:
    blocked_candidates = sum(
        1 for record in candidate_records if not bool(record.get("generated_intent"))
    )
    direct_rejections = sum(
        1
        for record in candidate_records
        if bool(record.get("rejected")) and not _string(record.get("intent_id"))
    )
    rejected_gtc_orders = _rejected_gtc_order_count(candidate_records)
    expired = sum(1 for order in final_orders_day if _order_status(order) == "EXPIRED")
    open_count = sum(
        1 for order in final_orders_day if _order_status(order) in {"SUBMITTED", "PARTIALLY_FILLED"}
    )
    record: dict[str, Any] = {
        "as_of": as_of.isoformat(),
        "status": _continuous_daily_status(
            reconciliation_status=reconciliation_status,
            candidate_count=len(candidate_records),
            blocked_candidates=blocked_candidates,
        ),
        "production_effect": "none",
        "candidate_file_preexisting": candidate_file_preexisting,
        "limited_upstream_generated": not candidate_file_preexisting,
        "candidates_path": str(candidates_path),
        "summary_output_path": "",
        "report_path": "",
        "audit_log_path": "",
        "reconciliation_status": reconciliation_status,
        "market_snapshot_source": _market_snapshot_source_label(market_snapshot_source_counts),
        "market_snapshot_source_counts": dict(market_snapshot_source_counts),
        "candidate_count": len(candidate_records),
        "blocked_candidates": blocked_candidates,
        "generated_intents": sum(
            1 for record in candidate_records if bool(record.get("generated_intent"))
        ),
        "approved": sum(1 for result in risk_results if bool(result.approved)),
        "rejected": sum(1 for result in risk_results if not bool(result.approved))
        + direct_rejections,
        "submitted": len(final_orders_day),
        "filled": sum(1 for order in final_orders_day if _order_status(order) == "FILLED"),
        "open": open_count,
        "cancelled": sum(1 for order in final_orders_day if _order_status(order) == "CANCELLED"),
        "expired": expired,
        "rejected_gtc_orders": rejected_gtc_orders,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "portfolio_snapshot": portfolio_snapshot,
        "open_orders_end_of_day": open_count,
    }
    return record


def _continuous_daily_error_result(
    *,
    as_of: date,
    candidates_path: Path,
    candidate_file_preexisting: bool,
    portfolio_snapshot: dict[str, Any],
    error: str,
) -> dict[str, Any]:
    record = _daily_error_result(
        as_of=as_of,
        candidates_path=candidates_path,
        summary_output_path=Path(""),
        candidate_file_preexisting=candidate_file_preexisting,
        error=error,
    )
    record["expired"] = 0
    record["rejected_gtc_orders"] = 0
    record["summary_output_path"] = ""
    record["portfolio_snapshot"] = portfolio_snapshot
    record["open_orders_end_of_day"] = 0
    return record


def _continuous_daily_status(
    *,
    reconciliation_status: str,
    candidate_count: int,
    blocked_candidates: int,
) -> str:
    if reconciliation_status == "ERROR":
        return "ERROR"
    if reconciliation_status != "PASS":
        return "LIMITED"
    if candidate_count == 0 or blocked_candidates:
        return "LIMITED"
    return "PASS"


def _load_continuous_candidates(
    payload: dict[str, Any],
    *,
    as_of: date,
) -> tuple[tuple[Any, ...], list[dict[str, Any]]]:
    from run_paper_trading_from_candidates import _candidate_replay_record

    from ai_trading_system.trading_engine.schemas import OrderIntentCandidate

    raw_candidates = payload.get("candidates")
    if not isinstance(raw_candidates, list):
        return (), []
    parsed_candidates = []
    records: list[dict[str, Any]] = []
    fallback_run_id = str(payload.get("run_id") or "missing_run_id")
    for index, raw_candidate in enumerate(raw_candidates, start=1):
        if not isinstance(raw_candidate, dict):
            records.append(
                _invalid_candidate_record(
                    candidate_id=f"invalid_candidate_{index}",
                    error="candidate item must be a JSON object",
                )
            )
            continue
        raw_record = _candidate_record_from_raw(
            raw_candidate,
            fallback_candidate_id=f"candidate_{index}",
        )
        candidate_payload = dict(raw_candidate)
        candidate_payload.setdefault("run_id", fallback_run_id)
        candidate_payload.setdefault(
            "created_at",
            datetime.combine(as_of, time(14, 0), tzinfo=UTC).isoformat(),
        )
        time_in_force = str(candidate_payload.get("time_in_force") or "DAY").upper()
        if time_in_force != "DAY":
            raw_record["blocked"] = True
            raw_record["blocked_by"] = _append_unique_code(
                _string_list(raw_record.get("blocked_by")),
                "unsupported_time_in_force",
            )
            raw_record["rejected"] = True
            raw_record["conversion_error"] = (
                f"unsupported time_in_force for continuous replay: {time_in_force}"
            )
            records.append(raw_record)
            continue
        try:
            candidate = OrderIntentCandidate.model_validate(candidate_payload)
        except ValueError as exc:
            raw_record["rejected"] = True
            raw_record["conversion_error"] = str(exc)
            records.append(raw_record)
            continue
        parsed_candidates.append(candidate)
        records.append(_candidate_replay_record(candidate))
    return tuple(parsed_candidates), records


def _invalid_candidate_record(*, candidate_id: str, error: str) -> dict[str, Any]:
    record = _candidate_record_from_raw(
        {"candidate_id": candidate_id, "blocked": True},
        fallback_candidate_id=candidate_id,
    )
    record["rejected"] = True
    record["conversion_error"] = error
    return record


def _candidate_record_from_raw(
    raw_candidate: dict[str, Any],
    *,
    fallback_candidate_id: str,
) -> dict[str, Any]:
    return {
        "candidate_id": str(raw_candidate.get("candidate_id") or fallback_candidate_id),
        "strategy_id": str(raw_candidate.get("strategy_id") or "missing"),
        "symbol": str(raw_candidate.get("symbol") or "missing").upper(),
        "time_in_force": str(raw_candidate.get("time_in_force") or "DAY").upper(),
        "reason_codes": _string_list(raw_candidate.get("reason_codes")),
        "blocked": bool(raw_candidate.get("blocked", True)),
        "blocked_by": _string_list(raw_candidate.get("blocked_by")),
        "generated_intent": False,
        "intent_id": None,
        "approved": False,
        "rejected": False,
        "submitted": False,
        "filled": False,
        "open": False,
        "cancelled": False,
        "order_status": "NOT_SUBMITTED",
        "risk_blocked_by": [],
        "conversion_error": "",
        "market_snapshot_source": "not_applicable",
    }


def _append_unique_code(codes: list[str], code: str) -> list[str]:
    if code not in codes:
        codes.append(code)
    return codes


def _rejected_gtc_order_count(candidate_records: list[dict[str, Any]]) -> int:
    return sum(
        1
        for record in candidate_records
        if bool(record.get("rejected")) and _string(record.get("time_in_force")).upper() == "GTC"
    )


def _order_status(order: Any) -> str:
    status = getattr(order, "status", "")
    value = getattr(status, "value", status)
    return str(value)


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"candidate file must be a JSON object: {path}")
    return payload


def _snapshot_prices(snapshots: list[Any]) -> dict[str, float]:
    return {snapshot.symbol: snapshot.last for snapshot in snapshots}


def _portfolio_snapshot_record(
    *,
    as_of: date,
    portfolio_state: Any,
) -> dict[str, Any]:
    positions = [
        position.model_dump(mode="json")
        for position in sorted(
            portfolio_state.positions,
            key=lambda item: item.symbol,
        )
    ]
    return {
        "date": as_of.isoformat(),
        "cash": portfolio_state.cash_usd,
        "positions": positions,
        "equity": portfolio_state.equity_value_usd,
        "exposure": portfolio_state.gross_exposure_usd,
        "net_exposure": portfolio_state.net_exposure_usd,
        "realized_pnl": portfolio_state.realized_pnl_usd,
        "unrealized_pnl": sum(
            _float_value(position.get("unrealized_pnl")) for position in positions
        ),
    }


def _portfolio_unrealized_pnl(portfolio_state: Any) -> float:
    return float(sum(position.unrealized_pnl for position in portfolio_state.positions))


def _drawdown_point(
    *,
    equity: float,
    peak_equity: float | None,
) -> tuple[float, dict[str, float]]:
    next_peak = equity if peak_equity is None else max(peak_equity, equity)
    amount = equity - next_peak
    percent = amount / next_peak if next_peak else 0.0
    return next_peak, {"amount_usd": amount, "percent": percent}


def _position_concentration(snapshot_record: dict[str, Any]) -> float:
    equity = _float_value(snapshot_record.get("equity"))
    if equity <= 0:
        return 0.0
    positions = _list_mappings(snapshot_record.get("positions"))
    if not positions:
        return 0.0
    return max(abs(_float_value(position.get("market_value"))) / equity for position in positions)


def _add_daily_totals(totals: dict[str, int | float], record: dict[str, Any]) -> None:
    for field in COUNT_FIELDS:
        totals[field] = int(totals[field]) + _int_value(record.get(field))
    for field in PNL_FIELDS:
        totals[field] = float(totals[field]) + _float_value(record.get(field))


def _add_candidate_aggregations(
    aggregations: dict[str, dict[str, dict[str, int]]],
    candidate_records: tuple[dict[str, Any], ...],
) -> None:
    for record in candidate_records:
        _add_group_record(
            aggregations["by_symbol"],
            _string(record.get("symbol")) or "missing",
            record,
        )
        _add_group_record(
            aggregations["by_strategy_id"],
            _string(record.get("strategy_id")) or "missing",
            record,
        )
        reason_codes = _string_list(record.get("reason_codes")) or ["none"]
        for reason_code in reason_codes:
            _add_group_record(aggregations["by_reason_code"], reason_code, record)
        blockers = _string_list(record.get("blocked_by")) or ["none"]
        for blocker in blockers:
            _add_group_record(aggregations["by_blocked_by"], blocker, record)


def _add_group_record(
    group: dict[str, dict[str, int]],
    key: str,
    record: dict[str, Any],
) -> None:
    stats = group.setdefault(key, {field: 0 for field in GROUP_COUNT_FIELDS})
    stats["candidate_count"] += 1
    if bool(record.get("blocked")):
        stats["blocked_candidates"] += 1
    for source_key, target_key in (
        ("generated_intent", "generated_intents"),
        ("approved", "approved"),
        ("rejected", "rejected"),
        ("submitted", "submitted"),
        ("filled", "filled"),
        ("open", "open"),
        ("cancelled", "cancelled"),
    ):
        if bool(record.get(source_key)):
            stats[target_key] += 1
    if _string(record.get("conversion_error")):
        stats["conversion_errors"] += 1


def _serialize_aggregations(
    aggregations: dict[str, dict[str, dict[str, int]]],
) -> dict[str, list[dict[str, int | str]]]:
    serialized: dict[str, list[dict[str, int | str]]] = {}
    for group_name, group in aggregations.items():
        rows: list[dict[str, int | str]] = []
        for key, stats in sorted(group.items()):
            rows.append({"key": key, **{field: stats[field] for field in GROUP_COUNT_FIELDS}})
        serialized[group_name] = rows
    return serialized


def _overall_status(
    daily_results: list[dict[str, Any]],
    *,
    quality_flags: dict[str, int],
) -> str:
    statuses = {_string(record.get("status")) for record in daily_results}
    if "ERROR" in statuses:
        return "ERROR"
    if any(status != "PASS" for status in statuses):
        return "LIMITED"
    if any(record.get("limited_upstream_generated") for record in daily_results):
        return "LIMITED"
    if _synthetic_snapshot_dominates(daily_results, quality_flags=quality_flags):
        return "LIMITED"
    return "PASS"


def _quality_flags(daily_results: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "synthetic_snapshot_days": sum(
            1 for record in daily_results if _snapshot_count(record, "synthetic_limit_price") > 0
        ),
        "missing_candidate_days": sum(
            1 for record in daily_results if not record.get("candidate_file_preexisting")
        ),
        "limited_upstream_days": sum(
            1 for record in daily_results if bool(record.get("limited_upstream_generated"))
        ),
        "error_days": sum(1 for record in daily_results if record.get("status") == "ERROR"),
        "empty_candidate_days": sum(
            1 for record in daily_results if _int_value(record.get("candidate_count")) == 0
        ),
    }


def _synthetic_snapshot_dominates(
    daily_results: list[dict[str, Any]],
    *,
    quality_flags: dict[str, int],
) -> bool:
    synthetic_days = quality_flags.get("synthetic_snapshot_days", 0)
    if synthetic_days <= 0:
        return False
    snapshot_days = sum(
        1
        for record in daily_results
        if any(count > 0 for count in _market_snapshot_counts_from_record(record).values())
    )
    non_synthetic_days = snapshot_days - synthetic_days
    return synthetic_days > non_synthetic_days


def _snapshot_count(record: dict[str, Any], source: str) -> int:
    return _market_snapshot_counts_from_record(record).get(source, 0)


def _synthetic_snapshot_count(daily_results: tuple[dict[str, Any], ...]) -> int:
    return sum(_snapshot_count(record, "synthetic_limit_price") for record in daily_results)


def _market_snapshot_counts_from_record(record: dict[str, Any]) -> dict[str, int]:
    return _market_snapshot_source_counts(record.get("market_snapshot_source_counts"))


def _market_snapshot_source_counts(value: object) -> dict[str, int]:
    counts = _empty_market_snapshot_source_counts()
    if isinstance(value, dict):
        for key in counts:
            counts[key] = _int_value(value.get(key))
    return counts


def _market_snapshot_source_label(counts: dict[str, int]) -> str:
    active_sources = [source for source, count in counts.items() if count]
    if not active_sources:
        return "none"
    if len(active_sources) == 1:
        return active_sources[0]
    return "mixed"


def _empty_market_snapshot_source_counts() -> dict[str, int]:
    return {
        "historical_ohlc": 0,
        "candidate_metadata": 0,
        "synthetic_limit_price": 0,
    }


def _normalize_replay_mode(mode: str) -> str:
    normalized = mode.strip().lower().replace("_", "-")
    if normalized == "daily-independent":
        return REPLAY_MODE_DAILY_INDEPENDENT
    if normalized == "continuous-portfolio":
        return REPLAY_MODE_CONTINUOUS_PORTFOLIO
    raise ValueError("mode must be daily-independent or continuous-portfolio")


def _increment(distribution: dict[str, int], key: str) -> None:
    distribution[key or "MISSING"] = distribution.get(key or "MISSING", 0) + 1


def _candidate_records(value: object) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _list_mappings(value: object) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string(value: object) -> str:
    return value if isinstance(value, str) else ""


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item]


def _int_value(value: object) -> int:
    try:
        if isinstance(value, (str, bytes, bytearray, int, float)):
            return int(value)
    except (TypeError, ValueError):
        return 0
    return 0


def _float_value(value: object) -> float:
    try:
        if isinstance(value, (str, bytes, bytearray, int, float)):
            return float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0


def _format_value(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _format_percent(value: object) -> str:
    return f"{_float_value(value) * 100:.2f}%"


def _format_bool(value: object) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay paper trading daily flow over a date range."
    )
    parser.add_argument("--start", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end", required=True, help="End date in YYYY-MM-DD format.")
    parser.add_argument(
        "--mode",
        default="daily-independent",
        choices=REPLAY_MODE_CHOICES,
        help="Replay mode. daily-independent resets portfolio; continuous-portfolio carries it.",
    )
    parser.add_argument(
        "--reports-dir",
        default=str(REPO_ROOT / "outputs" / "reports"),
        help="Directory containing candidate and paper summary reports.",
    )
    parser.add_argument(
        "--audit-root",
        default=str(REPO_ROOT / "data" / "trading_engine" / "audit"),
        help="Audit JSONL root directory.",
    )
    parser.add_argument(
        "--report-dir",
        default=str(REPO_ROOT / "reports" / "trading_daily"),
        help="Trading daily report output directory.",
    )
    parser.add_argument(
        "--prices-path",
        help="Historical OHLC CSV path. Defaults to data/raw/prices_daily.csv.",
    )
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    payload = run_paper_trading_replay(
        start=start,
        end=end,
        reports_dir=Path(args.reports_dir),
        audit_root=Path(args.audit_root),
        trading_daily_report_dir=Path(args.report_dir),
        project_root=REPO_ROOT,
        mode=args.mode,
        prices_path=Path(args.prices_path) if args.prices_path else None,
    )
    totals = _mapping(payload.get("totals"))
    outputs = _mapping(payload.get("outputs"))
    print(f"Replay status：{payload['status']}")
    print(f"日期数：{payload['date_count']}")
    print(f"候选数：{totals.get('candidate_count', 0)}")
    print(f"生成 OrderIntent：{totals.get('generated_intents', 0)}")
    print(f"提交 paper 订单：{totals.get('submitted', 0)}")
    print(
        "成交 / open / cancelled："
        f"{totals.get('filled', 0)} / "
        f"{totals.get('open', 0)} / "
        f"{totals.get('cancelled', 0)}"
    )
    print(f"Replay JSON：{outputs.get('json')}")
    print(f"Replay Markdown：{outputs.get('markdown')}")


if __name__ == "__main__":
    main()
