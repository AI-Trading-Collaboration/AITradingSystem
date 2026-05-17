from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ai_trading_system.trading_engine.schemas.broker_order import BrokerOrder, OrderStatus
from ai_trading_system.trading_engine.schemas.execution_report import ExecutionReport
from ai_trading_system.trading_engine.schemas.order_intent import OrderIntent
from ai_trading_system.trading_engine.schemas.portfolio_state import PortfolioState
from ai_trading_system.trading_engine.schemas.risk_result import RiskCheckResult


@dataclass(frozen=True)
class TradingDailyReport:
    as_of: date
    order_intents: tuple[OrderIntent, ...]
    risk_results: tuple[RiskCheckResult, ...]
    submitted_orders: tuple[BrokerOrder, ...]
    execution_reports: tuple[ExecutionReport, ...]
    portfolio_state: PortfolioState
    audit_root: Path
    data_quality_status: str
    production_effect: str = "none"

    @property
    def approved_count(self) -> int:
        return sum(1 for result in self.risk_results if result.approved)

    @property
    def rejected_count(self) -> int:
        return sum(1 for result in self.risk_results if not result.approved)

    @property
    def filled_count(self) -> int:
        return sum(1 for report in self.execution_reports if report.status == OrderStatus.FILLED)

    @property
    def open_count(self) -> int:
        return sum(1 for order in self.submitted_orders if order.status == OrderStatus.SUBMITTED)


def build_trading_daily_report(
    *,
    as_of: date,
    order_intents: list[OrderIntent],
    risk_results: list[RiskCheckResult],
    submitted_orders: list[BrokerOrder],
    execution_reports: list[ExecutionReport],
    portfolio_state: PortfolioState,
    audit_root: Path,
    data_quality_status: str = "SIMULATED_DEMO",
) -> TradingDailyReport:
    return TradingDailyReport(
        as_of=as_of,
        order_intents=tuple(order_intents),
        risk_results=tuple(risk_results),
        submitted_orders=tuple(submitted_orders),
        execution_reports=tuple(execution_reports),
        portfolio_state=portfolio_state,
        audit_root=audit_root,
        data_quality_status=data_quality_status,
    )


def render_trading_daily_report(report: TradingDailyReport) -> str:
    lines = [
        "# Paper Trading Daily Report",
        "",
        f"- 交易日期：{report.as_of.isoformat()}",
        "- 市场阶段：`ai_after_chatgpt`，本报告为 paper trading 执行复盘，不是实盘交易指令。",
        f"- 数据质量状态：{report.data_quality_status}",
        f"- 生产影响：production_effect={report.production_effect}",
        f"- 审计目录：`{report.audit_root}`",
        "",
        "## 摘要",
        "",
        f"- OrderIntent 数量：{len(report.order_intents)}",
        f"- 风控通过 / 拒绝：{report.approved_count} / {report.rejected_count}",
        f"- Paper 订单提交：{len(report.submitted_orders)}",
        f"- 成交 / 未成交：{report.filled_count} / {report.open_count}",
        f"- 现金：{report.portfolio_state.cash_usd:.2f} USD",
        f"- 权益：{report.portfolio_state.equity_value_usd:.2f} USD",
        f"- Gross exposure：{report.portfolio_state.gross_exposure_usd:.2f} USD",
        f"- Realized PnL：{report.portfolio_state.realized_pnl_usd:.2f} USD",
        "",
        "## OrderIntent",
        "",
        "| Intent | Symbol | Side | Limit | Notional | Confidence | Reasons |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for intent in report.order_intents:
        lines.append(
            "| "
            f"`{intent.intent_id}` | "
            f"`{intent.symbol}` | "
            f"{intent.side.value} | "
            f"{intent.limit_price:.2f} | "
            f"{intent.requested_notional_usd:.2f} | "
            f"{intent.confidence:.2f} | "
            f"{', '.join(intent.reason_codes) or 'NA'} |"
        )

    lines.extend(
        [
            "",
            "## 风控结果",
            "",
            "| Intent | Approved | Severity | Blocked By |",
            "|---|---:|---:|---|",
        ]
    )
    for result in report.risk_results:
        blocked = ", ".join(result.blocked_by) if result.blocked_by else "none"
        lines.append(
            "| "
            f"`{result.intent_id}` | "
            f"{result.approved} | "
            f"{result.severity.value} | "
            f"{blocked} |"
        )

    lines.extend(
        [
            "",
            "## Paper Orders",
            "",
            "| Broker Order | Intent | Symbol | Side | Qty | Status | Filled | Avg Fill |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    orders: list[BrokerOrder]
    if report.submitted_orders:
        order_by_id = {order.broker_order_id: order for order in report.submitted_orders}
        for execution_report in report.execution_reports:
            if execution_report.broker_order_id in order_by_id:
                order_by_id[execution_report.broker_order_id] = order_by_id[
                    execution_report.broker_order_id
                ].model_copy(
                    update={
                        "status": execution_report.status,
                        "filled_quantity": execution_report.filled_quantity,
                        "avg_fill_price": execution_report.avg_fill_price,
                        "completed_at": execution_report.completed_at,
                    }
                )
        orders = list(order_by_id.values())
    else:
        orders = []
    for order in orders:
        avg_fill = "NA" if order.avg_fill_price is None else f"{order.avg_fill_price:.2f}"
        lines.append(
            "| "
            f"`{order.broker_order_id}` | "
            f"`{order.intent_id}` | "
            f"`{order.symbol}` | "
            f"{order.side.value} | "
            f"{order.quantity} | "
            f"{order.status.value} | "
            f"{order.filled_quantity} | "
            f"{avg_fill} |"
        )
    if not report.submitted_orders:
        lines.append("| NA | NA | NA | NA | 0 | NA | 0 | NA |")

    lines.extend(
        [
            "",
            "## Portfolio Snapshot",
            "",
            "| Symbol | Qty | Avg Cost | Market Price | Market Value | Unrealized PnL |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    if report.portfolio_state.positions:
        for position in report.portfolio_state.positions:
            lines.append(
                "| "
                f"`{position.symbol}` | "
                f"{position.quantity} | "
                f"{position.avg_cost:.2f} | "
                f"{position.market_price:.2f} | "
                f"{position.market_value:.2f} | "
                f"{position.unrealized_pnl:.2f} |"
            )
    else:
        lines.append("| 无持仓 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |")

    lines.extend(
        [
            "",
            "## 人工关注事项",
            "",
            "- 本报告仅覆盖 paper trading MVP，不代表真实账户、税费、滑点、流动性或券商状态。",
            "- 真实券商 adapter 在本阶段只能是 stub；任何实盘接入必须另行登记、设计和审批。",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def write_trading_daily_report(
    report: TradingDailyReport,
    output_path: Path | str,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_trading_daily_report(report), encoding="utf-8")
    return path
