from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import TYPE_CHECKING, Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

if TYPE_CHECKING:
    from ai_trading_system.trading_engine.schemas import MarketSnapshot, OrderIntentCandidate


def run_from_candidates(
    *,
    as_of: date,
    candidates_path: Path,
    audit_root: Path,
    report_dir: Path,
    summary_output_path: Path | None = None,
) -> dict[str, Any]:
    from ai_trading_system.trading_engine.audit import JsonlAuditLogger
    from ai_trading_system.trading_engine.config import load_trading_engine_config
    from ai_trading_system.trading_engine.execution import ExecutionService, PaperBroker
    from ai_trading_system.trading_engine.intent_builder import (
        build_order_intent_from_candidate,
    )
    from ai_trading_system.trading_engine.portfolio import PaperPortfolio
    from ai_trading_system.trading_engine.portfolio.reconciliation import (
        reconcile_portfolio_from_execution_reports,
    )
    from ai_trading_system.trading_engine.reports import (
        build_paper_trading_summary_payload,
        build_trading_daily_report,
        write_paper_trading_summary_json,
        write_trading_daily_report,
    )
    from ai_trading_system.trading_engine.risk import PreTradeRiskChecker
    from ai_trading_system.trading_engine.schemas import MarketContext

    payload = _read_json_object(candidates_path)
    candidates = _load_candidates(payload)
    config = load_trading_engine_config()
    portfolio = PaperPortfolio(config.execution.default_initial_cash_usd)
    broker = PaperBroker(portfolio=portfolio, execution_settings=config.execution)
    service = ExecutionService(
        risk_checker=PreTradeRiskChecker(config),
        broker=broker,
        audit_logger=JsonlAuditLogger(audit_root),
        config=config,
    )

    approved_candidates = []
    intents = []
    prices: dict[str, float] = {}
    for candidate in candidates:
        try:
            intent = build_order_intent_from_candidate(candidate, mode="paper")
        except (RuntimeError, ValueError):
            continue
        approved_candidates.append(candidate)
        intents.append(intent)
        if candidate.symbol is not None and candidate.limit_price is not None:
            prices[candidate.symbol] = candidate.limit_price

    market_context = MarketContext(as_of=as_of, prices=prices)
    for intent in intents:
        service.execute(intent, market_context=market_context)

    snapshots = [
        _candidate_market_snapshot(candidate, as_of=as_of)
        for candidate in approved_candidates
        if candidate.symbol is not None and candidate.limit_price is not None
    ]
    fill_reports = service.process_market_snapshot(snapshots) if snapshots else []
    final_orders = [
        broker.get_order(order.broker_order_id) for order in service.submitted_orders
    ]
    snapshot_time = max(
        (snapshot.timestamp for snapshot in snapshots),
        default=datetime.combine(as_of, time(20, 0), tzinfo=UTC),
    )
    portfolio_state = service.get_portfolio_state(
        prices=_snapshot_prices(snapshots) or prices,
        as_of=snapshot_time,
    )
    reconciliation_result = reconcile_portfolio_from_execution_reports(
        execution_reports=service.execution_reports,
        submitted_orders=final_orders,
        actual_portfolio=portfolio_state,
        initial_cash_usd=config.execution.default_initial_cash_usd,
        prices=_snapshot_prices(snapshots) or prices,
        as_of=snapshot_time,
    )
    report = build_trading_daily_report(
        as_of=as_of,
        order_intents=intents,
        risk_results=service.risk_results,
        submitted_orders=final_orders,
        execution_reports=service.execution_reports,
        portfolio_state=portfolio_state,
        audit_root=audit_root,
        data_quality_status=_source_data_quality_status(payload),
        reconciliation_result=reconciliation_result,
    )
    report_path = write_trading_daily_report(report, report_dir / f"{as_of.isoformat()}.md")
    if summary_output_path is None:
        summary_output_path = (
            REPO_ROOT / "outputs" / "reports" / f"paper_trading_summary_{as_of.isoformat()}.json"
        )
    write_paper_trading_summary_json(
        report,
        summary_output_path,
        report_path=report_path,
    )
    summary = build_paper_trading_summary_payload(report, report_path=report_path)
    summary["summary_output_path"] = summary_output_path
    summary["candidate_count"] = len(candidates)
    summary["blocked_candidates"] = len(candidates) - len(intents)
    summary["filled"] = len(fill_reports)
    return summary


def _load_candidates(payload: dict[str, Any]) -> tuple[OrderIntentCandidate, ...]:
    from ai_trading_system.trading_engine.schemas import OrderIntentCandidate

    raw_candidates = payload.get("candidates")
    if not isinstance(raw_candidates, list):
        return ()
    parsed: list[OrderIntentCandidate] = []
    fallback_run_id = str(payload.get("run_id") or "missing_run_id")
    for raw_candidate in raw_candidates:
        if not isinstance(raw_candidate, dict):
            continue
        candidate_payload = dict(raw_candidate)
        candidate_payload.setdefault("run_id", fallback_run_id)
        parsed.append(OrderIntentCandidate.model_validate(candidate_payload))
    return tuple(parsed)


def _candidate_market_snapshot(
    candidate: OrderIntentCandidate,
    *,
    as_of: date,
) -> MarketSnapshot:
    from ai_trading_system.trading_engine.schemas import MarketSnapshot

    if candidate.symbol is None or candidate.limit_price is None:
        raise ValueError("candidate cannot create market snapshot without symbol and limit")
    raw_snapshot = candidate.metadata.get("market_snapshot")
    timestamp = datetime.combine(as_of, time(20, 0), tzinfo=UTC)
    if isinstance(raw_snapshot, dict):
        return MarketSnapshot(
            symbol=str(raw_snapshot.get("symbol") or candidate.symbol),
            timestamp=_parse_datetime(raw_snapshot.get("timestamp")) or timestamp,
            open=_float_or_default(raw_snapshot.get("open"), candidate.limit_price),
            high=_float_or_default(raw_snapshot.get("high"), candidate.limit_price),
            low=_float_or_default(raw_snapshot.get("low"), candidate.limit_price),
            last=_float_or_default(raw_snapshot.get("last"), candidate.limit_price),
        )
    return MarketSnapshot(
        symbol=candidate.symbol,
        timestamp=timestamp,
        open=candidate.limit_price,
        high=candidate.limit_price,
        low=candidate.limit_price,
        last=candidate.limit_price,
    )


def _source_data_quality_status(payload: dict[str, Any]) -> str:
    source_inputs = payload.get("source_inputs")
    if isinstance(source_inputs, dict) and source_inputs:
        return "CANDIDATE_SOURCE_PRESENT"
    return "CANDIDATE_SOURCE_UNSPECIFIED"


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"candidate file must be a JSON object: {path}")
    return payload


def _snapshot_prices(snapshots: list[MarketSnapshot]) -> dict[str, float]:
    return {snapshot.symbol: snapshot.last for snapshot in snapshots}


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _float_or_default(value: object, default: float) -> float:
    try:
        if isinstance(value, (int, float, str)):
            return float(value)
    except ValueError:
        pass
    return default


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run paper trading from order intent candidates."
    )
    parser.add_argument("--date", required=True, help="Trading date in YYYY-MM-DD format.")
    parser.add_argument(
        "--candidates-path",
        help="order_intent_candidates JSON path. Defaults to outputs/reports for the date.",
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
        "--summary-output-path",
        help="Paper trading summary JSON path. Defaults to outputs/reports for the date.",
    )
    args = parser.parse_args()
    as_of = date.fromisoformat(args.date)
    candidates_path = (
        Path(args.candidates_path)
        if args.candidates_path
        else REPO_ROOT / "outputs" / "reports" / f"order_intent_candidates_{args.date}.json"
    )
    summary_output_path = (
        Path(args.summary_output_path) if args.summary_output_path else None
    )
    summary = run_from_candidates(
        as_of=as_of,
        candidates_path=candidates_path,
        audit_root=Path(args.audit_root),
        report_dir=Path(args.report_dir),
        summary_output_path=summary_output_path,
    )
    print(f"候选数：{summary['candidate_count']}")
    print(f"生成 OrderIntent：{summary['generated_intents']}")
    print(f"风控通过 / 拒绝：{summary['approved']} / {summary['rejected']}")
    print(f"提交 paper 订单：{summary['submitted']}")
    print(
        "成交 / open / cancelled："
        f"{summary['filled']} / {summary['open']} / {summary['cancelled']}"
    )
    print(f"Reconciliation：{summary['reconciliation_status']}")
    print(f"交易日报：{summary['report_path']}")
    print(f"Paper summary：{summary['summary_output_path']}")


if __name__ == "__main__":
    main()
