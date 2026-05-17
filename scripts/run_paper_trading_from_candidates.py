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
    project_root: Path = REPO_ROOT,
    ensure_upstream_artifacts: bool = False,
) -> dict[str, Any]:
    if ensure_upstream_artifacts:
        _ensure_upstream_candidate_artifacts(
            as_of=as_of,
            candidates_path=candidates_path,
            project_root=project_root,
        )

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

    audit_root.mkdir(parents=True, exist_ok=True)
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
            project_root
            / "outputs"
            / "reports"
            / f"paper_trading_summary_{as_of.isoformat()}.json"
        )
    candidate_count = len(candidates)
    blocked_candidates = len(candidates) - len(intents)
    write_paper_trading_summary_json(
        report,
        summary_output_path,
        report_path=report_path,
        candidate_count=candidate_count,
        blocked_candidates=blocked_candidates,
    )
    summary = build_paper_trading_summary_payload(
        report,
        report_path=report_path,
        candidate_count=candidate_count,
        blocked_candidates=blocked_candidates,
    )
    summary["summary_output_path"] = summary_output_path
    summary["filled"] = len(fill_reports)
    return summary


def _ensure_upstream_candidate_artifacts(
    *,
    as_of: date,
    candidates_path: Path,
    project_root: Path,
) -> None:
    from ai_trading_system.order_intent_candidates import write_order_intent_candidates_json

    reports_dir = candidates_path.parent
    daily_summary_path = reports_dir / f"daily_decision_summary_{as_of.isoformat()}.json"
    if not daily_summary_path.exists():
        _write_limited_daily_decision_summary(
            as_of=as_of,
            output_path=daily_summary_path,
            reports_dir=reports_dir,
            project_root=project_root,
        )
    if not candidates_path.exists():
        write_order_intent_candidates_json(
            as_of=as_of,
            daily_decision_summary_path=daily_summary_path,
            output_path=candidates_path,
            project_root=project_root,
        )


def _write_limited_daily_decision_summary(
    *,
    as_of: date,
    output_path: Path,
    reports_dir: Path,
    project_root: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = _limited_daily_decision_summary_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        project_root=project_root,
    )
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def _limited_daily_decision_summary_payload(
    *,
    as_of: date,
    reports_dir: Path,
    project_root: Path,
) -> dict[str, Any]:
    from ai_trading_system.core import ArtifactRef, ProductionEffect

    missing_decision_reason = (
        "上游 daily-run metadata / evidence dashboard / score 结论缺失；"
        "paper runner 只能生成 limited summary，不能补造投资动作、置信度或仓位。"
    )
    source_artifacts = [
        _artifact_record(
            artifact_id="daily_ops_metadata",
            label="daily ops metadata",
            path=reports_dir / f"daily_ops_run_metadata_{as_of.isoformat()}.json",
            base_dir=reports_dir,
            artifact_ref=ArtifactRef,
        ),
        _artifact_record(
            artifact_id="daily_ops_run_report",
            label="daily ops run report",
            path=reports_dir / f"daily_ops_run_{as_of.isoformat()}.md",
            base_dir=reports_dir,
            artifact_ref=ArtifactRef,
        ),
        _artifact_record(
            artifact_id="data_quality_report",
            label="数据质量门禁",
            path=reports_dir / f"data_quality_{as_of.isoformat()}.md",
            base_dir=reports_dir,
            artifact_ref=ArtifactRef,
        ),
        _artifact_record(
            artifact_id="daily_score_report",
            label="每日评分报告",
            path=reports_dir / f"daily_score_{as_of.isoformat()}.md",
            base_dir=reports_dir,
            artifact_ref=ArtifactRef,
        ),
        _artifact_record(
            artifact_id="evidence_dashboard_json",
            label="evidence dashboard JSON",
            path=reports_dir / f"evidence_dashboard_{as_of.isoformat()}.json",
            base_dir=reports_dir,
            artifact_ref=ArtifactRef,
        ),
        _artifact_record(
            artifact_id="parameter_governance_json",
            label="parameter governance JSON",
            path=reports_dir / f"parameter_governance_{as_of.isoformat()}.json",
            base_dir=reports_dir,
            artifact_ref=ArtifactRef,
        ),
        _artifact_record(
            artifact_id="pipeline_health_report",
            label="pipeline health",
            path=reports_dir / f"pipeline_health_{as_of.isoformat()}.md",
            base_dir=reports_dir,
            artifact_ref=ArtifactRef,
        ),
        _artifact_record(
            artifact_id="decision_snapshot",
            label="score decision snapshot",
            path=(
                project_root
                / "data"
                / "processed"
                / "decision_snapshots"
                / f"decision_snapshot_{as_of.isoformat()}.json"
            ),
            base_dir=reports_dir,
            artifact_ref=ArtifactRef,
        ),
    ]
    return {
        "schema_version": 1,
        "report_type": "daily_decision_summary",
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "run_id": f"paper_trading_runner:{as_of.isoformat()}:limited",
        "production_effect": ProductionEffect.NONE.value,
        "status": "limited",
        "decision_bus_role": {
            "upstream_for": "order_intent_candidate",
            "current_behavior": "read_only_no_trade",
            "order_intent_builder_connected": False,
        },
        "data_gate": {
            "availability": "missing",
            "status": "MISSING",
            "blocking_reasons": [missing_decision_reason],
            "source_dashboard_key_conclusion": _missing_key_conclusion_ref(),
        },
        "investment_conclusion": {
            "availability": "missing",
            "action_bias": "missing",
            "confidence": "missing",
            "position_band": "missing",
            "major_risks": [missing_decision_reason],
            "source_dashboard_key_conclusion": _missing_key_conclusion_ref(),
            "source_steps": [],
            "production_effect": ProductionEffect.NONE.value,
        },
        "parameter_governance": {
            "availability": "missing",
            "status": "MISSING",
            "production_profile": {
                "availability": "missing",
                "manifest_version": "missing",
                "manifest_status": "missing",
                "owner_quantitative_input_status": "missing",
                "action_counts": {},
            },
            "shadow_candidate": {
                "availability": "missing",
                "source": "missing",
                "run_id": "missing",
                "selected_trial_id": "missing",
                "selected_kind": "missing",
                "summary": "missing",
            },
            "promotion_status": "NOT_EVALUATED",
            "blocking_reasons": [missing_decision_reason],
            "source_dashboard_key_conclusion": _missing_key_conclusion_ref(),
        },
        "feedback_review": {
            "availability": "missing",
            "status": "MISSING",
            "summary": "missing",
            "market_feedback_status": "MISSING",
            "feedback_loop_status": "MISSING",
            "investment_review_status": "MISSING",
            "blocking_reasons": [missing_decision_reason],
            "source_dashboard_key_conclusion": _missing_key_conclusion_ref(),
        },
        "system_health": {
            "availability": "missing",
            "status": "LIMITED",
            "warnings": [missing_decision_reason],
            "run_status": "LIMITED",
            "failed_count": 0,
            "skipped_count": 0,
            "source_dashboard_key_conclusion": _missing_key_conclusion_ref(),
        },
        "source_artifacts": source_artifacts,
        "hrefs": {
            str(artifact["id"]): artifact["href"]
            for artifact in source_artifacts
            if artifact.get("exists") and artifact.get("href")
        },
        "checksums": {
            str(artifact["id"]): artifact["checksum_sha256"]
            for artifact in source_artifacts
            if artifact.get("checksum_sha256")
        },
    }


def _missing_key_conclusion_ref() -> dict[str, Any]:
    return {
        "availability": "missing",
        "area": "missing",
        "status": "MISSING",
        "primary": "missing",
        "source_steps": [],
    }


def _artifact_record(
    *,
    artifact_id: str,
    label: str,
    path: Path,
    base_dir: Path,
    artifact_ref: Any,
) -> dict[str, Any]:
    ref = artifact_ref.from_path(path)
    return {
        "id": artifact_id,
        "label": label,
        "path": str(path),
        "href": _href(path, base_dir),
        "exists": ref.exists,
        "artifact_type": ref.artifact_type,
        "checksum_sha256": ref.sha256,
        "size_bytes": ref.size_bytes,
    }


def _href(path: Path, base_dir: Path) -> str:
    try:
        link_path = path.relative_to(base_dir)
    except ValueError:
        try:
            link_path = path.resolve().relative_to(base_dir.resolve())
        except (OSError, RuntimeError, ValueError):
            if path.is_absolute():
                try:
                    return path.as_uri()
                except ValueError:
                    pass
            link_path = path
    return link_path.as_posix()


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
        project_root=REPO_ROOT,
        ensure_upstream_artifacts=args.candidates_path is None,
    )
    print(f"候选数：{summary['candidate_count']}")
    print(f"生成 OrderIntent：{summary['generated_intents']}")
    print(f"风控通过 / 拒绝：{summary['approved']} / {summary['rejected']}")
    print(f"提交 paper 订单：{summary['submitted']}")
    print(
        "成交 / open / cancelled："
        f"{summary['filled']} / {summary['open']} / {summary['cancelled']}"
    )
    print(f"Runner status：{summary['status']}")
    print(f"Reconciliation：{summary['reconciliation_status']}")
    print(f"交易日报：{summary['report_path']}")
    print(f"Paper summary：{summary['summary_output_path']}")


if __name__ == "__main__":
    main()
