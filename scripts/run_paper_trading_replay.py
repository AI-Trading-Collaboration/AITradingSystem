from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
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
        return _write_continuous_portfolio_not_implemented_payload(
            start=start,
            end=end,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            reports_dir=reports_dir,
            audit_root=audit_root,
            trading_daily_report_dir=trading_daily_report_dir,
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


def render_paper_trading_replay_report(payload: dict[str, Any]) -> str:
    totals = _mapping(payload.get("totals"))
    quality_flags = _mapping(payload.get("quality_flags"))
    lines = [
        "# Paper Trading Replay Summary",
        "",
        f"- 日期范围：{payload.get('start')} 到 {payload.get('end')}",
        "- 市场阶段：`ai_after_chatgpt`",
        f"- 状态：{payload.get('status')}",
        f"- replay_mode={payload.get('replay_mode')}",
        f"- portfolio_carry_forward={payload.get('portfolio_carry_forward')}",
        "- production_effect=none",
        (
            "- 边界：paper-only 复盘；不读取 broker API key；不调用真实 broker；"
            "不改变 production 仓位建议。"
        ),
        (
            "- 当前默认 replay 是逐日独立模拟，不是连续组合收益；不会结转前一日"
            "持仓、cash 或 open order。"
        ),
        "",
        "## 汇总",
        "",
        "| 指标 | 数值 |",
        "|---|---:|",
    ]
    for field in (*COUNT_FIELDS, *PNL_FIELDS):
        lines.append(f"| {field} | {_format_value(totals.get(field))} |")

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

    lines.extend(
        [
            "",
            "## 每日结果",
            "",
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
    return "\n".join(lines).rstrip() + "\n"


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
    if replay_mode != REPLAY_MODE_DAILY_INDEPENDENT:
        suffix = f"{suffix}_{replay_mode}"
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


def _write_continuous_portfolio_not_implemented_payload(
    *,
    start: date,
    end: date,
    output_json_path: Path,
    output_md_path: Path,
    reports_dir: Path,
    audit_root: Path,
    trading_daily_report_dir: Path,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": PAPER_TRADING_REPLAY_SCHEMA_VERSION,
        "report_type": "paper_trading_replay",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "market_regime": "ai_after_chatgpt",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "date_count": 0,
        "status": "NOT_IMPLEMENTED",
        "production_effect": "none",
        "replay_mode": REPLAY_MODE_CONTINUOUS_PORTFOLIO,
        "portfolio_carry_forward": False,
        "implementation_status": "NOT_IMPLEMENTED",
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
        "totals": _zero_totals(),
        "distributions": {
            "daily_status": {},
            "reconciliation_status": {},
            "market_snapshot_source": {},
        },
        "aggregations": _serialize_aggregations(_empty_aggregations()),
        "quality_flags": {
            "synthetic_snapshot_days": 0,
            "missing_candidate_days": 0,
            "limited_upstream_days": 0,
            "error_days": 0,
            "empty_candidate_days": 0,
        },
        "daily_results": [],
        "notes": [
            "continuous-portfolio 模式本阶段仅预留结构，尚未实现。",
            "本报告没有结转前一日持仓、cash 或 open order，不能解释为连续组合收益。",
            "replay 不读取 broker API key，不调用真实 broker，不改变 production 仓位建议。",
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
        help="Replay mode. continuous-portfolio is a reserved, not implemented mode.",
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
