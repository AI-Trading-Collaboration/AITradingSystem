from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

PAPER_SIGNAL_QUALITY_SCHEMA_VERSION = 1
PAPER_SIGNAL_QUALITY_WINDOWS: tuple[int, ...] = (7, 14, 30)
PAPER_SIGNAL_QUALITY_REPORT_TYPE = "paper_signal_quality"
REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_PAPER_SIGNAL_QUALITY_POLICY_PATH = REPO_ROOT / "config" / "paper_signal_quality_policy.yaml"


@dataclass
class _GroupStats:
    dates: set[str] = field(default_factory=set)
    candidate_count: int = 0
    generated_intents: int = 0
    filled_count: int = 0
    paper_pnl_total: float = 0.0
    market_snapshot_source_counts: Counter[str] = field(default_factory=Counter)


@dataclass(frozen=True)
class _CandidateSample:
    as_of: str
    candidate: dict[str, Any]
    generated_intent: bool
    filled: bool
    paper_pnl: float
    market_snapshot_source: str


def default_paper_signal_quality_json_path(reports_dir: Path, as_of: date) -> Path:
    return reports_dir / f"paper_signal_quality_{as_of.isoformat()}.json"


def build_paper_signal_quality_payload(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    policy_path: Path = DEFAULT_PAPER_SIGNAL_QUALITY_POLICY_PATH,
    replay_json_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    selected_window_days: int = 30,
) -> dict[str, Any]:
    if selected_window_days not in set(PAPER_SIGNAL_QUALITY_WINDOWS):
        raise ValueError("selected_window_days must be one of 7, 14, or 30")

    policy = _load_policy(policy_path)
    replay_payload = _read_json_object(replay_json_path) if replay_json_path else {}
    replay_daily = _replay_daily_results_by_date(replay_payload)
    output_json_path = output_json_path or default_paper_signal_quality_json_path(
        reports_dir,
        as_of,
    )
    output_md_path = output_md_path or output_json_path.with_suffix(".md")

    windows = {
        str(days): _build_window_evaluation(
            as_of=as_of,
            reports_dir=reports_dir,
            days=days,
            policy=policy,
            replay_daily=replay_daily,
        )
        for days in PAPER_SIGNAL_QUALITY_WINDOWS
    }
    selected = windows[str(selected_window_days)]
    return {
        "schema_version": PAPER_SIGNAL_QUALITY_SCHEMA_VERSION,
        "report_type": PAPER_SIGNAL_QUALITY_REPORT_TYPE,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "as_of": as_of.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "production_effect": "none",
        "evaluation_status": selected["evaluation_status"],
        "selected_window_days": selected_window_days,
        "policy": _policy_report(policy, policy_path),
        "evaluation_scope": {
            "observe_only": True,
            "production_effect": "none",
            "changes_production_position_recommendation": False,
            "changes_parameter_promotion": False,
            "uses_paper_pnl_as_launch_evidence": False,
        },
        "safety_boundary": {
            "reads_broker_api_key": False,
            "calls_real_broker": False,
            "runs_paper_runner": False,
            "runs_replay": False,
            "changes_production_position_recommendation": False,
            "changes_parameter_promotion": False,
            "paper_pnl_is_launch_evidence": False,
        },
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
        },
        "source_artifacts": {
            "reports_dir": str(reports_dir),
            "policy_path": str(policy_path),
            "optional_replay": _optional_replay_source(replay_json_path, replay_payload),
            "summaries": selected["source_artifacts"]["summaries"],
            "order_intent_candidates": selected["source_artifacts"]["order_intent_candidates"],
        },
        "summary": selected["summary"],
        "evaluation_gate": selected["evaluation_gate"],
        "aggregations": selected["aggregations"],
        "windows": windows,
        "notes": [
            "本报告只解释 paper signal quality，不是实盘交易或参数晋级依据。",
            "evaluation_gate 只影响 paper signal quality 解释，不改变 production 仓位建议。",
            "报告不会读取 broker API key、不会调用真实 broker、不会触发 paper runner 或 replay。",
            (
                "分组 avg_paper_pnl 使用现有 paper summary 的可见粒度计算；"
                "paper PnL 不得作为上线依据。"
            ),
        ],
    }


def write_paper_signal_quality_report(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    policy_path: Path = DEFAULT_PAPER_SIGNAL_QUALITY_POLICY_PATH,
    replay_json_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    selected_window_days: int = 30,
) -> dict[str, Any]:
    payload = build_paper_signal_quality_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=policy_path,
        replay_json_path=replay_json_path,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
        selected_window_days=selected_window_days,
    )
    outputs = _mapping(payload.get("outputs"))
    json_path = Path(str(outputs["json"]))
    md_path = Path(str(outputs["markdown"]))
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_paper_signal_quality_report(payload), encoding="utf-8")
    return payload


def render_paper_signal_quality_report(payload: dict[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    gate = _mapping(payload.get("evaluation_gate"))
    lines = [
        "# Paper Signal Quality Evaluation",
        "",
        f"- 评估日期：{payload.get('as_of')}",
        "- 市场阶段：`ai_after_chatgpt`",
        f"- 状态：{payload.get('evaluation_status')}",
        "- observe-only：true",
        "- production_effect=none",
        (
            "- 安全边界：不读取 broker API key；不调用真实 broker；不触发 paper runner / "
            "replay；不改变 production 仓位建议；不影响参数晋级。"
        ),
        "- Paper PnL 只作诊断字段，不作为上线依据。",
        "",
        "## 摘要",
        "",
        "| 指标 | 数值 |",
        "|---|---:|",
        f"| sample_count | {summary.get('sample_count', 0)} |",
        f"| candidate_count | {summary.get('candidate_count', 0)} |",
        f"| generated_intents | {summary.get('generated_intents', 0)} |",
        f"| filled_count | {summary.get('filled_count', 0)} |",
        f"| primary_blocked_by | {summary.get('primary_blocked_by', 'none')} |",
        (
            "| synthetic_snapshot_ratio | "
            f"{_format_percent(_float_value(summary.get('synthetic_snapshot_ratio')))} |"
        ),
        (
            "| historical_ohlc_coverage | "
            f"{_format_percent(_float_value(summary.get('historical_ohlc_coverage')))} |"
        ),
        (
            "| reconciliation_pass_ratio | "
            f"{_format_percent(_float_value(summary.get('reconciliation_pass_ratio')))} |"
        ),
        "",
        "## Evaluation Gate",
        "",
        f"- Gate status：{gate.get('status', 'LIMITED')}",
        f"- Blocking reasons：{', '.join(_strings(gate.get('blocking_reasons'))) or 'none'}",
        "",
        "| Check | Status | Observed | Threshold | Reason |",
        "|---|---|---:|---:|---|",
    ]
    for check in _records(gate.get("checks")):
        lines.append(
            "| "
            f"{check.get('check_id')} | "
            f"{check.get('status')} | "
            f"{_format_check_value(check.get('observed'))} | "
            f"{_format_check_value(check.get('threshold'))} | "
            f"{check.get('reason_code') or 'none'} |"
        )

    aggregations = _mapping(payload.get("aggregations"))
    lines.extend(_render_aggregation_section(aggregations, "by_strategy_id", "按 Strategy 聚合"))
    lines.extend(_render_aggregation_section(aggregations, "by_symbol", "按 Symbol 聚合"))
    lines.extend(_render_aggregation_section(aggregations, "by_reason_code", "按 Reason Code 聚合"))
    lines.extend(_render_aggregation_section(aggregations, "by_blocked_by", "按 Blocked By 聚合"))
    lines.extend(
        _render_aggregation_section(
            aggregations,
            "by_confidence_bucket",
            "按 Confidence Bucket 聚合",
        )
    )
    lines.extend(
        _render_aggregation_section(
            aggregations,
            "by_market_snapshot_source",
            "按 Market Snapshot Source 聚合",
        )
    )
    lines.extend(
        [
            "",
            "## 窗口状态",
            "",
            "| Window | Status | Samples | Filled | Synthetic | Reconciliation PASS |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for window_key, window in _mapping(payload.get("windows")).items():
        window_summary = _mapping(_mapping(window).get("summary"))
        lines.append(
            "| "
            f"{window_key} 日 | "
            f"{window.get('evaluation_status')} | "
            f"{window_summary.get('sample_count', 0)} | "
            f"{window_summary.get('filled_count', 0)} | "
            f"{_format_percent(_float_value(window_summary.get('synthetic_snapshot_ratio')))} | "
            f"{_format_percent(_float_value(window_summary.get('reconciliation_pass_ratio')))} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def _build_window_evaluation(
    *,
    as_of: date,
    reports_dir: Path,
    days: int,
    policy: dict[str, Any],
    replay_daily: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    start = as_of - timedelta(days=days - 1)
    daily_results: list[dict[str, Any]] = []
    summary_artifacts: list[dict[str, Any]] = []
    candidate_artifacts: list[dict[str, Any]] = []
    group_stats = _empty_group_stats()
    blocked_by_counter: Counter[str] = Counter()
    totals = {
        "candidate_count": 0,
        "generated_intents": 0,
        "filled_count": 0,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
    }
    reconciliation_counter: Counter[str] = Counter()
    market_snapshot_source_counts: Counter[str] = Counter()
    available_summary_count = 0

    for offset in range(days):
        current = start + timedelta(days=offset)
        day_key = current.isoformat()
        summary_path = reports_dir / f"paper_trading_summary_{day_key}.json"
        candidate_path = reports_dir / f"order_intent_candidates_{day_key}.json"
        summary_payload = _read_json_object(summary_path)
        summary_source = "file"
        if summary_payload.get("report_type") != "paper_trading_summary":
            summary_payload = _summary_from_replay_day(replay_daily.get(day_key))
            summary_source = "optional_replay" if summary_payload else "missing"
        candidate_payload = _read_json_object(candidate_path)
        candidates = _candidate_records(candidate_payload)
        candidate_artifacts.append(
            _artifact_record(
                path=candidate_path,
                reports_dir=reports_dir,
                exists=candidate_payload.get("report_type") == "order_intent_candidates",
                source="file",
            )
        )
        summary_exists = bool(summary_payload)
        summary_artifacts.append(
            _artifact_record(
                path=summary_path,
                reports_dir=reports_dir,
                exists=summary_exists and summary_source == "file",
                source=summary_source,
            )
        )
        if not summary_exists:
            daily_results.append(
                {
                    "as_of": day_key,
                    "status": "MISSING",
                    "summary_source": "missing",
                    "candidate_count": len(candidates),
                    "generated_intents": 0,
                    "filled_count": 0,
                    "reconciliation_status": "MISSING",
                    "market_snapshot_source_counts": {},
                }
            )
            _add_candidate_only_groups(
                group_stats=group_stats,
                candidates=candidates,
                day_key=day_key,
                blocked_by_counter=blocked_by_counter,
                policy=policy,
            )
            continue

        available_summary_count += 1
        source_counts = _snapshot_source_counts(summary_payload)
        market_snapshot_source_counts.update(source_counts)
        reconciliation_status = _string_value(summary_payload.get("reconciliation_status"))
        reconciliation_counter[reconciliation_status or "MISSING"] += 1
        day_generated = _int_value(summary_payload.get("generated_intents"))
        day_filled = _int_value(summary_payload.get("filled"))
        day_realized_pnl = _float_value(summary_payload.get("realized_pnl"))
        day_unrealized_pnl = _float_value(summary_payload.get("unrealized_pnl"))
        totals["candidate_count"] += _int_value(
            summary_payload.get("candidate_count"),
            default=len(candidates),
        )
        totals["generated_intents"] += day_generated
        totals["filled_count"] += day_filled
        totals["realized_pnl"] += day_realized_pnl
        totals["unrealized_pnl"] += day_unrealized_pnl
        candidate_samples = _candidate_samples(
            day_key=day_key,
            candidates=candidates,
            summary=summary_payload,
            policy=policy,
        )
        _add_candidate_samples_to_groups(
            group_stats=group_stats,
            samples=candidate_samples,
            blocked_by_counter=blocked_by_counter,
            policy=policy,
        )
        _add_market_snapshot_source_groups(
            group_stats["by_market_snapshot_source"],
            day_key=day_key,
            source_counts=source_counts,
            filled_count=day_filled,
            paper_pnl=day_realized_pnl + day_unrealized_pnl,
        )
        daily_results.append(
            {
                "as_of": day_key,
                "status": _string_value(summary_payload.get("status")) or "UNKNOWN",
                "summary_source": summary_source,
                "candidate_count": _int_value(summary_payload.get("candidate_count")),
                "generated_intents": day_generated,
                "filled_count": day_filled,
                "reconciliation_status": reconciliation_status or "MISSING",
                "market_snapshot_source_counts": dict(source_counts),
            }
        )

    summary = _window_summary(
        sample_count=available_summary_count,
        totals=totals,
        reconciliation_counter=reconciliation_counter,
        market_snapshot_source_counts=market_snapshot_source_counts,
        blocked_by_counter=blocked_by_counter,
    )
    gate = _evaluation_gate(summary, policy)
    summary["evaluation_status"] = gate["status"]
    return {
        "window_days": days,
        "start": start.isoformat(),
        "end": as_of.isoformat(),
        "evaluation_status": gate["status"],
        "summary": summary,
        "evaluation_gate": gate,
        "aggregations": _serialize_aggregations(group_stats, policy),
        "daily_results": daily_results,
        "source_artifacts": {
            "summaries": summary_artifacts,
            "order_intent_candidates": candidate_artifacts,
        },
    }


def _window_summary(
    *,
    sample_count: int,
    totals: dict[str, int | float],
    reconciliation_counter: Counter[str],
    market_snapshot_source_counts: Counter[str],
    blocked_by_counter: Counter[str],
) -> dict[str, Any]:
    snapshot_total = sum(market_snapshot_source_counts.values())
    synthetic_count = market_snapshot_source_counts.get("synthetic_limit_price", 0)
    historical_count = market_snapshot_source_counts.get("historical_ohlc", 0)
    reconciliation_total = sum(reconciliation_counter.values())
    reconciliation_pass = sum(
        count for status, count in reconciliation_counter.items() if status.startswith("PASS")
    )
    primary_blocked_by = "none"
    if blocked_by_counter:
        primary_blocked_by = blocked_by_counter.most_common(1)[0][0]
    return {
        "sample_count": sample_count,
        "candidate_count": int(totals["candidate_count"]),
        "generated_intents": int(totals["generated_intents"]),
        "filled_count": int(totals["filled_count"]),
        "avg_paper_pnl": _average_pnl(
            _float_value(totals["realized_pnl"]) + _float_value(totals["unrealized_pnl"]),
            int(totals["filled_count"]),
        ),
        "realized_pnl": totals["realized_pnl"],
        "unrealized_pnl": totals["unrealized_pnl"],
        "synthetic_snapshot_ratio": synthetic_count / snapshot_total if snapshot_total else 0.0,
        "historical_ohlc_coverage": historical_count / snapshot_total if snapshot_total else 0.0,
        "reconciliation_pass_ratio": (
            reconciliation_pass / reconciliation_total if reconciliation_total else 0.0
        ),
        "primary_blocked_by": primary_blocked_by,
        "market_snapshot_source_counts": dict(sorted(market_snapshot_source_counts.items())),
        "reconciliation_status_distribution": dict(sorted(reconciliation_counter.items())),
    }


def _evaluation_gate(summary: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    thresholds = _mapping(policy["thresholds"])
    checks = [
        _minimum_check(
            check_id="sample_count",
            observed=_int_value(summary.get("sample_count")),
            threshold=_int_value(thresholds["minimum_sample_count"]),
            reason_code="INSUFFICIENT_SAMPLE",
        ),
        _minimum_check(
            check_id="filled_count",
            observed=_int_value(summary.get("filled_count")),
            threshold=_int_value(thresholds["minimum_filled_count"]),
            reason_code="INSUFFICIENT_FILLED_SAMPLE",
        ),
        _maximum_check(
            check_id="synthetic_snapshot_ratio",
            observed=_float_value(summary.get("synthetic_snapshot_ratio")),
            threshold=_float_value(thresholds["maximum_synthetic_snapshot_ratio"]),
            reason_code="LOW_DATA_QUALITY",
        ),
        _minimum_ratio_check(
            check_id="historical_ohlc_coverage",
            observed=_float_value(summary.get("historical_ohlc_coverage")),
            threshold=_float_value(thresholds["minimum_historical_ohlc_coverage"]),
            reason_code="LIMITED_MARKET_DATA",
            enabled=_int_value(summary.get("generated_intents")) > 0,
        ),
        _minimum_ratio_check(
            check_id="reconciliation_pass_ratio",
            observed=_float_value(summary.get("reconciliation_pass_ratio")),
            threshold=_float_value(thresholds["minimum_reconciliation_pass_ratio"]),
            reason_code="UNRELIABLE_EXECUTION_STATE",
            enabled=_int_value(summary.get("sample_count")) > 0,
        ),
    ]
    blocking_reasons = [
        str(check["reason_code"]) for check in checks if check.get("status") == "FAIL"
    ]
    return {
        "status": "LIMITED" if blocking_reasons else "PASS",
        "blocking_reasons": blocking_reasons,
        "checks": checks,
        "production_effect": "none",
        "scope": "paper_signal_quality_only",
    }


def _minimum_check(
    *,
    check_id: str,
    observed: int,
    threshold: int,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if observed >= threshold else "FAIL",
        "observed": observed,
        "threshold": threshold,
        "operator": ">=",
        "reason_code": "" if observed >= threshold else reason_code,
    }


def _minimum_ratio_check(
    *,
    check_id: str,
    observed: float,
    threshold: float,
    reason_code: str,
    enabled: bool,
) -> dict[str, Any]:
    if not enabled:
        return {
            "check_id": check_id,
            "status": "SKIPPED",
            "observed": observed,
            "threshold": threshold,
            "operator": ">=",
            "reason_code": "",
        }
    return {
        "check_id": check_id,
        "status": "PASS" if observed >= threshold else "FAIL",
        "observed": observed,
        "threshold": threshold,
        "operator": ">=",
        "reason_code": "" if observed >= threshold else reason_code,
    }


def _maximum_check(
    *,
    check_id: str,
    observed: float,
    threshold: float,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if observed <= threshold else "FAIL",
        "observed": observed,
        "threshold": threshold,
        "operator": "<=",
        "reason_code": "" if observed <= threshold else reason_code,
    }


def _candidate_samples(
    *,
    day_key: str,
    candidates: tuple[dict[str, Any], ...],
    summary: dict[str, Any],
    policy: dict[str, Any],
) -> tuple[_CandidateSample, ...]:
    replay_records = {
        _string_value(record.get("candidate_id")): record
        for record in _records(summary.get("candidate_records"))
        if _string_value(record.get("candidate_id"))
    }
    source_counts = _snapshot_source_counts(summary)
    default_source = _single_snapshot_source(source_counts)
    prepared: list[dict[str, Any]] = []
    for candidate in candidates:
        candidate_id = _string_value(candidate.get("candidate_id"))
        replay_record = replay_records.get(candidate_id, {})
        generated = (
            bool(replay_record.get("generated_intent"))
            if replay_record
            else _candidate_generates_intent(candidate)
        )
        filled = bool(replay_record.get("filled")) if replay_record else False
        source = _string_value(replay_record.get("market_snapshot_source")) or default_source
        if source == "not_applicable" and generated:
            source = default_source
        prepared.append(
            {
                "candidate": candidate,
                "generated": generated,
                "filled": filled,
                "source": source or "unknown",
            }
        )
    if not any(record["filled"] for record in prepared):
        filled_remaining = _int_value(summary.get("filled"))
        for record in prepared:
            if filled_remaining <= 0:
                break
            if record["generated"]:
                record["filled"] = True
                filled_remaining -= 1
    filled_count = sum(1 for record in prepared if record["filled"])
    pnl_per_filled = _average_pnl(
        _float_value(summary.get("realized_pnl")) + _float_value(summary.get("unrealized_pnl")),
        filled_count,
    )
    return tuple(
        _CandidateSample(
            as_of=day_key,
            candidate=record["candidate"],
            generated_intent=bool(record["generated"]),
            filled=bool(record["filled"]),
            paper_pnl=pnl_per_filled if record["filled"] else 0.0,
            market_snapshot_source=str(record["source"]),
        )
        for record in prepared
    )


def _add_candidate_only_groups(
    *,
    group_stats: dict[str, dict[str, _GroupStats]],
    candidates: tuple[dict[str, Any], ...],
    day_key: str,
    blocked_by_counter: Counter[str],
    policy: dict[str, Any],
) -> None:
    samples = tuple(
        _CandidateSample(
            as_of=day_key,
            candidate=candidate,
            generated_intent=False,
            filled=False,
            paper_pnl=0.0,
            market_snapshot_source="unknown",
        )
        for candidate in candidates
    )
    _add_candidate_samples_to_groups(
        group_stats=group_stats,
        samples=samples,
        blocked_by_counter=blocked_by_counter,
        policy=policy,
    )


def _add_candidate_samples_to_groups(
    *,
    group_stats: dict[str, dict[str, _GroupStats]],
    samples: tuple[_CandidateSample, ...],
    blocked_by_counter: Counter[str],
    policy: dict[str, Any],
) -> None:
    for sample in samples:
        candidate = sample.candidate
        blockers = _strings(candidate.get("blocked_by")) or ["none"]
        for blocker in blockers:
            if blocker != "none":
                blocked_by_counter[blocker] += 1
        group_keys = {
            "by_strategy_id": [_string_value(candidate.get("strategy_id")) or "missing"],
            "by_symbol": [_string_value(candidate.get("symbol")) or "missing"],
            "by_reason_code": _strings(candidate.get("reason_codes")) or ["none"],
            "by_blocked_by": blockers,
            "by_confidence_bucket": [_confidence_bucket(candidate.get("confidence"), policy)],
        }
        for group_name, keys in group_keys.items():
            for key in keys:
                _add_sample_to_group(group_stats[group_name], key, sample)


def _add_market_snapshot_source_groups(
    groups: dict[str, _GroupStats],
    *,
    day_key: str,
    source_counts: Counter[str],
    filled_count: int,
    paper_pnl: float,
) -> None:
    total_sources = sum(source_counts.values())
    if total_sources <= 0:
        return
    for source, count in sorted(source_counts.items()):
        if count <= 0:
            continue
        stats = groups.setdefault(source, _GroupStats())
        stats.dates.add(day_key)
        stats.candidate_count += count
        stats.generated_intents += count
        allocated_fills = int(round(filled_count * (count / total_sources)))
        stats.filled_count += allocated_fills
        stats.paper_pnl_total += paper_pnl * (count / total_sources)
        stats.market_snapshot_source_counts[source] += count


def _add_sample_to_group(
    groups: dict[str, _GroupStats],
    key: str,
    sample: _CandidateSample,
) -> None:
    stats = groups.setdefault(key, _GroupStats())
    stats.dates.add(sample.as_of)
    stats.candidate_count += 1
    if sample.generated_intent:
        stats.generated_intents += 1
        if sample.market_snapshot_source:
            stats.market_snapshot_source_counts[sample.market_snapshot_source] += 1
    if sample.filled:
        stats.filled_count += 1
        stats.paper_pnl_total += sample.paper_pnl


def _serialize_aggregations(
    group_stats: dict[str, dict[str, _GroupStats]],
    policy: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    return {
        group_name: _serialize_group(stats_by_key, policy)
        for group_name, stats_by_key in group_stats.items()
    }


def _serialize_group(
    stats_by_key: dict[str, _GroupStats],
    policy: dict[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for key, stats in stats_by_key.items():
        source_total = sum(stats.market_snapshot_source_counts.values())
        synthetic_ratio = (
            stats.market_snapshot_source_counts.get("synthetic_limit_price", 0) / source_total
            if source_total
            else 0.0
        )
        quality_reasons = _group_quality_reasons(stats, synthetic_ratio, policy)
        rows.append(
            {
                "key": key,
                "sample_count": len(stats.dates),
                "candidate_count": stats.candidate_count,
                "generated_intents": stats.generated_intents,
                "filled_count": stats.filled_count,
                "avg_paper_pnl": _average_pnl(stats.paper_pnl_total, stats.filled_count),
                "synthetic_snapshot_ratio": synthetic_ratio,
                "quality_status": quality_reasons[0] if quality_reasons else "PASS",
                "quality_reasons": quality_reasons,
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            -_int_value(row.get("candidate_count")),
            str(row.get("key")),
        ),
    )


def _group_quality_reasons(
    stats: _GroupStats,
    synthetic_ratio: float,
    policy: dict[str, Any],
) -> list[str]:
    thresholds = _mapping(policy["thresholds"])
    reasons: list[str] = []
    if not stats.candidate_count:
        return ["NO_DATA"]
    if len(stats.dates) < _int_value(thresholds["minimum_sample_count"]):
        reasons.append("INSUFFICIENT_SAMPLE")
    if stats.generated_intents and stats.filled_count < _int_value(
        thresholds["minimum_filled_count"]
    ):
        reasons.append("INSUFFICIENT_FILLED_SAMPLE")
    if synthetic_ratio > _float_value(thresholds["maximum_synthetic_snapshot_ratio"]):
        reasons.append("LOW_DATA_QUALITY")
    source_total = sum(stats.market_snapshot_source_counts.values())
    historical_coverage = (
        stats.market_snapshot_source_counts.get("historical_ohlc", 0) / source_total
        if source_total
        else 0.0
    )
    if (
        stats.generated_intents
        and source_total
        and historical_coverage < _float_value(thresholds["minimum_historical_ohlc_coverage"])
    ):
        reasons.append("LIMITED_MARKET_DATA")
    return reasons


def _render_aggregation_section(
    aggregations: dict[str, Any],
    key: str,
    title: str,
) -> list[str]:
    rows = _records(aggregations.get(key))
    lines = [
        "",
        f"## {title}",
        "",
        "| Key | Samples | Candidates | Intents | Filled | Avg Paper PnL | Synthetic | Quality |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    if not rows:
        lines.append("| none | 0 | 0 | 0 | 0 | 0.00 | 0.00% | NO_DATA |")
        return lines
    for row in rows:
        lines.append(
            "| "
            f"{row.get('key')} | "
            f"{row.get('sample_count', 0)} | "
            f"{row.get('candidate_count', 0)} | "
            f"{row.get('generated_intents', 0)} | "
            f"{row.get('filled_count', 0)} | "
            f"{_float_value(row.get('avg_paper_pnl')):.2f} | "
            f"{_format_percent(_float_value(row.get('synthetic_snapshot_ratio')))} | "
            f"{row.get('quality_status')} |"
        )
    return lines


def _empty_group_stats() -> dict[str, dict[str, _GroupStats]]:
    return {
        "by_strategy_id": {},
        "by_symbol": {},
        "by_reason_code": {},
        "by_blocked_by": {},
        "by_confidence_bucket": {},
        "by_market_snapshot_source": {},
    }


def _load_policy(path: Path) -> dict[str, Any]:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"failed to read paper signal quality policy: {path}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"paper signal quality policy must be a YAML mapping: {path}")
    thresholds = raw.get("thresholds")
    if not isinstance(thresholds, dict):
        raise ValueError("paper signal quality policy missing thresholds")
    for key in (
        "minimum_sample_count",
        "minimum_filled_count",
        "maximum_synthetic_snapshot_ratio",
        "minimum_historical_ohlc_coverage",
        "minimum_reconciliation_pass_ratio",
    ):
        if key not in thresholds:
            raise ValueError(f"paper signal quality policy missing threshold: {key}")
    return raw


def _policy_report(policy: dict[str, Any], policy_path: Path) -> dict[str, Any]:
    return {
        "policy_id": _string_value(policy.get("policy_id")) or "paper_signal_quality_policy",
        "version": policy.get("version"),
        "status": _string_value(policy.get("status")),
        "owner": _string_value(policy.get("owner")),
        "production_effect": _string_value(policy.get("production_effect")) or "none",
        "path": str(policy_path),
        "thresholds": dict(_mapping(policy.get("thresholds"))),
        "review_condition": _string_value(policy.get("review_condition")),
    }


def _optional_replay_source(
    replay_json_path: Path | None,
    replay_payload: dict[str, Any],
) -> dict[str, Any]:
    if replay_json_path is None:
        return {
            "provided": False,
            "path": "",
            "exists": False,
            "used_as_daily_summary_fallback": False,
        }
    return {
        "provided": True,
        "path": str(replay_json_path),
        "exists": replay_json_path.exists(),
        "report_type": _string_value(replay_payload.get("report_type")),
        "start": _string_value(replay_payload.get("start")),
        "end": _string_value(replay_payload.get("end")),
        "used_as_daily_summary_fallback": bool(_replay_daily_results_by_date(replay_payload)),
    }


def _artifact_record(
    *,
    path: Path,
    reports_dir: Path,
    exists: bool,
    source: str,
) -> dict[str, Any]:
    return {
        "path": str(path),
        "href": _report_href(path, reports_dir),
        "exists": exists,
        "source": source,
    }


def _summary_from_replay_day(record: dict[str, Any] | None) -> dict[str, Any]:
    if not record:
        return {}
    return {
        "report_type": "paper_trading_summary",
        "status": _string_value(record.get("status")) or "UNKNOWN",
        "production_effect": _string_value(record.get("production_effect")) or "none",
        "candidate_count": _int_value(record.get("candidate_count")),
        "blocked_candidates": _int_value(record.get("blocked_candidates")),
        "generated_intents": _int_value(record.get("generated_intents")),
        "filled": _int_value(record.get("filled")),
        "realized_pnl": _float_value(record.get("realized_pnl")),
        "unrealized_pnl": _float_value(record.get("unrealized_pnl")),
        "reconciliation_status": _string_value(record.get("reconciliation_status")) or "MISSING",
        "market_snapshot_source": _string_value(record.get("market_snapshot_source")) or "none",
        "market_snapshot_source_counts": _mapping(record.get("market_snapshot_source_counts")),
    }


def _replay_daily_results_by_date(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if payload.get("report_type") != "paper_trading_replay":
        return {}
    return {
        _string_value(record.get("as_of")): record
        for record in _records(payload.get("daily_results"))
        if _string_value(record.get("as_of"))
    }


def _candidate_records(payload: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    if payload.get("report_type") != "order_intent_candidates":
        return ()
    return _records(payload.get("candidates"))


def _candidate_generates_intent(candidate: dict[str, Any]) -> bool:
    if bool(candidate.get("blocked")):
        return False
    mode = _string_value(candidate.get("mode"))
    return mode in {"", "paper"}


def _confidence_bucket(value: object, policy: dict[str, Any]) -> str:
    confidence = _optional_float(value)
    if confidence is None:
        return "missing_confidence"
    for raw_bucket in _records(policy.get("confidence_buckets")):
        bucket = _string_value(raw_bucket.get("bucket"))
        label = _string_value(raw_bucket.get("label")) or bucket
        if bucket == "missing":
            continue
        minimum = _optional_float(raw_bucket.get("min_inclusive"))
        max_exclusive = _optional_float(raw_bucket.get("max_exclusive"))
        max_inclusive = _optional_float(raw_bucket.get("max_inclusive"))
        if minimum is not None and confidence < minimum:
            continue
        if max_exclusive is not None and confidence >= max_exclusive:
            continue
        if max_inclusive is not None and confidence > max_inclusive:
            continue
        return label
    return "out_of_policy_range"


def _snapshot_source_counts(payload: dict[str, Any]) -> Counter[str]:
    counts: Counter[str] = Counter()
    source_counts = _mapping(payload.get("market_snapshot_source_counts"))
    for source, raw_count in source_counts.items():
        source_name = _string_value(source) or str(source)
        count = _int_value(raw_count)
        if source_name and count > 0:
            counts[source_name] += count
    if counts:
        return counts
    source = _string_value(payload.get("market_snapshot_source"))
    if source and source != "none":
        counts[source] += _int_value(payload.get("generated_intents"), default=1)
    return counts


def _single_snapshot_source(source_counts: Counter[str]) -> str:
    active = [source for source, count in source_counts.items() if count > 0]
    if len(active) == 1:
        return active[0]
    if len(active) > 1:
        return "mixed"
    return "unknown"


def _average_pnl(total: float, count: int) -> float:
    return total / count if count else 0.0


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _report_href(path: Path, reports_dir: Path) -> str:
    try:
        return path.relative_to(reports_dir).as_posix()
    except ValueError:
        try:
            return path.resolve().relative_to(reports_dir.resolve()).as_posix()
        except (OSError, RuntimeError, ValueError):
            return path.as_posix()


def _records(value: object) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item]


def _string_value(value: object) -> str:
    return value if isinstance(value, str) else ""


def _int_value(value: object, *, default: int = 0) -> int:
    try:
        if isinstance(value, (str, bytes, bytearray, int, float)):
            return int(value)
    except (TypeError, ValueError):
        return default
    return default


def _float_value(value: object) -> float:
    try:
        if isinstance(value, (str, bytes, bytearray, int, float)):
            return float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0


def _optional_float(value: object) -> float | None:
    try:
        if isinstance(value, (str, bytes, bytearray, int, float)):
            return float(value)
    except (TypeError, ValueError):
        return None
    return None


def _format_percent(value: float) -> str:
    return f"{value * 100:.2f}%"


def _format_check_value(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)
