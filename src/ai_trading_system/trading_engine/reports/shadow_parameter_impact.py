from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.yaml_loader import safe_load_yaml_path

SHADOW_PARAMETER_IMPACT_SCHEMA_VERSION = 1
SHADOW_PARAMETER_IMPACT_REPORT_TYPE = "shadow_parameter_impact"
SHADOW_PARAMETER_IMPACT_WINDOWS: tuple[int, ...] = (7, 14, 30)
SOURCE_PROFILES: tuple[str, ...] = ("production", "shadow", "unknown")
REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_SHADOW_PARAMETER_IMPACT_POLICY_PATH = (
    REPO_ROOT / "config" / "shadow_parameter_impact_policy.yaml"
)

STATUS_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
STATUS_OBSERVE_ONLY = "OBSERVE_ONLY"
STATUS_SHADOW_PROMISING_BUT_LIMITED = "SHADOW_PROMISING_BUT_LIMITED"
STATUS_NO_CLEAR_IMPROVEMENT = "NO_CLEAR_IMPROVEMENT"
STATUS_SHADOW_UNRELIABLE = "SHADOW_UNRELIABLE"
STATUS_LOW_DATA_QUALITY = "LOW_DATA_QUALITY"
ALLOWED_IMPACT_STATUSES = {
    STATUS_INSUFFICIENT_DATA,
    STATUS_OBSERVE_ONLY,
    STATUS_SHADOW_PROMISING_BUT_LIMITED,
    STATUS_NO_CLEAR_IMPROVEMENT,
    STATUS_SHADOW_UNRELIABLE,
    STATUS_LOW_DATA_QUALITY,
}

REASON_INSUFFICIENT_SHADOW_SAMPLE = "insufficient_shadow_sample"
REASON_INSUFFICIENT_PRODUCTION_BASELINE = "insufficient_production_baseline"
REASON_LOW_DATA_QUALITY = "low_data_quality"
REASON_SYNTHETIC_SNAPSHOT_RATIO_TOO_HIGH = "synthetic_snapshot_ratio_too_high"
REASON_DAILY_INDEPENDENT_ONLY = "daily_independent_only"
REASON_UNRELIABLE_RECONCILIATION = "unreliable_reconciliation"
REASON_CONTINUOUS_REPLAY_MISSING = "continuous_replay_missing"
REASON_EXPLANATIONS = {
    REASON_INSUFFICIENT_SHADOW_SAMPLE: "shadow profile 样本少于 policy floor。",
    REASON_INSUFFICIENT_PRODUCTION_BASELINE: "production baseline 样本少于 policy floor。",
    REASON_LOW_DATA_QUALITY: "historical OHLC 覆盖不足，数据质量无法支持 impact 判断。",
    REASON_SYNTHETIC_SNAPSHOT_RATIO_TOO_HIGH: (
        "synthetic limit price snapshot 占比高于 policy 上限。"
    ),
    REASON_DAILY_INDEPENDENT_ONLY: (
        "找到的 replay 不是 continuous portfolio 或没有 portfolio carry-forward，"
        "不能当作连续组合结果。"
    ),
    REASON_UNRELIABLE_RECONCILIATION: "portfolio reconciliation PASS 比例低于 policy floor。",
    REASON_CONTINUOUS_REPLAY_MISSING: (
        "未找到可用于 shadow impact comparison 的 continuous-portfolio replay artifact。"
    ),
    "paper_only_simulation": (
        "shadow impact evaluation 只解释 paper-only 模拟和只读报告，不是实盘或上线依据。"
    ),
}


@dataclass
class _BucketStats:
    dates: set[str] = field(default_factory=set)
    candidate_count: int = 0
    generated_intents: int = 0
    filled_count: int = 0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0


@dataclass
class _ProfileStats:
    dates: set[str] = field(default_factory=set)
    summary_dates: set[str] = field(default_factory=set)
    candidate_dates: set[str] = field(default_factory=set)
    signal_quality_dates: set[str] = field(default_factory=set)
    candidate_count: int = 0
    generated_intents: int = 0
    approved: int = 0
    rejected: int = 0
    submitted: int = 0
    filled: int = 0
    open: int = 0
    cancelled: int = 0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    market_snapshot_source_counts: Counter[str] = field(default_factory=Counter)
    reconciliation_status_counts: Counter[str] = field(default_factory=Counter)
    blocked_by_counts: Counter[str] = field(default_factory=Counter)
    reason_code_counts: Counter[str] = field(default_factory=Counter)
    mode_counts: Counter[str] = field(default_factory=Counter)
    strategy_version_counts: Counter[str] = field(default_factory=Counter)
    signal_quality_status_counts: Counter[str] = field(default_factory=Counter)
    confidence_buckets: dict[str, _BucketStats] = field(default_factory=dict)


def default_shadow_parameter_impact_json_path(reports_dir: Path, as_of: date) -> Path:
    return reports_dir / f"shadow_parameter_impact_{as_of.isoformat()}.json"


def build_shadow_parameter_impact_payload(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    policy_path: Path = DEFAULT_SHADOW_PARAMETER_IMPACT_POLICY_PATH,
    replay_json_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    selected_window_days: int = 30,
) -> dict[str, Any]:
    if selected_window_days not in set(SHADOW_PARAMETER_IMPACT_WINDOWS):
        raise ValueError("selected_window_days must be one of 7, 14, or 30")

    policy = _load_policy(policy_path)
    output_json_path = output_json_path or default_shadow_parameter_impact_json_path(
        reports_dir,
        as_of,
    )
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    replay_payload = _select_replay_payload(
        reports_dir=reports_dir,
        as_of=as_of,
        replay_json_path=replay_json_path,
    )
    replay_mode = _paper_evaluation_mode(replay_payload)
    windows = {
        str(days): _build_window_evaluation(
            as_of=as_of,
            reports_dir=reports_dir,
            days=days,
            policy=policy,
            replay_payload=replay_payload,
            replay_mode=replay_mode,
        )
        for days in SHADOW_PARAMETER_IMPACT_WINDOWS
    }
    selected = windows[str(selected_window_days)]
    policy_report = _policy_report(policy, policy_path)
    warnings = _impact_warnings(replay_payload, replay_mode)
    warning_codes = [str(record["code"]) for record in warnings]
    warning_explanations = {str(record["code"]): str(record["message"]) for record in warnings}
    for window in windows.values():
        gate = _mapping(window.get("impact_gate"))
        blocking_reasons = _strings(gate.get("blocking_reasons"))
        gate["warnings"] = warning_codes
        gate["warning_explanations"] = warning_explanations
        reason_explanations = dict(_mapping(gate.get("reason_explanations")))
        reason_explanations.update(warning_explanations)
        gate["reason_explanations"] = reason_explanations
        gate["explanation"] = _gate_explanation(
            str(gate.get("status", STATUS_INSUFFICIENT_DATA)),
            blocking_reasons,
            warning_codes,
        )

    return {
        "schema_version": SHADOW_PARAMETER_IMPACT_SCHEMA_VERSION,
        "report_type": SHADOW_PARAMETER_IMPACT_REPORT_TYPE,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "as_of": as_of.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "production_effect": "none",
        "impact_status": selected["impact_status"],
        "selected_window_days": selected_window_days,
        "policy_id": policy_report["policy_id"],
        "policy_version": policy_report["version"],
        "thresholds_snapshot": policy_report["thresholds"],
        "policy": policy_report,
        "warnings": warnings,
        "warning_codes": warning_codes,
        "paper_evaluation_mode": replay_mode,
        "evaluation_scope": {
            "observe_only": True,
            "production_effect": "none",
            "changes_production_parameters": False,
            "changes_production_position_recommendation": False,
            "changes_parameter_promotion": False,
            "changes_trade_execution": False,
            "paper_pnl_is_launch_evidence": False,
        },
        "safety_boundary": {
            "reads_broker_api_key": False,
            "calls_real_broker": False,
            "runs_paper_runner": False,
            "runs_replay": False,
            "changes_production_parameters": False,
            "changes_production_position_recommendation": False,
            "changes_parameter_promotion": False,
            "changes_trade_execution": False,
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
            "candidates": selected["source_artifacts"]["candidates"],
            "paper_trading_summaries": selected["source_artifacts"]["paper_trading_summaries"],
            "paper_signal_quality": selected["source_artifacts"]["paper_signal_quality"],
        },
        "summary": selected["summary"],
        "impact_gate": selected["impact_gate"],
        "profile_comparison": selected["profile_comparison"],
        "distributions": selected["distributions"],
        "confidence_bucket_performance": selected["confidence_bucket_performance"],
        "continuous_replay": selected["continuous_replay"],
        "windows": windows,
        "notes": [
            "本报告只观察 shadow 参数 impact，不是实盘交易、参数晋级或 production 建议。",
            "短期 paper PnL 为正不会单独构成 shadow 改善证据。",
            "报告不会读取 broker API key、不会调用真实 broker、不会触发 paper runner 或 replay。",
        ],
    }


def write_shadow_parameter_impact_report(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    policy_path: Path = DEFAULT_SHADOW_PARAMETER_IMPACT_POLICY_PATH,
    replay_json_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    selected_window_days: int = 30,
) -> dict[str, Any]:
    payload = build_shadow_parameter_impact_payload(
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
    md_path.write_text(render_shadow_parameter_impact_report(payload), encoding="utf-8")
    return payload


def render_shadow_parameter_impact_report(payload: dict[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    gate = _mapping(payload.get("impact_gate"))
    thresholds = _mapping(payload.get("thresholds_snapshot"))
    replay_mode = _mapping(payload.get("paper_evaluation_mode"))
    comparison = _mapping(payload.get("profile_comparison"))
    production = _mapping(comparison.get("production"))
    shadow = _mapping(comparison.get("shadow"))
    continuous = _mapping(payload.get("continuous_replay"))
    replay_source = _mapping(continuous.get("source_artifact"))
    replay_date_range = _mapping(replay_source.get("date_range"))
    lines = [
        "# Shadow Parameter Impact Evaluation",
        "",
        f"- 评估日期：{payload.get('as_of')}",
        "- 市场阶段：`ai_after_chatgpt`",
        f"- impact_status：{payload.get('impact_status')}",
        f"- Policy：{payload.get('policy_id')} v{payload.get('policy_version')}",
        "- observe-only：true",
        "- production_effect=none",
        (
            "- paper_evaluation_mode："
            f"replay_mode={replay_mode.get('replay_mode', 'daily_independent')}；"
            "portfolio_carry_forward="
            f"{replay_mode.get('portfolio_carry_forward', False)}"
        ),
        (
            "- 安全边界：不读取 broker API key；不调用真实 broker；不触发 paper runner / "
            "replay；不改变 production 参数、仓位建议、参数晋级或交易执行。"
        ),
        "- Paper PnL 只作诊断字段，不能单独证明 shadow 更好。",
        "",
        "## Policy Thresholds",
        "",
        "| Threshold | Value |",
        "|---|---:|",
        (
            "| minimum_shadow_sample_count | "
            f"{thresholds.get('minimum_shadow_sample_count', 'missing')} |"
        ),
        (
            "| minimum_production_baseline_count | "
            f"{thresholds.get('minimum_production_baseline_count', 'missing')} |"
        ),
        (
            "| minimum_filled_count_for_comparison | "
            f"{thresholds.get('minimum_filled_count_for_comparison', 'missing')} |"
        ),
        (
            "| maximum_synthetic_snapshot_ratio | "
            f"{thresholds.get('maximum_synthetic_snapshot_ratio', 'missing')} |"
        ),
        (
            "| minimum_historical_ohlc_coverage | "
            f"{thresholds.get('minimum_historical_ohlc_coverage', 'missing')} |"
        ),
        (
            "| minimum_reconciliation_pass_ratio | "
            f"{thresholds.get('minimum_reconciliation_pass_ratio', 'missing')} |"
        ),
        "",
        "## 摘要",
        "",
        "| 指标 | Production | Shadow |",
        "|---|---:|---:|",
        f"| sample_count | {production.get('sample_count', 0)} | {shadow.get('sample_count', 0)} |",
        (
            f"| candidate_count | {production.get('candidate_count', 0)} | "
            f"{shadow.get('candidate_count', 0)} |"
        ),
        (
            f"| generated_intents | {production.get('generated_intents', 0)} | "
            f"{shadow.get('generated_intents', 0)} |"
        ),
        f"| filled_count | {production.get('filled_count', 0)} | {shadow.get('filled_count', 0)} |",
        (
            f"| paper_pnl_total | {_format_money_value(production.get('paper_pnl_total'))} | "
            f"{_format_money_value(shadow.get('paper_pnl_total'))} |"
        ),
        (
            "| synthetic_snapshot_ratio | "
            f"{_format_percent(_optional_float(production.get('synthetic_snapshot_ratio')))} | "
            f"{_format_percent(_optional_float(shadow.get('synthetic_snapshot_ratio')))} |"
        ),
        (
            "| reconciliation_pass_ratio | "
            f"{_format_percent(_optional_float(production.get('reconciliation_pass_ratio')))} | "
            f"{_format_percent(_optional_float(shadow.get('reconciliation_pass_ratio')))} |"
        ),
        "",
        "## Continuous Replay",
        "",
        f"- available：{continuous.get('available', False)}",
        f"- replay_mode：{continuous.get('replay_mode', 'daily_independent')}",
        f"- artifact_path：{replay_source.get('path') or continuous.get('path') or 'missing'}",
        (
            "- date_range："
            f"{replay_date_range.get('start') or continuous.get('start') or 'missing'}"
            " to "
            f"{replay_date_range.get('end') or continuous.get('end') or 'missing'}"
        ),
        f"- final_equity production/shadow：{_continuous_metric_pair(continuous, 'final_equity')}",
        (
            "- max_drawdown production/shadow："
            f"{_continuous_metric_pair(continuous, 'max_drawdown_pct')}"
        ),
        "",
        "## Impact Gate",
        "",
        f"- Gate status：{gate.get('status', STATUS_INSUFFICIENT_DATA)}",
        f"- Explanation：{gate.get('explanation', '')}",
        f"- Blocking reasons：{', '.join(_strings(gate.get('blocking_reasons'))) or 'none'}",
        f"- Warnings：{', '.join(_strings(gate.get('warnings'))) or 'none'}",
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
    lines.extend(
        [
            "",
            "## Blocked Reason Explanation",
            "",
            "| Reason | Explanation |",
            "|---|---|",
        ]
    )
    reason_explanations = _mapping(gate.get("reason_explanations"))
    if reason_explanations:
        for reason, explanation in sorted(reason_explanations.items()):
            lines.append(f"| {reason} | {explanation} |")
    else:
        lines.append("| none | 当前没有触发 blocking reason。 |")
    lines.extend(
        [
            "",
            "## 窗口状态",
            "",
            (
                "| Window | Impact status | Production samples | Shadow samples | "
                "Shadow filled | Shadow PnL |"
            ),
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for window_key, window in _mapping(payload.get("windows")).items():
        window_comparison = _mapping(_mapping(window).get("profile_comparison"))
        window_production = _mapping(window_comparison.get("production"))
        window_shadow = _mapping(window_comparison.get("shadow"))
        lines.append(
            "| "
            f"{window_key} 日 | "
            f"{window.get('impact_status')} | "
            f"{window_production.get('sample_count', 0)} | "
            f"{window_shadow.get('sample_count', 0)} | "
            f"{window_shadow.get('filled_count', 0)} | "
            f"{_format_money_value(window_shadow.get('paper_pnl_total'))} |"
        )
    lines.extend(
        [
            "",
            "## Distribution Snapshot",
            "",
            (
                "- production blocked_by："
                f"{_format_top_records(production.get('blocked_by_distribution'))}"
            ),
            f"- shadow blocked_by：{_format_top_records(shadow.get('blocked_by_distribution'))}",
            (
                "- production reason_code："
                f"{_format_top_records(production.get('reason_code_distribution'))}"
            ),
            f"- shadow reason_code：{_format_top_records(shadow.get('reason_code_distribution'))}",
            "",
        ]
    )
    if summary.get("main_warning"):
        lines.append(f"- 主要 warning：{summary.get('main_warning')}")
    return "\n".join(lines).rstrip() + "\n"


def _build_window_evaluation(
    *,
    as_of: date,
    reports_dir: Path,
    days: int,
    policy: dict[str, Any],
    replay_payload: dict[str, Any],
    replay_mode: dict[str, Any],
) -> dict[str, Any]:
    start = as_of - timedelta(days=days - 1)
    stats = _empty_profile_stats()
    source_artifacts = {
        "candidates": [],
        "paper_trading_summaries": [],
        "paper_signal_quality": [],
    }
    daily_results: list[dict[str, Any]] = []

    for offset in range(days):
        current = start + timedelta(days=offset)
        day_key = current.isoformat()
        candidate_path = reports_dir / f"order_intent_candidates_{day_key}.json"
        summary_path = reports_dir / f"paper_trading_summary_{day_key}.json"
        quality_path = reports_dir / f"paper_signal_quality_{day_key}.json"
        candidate_payload = _read_json_object(candidate_path)
        summary_payload = _read_json_object(summary_path)
        quality_payload = _read_json_object(quality_path)
        candidate_valid = candidate_payload.get("report_type") == "order_intent_candidates"
        summary_valid = summary_payload.get("report_type") == "paper_trading_summary"
        quality_valid = quality_payload.get("report_type") == "paper_signal_quality"
        candidates = _records(candidate_payload.get("candidates")) if candidate_valid else ()
        candidate_profiles = _add_candidates_to_stats(
            stats=stats,
            candidates=candidates,
            day_key=day_key,
            policy=policy,
        )
        if summary_valid:
            _add_summary_to_stats(
                stats=stats,
                summary=summary_payload,
                candidates=candidates,
                candidate_profiles=candidate_profiles,
                day_key=day_key,
                policy=policy,
            )
        if quality_valid:
            _add_quality_to_stats(stats=stats, quality=quality_payload, day_key=day_key)
        source_artifacts["candidates"].append(
            _artifact_record(
                path=candidate_path,
                reports_dir=reports_dir,
                exists=candidate_valid,
            )
        )
        source_artifacts["paper_trading_summaries"].append(
            _artifact_record(
                path=summary_path,
                reports_dir=reports_dir,
                exists=summary_valid,
            )
        )
        source_artifacts["paper_signal_quality"].append(
            _artifact_record(
                path=quality_path,
                reports_dir=reports_dir,
                exists=quality_valid,
            )
        )
        daily_results.append(
            {
                "as_of": day_key,
                "candidate_exists": candidate_valid,
                "summary_exists": summary_valid,
                "paper_signal_quality_exists": quality_valid,
                "source_profile_distribution": dict(sorted(candidate_profiles.items())),
            }
        )

    profile_comparison = {
        profile: _profile_summary(profile_stats) for profile, profile_stats in stats.items()
    }
    continuous_replay = _continuous_replay_summary(replay_payload, profile_comparison)
    gate = _impact_gate(
        profile_comparison=profile_comparison,
        continuous_replay=continuous_replay,
        policy=policy,
    )
    impact_status = gate["status"]
    if impact_status not in ALLOWED_IMPACT_STATUSES:
        raise ValueError(f"unsupported shadow impact status: {impact_status}")
    summary = _impact_summary(
        profile_comparison=profile_comparison,
        impact_status=impact_status,
        gate=gate,
        continuous_replay=continuous_replay,
    )
    return {
        "window_days": days,
        "start": start.isoformat(),
        "end": as_of.isoformat(),
        "impact_status": impact_status,
        "summary": summary,
        "impact_gate": gate,
        "profile_comparison": profile_comparison,
        "distributions": _profile_distributions(profile_comparison),
        "confidence_bucket_performance": {
            profile: _records(summary_by_profile.get("confidence_bucket_performance"))
            for profile, summary_by_profile in profile_comparison.items()
        },
        "continuous_replay": continuous_replay,
        "daily_results": daily_results,
        "source_artifacts": source_artifacts,
        "production_effect": "none",
    }


def _add_candidates_to_stats(
    *,
    stats: dict[str, _ProfileStats],
    candidates: tuple[dict[str, Any], ...],
    day_key: str,
    policy: dict[str, Any],
) -> Counter[str]:
    profile_counts: Counter[str] = Counter()
    for candidate in candidates:
        profile = _source_profile(candidate)
        profile_counts[profile] += 1
        profile_stats = stats[profile]
        profile_stats.dates.add(day_key)
        profile_stats.candidate_dates.add(day_key)
        profile_stats.candidate_count += 1
        mode = _string_value(candidate.get("mode")) or "missing"
        strategy_version = _string_value(candidate.get("strategy_version")) or "missing"
        profile_stats.mode_counts[mode] += 1
        profile_stats.strategy_version_counts[strategy_version] += 1
        for blocker in _strings(candidate.get("blocked_by")) or ["none"]:
            if blocker != "none":
                profile_stats.blocked_by_counts[blocker] += 1
        for reason_code in _strings(candidate.get("reason_codes")) or ["none"]:
            if reason_code != "none":
                profile_stats.reason_code_counts[reason_code] += 1
        bucket = _confidence_bucket(candidate.get("confidence"), policy)
        bucket_stats = profile_stats.confidence_buckets.setdefault(bucket, _BucketStats())
        bucket_stats.dates.add(day_key)
        bucket_stats.candidate_count += 1
    return profile_counts


def _add_summary_to_stats(
    *,
    stats: dict[str, _ProfileStats],
    summary: dict[str, Any],
    candidates: tuple[dict[str, Any], ...],
    candidate_profiles: Counter[str],
    day_key: str,
    policy: dict[str, Any],
) -> None:
    whole_profile = _whole_summary_profile(summary, candidate_profiles)
    candidate_records = _records(summary.get("candidate_records"))
    if whole_profile is not None:
        _add_whole_summary_to_profile(stats[whole_profile], summary, day_key)
        _add_summary_candidate_records_to_buckets(
            profile_stats=stats[whole_profile],
            records=candidate_records,
            candidates=candidates,
            day_key=day_key,
            policy=policy,
            use_summary_totals=True,
            summary=summary,
        )
        return
    if candidate_records:
        _add_split_summary_records(
            stats=stats,
            summary=summary,
            records=candidate_records,
            candidates=candidates,
            day_key=day_key,
            policy=policy,
        )
        return
    _add_whole_summary_to_profile(stats["unknown"], summary, day_key)


def _add_whole_summary_to_profile(
    profile_stats: _ProfileStats,
    summary: dict[str, Any],
    day_key: str,
) -> None:
    profile_stats.dates.add(day_key)
    profile_stats.summary_dates.add(day_key)
    profile_stats.generated_intents += _int_value(summary.get("generated_intents"))
    profile_stats.approved += _int_value(summary.get("approved"))
    profile_stats.rejected += _int_value(summary.get("rejected"))
    profile_stats.submitted += _int_value(summary.get("submitted"))
    profile_stats.filled += _int_value(summary.get("filled"))
    profile_stats.open += _int_value(summary.get("open"))
    profile_stats.cancelled += _int_value(summary.get("cancelled"))
    profile_stats.realized_pnl += _float_value(summary.get("realized_pnl"))
    profile_stats.unrealized_pnl += _float_value(summary.get("unrealized_pnl"))
    reconciliation = _string_value(summary.get("reconciliation_status")) or "MISSING"
    profile_stats.reconciliation_status_counts[reconciliation] += 1
    profile_stats.market_snapshot_source_counts.update(_snapshot_source_counts(summary))


def _add_summary_candidate_records_to_buckets(
    *,
    profile_stats: _ProfileStats,
    records: tuple[dict[str, Any], ...],
    candidates: tuple[dict[str, Any], ...],
    day_key: str,
    policy: dict[str, Any],
    use_summary_totals: bool,
    summary: dict[str, Any],
) -> None:
    if not records:
        return
    candidates_by_id = _candidates_by_id(candidates)
    filled_records = [record for record in records if _bool_value(record.get("filled"))]
    allocated_pnl = _average_pnl(
        _float_value(summary.get("realized_pnl")) + _float_value(summary.get("unrealized_pnl")),
        len(filled_records),
    )
    for record in records:
        candidate = candidates_by_id.get(_string_value(record.get("candidate_id")), record)
        bucket = _confidence_bucket(candidate.get("confidence"), policy)
        bucket_stats = profile_stats.confidence_buckets.setdefault(bucket, _BucketStats())
        bucket_stats.dates.add(day_key)
        if not use_summary_totals:
            bucket_stats.candidate_count += 1
        if _bool_value(record.get("generated_intent")):
            bucket_stats.generated_intents += 1
        if _bool_value(record.get("filled")):
            bucket_stats.filled_count += 1
            record_realized = _optional_float(record.get("realized_pnl"))
            record_unrealized = _optional_float(record.get("unrealized_pnl"))
            if record_realized is None and record_unrealized is None:
                bucket_stats.unrealized_pnl += (
                    _optional_float(record.get("paper_pnl")) or allocated_pnl
                )
            else:
                bucket_stats.realized_pnl += record_realized or 0.0
                bucket_stats.unrealized_pnl += record_unrealized or 0.0


def _add_split_summary_records(
    *,
    stats: dict[str, _ProfileStats],
    summary: dict[str, Any],
    records: tuple[dict[str, Any], ...],
    candidates: tuple[dict[str, Any], ...],
    day_key: str,
    policy: dict[str, Any],
) -> None:
    candidates_by_id = _candidates_by_id(candidates)
    filled_records = [record for record in records if _bool_value(record.get("filled"))]
    allocated_pnl = _average_pnl(
        _float_value(summary.get("realized_pnl")) + _float_value(summary.get("unrealized_pnl")),
        len(filled_records),
    )
    for record in records:
        candidate = candidates_by_id.get(_string_value(record.get("candidate_id")), record)
        profile = _source_profile(record, fallback=_source_profile(candidate))
        profile_stats = stats[profile]
        profile_stats.dates.add(day_key)
        profile_stats.summary_dates.add(day_key)
        if _bool_value(record.get("generated_intent")):
            profile_stats.generated_intents += 1
        profile_stats.approved += _int_value(record.get("approved"))
        profile_stats.rejected += _int_value(record.get("rejected"))
        profile_stats.submitted += _int_value(record.get("submitted"))
        if _bool_value(record.get("filled")):
            profile_stats.filled += 1
        profile_stats.open += _int_value(record.get("open"))
        profile_stats.cancelled += _int_value(record.get("cancelled"))
        record_realized = _optional_float(record.get("realized_pnl"))
        record_unrealized = _optional_float(record.get("unrealized_pnl"))
        if record_realized is None and record_unrealized is None:
            if _bool_value(record.get("filled")):
                profile_stats.unrealized_pnl += (
                    _optional_float(record.get("paper_pnl")) or allocated_pnl
                )
        else:
            profile_stats.realized_pnl += record_realized or 0.0
            profile_stats.unrealized_pnl += record_unrealized or 0.0
        source = _string_value(record.get("market_snapshot_source"))
        if source:
            profile_stats.market_snapshot_source_counts[source] += 1
        reconciliation = _string_value(record.get("reconciliation_status")) or _string_value(
            summary.get("reconciliation_status")
        )
        if reconciliation:
            profile_stats.reconciliation_status_counts[reconciliation] += 1
        _add_summary_candidate_records_to_buckets(
            profile_stats=profile_stats,
            records=(record,),
            candidates=(candidate,),
            day_key=day_key,
            policy=policy,
            use_summary_totals=True,
            summary=summary,
        )


def _add_quality_to_stats(
    *,
    stats: dict[str, _ProfileStats],
    quality: dict[str, Any],
    day_key: str,
) -> None:
    profile = _source_profile(quality)
    if profile == "unknown":
        profile = _quality_profile_from_comparison(quality)
    if profile == "unknown":
        return
    profile_stats = stats[profile]
    profile_stats.dates.add(day_key)
    profile_stats.signal_quality_dates.add(day_key)
    profile_stats.signal_quality_status_counts[
        _string_value(quality.get("evaluation_status")) or "UNKNOWN"
    ] += 1


def _quality_profile_from_comparison(quality: dict[str, Any]) -> str:
    summary = _mapping(quality.get("summary"))
    profile = _source_profile(summary)
    if profile != "unknown":
        return profile
    return "unknown"


def _whole_summary_profile(summary: dict[str, Any], candidate_profiles: Counter[str]) -> str | None:
    explicit_profile = _source_profile(summary)
    if explicit_profile != "unknown":
        return explicit_profile
    active_profiles = [profile for profile, count in candidate_profiles.items() if count > 0]
    if len(active_profiles) == 1:
        return active_profiles[0]
    return None


def _profile_summary(stats: _ProfileStats) -> dict[str, Any]:
    source_total = sum(stats.market_snapshot_source_counts.values())
    synthetic_count = stats.market_snapshot_source_counts.get("synthetic_limit_price", 0)
    historical_count = stats.market_snapshot_source_counts.get("historical_ohlc", 0)
    reconciliation_total = sum(stats.reconciliation_status_counts.values())
    reconciliation_pass = sum(
        count
        for status, count in stats.reconciliation_status_counts.items()
        if status.startswith("PASS")
    )
    confidence_rows = _serialize_confidence_buckets(stats.confidence_buckets)
    return {
        "sample_count": len(stats.dates),
        "summary_sample_count": len(stats.summary_dates),
        "candidate_sample_count": len(stats.candidate_dates),
        "signal_quality_sample_count": len(stats.signal_quality_dates),
        "candidate_count": stats.candidate_count,
        "generated_intents": stats.generated_intents,
        "approved": stats.approved,
        "rejected": stats.rejected,
        "submitted": stats.submitted,
        "filled_count": stats.filled,
        "filled": stats.filled,
        "open": stats.open,
        "cancelled": stats.cancelled,
        "realized_pnl": stats.realized_pnl,
        "unrealized_pnl": stats.unrealized_pnl,
        "paper_pnl_total": stats.realized_pnl + stats.unrealized_pnl,
        "synthetic_snapshot_ratio": synthetic_count / source_total if source_total else 0.0,
        "historical_ohlc_coverage": historical_count / source_total if source_total else 0.0,
        "reconciliation_pass_ratio": (
            reconciliation_pass / reconciliation_total if reconciliation_total else 0.0
        ),
        "market_snapshot_source_counts": dict(sorted(stats.market_snapshot_source_counts.items())),
        "reconciliation_status_distribution": dict(
            sorted(stats.reconciliation_status_counts.items())
        ),
        "blocked_by_distribution": _top_counter_records(stats.blocked_by_counts),
        "reason_code_distribution": _top_counter_records(stats.reason_code_counts),
        "mode_distribution": dict(sorted(stats.mode_counts.items())),
        "strategy_version_distribution": dict(sorted(stats.strategy_version_counts.items())),
        "paper_signal_quality_status_distribution": dict(
            sorted(stats.signal_quality_status_counts.items())
        ),
        "confidence_bucket_performance": confidence_rows,
    }


def _serialize_confidence_buckets(buckets: dict[str, _BucketStats]) -> list[dict[str, Any]]:
    rows = []
    for bucket, stats in buckets.items():
        paper_pnl = stats.realized_pnl + stats.unrealized_pnl
        rows.append(
            {
                "bucket": bucket,
                "sample_count": len(stats.dates),
                "candidate_count": stats.candidate_count,
                "generated_intents": stats.generated_intents,
                "filled_count": stats.filled_count,
                "realized_pnl": stats.realized_pnl,
                "unrealized_pnl": stats.unrealized_pnl,
                "paper_pnl_total": paper_pnl,
                "avg_paper_pnl": _average_pnl(paper_pnl, stats.filled_count),
            }
        )
    return sorted(rows, key=lambda row: (-_int_value(row.get("candidate_count")), row["bucket"]))


def _impact_summary(
    *,
    profile_comparison: dict[str, dict[str, Any]],
    impact_status: str,
    gate: dict[str, Any],
    continuous_replay: dict[str, Any],
) -> dict[str, Any]:
    production = _mapping(profile_comparison.get("production"))
    shadow = _mapping(profile_comparison.get("shadow"))
    warnings = _strings(gate.get("warnings"))
    blocking = _strings(gate.get("blocking_reasons"))
    return {
        "impact_status": impact_status,
        "sample_counts": {
            "production": production.get("sample_count", 0),
            "shadow": shadow.get("sample_count", 0),
            "unknown": _mapping(profile_comparison.get("unknown")).get("sample_count", 0),
        },
        "filled_count": {
            "production": production.get("filled_count", 0),
            "shadow": shadow.get("filled_count", 0),
        },
        "paper_pnl_total": {
            "production": production.get("paper_pnl_total", 0.0),
            "shadow": shadow.get("paper_pnl_total", 0.0),
        },
        "main_blocked_by": blocking[0] if blocking else "none",
        "main_warning": warnings[0] if warnings else "none",
        "continuous_replay_available": continuous_replay.get("available", False),
        "continuous_replay_mode": continuous_replay.get("replay_mode", "daily_independent"),
    }


def _impact_gate(
    *,
    profile_comparison: dict[str, dict[str, Any]],
    continuous_replay: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any]:
    thresholds = _mapping(policy["thresholds"])
    production = _mapping(profile_comparison.get("production"))
    shadow = _mapping(profile_comparison.get("shadow"))
    checks = [
        _minimum_check(
            check_id="shadow_sample_count",
            observed=_int_value(shadow.get("sample_count")),
            threshold=_int_value(thresholds["minimum_shadow_sample_count"]),
            reason_code=REASON_INSUFFICIENT_SHADOW_SAMPLE,
        ),
        _minimum_check(
            check_id="production_sample_count",
            observed=_int_value(production.get("sample_count")),
            threshold=_int_value(thresholds["minimum_production_baseline_count"]),
            reason_code=REASON_INSUFFICIENT_PRODUCTION_BASELINE,
        ),
        _minimum_check(
            check_id="shadow_filled_count",
            observed=_int_value(shadow.get("filled_count")),
            threshold=_int_value(thresholds["minimum_filled_count_for_comparison"]),
            reason_code=REASON_INSUFFICIENT_SHADOW_SAMPLE,
            enabled=_int_value(shadow.get("generated_intents")) > 0,
        ),
        _maximum_check(
            check_id="shadow_synthetic_snapshot_ratio",
            observed=_float_value(shadow.get("synthetic_snapshot_ratio")),
            threshold=_float_value(thresholds["maximum_synthetic_snapshot_ratio"]),
            reason_code=REASON_SYNTHETIC_SNAPSHOT_RATIO_TOO_HIGH,
            enabled=_int_value(shadow.get("generated_intents")) > 0,
        ),
        _minimum_ratio_check(
            check_id="shadow_historical_ohlc_coverage",
            observed=_float_value(shadow.get("historical_ohlc_coverage")),
            threshold=_float_value(thresholds["minimum_historical_ohlc_coverage"]),
            reason_code=REASON_LOW_DATA_QUALITY,
            enabled=_int_value(shadow.get("generated_intents")) > 0,
        ),
        _minimum_ratio_check(
            check_id="shadow_reconciliation_pass_ratio",
            observed=_float_value(shadow.get("reconciliation_pass_ratio")),
            threshold=_float_value(thresholds["minimum_reconciliation_pass_ratio"]),
            reason_code=REASON_UNRELIABLE_RECONCILIATION,
            enabled=_int_value(shadow.get("summary_sample_count")) > 0,
        ),
    ]
    blocking_reasons = [
        str(check["reason_code"]) for check in checks if check.get("status") == "FAIL"
    ]
    if REASON_SYNTHETIC_SNAPSHOT_RATIO_TOO_HIGH in blocking_reasons:
        blocking_reasons.append(REASON_LOW_DATA_QUALITY)
    warnings = _continuous_replay_warning_codes(continuous_replay)
    status = _impact_status(
        production=production,
        shadow=shadow,
        blocking_reasons=blocking_reasons,
        continuous_replay=continuous_replay,
    )
    blocking_explanations = {
        reason: REASON_EXPLANATIONS.get(reason, "shadow impact 判断受限。")
        for reason in blocking_reasons
    }
    warning_explanations = {
        warning: REASON_EXPLANATIONS.get(warning, "shadow impact warning。") for warning in warnings
    }
    reason_explanations = {**blocking_explanations, **warning_explanations}
    return {
        "status": status,
        "blocked": bool(blocking_reasons),
        "blocked_by": list(dict.fromkeys(blocking_reasons)),
        "blocking_reasons": list(dict.fromkeys(blocking_reasons)),
        "warnings": warnings,
        "checks": checks,
        "blocking_reason_explanations": blocking_explanations,
        "warning_explanations": warning_explanations,
        "reason_explanations": reason_explanations,
        "explanation": _gate_explanation(status, blocking_reasons, warnings),
        "production_effect": "none",
        "scope": "shadow parameter impact observation only",
    }


def _impact_status(
    *,
    production: dict[str, Any],
    shadow: dict[str, Any],
    blocking_reasons: list[str],
    continuous_replay: dict[str, Any],
) -> str:
    reason_set = set(blocking_reasons)
    if reason_set & {
        REASON_INSUFFICIENT_SHADOW_SAMPLE,
        REASON_INSUFFICIENT_PRODUCTION_BASELINE,
    }:
        return STATUS_INSUFFICIENT_DATA
    if reason_set & {
        REASON_LOW_DATA_QUALITY,
        REASON_SYNTHETIC_SNAPSHOT_RATIO_TOO_HIGH,
    }:
        return STATUS_LOW_DATA_QUALITY
    if reason_set & {
        REASON_UNRELIABLE_RECONCILIATION,
    }:
        return STATUS_SHADOW_UNRELIABLE
    if not production.get("sample_count") or not shadow.get("sample_count"):
        return STATUS_OBSERVE_ONLY
    if _shadow_has_non_pnl_improvement(
        production=production,
        shadow=shadow,
        continuous_replay=continuous_replay,
    ):
        return STATUS_SHADOW_PROMISING_BUT_LIMITED
    return STATUS_NO_CLEAR_IMPROVEMENT


def _shadow_has_non_pnl_improvement(
    *,
    production: dict[str, Any],
    shadow: dict[str, Any],
    continuous_replay: dict[str, Any],
) -> bool:
    shadow_pnl = _float_value(shadow.get("paper_pnl_total"))
    production_pnl = _float_value(production.get("paper_pnl_total"))
    filled_not_worse = _int_value(shadow.get("filled_count")) >= _int_value(
        production.get("filled_count")
    )
    quality_not_worse = _float_value(shadow.get("synthetic_snapshot_ratio")) <= _float_value(
        production.get("synthetic_snapshot_ratio")
    )
    reconciliation_not_worse = _float_value(
        shadow.get("reconciliation_pass_ratio")
    ) >= _float_value(production.get("reconciliation_pass_ratio"))
    pnl_better = shadow_pnl > production_pnl
    replay_not_worse = True
    if continuous_replay.get("available"):
        prod_replay = _mapping(_mapping(continuous_replay.get("profiles")).get("production"))
        shadow_replay = _mapping(_mapping(continuous_replay.get("profiles")).get("shadow"))
        prod_drawdown = _optional_float(prod_replay.get("max_drawdown_pct"))
        shadow_drawdown = _optional_float(shadow_replay.get("max_drawdown_pct"))
        if prod_drawdown is not None and shadow_drawdown is not None:
            replay_not_worse = shadow_drawdown >= prod_drawdown
    return (
        pnl_better
        and filled_not_worse
        and quality_not_worse
        and reconciliation_not_worse
        and replay_not_worse
    )


def _continuous_replay_summary(
    replay_payload: dict[str, Any],
    profile_comparison: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if replay_payload.get("report_type") != "paper_trading_replay":
        return {
            "available": False,
            "path": "",
            "start": "",
            "end": "",
            "replay_mode": "daily_independent",
            "portfolio_carry_forward": False,
            "source_artifact": {
                "exists": False,
                "path": "",
                "mode": "missing",
                "date_range": {"start": "", "end": ""},
                "used_for_comparison": False,
            },
            "profiles": {
                profile: {"available": False, "final_equity": None, "max_drawdown_pct": None}
                for profile in SOURCE_PROFILES
            },
        }
    replay_mode = _string_value(replay_payload.get("replay_mode")) or "daily_independent"
    carry_forward = _bool_value(replay_payload.get("portfolio_carry_forward"))
    start = _string_value(replay_payload.get("start"))
    end = _string_value(replay_payload.get("end"))
    artifact_path = _replay_source_path(replay_payload)
    used_for_comparison = replay_mode == "continuous_portfolio" and carry_forward
    profile_results = _replay_profile_results(replay_payload) if used_for_comparison else {}
    return {
        "available": used_for_comparison,
        "path": artifact_path,
        "start": start,
        "end": end,
        "replay_mode": replay_mode,
        "portfolio_carry_forward": carry_forward,
        "continuous_metrics_available": bool(replay_payload.get("continuous_metrics_available")),
        "source_artifact": {
            "exists": True,
            "path": artifact_path,
            "mode": replay_mode,
            "date_range": {"start": start, "end": end},
            "used_for_comparison": used_for_comparison,
        },
        "profiles": {
            profile: profile_results.get(
                profile,
                {
                    "available": False,
                    "final_equity": None,
                    "max_drawdown_pct": None,
                    "filled_count": _mapping(profile_comparison.get(profile)).get(
                        "filled_count",
                        0,
                    ),
                },
            )
            for profile in SOURCE_PROFILES
        },
    }


def _replay_source_path(replay_payload: dict[str, Any]) -> str:
    return (
        _string_value(replay_payload.get("_source_artifact_path"))
        or _string_value(_mapping(replay_payload.get("outputs")).get("json"))
        or _string_value(replay_payload.get("path"))
    )


def _replay_profile_results(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_profiles = payload.get("profile_results")
    if not isinstance(raw_profiles, dict):
        raw_profiles = payload.get("profiles")
    results: dict[str, dict[str, Any]] = {}
    if isinstance(raw_profiles, dict):
        for raw_profile, raw_result in raw_profiles.items():
            profile = _normalize_profile(str(raw_profile))
            if profile not in SOURCE_PROFILES or not isinstance(raw_result, dict):
                continue
            results[profile] = {
                "available": True,
                "final_equity": raw_result.get("final_equity"),
                "max_drawdown_pct": _max_drawdown_pct(raw_result),
                "filled_count": raw_result.get("filled_count"),
            }
    if results:
        return results
    profile = _source_profile(payload)
    if profile == "unknown":
        profile = "unknown"
    return {
        profile: {
            "available": True,
            "final_equity": payload.get("final_equity"),
            "max_drawdown_pct": _max_drawdown_pct(payload),
            "filled_count": _mapping(payload.get("totals")).get("filled"),
        }
    }


def _max_drawdown_pct(payload: dict[str, Any]) -> float | None:
    value = _optional_float(payload.get("max_drawdown_pct"))
    if value is not None:
        return value
    return _optional_float(_mapping(payload.get("max_drawdown")).get("percent"))


def _profile_distributions(
    profile_comparison: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        profile: {
            "blocked_by": _records(summary.get("blocked_by_distribution")),
            "reason_code": _records(summary.get("reason_code_distribution")),
            "mode": _mapping(summary.get("mode_distribution")),
            "strategy_version": _mapping(summary.get("strategy_version_distribution")),
        }
        for profile, summary in profile_comparison.items()
    }


def _impact_warnings(
    replay_payload: dict[str, Any],
    replay_mode: dict[str, Any],
) -> list[dict[str, str]]:
    warnings = [
        {
            "code": "paper_only_simulation",
            "message": (
                "shadow impact evaluation 只解释 paper-only 模拟与只读报告；不是实盘收益、"
                "真实 broker 成交、完整税费/滑点模拟或上线依据。"
            ),
        }
    ]
    if not (
        replay_payload.get("report_type") == "paper_trading_replay"
        and replay_mode.get("replay_mode") == "continuous_portfolio"
        and replay_mode.get("portfolio_carry_forward") is True
    ):
        warnings.append(
            {
                "code": REASON_CONTINUOUS_REPLAY_MISSING,
                "message": REASON_EXPLANATIONS[REASON_CONTINUOUS_REPLAY_MISSING],
            }
        )
    if replay_payload.get("report_type") == "paper_trading_replay" and (
        replay_mode.get("replay_mode") != "continuous_portfolio"
        or replay_mode.get("portfolio_carry_forward") is not True
    ):
        warnings.append(
            {
                "code": REASON_DAILY_INDEPENDENT_ONLY,
                "message": REASON_EXPLANATIONS[REASON_DAILY_INDEPENDENT_ONLY],
            }
        )
    return warnings


def _continuous_replay_warning_codes(continuous_replay: dict[str, Any]) -> list[str]:
    if continuous_replay.get("available"):
        return []
    source = _mapping(continuous_replay.get("source_artifact"))
    warnings = [REASON_CONTINUOUS_REPLAY_MISSING]
    if source.get("exists"):
        warnings.append(REASON_DAILY_INDEPENDENT_ONLY)
    return warnings


def _gate_explanation(
    status: str,
    blocking_reasons: list[str],
    warnings: list[str],
) -> str:
    if blocking_reasons:
        explanations = [
            REASON_EXPLANATIONS.get(reason, "shadow impact 判断受限。")
            for reason in dict.fromkeys(blocking_reasons)
        ]
        return f"{status}：{'；'.join(explanations)}"
    if status == STATUS_SHADOW_PROMISING_BUT_LIMITED:
        return (
            "shadow 在 paper 观察中有非 PnL 维度支持，但仍是 observe-only，" "不能改变 production。"
        )
    if warnings:
        return (
            f"{status}：当前仅有 warning（{', '.join(warnings)}），"
            "仍需 continuous replay 和更长样本观察。"
        )
    return f"{status}：未发现清晰、可审计的 shadow 改善。"


def _minimum_check(
    *,
    check_id: str,
    observed: int,
    threshold: int,
    reason_code: str,
    enabled: bool = True,
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
    enabled: bool,
) -> dict[str, Any]:
    if not enabled:
        return {
            "check_id": check_id,
            "status": "SKIPPED",
            "observed": observed,
            "threshold": threshold,
            "operator": "<=",
            "reason_code": "",
        }
    return {
        "check_id": check_id,
        "status": "PASS" if observed <= threshold else "FAIL",
        "observed": observed,
        "threshold": threshold,
        "operator": "<=",
        "reason_code": "" if observed <= threshold else reason_code,
    }


def _source_profile(record: dict[str, Any], fallback: str = "unknown") -> str:
    direct_fields = (
        "source_profile",
        "parameter_profile",
        "profile",
        "parameter_profile_id",
        "source_profile_id",
    )
    for field_name in direct_fields:
        profile = _normalize_profile(_string_value(record.get(field_name)))
        if profile != "unknown":
            return profile
    metadata = _mapping(record.get("metadata"))
    for field_name in (*direct_fields, "mode"):
        profile = _normalize_profile(_string_value(metadata.get(field_name)))
        if profile != "unknown":
            return profile
    mode_profile = _normalize_profile(_string_value(record.get("mode")))
    if mode_profile in {"production", "shadow"}:
        return mode_profile
    for field_name in ("strategy_version", "strategy_id", "candidate_id", "run_id"):
        value = _string_value(record.get(field_name))
        profile = _profile_from_text(value)
        if profile != "unknown":
            return profile
    if (
        _mapping(record.get("source_decision"))
        or metadata.get("source") == "daily_decision_summary"
    ):
        return "production"
    return fallback


def _normalize_profile(value: str) -> str:
    normalized = value.strip().lower().replace("-", "_")
    if not normalized:
        return "unknown"
    if normalized in {"production", "prod", "current", "baseline", "production_current"}:
        return "production"
    if normalized in {"shadow", "shadow_candidate", "candidate_shadow", "validation_shadow"}:
        return "shadow"
    if normalized in {"unknown", "missing", "none"}:
        return "unknown"
    return _profile_from_text(normalized)


def _profile_from_text(value: str) -> str:
    lowered = value.strip().lower()
    if not lowered:
        return "unknown"
    if "shadow" in lowered:
        return "shadow"
    if "production" in lowered or "daily_decision_bus" in lowered:
        return "production"
    return "unknown"


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
    for source, raw_count in _mapping(payload.get("market_snapshot_source_counts")).items():
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


def _select_replay_payload(
    *,
    reports_dir: Path,
    as_of: date,
    replay_json_path: Path | None,
) -> dict[str, Any]:
    if replay_json_path is not None:
        return _with_replay_source_path(_read_json_object(replay_json_path), replay_json_path)
    candidates: list[tuple[date, datetime, str, Path, dict[str, Any]]] = []
    for path in reports_dir.glob("paper_trading_replay_*.json"):
        payload = _read_json_object(path)
        if payload.get("report_type") != "paper_trading_replay":
            continue
        end_date = _parse_iso_date(_string_value(payload.get("end")))
        if end_date is None or end_date > as_of:
            continue
        generated_at = _parse_iso_datetime(_string_value(payload.get("generated_at")))
        candidates.append(
            (
                end_date,
                generated_at,
                path.name,
                path,
                _with_replay_source_path(payload, path),
            )
        )
    if not candidates:
        return {}
    return max(candidates)[4]


def _with_replay_source_path(payload: dict[str, Any], path: Path) -> dict[str, Any]:
    if not payload:
        return {}
    marked = dict(payload)
    marked["_source_artifact_path"] = str(path)
    return marked


def _paper_evaluation_mode(replay_payload: dict[str, Any]) -> dict[str, Any]:
    replay_mode = _string_value(replay_payload.get("replay_mode")) or "daily_independent"
    portfolio_carry_forward = _bool_value(replay_payload.get("portfolio_carry_forward"))
    return {
        "replay_mode": replay_mode,
        "portfolio_carry_forward": portfolio_carry_forward,
        "continuous_portfolio_metrics_available": (
            replay_mode == "continuous_portfolio" and portfolio_carry_forward
        ),
    }


def _optional_replay_source(
    replay_json_path: Path | None,
    replay_payload: dict[str, Any],
) -> dict[str, Any]:
    if replay_json_path is None:
        return {
            "provided": False,
            "path": _replay_source_path(replay_payload),
            "exists": bool(replay_payload),
            "report_type": _string_value(replay_payload.get("report_type")),
            "start": _string_value(replay_payload.get("start")),
            "end": _string_value(replay_payload.get("end")),
            "mode": _string_value(replay_payload.get("replay_mode")),
        }
    return {
        "provided": True,
        "path": str(replay_json_path),
        "exists": replay_json_path.exists(),
        "report_type": _string_value(replay_payload.get("report_type")),
        "start": _string_value(replay_payload.get("start")),
        "end": _string_value(replay_payload.get("end")),
        "mode": _string_value(replay_payload.get("replay_mode")),
    }


def _artifact_record(*, path: Path, reports_dir: Path, exists: bool) -> dict[str, Any]:
    return {
        "path": str(path),
        "href": _report_href(path, reports_dir),
        "exists": exists,
    }


def _empty_profile_stats() -> dict[str, _ProfileStats]:
    return {profile: _ProfileStats() for profile in SOURCE_PROFILES}


def _candidates_by_id(candidates: tuple[dict[str, Any], ...]) -> dict[str, dict[str, Any]]:
    return {
        _string_value(candidate.get("candidate_id")): candidate
        for candidate in candidates
        if _string_value(candidate.get("candidate_id"))
    }


def _load_policy(path: Path) -> dict[str, Any]:
    try:
        raw = safe_load_yaml_path(path)
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"failed to read shadow parameter impact policy: {path}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"shadow parameter impact policy must be a YAML mapping: {path}")
    thresholds = raw.get("thresholds")
    if not isinstance(thresholds, dict):
        raise ValueError("shadow parameter impact policy missing thresholds")
    for key in (
        "minimum_shadow_sample_count",
        "minimum_production_baseline_count",
        "minimum_filled_count_for_comparison",
        "maximum_synthetic_snapshot_ratio",
        "minimum_historical_ohlc_coverage",
        "minimum_reconciliation_pass_ratio",
    ):
        if key not in thresholds:
            raise ValueError(f"shadow parameter impact policy missing threshold: {key}")
    return raw


def _policy_report(policy: dict[str, Any], policy_path: Path) -> dict[str, Any]:
    return {
        "policy_id": _string_value(policy.get("policy_id")) or "shadow_parameter_impact_policy",
        "version": policy.get("version"),
        "status": _string_value(policy.get("status")),
        "owner": _string_value(policy.get("owner")),
        "production_effect": _string_value(policy.get("production_effect")) or "none",
        "path": str(policy_path),
        "thresholds": dict(_mapping(policy.get("thresholds"))),
        "review_condition": _string_value(policy.get("review_condition")),
    }


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
        except ValueError:
            return path.as_posix()


def _top_counter_records(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    return [
        {"value": value, "count": count}
        for value, count in counter.most_common(limit)
        if value and count > 0
    ]


def _format_top_records(value: object) -> str:
    parts = []
    for record in _records(value):
        label = _string_value(record.get("value"))
        count = _optional_int(record.get("count"))
        if label and count is not None:
            parts.append(f"{label}:{count}")
    return "；".join(parts) or "none"


def _continuous_metric_pair(continuous: dict[str, Any], metric: str) -> str:
    profiles = _mapping(continuous.get("profiles"))
    production = _mapping(profiles.get("production")).get(metric)
    shadow = _mapping(profiles.get("shadow")).get(metric)
    if metric == "final_equity":
        return f"{_format_money_value(production)} / {_format_money_value(shadow)}"
    return (
        f"{_format_percent(_optional_float(production))} / "
        f"{_format_percent(_optional_float(shadow))}"
    )


def _format_money_value(value: object) -> str:
    number = _optional_float(value)
    if number is None:
        return "missing"
    return f"{number:.2f}"


def _format_check_value(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _format_percent(value: float | None) -> str:
    if value is None:
        return "missing"
    return f"{value:.2%}"


def _average_pnl(total: float, count: int) -> float:
    return total / count if count else 0.0


def _records(value: object) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _strings(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str) and value:
        return [value]
    return []


def _string_value(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _int_value(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _float_value(value: object, default: float = 0.0) -> float:
    optional = _optional_float(value)
    return default if optional is None else optional


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _optional_float(value: object) -> float | None:
    if isinstance(value, bool):
        return float(value)
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _bool_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def _parse_iso_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_iso_datetime(value: str) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
