from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.fundamentals.sec_pit_baseline_coverage import (
    DEFAULT_SEC_PIT_BASELINE_COVERAGE_OUTPUT_DIR,
)
from ai_trading_system.fundamentals.sec_pit_shadow_observe import (
    ACTIVE_SHADOW_CONFIG_PATHS,
    DEFAULT_SEC_PIT_RESEARCH_BASELINE_SCORE_PATH,
    DEFAULT_SEC_PIT_SHADOW_OBSERVE_OUTPUT_DIR,
    PRODUCTION_CONFIG_PATHS,
)

SEC_PIT_SHADOW_MONITOR_TASK_ID = "TRADING-046"
SEC_PIT_SHADOW_MONITOR_STATE_POLICY_TASK_ID = "TRADING-046A"
SEC_PIT_SHADOW_MONITOR_REPORT_TYPE = "sec_pit_shadow_monitor"
SEC_PIT_SHADOW_MONITOR_PRODUCTION_EFFECT = "none"
DEFAULT_SEC_PIT_SHADOW_MONITOR_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "sec_pit_shadow_monitor"

MONITOR_STATUSES: tuple[str, ...] = (
    "INSUFFICIENT_MONITORING_SAMPLE",
    "MONITORING_ACTIVE",
    "OK_MONITORING",
    "WARNING",
    "ROLLBACK_RECOMMENDED",
    "FAILED_VALIDATION",
)

ROLLING_METRICS_COLUMNS: tuple[str, ...] = (
    "decision_date",
    "rolling_rank_ic_20d",
    "rolling_rank_ic_60d",
    "rolling_hit_rate_20d",
    "rolling_relative_return_vs_baseline_20d",
    "rolling_drawdown_improvement_20d",
    "semiconductor_bucket_rank_ic_20d",
    "platform_bucket_rank_ic_20d",
    "monitoring_sample_count",
    "monitoring_days_elapsed",
    "monitoring_days_remaining",
    "manual_review_required",
    "production_effect",
)

WARNING_EVENTS_COLUMNS: tuple[str, ...] = (
    "event_date",
    "event_type",
    "severity",
    "metric",
    "observed_value",
    "threshold",
    "coverage_gate_passed",
    "monitoring_ready",
    "rollback_recommended",
    "reason",
    "manual_review_required",
    "production_effect",
)

SHADOW_SCORE_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "decision_date",
        "ticker",
        "bucket",
        "baseline_score",
        "sec_pit_shadow_component",
        "baseline_rank",
        "sec_pit_observe_rank",
        "forward_return_20d",
        "forward_return_60d",
        "max_drawdown_forward_20d",
    }
)

MONITORING_PLAN_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "monitoring_metric",
        "minimum_monitoring_days",
        "preferred_monitoring_days",
        "warning_threshold",
        "rollback_threshold",
        "production_effect",
    }
)

# TRADING-046 pilot baseline: rollback recommendation needs one RankIC breach and one
# outcome breach after coverage/sample gates pass, so a single noisy rolling metric cannot
# become an investment-facing rollback recommendation.
FACTOR_ROLLBACK_REQUIRES_RANK_AND_OUTCOME_BREACH = True

# TRADING-046A state policy: rollback confirmation requires both RankIC direction
# and outcome evidence to be observable; bucket metrics remain diagnostics.
ROLLBACK_REQUIRED_ROLLING_METRICS: tuple[str, ...] = (
    "rolling_rank_ic_20d",
    "rolling_rank_ic_60d",
    "rolling_relative_return_vs_baseline_20d",
    "rolling_drawdown_improvement_20d",
)


@dataclass(frozen=True)
class SecPitShadowMonitorArtifacts:
    status: str
    summary_json_path: Path
    summary_markdown_path: Path
    rolling_metrics_path: Path
    warning_events_path: Path

    @property
    def json_path(self) -> Path:
        return self.summary_json_path

    @property
    def markdown_path(self) -> Path:
        return self.summary_markdown_path


@dataclass(frozen=True)
class _MonitorInputs:
    monitor_date: date
    shadow_summary_path: Path
    shadow_scores_path: Path
    bucket_comparison_path: Path
    monitoring_plan_path: Path
    baseline_coverage_summary_path: Path
    baseline_score_path: Path
    shadow_summary: dict[str, Any]
    shadow_scores: pd.DataFrame
    bucket_comparison: pd.DataFrame
    monitoring_plan: pd.DataFrame
    baseline_coverage_summary: dict[str, Any]
    baseline_scores: pd.DataFrame


@dataclass(frozen=True)
class _MonitorPolicy:
    minimum_monitoring_days: int
    preferred_monitoring_days: int
    min_monitoring_sample_count: int
    rank_ic_warning_threshold: float
    rank_ic_rollback_threshold: float
    relative_return_warning_threshold: float
    relative_return_rollback_threshold: float
    drawdown_warning_threshold: float
    drawdown_rollback_threshold: float


def run_sec_pit_shadow_monitor(
    *,
    as_of: date | None = None,
    shadow_observe_dir: Path = DEFAULT_SEC_PIT_SHADOW_OBSERVE_OUTPUT_DIR,
    baseline_coverage_dir: Path = DEFAULT_SEC_PIT_BASELINE_COVERAGE_OUTPUT_DIR,
    baseline_score_path: Path = DEFAULT_SEC_PIT_RESEARCH_BASELINE_SCORE_PATH,
    window_days: tuple[int, ...] = (20, 60),
    output_dir: Path = DEFAULT_SEC_PIT_SHADOW_MONITOR_OUTPUT_DIR,
    latest: bool = False,
) -> SecPitShadowMonitorArtifacts:
    del latest
    output_dir.mkdir(parents=True, exist_ok=True)
    monitor_date = as_of or _discover_monitor_date(shadow_observe_dir)
    suffix = monitor_date.isoformat()
    summary_json_path = output_dir / f"sec_pit_shadow_monitor_summary_{suffix}.json"
    summary_markdown_path = output_dir / f"sec_pit_shadow_monitor_summary_{suffix}.md"
    rolling_metrics_path = output_dir / f"sec_pit_shadow_rolling_metrics_{suffix}.csv"
    warning_events_path = output_dir / f"sec_pit_shadow_warning_events_{suffix}.csv"
    limitations: list[str] = []

    try:
        inputs = _load_inputs(
            shadow_observe_dir=shadow_observe_dir,
            baseline_coverage_dir=baseline_coverage_dir,
            baseline_score_path=baseline_score_path,
            monitor_date=monitor_date,
            require_exact_shadow_observe=as_of is not None,
        )
        _validate_inputs(inputs)
        policy = _monitor_policy(inputs)
        rolling_metrics = _rolling_metrics(inputs.shadow_scores, policy, window_days)
        latest_metrics = _latest_metrics(rolling_metrics)
        coverage_gate_passed = _coverage_gate_passed(inputs)
        minimum_evidence_achieved = _minimum_evidence_achieved(latest_metrics, policy)
        monitoring_ready = coverage_gate_passed and minimum_evidence_achieved
        rolling_metrics_available = _rolling_metrics_available(latest_metrics)
        warning_events = _warning_events(
            latest_metrics=latest_metrics,
            policy=policy,
            coverage_gate_passed=coverage_gate_passed,
            monitoring_ready=monitoring_ready,
            rollback_gate_passed=(
                coverage_gate_passed and minimum_evidence_achieved and rolling_metrics_available
            ),
        )
        rollback_recommended = _rollback_recommended(
            warning_events,
            coverage_gate_passed=coverage_gate_passed,
            minimum_evidence_achieved=minimum_evidence_achieved,
            rolling_metrics_available=rolling_metrics_available,
        )
        status = _monitor_status(
            coverage_gate_passed=coverage_gate_passed,
            minimum_evidence_achieved=minimum_evidence_achieved,
            warning_events=warning_events,
            rollback_recommended=rollback_recommended,
        )
        monitor_maturity = _monitor_maturity(
            coverage_gate_passed=coverage_gate_passed,
            minimum_evidence_achieved=minimum_evidence_achieved,
        )
        state_transition_reason = _state_transition_reason(
            status=status,
            coverage_gate_passed=coverage_gate_passed,
            minimum_evidence_achieved=minimum_evidence_achieved,
            rolling_metrics_available=rolling_metrics_available,
            warning_events=warning_events,
            rollback_recommended=rollback_recommended,
        )
        if not coverage_gate_passed:
            limitations.append("Baseline coverage gate did not pass; rollback is blocked.")
        if coverage_gate_passed and not minimum_evidence_achieved:
            limitations.append(
                "Monitoring sample or observation-day evidence is still accumulating."
            )
        if coverage_gate_passed and minimum_evidence_achieved and not rolling_metrics_available:
            limitations.append(
                "Rolling metrics are not fully available; rollback remains blocked until "
                "RankIC and outcome metrics are observable."
            )
    except (KeyError, ValueError, pd.errors.ParserError) as exc:
        inputs = _failed_inputs(
            shadow_observe_dir=shadow_observe_dir,
            baseline_coverage_dir=baseline_coverage_dir,
            baseline_score_path=baseline_score_path,
            monitor_date=monitor_date,
            require_exact_shadow_observe=as_of is not None,
        )
        policy = _default_policy(inputs)
        rolling_metrics = _empty_frame(ROLLING_METRICS_COLUMNS)
        warning_events = _empty_frame(WARNING_EVENTS_COLUMNS)
        latest_metrics = {}
        coverage_gate_passed = False
        minimum_evidence_achieved = False
        monitoring_ready = False
        rolling_metrics_available = False
        rollback_recommended = False
        status = "FAILED_VALIDATION"
        monitor_maturity = "VALIDATION_FAILED"
        state_transition_reason = "输入 artifact validation failed，monitor 状态不可用。"
        limitations.append(f"Input artifact validation failed: {exc}")

    _write_csv(rolling_metrics, rolling_metrics_path, ROLLING_METRICS_COLUMNS)
    _write_csv(warning_events, warning_events_path, WARNING_EVENTS_COLUMNS)
    summary = _summary_payload(
        status=status,
        inputs=inputs,
        policy=policy,
        latest_metrics=latest_metrics,
        coverage_gate_passed=coverage_gate_passed,
        minimum_evidence_achieved=minimum_evidence_achieved,
        monitoring_ready=monitoring_ready,
        rolling_metrics_available=rolling_metrics_available,
        monitor_maturity=monitor_maturity,
        state_transition_reason=state_transition_reason,
        warning_events=warning_events,
        rollback_recommended=rollback_recommended,
        limitations=limitations,
        artifacts={
            "summary_json": summary_json_path,
            "summary_markdown": summary_markdown_path,
            "rolling_metrics_csv": rolling_metrics_path,
            "warning_events_csv": warning_events_path,
        },
    )
    summary_json_path.write_text(
        json.dumps(_json_value(summary), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_markdown_path.write_text(
        render_sec_pit_shadow_monitor_summary(
            summary=summary,
            rolling_metrics=rolling_metrics,
            warning_events=warning_events,
        ),
        encoding="utf-8",
    )
    return SecPitShadowMonitorArtifacts(
        status=status,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        rolling_metrics_path=rolling_metrics_path,
        warning_events_path=warning_events_path,
    )


def render_sec_pit_shadow_monitor_summary(
    *,
    summary: dict[str, Any],
    rolling_metrics: pd.DataFrame,
    warning_events: pd.DataFrame,
) -> str:
    latest_row = rolling_metrics.tail(1).to_dict(orient="records")
    latest = latest_row[0] if latest_row else {}
    lines = [
        "# SEC PIT Shadow Observe Rolling Monitor",
        "",
        "## Metadata",
        f"- generated_at: {summary.get('generated_at', '')}",
        f"- monitor_date: {summary.get('monitor_date', '')}",
        f"- candidate feature: {summary.get('candidate_feature', '')}",
        f"- observe weight: {_format_decimal(summary.get('observe_weight'))}",
        f"- production_effect: {summary.get('production_effect', 'none')}",
        f"- manual_review_required: {summary.get('manual_review_required', True)}",
        "",
        "## 状态摘要",
        f"- monitor_status: {summary.get('monitor_status', '')}",
        f"- monitor_maturity: {summary.get('monitor_maturity', '')}",
        f"- coverage_gate_passed: {summary.get('coverage_gate_passed', False)}",
        f"- minimum_evidence_achieved: {summary.get('minimum_evidence_achieved', False)}",
        f"- monitoring_ready: {summary.get('monitoring_ready', False)}",
        f"- rolling_metrics_available: {summary.get('rolling_metrics_available', False)}",
        f"- rollback_recommended: {summary.get('rollback_recommended', False)}",
        f"- warning_count: {summary.get('warning_count', 0)}",
        f"- state_transition_reason: {summary.get('state_transition_reason', '')}",
        f"- key limitations: {_limitations_text(summary.get('limitations'))}",
        "",
        "## Rolling Metrics",
        "| metric | latest value |",
        "|---|---:|",
        f"| rolling_rank_ic_20d | {_format_decimal(latest.get('rolling_rank_ic_20d'))} |",
        f"| rolling_rank_ic_60d | {_format_decimal(latest.get('rolling_rank_ic_60d'))} |",
        f"| rolling_hit_rate_20d | {_format_decimal(latest.get('rolling_hit_rate_20d'))} |",
        (
            "| rolling_relative_return_vs_baseline_20d | "
            f"{_format_decimal(latest.get('rolling_relative_return_vs_baseline_20d'))} |"
        ),
        (
            "| rolling_drawdown_improvement_20d | "
            f"{_format_decimal(latest.get('rolling_drawdown_improvement_20d'))} |"
        ),
        (
            "| semiconductor_bucket_rank_ic_20d | "
            f"{_format_decimal(latest.get('semiconductor_bucket_rank_ic_20d'))} |"
        ),
        (
            "| platform_bucket_rank_ic_20d | "
            f"{_format_decimal(latest.get('platform_bucket_rank_ic_20d'))} |"
        ),
        f"| monitoring_sample_count | {summary.get('monitoring_sample_count', 0)} |",
        f"| monitoring_days_elapsed | {summary.get('monitoring_days_elapsed', 0)} |",
        f"| monitoring_days_remaining | {summary.get('monitoring_days_remaining', 0)} |",
        "",
        "## Warning Events",
        *_warning_event_lines(warning_events),
        "",
        "## 审计边界",
        "- 本报告只读读取既有 TRADING-044/045 artifacts。",
        (
            "- 不修改 production weights、active shadow weights、production scoring config "
            "或交易动作。"
        ),
        "- rollback recommendation 仍要求人工复核，且 `production_effect=none`。",
        "",
    ]
    return "\n".join(lines)


def _load_inputs(
    *,
    shadow_observe_dir: Path,
    baseline_coverage_dir: Path,
    baseline_score_path: Path,
    monitor_date: date,
    require_exact_shadow_observe: bool = False,
) -> _MonitorInputs:
    shadow_summary_path = _shadow_observe_path(
        shadow_observe_dir,
        "sec_pit_shadow_observe_summary_",
        ".json",
        monitor_date,
        exact=require_exact_shadow_observe,
    )
    shadow_summary = _read_json_object(shadow_summary_path)
    shadow_scores_path = _artifact_path(
        shadow_summary,
        "shadow_scores_csv",
        _shadow_observe_path(
            shadow_observe_dir,
            "sec_pit_shadow_scores_",
            ".csv",
            monitor_date,
            exact=require_exact_shadow_observe,
        ),
    )
    bucket_comparison_path = _artifact_path(
        shadow_summary,
        "bucket_comparison_csv",
        _shadow_observe_path(
            shadow_observe_dir,
            "sec_pit_shadow_bucket_comparison_",
            ".csv",
            monitor_date,
            exact=require_exact_shadow_observe,
        ),
    )
    monitoring_plan_path = _artifact_path(
        shadow_summary,
        "monitoring_plan_csv",
        _shadow_observe_path(
            shadow_observe_dir,
            "sec_pit_shadow_monitoring_plan_",
            ".csv",
            monitor_date,
            exact=require_exact_shadow_observe,
        ),
    )
    baseline_coverage_summary_path = _latest_dated_path(
        baseline_coverage_dir,
        "sec_pit_baseline_coverage_summary_",
        ".json",
        monitor_date,
    )
    return _MonitorInputs(
        monitor_date=monitor_date,
        shadow_summary_path=shadow_summary_path,
        shadow_scores_path=shadow_scores_path,
        bucket_comparison_path=bucket_comparison_path,
        monitoring_plan_path=monitoring_plan_path,
        baseline_coverage_summary_path=baseline_coverage_summary_path,
        baseline_score_path=baseline_score_path,
        shadow_summary=shadow_summary,
        shadow_scores=_read_csv_or_empty(shadow_scores_path),
        bucket_comparison=_read_csv_or_empty(bucket_comparison_path),
        monitoring_plan=_read_csv_or_empty(monitoring_plan_path),
        baseline_coverage_summary=_read_json_object(baseline_coverage_summary_path),
        baseline_scores=_read_csv_or_empty(baseline_score_path),
    )


def _failed_inputs(
    *,
    shadow_observe_dir: Path,
    baseline_coverage_dir: Path,
    baseline_score_path: Path,
    monitor_date: date,
    require_exact_shadow_observe: bool = False,
) -> _MonitorInputs:
    return _MonitorInputs(
        monitor_date=monitor_date,
        shadow_summary_path=_shadow_observe_path(
            shadow_observe_dir,
            "sec_pit_shadow_observe_summary_",
            ".json",
            monitor_date,
            exact=require_exact_shadow_observe,
        ),
        shadow_scores_path=_shadow_observe_path(
            shadow_observe_dir,
            "sec_pit_shadow_scores_",
            ".csv",
            monitor_date,
            exact=require_exact_shadow_observe,
        ),
        bucket_comparison_path=_shadow_observe_path(
            shadow_observe_dir,
            "sec_pit_shadow_bucket_comparison_",
            ".csv",
            monitor_date,
            exact=require_exact_shadow_observe,
        ),
        monitoring_plan_path=_shadow_observe_path(
            shadow_observe_dir,
            "sec_pit_shadow_monitoring_plan_",
            ".csv",
            monitor_date,
            exact=require_exact_shadow_observe,
        ),
        baseline_coverage_summary_path=_latest_dated_path(
            baseline_coverage_dir,
            "sec_pit_baseline_coverage_summary_",
            ".json",
            monitor_date,
        ),
        baseline_score_path=baseline_score_path,
        shadow_summary={},
        shadow_scores=_empty_frame(tuple(SHADOW_SCORE_REQUIRED_COLUMNS)),
        bucket_comparison=pd.DataFrame(),
        monitoring_plan=_empty_frame(tuple(MONITORING_PLAN_REQUIRED_COLUMNS)),
        baseline_coverage_summary={},
        baseline_scores=pd.DataFrame(),
    )


def _validate_inputs(inputs: _MonitorInputs) -> None:
    if inputs.shadow_summary.get("report_type") != "sec_pit_shadow_observe":
        raise ValueError(f"shadow observe summary missing or invalid: {inputs.shadow_summary_path}")
    if inputs.baseline_coverage_summary.get("report_type") != "sec_pit_baseline_coverage":
        raise ValueError(
            f"baseline coverage summary missing or invalid: "
            f"{inputs.baseline_coverage_summary_path}"
        )
    if not inputs.shadow_scores_path.exists():
        raise ValueError(f"shadow scores CSV not found: {inputs.shadow_scores_path}")
    if not inputs.monitoring_plan_path.exists():
        raise ValueError(f"monitoring plan CSV not found: {inputs.monitoring_plan_path}")
    if not inputs.baseline_score_path.exists():
        raise ValueError(f"research baseline score CSV not found: {inputs.baseline_score_path}")
    _require_columns(inputs.shadow_scores, SHADOW_SCORE_REQUIRED_COLUMNS, "shadow scores")
    _require_columns(inputs.monitoring_plan, MONITORING_PLAN_REQUIRED_COLUMNS, "monitoring plan")
    if inputs.baseline_scores.empty:
        raise ValueError(f"research baseline score CSV has no rows: {inputs.baseline_score_path}")
    if str(inputs.shadow_summary.get("production_effect") or "") != "none":
        raise ValueError("shadow observe summary production_effect must be none")
    if inputs.shadow_summary.get("manual_review_required") is not True:
        raise ValueError("shadow observe summary manual_review_required must be true")


def _rolling_metrics(
    scores: pd.DataFrame,
    policy: _MonitorPolicy,
    window_days: tuple[int, ...],
) -> pd.DataFrame:
    frame = _normalize_scores(scores)
    if frame.empty:
        return _empty_frame(ROLLING_METRICS_COLUMNS)
    unique_dates = sorted(frame["decision_date"].dropna().unique().tolist())
    records: list[dict[str, Any]] = []
    windows = sorted({int(value) for value in window_days if int(value) > 0})
    if not windows:
        windows = [20, 60]
    for index, current_date in enumerate(unique_dates):
        record: dict[str, Any] = {
            "decision_date": _date_text(current_date),
            "manual_review_required": True,
            "production_effect": SEC_PIT_SHADOW_MONITOR_PRODUCTION_EFFECT,
        }
        if 20 in windows:
            frame_20 = _window_frame(frame, unique_dates, index, 20)
            record.update(_window_20_metrics(frame_20))
        if 60 in windows:
            frame_60 = _window_frame(frame, unique_dates, index, 60)
            record["rolling_rank_ic_60d"] = _rank_correlation(
                frame_60["sec_pit_shadow_component"],
                frame_60["forward_return_60d"],
            )
        cumulative = frame.loc[frame["decision_date"].isin(unique_dates[: index + 1])]
        record["monitoring_sample_count"] = _monitoring_sample_count(cumulative)
        elapsed = index + 1
        record["monitoring_days_elapsed"] = elapsed
        record["monitoring_days_remaining"] = max(policy.minimum_monitoring_days - elapsed, 0)
        for column in ROLLING_METRICS_COLUMNS:
            if column not in record:
                record[column] = np.nan
        records.append(record)
    return pd.DataFrame(records, columns=list(ROLLING_METRICS_COLUMNS))


def _window_20_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rolling_rank_ic_20d": _rank_correlation(
            frame["sec_pit_shadow_component"],
            frame["forward_return_20d"],
        ),
        "rolling_hit_rate_20d": _hit_rate(frame["forward_return_20d"]),
        "rolling_relative_return_vs_baseline_20d": _top_rank_label_delta(
            frame,
            "forward_return_20d",
        ),
        "rolling_drawdown_improvement_20d": _top_rank_label_delta(
            frame,
            "max_drawdown_forward_20d",
        ),
        "semiconductor_bucket_rank_ic_20d": _bucket_rank_ic(frame, "semiconductor"),
        "platform_bucket_rank_ic_20d": _bucket_rank_ic(frame, "platform"),
    }


def _warning_events(
    *,
    latest_metrics: dict[str, Any],
    policy: _MonitorPolicy,
    coverage_gate_passed: bool,
    monitoring_ready: bool,
    rollback_gate_passed: bool,
) -> pd.DataFrame:
    if not latest_metrics:
        return _empty_frame(WARNING_EVENTS_COLUMNS)
    event_date = str(latest_metrics.get("decision_date") or "")
    events: list[dict[str, Any]] = []
    _append_threshold_event(
        events,
        event_date=event_date,
        metric="rolling_rank_ic_20d",
        value=latest_metrics.get("rolling_rank_ic_20d"),
        warning_threshold=policy.rank_ic_warning_threshold,
        rollback_threshold=policy.rank_ic_rollback_threshold,
        coverage_gate_passed=coverage_gate_passed,
        monitoring_ready=monitoring_ready,
        rollback_gate_passed=rollback_gate_passed,
        reason="20D rolling RankIC no longer supports the observe component direction.",
    )
    _append_threshold_event(
        events,
        event_date=event_date,
        metric="rolling_rank_ic_60d",
        value=latest_metrics.get("rolling_rank_ic_60d"),
        warning_threshold=policy.rank_ic_warning_threshold,
        rollback_threshold=policy.rank_ic_rollback_threshold,
        coverage_gate_passed=coverage_gate_passed,
        monitoring_ready=monitoring_ready,
        rollback_gate_passed=rollback_gate_passed,
        reason="60D rolling RankIC no longer supports the observe component direction.",
    )
    _append_threshold_event(
        events,
        event_date=event_date,
        metric="rolling_relative_return_vs_baseline_20d",
        value=latest_metrics.get("rolling_relative_return_vs_baseline_20d"),
        warning_threshold=policy.relative_return_warning_threshold,
        rollback_threshold=policy.relative_return_rollback_threshold,
        coverage_gate_passed=coverage_gate_passed,
        monitoring_ready=monitoring_ready,
        rollback_gate_passed=rollback_gate_passed,
        reason="Top observe-ranked return is weaker than the baseline top-ranked return.",
    )
    _append_threshold_event(
        events,
        event_date=event_date,
        metric="rolling_drawdown_improvement_20d",
        value=latest_metrics.get("rolling_drawdown_improvement_20d"),
        warning_threshold=policy.drawdown_warning_threshold,
        rollback_threshold=policy.drawdown_rollback_threshold,
        coverage_gate_passed=coverage_gate_passed,
        monitoring_ready=monitoring_ready,
        rollback_gate_passed=rollback_gate_passed,
        reason="Top observe-ranked drawdown is worse than the baseline top-ranked drawdown.",
    )
    semis = _float_or_nan(latest_metrics.get("semiconductor_bucket_rank_ic_20d"))
    platform = _float_or_nan(latest_metrics.get("platform_bucket_rank_ic_20d"))
    if pd.notna(semis) and pd.notna(platform) and semis < platform:
        events.append(
            _warning_event(
                event_date=event_date,
                event_type="BUCKET_WARNING",
                severity="WARNING",
                metric="semiconductor_bucket_rank_ic_20d",
                observed_value=semis,
                threshold=f">= platform_bucket_rank_ic_20d ({platform:.6f})",
                coverage_gate_passed=coverage_gate_passed,
                monitoring_ready=monitoring_ready,
                rollback_recommended=False,
                reason="Semiconductor bucket RankIC is below platform bucket RankIC.",
            )
        )
    if not events:
        return _empty_frame(WARNING_EVENTS_COLUMNS)
    frame = pd.DataFrame(events, columns=list(WARNING_EVENTS_COLUMNS))
    if not _factor_underperformance_confirmed(frame):
        frame["rollback_recommended"] = False
    return frame


def _append_threshold_event(
    events: list[dict[str, Any]],
    *,
    event_date: str,
    metric: str,
    value: object,
    warning_threshold: float,
    rollback_threshold: float,
    coverage_gate_passed: bool,
    monitoring_ready: bool,
    rollback_gate_passed: bool,
    reason: str,
) -> None:
    number = _float_or_nan(value)
    if pd.isna(number):
        return
    if number < rollback_threshold:
        events.append(
            _warning_event(
                event_date=event_date,
                event_type="ROLLBACK_CONDITION",
                severity="ROLLBACK_CONDITION",
                metric=metric,
                observed_value=number,
                threshold=f"< {rollback_threshold:.6f}",
                coverage_gate_passed=coverage_gate_passed,
                monitoring_ready=monitoring_ready,
                rollback_recommended=rollback_gate_passed,
                reason=reason,
            )
        )
        return
    if number < warning_threshold:
        events.append(
            _warning_event(
                event_date=event_date,
                event_type="WARNING",
                severity="WARNING",
                metric=metric,
                observed_value=number,
                threshold=f">= {warning_threshold:.6f}",
                coverage_gate_passed=coverage_gate_passed,
                monitoring_ready=monitoring_ready,
                rollback_recommended=False,
                reason=reason,
            )
        )


def _warning_event(
    *,
    event_date: str,
    event_type: str,
    severity: str,
    metric: str,
    observed_value: float,
    threshold: str,
    coverage_gate_passed: bool,
    monitoring_ready: bool,
    rollback_recommended: bool,
    reason: str,
) -> dict[str, Any]:
    return {
        "event_date": event_date,
        "event_type": event_type,
        "severity": severity,
        "metric": metric,
        "observed_value": observed_value,
        "threshold": threshold,
        "coverage_gate_passed": coverage_gate_passed,
        "monitoring_ready": monitoring_ready,
        "rollback_recommended": rollback_recommended,
        "reason": reason,
        "manual_review_required": True,
        "production_effect": SEC_PIT_SHADOW_MONITOR_PRODUCTION_EFFECT,
    }


def _summary_payload(
    *,
    status: str,
    inputs: _MonitorInputs,
    policy: _MonitorPolicy,
    latest_metrics: dict[str, Any],
    coverage_gate_passed: bool,
    minimum_evidence_achieved: bool,
    monitoring_ready: bool,
    rolling_metrics_available: bool,
    monitor_maturity: str,
    state_transition_reason: str,
    warning_events: pd.DataFrame,
    rollback_recommended: bool,
    limitations: list[str],
    artifacts: dict[str, Path],
) -> dict[str, Any]:
    safety = {
        "manual_review_required": True,
        "production_effect": SEC_PIT_SHADOW_MONITOR_PRODUCTION_EFFECT,
        "production_weights_modified": False,
        "active_shadow_weights_modified": False,
        "observe_only": True,
        "read_only": True,
    }
    return {
        "schema_version": "1.1",
        "report_type": SEC_PIT_SHADOW_MONITOR_REPORT_TYPE,
        "task_id": SEC_PIT_SHADOW_MONITOR_TASK_ID,
        "state_policy_task_id": SEC_PIT_SHADOW_MONITOR_STATE_POLICY_TASK_ID,
        "generated_at": _deterministic_generated_at(inputs.monitor_date),
        "monitor_date": inputs.monitor_date.isoformat(),
        "monitor_status": status,
        "monitor_maturity": monitor_maturity,
        "state_transition_reason": state_transition_reason,
        "candidate_feature": str(inputs.shadow_summary.get("candidate_feature") or ""),
        "observe_weight": _json_number(inputs.shadow_summary.get("observe_weight")),
        "production_effect": SEC_PIT_SHADOW_MONITOR_PRODUCTION_EFFECT,
        "manual_review_required": True,
        "shadow_observe_status": str(inputs.shadow_summary.get("shadow_status") or "MISSING"),
        "upstream_monitoring_status": str(
            inputs.shadow_summary.get("monitoring_status") or "MISSING"
        ),
        "coverage_status": str(
            inputs.baseline_coverage_summary.get("coverage_status") or "MISSING"
        ),
        "coverage_ratio": _json_number(inputs.baseline_coverage_summary.get("coverage_ratio")),
        "coverage_gate_passed": coverage_gate_passed,
        "minimum_evidence_achieved": minimum_evidence_achieved,
        "monitoring_ready": monitoring_ready,
        "rolling_metrics_available": rolling_metrics_available,
        "minimum_monitoring_days": policy.minimum_monitoring_days,
        "preferred_monitoring_days": policy.preferred_monitoring_days,
        "min_monitoring_sample_count": policy.min_monitoring_sample_count,
        "monitoring_sample_count": _int_metric(latest_metrics, "monitoring_sample_count"),
        "monitoring_days_elapsed": _int_metric(latest_metrics, "monitoring_days_elapsed"),
        "monitoring_days_remaining": _int_metric(latest_metrics, "monitoring_days_remaining"),
        "rolling_rank_ic_20d": _json_number(latest_metrics.get("rolling_rank_ic_20d")),
        "rolling_rank_ic_60d": _json_number(latest_metrics.get("rolling_rank_ic_60d")),
        "rolling_hit_rate_20d": _json_number(latest_metrics.get("rolling_hit_rate_20d")),
        "rolling_relative_return_vs_baseline_20d": _json_number(
            latest_metrics.get("rolling_relative_return_vs_baseline_20d")
        ),
        "rolling_drawdown_improvement_20d": _json_number(
            latest_metrics.get("rolling_drawdown_improvement_20d")
        ),
        "semiconductor_bucket_rank_ic_20d": _json_number(
            latest_metrics.get("semiconductor_bucket_rank_ic_20d")
        ),
        "platform_bucket_rank_ic_20d": _json_number(
            latest_metrics.get("platform_bucket_rank_ic_20d")
        ),
        "warning_count": int(len(warning_events)),
        "rollback_recommended": rollback_recommended,
        "factor_underperformance_confirmed": _factor_underperformance_confirmed(warning_events),
        "limitations": _dedupe_text(limitations),
        "safety": safety,
        "input_artifacts": {
            "shadow_summary": str(inputs.shadow_summary_path),
            "shadow_scores": str(inputs.shadow_scores_path),
            "bucket_comparison": str(inputs.bucket_comparison_path),
            "monitoring_plan": str(inputs.monitoring_plan_path),
            "baseline_coverage_summary": str(inputs.baseline_coverage_summary_path),
            "baseline_score": str(inputs.baseline_score_path),
        },
        "input_checksums": {
            "shadow_summary_sha256": _file_sha256(inputs.shadow_summary_path),
            "shadow_scores_sha256": _file_sha256(inputs.shadow_scores_path),
            "monitoring_plan_sha256": _file_sha256(inputs.monitoring_plan_path),
            "baseline_coverage_summary_sha256": _file_sha256(inputs.baseline_coverage_summary_path),
            "baseline_score_sha256": _file_sha256(inputs.baseline_score_path),
            "production_config_sha256": _file_hashes(PRODUCTION_CONFIG_PATHS),
            "active_shadow_config_sha256": _file_hashes(ACTIVE_SHADOW_CONFIG_PATHS),
        },
        "output_artifacts": {key: str(value) for key, value in artifacts.items()},
    }


def _monitor_policy(inputs: _MonitorInputs) -> _MonitorPolicy:
    plan = inputs.monitoring_plan
    first = plan.iloc[0].to_dict() if not plan.empty else {}
    rank_row = _plan_row(plan, "rolling_rank_ic_20d")
    relative_row = _plan_row(plan, "relative_return_vs_baseline_20d")
    drawdown_row = _plan_row(plan, "drawdown_improvement_20d")
    return _MonitorPolicy(
        minimum_monitoring_days=max(_int_or_default(first.get("minimum_monitoring_days"), 60), 1),
        preferred_monitoring_days=max(
            _int_or_default(first.get("preferred_monitoring_days"), 90),
            1,
        ),
        min_monitoring_sample_count=max(
            _int_or_default(inputs.shadow_summary.get("min_monitoring_sample_count"), 20),
            1,
        ),
        rank_ic_warning_threshold=_threshold_value(
            rank_row.get("warning_threshold"),
            default=0.02,
        ),
        rank_ic_rollback_threshold=_threshold_value(
            rank_row.get("rollback_threshold"),
            default=-0.02,
        ),
        relative_return_warning_threshold=_threshold_value(
            relative_row.get("warning_threshold"),
            default=0.0,
        ),
        relative_return_rollback_threshold=_threshold_value(
            relative_row.get("rollback_threshold"),
            default=-0.05,
        ),
        drawdown_warning_threshold=_threshold_value(
            drawdown_row.get("warning_threshold"),
            default=0.0,
        ),
        drawdown_rollback_threshold=_threshold_value(
            drawdown_row.get("rollback_threshold"),
            default=-0.03,
        ),
    )


def _default_policy(inputs: _MonitorInputs) -> _MonitorPolicy:
    return _MonitorPolicy(
        minimum_monitoring_days=60,
        preferred_monitoring_days=90,
        min_monitoring_sample_count=max(
            _int_or_default(inputs.shadow_summary.get("min_monitoring_sample_count"), 20),
            1,
        ),
        rank_ic_warning_threshold=0.02,
        rank_ic_rollback_threshold=-0.02,
        relative_return_warning_threshold=0.0,
        relative_return_rollback_threshold=-0.05,
        drawdown_warning_threshold=0.0,
        drawdown_rollback_threshold=-0.03,
    )


def _monitor_status(
    *,
    coverage_gate_passed: bool,
    minimum_evidence_achieved: bool,
    warning_events: pd.DataFrame,
    rollback_recommended: bool,
) -> str:
    if not coverage_gate_passed:
        return "FAILED_VALIDATION"
    if rollback_recommended:
        return "ROLLBACK_RECOMMENDED"
    if minimum_evidence_achieved and not warning_events.empty:
        return "WARNING"
    if minimum_evidence_achieved:
        return "OK_MONITORING"
    return "MONITORING_ACTIVE"


def _minimum_evidence_achieved(latest_metrics: dict[str, Any], policy: _MonitorPolicy) -> bool:
    if not latest_metrics:
        return False
    if _int_metric(latest_metrics, "monitoring_sample_count") < policy.min_monitoring_sample_count:
        return False
    if _int_metric(latest_metrics, "monitoring_days_elapsed") < policy.minimum_monitoring_days:
        return False
    return True


def _rolling_metrics_available(latest_metrics: dict[str, Any]) -> bool:
    if not latest_metrics:
        return False
    for metric in ROLLBACK_REQUIRED_ROLLING_METRICS:
        if pd.isna(_float_or_nan(latest_metrics.get(metric))):
            return False
    return True


def _rollback_recommended(
    warning_events: pd.DataFrame,
    *,
    coverage_gate_passed: bool,
    minimum_evidence_achieved: bool,
    rolling_metrics_available: bool,
) -> bool:
    if not coverage_gate_passed or not minimum_evidence_achieved or not rolling_metrics_available:
        return False
    return _factor_underperformance_confirmed(warning_events)


def _monitor_maturity(
    *,
    coverage_gate_passed: bool,
    minimum_evidence_achieved: bool,
) -> str:
    if not coverage_gate_passed:
        return "COVERAGE_GATE_BLOCKED"
    if minimum_evidence_achieved:
        return "MINIMUM_EVIDENCE_ACHIEVED"
    return "ACCUMULATING_EVIDENCE"


def _state_transition_reason(
    *,
    status: str,
    coverage_gate_passed: bool,
    minimum_evidence_achieved: bool,
    rolling_metrics_available: bool,
    warning_events: pd.DataFrame,
    rollback_recommended: bool,
) -> str:
    if not coverage_gate_passed:
        return "coverage gate 未通过，monitor validation failed，rollback 被阻断。"
    if rollback_recommended or status == "ROLLBACK_RECOMMENDED":
        return (
            "coverage、minimum evidence 与 rolling metrics gates 均通过，且 factor "
            "deterioration 已由 RankIC + outcome 双重条件确认。"
        )
    if minimum_evidence_achieved and not warning_events.empty:
        return "minimum evidence 已达到，但 warning event 存在，需要人工复核。"
    if minimum_evidence_achieved and rolling_metrics_available:
        return "minimum evidence 与 rolling metrics 均可用，且无 warning 或 rollback。"
    if minimum_evidence_achieved:
        return (
            "minimum evidence 已达到且无 warning 或 rollback；rolling metrics 暂不完整，"
            "只阻断 rollback，不再视为 monitoring sample 不足。"
        )
    return "coverage gate 已通过，monitor 仍在积累 minimum sample / observation-day evidence。"


def _factor_underperformance_confirmed(warning_events: pd.DataFrame) -> bool:
    if warning_events.empty:
        return False
    rollback_rows = warning_events.loc[
        warning_events["severity"].astype(str) == "ROLLBACK_CONDITION"
    ]
    if rollback_rows.empty:
        return False
    if not FACTOR_ROLLBACK_REQUIRES_RANK_AND_OUTCOME_BREACH:
        return not rollback_rows.empty
    metrics = set(rollback_rows["metric"].astype(str))
    rank_breach = bool(metrics & {"rolling_rank_ic_20d", "rolling_rank_ic_60d"})
    outcome_breach = bool(
        metrics
        & {
            "rolling_relative_return_vs_baseline_20d",
            "rolling_drawdown_improvement_20d",
        }
    )
    return rank_breach and outcome_breach


def _coverage_gate_passed(inputs: _MonitorInputs) -> bool:
    coverage_status = str(inputs.baseline_coverage_summary.get("coverage_status") or "")
    shadow_baseline_status = str(inputs.shadow_summary.get("baseline_coverage_status") or "")
    if coverage_status != "OK":
        return False
    if shadow_baseline_status and shadow_baseline_status != "OK":
        return False
    return True


def _normalize_scores(scores: pd.DataFrame) -> pd.DataFrame:
    frame = scores.copy()
    frame["decision_date"] = pd.to_datetime(frame["decision_date"], errors="coerce").dt.date
    for column in (
        "baseline_score",
        "sec_pit_shadow_component",
        "baseline_rank",
        "sec_pit_observe_rank",
        "forward_return_20d",
        "forward_return_60d",
        "max_drawdown_forward_20d",
    ):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.loc[frame["decision_date"].notna()].copy()
    return frame.sort_values(["decision_date", "ticker"]).reset_index(drop=True)


def _window_frame(
    frame: pd.DataFrame,
    unique_dates: list[date],
    current_index: int,
    window: int,
) -> pd.DataFrame:
    start_index = max(0, current_index - window + 1)
    window_dates = set(unique_dates[start_index : current_index + 1])
    return frame.loc[frame["decision_date"].isin(window_dates)].copy()


def _monitoring_sample_count(frame: pd.DataFrame) -> int:
    baseline = pd.to_numeric(frame["baseline_score"], errors="coerce").notna()
    labels = pd.to_numeric(frame["forward_return_20d"], errors="coerce").notna()
    return int((baseline & labels).sum())


def _bucket_rank_ic(frame: pd.DataFrame, bucket: str) -> float:
    rows = frame.loc[frame["bucket"].astype(str) == bucket]
    if rows.empty:
        return np.nan
    return _rank_correlation(rows["sec_pit_shadow_component"], rows["forward_return_20d"])


def _top_rank_label_delta(scores: pd.DataFrame, label_column: str) -> float:
    if scores.empty or label_column not in scores.columns:
        return np.nan
    deltas: list[float] = []
    for _, group in scores.groupby("decision_date", sort=True):
        label_values = _numeric_array(group[label_column])
        baseline_rank = _numeric_array(group["baseline_rank"])
        observe_rank = _numeric_array(group["sec_pit_observe_rank"])
        valid_label = ~np.isnan(label_values)
        if not valid_label.any():
            continue
        baseline_mask = valid_label & ~np.isnan(baseline_rank)
        observe_mask = valid_label & ~np.isnan(observe_rank)
        if not baseline_mask.any() or not observe_mask.any():
            continue
        baseline_indices = np.flatnonzero(baseline_mask)
        observe_indices = np.flatnonzero(observe_mask)
        baseline_index = baseline_indices[int(np.argmin(baseline_rank[baseline_indices]))]
        observe_index = observe_indices[int(np.argmin(observe_rank[observe_indices]))]
        baseline_label = _float_or_nan(label_values[baseline_index])
        observe_label = _float_or_nan(label_values[observe_index])
        if pd.notna(baseline_label) and pd.notna(observe_label):
            deltas.append(float(observe_label - baseline_label))
    return float(np.mean(deltas)) if deltas else np.nan


def _numeric_array(values: object) -> np.ndarray:
    return pd.to_numeric(values, errors="coerce").to_numpy(dtype=float, na_value=np.nan)


def _rank_correlation(left: object, right: object) -> float:
    frame = pd.DataFrame(
        {
            "left": pd.to_numeric(left, errors="coerce"),
            "right": pd.to_numeric(right, errors="coerce"),
        }
    ).dropna()
    if len(frame) < 2 or frame["left"].nunique() < 2 or frame["right"].nunique() < 2:
        return np.nan
    return float(frame["left"].rank(method="average").corr(frame["right"].rank(method="average")))


def _hit_rate(value: object) -> float:
    values = pd.to_numeric(value, errors="coerce").dropna()
    return float((values > 0).mean()) if not values.empty else np.nan


def _latest_metrics(rolling_metrics: pd.DataFrame) -> dict[str, Any]:
    if rolling_metrics.empty:
        return {}
    return rolling_metrics.iloc[-1].to_dict()


def _discover_monitor_date(shadow_observe_dir: Path) -> date:
    summary = _latest_dated_path(
        shadow_observe_dir,
        "sec_pit_shadow_observe_summary_",
        ".json",
        None,
    )
    parsed = _date_from_prefixed_path(summary, "sec_pit_shadow_observe_summary_")
    if parsed is not None:
        return parsed
    scores = _latest_dated_path(shadow_observe_dir, "sec_pit_shadow_scores_", ".csv", None)
    parsed = _date_from_prefixed_path(scores, "sec_pit_shadow_scores_")
    return parsed or date(1970, 1, 1)


def _shadow_observe_path(
    root: Path,
    prefix: str,
    suffix: str,
    monitor_date: date,
    *,
    exact: bool,
) -> Path:
    if exact:
        return root / f"{prefix}{monitor_date.isoformat()}{suffix}"
    return _latest_dated_path(root, prefix, suffix, monitor_date)


def _latest_dated_path(
    root: Path,
    prefix: str,
    suffix: str,
    end: date | None,
) -> Path:
    default_end = end or date(1970, 1, 1)
    default_path = root / f"{prefix}{default_end.isoformat()}{suffix}"
    if not root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"{prefix}*{suffix}"):
        parsed = _date_from_prefixed_path(path, prefix)
        if parsed is not None and (end is None or parsed <= end):
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _date_from_prefixed_path(path: Path, prefix: str) -> date | None:
    raw_date = path.stem.removeprefix(prefix)
    return _parse_date(raw_date)


def _artifact_path(summary: dict[str, Any], key: str, default: Path) -> Path:
    outputs = summary.get("output_artifacts") if isinstance(summary, dict) else {}
    raw = outputs.get(key) if isinstance(outputs, dict) else None
    if not raw:
        return default
    path = Path(str(raw))
    return path if path.is_absolute() else PROJECT_ROOT / path


def _plan_row(plan: pd.DataFrame, metric: str) -> dict[str, Any]:
    if plan.empty or "monitoring_metric" not in plan.columns:
        return {}
    rows = plan.loc[plan["monitoring_metric"].astype(str) == metric]
    if rows.empty:
        return {}
    return rows.iloc[0].to_dict()


def _threshold_value(value: object, *, default: float) -> float:
    if value is None:
        return default
    text = str(value).strip()
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if match is None:
        return default
    try:
        return float(match.group(0))
    except ValueError:
        return default


def _require_columns(frame: pd.DataFrame, required: frozenset[str], label: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{label} missing required columns: {', '.join(missing)}")


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists() or not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path).fillna("")


def _write_csv(frame: pd.DataFrame, path: Path, columns: tuple[str, ...]) -> None:
    output = frame.copy() if not frame.empty else _empty_frame(columns)
    for column in columns:
        if column not in output.columns:
            output[column] = ""
    path.parent.mkdir(parents=True, exist_ok=True)
    output.loc[:, list(columns)].to_csv(path, index=False)


def _empty_frame(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _file_hashes(paths: tuple[Path, ...]) -> dict[str, str]:
    return {str(path): _file_sha256(path) for path in paths}


def _file_sha256(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _warning_event_lines(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["- none"]
    lines: list[str] = []
    for row in frame.to_dict(orient="records"):
        lines.append(
            "- "
            f"`{row.get('metric', '')}` severity={row.get('severity', '')}; "
            f"value={_format_decimal(row.get('observed_value'))}; "
            f"threshold={row.get('threshold', '')}; "
            f"rollback={row.get('rollback_recommended', '')}"
        )
    return lines


def _int_metric(values: dict[str, Any], key: str) -> int:
    return _int_or_default(values.get(key), 0)


def _int_or_default(value: object, default: int) -> int:
    try:
        if isinstance(value, str) and not value.strip():
            return default
        if pd.isna(value):
            return default
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _float_or_nan(value: object) -> float:
    try:
        if isinstance(value, str) and not value.strip():
            return np.nan
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return np.nan


def _json_number(value: object) -> float | None:
    number = _float_or_nan(value)
    return None if pd.isna(number) else float(number)


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if np.isnan(value) else float(value)
    if isinstance(value, float) and np.isnan(value):
        return None
    return value


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _date_text(value: object) -> str:
    if isinstance(value, date):
        return value.isoformat()
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.date().isoformat()


def _deterministic_generated_at(value: date) -> str:
    return datetime(value.year, value.month, value.day, tzinfo=UTC).isoformat()


def _dedupe_text(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if text and text not in seen:
            result.append(text)
            seen.add(text)
    return result


def _limitations_text(value: object) -> str:
    if not isinstance(value, list) or not value:
        return "none"
    return "; ".join(str(item) for item in value[:5])


def _format_decimal(value: object, *, digits: int = 4) -> str:
    number = _json_number(value)
    return "NA" if number is None else f"{number:.{digits}f}"
