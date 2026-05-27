from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.fundamentals.sec_pit_aliases import (
    canonicalize_ticker_series,
    load_ticker_aliases,
)
from ai_trading_system.fundamentals.sec_pit_backfill import SEC_PIT_BACKTEST_DATA_GRADE
from ai_trading_system.fundamentals.sec_pit_baseline_comparison import (
    DEFAULT_BASELINE_SCORE_DIR,
    DEFAULT_PROCESSED_BASELINE_SCORE_PATH,
    DEFAULT_SEC_PIT_BASELINE_COMPARISON_OUTPUT_DIR,
    DEFAULT_SEC_PIT_EVALUATION_DIR,
)
from ai_trading_system.fundamentals.sec_pit_candidate_review import (
    DEFAULT_SEC_PIT_CANDIDATE_REVIEW_OUTPUT_DIR,
)
from ai_trading_system.fundamentals.sec_pit_evaluation import (
    DEFAULT_SEC_PIT_FEATURE_PANEL_PATH,
)
from ai_trading_system.fundamentals.sec_pit_real_run_diagnostics import (
    DEFAULT_SEC_PIT_DIAGNOSTICS_OUTPUT_DIR,
)

SEC_PIT_SHADOW_OBSERVE_TASK_ID = "TRADING-044"
SEC_PIT_SHADOW_OBSERVE_REPORT_TYPE = "sec_pit_shadow_observe"
SEC_PIT_SHADOW_OBSERVE_PRODUCTION_EFFECT = "none"
DEFAULT_SEC_PIT_SHADOW_OBSERVE_CONFIG_PATH = PROJECT_ROOT / "config" / "sec_pit_shadow_observe.yaml"
DEFAULT_SEC_PIT_SHADOW_OBSERVE_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "sec_pit_shadow_observe"
LEGACY_SEC_PIT_FEATURE_PANEL_PATH = (
    PROJECT_ROOT / "data" / "processed" / "sec_pit" / "sec_pit_feature_panel.csv"
)

SHADOW_STATUSES: tuple[str, ...] = (
    "OK",
    "LIMITED_BASELINE_MISSING",
    "LIMITED_LABELS_MISSING",
    "INSUFFICIENT_MONITORING_SAMPLE",
    "LIMITED_CANDIDATE_REVIEW_MISSING",
    "FAILED_SAFETY_CHECK",
    "FAILED_VALIDATION",
)

SHADOW_SCORE_COLUMNS: tuple[str, ...] = (
    "decision_date",
    "ticker",
    "bucket",
    "baseline_score",
    "feature_id",
    "feature_value",
    "normalized_feature_value",
    "observe_weight",
    "sec_pit_shadow_component",
    "sec_pit_observe_score",
    "baseline_rank",
    "sec_pit_observe_rank",
    "rank_delta",
    "forward_return_20d",
    "forward_return_60d",
    "relative_return_vs_QQQ_20d",
    "max_drawdown_forward_20d",
    "pit_grade",
    "available_time",
    "accession_number",
    "source_lineage",
    "manual_review_required",
    "production_effect",
)

RANK_SHIFT_COLUMNS: tuple[str, ...] = (
    "decision_date",
    "ticker",
    "bucket",
    "baseline_rank",
    "sec_pit_observe_rank",
    "rank_delta",
    "abs_rank_delta",
    "baseline_score",
    "sec_pit_observe_score",
    "score_delta",
    "feature_id",
    "feature_value",
    "normalized_feature_value",
    "observe_weight",
    "rank_shift_reason",
    "manual_review_required",
    "production_effect",
)

BUCKET_COMPARISON_COLUMNS: tuple[str, ...] = (
    "bucket",
    "sample_count",
    "avg_baseline_score",
    "avg_sec_pit_observe_score",
    "avg_score_delta",
    "avg_rank_delta",
    "avg_forward_return_20d",
    "avg_forward_return_60d",
    "avg_relative_return_vs_QQQ_20d",
    "avg_max_drawdown_forward_20d",
    "hit_rate_20d",
    "rank_ic_20d",
    "rank_ic_60d",
    "drawdown_improvement_20d",
    "interpretation",
)

MONITORING_PLAN_COLUMNS: tuple[str, ...] = (
    "lane_id",
    "feature_id",
    "observe_weight",
    "start_date",
    "minimum_monitoring_days",
    "preferred_monitoring_days",
    "monitoring_metric",
    "target_direction",
    "warning_threshold",
    "rollback_threshold",
    "current_value",
    "status",
    "manual_review_required",
    "production_effect",
)

SAFETY_AUDIT_COLUMNS: tuple[str, ...] = (
    "check_name",
    "status",
    "expected",
    "actual",
    "severity",
    "recommended_action",
)

MONITORING_METRICS: tuple[str, ...] = (
    "rolling_rank_ic_20d",
    "rolling_rank_ic_60d",
    "relative_return_vs_baseline_20d",
    "drawdown_improvement_20d",
    "hit_rate_20d",
    "data_quality_score",
    "provenance_complete",
    "baseline_overlap",
)

FACTOR_MONITORING_METRICS: frozenset[str] = frozenset(
    {
        "rolling_rank_ic_20d",
        "rolling_rank_ic_60d",
        "relative_return_vs_baseline_20d",
        "drawdown_improvement_20d",
        "hit_rate_20d",
    }
)

PRODUCTION_CONFIG_PATHS: tuple[Path, ...] = (
    PROJECT_ROOT / "config" / "weights" / "weight_profile_current.yaml",
)
ACTIVE_SHADOW_CONFIG_PATHS: tuple[Path, ...] = (
    PROJECT_ROOT / "config" / "weights" / "shadow_weight_profiles.yaml",
)


@dataclass(frozen=True)
class SecPitShadowObserveCandidate:
    feature_id: str
    metric_id: str
    approval_status: str
    observe_weight: float
    max_allowed_initial_weight: float
    weight_direction: str
    pit_grade_policy: str
    minimum_monitoring_days: int
    preferred_monitoring_days: int
    enabled: bool


@dataclass(frozen=True)
class SecPitShadowObserveConfig:
    lane_id: str
    lane_status: str
    production_effect: str
    manual_review_required: bool
    candidates: tuple[SecPitShadowObserveCandidate, ...]
    safety: dict[str, Any]
    monitoring: dict[str, Any]
    rollback: dict[str, Any]
    monitoring_quality_gate: dict[str, Any]


@dataclass(frozen=True)
class SecPitShadowObserveArtifacts:
    status: str
    summary_json_path: Path
    summary_markdown_path: Path
    shadow_scores_path: Path
    rank_shift_path: Path
    bucket_comparison_path: Path
    monitoring_plan_path: Path
    safety_audit_path: Path

    @property
    def json_path(self) -> Path:
        return self.summary_json_path

    @property
    def markdown_path(self) -> Path:
        return self.summary_markdown_path


@dataclass(frozen=True)
class _BaselineInputs:
    path: Path
    frame: pd.DataFrame
    status: str

    @property
    def exists(self) -> bool:
        return not self.frame.empty


@dataclass(frozen=True)
class _ShadowObserveInputs:
    candidate_review_summary_path: Path
    candidate_evidence_path: Path
    shadow_proposal_path: Path
    baseline_overlap_path: Path
    evaluation_summary_path: Path
    feature_effectiveness_path: Path
    signal_attribution_path: Path
    comparison_summary_path: Path
    decision_impact_path: Path
    diagnostics_summary_path: Path
    label_coverage_path: Path
    feature_panel_path: Path
    baseline: _BaselineInputs
    candidate_review_summary: dict[str, Any]
    candidate_evidence: pd.DataFrame
    shadow_proposal: pd.DataFrame
    baseline_overlap: pd.DataFrame
    evaluation_summary: dict[str, Any]
    feature_effectiveness: pd.DataFrame
    signal_attribution: pd.DataFrame
    comparison_summary: dict[str, Any]
    decision_impact: pd.DataFrame
    diagnostics_summary: dict[str, Any]
    label_coverage: pd.DataFrame

    @property
    def candidate_review_exists(self) -> bool:
        return self.candidate_review_summary.get("report_type") == "sec_pit_candidate_review"


def load_sec_pit_shadow_observe_config(
    path: Path | str = DEFAULT_SEC_PIT_SHADOW_OBSERVE_CONFIG_PATH,
) -> SecPitShadowObserveConfig:
    raw_path = Path(path)
    with raw_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}
    if not isinstance(raw, dict):
        raise ValueError("sec_pit_shadow_observe config must be a mapping")
    raw_candidates = raw.get("candidates") or []
    if not isinstance(raw_candidates, list):
        raise ValueError("sec_pit_shadow_observe.candidates must be a list")
    candidates = tuple(_candidate_from_mapping(item) for item in raw_candidates)
    config = SecPitShadowObserveConfig(
        lane_id=str(raw.get("lane_id") or "sec_pit_capex_intensity_observe_only"),
        lane_status=str(raw.get("lane_status") or "observe_only"),
        production_effect=str(raw.get("production_effect") or "none"),
        manual_review_required=_bool_value(raw.get("manual_review_required"), default=True),
        candidates=candidates,
        safety=_mapping_value(raw.get("safety")),
        monitoring=_mapping_value(raw.get("monitoring")),
        rollback=_mapping_value(raw.get("rollback")),
        monitoring_quality_gate=_mapping_value(raw.get("monitoring_quality_gate")),
    )
    _validate_config(config)
    return config


def run_sec_pit_shadow_observe(
    *,
    start: date | None = None,
    end: date | None = None,
    candidate_review_dir: Path = DEFAULT_SEC_PIT_CANDIDATE_REVIEW_OUTPUT_DIR,
    evaluation_dir: Path = DEFAULT_SEC_PIT_EVALUATION_DIR,
    comparison_dir: Path = DEFAULT_SEC_PIT_BASELINE_COMPARISON_OUTPUT_DIR,
    diagnostics_dir: Path = DEFAULT_SEC_PIT_DIAGNOSTICS_OUTPUT_DIR,
    feature_panel_path: Path = DEFAULT_SEC_PIT_FEATURE_PANEL_PATH,
    baseline_score_path: Path | None = None,
    baseline_score_dir: Path = DEFAULT_BASELINE_SCORE_DIR,
    candidate_feature: str = "capex_intensity",
    observe_weight: float | None = None,
    max_allowed_weight: float | None = None,
    output_dir: Path = DEFAULT_SEC_PIT_SHADOW_OBSERVE_OUTPUT_DIR,
    config_path: Path = DEFAULT_SEC_PIT_SHADOW_OBSERVE_CONFIG_PATH,
    latest: bool = False,
) -> SecPitShadowObserveArtifacts:
    output_dir.mkdir(parents=True, exist_ok=True)
    config = load_sec_pit_shadow_observe_config(config_path)
    candidate = _candidate_config(config, candidate_feature)
    resolved_observe_weight = (
        float(observe_weight) if observe_weight is not None else candidate.observe_weight
    )
    resolved_max_allowed = (
        abs(float(max_allowed_weight))
        if max_allowed_weight is not None
        else abs(candidate.max_allowed_initial_weight)
    )

    inputs = _load_inputs(
        candidate_review_dir=candidate_review_dir,
        evaluation_dir=evaluation_dir,
        comparison_dir=comparison_dir,
        diagnostics_dir=diagnostics_dir,
        feature_panel_path=feature_panel_path,
        baseline_score_path=baseline_score_path,
        baseline_score_dir=baseline_score_dir,
        end=end,
        latest=latest,
    )
    inferred_start = (
        _summary_date(inputs.candidate_review_summary, "start_date")
        or _summary_date(inputs.evaluation_summary, "start_date")
        or _summary_date(inputs.comparison_summary, "start_date")
        or _summary_date(inputs.diagnostics_summary, "start_date")
    )
    inferred_end = (
        _summary_date(inputs.candidate_review_summary, "end_date")
        or _summary_date(inputs.evaluation_summary, "end_date")
        or _summary_date(inputs.comparison_summary, "end_date")
        or _summary_date(inputs.diagnostics_summary, "end_date")
    )
    if latest:
        start = start or inferred_start
        end = end or inferred_end
    if start is None or end is None:
        raise ValueError("start and end are required unless --latest can infer them")
    if start > end:
        raise ValueError("start must be on or before end")

    suffix = end.isoformat()
    summary_json_path = output_dir / f"sec_pit_shadow_observe_summary_{suffix}.json"
    summary_markdown_path = output_dir / f"sec_pit_shadow_observe_summary_{suffix}.md"
    shadow_scores_path = output_dir / f"sec_pit_shadow_scores_{suffix}.csv"
    rank_shift_path = output_dir / f"sec_pit_shadow_rank_shift_{suffix}.csv"
    bucket_comparison_path = output_dir / f"sec_pit_shadow_bucket_comparison_{suffix}.csv"
    monitoring_plan_path = output_dir / f"sec_pit_shadow_monitoring_plan_{suffix}.csv"
    safety_audit_path = output_dir / f"sec_pit_shadow_safety_audit_{suffix}.csv"

    production_hashes_before = _file_hashes(PRODUCTION_CONFIG_PATHS)
    active_shadow_hashes_before = _file_hashes(ACTIVE_SHADOW_CONFIG_PATHS)
    safety = _safety_audit(
        inputs=inputs,
        config=config,
        candidate=candidate,
        candidate_feature=candidate_feature,
        observe_weight=resolved_observe_weight,
        max_allowed_weight=resolved_max_allowed,
        output_dir=output_dir,
        production_hashes_before=production_hashes_before,
        active_shadow_hashes_before=active_shadow_hashes_before,
    )
    critical_failed = _critical_safety_failed(safety)
    candidate_missing = not inputs.candidate_review_exists
    limitations = _base_limitations(inputs, config, candidate_feature)

    if critical_failed:
        status = "FAILED_SAFETY_CHECK"
        shadow_scores = _empty_frame(SHADOW_SCORE_COLUMNS)
        rank_shift = _empty_frame(RANK_SHIFT_COLUMNS)
        bucket_comparison = _empty_frame(BUCKET_COMPARISON_COLUMNS)
        monitoring_plan = _empty_frame(MONITORING_PLAN_COLUMNS)
        _write_csv(safety, safety_audit_path, SAFETY_AUDIT_COLUMNS)
    elif candidate_missing:
        status = "LIMITED_CANDIDATE_REVIEW_MISSING"
        limitations.append("Candidate review artifact is missing; no observe scores were emitted.")
        shadow_scores = _empty_frame(SHADOW_SCORE_COLUMNS)
        rank_shift = _empty_frame(RANK_SHIFT_COLUMNS)
        bucket_comparison = _empty_bucket_comparison(config)
        monitoring_plan = _monitoring_plan(
            config=config,
            candidate=candidate,
            start=start,
            observe_weight=resolved_observe_weight,
            scores=shadow_scores,
            bucket_comparison=bucket_comparison,
            candidate_review=_candidate_review_values(inputs, candidate_feature),
        )
        _write_csv(shadow_scores, shadow_scores_path, SHADOW_SCORE_COLUMNS)
        _write_csv(rank_shift, rank_shift_path, RANK_SHIFT_COLUMNS)
        _write_csv(bucket_comparison, bucket_comparison_path, BUCKET_COMPARISON_COLUMNS)
        _write_csv(monitoring_plan, monitoring_plan_path, MONITORING_PLAN_COLUMNS)
        _write_csv(safety, safety_audit_path, SAFETY_AUDIT_COLUMNS)
    else:
        shadow_scores = _shadow_scores(
            inputs=inputs,
            config=config,
            candidate_feature=candidate_feature,
            observe_weight=resolved_observe_weight,
            start=start,
            end=end,
        )
        monitoring_quality = _monitoring_quality(shadow_scores, config)
        if (
            not inputs.baseline.exists
            or _baseline_missing(shadow_scores)
            or monitoring_quality.status == "LIMITED_BASELINE_MISSING"
        ):
            status = "LIMITED_BASELINE_MISSING"
            limitations.append(
                "Baseline score is missing for part or all of the requested shadow rows; "
                "rank shift fields are limited."
            )
        elif (
            _labels_missing(shadow_scores) or monitoring_quality.status == "LIMITED_LABELS_MISSING"
        ):
            status = "LIMITED_LABELS_MISSING"
            limitations.append("Forward return labels are missing or incomplete.")
        else:
            status = "OK"
        rank_shift = _rank_shift(shadow_scores)
        bucket_comparison = _bucket_comparison(shadow_scores, config=config)
        monitoring_plan = _monitoring_plan(
            config=config,
            candidate=candidate,
            start=start,
            observe_weight=resolved_observe_weight,
            scores=shadow_scores,
            bucket_comparison=bucket_comparison,
            candidate_review=_candidate_review_values(inputs, candidate_feature),
        )
        _write_csv(shadow_scores, shadow_scores_path, SHADOW_SCORE_COLUMNS)
        _write_csv(rank_shift, rank_shift_path, RANK_SHIFT_COLUMNS)
        _write_csv(bucket_comparison, bucket_comparison_path, BUCKET_COMPARISON_COLUMNS)
        _write_csv(monitoring_plan, monitoring_plan_path, MONITORING_PLAN_COLUMNS)
        _write_csv(safety, safety_audit_path, SAFETY_AUDIT_COLUMNS)

    safety_counts = _safety_counts(safety)
    summary = _summary_payload(
        status=status,
        start=start,
        end=end,
        config=config,
        candidate=candidate,
        candidate_feature=candidate_feature,
        observe_weight=resolved_observe_weight,
        max_allowed_weight=resolved_max_allowed,
        inputs=inputs,
        shadow_scores=shadow_scores,
        rank_shift=rank_shift,
        bucket_comparison=bucket_comparison,
        monitoring_plan=monitoring_plan,
        safety=safety,
        safety_counts=safety_counts,
        limitations=limitations,
        artifacts={
            "summary_json": summary_json_path,
            "summary_markdown": summary_markdown_path,
            "shadow_scores_csv": shadow_scores_path,
            "rank_shift_csv": rank_shift_path,
            "bucket_comparison_csv": bucket_comparison_path,
            "monitoring_plan_csv": monitoring_plan_path,
            "safety_audit_csv": safety_audit_path,
        },
    )
    summary_json_path.write_text(
        json.dumps(_json_value(summary), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_markdown_path.write_text(
        render_sec_pit_shadow_observe_summary(
            summary=summary,
            rank_shift=rank_shift,
            bucket_comparison=bucket_comparison,
            safety=safety,
            monitoring_plan=monitoring_plan,
        ),
        encoding="utf-8",
    )
    return SecPitShadowObserveArtifacts(
        status=status,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        shadow_scores_path=shadow_scores_path,
        rank_shift_path=rank_shift_path,
        bucket_comparison_path=bucket_comparison_path,
        monitoring_plan_path=monitoring_plan_path,
        safety_audit_path=safety_audit_path,
    )


def render_sec_pit_shadow_observe_summary(
    *,
    summary: dict[str, Any],
    rank_shift: pd.DataFrame,
    bucket_comparison: pd.DataFrame,
    safety: pd.DataFrame,
    monitoring_plan: pd.DataFrame,
) -> str:
    positive = _rank_shift_lines(rank_shift, positive=True)
    negative = _rank_shift_lines(rank_shift, positive=False)
    safety_counts = (
        summary.get("safety_checks") if isinstance(summary.get("safety_checks"), dict) else {}
    )
    lines = [
        "# SEC PIT Observe-Only Shadow Lane Summary",
        "",
        "## Metadata",
        f"- generated_at: {summary.get('generated_at', '')}",
        f"- start_date: {summary.get('start_date', '')}",
        f"- end_date: {summary.get('end_date', '')}",
        f"- lane_id: {summary.get('lane_id', '')}",
        f"- lane_status: {summary.get('lane_status', '')}",
        f"- candidate feature: {summary.get('candidate_feature', '')}",
        f"- observe weight: {_format_float(summary.get('observe_weight'))}",
        f"- max allowed initial weight: {_format_float(summary.get('max_allowed_initial_weight'))}",
        f"- production effect: {summary.get('production_effect', 'none')}",
        "",
        "## Executive Summary",
        f"- shadow_status: {summary.get('shadow_status', 'UNKNOWN')}",
        (
            "- observe-only shadow lane generated: "
            f"{'yes' if summary.get('score_rows', 0) else 'no or limited'}"
        ),
        "- production is unaffected because every output is isolated and `production_effect=none`.",
        "- primary expected signal direction: lower capex intensity receives a positive component.",
        f"- key limitations: {_limitations_text(summary.get('limitations'))}",
        "",
        "## Candidate Review Input",
        f"- candidate review status: {summary.get('candidate_review_status', '')}",
        f"- rank_ic_20d: {_format_float(summary.get('rank_ic_20d'))}",
        f"- hit_rate_20d: {_format_float(summary.get('hit_rate_20d'))}",
        f"- coverage: {_format_float(summary.get('coverage'))}",
        f"- data quality: {_format_float(summary.get('data_quality_score'))}",
        f"- baseline overlap risk: {summary.get('baseline_overlap_risk', '')}",
        "",
        "## Shadow Score Impact",
        f"- number of score rows: {summary.get('score_rows', 0)}",
        f"- average score delta: {_format_float(summary.get('average_score_delta'))}",
        "- largest positive rank shifts:",
        *positive,
        "- largest negative rank shifts:",
        *negative,
        "",
        "## Bucket Comparison",
    ]
    lines.extend(_bucket_lines(bucket_comparison))
    lines.extend(
        [
            "",
            "## Safety Audit",
            f"- passed checks: {safety_counts.get('passed', 0)}",
            f"- warnings: {safety_counts.get('warning', 0)}",
            f"- failed checks: {safety_counts.get('failed', 0)}",
            "- production write protection: enabled",
            "- active shadow write protection: enabled",
        ]
    )
    lines.extend(_safety_failed_lines(safety))
    lines.extend(
        [
            "",
            "## Monitoring Plan",
            f"- monitoring_status: {summary.get('monitoring_status', 'UNKNOWN')}",
            f"- monitoring_status_reason: {summary.get('monitoring_status_reason', '')}",
            f"- baseline coverage ratio: {_format_float(summary.get('baseline_coverage_ratio'))}",
            f"- label coverage ratio: {_format_float(summary.get('label_coverage_ratio'))}",
            f"- monitoring sample count: {summary.get('monitoring_sample_count', 0)}",
            (
                "- factor rollback triggered: "
                f"{'yes' if summary.get('factor_rollback_triggered') else 'no'}"
            ),
            (
                "- data limitation triggered: "
                f"{'yes' if summary.get('data_limitation_triggered') else 'no'}"
            ),
        ]
    )
    lines.extend(_monitoring_lines(monitoring_plan))
    lines.extend(
        [
            "",
            "## Recommendation",
            f"- {_recommendation(summary)}",
            "",
            "## Manual Review Checklist",
            "- Confirm `production_effect=none` on all generated rows.",
            "- Confirm `manual_review_required=true` on all generated rows.",
            "- Review mixed regime and ticker-level evidence before any future promotion.",
            "- Keep this lane observe-only until at least the configured monitoring window passes.",
            "- Stop the lane if rollback metrics breach configured thresholds.",
        ]
    )
    return "\n".join(lines) + "\n"


def _candidate_from_mapping(value: object) -> SecPitShadowObserveCandidate:
    raw = _mapping_value(value)
    return SecPitShadowObserveCandidate(
        feature_id=str(raw.get("feature_id") or ""),
        metric_id=str(raw.get("metric_id") or ""),
        approval_status=str(raw.get("approval_status") or ""),
        observe_weight=float(raw.get("observe_weight", 0.0)),
        max_allowed_initial_weight=float(raw.get("max_allowed_initial_weight", 0.0)),
        weight_direction=str(raw.get("weight_direction") or ""),
        pit_grade_policy=str(raw.get("pit_grade_policy") or SEC_PIT_BACKTEST_DATA_GRADE),
        minimum_monitoring_days=int(raw.get("minimum_monitoring_days", 60)),
        preferred_monitoring_days=int(raw.get("preferred_monitoring_days", 90)),
        enabled=_bool_value(raw.get("enabled"), default=False),
    )


def _validate_config(config: SecPitShadowObserveConfig) -> None:
    if config.production_effect != SEC_PIT_SHADOW_OBSERVE_PRODUCTION_EFFECT:
        raise ValueError("sec_pit_shadow_observe production_effect must be none")
    if not config.manual_review_required:
        raise ValueError("sec_pit_shadow_observe manual_review_required must be true")
    if not config.candidates:
        raise ValueError("sec_pit_shadow_observe must define at least one candidate")
    for candidate in config.candidates:
        if abs(candidate.observe_weight) > abs(candidate.max_allowed_initial_weight):
            raise ValueError(
                f"{candidate.feature_id} observe_weight exceeds max_allowed_initial_weight"
            )


def _candidate_config(
    config: SecPitShadowObserveConfig,
    candidate_feature: str,
) -> SecPitShadowObserveCandidate:
    for candidate in config.candidates:
        if candidate.feature_id == candidate_feature and candidate.enabled:
            return candidate
    raise ValueError(
        f"enabled SEC PIT shadow observe candidate not configured: {candidate_feature}"
    )


def _load_inputs(
    *,
    candidate_review_dir: Path,
    evaluation_dir: Path,
    comparison_dir: Path,
    diagnostics_dir: Path,
    feature_panel_path: Path,
    baseline_score_path: Path | None,
    baseline_score_dir: Path,
    end: date | None,
    latest: bool,
) -> _ShadowObserveInputs:
    candidate_review_summary_path = _latest_dated_path(
        candidate_review_dir,
        "sec_pit_candidate_review_summary_",
        ".json",
        None if latest else end,
    )
    evaluation_summary_path = _latest_dated_path(
        evaluation_dir,
        "sec_pit_evaluation_summary_",
        ".json",
        None if latest else end,
    )
    comparison_summary_path = _latest_dated_path(
        comparison_dir,
        "sec_pit_baseline_comparison_summary_",
        ".json",
        None if latest else end,
    )
    diagnostics_summary_path = _latest_dated_path(
        diagnostics_dir,
        "sec_pit_real_run_diagnostics_",
        ".json",
        None if latest else end,
    )
    candidate_review_summary = _read_json_object(candidate_review_summary_path)
    evaluation_summary = _read_json_object(evaluation_summary_path)
    comparison_summary = _read_json_object(comparison_summary_path)
    diagnostics_summary = _read_json_object(diagnostics_summary_path)
    resolved_end = (
        _summary_date(candidate_review_summary, "end_date")
        or _summary_date(evaluation_summary, "end_date")
        or _summary_date(comparison_summary, "end_date")
        or _summary_date(diagnostics_summary, "end_date")
        or end
    )
    candidate_evidence_path = _artifact_path(
        candidate_review_summary,
        "candidate_evidence_csv",
        _latest_dated_path(
            candidate_review_dir,
            "sec_pit_candidate_evidence_",
            ".csv",
            resolved_end,
        ),
    )
    shadow_proposal_path = _artifact_path(
        candidate_review_summary,
        "candidate_shadow_proposal_csv",
        _latest_dated_path(
            candidate_review_dir,
            "sec_pit_candidate_shadow_proposal_",
            ".csv",
            resolved_end,
        ),
    )
    baseline_overlap_path = _artifact_path(
        candidate_review_summary,
        "candidate_overlap_with_baseline_csv",
        _latest_dated_path(
            candidate_review_dir,
            "sec_pit_candidate_overlap_with_baseline_",
            ".csv",
            resolved_end,
        ),
    )
    feature_effectiveness_path = _artifact_path(
        evaluation_summary,
        "feature_effectiveness_csv",
        _latest_dated_path(evaluation_dir, "sec_pit_feature_effectiveness_", ".csv", resolved_end),
    )
    signal_attribution_path = _artifact_path(
        evaluation_summary,
        "signal_attribution_csv",
        _latest_dated_path(evaluation_dir, "sec_pit_signal_attribution_", ".csv", resolved_end),
    )
    decision_impact_path = _artifact_path(
        comparison_summary,
        "decision_impact_csv",
        _latest_dated_path(comparison_dir, "sec_pit_decision_impact_", ".csv", resolved_end),
    )
    label_coverage_path = _artifact_path(
        diagnostics_summary,
        "label_coverage_audit_csv",
        _latest_dated_path(
            diagnostics_dir,
            "sec_pit_label_coverage_audit_",
            ".csv",
            resolved_end,
        ),
    )
    resolved_feature_panel = _resolve_feature_panel_path(feature_panel_path)
    baseline = _load_baseline_inputs(
        baseline_score_dir=baseline_score_dir,
        end=resolved_end,
        baseline_score_path=baseline_score_path,
    )
    return _ShadowObserveInputs(
        candidate_review_summary_path=candidate_review_summary_path,
        candidate_evidence_path=candidate_evidence_path,
        shadow_proposal_path=shadow_proposal_path,
        baseline_overlap_path=baseline_overlap_path,
        evaluation_summary_path=evaluation_summary_path,
        feature_effectiveness_path=feature_effectiveness_path,
        signal_attribution_path=signal_attribution_path,
        comparison_summary_path=comparison_summary_path,
        decision_impact_path=decision_impact_path,
        diagnostics_summary_path=diagnostics_summary_path,
        label_coverage_path=label_coverage_path,
        feature_panel_path=resolved_feature_panel,
        baseline=baseline,
        candidate_review_summary=candidate_review_summary,
        candidate_evidence=_read_csv_or_empty(candidate_evidence_path),
        shadow_proposal=_read_csv_or_empty(shadow_proposal_path),
        baseline_overlap=_read_csv_or_empty(baseline_overlap_path),
        evaluation_summary=evaluation_summary,
        feature_effectiveness=_read_csv_or_empty(feature_effectiveness_path),
        signal_attribution=_read_csv_or_empty(signal_attribution_path),
        comparison_summary=comparison_summary,
        decision_impact=_read_csv_or_empty(decision_impact_path),
        diagnostics_summary=diagnostics_summary,
        label_coverage=_read_csv_or_empty(label_coverage_path),
    )


def _resolve_feature_panel_path(path: Path) -> Path:
    if path.exists():
        return path
    if DEFAULT_SEC_PIT_FEATURE_PANEL_PATH.exists():
        return DEFAULT_SEC_PIT_FEATURE_PANEL_PATH
    if LEGACY_SEC_PIT_FEATURE_PANEL_PATH.exists():
        return LEGACY_SEC_PIT_FEATURE_PANEL_PATH
    return path


def _load_baseline_inputs(
    *,
    baseline_score_dir: Path,
    end: date | None,
    baseline_score_path: Path | None,
) -> _BaselineInputs:
    resolved_end = end or date.max
    if baseline_score_path is not None:
        frame = _read_csv_or_empty(baseline_score_path)
        status = "OK" if not frame.empty else "LIMITED_BASELINE_MISSING"
        return _BaselineInputs(path=baseline_score_path, frame=frame, status=status)
    path = _baseline_path(baseline_score_dir, resolved_end)
    frame = _read_csv_or_empty(path)
    if not frame.empty:
        return _BaselineInputs(path=path, frame=frame, status="OK")
    if _is_default_baseline_score_dir(baseline_score_dir):
        fallback_frame = _read_csv_or_empty(DEFAULT_PROCESSED_BASELINE_SCORE_PATH)
        if not fallback_frame.empty:
            return _BaselineInputs(
                path=DEFAULT_PROCESSED_BASELINE_SCORE_PATH,
                frame=fallback_frame,
                status="FALLBACK_USED",
            )
    return _BaselineInputs(path=path, frame=frame, status="LIMITED_BASELINE_MISSING")


def _safety_audit(
    *,
    inputs: _ShadowObserveInputs,
    config: SecPitShadowObserveConfig,
    candidate: SecPitShadowObserveCandidate,
    candidate_feature: str,
    observe_weight: float,
    max_allowed_weight: float,
    output_dir: Path,
    production_hashes_before: dict[str, str],
    active_shadow_hashes_before: dict[str, str],
) -> pd.DataFrame:
    values = _candidate_review_values(inputs, candidate_feature)
    diagnostics_status = str(inputs.diagnostics_summary.get("diagnostics_status") or "MISSING")
    provenance_complete = _provenance_complete(inputs.diagnostics_summary.get("provenance"))
    data_quality = values.data_quality_score
    drawdown_coverage = _drawdown_label_coverage(inputs)
    overlap_risk = values.baseline_overlap_risk or "UNKNOWN"
    production_effect = _candidate_production_effect(inputs, candidate_feature)
    max_abs_initial = min(
        abs(max_allowed_weight),
        abs(candidate.max_allowed_initial_weight),
        abs(_float_or_default(config.safety.get("max_abs_initial_weight"), max_allowed_weight)),
    )
    production_hashes_after = _file_hashes(PRODUCTION_CONFIG_PATHS)
    active_shadow_hashes_after = _file_hashes(ACTIVE_SHADOW_CONFIG_PATHS)
    source_lineage_status = _source_lineage_status(inputs, candidate_feature)
    rows = [
        _audit_row(
            "candidate_review_exists",
            inputs.candidate_review_exists,
            "candidate review summary artifact exists",
            str(inputs.candidate_review_summary_path),
            "warning",
            "Generate TRADING-043 candidate review before observe-only scoring.",
        ),
        _audit_row(
            "candidate_review_status",
            (not inputs.candidate_review_exists)
            or values.candidate_review_status
            == str(config.safety.get("require_candidate_review_status")),
            str(config.safety.get("require_candidate_review_status")),
            values.candidate_review_status or "MISSING",
            "warning" if not inputs.candidate_review_exists else "critical",
            "Do not observe candidate until TRADING-043 marks it READY_FOR_MANUAL_REVIEW.",
        ),
        _audit_row(
            "diagnostics_status",
            diagnostics_status == str(config.safety.get("require_diagnostics_status")),
            str(config.safety.get("require_diagnostics_status")),
            diagnostics_status,
            "critical",
            "Rerun or fix TRADING-042 diagnostics before observe-only scoring.",
        ),
        _audit_row(
            "provenance_complete",
            provenance_complete is bool(config.safety.get("require_provenance_complete")),
            str(config.safety.get("require_provenance_complete")),
            str(provenance_complete),
            "critical",
            "Preserve SEC PIT provenance and rerun upstream artifacts.",
        ),
        _audit_row(
            "data_quality_score_min",
            pd.notna(data_quality)
            and data_quality >= _float_or_default(config.safety.get("min_data_quality_score"), 0.0),
            f">= {config.safety.get('min_data_quality_score')}",
            _format_float(data_quality),
            "warning" if not inputs.candidate_review_exists else "critical",
            "Improve candidate data quality before observe-only scoring.",
        ),
        _audit_row(
            "drawdown_label_coverage_min",
            drawdown_coverage
            >= _float_or_default(config.safety.get("min_drawdown_label_coverage"), 0.0),
            f">= {config.safety.get('min_drawdown_label_coverage')}",
            _format_float(drawdown_coverage),
            "critical",
            "Regenerate evaluation/comparison labels before observe-only scoring.",
        ),
        _audit_row(
            "observe_weight_within_limit",
            abs(observe_weight) <= max_abs_initial,
            f"abs(weight) <= {max_abs_initial:.4f}",
            f"{observe_weight:.4f}",
            "critical",
            "Reduce observe-only weight to the configured initial maximum.",
        ),
        _audit_row(
            "production_effect_none",
            production_effect == SEC_PIT_SHADOW_OBSERVE_PRODUCTION_EFFECT
            and config.production_effect == SEC_PIT_SHADOW_OBSERVE_PRODUCTION_EFFECT,
            SEC_PIT_SHADOW_OBSERVE_PRODUCTION_EFFECT,
            production_effect,
            "critical",
            "Stop and correct upstream artifact production_effect before continuing.",
        ),
        _audit_row(
            "production_config_unchanged",
            production_hashes_before == production_hashes_after,
            "no production config write",
            "unchanged" if production_hashes_before == production_hashes_after else "changed",
            "critical",
            "Restore production config and rerun observe-only command.",
        ),
        _audit_row(
            "active_shadow_config_unchanged",
            active_shadow_hashes_before == active_shadow_hashes_after,
            "no active shadow config write",
            "unchanged" if active_shadow_hashes_before == active_shadow_hashes_after else "changed",
            "critical",
            "Restore active shadow config and rerun observe-only command.",
        ),
        _audit_row(
            "output_path_isolated",
            _is_isolated_output_dir(output_dir),
            "outputs/sec_pit_shadow_observe",
            str(output_dir),
            "critical",
            "Write SEC PIT observe-only artifacts only under outputs/sec_pit_shadow_observe.",
        ),
        _audit_row(
            "pit_grade_policy_preserved",
            values.pit_grade in {"", candidate.pit_grade_policy}
            and candidate.pit_grade_policy == SEC_PIT_BACKTEST_DATA_GRADE,
            candidate.pit_grade_policy,
            values.pit_grade or candidate.pit_grade_policy,
            "critical",
            "Do not treat reconstructed SEC PIT as strict vendor-grade PIT.",
        ),
        _audit_row(
            "source_lineage_present",
            source_lineage_status in {"PRESENT", "NOT_AVAILABLE"},
            "source_lineage present where available",
            source_lineage_status,
            "warning",
            "Preserve source_lineage through SEC PIT evaluation artifacts.",
        ),
        _audit_row(
            "baseline_overlap_not_high",
            overlap_risk != "HIGH",
            "not HIGH",
            overlap_risk,
            "critical",
            "Keep candidate research-only when baseline overlap risk is HIGH.",
        ),
    ]
    return pd.DataFrame(rows, columns=list(SAFETY_AUDIT_COLUMNS))


@dataclass(frozen=True)
class _CandidateReviewValues:
    candidate_review_status: str
    rank_ic_20d: float
    hit_rate_20d: float
    coverage: float
    data_quality_score: float
    baseline_overlap_risk: str
    pit_grade: str


def _candidate_review_values(
    inputs: _ShadowObserveInputs,
    candidate_feature: str,
) -> _CandidateReviewValues:
    evidence_row = _first_row_for_feature(inputs.candidate_evidence, candidate_feature)
    proposal_row = _first_row_for_feature(inputs.shadow_proposal, candidate_feature)
    overlap_row = _first_row_for_feature(inputs.baseline_overlap, candidate_feature)
    status = str(
        proposal_row.get("proposal_status")
        or evidence_row.get("recommendation")
        or _summary_candidate_status(inputs.candidate_review_summary, candidate_feature)
        or ""
    )
    return _CandidateReviewValues(
        candidate_review_status=status,
        rank_ic_20d=_float_or_nan(evidence_row.get("rank_ic_20d")),
        hit_rate_20d=_float_or_nan(evidence_row.get("hit_rate_20d")),
        coverage=_float_or_nan(
            evidence_row.get("coverage_ratio", evidence_row.get("coverage")),
        ),
        data_quality_score=_float_or_nan(evidence_row.get("data_quality_score")),
        baseline_overlap_risk=str(overlap_row.get("redundancy_risk") or "UNKNOWN"),
        pit_grade=str(evidence_row.get("pit_grade") or ""),
    )


def _shadow_scores(
    *,
    inputs: _ShadowObserveInputs,
    config: SecPitShadowObserveConfig,
    candidate_feature: str,
    observe_weight: float,
    start: date,
    end: date,
) -> pd.DataFrame:
    attribution = _normalize_signal_attribution(
        inputs.signal_attribution,
        candidate_feature=candidate_feature,
        start=start,
        end=end,
    )
    if attribution.empty:
        return _empty_frame(SHADOW_SCORE_COLUMNS)
    baseline = _normalize_baseline_scores(
        inputs.baseline.frame,
        start=start,
        end=end,
        tickers=sorted(attribution["ticker"].dropna().astype(str).unique().tolist()),
    )
    if baseline.empty:
        merged = attribution.copy()
        merged["baseline_score"] = np.nan
    else:
        merged = attribution.merge(baseline, on=["decision_date", "ticker"], how="left")
    merged["bucket"] = _bucket_series(merged["ticker"], config)
    merged["observe_weight"] = observe_weight
    merged["sec_pit_shadow_component"] = (
        pd.to_numeric(merged["normalized_feature_value"], errors="coerce") * observe_weight
    )
    merged["sec_pit_observe_score"] = (
        pd.to_numeric(merged["baseline_score"], errors="coerce")
        + merged["sec_pit_shadow_component"]
    )
    merged = _rank_by_date(merged, "baseline_score", "baseline_rank")
    merged = _rank_by_date(merged, "sec_pit_observe_score", "sec_pit_observe_rank")
    merged["rank_delta"] = pd.to_numeric(merged["baseline_rank"], errors="coerce") - pd.to_numeric(
        merged["sec_pit_observe_rank"], errors="coerce"
    )
    merged["manual_review_required"] = True
    merged["production_effect"] = SEC_PIT_SHADOW_OBSERVE_PRODUCTION_EFFECT
    for column in SHADOW_SCORE_COLUMNS:
        if column not in merged.columns:
            merged[column] = np.nan
    return merged.loc[:, list(SHADOW_SCORE_COLUMNS)].sort_values(
        ["decision_date", "sec_pit_observe_rank", "ticker"],
        na_position="last",
    )


def _normalize_signal_attribution(
    frame: pd.DataFrame,
    *,
    candidate_feature: str,
    start: date,
    end: date,
) -> pd.DataFrame:
    columns = [
        "decision_date",
        "ticker",
        "feature_id",
        "feature_value",
        "normalized_feature_value",
        "forward_return_20d",
        "forward_return_60d",
        "relative_return_vs_QQQ_20d",
        "max_drawdown_forward_20d",
        "pit_grade",
        "available_time",
        "accession_number",
        "source_lineage",
    ]
    required = {"decision_date", "ticker", "feature_id", "feature_value", "normalized_value"}
    if frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame(columns=columns)
    normalized = frame.copy().fillna("")
    aliases = load_ticker_aliases()
    normalized["decision_date"] = pd.to_datetime(
        normalized["decision_date"],
        errors="coerce",
    ).dt.date
    normalized["ticker"] = canonicalize_ticker_series(normalized["ticker"], aliases=aliases)
    normalized = normalized.loc[
        normalized["decision_date"].notna()
        & normalized["decision_date"].map(lambda value: start <= value <= end)
        & (normalized["feature_id"].astype(str) == candidate_feature)
    ].copy()
    if normalized.empty:
        return pd.DataFrame(columns=columns)
    if "available_time" in normalized.columns:
        available = pd.to_datetime(normalized["available_time"], errors="coerce", utc=True)
        valid_available = available.notna() & (available.dt.date <= normalized["decision_date"])
        normalized = normalized.loc[valid_available].copy()
    normalized["feature_value"] = pd.to_numeric(normalized["feature_value"], errors="coerce")
    normalized["normalized_feature_value"] = pd.to_numeric(
        normalized["normalized_value"],
        errors="coerce",
    )
    normalized["forward_return_20d"] = _numeric_column(normalized, "forward_return_20d")
    normalized["forward_return_60d"] = _numeric_column(normalized, "forward_return_60d")
    normalized["relative_return_vs_QQQ_20d"] = _numeric_column(
        normalized,
        "relative_return_vs_QQQ_20d",
    )
    normalized["max_drawdown_forward_20d"] = _numeric_column(
        normalized,
        "max_drawdown_forward_20d",
    )
    for column in ("pit_grade", "available_time", "accession_number", "source_lineage"):
        if column not in normalized.columns:
            normalized[column] = ""
    return normalized.loc[:, columns].sort_values(["decision_date", "ticker"])


def _normalize_baseline_scores(
    frame: pd.DataFrame,
    *,
    start: date,
    end: date,
    tickers: list[str],
) -> pd.DataFrame:
    columns = ["decision_date", "ticker", "baseline_score"]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    date_column = _first_existing_column(frame, ("decision_date", "as_of", "date"))
    score_column = _first_existing_column(
        frame,
        ("baseline_score", "score", "overall_score", "risk_adjusted_score"),
    )
    if date_column is None or score_column is None:
        return pd.DataFrame(columns=columns)
    normalized = frame.copy().fillna("")
    if "component" in normalized.columns:
        overall = normalized.loc[normalized["component"].astype(str) == "overall"].copy()
        if not overall.empty:
            normalized = overall
    aliases = load_ticker_aliases()
    normalized["decision_date"] = pd.to_datetime(
        normalized[date_column],
        errors="coerce",
    ).dt.date
    normalized["baseline_score"] = pd.to_numeric(normalized[score_column], errors="coerce")
    normalized = normalized.loc[
        normalized["decision_date"].notna()
        & normalized["baseline_score"].notna()
        & normalized["decision_date"].map(lambda value: start <= value <= end)
    ].copy()
    if normalized.empty:
        return pd.DataFrame(columns=columns)
    ticker_column = _first_existing_column(normalized, ("ticker", "symbol"))
    records: list[dict[str, Any]] = []
    for row in normalized.to_dict(orient="records"):
        raw_ticker = str(row.get(ticker_column or "") or "").upper()
        row_ticker = aliases.get(raw_ticker, raw_ticker)
        row_tickers = [row_ticker] if row_ticker else tickers
        for ticker in row_tickers:
            records.append(
                {
                    "decision_date": row["decision_date"],
                    "ticker": ticker,
                    "baseline_score": float(row["baseline_score"]),
                }
            )
    return pd.DataFrame(records, columns=columns).drop_duplicates(
        subset=["decision_date", "ticker"],
        keep="last",
    )


def _rank_shift(scores: pd.DataFrame) -> pd.DataFrame:
    if scores.empty:
        return _empty_frame(RANK_SHIFT_COLUMNS)
    required = {"baseline_rank", "sec_pit_observe_rank", "baseline_score", "sec_pit_observe_score"}
    if not required.issubset(scores.columns):
        return _empty_frame(RANK_SHIFT_COLUMNS)
    frame = scores.copy()
    frame["score_delta"] = pd.to_numeric(
        frame["sec_pit_observe_score"], errors="coerce"
    ) - pd.to_numeric(frame["baseline_score"], errors="coerce")
    frame["rank_delta"] = pd.to_numeric(frame["rank_delta"], errors="coerce")
    frame = frame.loc[
        pd.to_numeric(frame["baseline_rank"], errors="coerce").notna()
        & pd.to_numeric(frame["sec_pit_observe_rank"], errors="coerce").notna()
    ].copy()
    if frame.empty:
        return _empty_frame(RANK_SHIFT_COLUMNS)
    frame["abs_rank_delta"] = frame["rank_delta"].abs()
    frame["rank_shift_reason"] = frame["rank_delta"].map(_rank_shift_reason)
    for column in RANK_SHIFT_COLUMNS:
        if column not in frame.columns:
            frame[column] = np.nan
    return frame.loc[:, list(RANK_SHIFT_COLUMNS)].sort_values(
        ["abs_rank_delta", "decision_date", "ticker"],
        ascending=[False, True, True],
    )


def _bucket_comparison(
    scores: pd.DataFrame,
    *,
    config: SecPitShadowObserveConfig,
) -> pd.DataFrame:
    buckets = _required_buckets(config)
    if not scores.empty and (scores["bucket"].astype(str) == "other").any():
        buckets.append("other")
    records = [_bucket_row(scores, bucket) for bucket in buckets]
    return pd.DataFrame(records, columns=list(BUCKET_COMPARISON_COLUMNS))


def _empty_bucket_comparison(config: SecPitShadowObserveConfig) -> pd.DataFrame:
    return pd.DataFrame(
        [
            _bucket_row(_empty_frame(SHADOW_SCORE_COLUMNS), bucket)
            for bucket in _required_buckets(config)
        ],
        columns=list(BUCKET_COMPARISON_COLUMNS),
    )


def _bucket_row(scores: pd.DataFrame, bucket: str) -> dict[str, Any]:
    frame = scores if bucket == "all" else scores.loc[scores["bucket"].astype(str) == bucket]
    if frame.empty:
        return {
            "bucket": bucket,
            "sample_count": 0,
            "avg_baseline_score": np.nan,
            "avg_sec_pit_observe_score": np.nan,
            "avg_score_delta": np.nan,
            "avg_rank_delta": np.nan,
            "avg_forward_return_20d": np.nan,
            "avg_forward_return_60d": np.nan,
            "avg_relative_return_vs_QQQ_20d": np.nan,
            "avg_max_drawdown_forward_20d": np.nan,
            "hit_rate_20d": np.nan,
            "rank_ic_20d": np.nan,
            "rank_ic_60d": np.nan,
            "drawdown_improvement_20d": np.nan,
            "interpretation": "NO_ROWS",
        }
    score_delta = pd.to_numeric(frame["sec_pit_observe_score"], errors="coerce") - pd.to_numeric(
        frame["baseline_score"], errors="coerce"
    )
    rank_ic_20d = _rank_correlation(
        frame["sec_pit_shadow_component"],
        frame["forward_return_20d"],
    )
    return {
        "bucket": bucket,
        "sample_count": int(len(frame)),
        "avg_baseline_score": _mean_or_nan(frame["baseline_score"]),
        "avg_sec_pit_observe_score": _mean_or_nan(frame["sec_pit_observe_score"]),
        "avg_score_delta": _mean_or_nan(score_delta),
        "avg_rank_delta": _mean_or_nan(frame["rank_delta"]),
        "avg_forward_return_20d": _mean_or_nan(frame["forward_return_20d"]),
        "avg_forward_return_60d": _mean_or_nan(frame["forward_return_60d"]),
        "avg_relative_return_vs_QQQ_20d": _mean_or_nan(frame["relative_return_vs_QQQ_20d"]),
        "avg_max_drawdown_forward_20d": _mean_or_nan(frame["max_drawdown_forward_20d"]),
        "hit_rate_20d": _hit_rate(frame["forward_return_20d"]),
        "rank_ic_20d": rank_ic_20d,
        "rank_ic_60d": _rank_correlation(
            frame["sec_pit_shadow_component"],
            frame["forward_return_60d"],
        ),
        "drawdown_improvement_20d": _top_rank_label_delta(frame, "max_drawdown_forward_20d"),
        "interpretation": _bucket_interpretation(rank_ic_20d),
    }


@dataclass(frozen=True)
class _MonitoringQuality:
    baseline_coverage_ratio: float
    label_coverage_ratio: float
    monitoring_sample_count: int
    min_baseline_coverage_ratio: float
    min_label_coverage_ratio: float
    min_monitoring_sample_count: int
    status: str
    reason: str

    @property
    def data_limited(self) -> bool:
        return self.status in {
            "LIMITED_BASELINE_MISSING",
            "LIMITED_LABELS_MISSING",
            "INSUFFICIENT_MONITORING_SAMPLE",
        }


def _monitoring_plan(
    *,
    config: SecPitShadowObserveConfig,
    candidate: SecPitShadowObserveCandidate,
    start: date,
    observe_weight: float,
    scores: pd.DataFrame,
    bucket_comparison: pd.DataFrame,
    candidate_review: _CandidateReviewValues,
) -> pd.DataFrame:
    quality = _monitoring_quality(scores, config)
    all_bucket = _bucket_record(bucket_comparison, "all")
    current_values = {
        "rolling_rank_ic_20d": _float_or_nan(all_bucket.get("rank_ic_20d")),
        "rolling_rank_ic_60d": _float_or_nan(all_bucket.get("rank_ic_60d")),
        "relative_return_vs_baseline_20d": _top_rank_label_delta(scores, "forward_return_20d"),
        "drawdown_improvement_20d": _top_rank_label_delta(scores, "max_drawdown_forward_20d"),
        "hit_rate_20d": _float_or_nan(all_bucket.get("hit_rate_20d")),
        "data_quality_score": candidate_review.data_quality_score,
        "provenance_complete": np.nan,
        "baseline_overlap": np.nan,
    }
    records: list[dict[str, Any]] = []
    for metric in MONITORING_METRICS:
        threshold = _monitoring_thresholds(config, metric)
        current = current_values.get(metric)
        records.append(
            {
                "lane_id": config.lane_id,
                "feature_id": candidate.feature_id,
                "observe_weight": observe_weight,
                "start_date": start.isoformat(),
                "minimum_monitoring_days": candidate.minimum_monitoring_days,
                "preferred_monitoring_days": candidate.preferred_monitoring_days,
                "monitoring_metric": metric,
                "target_direction": threshold["target_direction"],
                "warning_threshold": threshold["warning_threshold"],
                "rollback_threshold": threshold["rollback_threshold"],
                "current_value": _monitoring_value(metric, current, candidate_review),
                "status": _monitoring_status(
                    metric,
                    current,
                    candidate_review,
                    config,
                    quality,
                ),
                "manual_review_required": True,
                "production_effect": SEC_PIT_SHADOW_OBSERVE_PRODUCTION_EFFECT,
            }
        )
    return pd.DataFrame(records, columns=list(MONITORING_PLAN_COLUMNS))


def _summary_payload(
    *,
    status: str,
    start: date,
    end: date,
    config: SecPitShadowObserveConfig,
    candidate: SecPitShadowObserveCandidate,
    candidate_feature: str,
    observe_weight: float,
    max_allowed_weight: float,
    inputs: _ShadowObserveInputs,
    shadow_scores: pd.DataFrame,
    rank_shift: pd.DataFrame,
    bucket_comparison: pd.DataFrame,
    monitoring_plan: pd.DataFrame,
    safety: pd.DataFrame,
    safety_counts: dict[str, int],
    limitations: list[str],
    artifacts: dict[str, Path],
) -> dict[str, Any]:
    values = _candidate_review_values(inputs, candidate_feature)
    monitoring_quality = _monitoring_quality(shadow_scores, config)
    monitoring_status = _monitoring_overall_status(monitoring_plan)
    return {
        "schema_version": "1.0",
        "report_type": SEC_PIT_SHADOW_OBSERVE_REPORT_TYPE,
        "task_id": SEC_PIT_SHADOW_OBSERVE_TASK_ID,
        "generated_at": _deterministic_generated_at(end),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "shadow_status": status,
        "lane_id": config.lane_id,
        "lane_status": config.lane_status,
        "candidate_feature": candidate_feature,
        "observe_weight": observe_weight,
        "max_allowed_initial_weight": max_allowed_weight,
        "production_effect": SEC_PIT_SHADOW_OBSERVE_PRODUCTION_EFFECT,
        "manual_review_required": True,
        "candidate_review_status": values.candidate_review_status or "MISSING",
        "diagnostics_status": str(
            inputs.diagnostics_summary.get("diagnostics_status") or "MISSING"
        ),
        "provenance_complete": _provenance_complete(inputs.diagnostics_summary.get("provenance")),
        "data_quality_score": _json_number(values.data_quality_score) or 0.0,
        "drawdown_label_coverage": _json_number(_drawdown_label_coverage(inputs)) or 0.0,
        "baseline_overlap_risk": values.baseline_overlap_risk or "UNKNOWN",
        "rank_ic_20d": _json_number(values.rank_ic_20d),
        "hit_rate_20d": _json_number(values.hit_rate_20d),
        "coverage": _json_number(values.coverage),
        "score_rows": int(len(shadow_scores)),
        "rank_shift_rows": int(len(rank_shift)),
        "bucket_comparison_rows": int(len(bucket_comparison)),
        "baseline_coverage_ratio": _json_number(monitoring_quality.baseline_coverage_ratio) or 0.0,
        "label_coverage_ratio": _json_number(monitoring_quality.label_coverage_ratio) or 0.0,
        "monitoring_sample_count": monitoring_quality.monitoring_sample_count,
        "min_baseline_coverage_ratio": monitoring_quality.min_baseline_coverage_ratio,
        "min_label_coverage_ratio": monitoring_quality.min_label_coverage_ratio,
        "min_monitoring_sample_count": monitoring_quality.min_monitoring_sample_count,
        "baseline_coverage_status": (
            "OK"
            if monitoring_quality.baseline_coverage_ratio
            >= monitoring_quality.min_baseline_coverage_ratio
            else "LIMITED_BASELINE_MISSING"
        ),
        "monitoring_status": monitoring_status,
        "monitoring_status_reason": (
            monitoring_quality.reason
            if monitoring_quality.data_limited
            else _monitoring_status_reason(monitoring_status)
        ),
        "factor_rollback_triggered": monitoring_status == "ROLLBACK_TRIGGERED_BY_FACTOR",
        "data_limitation_triggered": monitoring_quality.data_limited
        or monitoring_status == "ROLLBACK_TRIGGERED_BY_DATA",
        "average_score_delta": _json_number(
            _mean_or_nan(_score_delta_series(shadow_scores)),
        ),
        "top_positive_rank_shifts": _top_rank_shifts(rank_shift, positive=True),
        "top_negative_rank_shifts": _top_rank_shifts(rank_shift, positive=False),
        "bucket_summary": _bucket_summary(bucket_comparison),
        "safety_checks": safety_counts,
        "limitations": _dedupe_text(limitations),
        "safety": {
            "manual_review_required": True,
            "production_effect": SEC_PIT_SHADOW_OBSERVE_PRODUCTION_EFFECT,
            "production_weights_modified": False,
            "active_shadow_weights_modified": False,
            "observe_only": True,
            "allow_production_write": False,
            "allow_active_shadow_write": False,
        },
        "artifact_paths": {
            "candidate_review_summary": str(inputs.candidate_review_summary_path),
            "candidate_evidence": str(inputs.candidate_evidence_path),
            "candidate_shadow_proposal": str(inputs.shadow_proposal_path),
            "candidate_overlap_with_baseline": str(inputs.baseline_overlap_path),
            "evaluation_summary": str(inputs.evaluation_summary_path),
            "feature_effectiveness": str(inputs.feature_effectiveness_path),
            "signal_attribution": str(inputs.signal_attribution_path),
            "comparison_summary": str(inputs.comparison_summary_path),
            "decision_impact": str(inputs.decision_impact_path),
            "diagnostics_summary": str(inputs.diagnostics_summary_path),
            "label_coverage": str(inputs.label_coverage_path),
            "feature_panel": str(inputs.feature_panel_path),
            "baseline_score": str(inputs.baseline.path),
        },
        "input_checksums": {
            "candidate_review_summary_sha256": _file_sha256(inputs.candidate_review_summary_path),
            "signal_attribution_sha256": _file_sha256(inputs.signal_attribution_path),
            "baseline_score_sha256": _file_sha256(inputs.baseline.path),
            "diagnostics_summary_sha256": _file_sha256(inputs.diagnostics_summary_path),
        },
        "output_artifacts": {key: str(value) for key, value in artifacts.items()},
    }


def _summary_candidate_status(summary: dict[str, Any], candidate_feature: str) -> str:
    top = summary.get("top_candidates") if isinstance(summary, dict) else None
    if not isinstance(top, list):
        return ""
    for item in top:
        if not isinstance(item, dict):
            continue
        if str(item.get("feature_id") or "") == candidate_feature:
            return str(item.get("proposal_status") or item.get("recommendation") or "")
    return ""


def _candidate_production_effect(
    inputs: _ShadowObserveInputs,
    candidate_feature: str,
) -> str:
    proposal_row = _first_row_for_feature(inputs.shadow_proposal, candidate_feature)
    evidence_row = _first_row_for_feature(inputs.candidate_evidence, candidate_feature)
    return str(
        proposal_row.get("production_effect")
        or evidence_row.get("production_effect")
        or inputs.candidate_review_summary.get("production_effect")
        or SEC_PIT_SHADOW_OBSERVE_PRODUCTION_EFFECT
    )


def _drawdown_label_coverage(inputs: _ShadowObserveInputs) -> float:
    labels = inputs.diagnostics_summary.get("labels")
    if isinstance(labels, dict):
        value = _float_or_nan(labels.get("max_drawdown_forward_20d_coverage"))
        if pd.notna(value):
            return value
    value = _float_or_nan(inputs.candidate_review_summary.get("drawdown_label_coverage"))
    if pd.notna(value):
        return value
    if not inputs.label_coverage.empty and "label_name" in inputs.label_coverage.columns:
        rows = inputs.label_coverage.loc[
            inputs.label_coverage["label_name"].astype(str) == "max_drawdown_forward_20d"
        ]
        if not rows.empty:
            value = _float_or_nan(rows.iloc[0].get("coverage_ratio"))
            if pd.notna(value):
                return value
    return 0.0


def _provenance_complete(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    missing = _float_or_default(value.get("missing_rows"), 0.0)
    complete_ratio = _float_or_default(value.get("complete_ratio"), 0.0)
    return missing == 0 and complete_ratio >= 1.0


def _source_lineage_status(inputs: _ShadowObserveInputs, candidate_feature: str) -> str:
    frame = inputs.signal_attribution
    if frame.empty or "feature_id" not in frame.columns:
        return "NOT_AVAILABLE"
    rows = frame.loc[frame["feature_id"].astype(str) == candidate_feature]
    if rows.empty:
        return "NOT_AVAILABLE"
    if "source_lineage" not in rows.columns:
        return "MISSING"
    present = rows["source_lineage"].fillna("").astype(str).str.strip().astype(bool)
    return "PRESENT" if bool(present.all()) else "MISSING"


def _critical_safety_failed(safety: pd.DataFrame) -> bool:
    if safety.empty:
        return False
    return bool(
        (
            (safety["status"].astype(str) == "FAIL")
            & (safety["severity"].astype(str) == "critical")
        ).any()
    )


def _safety_counts(safety: pd.DataFrame) -> dict[str, int]:
    counter = Counter(safety["status"].astype(str)) if not safety.empty else Counter()
    return {
        "passed": int(counter.get("PASS", 0)),
        "warning": int(counter.get("WARN", 0)),
        "failed": int(counter.get("FAIL", 0)),
    }


def _audit_row(
    check_name: str,
    passed: bool,
    expected: object,
    actual: object,
    severity: str,
    recommended_action: str,
) -> dict[str, str]:
    return {
        "check_name": check_name,
        "status": "PASS" if passed else "FAIL",
        "expected": str(expected),
        "actual": str(actual),
        "severity": severity,
        "recommended_action": "" if passed else recommended_action,
    }


def _base_limitations(
    inputs: _ShadowObserveInputs,
    config: SecPitShadowObserveConfig,
    candidate_feature: str,
) -> list[str]:
    limitations = [
        "SEC reconstructed PIT remains B-grade filing-time PIT, not strict vendor archive PIT.",
        "This lane is observe-only and does not modify production or active shadow weights.",
    ]
    if config.production_effect != SEC_PIT_SHADOW_OBSERVE_PRODUCTION_EFFECT:
        limitations.append("Config production_effect is not none.")
    if not inputs.candidate_review_exists:
        limitations.append("Candidate review summary is missing.")
    values = _candidate_review_values(inputs, candidate_feature)
    if values.baseline_overlap_risk == "HIGH":
        limitations.append("Baseline overlap risk is HIGH.")
    if inputs.baseline.status == "FALLBACK_USED":
        limitations.append("Baseline score fallback path was used.")
    return _dedupe_text(limitations)


def _labels_missing(scores: pd.DataFrame) -> bool:
    if scores.empty:
        return False
    values = pd.to_numeric(scores.get("forward_return_20d"), errors="coerce")
    return bool(values.isna().all())


def _baseline_missing(scores: pd.DataFrame) -> bool:
    if scores.empty or "baseline_score" not in scores.columns:
        return False
    return bool(pd.to_numeric(scores["baseline_score"], errors="coerce").isna().any())


def _rank_by_date(frame: pd.DataFrame, score_column: str, rank_column: str) -> pd.DataFrame:
    result = frame.copy()
    result[rank_column] = np.nan
    scores = pd.to_numeric(result[score_column], errors="coerce")
    valid = scores.notna()
    if not bool(valid.any()):
        return result
    ranked = result.loc[valid].copy()
    ranked["_score"] = scores.loc[valid]
    ranked = ranked.sort_values(
        ["decision_date", "_score", "ticker"],
        ascending=[True, False, True],
    )
    ranks = ranked.groupby("decision_date").cumcount() + 1
    result.loc[ranked.index, rank_column] = ranks.astype(int)
    return result


def _bucket_series(tickers: pd.Series, config: SecPitShadowObserveConfig) -> pd.Series:
    monitoring = config.monitoring
    semis = {str(ticker).upper() for ticker in monitoring.get("semiconductor_tickers", [])}
    platforms = {str(ticker).upper() for ticker in monitoring.get("platform_tickers", [])}
    aliases = load_ticker_aliases()
    canonical = canonicalize_ticker_series(tickers, aliases=aliases).astype(str).str.upper()
    return canonical.map(
        lambda ticker: (
            "semiconductor" if ticker in semis else "platform" if ticker in platforms else "other"
        )
    )


def _required_buckets(config: SecPitShadowObserveConfig) -> list[str]:
    buckets = [str(bucket) for bucket in config.monitoring.get("buckets", [])]
    result = ["all"]
    for bucket in buckets:
        if bucket != "all" and bucket not in result:
            result.append(bucket)
    for bucket in ("semiconductor", "platform"):
        if bucket not in result:
            result.append(bucket)
    return result


def _rank_shift_reason(value: object) -> str:
    delta = _float_or_nan(value)
    if pd.isna(delta):
        return "BASELINE_MISSING"
    if delta > 0:
        return "PROMOTED_BY_SEC_PIT_OBSERVE_COMPONENT"
    if delta < 0:
        return "DOWNGRADED_BY_SEC_PIT_OBSERVE_COMPONENT"
    return "RANK_UNCHANGED"


def _bucket_interpretation(rank_ic_20d: float) -> str:
    if pd.isna(rank_ic_20d):
        return "LIMITED_LABELS_OR_SAMPLE"
    if rank_ic_20d > 0:
        return "OBSERVE_COMPONENT_SUPPORTIVE"
    if rank_ic_20d < 0:
        return "OBSERVE_COMPONENT_WEAK_OR_WRONG_DIRECTION"
    return "NEUTRAL"


def _monitoring_thresholds(
    config: SecPitShadowObserveConfig,
    metric: str,
) -> dict[str, object]:
    rollback = config.rollback
    if metric in {"rolling_rank_ic_20d", "rolling_rank_ic_60d"}:
        threshold = _float_or_default(
            rollback.get("rolling_rank_ic_20d_wrong_direction_threshold"),
            0.02,
        )
        return {
            "target_direction": "positive",
            "warning_threshold": f">= {threshold:.4f}",
            "rollback_threshold": f"< {-threshold:.4f}",
        }
    if metric == "relative_return_vs_baseline_20d":
        return {
            "target_direction": "positive",
            "warning_threshold": ">= 0.0000",
            "rollback_threshold": str(rollback.get("max_negative_relative_return_20d", "")),
        }
    if metric == "drawdown_improvement_20d":
        return {
            "target_direction": "positive",
            "warning_threshold": ">= 0.0000",
            "rollback_threshold": str(rollback.get("max_drawdown_deterioration_20d", "")),
        }
    if metric == "hit_rate_20d":
        return {
            "target_direction": "positive",
            "warning_threshold": "",
            "rollback_threshold": "",
        }
    if metric == "data_quality_score":
        return {
            "target_direction": "above_minimum",
            "warning_threshold": str(config.safety.get("min_data_quality_score", "")),
            "rollback_threshold": str(config.safety.get("min_data_quality_score", "")),
        }
    if metric == "provenance_complete":
        return {
            "target_direction": "true",
            "warning_threshold": "true",
            "rollback_threshold": "false",
        }
    return {
        "target_direction": "not_high",
        "warning_threshold": "MEDIUM",
        "rollback_threshold": "HIGH",
    }


def _monitoring_value(
    metric: str,
    current: object,
    candidate_review: _CandidateReviewValues,
) -> object:
    if metric == "provenance_complete":
        return ""
    if metric == "baseline_overlap":
        return candidate_review.baseline_overlap_risk
    return current


def _monitoring_quality(
    scores: pd.DataFrame,
    config: SecPitShadowObserveConfig,
) -> _MonitoringQuality:
    gate = config.monitoring_quality_gate
    min_baseline = _float_or_default(gate.get("min_baseline_coverage_ratio"), 0.90)
    min_labels = _float_or_default(gate.get("min_label_coverage_ratio"), 0.90)
    min_sample = int(
        gate.get("min_monitoring_sample_count")
        or config.rollback.get("min_monitoring_sample_count")
        or 20
    )
    if scores.empty:
        return _MonitoringQuality(
            baseline_coverage_ratio=0.0,
            label_coverage_ratio=0.0,
            monitoring_sample_count=0,
            min_baseline_coverage_ratio=min_baseline,
            min_label_coverage_ratio=min_labels,
            min_monitoring_sample_count=min_sample,
            status="LIMITED_BASELINE_MISSING",
            reason="No shadow score rows were available for monitoring coverage gates.",
        )
    baseline_values = pd.to_numeric(scores.get("baseline_score"), errors="coerce")
    label_values = pd.to_numeric(scores.get("forward_return_20d"), errors="coerce")
    baseline_present = baseline_values.notna()
    label_present = label_values.notna()
    row_count = int(len(scores))
    baseline_ratio = float(baseline_present.sum() / row_count) if row_count else 0.0
    label_ratio = float(label_present.sum() / row_count) if row_count else 0.0
    sample_count = int((baseline_present & label_present).sum())
    if baseline_ratio < min_baseline:
        return _MonitoringQuality(
            baseline_coverage_ratio=baseline_ratio,
            label_coverage_ratio=label_ratio,
            monitoring_sample_count=sample_count,
            min_baseline_coverage_ratio=min_baseline,
            min_label_coverage_ratio=min_labels,
            min_monitoring_sample_count=min_sample,
            status="LIMITED_BASELINE_MISSING",
            reason=(
                "Baseline score coverage is below the configured monitoring quality gate "
                f"({baseline_ratio:.2%} < {min_baseline:.2%})."
            ),
        )
    if label_ratio < min_labels:
        return _MonitoringQuality(
            baseline_coverage_ratio=baseline_ratio,
            label_coverage_ratio=label_ratio,
            monitoring_sample_count=sample_count,
            min_baseline_coverage_ratio=min_baseline,
            min_label_coverage_ratio=min_labels,
            min_monitoring_sample_count=min_sample,
            status="LIMITED_LABELS_MISSING",
            reason=(
                "Forward label coverage is below the configured monitoring quality gate "
                f"({label_ratio:.2%} < {min_labels:.2%})."
            ),
        )
    if sample_count < min_sample:
        return _MonitoringQuality(
            baseline_coverage_ratio=baseline_ratio,
            label_coverage_ratio=label_ratio,
            monitoring_sample_count=sample_count,
            min_baseline_coverage_ratio=min_baseline,
            min_label_coverage_ratio=min_labels,
            min_monitoring_sample_count=min_sample,
            status="INSUFFICIENT_MONITORING_SAMPLE",
            reason=(
                "Monitoring sample count is below the configured minimum "
                f"({sample_count} < {min_sample})."
            ),
        )
    return _MonitoringQuality(
        baseline_coverage_ratio=baseline_ratio,
        label_coverage_ratio=label_ratio,
        monitoring_sample_count=sample_count,
        min_baseline_coverage_ratio=min_baseline,
        min_label_coverage_ratio=min_labels,
        min_monitoring_sample_count=min_sample,
        status="OK",
        reason="Monitoring quality gates passed.",
    )


def _monitoring_status(
    metric: str,
    current: object,
    candidate_review: _CandidateReviewValues,
    config: SecPitShadowObserveConfig,
    quality: _MonitoringQuality,
) -> str:
    if (
        quality.data_limited
        and bool(
            config.monitoring_quality_gate.get("data_limited_status_prevents_factor_rollback", True)
        )
        and metric in FACTOR_MONITORING_METRICS
    ):
        return quality.status
    if metric == "baseline_overlap":
        if candidate_review.baseline_overlap_risk == "HIGH":
            return "ROLLBACK_TRIGGERED_BY_DATA"
        if candidate_review.baseline_overlap_risk in {"MEDIUM", "UNKNOWN"}:
            return "WATCH"
        return "OK"
    if metric == "provenance_complete":
        return "OK"
    value = _float_or_nan(current)
    if pd.isna(value):
        return "LIMITED"
    rollback = config.rollback
    if metric in {"rolling_rank_ic_20d", "rolling_rank_ic_60d"}:
        threshold = _float_or_default(
            rollback.get("rolling_rank_ic_20d_wrong_direction_threshold"),
            0.02,
        )
        if value < -threshold:
            return "ROLLBACK_TRIGGERED_BY_FACTOR"
        if value < threshold:
            return "WATCH"
        return "OK"
    if metric == "relative_return_vs_baseline_20d":
        limit = _float_or_default(rollback.get("max_negative_relative_return_20d"), -0.05)
        if value < limit:
            return "ROLLBACK_TRIGGERED_BY_FACTOR"
        if value < 0:
            return "WATCH"
        return "OK"
    if metric == "drawdown_improvement_20d":
        limit = _float_or_default(rollback.get("max_drawdown_deterioration_20d"), -0.03)
        if value < limit:
            return "ROLLBACK_TRIGGERED_BY_FACTOR"
        if value < 0:
            return "WATCH"
        return "OK"
    if metric == "data_quality_score":
        limit = _float_or_default(config.safety.get("min_data_quality_score"), 0.0)
        return "OK" if value >= limit else "ROLLBACK_TRIGGERED_BY_DATA"
    return "OK"


def _monitoring_overall_status(frame: pd.DataFrame) -> str:
    if frame.empty or "status" not in frame.columns:
        return "INSUFFICIENT_MONITORING_SAMPLE"
    statuses = set(frame["status"].astype(str))
    for status in (
        "FAILED_SAFETY_CHECK",
        "LIMITED_BASELINE_MISSING",
        "LIMITED_LABELS_MISSING",
        "INSUFFICIENT_MONITORING_SAMPLE",
        "ROLLBACK_TRIGGERED_BY_DATA",
        "ROLLBACK_TRIGGERED_BY_FACTOR",
    ):
        if status in statuses:
            return status
    if "LIMITED" in statuses:
        return "INSUFFICIENT_MONITORING_SAMPLE"
    if statuses and statuses <= {"OK"}:
        return "OK"
    return "OK"


def _monitoring_status_reason(status: str) -> str:
    if status == "ROLLBACK_TRIGGERED_BY_FACTOR":
        return (
            "Factor monitoring metric breached a configured rollback threshold "
            "after data gates passed."
        )
    if status == "ROLLBACK_TRIGGERED_BY_DATA":
        return "A data or design quality condition breached monitoring safety thresholds."
    if status == "OK":
        return "Monitoring quality gates passed and no factor rollback threshold was breached."
    return "Monitoring output is limited by data coverage or sample availability."


def _top_rank_label_delta(scores: pd.DataFrame, label_column: str) -> float:
    if scores.empty or label_column not in scores.columns:
        return np.nan
    deltas: list[float] = []
    for _, group in scores.groupby("decision_date", sort=True):
        values = group.copy()
        label = pd.to_numeric(values[label_column], errors="coerce")
        values = values.loc[label.notna()].copy()
        if values.empty:
            continue
        baseline_rank = pd.to_numeric(values["baseline_rank"], errors="coerce")
        observe_rank = pd.to_numeric(values["sec_pit_observe_rank"], errors="coerce")
        if baseline_rank.isna().all() or observe_rank.isna().all():
            continue
        baseline_row = values.loc[baseline_rank.idxmin()]
        observe_row = values.loc[observe_rank.idxmin()]
        baseline_label = _float_or_nan(baseline_row.get(label_column))
        observe_label = _float_or_nan(observe_row.get(label_column))
        if pd.notna(baseline_label) and pd.notna(observe_label):
            deltas.append(float(observe_label - baseline_label))
    return float(np.mean(deltas)) if deltas else np.nan


def _bucket_record(frame: pd.DataFrame, bucket: str) -> dict[str, Any]:
    if frame.empty or "bucket" not in frame.columns:
        return {}
    rows = frame.loc[frame["bucket"].astype(str) == bucket]
    if rows.empty:
        return {}
    return rows.iloc[0].to_dict()


def _first_row_for_feature(frame: pd.DataFrame, feature_id: str) -> dict[str, Any]:
    if frame.empty or "feature_id" not in frame.columns:
        return {}
    rows = frame.loc[frame["feature_id"].astype(str) == feature_id]
    if rows.empty:
        return {}
    return rows.iloc[0].to_dict()


def _artifact_path(summary: dict[str, Any], key: str, default: Path) -> Path:
    outputs = summary.get("output_artifacts") if isinstance(summary, dict) else {}
    raw = outputs.get(key) if isinstance(outputs, dict) else None
    if not raw:
        return default
    path = Path(str(raw))
    return path if path.is_absolute() else PROJECT_ROOT / path


def _latest_dated_path(
    root: Path,
    prefix: str,
    suffix: str,
    end: date | None,
) -> Path:
    default_end = end or date.max
    default_path = root / f"{prefix}{default_end.isoformat()}{suffix}"
    if not root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"{prefix}*{suffix}"):
        raw_date = path.stem.removeprefix(prefix)
        parsed = _parse_date(raw_date)
        if parsed is not None and (end is None or parsed <= end):
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _baseline_path(root: Path, end: date) -> Path:
    if root.is_file():
        return root
    if not root.exists():
        return root / "scores_daily.csv"
    for path in (
        root / "scores_daily.csv",
        root / f"scores_daily_{end.isoformat()}.csv",
        root / f"daily_score_baseline_{end.isoformat()}.csv",
        root / f"baseline_scores_{end.isoformat()}.csv",
    ):
        if path.exists():
            return path
    dated: list[tuple[date, Path]] = []
    for pattern in ("scores_*.csv", "daily_score_*.csv", "baseline_scores_*.csv"):
        for path in root.glob(pattern):
            parsed = _date_from_stem(path.stem)
            if parsed is not None and parsed <= end:
                dated.append((parsed, path))
    if dated:
        return max(dated, key=lambda item: item[0])[1]
    csv_files = sorted(root.glob("*.csv"))
    return csv_files[0] if csv_files else root / "scores_daily.csv"


def _is_default_baseline_score_dir(root: Path) -> bool:
    path = Path(root)
    normalized = path if path.is_absolute() else PROJECT_ROOT / path
    return normalized.resolve() == DEFAULT_BASELINE_SCORE_DIR.resolve()


def _is_isolated_output_dir(path: Path) -> bool:
    normalized = path.resolve()
    parts = tuple(part.lower() for part in normalized.parts)
    for index in range(len(parts) - 1):
        if parts[index] == "outputs" and parts[index + 1] == "sec_pit_shadow_observe":
            return True
    return False


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


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists() or not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def _write_csv(frame: pd.DataFrame, path: Path, columns: tuple[str, ...]) -> None:
    output = frame.copy() if not frame.empty else _empty_frame(columns)
    for column in columns:
        if column not in output.columns:
            output[column] = ""
    path.parent.mkdir(parents=True, exist_ok=True)
    output.loc[:, list(columns)].to_csv(path, index=False)


def _empty_frame(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _mapping_value(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _bool_value(value: object, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index)
    return pd.to_numeric(frame[column], errors="coerce")


def _first_existing_column(frame: pd.DataFrame, columns: tuple[str, ...]) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None


def _summary_date(summary: dict[str, Any], key: str) -> date | None:
    if not isinstance(summary, dict):
        return None
    return _parse_date(str(summary.get(key) or ""))


def _date_from_stem(stem: str) -> date | None:
    for part in stem.split("_"):
        parsed = _parse_date(part)
        if parsed is not None:
            return parsed
    return None


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _score_delta_series(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype="float64")
    return pd.to_numeric(frame["sec_pit_observe_score"], errors="coerce") - pd.to_numeric(
        frame["baseline_score"], errors="coerce"
    )


def _mean_or_nan(value: object) -> float:
    values = pd.to_numeric(value, errors="coerce").dropna()
    return float(values.mean()) if not values.empty else np.nan


def _hit_rate(value: object) -> float:
    values = pd.to_numeric(value, errors="coerce").dropna()
    return float((values > 0).mean()) if not values.empty else np.nan


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


def _float_or_nan(value: object) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return np.nan
    return number


def _float_or_default(value: object, default: float) -> float:
    number = _float_or_nan(value)
    return default if pd.isna(number) else number


def _json_number(value: object) -> float | None:
    number = _float_or_nan(value)
    return None if pd.isna(number) else float(number)


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if np.isnan(value) else float(value)
    if isinstance(value, float) and np.isnan(value):
        return None
    return value


def _top_rank_shifts(frame: pd.DataFrame, *, positive: bool) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    rank_delta = pd.to_numeric(frame["rank_delta"], errors="coerce")
    selected = frame.loc[rank_delta > 0] if positive else frame.loc[rank_delta < 0]
    if selected.empty:
        return []
    selected = selected.sort_values(
        ["rank_delta", "ticker"],
        ascending=[not positive, True],
    )
    return [
        {
            "decision_date": str(row.get("decision_date") or ""),
            "ticker": str(row.get("ticker") or ""),
            "rank_delta": _json_number(row.get("rank_delta")),
            "score_delta": _json_number(row.get("score_delta")),
        }
        for row in selected.head(10).to_dict(orient="records")
    ]


def _bucket_summary(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    return [
        {
            "bucket": str(row.get("bucket") or ""),
            "sample_count": int(_float_or_default(row.get("sample_count"), 0.0)),
            "rank_ic_20d": _json_number(row.get("rank_ic_20d")),
            "hit_rate_20d": _json_number(row.get("hit_rate_20d")),
            "interpretation": str(row.get("interpretation") or ""),
        }
        for row in frame.to_dict(orient="records")
    ]


def _dedupe_text(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if normalized and normalized not in seen:
            result.append(normalized)
            seen.add(normalized)
    return result


def _deterministic_generated_at(end: date) -> str:
    return f"{end.isoformat()}T00:00:00+00:00"


def _format_float(value: object) -> str:
    number = _json_number(value)
    return "NA" if number is None else f"{number:.4f}"


def _limitations_text(value: object) -> str:
    if not isinstance(value, list) or not value:
        return "none"
    return "; ".join(str(item) for item in value[:5])


def _rank_shift_lines(frame: pd.DataFrame, *, positive: bool) -> list[str]:
    shifts = _top_rank_shifts(frame, positive=positive)
    if not shifts:
        return ["  - none"]
    return [
        (
            f"  - `{item['ticker']}` {item['decision_date']}: "
            f"rank_delta={_format_float(item['rank_delta'])}, "
            f"score_delta={_format_float(item['score_delta'])}"
        )
        for item in shifts[:5]
    ]


def _bucket_lines(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["- no bucket comparison rows available"]
    lines = [
        "| bucket | sample_count | avg_score_delta | rank_ic_20d | hit_rate_20d | interpretation |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for row in frame.to_dict(orient="records"):
        lines.append(
            "| "
            f"`{row.get('bucket', '')}` | "
            f"{int(_float_or_default(row.get('sample_count'), 0.0))} | "
            f"{_format_float(row.get('avg_score_delta'))} | "
            f"{_format_float(row.get('rank_ic_20d'))} | "
            f"{_format_float(row.get('hit_rate_20d'))} | "
            f"`{row.get('interpretation', '')}` |"
        )
    return lines


def _safety_failed_lines(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return []
    failed = frame.loc[frame["status"].astype(str) == "FAIL"]
    if failed.empty:
        return []
    lines = ["- failed / warning checks:"]
    for row in failed.to_dict(orient="records"):
        lines.append(
            "  - "
            f"`{row.get('check_name', '')}` severity={row.get('severity', '')}; "
            f"actual={row.get('actual', '')}; action={row.get('recommended_action', '')}"
        )
    return lines


def _monitoring_lines(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["- no monitoring plan rows available"]
    lines: list[str] = []
    for row in frame.to_dict(orient="records"):
        lines.append(
            "- "
            f"`{row.get('monitoring_metric', '')}`: current={row.get('current_value', '')}, "
            f"status={row.get('status', '')}, rollback={row.get('rollback_threshold', '')}"
        )
    return lines


def _recommendation(summary: dict[str, Any]) -> str:
    status = str(summary.get("shadow_status") or "")
    if status == "OK":
        return "continue observe-only monitoring; do not promote automatically."
    if status == "FAILED_SAFETY_CHECK":
        return "stop; fix failed safety checks before any observe-only output."
    return "needs manual review before interpreting this lane."
