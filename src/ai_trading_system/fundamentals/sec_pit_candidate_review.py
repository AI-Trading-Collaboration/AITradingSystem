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

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.fundamentals.sec_pit_baseline_comparison import (
    DEFAULT_SEC_PIT_BASELINE_COMPARISON_OUTPUT_DIR,
    DEFAULT_SEC_PIT_EVALUATION_DIR,
)
from ai_trading_system.fundamentals.sec_pit_real_run_diagnostics import (
    DEFAULT_SEC_PIT_DIAGNOSTICS_OUTPUT_DIR,
)

SEC_PIT_CANDIDATE_REVIEW_TASK_ID = "TRADING-043"
SEC_PIT_CANDIDATE_REVIEW_REPORT_TYPE = "sec_pit_candidate_review"
SEC_PIT_CANDIDATE_REVIEW_PRODUCTION_EFFECT = "none"
DEFAULT_SEC_PIT_CANDIDATE_REVIEW_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "sec_pit_candidate_review"

REVIEW_STATUSES: tuple[str, ...] = (
    "OK",
    "LIMITED_MISSING_ARTIFACTS",
    "INSUFFICIENT_EVIDENCE",
    "FAILED_VALIDATION",
)
PROPOSAL_STATUSES: tuple[str, ...] = (
    "READY_FOR_MANUAL_REVIEW",
    "KEEP_RESEARCH_ONLY",
    "INSUFFICIENT_EVIDENCE",
    "REJECT",
)

CANDIDATE_EVIDENCE_COLUMNS: tuple[str, ...] = (
    "feature_id",
    "metric_id",
    "rank_ic_20d",
    "ic_20d",
    "hit_rate_20d",
    "coverage_ratio",
    "data_quality_score",
    "stability_score",
    "drawdown_improvement_20d",
    "incremental_alpha_20d",
    "sample_count",
    "valid_ticker_count",
    "pit_grade",
    "manual_review_required",
    "production_effect",
    "recommendation",
    "blocking_reasons",
    "supporting_reasons",
)

BY_TICKER_COLUMNS: tuple[str, ...] = (
    "ticker",
    "feature_id",
    "sample_count",
    "avg_feature_value",
    "avg_forward_return_20d",
    "avg_relative_return_vs_QQQ_20d",
    "avg_max_drawdown_forward_20d",
    "rank_ic_20d",
    "hit_rate_20d",
    "positive_contribution_count",
    "negative_contribution_count",
    "net_contribution",
    "interpretation",
)

BY_PERIOD_COLUMNS: tuple[str, ...] = (
    "period_bucket",
    "feature_id",
    "sample_count",
    "rank_ic_20d",
    "hit_rate_20d",
    "avg_forward_return_20d",
    "avg_relative_return_vs_QQQ_20d",
    "avg_max_drawdown_forward_20d",
    "incremental_alpha_20d",
    "interpretation",
)

BASELINE_OVERLAP_COLUMNS: tuple[str, ...] = (
    "feature_id",
    "baseline_signal",
    "correlation",
    "rank_correlation",
    "overlap_sample_count",
    "overlap_interpretation",
    "redundancy_risk",
)

SHADOW_PROPOSAL_COLUMNS: tuple[str, ...] = (
    "feature_id",
    "metric_id",
    "proposal_status",
    "suggested_observe_only_weight",
    "max_allowed_initial_weight",
    "review_required",
    "production_effect",
    "rationale",
    "risk_notes",
    "minimum_monitoring_days",
    "rollback_condition",
)

# Review-only concentration flag. It never mutates production or shadow weights;
# it highlights when one ticker dominates candidate evidence for manual review.
CONCENTRATION_ABS_CONTRIBUTION_SHARE_WARN = 0.50

# Review-only redundancy labels. These do not gate production behavior; they
# classify whether a candidate appears to duplicate baseline score information.
HIGH_REDUNDANCY_RANK_CORRELATION = 0.70
MEDIUM_REDUNDANCY_RANK_CORRELATION = 0.40

# The review proposal asks for at least one full 20D forward-label observation
# cycle before any future owner decision about observe-only shadow iteration.
MINIMUM_OBSERVE_ONLY_MONITORING_DAYS = 20


@dataclass(frozen=True)
class SecPitCandidateReviewArtifacts:
    status: str
    summary_json_path: Path
    summary_markdown_path: Path
    candidate_evidence_path: Path
    by_ticker_path: Path
    by_period_path: Path
    baseline_overlap_path: Path
    shadow_proposal_path: Path

    @property
    def json_path(self) -> Path:
        return self.summary_json_path

    @property
    def markdown_path(self) -> Path:
        return self.summary_markdown_path


@dataclass(frozen=True)
class _ReviewInputs:
    evaluation_summary_path: Path
    feature_effectiveness_path: Path
    signal_attribution_path: Path
    shadow_candidate_weights_path: Path
    comparison_summary_path: Path
    decision_impact_path: Path
    incremental_alpha_path: Path
    diagnostics_summary_path: Path
    candidate_sensitivity_path: Path
    label_coverage_path: Path
    evaluation_summary: dict[str, Any]
    comparison_summary: dict[str, Any]
    diagnostics_summary: dict[str, Any]
    feature_effectiveness: pd.DataFrame
    signal_attribution: pd.DataFrame
    shadow_candidate_weights: pd.DataFrame
    decision_impact: pd.DataFrame
    incremental_alpha: pd.DataFrame
    candidate_sensitivity: pd.DataFrame
    label_coverage: pd.DataFrame

    @property
    def missing_required_artifacts(self) -> list[str]:
        missing: list[str] = []
        if not self.evaluation_summary:
            missing.append("evaluation_summary")
        if self.feature_effectiveness.empty:
            missing.append("feature_effectiveness")
        if self.signal_attribution.empty:
            missing.append("signal_attribution")
        if not self.comparison_summary:
            missing.append("comparison_summary")
        if not self.diagnostics_summary:
            missing.append("diagnostics_summary")
        return missing


def run_sec_pit_candidate_review(
    *,
    start: date | None = None,
    end: date | None = None,
    evaluation_dir: Path = DEFAULT_SEC_PIT_EVALUATION_DIR,
    comparison_dir: Path = DEFAULT_SEC_PIT_BASELINE_COMPARISON_OUTPUT_DIR,
    diagnostics_dir: Path = DEFAULT_SEC_PIT_DIAGNOSTICS_OUTPUT_DIR,
    candidate_features: list[str] | None = None,
    output_dir: Path = DEFAULT_SEC_PIT_CANDIDATE_REVIEW_OUTPUT_DIR,
    latest: bool = False,
) -> SecPitCandidateReviewArtifacts:
    output_dir.mkdir(parents=True, exist_ok=True)
    inputs = _load_review_inputs(
        evaluation_dir=evaluation_dir,
        comparison_dir=comparison_dir,
        diagnostics_dir=diagnostics_dir,
        end=end if not latest else end,
    )
    inferred_start = (
        _summary_date(inputs.evaluation_summary, "start_date")
        or _summary_date(inputs.comparison_summary, "start_date")
        or _summary_date(inputs.diagnostics_summary, "start_date")
    )
    inferred_end = (
        _summary_date(inputs.evaluation_summary, "end_date")
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
    summary_json_path = output_dir / f"sec_pit_candidate_review_summary_{suffix}.json"
    summary_markdown_path = output_dir / f"sec_pit_candidate_review_summary_{suffix}.md"
    evidence_path = output_dir / f"sec_pit_candidate_evidence_{suffix}.csv"
    by_ticker_path = output_dir / f"sec_pit_candidate_by_ticker_{suffix}.csv"
    by_period_path = output_dir / f"sec_pit_candidate_by_period_{suffix}.csv"
    overlap_path = output_dir / f"sec_pit_candidate_overlap_with_baseline_{suffix}.csv"
    proposal_path = output_dir / f"sec_pit_candidate_shadow_proposal_{suffix}.csv"

    try:
        requested_candidates = _candidate_feature_ids(
            requested=candidate_features,
            feature_effectiveness=inputs.feature_effectiveness,
            shadow_candidate_weights=inputs.shadow_candidate_weights,
            candidate_sensitivity=inputs.candidate_sensitivity,
        )
        signal_frame = _candidate_signal_frame(
            attribution=inputs.signal_attribution,
            feature_effectiveness=inputs.feature_effectiveness,
            candidate_features=requested_candidates,
            start=start,
            end=end,
        )
        ticker = _candidate_by_ticker(signal_frame)
        period = _candidate_by_period(signal_frame)
        overlap = _baseline_overlap(
            signal_frame=signal_frame,
            decision_impact=inputs.decision_impact,
            candidate_features=requested_candidates,
        )
        evidence = _candidate_evidence(
            feature_effectiveness=inputs.feature_effectiveness,
            candidate_sensitivity=inputs.candidate_sensitivity,
            signal_frame=signal_frame,
            requested_candidates=requested_candidates,
            inputs=inputs,
            ticker_evidence=ticker,
            baseline_overlap=overlap,
        )
        proposal = _shadow_proposal(
            evidence=evidence,
            shadow_candidate_weights=inputs.shadow_candidate_weights,
            ticker_evidence=ticker,
            baseline_overlap=overlap,
        )
        status = _review_status(inputs, evidence)
        limitations = _review_limitations(
            inputs=inputs,
            evidence=evidence,
            overlap=overlap,
            ticker_evidence=ticker,
        )
    except (KeyError, ValueError, pd.errors.ParserError) as exc:
        requested_candidates = _dedupe_feature_ids(candidate_features or [])
        evidence = _empty_candidate_evidence(requested_candidates, str(exc))
        ticker = _empty_frame(BY_TICKER_COLUMNS)
        period = _empty_frame(BY_PERIOD_COLUMNS)
        overlap = _baseline_overlap(
            signal_frame=_empty_candidate_signal_frame(),
            decision_impact=pd.DataFrame(),
            candidate_features=requested_candidates,
        )
        proposal = _shadow_proposal(
            evidence=evidence,
            shadow_candidate_weights=pd.DataFrame(),
            ticker_evidence=ticker,
            baseline_overlap=overlap,
        )
        status = "FAILED_VALIDATION"
        limitations = [f"Input artifact validation failed: {exc}"]

    _write_csv(evidence, evidence_path, CANDIDATE_EVIDENCE_COLUMNS)
    _write_csv(ticker, by_ticker_path, BY_TICKER_COLUMNS)
    _write_csv(period, by_period_path, BY_PERIOD_COLUMNS)
    _write_csv(overlap, overlap_path, BASELINE_OVERLAP_COLUMNS)
    _write_csv(proposal, proposal_path, SHADOW_PROPOSAL_COLUMNS)

    summary = _summary_payload(
        status=status,
        start=start,
        end=end,
        candidate_features=requested_candidates,
        primary_candidate=requested_candidates[0] if requested_candidates else "",
        diagnostics_summary=inputs.diagnostics_summary,
        evidence=evidence,
        proposal=proposal,
        limitations=limitations,
        inputs=inputs,
        artifacts={
            "summary_json": summary_json_path,
            "summary_markdown": summary_markdown_path,
            "candidate_evidence_csv": evidence_path,
            "candidate_by_ticker_csv": by_ticker_path,
            "candidate_by_period_csv": by_period_path,
            "candidate_overlap_with_baseline_csv": overlap_path,
            "candidate_shadow_proposal_csv": proposal_path,
        },
    )
    summary_json_path.write_text(
        json.dumps(_json_value(summary), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_markdown_path.write_text(
        render_sec_pit_candidate_review_summary(
            summary=summary,
            evidence=evidence,
            ticker=ticker,
            period=period,
            overlap=overlap,
            proposal=proposal,
        ),
        encoding="utf-8",
    )
    return SecPitCandidateReviewArtifacts(
        status=status,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        candidate_evidence_path=evidence_path,
        by_ticker_path=by_ticker_path,
        by_period_path=by_period_path,
        baseline_overlap_path=overlap_path,
        shadow_proposal_path=proposal_path,
    )


def render_sec_pit_candidate_review_summary(
    *,
    summary: dict[str, Any],
    evidence: pd.DataFrame,
    ticker: pd.DataFrame,
    period: pd.DataFrame,
    overlap: pd.DataFrame,
    proposal: pd.DataFrame,
) -> str:
    ready = int(summary.get("ready_for_manual_review_count") or 0)
    primary = str(summary.get("primary_candidate") or "")
    candidate_features = ", ".join(summary.get("candidate_features") or []) or "none"
    outputs = (
        summary.get("output_artifacts") if isinstance(summary.get("output_artifacts"), dict) else {}
    )
    artifact_lines = [f"- {key}: {value}" for key, value in sorted(outputs.items())]
    strongest = _ticker_lines(
        (
            ticker.sort_values(["net_contribution", "ticker"], ascending=[False, True])
            if not ticker.empty
            else ticker
        ),
        "strongest tickers",
    )
    weakest = _ticker_lines(
        (
            ticker.sort_values(["net_contribution", "ticker"], ascending=[True, True])
            if not ticker.empty
            else ticker
        ),
        "weakest tickers",
    )
    stable_periods = _period_lines(
        (
            period.sort_values(["incremental_alpha_20d", "period_bucket"], ascending=[False, True])
            if not period.empty
            else period
        ),
        "stable periods",
    )
    weak_periods = _period_lines(
        (
            period.sort_values(["incremental_alpha_20d", "period_bucket"], ascending=[True, True])
            if not period.empty
            else period
        ),
        "weak periods",
    )
    proposal_status = (
        str(proposal.iloc[0]["proposal_status"])
        if not proposal.empty and "proposal_status" in proposal.columns
        else "INSUFFICIENT_EVIDENCE"
    )
    lines = [
        "# SEC PIT Shadow Candidate Review",
        "",
        "## Metadata",
        f"- generated_at: {summary['generated_at']}",
        f"- start_date: {summary['start_date']}",
        f"- end_date: {summary['end_date']}",
        f"- candidate features: {candidate_features}",
        *artifact_lines,
        "",
        "## Executive Summary",
        (f"- ready for manual review: {'yes' if ready else 'no'} " f"({ready} candidate(s))."),
        (
            "- no automatic promotion: this review is evidence-only; production weights and "
            "active shadow weights remain unchanged."
        ),
        f"- primary candidate: {primary or 'none'}",
        f"- diagnostics_status: {summary.get('diagnostics_status', 'UNKNOWN')}",
        f"- provenance_complete: {summary.get('provenance_complete', False)}",
        f"- drawdown_label_coverage: {_format_float(summary.get('drawdown_label_coverage'))}",
        "",
        "## Candidate Evidence",
    ]
    lines.extend(_evidence_table_lines(evidence))
    lines.extend(
        [
            "",
            "## By-Ticker Analysis",
            "- strongest tickers:",
            *strongest,
            "- weakest tickers:",
            *weakest,
            f"- concentration risk: {_concentration_text(ticker)}",
            "",
            "## By-Period Analysis",
            "- stable periods:",
            *stable_periods,
            "- weak periods:",
            *weak_periods,
            f"- regime dependency: {_period_dependency_text(period)}",
            "",
            "## Baseline Overlap",
        ]
    )
    lines.extend(_overlap_lines(overlap))
    lines.extend(
        [
            "",
            "## Shadow Proposal",
        ]
    )
    lines.extend(_proposal_lines(proposal))
    lines.extend(
        [
            "",
            "## Recommendation",
            f"- {proposal_status}",
            "",
            "## Manual Review Checklist",
            "- Confirm the candidate evidence comes only from TRADING-040/041/042 artifacts.",
            (
                "- Confirm `production_effect=none` and no production or active shadow "
                "weights changed."
            ),
            "- Review SEC PIT provenance, `pit_grade`, label coverage, and baseline redundancy.",
            "- Decide manually whether observe-only shadow iteration is appropriate.",
            (
                "- Define monitoring owner, monitoring window, and rollback condition "
                "before any shadow iteration."
            ),
        ]
    )
    limitations = summary.get("limitations") if isinstance(summary.get("limitations"), list) else []
    if limitations:
        lines.extend(["", "## Limitations"])
        lines.extend(f"- {item}" for item in limitations)
    return "\n".join(lines) + "\n"


def _load_review_inputs(
    *,
    evaluation_dir: Path,
    comparison_dir: Path,
    diagnostics_dir: Path,
    end: date | None,
) -> _ReviewInputs:
    evaluation_summary_path = _latest_dated_path(
        evaluation_dir,
        "sec_pit_evaluation_summary_",
        ".json",
        end,
    )
    comparison_summary_path = _latest_dated_path(
        comparison_dir,
        "sec_pit_baseline_comparison_summary_",
        ".json",
        end,
    )
    diagnostics_summary_path = _latest_dated_path(
        diagnostics_dir,
        "sec_pit_real_run_diagnostics_",
        ".json",
        end,
    )
    evaluation_summary = _read_json_object(evaluation_summary_path)
    comparison_summary = _read_json_object(comparison_summary_path)
    diagnostics_summary = _read_json_object(diagnostics_summary_path)
    resolved_end = (
        _summary_date(evaluation_summary, "end_date")
        or _summary_date(comparison_summary, "end_date")
        or _summary_date(diagnostics_summary, "end_date")
        or end
    )
    feature_effectiveness_path = _artifact_path(
        evaluation_summary,
        "feature_effectiveness_csv",
        _latest_dated_path(
            evaluation_dir,
            "sec_pit_feature_effectiveness_",
            ".csv",
            resolved_end,
        ),
    )
    signal_attribution_path = _artifact_path(
        evaluation_summary,
        "signal_attribution_csv",
        _latest_dated_path(evaluation_dir, "sec_pit_signal_attribution_", ".csv", resolved_end),
    )
    shadow_candidate_weights_path = _artifact_path(
        evaluation_summary,
        "shadow_candidate_weights_csv",
        _latest_dated_path(
            evaluation_dir,
            "sec_pit_shadow_candidate_weights_",
            ".csv",
            resolved_end,
        ),
    )
    decision_impact_path = _artifact_path(
        comparison_summary,
        "decision_impact_csv",
        _latest_dated_path(comparison_dir, "sec_pit_decision_impact_", ".csv", resolved_end),
    )
    incremental_alpha_path = _artifact_path(
        comparison_summary,
        "incremental_alpha_csv",
        _latest_dated_path(comparison_dir, "sec_pit_incremental_alpha_", ".csv", resolved_end),
    )
    candidate_sensitivity_path = _artifact_path(
        diagnostics_summary,
        "candidate_sensitivity_csv",
        _latest_dated_path(
            diagnostics_dir,
            "sec_pit_candidate_sensitivity_",
            ".csv",
            resolved_end,
        ),
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
    return _ReviewInputs(
        evaluation_summary_path=evaluation_summary_path,
        feature_effectiveness_path=feature_effectiveness_path,
        signal_attribution_path=signal_attribution_path,
        shadow_candidate_weights_path=shadow_candidate_weights_path,
        comparison_summary_path=comparison_summary_path,
        decision_impact_path=decision_impact_path,
        incremental_alpha_path=incremental_alpha_path,
        diagnostics_summary_path=diagnostics_summary_path,
        candidate_sensitivity_path=candidate_sensitivity_path,
        label_coverage_path=label_coverage_path,
        evaluation_summary=evaluation_summary,
        comparison_summary=comparison_summary,
        diagnostics_summary=diagnostics_summary,
        feature_effectiveness=_read_csv_or_empty(feature_effectiveness_path),
        signal_attribution=_read_csv_or_empty(signal_attribution_path),
        shadow_candidate_weights=_read_csv_or_empty(shadow_candidate_weights_path),
        decision_impact=_read_csv_or_empty(decision_impact_path),
        incremental_alpha=_read_csv_or_empty(incremental_alpha_path),
        candidate_sensitivity=_read_csv_or_empty(candidate_sensitivity_path),
        label_coverage=_read_csv_or_empty(label_coverage_path),
    )


def _candidate_feature_ids(
    *,
    requested: list[str] | None,
    feature_effectiveness: pd.DataFrame,
    shadow_candidate_weights: pd.DataFrame,
    candidate_sensitivity: pd.DataFrame,
) -> list[str]:
    if requested:
        return _dedupe_feature_ids(requested)
    result: list[str] = []
    if not shadow_candidate_weights.empty and "feature_id" in shadow_candidate_weights.columns:
        frame = shadow_candidate_weights.copy()
        suggested = _numeric_column(frame, "suggested_shadow_weight").abs()
        delta = _numeric_column(frame, "weight_delta").abs()
        selected = frame.loc[(suggested > 0) | (delta > 0)]
        result.extend(selected["feature_id"].astype(str).tolist())
    if not feature_effectiveness.empty and {"feature_id", "recommendation"}.issubset(
        feature_effectiveness.columns
    ):
        selected = feature_effectiveness.loc[
            feature_effectiveness["recommendation"].astype(str) == "PROMOTE_TO_SHADOW"
        ]
        result.extend(selected["feature_id"].astype(str).tolist())
    if not candidate_sensitivity.empty and "feature_id" in candidate_sensitivity.columns:
        selected = candidate_sensitivity.loc[
            _first_existing(candidate_sensitivity, ("current_recommendation",)).isin(
                {"PROMOTE_TO_SHADOW"}
            )
            | _first_existing(
                candidate_sensitivity,
                ("hypothetical_recommendation_if_provenance_fixed",),
            ).isin({"PROMOTE_TO_SHADOW"})
        ]
        result.extend(selected["feature_id"].astype(str).tolist())
    if not result and not feature_effectiveness.empty and "feature_id" in feature_effectiveness:
        result.extend(feature_effectiveness["feature_id"].astype(str).tolist())
    return _dedupe_feature_ids(result)


def _candidate_signal_frame(
    *,
    attribution: pd.DataFrame,
    feature_effectiveness: pd.DataFrame,
    candidate_features: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    columns = [
        "decision_date",
        "ticker",
        "feature_id",
        "metric_id",
        "feature_value",
        "normalized_value",
        "contribution",
        "signal_score",
        "forward_return_20d",
        "relative_return_vs_QQQ_20d",
        "max_drawdown_forward_20d",
        "pit_grade",
    ]
    if attribution.empty or "feature_id" not in attribution.columns:
        return pd.DataFrame(columns=columns)
    candidates = set(candidate_features)
    frame = attribution.loc[attribution["feature_id"].astype(str).isin(candidates)].copy()
    if frame.empty:
        return pd.DataFrame(columns=columns)
    frame["decision_date"] = pd.to_datetime(frame["decision_date"], errors="coerce").dt.date
    frame = frame.loc[
        frame["decision_date"].notna()
        & frame["decision_date"].map(lambda value: start <= value <= end)
    ].copy()
    if frame.empty:
        return pd.DataFrame(columns=columns)
    rank_ic = _rank_ic_by_feature(feature_effectiveness)
    frame["feature_value"] = _numeric_column(frame, "feature_value")
    frame["normalized_value"] = _numeric_column(frame, "normalized_value")
    frame["contribution"] = _numeric_column(frame, "contribution").fillna(0.0)
    frame["forward_return_20d"] = _numeric_column(frame, "forward_return_20d")
    frame["relative_return_vs_QQQ_20d"] = _numeric_column(frame, "relative_return_vs_QQQ_20d")
    frame["max_drawdown_forward_20d"] = _numeric_column(frame, "max_drawdown_forward_20d")
    frame["signal_score"] = [
        _signal_score(feature, normalized, rank_ic)
        for feature, normalized in zip(
            frame["feature_id"].astype(str),
            frame["normalized_value"],
            strict=False,
        )
    ]
    for column in columns:
        if column not in frame.columns:
            frame[column] = ""
    return frame.loc[:, columns].sort_values(["feature_id", "decision_date", "ticker"])


def _candidate_evidence(
    *,
    feature_effectiveness: pd.DataFrame,
    candidate_sensitivity: pd.DataFrame,
    signal_frame: pd.DataFrame,
    requested_candidates: list[str],
    inputs: _ReviewInputs,
    ticker_evidence: pd.DataFrame,
    baseline_overlap: pd.DataFrame,
) -> pd.DataFrame:
    if not requested_candidates:
        return _empty_frame(CANDIDATE_EVIDENCE_COLUMNS)
    feature_rows = (
        feature_effectiveness.fillna("").set_index("feature_id", drop=False)
        if not feature_effectiveness.empty and "feature_id" in feature_effectiveness.columns
        else pd.DataFrame()
    )
    sensitivity_rows = (
        candidate_sensitivity.fillna("").set_index("feature_id", drop=False)
        if not candidate_sensitivity.empty and "feature_id" in candidate_sensitivity.columns
        else pd.DataFrame()
    )
    records: list[dict[str, Any]] = []
    for feature_id in requested_candidates:
        if feature_rows.empty or feature_id not in feature_rows.index:
            records.append(
                _missing_candidate_record(
                    feature_id,
                    "candidate_feature_missing_in_evaluation",
                )
            )
            continue
        rows = (
            feature_rows.loc[[feature_id]] if feature_id in feature_rows.index else pd.DataFrame()
        )
        for row in rows.to_dict(orient="records"):
            signal = signal_frame.loc[signal_frame["feature_id"].astype(str) == feature_id]
            incremental_alpha, drawdown_improvement = _candidate_incremental_metrics(signal)
            sensitivity = (
                sensitivity_rows.loc[feature_id].to_dict()
                if not sensitivity_rows.empty and feature_id in sensitivity_rows.index
                else {}
            )
            blocking = _blocking_reasons(row, sensitivity, inputs)
            concentration = _feature_concentration_risk(ticker_evidence, feature_id)
            if concentration:
                blocking = _join_reason(blocking, concentration)
            overlap_risk = _feature_overlap_risk(baseline_overlap, feature_id)
            proposal_status = _proposal_status(row, inputs, blocking)
            records.append(
                {
                    "feature_id": feature_id,
                    "metric_id": str(row.get("metric_id") or ""),
                    "rank_ic_20d": _json_number(row.get("rank_ic_20d")),
                    "ic_20d": _json_number(row.get("ic_20d")),
                    "hit_rate_20d": _json_number(row.get("hit_rate_20d")),
                    "coverage_ratio": _json_number(row.get("coverage_ratio")),
                    "data_quality_score": _json_number(row.get("data_quality_score")),
                    "stability_score": _json_number(row.get("stability_score")),
                    "drawdown_improvement_20d": _json_number(drawdown_improvement),
                    "incremental_alpha_20d": _json_number(incremental_alpha),
                    "sample_count": int(_float_or_zero(row.get("sample_count"))),
                    "valid_ticker_count": int(_float_or_zero(row.get("valid_ticker_count"))),
                    "pit_grade": str(row.get("pit_grade") or _dominant_value(signal, "pit_grade")),
                    "manual_review_required": True,
                    "production_effect": SEC_PIT_CANDIDATE_REVIEW_PRODUCTION_EFFECT,
                    "recommendation": proposal_status,
                    "blocking_reasons": blocking,
                    "supporting_reasons": _supporting_reasons(
                        row=row,
                        inputs=inputs,
                        overlap_risk=overlap_risk,
                    ),
                }
            )
    return pd.DataFrame(records, columns=list(CANDIDATE_EVIDENCE_COLUMNS)).sort_values(
        ["recommendation", "feature_id", "metric_id"]
    )


def _candidate_by_ticker(signal_frame: pd.DataFrame) -> pd.DataFrame:
    if signal_frame.empty:
        return _empty_frame(BY_TICKER_COLUMNS)
    records: list[dict[str, Any]] = []
    for (feature_id, ticker), group in signal_frame.groupby(["feature_id", "ticker"], sort=True):
        records.append(
            {
                "ticker": str(ticker),
                "feature_id": str(feature_id),
                "sample_count": int(len(group)),
                "avg_feature_value": _mean_or_nan(group["feature_value"]),
                "avg_forward_return_20d": _mean_or_nan(group["forward_return_20d"]),
                "avg_relative_return_vs_QQQ_20d": _mean_or_nan(group["relative_return_vs_QQQ_20d"]),
                "avg_max_drawdown_forward_20d": _mean_or_nan(group["max_drawdown_forward_20d"]),
                "rank_ic_20d": _correlation(group["feature_value"], group["forward_return_20d"]),
                "hit_rate_20d": _hit_rate(group["forward_return_20d"]),
                "positive_contribution_count": int((group["signal_score"] > 0).sum()),
                "negative_contribution_count": int((group["signal_score"] < 0).sum()),
                "net_contribution": _net_contribution(group),
                "interpretation": _ticker_interpretation(group),
            }
        )
    return pd.DataFrame(records, columns=list(BY_TICKER_COLUMNS)).sort_values(
        ["feature_id", "ticker"]
    )


def _candidate_by_period(signal_frame: pd.DataFrame) -> pd.DataFrame:
    if signal_frame.empty:
        return _empty_frame(BY_PERIOD_COLUMNS)
    frame = signal_frame.copy()
    frame["period_bucket"] = pd.to_datetime(frame["decision_date"], errors="coerce").dt.strftime(
        "%Y-%m"
    )
    frame = frame.loc[frame["period_bucket"].astype(str).str.strip().astype(bool)].copy()
    if frame.empty:
        return _empty_frame(BY_PERIOD_COLUMNS)
    records: list[dict[str, Any]] = []
    for (feature_id, bucket), group in frame.groupby(["feature_id", "period_bucket"], sort=True):
        incremental_alpha, _drawdown = _candidate_incremental_metrics(group)
        records.append(
            {
                "period_bucket": str(bucket),
                "feature_id": str(feature_id),
                "sample_count": int(len(group)),
                "rank_ic_20d": _correlation(group["feature_value"], group["forward_return_20d"]),
                "hit_rate_20d": _hit_rate(group["forward_return_20d"]),
                "avg_forward_return_20d": _mean_or_nan(group["forward_return_20d"]),
                "avg_relative_return_vs_QQQ_20d": _mean_or_nan(group["relative_return_vs_QQQ_20d"]),
                "avg_max_drawdown_forward_20d": _mean_or_nan(group["max_drawdown_forward_20d"]),
                "incremental_alpha_20d": incremental_alpha,
                "interpretation": _period_interpretation(group, incremental_alpha),
            }
        )
    return pd.DataFrame(records, columns=list(BY_PERIOD_COLUMNS)).sort_values(
        ["feature_id", "period_bucket"]
    )


def _baseline_overlap(
    *,
    signal_frame: pd.DataFrame,
    decision_impact: pd.DataFrame,
    candidate_features: list[str],
) -> pd.DataFrame:
    if not candidate_features:
        return _empty_frame(BASELINE_OVERLAP_COLUMNS)
    required = {"decision_date", "ticker", "baseline_score"}
    if decision_impact.empty or not required.issubset(decision_impact.columns):
        return pd.DataFrame(
            [
                {
                    "feature_id": feature,
                    "baseline_signal": "baseline_score",
                    "correlation": np.nan,
                    "rank_correlation": np.nan,
                    "overlap_sample_count": 0,
                    "overlap_interpretation": (
                        "overlap_status=LIMITED_BASELINE_FIELDS_MISSING; "
                        "missing baseline_score or decision_date/ticker fields"
                    ),
                    "redundancy_risk": "UNKNOWN",
                }
                for feature in candidate_features
            ],
            columns=list(BASELINE_OVERLAP_COLUMNS),
        )
    if signal_frame.empty:
        return pd.DataFrame(
            [
                {
                    "feature_id": feature,
                    "baseline_signal": "baseline_score",
                    "correlation": np.nan,
                    "rank_correlation": np.nan,
                    "overlap_sample_count": 0,
                    "overlap_interpretation": "overlap_status=INSUFFICIENT_OVERLAP",
                    "redundancy_risk": "UNKNOWN",
                }
                for feature in candidate_features
            ],
            columns=list(BASELINE_OVERLAP_COLUMNS),
        )
    baseline = decision_impact.loc[:, ["decision_date", "ticker", "baseline_score"]].copy()
    baseline["decision_date"] = pd.to_datetime(baseline["decision_date"], errors="coerce").dt.date
    baseline["ticker"] = baseline["ticker"].astype(str).str.upper()
    baseline["baseline_score"] = _numeric_column(baseline, "baseline_score")
    records: list[dict[str, Any]] = []
    for feature in candidate_features:
        signals = signal_frame.loc[signal_frame["feature_id"].astype(str) == feature].copy()
        if signals.empty:
            records.append(
                {
                    "feature_id": feature,
                    "baseline_signal": "baseline_score",
                    "correlation": np.nan,
                    "rank_correlation": np.nan,
                    "overlap_sample_count": 0,
                    "overlap_interpretation": "overlap_status=INSUFFICIENT_OVERLAP",
                    "redundancy_risk": "UNKNOWN",
                }
            )
            continue
        grouped = (
            signals.groupby(["decision_date", "ticker"], sort=True)["signal_score"]
            .sum()
            .reset_index()
            .rename(columns={"signal_score": "candidate_signal"})
        )
        grouped["ticker"] = grouped["ticker"].astype(str).str.upper()
        merged = grouped.merge(baseline, on=["decision_date", "ticker"], how="inner")
        merged = merged.loc[
            merged["candidate_signal"].notna() & merged["baseline_score"].notna()
        ].copy()
        corr = _correlation(merged["candidate_signal"], merged["baseline_score"])
        rank_corr = _rank_correlation(merged["candidate_signal"], merged["baseline_score"])
        records.append(
            {
                "feature_id": feature,
                "baseline_signal": "baseline_score",
                "correlation": corr,
                "rank_correlation": rank_corr,
                "overlap_sample_count": int(len(merged)),
                "overlap_interpretation": _overlap_interpretation(rank_corr, len(merged)),
                "redundancy_risk": _redundancy_risk(rank_corr),
            }
        )
    return pd.DataFrame(records, columns=list(BASELINE_OVERLAP_COLUMNS)).sort_values("feature_id")


def _shadow_proposal(
    *,
    evidence: pd.DataFrame,
    shadow_candidate_weights: pd.DataFrame,
    ticker_evidence: pd.DataFrame,
    baseline_overlap: pd.DataFrame,
) -> pd.DataFrame:
    if evidence.empty:
        return _empty_frame(SHADOW_PROPOSAL_COLUMNS)
    weights = (
        shadow_candidate_weights.fillna("").set_index(["feature_id", "metric_id"], drop=False)
        if not shadow_candidate_weights.empty
        and {"feature_id", "metric_id"}.issubset(shadow_candidate_weights.columns)
        else pd.DataFrame()
    )
    records: list[dict[str, Any]] = []
    for row in evidence.to_dict(orient="records"):
        feature_id = str(row.get("feature_id") or "")
        metric_id = str(row.get("metric_id") or "")
        weight_row: dict[str, Any] = {}
        if not weights.empty and (feature_id, metric_id) in weights.index:
            selected = weights.loc[(feature_id, metric_id)]
            weight_row = (
                selected.iloc[0].to_dict()
                if isinstance(selected, pd.DataFrame)
                else selected.to_dict()
            )
        status = str(row.get("recommendation") or "INSUFFICIENT_EVIDENCE")
        suggested = (
            _float_or_zero(weight_row.get("suggested_shadow_weight"))
            if status == "READY_FOR_MANUAL_REVIEW"
            else 0.0
        )
        max_allowed = max(abs(suggested), abs(_float_or_zero(weight_row.get("weight_delta"))), 0.0)
        concentration = _feature_concentration_risk(ticker_evidence, feature_id)
        redundancy = _feature_overlap_risk(baseline_overlap, feature_id)
        records.append(
            {
                "feature_id": feature_id,
                "metric_id": metric_id,
                "proposal_status": status,
                "suggested_observe_only_weight": suggested,
                "max_allowed_initial_weight": max_allowed,
                "review_required": True,
                "production_effect": SEC_PIT_CANDIDATE_REVIEW_PRODUCTION_EFFECT,
                "rationale": _proposal_rationale(row),
                "risk_notes": _join_reason(
                    _join_reason(str(row.get("blocking_reasons") or ""), concentration),
                    redundancy,
                ),
                "minimum_monitoring_days": MINIMUM_OBSERVE_ONLY_MONITORING_DAYS,
                "rollback_condition": (
                    "Stop observe-only iteration if provenance/data quality regresses, "
                    "if production_effect is not none, or if monitored 20D evidence "
                    "contradicts the review packet."
                ),
            }
        )
    return pd.DataFrame(records, columns=list(SHADOW_PROPOSAL_COLUMNS)).sort_values(
        ["proposal_status", "feature_id", "metric_id"]
    )


def _summary_payload(
    *,
    status: str,
    start: date,
    end: date,
    candidate_features: list[str],
    primary_candidate: str,
    diagnostics_summary: dict[str, Any],
    evidence: pd.DataFrame,
    proposal: pd.DataFrame,
    limitations: list[str],
    inputs: _ReviewInputs,
    artifacts: dict[str, Path],
) -> dict[str, Any]:
    proposal_counts = (
        Counter(proposal["proposal_status"].astype(str)) if not proposal.empty else Counter()
    )
    diagnostics_status = str(diagnostics_summary.get("diagnostics_status") or "UNKNOWN")
    provenance = (
        diagnostics_summary.get("provenance") if isinstance(diagnostics_summary, dict) else {}
    )
    labels = diagnostics_summary.get("labels") if isinstance(diagnostics_summary, dict) else {}
    return {
        "schema_version": "1.0",
        "report_type": SEC_PIT_CANDIDATE_REVIEW_REPORT_TYPE,
        "task_id": SEC_PIT_CANDIDATE_REVIEW_TASK_ID,
        "generated_at": _deterministic_generated_at(end),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "review_status": status,
        "candidate_count": int(len(evidence)),
        "ready_for_manual_review_count": int(proposal_counts.get("READY_FOR_MANUAL_REVIEW", 0)),
        "keep_research_only_count": int(proposal_counts.get("KEEP_RESEARCH_ONLY", 0)),
        "insufficient_evidence_count": int(proposal_counts.get("INSUFFICIENT_EVIDENCE", 0)),
        "reject_count": int(proposal_counts.get("REJECT", 0)),
        "top_candidates": _top_candidates(evidence, proposal),
        "primary_candidate": primary_candidate,
        "candidate_features": list(candidate_features),
        "diagnostics_status": diagnostics_status,
        "provenance_complete": _provenance_complete(provenance),
        "drawdown_label_coverage": _json_number(
            labels.get("max_drawdown_forward_20d_coverage") if isinstance(labels, dict) else None
        )
        or 0.0,
        "limitations": limitations,
        "manual_review_required": True,
        "production_effect": SEC_PIT_CANDIDATE_REVIEW_PRODUCTION_EFFECT,
        "artifact_paths": {
            "evaluation_summary": str(inputs.evaluation_summary_path),
            "feature_effectiveness": str(inputs.feature_effectiveness_path),
            "signal_attribution": str(inputs.signal_attribution_path),
            "shadow_candidate_weights": str(inputs.shadow_candidate_weights_path),
            "comparison_summary": str(inputs.comparison_summary_path),
            "decision_impact": str(inputs.decision_impact_path),
            "incremental_alpha": str(inputs.incremental_alpha_path),
            "diagnostics_summary": str(inputs.diagnostics_summary_path),
            "candidate_sensitivity": str(inputs.candidate_sensitivity_path),
            "label_coverage": str(inputs.label_coverage_path),
        },
        "input_checksums": {
            "evaluation_summary_sha256": _file_sha256(inputs.evaluation_summary_path),
            "feature_effectiveness_sha256": _file_sha256(inputs.feature_effectiveness_path),
            "signal_attribution_sha256": _file_sha256(inputs.signal_attribution_path),
            "comparison_summary_sha256": _file_sha256(inputs.comparison_summary_path),
            "diagnostics_summary_sha256": _file_sha256(inputs.diagnostics_summary_path),
        },
        "output_artifacts": {key: str(value) for key, value in artifacts.items()},
        "safety": {
            "manual_review_required": True,
            "production_effect": SEC_PIT_CANDIDATE_REVIEW_PRODUCTION_EFFECT,
            "production_weights_modified": False,
            "active_shadow_weights_modified": False,
            "review_only": True,
        },
    }


def _review_status(inputs: _ReviewInputs, evidence: pd.DataFrame) -> str:
    if inputs.missing_required_artifacts:
        return "LIMITED_MISSING_ARTIFACTS"
    if evidence.empty:
        return "INSUFFICIENT_EVIDENCE"
    if set(evidence["recommendation"].astype(str)) == {"INSUFFICIENT_EVIDENCE"}:
        return "INSUFFICIENT_EVIDENCE"
    return "OK"


def _review_limitations(
    *,
    inputs: _ReviewInputs,
    evidence: pd.DataFrame,
    overlap: pd.DataFrame,
    ticker_evidence: pd.DataFrame,
) -> list[str]:
    limitations = [
        "SEC reconstructed PIT remains B-grade filing-time PIT, not strict vendor archive PIT.",
        (
            "Candidate review is evidence-only and does not modify production or active "
            "shadow weights."
        ),
    ]
    for artifact in inputs.missing_required_artifacts:
        limitations.append(f"Missing or empty input artifact: {artifact}.")
    diagnostics_status = str(inputs.diagnostics_summary.get("diagnostics_status") or "")
    if diagnostics_status and diagnostics_status != "OK":
        limitations.append(f"Diagnostics status is {diagnostics_status}.")
    provenance = inputs.diagnostics_summary.get("provenance")
    if isinstance(provenance, dict) and not _provenance_complete(provenance):
        limitations.append("Diagnostics does not show complete SEC PIT provenance.")
    if _overlap_limited(overlap):
        limitations.append(
            "Baseline overlap is limited because baseline fields or overlap are missing."
        )
    if _has_concentration_risk(ticker_evidence):
        limitations.append("Candidate evidence may be concentrated in one ticker.")
    if evidence.empty:
        limitations.append("No candidate evidence rows were produced.")
    return _dedupe_text(limitations)


def _proposal_status(row: dict[str, Any], inputs: _ReviewInputs, blocking_reasons: str) -> str:
    recommendation = str(row.get("recommendation") or "")
    if inputs.missing_required_artifacts:
        return "INSUFFICIENT_EVIDENCE"
    if int(_float_or_zero(row.get("sample_count"))) <= 0:
        return "INSUFFICIENT_EVIDENCE"
    if recommendation == "PROMOTE_TO_SHADOW":
        return "READY_FOR_MANUAL_REVIEW"
    if recommendation == "EXCLUDE_INSUFFICIENT_DATA":
        return "INSUFFICIENT_EVIDENCE"
    if recommendation == "DOWNWEIGHT" and _float_or_zero(row.get("rank_ic_20d")) == 0.0:
        return "REJECT"
    return "KEEP_RESEARCH_ONLY"


def _blocking_reasons(
    row: dict[str, Any],
    sensitivity: dict[str, Any],
    inputs: _ReviewInputs,
) -> str:
    reasons: list[str] = []
    if inputs.missing_required_artifacts:
        reasons.append("missing_required_artifacts")
    sensitivity_blocking = str(sensitivity.get("blocking_reason") or "").strip()
    if sensitivity_blocking:
        reasons.extend(part for part in sensitivity_blocking.split(";") if part)
    recommendation = str(row.get("recommendation") or "")
    if recommendation == "EXCLUDE_INSUFFICIENT_DATA":
        reasons.append("insufficient_sample_or_coverage")
    elif recommendation == "KEEP_RESEARCH_ONLY":
        reasons.append("research_only_policy_gate")
    elif recommendation == "DOWNWEIGHT":
        reasons.append("below_shadow_policy")
    return ";".join(_dedupe_text(reasons))


def _supporting_reasons(
    *,
    row: dict[str, Any],
    inputs: _ReviewInputs,
    overlap_risk: str,
) -> str:
    reasons = [
        f"evaluation_recommendation={row.get('recommendation', '')}",
        f"diagnostics_status={inputs.diagnostics_summary.get('diagnostics_status', 'UNKNOWN')}",
        f"rank_ic_20d={_format_float(row.get('rank_ic_20d'))}",
        f"coverage_ratio={_format_float(row.get('coverage_ratio'))}",
        f"data_quality_score={_format_float(row.get('data_quality_score'))}",
        f"stability_score={_format_float(row.get('stability_score'))}",
        f"baseline_redundancy={overlap_risk or 'UNKNOWN'}",
    ]
    return ";".join(reasons)


def _missing_candidate_record(feature_id: str, reason: str) -> dict[str, Any]:
    return {
        "feature_id": feature_id,
        "metric_id": "",
        "rank_ic_20d": np.nan,
        "ic_20d": np.nan,
        "hit_rate_20d": np.nan,
        "coverage_ratio": np.nan,
        "data_quality_score": np.nan,
        "stability_score": np.nan,
        "drawdown_improvement_20d": np.nan,
        "incremental_alpha_20d": np.nan,
        "sample_count": 0,
        "valid_ticker_count": 0,
        "pit_grade": "",
        "manual_review_required": True,
        "production_effect": SEC_PIT_CANDIDATE_REVIEW_PRODUCTION_EFFECT,
        "recommendation": "INSUFFICIENT_EVIDENCE",
        "blocking_reasons": reason,
        "supporting_reasons": "candidate row not found in TRADING-040 feature effectiveness",
    }


def _empty_candidate_evidence(candidate_features: list[str], reason: str) -> pd.DataFrame:
    return pd.DataFrame(
        [_missing_candidate_record(feature, reason) for feature in candidate_features],
        columns=list(CANDIDATE_EVIDENCE_COLUMNS),
    )


def _candidate_incremental_metrics(frame: pd.DataFrame) -> tuple[float, float]:
    if frame.empty:
        return np.nan, np.nan
    positive = frame.loc[frame["signal_score"] > 0]
    negative = frame.loc[frame["signal_score"] < 0]
    if positive.empty or negative.empty:
        return np.nan, np.nan
    incremental_alpha = _mean_or_nan(positive["relative_return_vs_QQQ_20d"]) - _mean_or_nan(
        negative["relative_return_vs_QQQ_20d"]
    )
    drawdown_improvement = _mean_or_nan(positive["max_drawdown_forward_20d"]) - _mean_or_nan(
        negative["max_drawdown_forward_20d"]
    )
    return incremental_alpha, drawdown_improvement


def _rank_ic_by_feature(feature_effectiveness: pd.DataFrame) -> dict[str, float]:
    if feature_effectiveness.empty or "feature_id" not in feature_effectiveness.columns:
        return {}
    result: dict[str, float] = {}
    for row in feature_effectiveness.to_dict(orient="records"):
        result[str(row.get("feature_id") or "")] = _float_or_nan(row.get("rank_ic_20d"))
    return result


def _signal_score(feature_id: str, normalized_value: object, rank_ic: dict[str, float]) -> float:
    normalized = _float_or_nan(normalized_value)
    if pd.isna(normalized):
        return np.nan
    rank_value = rank_ic.get(feature_id, np.nan)
    direction = -1.0 if pd.notna(rank_value) and rank_value < 0 else 1.0
    return float(normalized * direction)


def _net_contribution(group: pd.DataFrame) -> float:
    contribution = pd.to_numeric(group["contribution"], errors="coerce").fillna(0.0)
    if contribution.abs().sum() > 0:
        return float(contribution.sum())
    return float(pd.to_numeric(group["signal_score"], errors="coerce").fillna(0.0).sum())


def _ticker_interpretation(group: pd.DataFrame) -> str:
    if len(group) < 3:
        return "LIMITED_SAMPLE"
    net = _net_contribution(group)
    relative = _mean_or_nan(group["relative_return_vs_QQQ_20d"])
    if net > 0 and pd.notna(relative) and relative > 0:
        return "SUPPORTING_TICKER"
    if net > 0 and pd.notna(relative) and relative < 0:
        return "NEGATIVE_OUTCOME_DESPITE_POSITIVE_SIGNAL"
    if net < 0:
        return "WEAK_OR_NEGATIVE_SIGNAL"
    return "NEUTRAL"


def _period_interpretation(group: pd.DataFrame, incremental_alpha: float) -> str:
    if len(group) < 3:
        return "LIMITED_SAMPLE"
    if pd.notna(incremental_alpha) and incremental_alpha > 0:
        return "STABLE_SUPPORTIVE_PERIOD"
    if pd.notna(incremental_alpha) and incremental_alpha < 0:
        return "WEAK_PERIOD"
    return "NEUTRAL_PERIOD"


def _overlap_interpretation(rank_correlation: float, sample_count: int) -> str:
    if sample_count < 2 or pd.isna(rank_correlation):
        return "overlap_status=INSUFFICIENT_OVERLAP"
    risk = _redundancy_risk(rank_correlation)
    if risk == "HIGH":
        return (
            "Candidate is highly rank-correlated with baseline score; "
            "new information is limited."
        )
    if risk == "MEDIUM":
        return "Candidate has partial overlap with baseline score; review incremental evidence."
    return "Candidate appears distinct from baseline score in available overlap."


def _redundancy_risk(rank_correlation: float) -> str:
    if pd.isna(rank_correlation):
        return "UNKNOWN"
    magnitude = abs(float(rank_correlation))
    if magnitude >= HIGH_REDUNDANCY_RANK_CORRELATION:
        return "HIGH"
    if magnitude >= MEDIUM_REDUNDANCY_RANK_CORRELATION:
        return "MEDIUM"
    return "LOW"


def _feature_concentration_risk(ticker_evidence: pd.DataFrame, feature_id: str) -> str:
    rows = ticker_evidence.loc[ticker_evidence["feature_id"].astype(str) == feature_id]
    if rows.empty:
        return ""
    values = pd.to_numeric(rows["net_contribution"], errors="coerce").abs().fillna(0.0)
    total = float(values.sum())
    if total <= 0:
        return ""
    share = float(values.max() / total)
    return (
        "candidate_concentration_risk" if share >= CONCENTRATION_ABS_CONTRIBUTION_SHARE_WARN else ""
    )


def _has_concentration_risk(ticker_evidence: pd.DataFrame) -> bool:
    if ticker_evidence.empty:
        return False
    for feature_id in ticker_evidence["feature_id"].dropna().astype(str).unique():
        if _feature_concentration_risk(ticker_evidence, feature_id):
            return True
    return False


def _feature_overlap_risk(overlap: pd.DataFrame, feature_id: str) -> str:
    if overlap.empty or "feature_id" not in overlap.columns:
        return ""
    rows = overlap.loc[overlap["feature_id"].astype(str) == feature_id]
    if rows.empty:
        return ""
    risk = str(rows.iloc[0].get("redundancy_risk") or "")
    return f"baseline_redundancy_risk={risk}" if risk and risk != "LOW" else ""


def _overlap_limited(overlap: pd.DataFrame) -> bool:
    if overlap.empty or "overlap_interpretation" not in overlap.columns:
        return True
    return bool(
        overlap["overlap_interpretation"]
        .astype(str)
        .str.contains("LIMITED_BASELINE_FIELDS_MISSING|INSUFFICIENT_OVERLAP")
        .any()
    )


def _proposal_rationale(row: dict[str, Any]) -> str:
    status = str(row.get("recommendation") or "")
    if status == "READY_FOR_MANUAL_REVIEW":
        return (
            "TRADING-040 evidence marks the feature as PROMOTE_TO_SHADOW, "
            "but this review only proposes human review for observe-only iteration."
        )
    if status == "INSUFFICIENT_EVIDENCE":
        return "Evidence is insufficient for observe-only shadow iteration."
    if status == "REJECT":
        return "Available evidence does not support continued shadow candidate review."
    return (
        "Keep research-only until additional out-of-sample evidence or owner review "
        "is available."
    )


def _top_candidates(evidence: pd.DataFrame, proposal: pd.DataFrame) -> list[dict[str, Any]]:
    if evidence.empty:
        return []
    merged = evidence.copy()
    if not proposal.empty:
        merged = merged.merge(
            proposal[["feature_id", "proposal_status"]],
            on="feature_id",
            how="left",
        )
    merged["_abs_rank_ic_20d"] = pd.to_numeric(merged["rank_ic_20d"], errors="coerce").abs()
    merged = merged.sort_values(
        ["_abs_rank_ic_20d", "coverage_ratio", "feature_id"],
        ascending=[False, False, True],
        na_position="last",
    )
    return [
        {
            "feature_id": str(row.get("feature_id") or ""),
            "metric_id": str(row.get("metric_id") or ""),
            "rank_ic_20d": _json_number(row.get("rank_ic_20d")),
            "recommendation": str(row.get("recommendation") or ""),
            "proposal_status": str(row.get("proposal_status") or row.get("recommendation") or ""),
        }
        for row in merged.head(10).to_dict(orient="records")
    ]


def _provenance_complete(provenance: object) -> bool:
    if not isinstance(provenance, dict):
        return False
    missing = _float_or_zero(provenance.get("missing_rows"))
    complete_ratio = _float_or_zero(provenance.get("complete_ratio"))
    return missing == 0 and complete_ratio >= 1.0


def _evidence_table_lines(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["- no candidate evidence available"]
    lines = [
        "| feature_id | metric_id | rank_ic_20d | hit_rate_20d | coverage | "
        "data_quality | recommendation |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for row in frame.to_dict(orient="records"):
        lines.append(
            "| "
            f"`{_escape_markdown(str(row.get('feature_id', '')))}` | "
            f"`{_escape_markdown(str(row.get('metric_id', '')))}` | "
            f"{_format_float(row.get('rank_ic_20d'))} | "
            f"{_format_float(row.get('hit_rate_20d'))} | "
            f"{_format_float(row.get('coverage_ratio'))} | "
            f"{_format_float(row.get('data_quality_score'))} | "
            f"`{_escape_markdown(str(row.get('recommendation', '')))}` |"
        )
    return lines


def _ticker_lines(frame: pd.DataFrame, _title: str) -> list[str]:
    if frame.empty:
        return ["  - none"]
    lines: list[str] = []
    for row in frame.head(5).to_dict(orient="records"):
        lines.append(
            "  - "
            f"`{row.get('ticker', '')}` / `{row.get('feature_id', '')}`: "
            f"net_contribution={_format_float(row.get('net_contribution'))}, "
            f"relative_return={_format_float(row.get('avg_relative_return_vs_QQQ_20d'))}, "
            f"{row.get('interpretation', '')}"
        )
    return lines


def _period_lines(frame: pd.DataFrame, _title: str) -> list[str]:
    if frame.empty:
        return ["  - none"]
    lines: list[str] = []
    for row in frame.head(5).to_dict(orient="records"):
        lines.append(
            "  - "
            f"`{row.get('period_bucket', '')}` / `{row.get('feature_id', '')}`: "
            f"incremental_alpha={_format_float(row.get('incremental_alpha_20d'))}, "
            f"rank_ic={_format_float(row.get('rank_ic_20d'))}, "
            f"{row.get('interpretation', '')}"
        )
    return lines


def _overlap_lines(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["- no baseline overlap evidence available"]
    lines: list[str] = []
    for row in frame.to_dict(orient="records"):
        lines.append(
            "- "
            f"`{row.get('feature_id', '')}` vs `{row.get('baseline_signal', '')}`: "
            f"correlation={_format_float(row.get('correlation'))}, "
            f"rank_correlation={_format_float(row.get('rank_correlation'))}, "
            f"samples={row.get('overlap_sample_count', 0)}, "
            f"redundancy_risk={row.get('redundancy_risk', '')}; "
            f"{row.get('overlap_interpretation', '')}"
        )
    return lines


def _proposal_lines(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["- no shadow proposal available"]
    lines: list[str] = []
    for row in frame.to_dict(orient="records"):
        lines.append(
            "- "
            f"`{row.get('feature_id', '')}`: status={row.get('proposal_status', '')}; "
            "suggested_observe_only_weight="
            f"{_format_float(row.get('suggested_observe_only_weight'))}; "
            f"max_allowed_initial_weight={_format_float(row.get('max_allowed_initial_weight'))}; "
            f"review_required={row.get('review_required', True)}; "
            f"production_effect={row.get('production_effect', 'none')}; "
            f"rollback_condition={row.get('rollback_condition', '')}"
        )
    return lines


def _concentration_text(ticker: pd.DataFrame) -> str:
    if ticker.empty:
        return "UNKNOWN"
    risks = [
        feature
        for feature in ticker["feature_id"].dropna().astype(str).unique()
        if _feature_concentration_risk(ticker, feature)
    ]
    return "HIGH for " + ",".join(risks) if risks else "LOW"


def _period_dependency_text(period: pd.DataFrame) -> str:
    if period.empty:
        return "UNKNOWN"
    interpretations = set(period["interpretation"].astype(str))
    if "WEAK_PERIOD" in interpretations and "STABLE_SUPPORTIVE_PERIOD" in interpretations:
        return "MIXED"
    if "WEAK_PERIOD" in interpretations:
        return "WEAK"
    if "STABLE_SUPPORTIVE_PERIOD" in interpretations:
        return "SUPPORTIVE"
    return "LIMITED"


def _artifact_path(summary: dict[str, Any], key: str, default: Path) -> Path:
    outputs = summary.get("output_artifacts") if isinstance(summary, dict) else {}
    raw = outputs.get(key) if isinstance(outputs, dict) else None
    if not raw:
        return default
    path = Path(str(raw))
    return path if path.is_absolute() else PROJECT_ROOT / path


def _latest_dated_path(root: Path, prefix: str, suffix: str, end: date | None) -> Path:
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


def _summary_date(summary: dict[str, Any], key: str) -> date | None:
    if not isinstance(summary, dict):
        return None
    return _parse_date(str(summary.get(key) or ""))


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


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


def _empty_candidate_signal_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "decision_date",
            "ticker",
            "feature_id",
            "metric_id",
            "feature_value",
            "normalized_value",
            "contribution",
            "signal_score",
            "forward_return_20d",
            "relative_return_vs_QQQ_20d",
            "max_drawdown_forward_20d",
            "pit_grade",
        ]
    )


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index)
    return pd.to_numeric(frame[column], errors="coerce")


def _first_existing(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.Series:
    for column in columns:
        if column in frame.columns:
            return frame[column].astype(str)
    return pd.Series([""] * len(frame), index=frame.index, dtype="object")


def _mean_or_nan(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.mean()) if not values.empty else np.nan


def _hit_rate(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float((values > 0).mean()) if not values.empty else np.nan


def _correlation(left: pd.Series, right: pd.Series) -> float:
    frame = pd.DataFrame(
        {
            "left": pd.to_numeric(left, errors="coerce"),
            "right": pd.to_numeric(right, errors="coerce"),
        }
    ).dropna()
    if len(frame) < 2:
        return np.nan
    if frame["left"].nunique() < 2 or frame["right"].nunique() < 2:
        return np.nan
    return float(frame["left"].corr(frame["right"]))


def _rank_correlation(left: pd.Series, right: pd.Series) -> float:
    frame = pd.DataFrame(
        {
            "left": pd.to_numeric(left, errors="coerce"),
            "right": pd.to_numeric(right, errors="coerce"),
        }
    ).dropna()
    if len(frame) < 2:
        return np.nan
    return _correlation(frame["left"].rank(method="average"), frame["right"].rank(method="average"))


def _dominant_value(frame: pd.DataFrame, column: str) -> str:
    if frame.empty or column not in frame.columns:
        return ""
    clean = [str(value) for value in frame[column].dropna().tolist() if str(value)]
    if not clean:
        return ""
    return Counter(clean).most_common(1)[0][0]


def _dedupe_feature_ids(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        for item in str(value).replace(",", " ").split():
            normalized = item.strip()
            if normalized and normalized not in seen:
                result.append(normalized)
                seen.add(normalized)
    return result


def _dedupe_text(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if normalized and normalized not in seen:
            result.append(normalized)
            seen.add(normalized)
    return result


def _join_reason(left: str, right: str) -> str:
    return ";".join(_dedupe_text([*str(left or "").split(";"), *str(right or "").split(";")]))


def _float_or_nan(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return np.nan


def _float_or_zero(value: object) -> float:
    number = _float_or_nan(value)
    return 0.0 if pd.isna(number) else number


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


def _format_float(value: object) -> str:
    number = _json_number(value)
    return "NA" if number is None else f"{number:.4f}"


def _escape_markdown(value: str) -> str:
    return value.replace("|", "\\|")


def _file_sha256(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _deterministic_generated_at(end: date) -> str:
    return f"{end.isoformat()}T00:00:00+00:00"
