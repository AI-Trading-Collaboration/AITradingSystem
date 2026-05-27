from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.config import (
    DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    PROJECT_ROOT,
    load_sec_companies,
)
from ai_trading_system.fundamentals.sec_pit_aliases import (
    TickerAliasResolution,
    canonicalize_ticker_series,
    canonicalize_tickers,
    load_ticker_aliases,
    resolve_ticker_alias,
)
from ai_trading_system.fundamentals.sec_pit_baseline_comparison import (
    DEFAULT_BASELINE_SCORE_DIR,
    DEFAULT_PROCESSED_BASELINE_SCORE_PATH,
    DEFAULT_SEC_PIT_BASELINE_COMPARISON_OUTPUT_DIR,
    DEFAULT_SEC_PIT_EVALUATION_DIR,
)
from ai_trading_system.fundamentals.sec_pit_evaluation import (
    DEFAULT_SEC_PIT_EVALUATION_POLICY_PATH,
    DEFAULT_SEC_PIT_FEATURE_PANEL_PATH,
    load_sec_pit_evaluation_policy,
)

SEC_PIT_REAL_RUN_DIAGNOSTICS_TASK_ID = "TRADING-042"
SEC_PIT_REAL_RUN_DIAGNOSTICS_REPORT_TYPE = "sec_pit_real_run_diagnostics"
SEC_PIT_REAL_RUN_DIAGNOSTICS_PRODUCTION_EFFECT = "none"
DEFAULT_SEC_PIT_DIAGNOSTICS_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "sec_pit_diagnostics"

DIAGNOSTICS_STATUSES = {
    "OK",
    "LIMITED_MISSING_ARTIFACTS",
    "FAILED_VALIDATION",
    "INSUFFICIENT_DATA",
}

PROVENANCE_GAP_COLUMNS = (
    "stage",
    "stage_status",
    "ticker",
    "feature_id",
    "metric_id",
    "period",
    "decision_date",
    "row_count",
    "missing_accession_number_count",
    "missing_accepted_datetime_count",
    "missing_filed_date_count",
    "missing_raw_sha256_count",
    "missing_source_concept_count",
    "missing_pit_grade_count",
    "provenance_complete_ratio",
    "downgrade_reason",
    "first_missing_example",
    "recommended_fix",
)

COVERAGE_AUDIT_COLUMNS = (
    "feature_id",
    "metric_id",
    "expected_observations",
    "valid_observations",
    "duplicate_observations",
    "coverage_ratio_before",
    "coverage_ratio_after",
    "affected_ticker_count",
    "affected_date_count",
    "dedup_rule",
    "warning",
)

ALIAS_AUDIT_COLUMNS = (
    "input_ticker",
    "canonical_ticker",
    "source",
    "resolved",
    "resolution_reason",
    "used_in_sec_companies",
    "used_in_price_data",
    "used_in_baseline_score",
    "warning",
)

LABEL_AUDIT_COLUMNS = (
    "label_name",
    "required",
    "available_count",
    "missing_count",
    "coverage_ratio",
    "first_missing_date",
    "last_missing_date",
    "affected_ticker_count",
    "source_artifact",
    "recommended_fix",
)

CANDIDATE_SENSITIVITY_COLUMNS = (
    "feature_id",
    "metric_id",
    "rank_ic_20d",
    "data_quality_score",
    "coverage_ratio",
    "stability_score",
    "current_recommendation",
    "hypothetical_recommendation_if_provenance_fixed",
    "blocking_reason",
    "minimum_required_fix",
    "manual_review_required",
    "production_effect",
)

PROVENANCE_STAGE_ORDER = (
    "raw_companyfacts",
    "filing_timeline",
    "xbrl_facts_long",
    "mapped_metrics_long",
    "pit_intervals",
    "feature_panel",
    "evaluation_attribution",
    "baseline_comparison",
)

PROVENANCE_AUDIT_FIELDS = (
    "accession_number",
    "accepted_datetime",
    "filed_date",
    "raw_sha256",
    "source_concept",
    "pit_grade",
)


@dataclass(frozen=True)
class SecPitRealRunDiagnosticsArtifacts:
    status: str
    summary_json_path: Path
    summary_markdown_path: Path
    provenance_gap_path: Path
    coverage_audit_path: Path
    alias_resolution_audit_path: Path
    label_coverage_audit_path: Path
    candidate_sensitivity_path: Path


@dataclass(frozen=True)
class BaselineArtifactResolution:
    path: Path
    status: str
    frame: pd.DataFrame

    @property
    def rows(self) -> int:
        return int(len(self.frame))

    @property
    def date_range(self) -> list[str]:
        if self.frame.empty:
            return []
        column = _first_existing_column(self.frame, ("decision_date", "as_of", "date"))
        if column is None:
            return []
        values = pd.to_datetime(self.frame[column], errors="coerce").dropna()
        if values.empty:
            return []
        return [values.min().date().isoformat(), values.max().date().isoformat()]

    @property
    def ticker_count(self) -> int:
        if self.frame.empty:
            return 0
        column = _first_existing_column(self.frame, ("ticker", "symbol"))
        if column is None:
            return 0
        return int(self.frame[column].dropna().astype(str).str.upper().nunique())


def run_sec_pit_real_run_diagnostics(
    *,
    start: date | None = None,
    end: date | None = None,
    tickers: list[str] | None = None,
    feature_panel_path: Path = DEFAULT_SEC_PIT_FEATURE_PANEL_PATH,
    evaluation_dir: Path = DEFAULT_SEC_PIT_EVALUATION_DIR,
    comparison_dir: Path = DEFAULT_SEC_PIT_BASELINE_COMPARISON_OUTPUT_DIR,
    baseline_score_path: Path | None = None,
    baseline_score_dir: Path = DEFAULT_BASELINE_SCORE_DIR,
    output_dir: Path = DEFAULT_SEC_PIT_DIAGNOSTICS_OUTPUT_DIR,
    latest: bool = False,
) -> SecPitRealRunDiagnosticsArtifacts:
    output_dir.mkdir(parents=True, exist_ok=True)
    latest_evaluation_summary = _latest_dated_path(
        evaluation_dir,
        "sec_pit_evaluation_summary_",
        ".json",
        end,
    )
    latest_comparison_summary = _latest_dated_path(
        comparison_dir,
        "sec_pit_baseline_comparison_summary_",
        ".json",
        end,
    )
    evaluation_summary = _read_json_object(latest_evaluation_summary)
    comparison_summary = _read_json_object(latest_comparison_summary)

    inferred_start = _summary_date(evaluation_summary, "start_date") or _summary_date(
        comparison_summary,
        "start_date",
    )
    inferred_end = _summary_date(evaluation_summary, "end_date") or _summary_date(
        comparison_summary,
        "end_date",
    )
    if latest:
        start = start or inferred_start
        end = end or inferred_end
    if start is None or end is None:
        raise ValueError("start and end are required unless --latest can infer them")
    if start > end:
        raise ValueError("start must be on or before end")

    suffix = end.isoformat()
    summary_json_path = output_dir / f"sec_pit_real_run_diagnostics_{suffix}.json"
    summary_markdown_path = output_dir / f"sec_pit_real_run_diagnostics_{suffix}.md"
    provenance_gap_path = output_dir / f"sec_pit_provenance_gap_{suffix}.csv"
    coverage_audit_path = output_dir / f"sec_pit_coverage_audit_{suffix}.csv"
    alias_audit_path = output_dir / f"sec_pit_alias_resolution_audit_{suffix}.csv"
    label_audit_path = output_dir / f"sec_pit_label_coverage_audit_{suffix}.csv"
    sensitivity_path = output_dir / f"sec_pit_candidate_sensitivity_{suffix}.csv"

    input_tickers = _input_tickers(tickers, evaluation_summary)
    aliases = load_ticker_aliases()
    sec_company_tickers = _sec_company_tickers()
    canonical_tickers, alias_resolutions = canonicalize_tickers(
        input_tickers,
        aliases=aliases,
        sec_company_tickers=sec_company_tickers,
    )
    if not input_tickers:
        canonical_tickers = _summary_tickers(evaluation_summary)
        alias_resolutions = [
            resolve_ticker_alias(
                ticker,
                aliases=aliases,
                sec_company_tickers=sec_company_tickers,
            )
            for ticker in canonical_tickers
        ]

    baseline = resolve_baseline_artifact(
        explicit_path=baseline_score_path,
        baseline_score_dir=baseline_score_dir,
        end=end,
    )
    alias_audit = _alias_audit_frame(
        alias_resolutions,
        baseline=baseline.frame,
        price_tickers=set(),
    )
    provenance_gap = build_provenance_gap_audit(
        feature_panel_path=feature_panel_path,
        evaluation_dir=evaluation_dir,
        comparison_dir=comparison_dir,
        end=end,
    )
    coverage_audit = build_coverage_audit(
        feature_panel_path=feature_panel_path,
        start=start,
        end=end,
        canonical_tickers=canonical_tickers,
    )
    label_audit = build_label_coverage_audit(
        evaluation_dir=evaluation_dir,
        comparison_dir=comparison_dir,
        end=end,
    )
    sensitivity = build_candidate_sensitivity(
        evaluation_dir=evaluation_dir,
        end=end,
    )

    _write_csv(provenance_gap, provenance_gap_path, PROVENANCE_GAP_COLUMNS)
    _write_csv(coverage_audit, coverage_audit_path, COVERAGE_AUDIT_COLUMNS)
    _write_csv(alias_audit, alias_audit_path, ALIAS_AUDIT_COLUMNS)
    _write_csv(label_audit, label_audit_path, LABEL_AUDIT_COLUMNS)
    _write_csv(sensitivity, sensitivity_path, CANDIDATE_SENSITIVITY_COLUMNS)

    summary = _summary_payload(
        start=start,
        end=end,
        input_tickers=input_tickers,
        canonical_tickers=canonical_tickers,
        feature_panel_path=feature_panel_path,
        evaluation_dir=evaluation_dir,
        comparison_dir=comparison_dir,
        baseline=baseline,
        provenance_gap=provenance_gap,
        alias_audit=alias_audit,
        label_audit=label_audit,
        coverage_audit=coverage_audit,
        sensitivity=sensitivity,
        artifacts={
            "summary_json": summary_json_path,
            "summary_markdown": summary_markdown_path,
            "provenance_gap_csv": provenance_gap_path,
            "coverage_audit_csv": coverage_audit_path,
            "alias_resolution_audit_csv": alias_audit_path,
            "label_coverage_audit_csv": label_audit_path,
            "candidate_sensitivity_csv": sensitivity_path,
        },
    )
    summary_json_path.write_text(
        json.dumps(_json_value(summary), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_markdown_path.write_text(
        render_sec_pit_real_run_diagnostics(summary),
        encoding="utf-8",
    )
    return SecPitRealRunDiagnosticsArtifacts(
        status=str(summary["diagnostics_status"]),
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        provenance_gap_path=provenance_gap_path,
        coverage_audit_path=coverage_audit_path,
        alias_resolution_audit_path=alias_audit_path,
        label_coverage_audit_path=label_audit_path,
        candidate_sensitivity_path=sensitivity_path,
    )


def resolve_baseline_artifact(
    *,
    explicit_path: Path | None,
    baseline_score_dir: Path,
    end: date,
) -> BaselineArtifactResolution:
    if explicit_path is not None:
        frame = _read_csv_or_empty(explicit_path)
        status = "OK" if not frame.empty else "LIMITED_BASELINE_MISSING"
        return BaselineArtifactResolution(path=explicit_path, status=status, frame=frame)

    path = _baseline_path(baseline_score_dir, end)
    frame = _read_csv_or_empty(path)
    if not frame.empty:
        return BaselineArtifactResolution(path=path, status="OK", frame=frame)

    if _is_default_baseline_score_dir(baseline_score_dir):
        fallback = DEFAULT_PROCESSED_BASELINE_SCORE_PATH
        fallback_frame = _read_csv_or_empty(fallback)
        if not fallback_frame.empty:
            return BaselineArtifactResolution(
                path=fallback,
                status="FALLBACK_USED",
                frame=fallback_frame,
            )
    return BaselineArtifactResolution(
        path=path,
        status="LIMITED_BASELINE_MISSING",
        frame=frame,
    )


def build_provenance_gap_audit(
    *,
    feature_panel_path: Path,
    evaluation_dir: Path,
    comparison_dir: Path,
    end: date,
) -> pd.DataFrame:
    processed_dir = feature_panel_path.parent
    project_root = _infer_project_root(feature_panel_path)
    stage_paths = {
        "raw_companyfacts": project_root
        / "data"
        / "raw"
        / "sec_edgar"
        / "manifest"
        / "sec_edgar_raw_manifest.csv",
        "filing_timeline": processed_dir / "filing_timeline.csv",
        "xbrl_facts_long": processed_dir / "xbrl_facts_long.csv",
        "mapped_metrics_long": processed_dir / "mapped_metrics_long.csv",
        "pit_intervals": processed_dir / "fundamental_pit_intervals.csv",
        "feature_panel": feature_panel_path,
        "evaluation_attribution": _latest_dated_path(
            evaluation_dir,
            "sec_pit_signal_attribution_",
            ".csv",
            end,
        ),
        "baseline_comparison": _latest_dated_path(
            comparison_dir,
            "sec_pit_decision_impact_",
            ".csv",
            end,
        ),
    }
    frames = [_audit_provenance_stage(stage, path) for stage, path in stage_paths.items()]
    return pd.concat(frames, ignore_index=True) if frames else _empty_frame(PROVENANCE_GAP_COLUMNS)


def build_coverage_audit(
    *,
    feature_panel_path: Path,
    start: date,
    end: date,
    canonical_tickers: list[str],
) -> pd.DataFrame:
    frame = _read_csv_or_empty(feature_panel_path)
    if frame.empty:
        return _empty_frame(COVERAGE_AUDIT_COLUMNS)
    frame = frame.fillna("")
    aliases = load_ticker_aliases()
    date_column = _first_existing_column(frame, ("decision_date", "date", "signal_date"))
    if date_column is None or "ticker" not in frame.columns or "feature_id" not in frame.columns:
        return _empty_frame(COVERAGE_AUDIT_COLUMNS)
    frame["decision_date"] = pd.to_datetime(frame[date_column], errors="coerce").dt.date
    frame["ticker"] = canonicalize_ticker_series(frame["ticker"], aliases=aliases)
    frame = frame.loc[
        frame["decision_date"].notna()
        & frame["decision_date"].map(lambda value: start <= value <= end)
    ].copy()
    if canonical_tickers:
        frame = frame.loc[frame["ticker"].isin(set(canonical_tickers))].copy()
    if frame.empty:
        return _empty_frame(COVERAGE_AUDIT_COLUMNS)
    frame["metric_id"] = _string_column(frame, ("metric_id", "input_metric_ids"))
    all_dates = sorted(frame["decision_date"].dropna().unique().tolist())
    ticker_count = len(canonical_tickers) if canonical_tickers else int(frame["ticker"].nunique())
    expected = max(len(all_dates) * max(ticker_count, 1), 1)
    records: list[dict[str, Any]] = []
    for (feature_id, metric_id), group in frame.groupby(["feature_id", "metric_id"], sort=True):
        unique_pairs = group[["decision_date", "ticker"]].drop_duplicates()
        valid = int(len(unique_pairs))
        duplicate = int(len(group) - valid)
        before = float(len(group) / expected)
        after = min(float(valid / expected), 1.0)
        warning = ""
        if before > 1:
            warning = "coverage_ratio_before_above_1"
        if duplicate:
            warning = _join_nonempty([warning, "duplicate_observations"], separator=";")
        records.append(
            {
                "feature_id": str(feature_id),
                "metric_id": str(metric_id),
                "expected_observations": expected,
                "valid_observations": valid,
                "duplicate_observations": duplicate,
                "coverage_ratio_before": before,
                "coverage_ratio_after": after,
                "affected_ticker_count": int(group["ticker"].nunique()),
                "affected_date_count": int(group["decision_date"].nunique()),
                "dedup_rule": (
                    "ticker/feature/metric/decision_date: keep highest pit_quality_score, "
                    "latest available_time, latest period"
                ),
                "warning": warning,
            }
        )
    return pd.DataFrame(records, columns=list(COVERAGE_AUDIT_COLUMNS)).sort_values(
        ["feature_id", "metric_id"]
    )


def build_label_coverage_audit(
    *,
    evaluation_dir: Path,
    comparison_dir: Path,
    end: date,
) -> pd.DataFrame:
    comparison_path = _latest_dated_path(comparison_dir, "sec_pit_decision_impact_", ".csv", end)
    attribution_path = _latest_dated_path(
        evaluation_dir,
        "sec_pit_signal_attribution_",
        ".csv",
        end,
    )
    source_path = comparison_path if comparison_path.exists() else attribution_path
    frame = _read_csv_or_empty(source_path)
    labels = (
        "forward_return_20d",
        "relative_return_vs_QQQ_20d",
        "max_drawdown_forward_20d",
    )
    records: list[dict[str, Any]] = []
    for label in labels:
        if frame.empty:
            records.append(_missing_label_record(label, source_path, "source artifact missing"))
            continue
        label_column = label
        if label_column not in frame.columns and label.startswith("relative_return_vs_"):
            candidates = [
                column
                for column in frame.columns
                if column.startswith("relative_return_vs_") and column.endswith("_20d")
            ]
            label_column = candidates[0] if candidates else label
        if label_column not in frame.columns:
            records.append(_missing_label_record(label, source_path, "label column missing"))
            continue
        values = pd.to_numeric(frame[label_column], errors="coerce")
        available = int(values.notna().sum())
        missing_mask = values.isna()
        dates = _date_series(frame)
        missing_dates = dates.loc[missing_mask & dates.notna()]
        records.append(
            {
                "label_name": label,
                "required": True,
                "available_count": available,
                "missing_count": int(missing_mask.sum()),
                "coverage_ratio": _safe_ratio(available, len(frame)),
                "first_missing_date": (
                    missing_dates.min().isoformat() if not missing_dates.empty else ""
                ),
                "last_missing_date": (
                    missing_dates.max().isoformat() if not missing_dates.empty else ""
                ),
                "affected_ticker_count": _affected_ticker_count(frame, missing_mask),
                "source_artifact": str(source_path),
                "recommended_fix": (
                    ""
                    if available == len(frame)
                    else "rerun SEC PIT evaluation after label/provenance propagation fix"
                ),
            }
        )
    return pd.DataFrame(records, columns=list(LABEL_AUDIT_COLUMNS))


def build_candidate_sensitivity(*, evaluation_dir: Path, end: date) -> pd.DataFrame:
    feature_path = _latest_dated_path(
        evaluation_dir,
        "sec_pit_feature_effectiveness_",
        ".csv",
        end,
    )
    frame = _read_csv_or_empty(feature_path)
    if frame.empty:
        return _empty_frame(CANDIDATE_SENSITIVITY_COLUMNS)
    policy = load_sec_pit_evaluation_policy(DEFAULT_SEC_PIT_EVALUATION_POLICY_PATH)
    records: list[dict[str, Any]] = []
    for row in frame.fillna("").to_dict(orient="records"):
        rank_ic = _float_or_nan(row.get("rank_ic_20d"))
        data_quality = _float_or_nan(row.get("data_quality_score"))
        coverage = _float_or_nan(row.get("coverage_ratio"))
        stability = _float_or_nan(row.get("stability_score"))
        sample_count = _float_or_nan(row.get("sample_count"))
        valid_ticker_count = _float_or_nan(row.get("valid_ticker_count"))
        current = str(row.get("recommendation") or "")
        blocking = _candidate_blocking_reasons(
            rank_ic=rank_ic,
            data_quality=data_quality,
            coverage=coverage,
            stability=stability,
            sample_count=sample_count,
            valid_ticker_count=valid_ticker_count,
            policy=policy,
        )
        hypothetical = current
        if blocking == ["data_quality_score_below_policy"]:
            hypothetical = "PROMOTE_TO_SHADOW"
        records.append(
            {
                "feature_id": str(row.get("feature_id") or ""),
                "metric_id": str(row.get("metric_id") or ""),
                "rank_ic_20d": rank_ic,
                "data_quality_score": data_quality,
                "coverage_ratio": coverage,
                "stability_score": stability,
                "current_recommendation": current,
                "hypothetical_recommendation_if_provenance_fixed": hypothetical,
                "blocking_reason": ";".join(blocking),
                "minimum_required_fix": _minimum_required_fix(blocking),
                "manual_review_required": True,
                "production_effect": SEC_PIT_REAL_RUN_DIAGNOSTICS_PRODUCTION_EFFECT,
            }
        )
    return pd.DataFrame(records, columns=list(CANDIDATE_SENSITIVITY_COLUMNS)).sort_values(
        ["hypothetical_recommendation_if_provenance_fixed", "feature_id", "metric_id"]
    )


def render_sec_pit_real_run_diagnostics(summary: dict[str, Any]) -> str:
    provenance = summary["provenance"]
    alias = summary["alias_resolution"]
    baseline = summary["baseline"]
    labels = summary["labels"]
    coverage = summary["coverage"]
    sensitivity = summary["candidate_sensitivity"]
    lines = [
        "# SEC PIT Real Run Diagnostics",
        "",
        "## Metadata",
        f"- generated_at: {summary['generated_at']}",
        f"- start_date: {summary['start_date']}",
        f"- end_date: {summary['end_date']}",
        f"- input tickers: {', '.join(summary['input_tickers'])}",
        f"- canonical tickers: {', '.join(summary['canonical_tickers'])}",
        f"- artifact paths: {json.dumps(summary['artifact_paths'], ensure_ascii=False)}",
        "",
        "## Executive Summary",
        f"- diagnostics_status: {summary['diagnostics_status']}",
        "- promotion_ready: false",
        (
            "- main blockers: "
            f"provenance first_loss_stage={provenance['first_loss_stage']}; "
            f"baseline_status={baseline['status']}; "
            f"drawdown_coverage={labels['max_drawdown_forward_20d_coverage']:.4f}; "
            f"coverage_ratio_above_1_before_fix="
            f"{coverage['features_with_ratio_above_1_before_fix']}"
        ),
        "",
        "## Provenance Gap",
        f"- missing provenance count: {provenance['missing_rows']}",
        f"- first loss stage: {provenance['first_loss_stage']}",
        "- affected features: see provenance gap CSV",
        f"- recommended pipeline fix: {provenance['recommended_fix']}",
        "",
        "## Alias Resolution",
        f"- remapped tickers: {alias['remapped_count']}",
        f"- unresolved tickers: {alias['unresolved_count']}",
        "- GOOG / GOOGL handling: `GOOGL` resolves to `GOOG` when `GOOG` exists in SEC config.",
        "",
        "## Baseline Artifact Discovery",
        f"- chosen baseline artifact: {baseline['artifact_path']}",
        f"- fallback status: {baseline['status']}",
        f"- baseline rows / date range: {baseline['rows']} / {baseline['date_range']}",
        "",
        "## Label Coverage",
        f"- forward return coverage: {labels['forward_return_20d_coverage']:.4f}",
        f"- relative return coverage: {labels['relative_return_vs_QQQ_20d_coverage']:.4f}",
        f"- drawdown label coverage: {labels['max_drawdown_forward_20d_coverage']:.4f}",
        "- missing label fixes: see label coverage audit CSV",
        "",
        "## Coverage Ratio Audit",
        (
            "- features with coverage ratio above 1: "
            f"{coverage['features_with_ratio_above_1_before_fix']}"
        ),
        f"- duplicate rows: {coverage['duplicates_removed']}",
        "- corrected ratio: coverage_ratio_after is capped at 1.0",
        "",
        "## Candidate Sensitivity",
        f"- features close to promotion: {sensitivity['near_promotion_count']}",
        (
            "- blocking reasons: "
            f"{json.dumps(sensitivity['top_blocked_features'], ensure_ascii=False)}"
        ),
        "- minimum required fixes: restore provenance quality, then rerun evaluation/comparison.",
        "",
        "## Recommendation",
        "- Do not proceed to shadow promotion from the current run.",
        "- Another data coverage/provenance task is required before TRADING-043.",
        "- Fix provenance propagation first, then rerun end-to-end diagnostics.",
        "",
        "## Manual Review Checklist",
        "- Verify first provenance loss stage against upstream artifacts.",
        "- Confirm GOOG/GOOGL alias behavior in SEC, price, and baseline inputs.",
        "- Re-run evaluation after propagation fixes and confirm drawdown labels are populated.",
        "- Confirm all sensitivity rows remain manual-review-only with production_effect=none.",
    ]
    for limitation in summary.get("limitations", []):
        lines.append(f"- limitation: {limitation}")
    return "\n".join(lines) + "\n"


def _summary_payload(
    *,
    start: date,
    end: date,
    input_tickers: list[str],
    canonical_tickers: list[str],
    feature_panel_path: Path,
    evaluation_dir: Path,
    comparison_dir: Path,
    baseline: BaselineArtifactResolution,
    provenance_gap: pd.DataFrame,
    alias_audit: pd.DataFrame,
    label_audit: pd.DataFrame,
    coverage_audit: pd.DataFrame,
    sensitivity: pd.DataFrame,
    artifacts: dict[str, Path],
) -> dict[str, Any]:
    first_loss_stage = _first_provenance_loss_stage(provenance_gap)
    feature_stage = provenance_gap.loc[provenance_gap["stage"] == "feature_panel"]
    provenance_complete_ratio = (
        float(pd.to_numeric(feature_stage["provenance_complete_ratio"], errors="coerce").mean())
        if not feature_stage.empty
        else 0.0
    )
    missing_rows = _missing_provenance_rows(feature_stage)
    unresolved = int((alias_audit["resolved"].astype(str).str.lower() == "false").sum())
    remapped = int(
        (
            alias_audit["input_ticker"].astype(str) != alias_audit["canonical_ticker"].astype(str)
        ).sum()
    )
    labels = {
        str(row["label_name"]): _float_or_zero(row.get("coverage_ratio"))
        for row in label_audit.to_dict(orient="records")
    }
    ratio_above_one = (
        int((pd.to_numeric(coverage_audit["coverage_ratio_before"], errors="coerce") > 1.0).sum())
        if not coverage_audit.empty
        else 0
    )
    duplicates_removed = (
        int(
            pd.to_numeric(coverage_audit["duplicate_observations"], errors="coerce").fillna(0).sum()
        )
        if not coverage_audit.empty
        else 0
    )
    near = (
        sensitivity.loc[
            sensitivity["hypothetical_recommendation_if_provenance_fixed"].astype(str)
            == "PROMOTE_TO_SHADOW"
        ]
        if not sensitivity.empty
        else pd.DataFrame()
    )
    missing_artifacts = (
        int((provenance_gap["stage_status"].astype(str) == "MISSING_ARTIFACT").sum())
        if not provenance_gap.empty
        else 0
    )
    diagnostics_status = "OK"
    limitations: list[str] = []
    if missing_artifacts:
        diagnostics_status = "LIMITED_MISSING_ARTIFACTS"
        limitations.append("one or more upstream/downstream SEC PIT artifacts are missing")
    if baseline.status == "LIMITED_BASELINE_MISSING":
        diagnostics_status = "LIMITED_MISSING_ARTIFACTS"
        limitations.append("baseline score artifact is missing")
    if feature_stage.empty:
        diagnostics_status = "INSUFFICIENT_DATA"
        limitations.append("feature panel provenance stage has no auditable rows")
    if diagnostics_status not in DIAGNOSTICS_STATUSES:
        diagnostics_status = "FAILED_VALIDATION"
    return {
        "generated_at": f"{end.isoformat()}T00:00:00+00:00",
        "report_type": SEC_PIT_REAL_RUN_DIAGNOSTICS_REPORT_TYPE,
        "task_id": SEC_PIT_REAL_RUN_DIAGNOSTICS_TASK_ID,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "diagnostics_status": diagnostics_status,
        "production_effect": SEC_PIT_REAL_RUN_DIAGNOSTICS_PRODUCTION_EFFECT,
        "manual_review_required": True,
        "input_tickers": input_tickers,
        "canonical_tickers": canonical_tickers,
        "artifact_paths": {
            "feature_panel": str(feature_panel_path),
            "evaluation_dir": str(evaluation_dir),
            "comparison_dir": str(comparison_dir),
            "baseline_score": str(baseline.path),
        },
        "provenance": {
            "missing_rows": missing_rows,
            "first_loss_stage": first_loss_stage,
            "complete_ratio": provenance_complete_ratio,
            "recommended_fix": _provenance_recommended_fix(first_loss_stage),
        },
        "alias_resolution": {
            "remapped_count": remapped,
            "unresolved_count": unresolved,
        },
        "baseline": {
            "status": baseline.status,
            "artifact_path": str(baseline.path),
            "rows": baseline.rows,
            "date_range": baseline.date_range,
            "ticker_count": baseline.ticker_count,
        },
        "labels": {
            "forward_return_20d_coverage": labels.get("forward_return_20d", 0.0),
            "relative_return_vs_QQQ_20d_coverage": labels.get(
                "relative_return_vs_QQQ_20d",
                0.0,
            ),
            "max_drawdown_forward_20d_coverage": labels.get(
                "max_drawdown_forward_20d",
                0.0,
            ),
        },
        "coverage": {
            "features_with_ratio_above_1_before_fix": ratio_above_one,
            "duplicates_removed": duplicates_removed,
        },
        "candidate_sensitivity": {
            "near_promotion_count": int(len(near)),
            "top_blocked_features": _top_blocked_features(near),
        },
        "limitations": limitations,
        "output_artifacts": {key: str(path) for key, path in artifacts.items()},
        "input_checksums": {
            "feature_panel_sha256": _file_sha256(feature_panel_path),
            "baseline_score_sha256": _file_sha256(baseline.path),
        },
    }


def _audit_provenance_stage(stage: str, path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(
            [
                {
                    "stage": stage,
                    "stage_status": "MISSING_ARTIFACT",
                    "ticker": "",
                    "feature_id": "",
                    "metric_id": "",
                    "period": "",
                    "decision_date": "",
                    "row_count": 0,
                    "missing_accession_number_count": 0,
                    "missing_accepted_datetime_count": 0,
                    "missing_filed_date_count": 0,
                    "missing_raw_sha256_count": 0,
                    "missing_source_concept_count": 0,
                    "missing_pit_grade_count": 0,
                    "provenance_complete_ratio": 0.0,
                    "downgrade_reason": "artifact_missing",
                    "first_missing_example": "",
                    "recommended_fix": f"generate or pass {path}",
                }
            ],
            columns=list(PROVENANCE_GAP_COLUMNS),
        )
    frame = _read_csv_or_empty(path).fillna("")
    if frame.empty:
        return _empty_frame(PROVENANCE_GAP_COLUMNS)
    normalized = _normalize_provenance_frame(frame)
    group_columns = ["ticker", "feature_id", "metric_id", "period", "decision_date"]
    required_fields = _provenance_required_fields(stage)
    for field in PROVENANCE_AUDIT_FIELDS:
        normalized[f"_missing_{field}"] = ~normalized[field].astype(str).str.strip().astype(bool)
    missing_columns = [f"_missing_{field}" for field in required_fields]
    normalized["_complete"] = ~normalized.loc[:, missing_columns].any(axis=1)
    normalized["_first_missing_example"] = ""
    incomplete = ~normalized["_complete"]
    if bool(incomplete.any()):
        normalized.loc[incomplete, "_first_missing_example"] = normalized.loc[incomplete].apply(
            _row_missing_example,
            axis=1,
        )

    grouped = (
        normalized.groupby(group_columns, sort=True, dropna=False)
        .agg(
            row_count=("ticker", "size"),
            missing_accession_number_count=("_missing_accession_number", "sum"),
            missing_accepted_datetime_count=("_missing_accepted_datetime", "sum"),
            missing_filed_date_count=("_missing_filed_date", "sum"),
            missing_raw_sha256_count=("_missing_raw_sha256", "sum"),
            missing_source_concept_count=("_missing_source_concept", "sum"),
            missing_pit_grade_count=("_missing_pit_grade", "sum"),
            complete_count=("_complete", "sum"),
        )
        .reset_index()
    )
    examples = (
        normalized.loc[incomplete, [*group_columns, "_first_missing_example"]]
        .drop_duplicates(subset=group_columns, keep="first")
        .rename(columns={"_first_missing_example": "first_missing_example"})
    )
    result = grouped.merge(examples, on=group_columns, how="left")
    result["stage"] = stage
    result["stage_status"] = "OK"
    result["provenance_complete_ratio"] = result["complete_count"] / result["row_count"]
    has_gap = result["complete_count"] < result["row_count"]
    result["downgrade_reason"] = np.where(has_gap, "missing_sec_provenance", "")
    result["first_missing_example"] = result["first_missing_example"].fillna("")
    result["recommended_fix"] = np.where(
        has_gap,
        f"preserve provenance columns through {stage}",
        "",
    )
    return result.loc[:, list(PROVENANCE_GAP_COLUMNS)]


def _normalize_provenance_frame(frame: pd.DataFrame) -> pd.DataFrame:
    aliases = load_ticker_aliases()
    result = pd.DataFrame(index=frame.index)
    result["ticker"] = canonicalize_ticker_series(
        _first_existing(frame, ("ticker", "symbol")),
        aliases=aliases,
    )
    result["feature_id"] = _first_existing(frame, ("feature_id",))
    result["metric_id"] = _first_existing(frame, ("metric_id", "input_metric_ids"))
    result["period"] = _first_existing(frame, ("period", "period_end", "period_type"))
    result["decision_date"] = _first_existing(frame, ("decision_date", "date", "signal_date"))
    result["accession_number"] = _first_existing(
        frame,
        ("accession_number", "source_accession_number", "input_accession_numbers"),
    )
    result["accepted_datetime"] = _first_existing(
        frame,
        (
            "accepted_datetime",
            "filing_acceptance_datetime_utc",
            "acceptance_datetime_utc",
        ),
    )
    result["filed_date"] = _first_existing(frame, ("filed_date", "filing_date"))
    result["raw_sha256"] = _first_existing(
        frame,
        ("raw_sha256", "raw_payload_sha256", "checksum_sha256"),
    )
    result["source_concept"] = _first_existing(
        frame,
        ("source_concept", "concept", "input_metric_ids"),
    )
    result["pit_grade"] = _first_existing(frame, ("pit_grade", "pit_data_grade"))
    return result.fillna("")


def _provenance_required_fields(stage: str) -> tuple[str, ...]:
    if stage == "raw_companyfacts":
        return ("raw_sha256",)
    if stage == "filing_timeline":
        return (
            "accession_number",
            "accepted_datetime",
            "filed_date",
            "raw_sha256",
            "pit_grade",
        )
    return PROVENANCE_AUDIT_FIELDS


def _alias_audit_frame(
    resolutions: list[TickerAliasResolution],
    *,
    baseline: pd.DataFrame,
    price_tickers: set[str],
) -> pd.DataFrame:
    baseline_tickers = _ticker_set(baseline)
    records: list[dict[str, Any]] = []
    for item in resolutions:
        records.append(
            {
                "input_ticker": item.input_ticker,
                "canonical_ticker": item.canonical_ticker,
                "source": item.source,
                "resolved": item.resolved,
                "resolution_reason": item.resolution_reason,
                "used_in_sec_companies": item.used_in_sec_companies,
                "used_in_price_data": (
                    item.canonical_ticker in price_tickers if price_tickers else False
                ),
                "used_in_baseline_score": item.canonical_ticker in baseline_tickers,
                "warning": item.warning,
            }
        )
    return pd.DataFrame(records, columns=list(ALIAS_AUDIT_COLUMNS))


def _candidate_blocking_reasons(
    *,
    rank_ic: float,
    data_quality: float,
    coverage: float,
    stability: float,
    sample_count: float,
    valid_ticker_count: float,
    policy: Any,
) -> list[str]:
    reasons: list[str] = []
    if pd.isna(sample_count) or sample_count < policy.min_sample_count:
        reasons.append("sample_count_below_policy")
    if pd.isna(valid_ticker_count) or valid_ticker_count < policy.min_valid_ticker_count:
        reasons.append("valid_ticker_count_below_policy")
    if pd.isna(coverage) or coverage < policy.min_coverage_ratio:
        reasons.append("coverage_ratio_below_policy")
    if pd.isna(rank_ic) or abs(rank_ic) < policy.min_abs_rank_ic_20d:
        reasons.append("rank_ic_20d_below_policy")
    if pd.isna(stability) or stability < policy.min_stability_score:
        reasons.append("stability_score_below_policy")
    if pd.isna(data_quality) or data_quality < policy.min_pit_quality_score:
        reasons.append("data_quality_score_below_policy")
    return reasons


def _minimum_required_fix(blocking: list[str]) -> str:
    if blocking == ["data_quality_score_below_policy"]:
        return "restore SEC provenance propagation and rerun evaluation"
    if "coverage_ratio_below_policy" in blocking:
        return "increase SEC PIT feature coverage and rerun evaluation"
    if "sample_count_below_policy" in blocking:
        return "extend date range or universe until sample floor is met"
    if "stability_score_below_policy" in blocking:
        return "collect more stable out-of-sample evidence"
    if "rank_ic_20d_below_policy" in blocking:
        return "do not promote; signal strength is below policy"
    return "manual review required"


def _first_provenance_loss_stage(frame: pd.DataFrame) -> str:
    if frame.empty:
        return ""
    ratios: dict[str, float] = {}
    for stage in PROVENANCE_STAGE_ORDER:
        rows = frame.loc[(frame["stage"] == stage) & (frame["stage_status"] == "OK")]
        if rows.empty:
            continue
        total = pd.to_numeric(rows["row_count"], errors="coerce").fillna(0).sum()
        if total <= 0:
            continue
        complete = (
            pd.to_numeric(rows["provenance_complete_ratio"], errors="coerce").fillna(0)
            * pd.to_numeric(rows["row_count"], errors="coerce").fillna(0)
        ).sum()
        ratios[stage] = float(complete / total)
    previous = None
    for stage in PROVENANCE_STAGE_ORDER:
        ratio = ratios.get(stage)
        if ratio is None:
            continue
        if previous is not None and previous - ratio >= 0.20:
            return stage
        if previous is None and ratio < 0.80:
            return stage
        previous = ratio
    for stage, ratio in ratios.items():
        if ratio < 0.80:
            return stage
    return ""


def _missing_provenance_rows(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    counts = pd.to_numeric(frame["row_count"], errors="coerce").fillna(0)
    complete = pd.to_numeric(frame["provenance_complete_ratio"], errors="coerce").fillna(0)
    return int((counts * (1.0 - complete)).sum())


def _provenance_recommended_fix(stage: str) -> str:
    if not stage:
        return "no sharp provenance loss detected"
    return (
        f"preserve accession/accepted/filed/raw/source/pit lineage through {stage} and "
        "rerun SEC PIT evaluation/comparison"
    )


def _top_blocked_features(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    selected = frame.copy()
    selected["_abs_rank_ic_20d"] = pd.to_numeric(
        selected["rank_ic_20d"],
        errors="coerce",
    ).abs()
    selected = selected.sort_values(["_abs_rank_ic_20d", "feature_id"], ascending=[False, True])
    return [
        {
            "feature_id": str(row.get("feature_id") or ""),
            "metric_id": str(row.get("metric_id") or ""),
            "rank_ic_20d": _json_number(row.get("rank_ic_20d")),
            "blocking_reason": str(row.get("blocking_reason") or ""),
        }
        for row in selected.head(10).to_dict(orient="records")
    ]


def _input_tickers(tickers: list[str] | None, summary: dict[str, Any]) -> list[str]:
    if tickers:
        return _dedupe([part for value in tickers for part in str(value).replace(",", " ").split()])
    return _summary_tickers(summary)


def _summary_tickers(summary: dict[str, Any]) -> list[str]:
    metadata = summary.get("metadata") if isinstance(summary, dict) else {}
    tickers = metadata.get("tickers") if isinstance(metadata, dict) else None
    if not isinstance(tickers, list):
        return []
    return _dedupe([str(ticker).upper() for ticker in tickers if str(ticker).strip()])


def _sec_company_tickers() -> set[str]:
    try:
        companies = load_sec_companies(DEFAULT_SEC_COMPANIES_CONFIG_PATH)
    except (FileNotFoundError, ValueError):
        return set()
    return {company.ticker.upper() for company in companies.companies}


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


def _infer_project_root(path: Path) -> Path:
    parts = list(path.resolve().parts)
    marker = ("data", "processed")
    for index in range(len(parts) - 1):
        if tuple(parts[index : index + 2]) == marker:
            return Path(*parts[:index])
    return PROJECT_ROOT


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


def _read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists() or not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _write_csv(frame: pd.DataFrame, path: Path, columns: tuple[str, ...]) -> None:
    output = frame.copy() if not frame.empty else _empty_frame(columns)
    for column in columns:
        if column not in output.columns:
            output[column] = ""
    path.parent.mkdir(parents=True, exist_ok=True)
    output.loc[:, list(columns)].to_csv(path, index=False)


def _empty_frame(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _first_existing(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.Series:
    for column in columns:
        if column in frame.columns:
            return frame[column].astype(str)
    return pd.Series([""] * len(frame), index=frame.index, dtype="object")


def _first_existing_column(frame: pd.DataFrame, columns: tuple[str, ...]) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None


def _string_column(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.Series:
    return _first_existing(frame, columns).astype(str)


def _provenance_complete_mask(frame: pd.DataFrame) -> pd.Series:
    mask = pd.Series(True, index=frame.index)
    for column in PROVENANCE_AUDIT_FIELDS:
        mask &= frame[column].astype(str).str.strip().astype(bool)
    return mask


def _missing_count(frame: pd.DataFrame, column: str) -> int:
    return int((~frame[column].astype(str).str.strip().astype(bool)).sum())


def _first_missing_example(frame: pd.DataFrame) -> str:
    for row in frame.to_dict(orient="records"):
        missing = [
            field for field in PROVENANCE_AUDIT_FIELDS if not str(row.get(field) or "").strip()
        ]
        if missing:
            return json.dumps(
                {"missing_fields": missing, "row": row},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
    return ""


def _row_missing_example(row: pd.Series) -> str:
    missing = [field for field in PROVENANCE_AUDIT_FIELDS if not str(row.get(field) or "").strip()]
    if not missing:
        return ""
    return json.dumps(
        {
            "ticker": row.get("ticker", ""),
            "feature_id": row.get("feature_id", ""),
            "metric_id": row.get("metric_id", ""),
            "decision_date": row.get("decision_date", ""),
            "missing": missing,
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def _date_series(frame: pd.DataFrame) -> pd.Series:
    column = _first_existing_column(frame, ("decision_date", "date", "signal_date"))
    if column is None:
        return pd.Series([pd.NaT] * len(frame), index=frame.index)
    return pd.to_datetime(frame[column], errors="coerce").dt.date


def _affected_ticker_count(frame: pd.DataFrame, mask: pd.Series) -> int:
    column = _first_existing_column(frame, ("ticker", "symbol"))
    if column is None:
        return 0
    return int(frame.loc[mask, column].dropna().astype(str).str.upper().nunique())


def _missing_label_record(label: str, source_path: Path, reason: str) -> dict[str, Any]:
    return {
        "label_name": label,
        "required": True,
        "available_count": 0,
        "missing_count": 0,
        "coverage_ratio": 0.0,
        "first_missing_date": "",
        "last_missing_date": "",
        "affected_ticker_count": 0,
        "source_artifact": str(source_path),
        "recommended_fix": reason,
    }


def _ticker_set(frame: pd.DataFrame) -> set[str]:
    column = _first_existing_column(frame, ("ticker", "symbol"))
    if frame.empty or column is None:
        return set()
    aliases = load_ticker_aliases()
    return set(canonicalize_ticker_series(frame[column], aliases=aliases).dropna().tolist())


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip().upper()
        if normalized and normalized not in seen:
            result.append(normalized)
            seen.add(normalized)
    return result


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


def _float_or_nan(value: object) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return np.nan
    return number


def _float_or_zero(value: object) -> float:
    number = _float_or_nan(value)
    return 0.0 if pd.isna(number) else number


def _json_number(value: object) -> float | None:
    number = _float_or_nan(value)
    return None if pd.isna(number) else number


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


def _file_sha256(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _join_nonempty(values: list[str], *, separator: str) -> str:
    return separator.join(value for value in values if value)
