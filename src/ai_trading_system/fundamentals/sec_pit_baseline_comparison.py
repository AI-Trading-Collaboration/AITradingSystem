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
from ai_trading_system.fundamentals.sec_pit_aliases import (
    canonicalize_ticker_series,
    load_ticker_aliases,
)
from ai_trading_system.fundamentals.sec_pit_backfill import SEC_PIT_BACKTEST_DATA_GRADE

SEC_PIT_BASELINE_COMPARISON_TASK_ID = "TRADING-041"
SEC_PIT_BASELINE_COMPARISON_REPORT_TYPE = "sec_pit_baseline_comparison"
SEC_PIT_BASELINE_COMPARISON_PRODUCTION_EFFECT = "none"

DEFAULT_SEC_PIT_BASELINE_COMPARISON_OUTPUT_DIR = (
    PROJECT_ROOT / "outputs" / "sec_pit_baseline_comparison"
)
DEFAULT_SEC_PIT_EVALUATION_DIR = PROJECT_ROOT / "outputs" / "sec_pit_evaluation"
DEFAULT_BASELINE_SCORE_DIR = PROJECT_ROOT / "outputs" / "daily_score"
DEFAULT_PROCESSED_BASELINE_SCORE_PATH = PROJECT_ROOT / "data" / "processed" / "scores_daily.csv"

COMPARISON_STATUSES: tuple[str, ...] = (
    "OK",
    "LIMITED_BASELINE_MISSING",
    "LIMITED_SEC_PIT_EVALUATION_MISSING",
    "INSUFFICIENT_OVERLAP",
    "FAILED_VALIDATION",
)

DECISION_IMPACT_COLUMNS: tuple[str, ...] = (
    "decision_date",
    "ticker",
    "baseline_score",
    "sec_pit_enhanced_score",
    "score_delta",
    "baseline_rank",
    "sec_pit_rank",
    "rank_delta",
    "baseline_action",
    "sec_pit_suggested_action",
    "action_changed",
    "top_positive_sec_pit_features",
    "top_negative_sec_pit_features",
    "forward_return_20d",
    "relative_return_vs_QQQ_20d",
    "max_drawdown_forward_20d",
    "accession_number",
    "accepted_datetime",
    "filed_date",
    "form",
    "period",
    "source_concept",
    "source_taxonomy",
    "raw_sha256",
    "source_url_or_raw_path",
    "pit_grade",
    "available_time",
    "source_lineage",
    "manual_review_required",
    "production_effect",
)

RANK_SHIFT_COLUMNS: tuple[str, ...] = (
    "decision_date",
    "ticker",
    "baseline_rank",
    "sec_pit_rank",
    "rank_delta",
    "abs_rank_delta",
    "score_delta",
    "primary_positive_feature",
    "primary_negative_feature",
    "explanation",
)

INCREMENTAL_ALPHA_COLUMNS: tuple[str, ...] = (
    "bucket",
    "sample_count",
    "avg_forward_return_20d",
    "avg_relative_return_vs_QQQ_20d",
    "avg_max_drawdown_forward_20d",
    "hit_rate_20d",
    "baseline_avg_forward_return_20d",
    "sec_pit_avg_forward_return_20d",
    "incremental_return_20d",
    "drawdown_improvement_20d",
    "interpretation",
)

INCREMENTAL_ALPHA_BUCKETS: tuple[str, ...] = (
    "top_baseline",
    "top_sec_pit",
    "promoted_by_sec_pit",
    "downgraded_by_sec_pit",
    "unchanged",
)

# Display-only TRADING-041 triage thresholds. They do not affect production
# scores, position gates, approved overlays, or order actions.
MATERIAL_RANK_SHIFT_ABS_MIN = 2
TOP_BUCKET_FRACTION = 0.20
SEC_PIT_SCORE_DELTA_SCALE = 100.0
ACTION_POSITIVE_SCORE_MIN = 70.0
ACTION_WATCH_SCORE_MIN = 50.0


@dataclass(frozen=True)
class SecPitBaselineComparisonArtifacts:
    status: str
    summary_json_path: Path
    summary_markdown_path: Path
    decision_impact_path: Path
    rank_shift_path: Path
    incremental_alpha_path: Path
    run_log_path: Path


@dataclass(frozen=True)
class _SecPitEvaluationInputs:
    summary_path: Path
    feature_effectiveness_path: Path
    signal_attribution_path: Path
    shadow_candidate_weights_path: Path
    summary: dict[str, Any]
    feature_effectiveness: pd.DataFrame
    signal_attribution: pd.DataFrame
    shadow_candidate_weights: pd.DataFrame

    @property
    def exists(self) -> bool:
        return bool(self.summary) and not self.signal_attribution.empty


@dataclass(frozen=True)
class _BaselineInputs:
    path: Path
    frame: pd.DataFrame
    status: str = "OK"

    @property
    def exists(self) -> bool:
        return not self.frame.empty

    @property
    def rows(self) -> int:
        return int(len(self.frame))

    @property
    def date_range(self) -> list[str]:
        if self.frame.empty:
            return []
        date_column = _first_existing_column(self.frame, ("decision_date", "as_of", "date"))
        if date_column is None:
            return []
        values = pd.to_datetime(self.frame[date_column], errors="coerce").dropna()
        if values.empty:
            return []
        return [values.min().date().isoformat(), values.max().date().isoformat()]

    @property
    def ticker_count(self) -> int:
        if self.frame.empty:
            return 0
        ticker_column = _first_existing_column(self.frame, ("ticker", "symbol"))
        if ticker_column is None:
            return 0
        return int(self.frame[ticker_column].dropna().astype(str).str.upper().nunique())


def run_sec_pit_baseline_comparison(
    *,
    start: date,
    end: date,
    sec_pit_evaluation_dir: Path = DEFAULT_SEC_PIT_EVALUATION_DIR,
    baseline_score_dir: Path = DEFAULT_BASELINE_SCORE_DIR,
    baseline_score_path: Path | None = None,
    benchmark: str = "QQQ",
    output_dir: Path = DEFAULT_SEC_PIT_BASELINE_COMPARISON_OUTPUT_DIR,
    tickers: list[str] | None = None,
) -> SecPitBaselineComparisonArtifacts:
    if start > end:
        raise ValueError("start must be on or before end")

    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = end.isoformat()
    summary_json_path = output_dir / f"sec_pit_baseline_comparison_summary_{suffix}.json"
    summary_markdown_path = output_dir / f"sec_pit_baseline_comparison_summary_{suffix}.md"
    decision_impact_path = output_dir / f"sec_pit_decision_impact_{suffix}.csv"
    rank_shift_path = output_dir / f"sec_pit_rank_shift_{suffix}.csv"
    incremental_alpha_path = output_dir / f"sec_pit_incremental_alpha_{suffix}.csv"
    run_log_path = output_dir / "run.log"

    limitations: list[str] = []
    status = "OK"

    try:
        sec_inputs = _load_sec_pit_evaluation_inputs(sec_pit_evaluation_dir, end)
        baseline_inputs = _load_baseline_inputs(
            baseline_score_dir,
            end,
            baseline_score_path=baseline_score_path,
        )
        if not sec_inputs.exists:
            status = "LIMITED_SEC_PIT_EVALUATION_MISSING"
            limitations.append("SEC PIT evaluation artifacts are missing or incomplete.")
            impact = _empty_decision_impact()
            rank_shift = _empty_rank_shift()
            incremental_alpha = _empty_incremental_alpha()
        elif not baseline_inputs.exists:
            status = "LIMITED_BASELINE_MISSING"
            limitations.append("Baseline score artifacts are missing or contain no usable rows.")
            impact = _empty_decision_impact()
            rank_shift = _empty_rank_shift()
            incremental_alpha = _empty_incremental_alpha()
        else:
            if baseline_inputs.status == "FALLBACK_USED":
                limitations.append(
                    "baseline_artifact_status=FALLBACK_USED; default outputs/daily_score "
                    "artifact was unavailable, so data/processed/scores_daily.csv was used."
                )
            attribution, exclusion_counts = _normalize_signal_attribution(
                sec_inputs.signal_attribution,
                start=start,
                end=end,
                tickers=tickers,
            )
            limitations.extend(_pit_exclusion_limitations(exclusion_counts))
            active_tickers = _active_tickers(attribution, tickers)
            baseline = _normalize_baseline_scores(
                baseline_inputs.frame,
                start=start,
                end=end,
                tickers=active_tickers,
            )
            impact = _decision_impact(attribution, baseline, benchmark=benchmark)
            if impact.empty:
                status = "INSUFFICIENT_OVERLAP"
                limitations.append(
                    "No overlapping decision_date/ticker rows between baseline and SEC PIT "
                    "evaluation artifacts."
                )
                rank_shift = _empty_rank_shift()
                incremental_alpha = _empty_incremental_alpha()
            else:
                rank_shift = _rank_shift(impact)
                incremental_alpha = _incremental_alpha(impact)
    except (KeyError, ValueError, pd.errors.ParserError) as exc:
        status = "FAILED_VALIDATION"
        limitations.append(f"Input artifact validation failed: {exc}")
        sec_inputs = _empty_sec_inputs(sec_pit_evaluation_dir, end)
        baseline_inputs = _empty_baseline_inputs(baseline_score_dir)
        impact = _empty_decision_impact()
        rank_shift = _empty_rank_shift()
        incremental_alpha = _empty_incremental_alpha()

    _write_csv(impact, decision_impact_path, DECISION_IMPACT_COLUMNS)
    _write_csv(rank_shift, rank_shift_path, RANK_SHIFT_COLUMNS)
    _write_csv(incremental_alpha, incremental_alpha_path, INCREMENTAL_ALPHA_COLUMNS)
    summary = _summary_payload(
        status=status,
        start=start,
        end=end,
        benchmark=benchmark.upper(),
        sec_inputs=sec_inputs,
        baseline_inputs=baseline_inputs,
        impact=impact,
        rank_shift=rank_shift,
        incremental_alpha=incremental_alpha,
        limitations=limitations,
        artifacts={
            "summary_json": summary_json_path,
            "summary_markdown": summary_markdown_path,
            "decision_impact_csv": decision_impact_path,
            "rank_shift_csv": rank_shift_path,
            "incremental_alpha_csv": incremental_alpha_path,
        },
    )
    summary_json_path.write_text(
        json.dumps(_json_value(summary), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_markdown_path.write_text(
        render_sec_pit_baseline_comparison_summary(summary, impact, rank_shift, incremental_alpha),
        encoding="utf-8",
    )
    _write_run_log(run_log_path, summary, summary_json_path, summary_markdown_path)
    return SecPitBaselineComparisonArtifacts(
        status=status,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        decision_impact_path=decision_impact_path,
        rank_shift_path=rank_shift_path,
        incremental_alpha_path=incremental_alpha_path,
        run_log_path=run_log_path,
    )


def render_sec_pit_baseline_comparison_summary(
    summary: dict[str, Any],
    impact: pd.DataFrame | None = None,
    rank_shift: pd.DataFrame | None = None,
    incremental_alpha: pd.DataFrame | None = None,
) -> str:
    impact_frame = impact if impact is not None else pd.DataFrame()
    shift_frame = rank_shift if rank_shift is not None else pd.DataFrame()
    alpha_frame = incremental_alpha if incremental_alpha is not None else pd.DataFrame()
    positive_shifts = (
        shift_frame.sort_values(["rank_delta", "ticker"], ascending=[False, True]).head(10)
        if not shift_frame.empty
        else pd.DataFrame()
    )
    negative_shifts = (
        shift_frame.sort_values(["rank_delta", "ticker"], ascending=[True, True]).head(10)
        if not shift_frame.empty
        else pd.DataFrame()
    )
    action_changes = (
        impact_frame.loc[impact_frame["action_changed"].astype(str).str.lower() == "true"]
        if not impact_frame.empty
        else pd.DataFrame()
    )
    lines = [
        "# SEC PIT Baseline Comparison Summary",
        "",
        "## Metadata",
        f"- generated_at: {summary['generated_at']}",
        f"- start_date: {summary['start_date']}",
        f"- end_date: {summary['end_date']}",
        f"- baseline source: {summary['baseline_source']}",
        f"- baseline artifact status: {summary.get('baseline_artifact_status', 'UNKNOWN')}",
        f"- baseline rows: {summary.get('baseline_rows', 0)}",
        f"- SEC PIT evaluation source: {summary['sec_pit_evaluation_source']}",
        f"- comparison status: {summary['comparison_status']}",
        "",
        "## Executive Summary",
        f"- forward return ranking: {_ranking_interpretation(summary)}",
        f"- drawdown avoidance: {_drawdown_interpretation(summary)}",
        (
            "- action changes: "
            f"{summary['action_changed_count']} changed rows; all require manual review."
        ),
        "",
        "## Incremental Alpha",
    ]
    lines.extend(_alpha_table_lines(alpha_frame))
    lines.extend(
        [
            "",
            "## Decision Impact",
            "### largest positive rank shifts",
        ]
    )
    lines.extend(_shift_table_lines(positive_shifts))
    lines.extend(["", "### largest negative rank shifts"])
    lines.extend(_shift_table_lines(negative_shifts))
    lines.extend(["", "### tickers with action changes"])
    lines.extend(_action_change_lines(action_changes))
    lines.extend(
        [
            "",
            "## Feature Drivers",
            "### top positive SEC PIT features",
        ]
    )
    lines.extend(_feature_driver_lines(summary.get("top_positive_features", [])))
    lines.extend(["", "### top negative SEC PIT features"])
    lines.extend(_feature_driver_lines(summary.get("top_negative_features", [])))
    lines.extend(["", "### unstable or research-only features"])
    lines.extend(_feature_driver_lines(summary.get("research_only_features", [])))
    lines.extend(
        [
            "",
            "## PIT Safety",
            f"- pit grade policy: {SEC_PIT_BACKTEST_DATA_GRADE}",
            "- manual review requirement: true",
            f"- production effect policy: {SEC_PIT_BASELINE_COMPARISON_PRODUCTION_EFFECT}",
            "- available_time policy: rows with available_time > decision_date are excluded.",
            "",
            "## Limitations",
        ]
    )
    if summary.get("limitations"):
        lines.extend(f"- {item}" for item in summary["limitations"])
    else:
        lines.append("- No additional limitations beyond B-grade reconstructed PIT status.")
    lines.extend(
        [
            "",
            "## Manual Review Checklist",
            "- 复核 promoted / downgraded ticker 的 SEC filing provenance 和特征方向。",
            "- 确认 action changes 只作为人工复核队列，不进入 production action。",
            "- 对照 price-only / score-daily baseline 的样本覆盖，确认 overlap 足够。",
            "- 若要进入 shadow iteration，先建立独立 shadow 权重实验和 promotion gate。",
        ]
    )
    return "\n".join(lines) + "\n"


def _load_sec_pit_evaluation_inputs(root: Path, end: date) -> _SecPitEvaluationInputs:
    summary_path = _latest_dated_path(root, "sec_pit_evaluation_summary_", ".json", end)
    feature_path = _latest_dated_path(root, "sec_pit_feature_effectiveness_", ".csv", end)
    attribution_path = _latest_dated_path(root, "sec_pit_signal_attribution_", ".csv", end)
    weights_path = _latest_dated_path(root, "sec_pit_shadow_candidate_weights_", ".csv", end)
    if not summary_path.exists():
        return _empty_sec_inputs(root, end)
    summary = _read_json_object(summary_path)
    feature_effectiveness = _read_csv_or_empty(feature_path)
    attribution = _read_csv_or_empty(attribution_path)
    weights = _read_csv_or_empty(weights_path)
    return _SecPitEvaluationInputs(
        summary_path=summary_path,
        feature_effectiveness_path=feature_path,
        signal_attribution_path=attribution_path,
        shadow_candidate_weights_path=weights_path,
        summary=summary,
        feature_effectiveness=feature_effectiveness,
        signal_attribution=attribution,
        shadow_candidate_weights=weights,
    )


def _empty_sec_inputs(root: Path, end: date) -> _SecPitEvaluationInputs:
    suffix = end.isoformat()
    return _SecPitEvaluationInputs(
        summary_path=root / f"sec_pit_evaluation_summary_{suffix}.json",
        feature_effectiveness_path=root / f"sec_pit_feature_effectiveness_{suffix}.csv",
        signal_attribution_path=root / f"sec_pit_signal_attribution_{suffix}.csv",
        shadow_candidate_weights_path=root / f"sec_pit_shadow_candidate_weights_{suffix}.csv",
        summary={},
        feature_effectiveness=pd.DataFrame(),
        signal_attribution=pd.DataFrame(),
        shadow_candidate_weights=pd.DataFrame(),
    )


def _load_baseline_inputs(
    root: Path,
    end: date,
    *,
    baseline_score_path: Path | None = None,
) -> _BaselineInputs:
    if baseline_score_path is not None:
        frame = _read_csv_or_empty(baseline_score_path)
        status = "OK" if not frame.empty else "LIMITED_BASELINE_MISSING"
        return _BaselineInputs(path=baseline_score_path, frame=frame, status=status)

    path = _baseline_path(root, end)
    frame = _read_csv_or_empty(path)
    if not frame.empty:
        return _BaselineInputs(path=path, frame=frame, status="OK")

    if _is_default_baseline_score_dir(root) and DEFAULT_PROCESSED_BASELINE_SCORE_PATH.exists():
        fallback_frame = _read_csv_or_empty(DEFAULT_PROCESSED_BASELINE_SCORE_PATH)
        if not fallback_frame.empty:
            return _BaselineInputs(
                path=DEFAULT_PROCESSED_BASELINE_SCORE_PATH,
                frame=fallback_frame,
                status="FALLBACK_USED",
            )
    return _BaselineInputs(path=path, frame=frame, status="LIMITED_BASELINE_MISSING")


def _empty_baseline_inputs(root: Path) -> _BaselineInputs:
    return _BaselineInputs(path=root, frame=pd.DataFrame(), status="LIMITED_BASELINE_MISSING")


def _is_default_baseline_score_dir(root: Path) -> bool:
    path = Path(root)
    normalized = path if path.is_absolute() else PROJECT_ROOT / path
    return normalized.resolve() == DEFAULT_BASELINE_SCORE_DIR.resolve()


def _baseline_path(root: Path, end: date) -> Path:
    if root.is_file():
        return root
    if not root.exists():
        return root / "scores_daily.csv"
    direct_candidates = [
        root / "scores_daily.csv",
        root / f"scores_daily_{end.isoformat()}.csv",
        root / f"daily_score_baseline_{end.isoformat()}.csv",
        root / f"baseline_scores_{end.isoformat()}.csv",
    ]
    for path in direct_candidates:
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


def _latest_dated_path(root: Path, prefix: str, suffix: str, end: date) -> Path:
    default_path = root / f"{prefix}{end.isoformat()}{suffix}"
    if not root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"{prefix}*{suffix}"):
        raw_date = path.stem.removeprefix(prefix)
        parsed = _parse_date(raw_date)
        if parsed is not None and parsed <= end:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _normalize_signal_attribution(
    frame: pd.DataFrame,
    *,
    start: date,
    end: date,
    tickers: list[str] | None,
) -> tuple[pd.DataFrame, Counter[str]]:
    required = {"decision_date", "ticker", "feature_id", "contribution", "available_time"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("SEC PIT signal attribution missing columns: " + ", ".join(missing))
    normalized = frame.copy().fillna("")
    normalized["decision_date"] = normalized["decision_date"].astype(str)
    normalized["_decision_date"] = pd.to_datetime(
        normalized["decision_date"],
        errors="coerce",
    ).dt.date
    normalized["_available_time"] = pd.to_datetime(
        normalized["available_time"],
        errors="coerce",
        utc=True,
    )
    aliases = load_ticker_aliases()
    normalized["ticker"] = canonicalize_ticker_series(normalized["ticker"], aliases=aliases)
    normalized["feature_id"] = normalized["feature_id"].astype(str)
    normalized["_contribution"] = pd.to_numeric(
        normalized["contribution"],
        errors="coerce",
    ).fillna(0.0)
    normalized["_forward_return_20d"] = _numeric_column(normalized, "forward_return_20d")
    relative_column = "relative_return_vs_QQQ_20d"
    if relative_column not in normalized.columns:
        relative_candidates = [
            column
            for column in normalized.columns
            if column.startswith("relative_return_vs_") and column.endswith("_20d")
        ]
        relative_column = relative_candidates[0] if relative_candidates else ""
    normalized["_relative_return_20d"] = (
        _numeric_column(normalized, relative_column) if relative_column else np.nan
    )
    normalized["_max_drawdown_forward_20d"] = _numeric_column(
        normalized,
        "max_drawdown_forward_20d",
    )
    if "pit_grade" not in normalized.columns:
        normalized["pit_grade"] = SEC_PIT_BACKTEST_DATA_GRADE
    for column in (
        "accession_number",
        "accepted_datetime",
        "filed_date",
        "form",
        "period",
        "source_concept",
        "source_taxonomy",
        "raw_sha256",
        "source_url_or_raw_path",
        "source_lineage",
    ):
        if column not in normalized.columns:
            normalized[column] = ""

    exclusions: Counter[str] = Counter()
    valid = normalized["_decision_date"].notna()
    exclusions["invalid_decision_date"] = int((~valid).sum())
    valid &= normalized["_decision_date"].map(lambda value: start <= value <= end)
    if tickers:
        allowed = {ticker.upper() for ticker in tickers}
        ticker_mask = normalized["ticker"].isin(allowed)
        exclusions["ticker_excluded"] = int((valid & ~ticker_mask).sum())
        valid &= ticker_mask
    missing_available = normalized["_available_time"].isna()
    future_available = normalized.apply(
        lambda row: (
            False
            if pd.isna(row["_available_time"]) or pd.isna(row["_decision_date"])
            else row["_available_time"].date() > row["_decision_date"]
        ),
        axis=1,
    )
    exclusions["missing_available_time"] = int((valid & missing_available).sum())
    exclusions["future_available_time"] = int((valid & future_available).sum())
    valid &= ~missing_available & ~future_available
    return (
        normalized.loc[valid].sort_values(
            ["decision_date", "ticker", "feature_id"],
        ),
        exclusions,
    )


def _normalize_baseline_scores(
    frame: pd.DataFrame,
    *,
    start: date,
    end: date,
    tickers: list[str],
) -> pd.DataFrame:
    baseline_columns = ["decision_date", "ticker", "baseline_score", "baseline_action"]
    if frame.empty:
        return pd.DataFrame(columns=baseline_columns)
    date_column = _first_existing_column(frame, ("decision_date", "as_of", "date"))
    score_column = _first_existing_column(
        frame,
        ("baseline_score", "score", "overall_score", "risk_adjusted_score"),
    )
    if date_column is None or score_column is None:
        raise ValueError("baseline score artifact must include a date and score column")
    normalized = frame.copy().fillna("")
    aliases = load_ticker_aliases()
    if "component" in normalized.columns:
        overall = normalized.loc[normalized["component"].astype(str) == "overall"].copy()
        if not overall.empty:
            normalized = overall
    normalized["_decision_date"] = pd.to_datetime(
        normalized[date_column].astype(str),
        errors="coerce",
    ).dt.date
    normalized["_score"] = pd.to_numeric(normalized[score_column], errors="coerce")
    normalized = normalized.loc[
        normalized["_decision_date"].notna()
        & normalized["_score"].notna()
        & normalized["_decision_date"].map(lambda value: start <= value <= end)
    ].copy()
    if normalized.empty:
        return pd.DataFrame(columns=baseline_columns)

    ticker_column = _first_existing_column(normalized, ("ticker", "symbol"))
    action_column = _first_existing_column(normalized, ("baseline_action", "action", "label"))
    records: list[dict[str, Any]] = []
    for row in normalized.to_dict(orient="records"):
        raw_ticker = str(row.get(ticker_column or "") or "").upper()
        row_ticker = aliases.get(raw_ticker, raw_ticker)
        row_tickers = [row_ticker] if row_ticker else tickers
        score = float(row["_score"])
        action = str(row.get(action_column or "") or "") or _action_from_score(score)
        for ticker in row_tickers:
            records.append(
                {
                    "decision_date": row["_decision_date"].isoformat(),
                    "ticker": ticker,
                    "baseline_score": score,
                    "baseline_action": action,
                }
            )
    result = pd.DataFrame(records)
    return result.sort_values(["decision_date", "ticker"]).reset_index(drop=True)


def _decision_impact(
    attribution: pd.DataFrame,
    baseline: pd.DataFrame,
    *,
    benchmark: str,
) -> pd.DataFrame:
    if attribution.empty or baseline.empty:
        return _empty_decision_impact()
    sec = _aggregate_sec_contributions(attribution, benchmark=benchmark)
    merged = baseline.merge(sec, on=["decision_date", "ticker"], how="inner")
    if merged.empty:
        return _empty_decision_impact()
    merged["score_delta"] = merged["sec_pit_contribution"] * SEC_PIT_SCORE_DELTA_SCALE
    merged["sec_pit_enhanced_score"] = merged["baseline_score"] + merged["score_delta"]
    merged = _assign_rank(merged, "baseline_score", "baseline_rank")
    merged = _assign_rank(merged, "sec_pit_enhanced_score", "sec_pit_rank")
    merged["rank_delta"] = merged["baseline_rank"] - merged["sec_pit_rank"]
    merged["sec_pit_suggested_action"] = merged["sec_pit_enhanced_score"].map(
        _action_from_score,
    )
    merged["action_changed"] = merged["baseline_action"].astype(str) != merged[
        "sec_pit_suggested_action"
    ].astype(str)
    merged["manual_review_required"] = True
    merged["production_effect"] = SEC_PIT_BASELINE_COMPARISON_PRODUCTION_EFFECT
    result = merged.loc[:, DECISION_IMPACT_COLUMNS].sort_values(
        ["decision_date", "sec_pit_rank", "ticker"],
    )
    return result.reset_index(drop=True)


def _aggregate_sec_contributions(attribution: pd.DataFrame, *, benchmark: str) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for (decision_date, ticker), group in attribution.groupby(["decision_date", "ticker"]):
        positive_features = _feature_contribution_text(group, positive=True)
        negative_features = _feature_contribution_text(group, positive=False)
        records.append(
            {
                "decision_date": decision_date,
                "ticker": ticker,
                "sec_pit_contribution": float(group["_contribution"].sum()),
                "top_positive_sec_pit_features": positive_features,
                "top_negative_sec_pit_features": negative_features,
                "forward_return_20d": _mean_or_nan(group["_forward_return_20d"]),
                f"relative_return_vs_{benchmark.upper()}_20d": _mean_or_nan(
                    group["_relative_return_20d"],
                ),
                "relative_return_vs_QQQ_20d": _mean_or_nan(group["_relative_return_20d"]),
                "max_drawdown_forward_20d": _mean_or_nan(group["_max_drawdown_forward_20d"]),
                "accession_number": _unique_join(group["accession_number"]),
                "accepted_datetime": _unique_join(group["accepted_datetime"]),
                "filed_date": _unique_join(group["filed_date"]),
                "form": _unique_join(group["form"]),
                "period": _unique_join(group["period"]),
                "source_concept": _unique_join(group["source_concept"]),
                "source_taxonomy": _unique_join(group["source_taxonomy"]),
                "raw_sha256": _unique_join(group["raw_sha256"]),
                "source_url_or_raw_path": _unique_join(group["source_url_or_raw_path"]),
                "pit_grade": _dominant_value(group["pit_grade"]),
                "available_time": _unique_join(group["available_time"]),
                "source_lineage": _source_lineage_join(group["source_lineage"]),
            }
        )
    return pd.DataFrame(records).sort_values(["decision_date", "ticker"]).reset_index(drop=True)


def _rank_shift(impact: pd.DataFrame) -> pd.DataFrame:
    if impact.empty:
        return _empty_rank_shift()
    records: list[dict[str, Any]] = []
    for row in impact.to_dict(orient="records"):
        rank_delta = int(row["rank_delta"])
        positive = _first_feature(str(row.get("top_positive_sec_pit_features") or ""))
        negative = _first_feature(str(row.get("top_negative_sec_pit_features") or ""))
        records.append(
            {
                "decision_date": row["decision_date"],
                "ticker": row["ticker"],
                "baseline_rank": row["baseline_rank"],
                "sec_pit_rank": row["sec_pit_rank"],
                "rank_delta": rank_delta,
                "abs_rank_delta": abs(rank_delta),
                "score_delta": row["score_delta"],
                "primary_positive_feature": positive,
                "primary_negative_feature": negative,
                "explanation": _rank_shift_explanation(
                    row["ticker"],
                    rank_delta,
                    positive,
                    negative,
                ),
            }
        )
    frame = pd.DataFrame(records, columns=list(RANK_SHIFT_COLUMNS))
    return frame.sort_values(
        ["decision_date", "abs_rank_delta", "ticker"],
        ascending=[True, False, True],
    ).reset_index(drop=True)


def _incremental_alpha(impact: pd.DataFrame) -> pd.DataFrame:
    if impact.empty:
        return _empty_incremental_alpha()
    frames: dict[str, pd.DataFrame] = {}
    frames["top_baseline"] = _top_bucket(impact, "baseline_rank")
    frames["top_sec_pit"] = _top_bucket(impact, "sec_pit_rank")
    frames["promoted_by_sec_pit"] = impact.loc[impact["rank_delta"] > 0].copy()
    frames["downgraded_by_sec_pit"] = impact.loc[impact["rank_delta"] < 0].copy()
    frames["unchanged"] = impact.loc[impact["rank_delta"] == 0].copy()
    baseline_top_return = _mean_or_nan(frames["top_baseline"]["forward_return_20d"])
    sec_top_return = _mean_or_nan(frames["top_sec_pit"]["forward_return_20d"])
    baseline_top_drawdown = _mean_or_nan(frames["top_baseline"]["max_drawdown_forward_20d"])
    sec_top_drawdown = _mean_or_nan(frames["top_sec_pit"]["max_drawdown_forward_20d"])
    records: list[dict[str, Any]] = []
    for bucket in INCREMENTAL_ALPHA_BUCKETS:
        frame = frames[bucket]
        avg_return = _mean_or_nan(frame["forward_return_20d"])
        avg_drawdown = _mean_or_nan(frame["max_drawdown_forward_20d"])
        baseline_avg, sec_avg = _bucket_baseline_sec_returns(
            bucket,
            avg_return,
            baseline_top_return,
            sec_top_return,
        )
        drawdown_improvement = _drawdown_improvement(
            bucket,
            avg_drawdown,
            baseline_top_drawdown,
            sec_top_drawdown,
        )
        records.append(
            {
                "bucket": bucket,
                "sample_count": int(len(frame)),
                "avg_forward_return_20d": avg_return,
                "avg_relative_return_vs_QQQ_20d": _mean_or_nan(
                    frame["relative_return_vs_QQQ_20d"],
                ),
                "avg_max_drawdown_forward_20d": avg_drawdown,
                "hit_rate_20d": _hit_rate(frame),
                "baseline_avg_forward_return_20d": baseline_avg,
                "sec_pit_avg_forward_return_20d": sec_avg,
                "incremental_return_20d": sec_avg - baseline_avg,
                "drawdown_improvement_20d": drawdown_improvement,
                "interpretation": _bucket_interpretation(bucket, sec_avg - baseline_avg),
            }
        )
    return pd.DataFrame(records, columns=list(INCREMENTAL_ALPHA_COLUMNS))


def _summary_payload(
    *,
    status: str,
    start: date,
    end: date,
    benchmark: str,
    sec_inputs: _SecPitEvaluationInputs,
    baseline_inputs: _BaselineInputs,
    impact: pd.DataFrame,
    rank_shift: pd.DataFrame,
    incremental_alpha: pd.DataFrame,
    limitations: list[str],
    artifacts: dict[str, Path],
) -> dict[str, Any]:
    if status not in COMPARISON_STATUSES:
        raise ValueError(f"unsupported comparison_status: {status}")
    action_changed_count = (
        int(impact["action_changed"].astype(bool).sum()) if not impact.empty else 0
    )
    material_rank_shift_count = (
        int((rank_shift["abs_rank_delta"] >= MATERIAL_RANK_SHIFT_ABS_MIN).sum())
        if not rank_shift.empty
        else 0
    )
    top_baseline = _alpha_row(incremental_alpha, "top_baseline")
    top_sec_pit = _alpha_row(incremental_alpha, "top_sec_pit")
    incremental_alpha_20d = _float_or_zero(
        top_sec_pit.get("avg_forward_return_20d")
    ) - _float_or_zero(top_baseline.get("avg_forward_return_20d"))
    drawdown_improvement_20d = _float_or_zero(
        top_sec_pit.get("avg_max_drawdown_forward_20d")
    ) - _float_or_zero(top_baseline.get("avg_max_drawdown_forward_20d"))
    feature_drivers = _feature_driver_summary(impact)
    return {
        "schema_version": "1.0",
        "report_type": SEC_PIT_BASELINE_COMPARISON_REPORT_TYPE,
        "task_id": SEC_PIT_BASELINE_COMPARISON_TASK_ID,
        "generated_at": _deterministic_generated_at(end),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "comparison_status": status,
        "baseline_source": str(baseline_inputs.path),
        "baseline_artifact_path": str(baseline_inputs.path),
        "baseline_artifact_status": baseline_inputs.status,
        "baseline_rows": baseline_inputs.rows,
        "baseline_date_range": baseline_inputs.date_range,
        "baseline_ticker_count": baseline_inputs.ticker_count,
        "sec_pit_evaluation_source": str(sec_inputs.summary_path),
        "benchmark": benchmark,
        "universe_size": int(impact["ticker"].nunique()) if not impact.empty else 0,
        "decision_count": int(len(impact)),
        "action_changed_count": action_changed_count,
        "material_rank_shift_count": material_rank_shift_count,
        "incremental_alpha_20d": incremental_alpha_20d,
        "drawdown_improvement_20d": drawdown_improvement_20d,
        "top_promoted_tickers": _top_promoted_tickers(rank_shift),
        "top_downgraded_tickers": _top_downgraded_tickers(rank_shift),
        "top_positive_features": feature_drivers["positive"],
        "top_negative_features": feature_drivers["negative"],
        "research_only_features": _research_only_features(sec_inputs),
        "limitations": _comparison_limitations(status, limitations),
        "safety": {
            "pit_grade_policy": SEC_PIT_BACKTEST_DATA_GRADE,
            "available_time_gate": "available_time <= decision_date",
            "manual_review_required": True,
            "production_effect": SEC_PIT_BASELINE_COMPARISON_PRODUCTION_EFFECT,
            "production_weights_modified": False,
            "production_actions_modified": False,
        },
        "output_artifacts": {key: str(path) for key, path in artifacts.items()},
        "input_checksums": {
            "baseline_source_sha256": _file_sha256(baseline_inputs.path),
            "sec_pit_evaluation_source_sha256": _file_sha256(sec_inputs.summary_path),
            "signal_attribution_sha256": _file_sha256(sec_inputs.signal_attribution_path),
        },
    }


def _comparison_limitations(status: str, limitations: list[str]) -> list[str]:
    result = [
        "SEC reconstructed PIT is B-grade filing-time PIT, not strict vendor archive PIT.",
        "Comparison artifacts are research-only and require manual review.",
    ]
    if status == "LIMITED_BASELINE_MISSING":
        result.append("Missing baseline artifacts prevent full score-daily comparison.")
    if status == "LIMITED_SEC_PIT_EVALUATION_MISSING":
        result.append("Missing SEC PIT evaluation artifacts prevent decision-level comparison.")
    if status == "INSUFFICIENT_OVERLAP":
        result.append("Insufficient date/ticker overlap prevents incremental alpha conclusion.")
    result.extend(item for item in limitations if item not in result)
    return result


def _feature_driver_summary(impact: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    if impact.empty:
        return {"positive": [], "negative": []}
    positive = Counter()
    negative = Counter()
    for row in impact.to_dict(orient="records"):
        positive_text = str(row.get("top_positive_sec_pit_features") or "")
        negative_text = str(row.get("top_negative_sec_pit_features") or "")
        for feature, value in _parse_feature_text(positive_text):
            positive[feature] += value
        for feature, value in _parse_feature_text(negative_text):
            negative[feature] += value
    return {
        "positive": _counter_payload(positive, reverse=True),
        "negative": _counter_payload(negative, reverse=False),
    }


def _research_only_features(sec_inputs: _SecPitEvaluationInputs) -> list[dict[str, Any]]:
    frame = sec_inputs.feature_effectiveness
    if frame.empty or "recommendation" not in frame.columns:
        return []
    selected = frame.loc[
        frame["recommendation"]
        .astype(str)
        .isin(
            {"KEEP_RESEARCH_ONLY", "EXCLUDE_INSUFFICIENT_DATA", "DOWNWEIGHT"},
        )
    ].copy()
    if selected.empty:
        return []
    if "rank_ic_20d" in selected.columns:
        selected["_abs_rank_ic_20d"] = pd.to_numeric(
            selected["rank_ic_20d"],
            errors="coerce",
        ).abs()
        selected = selected.sort_values(
            ["recommendation", "_abs_rank_ic_20d", "feature_id"],
            ascending=[True, False, True],
        )
    return [
        {
            "feature_id": str(row.get("feature_id") or ""),
            "metric_id": str(row.get("metric_id") or ""),
            "recommendation": str(row.get("recommendation") or ""),
            "rank_ic_20d": _json_number(row.get("rank_ic_20d")),
        }
        for row in selected.head(10).to_dict(orient="records")
    ]


def _write_run_log(
    path: Path,
    summary: dict[str, Any],
    summary_json_path: Path,
    summary_markdown_path: Path,
) -> None:
    lines = [
        f"generated_at={summary['generated_at']}",
        f"status={summary['comparison_status']}",
        f"summary_json={summary_json_path}",
        f"summary_markdown={summary_markdown_path}",
        f"production_effect={SEC_PIT_BASELINE_COMPARISON_PRODUCTION_EFFECT}",
        f"manual_review_required={summary['safety']['manual_review_required']}",
    ]
    for key, value in summary.get("output_artifacts", {}).items():
        lines.append(f"{key}={value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_csv(frame: pd.DataFrame, path: Path, columns: tuple[str, ...]) -> None:
    output = frame.copy() if not frame.empty else pd.DataFrame(columns=list(columns))
    for column in columns:
        if column not in output.columns:
            output[column] = ""
    output = output.loc[:, list(columns)]
    path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(path, index=False)


def _empty_decision_impact() -> pd.DataFrame:
    return pd.DataFrame(columns=list(DECISION_IMPACT_COLUMNS))


def _empty_rank_shift() -> pd.DataFrame:
    return pd.DataFrame(columns=list(RANK_SHIFT_COLUMNS))


def _empty_incremental_alpha() -> pd.DataFrame:
    return pd.DataFrame(columns=list(INCREMENTAL_ALPHA_COLUMNS))


def _active_tickers(attribution: pd.DataFrame, tickers: list[str] | None) -> list[str]:
    aliases = load_ticker_aliases()
    if tickers:
        return sorted({aliases.get(ticker.upper(), ticker.upper()) for ticker in tickers})
    if attribution.empty:
        return []
    return sorted(attribution["ticker"].dropna().astype(str).str.upper().unique().tolist())


def _assign_rank(frame: pd.DataFrame, score_column: str, rank_column: str) -> pd.DataFrame:
    ranked = frame.sort_values(
        ["decision_date", score_column, "ticker"],
        ascending=[True, False, True],
    ).copy()
    ranked[rank_column] = ranked.groupby("decision_date").cumcount() + 1
    return ranked


def _feature_contribution_text(group: pd.DataFrame, *, positive: bool) -> str:
    values = (
        group.groupby("feature_id")["_contribution"]
        .sum()
        .reset_index()
        .sort_values(["_contribution", "feature_id"], ascending=[not positive, True])
    )
    if positive:
        values = values.loc[values["_contribution"] > 0]
    else:
        values = values.loc[values["_contribution"] < 0]
    parts = [
        f"{row['feature_id']}:{float(row['_contribution']):.6f}"
        for row in values.head(3).to_dict(orient="records")
    ]
    return ";".join(parts)


def _unique_join(values: pd.Series) -> str:
    result: list[str] = []
    seen: set[str] = set()
    for value in values.dropna().astype(str):
        for item in value.split(","):
            normalized = item.strip()
            if normalized and normalized not in seen:
                result.append(normalized)
                seen.add(normalized)
    return ";".join(result)


def _source_lineage_join(values: pd.Series) -> str:
    records: list[dict[str, str]] = []
    seen: set[str] = set()
    for value in values.dropna().astype(str):
        raw = value.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        items = parsed if isinstance(parsed, list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            cleaned = {
                str(key): str(val)
                for key, val in item.items()
                if val is not None and str(val).strip()
            }
            if not cleaned:
                continue
            stable = json.dumps(cleaned, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            if stable in seen:
                continue
            records.append(cleaned)
            seen.add(stable)
    return json.dumps(records, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _first_feature(value: str) -> str:
    parsed = _parse_feature_text(value)
    return parsed[0][0] if parsed else ""


def _parse_feature_text(value: str) -> list[tuple[str, float]]:
    records: list[tuple[str, float]] = []
    for item in value.split(";"):
        if not item or ":" not in item:
            continue
        feature, raw_value = item.rsplit(":", 1)
        try:
            records.append((feature, float(raw_value)))
        except ValueError:
            continue
    return records


def _rank_shift_explanation(
    ticker: str,
    rank_delta: int,
    positive_feature: str,
    negative_feature: str,
) -> str:
    if rank_delta > 0:
        driver = positive_feature or "positive SEC PIT contribution"
        return f"{ticker} promoted by {rank_delta} ranks; primary driver: {driver}."
    if rank_delta < 0:
        driver = negative_feature or "negative SEC PIT contribution"
        return f"{ticker} downgraded by {abs(rank_delta)} ranks; primary driver: {driver}."
    return f"{ticker} rank unchanged after SEC PIT evidence."


def _top_bucket(frame: pd.DataFrame, rank_column: str) -> pd.DataFrame:
    records: list[pd.DataFrame] = []
    for _, group in frame.groupby("decision_date"):
        count = max(1, int(np.ceil(len(group) * TOP_BUCKET_FRACTION)))
        records.append(group.sort_values([rank_column, "ticker"]).head(count))
    return pd.concat(records, ignore_index=True) if records else frame.head(0).copy()


def _bucket_baseline_sec_returns(
    bucket: str,
    avg_return: float,
    baseline_top_return: float,
    sec_top_return: float,
) -> tuple[float, float]:
    if bucket == "top_baseline":
        return avg_return, sec_top_return
    if bucket == "top_sec_pit":
        return baseline_top_return, avg_return
    if bucket == "unchanged":
        return avg_return, avg_return
    return baseline_top_return, avg_return


def _drawdown_improvement(
    bucket: str,
    avg_drawdown: float,
    baseline_top_drawdown: float,
    sec_top_drawdown: float,
) -> float:
    if pd.isna(avg_drawdown) or pd.isna(baseline_top_drawdown) or pd.isna(sec_top_drawdown):
        return 0.0
    if bucket == "top_baseline":
        return sec_top_drawdown - avg_drawdown
    if bucket == "top_sec_pit":
        return avg_drawdown - baseline_top_drawdown
    if bucket == "unchanged":
        return 0.0
    return avg_drawdown - baseline_top_drawdown


def _bucket_interpretation(bucket: str, incremental_return: float) -> str:
    if bucket == "top_baseline":
        return "Baseline top bucket used as existing score reference."
    if bucket == "top_sec_pit":
        return "SEC PIT top bucket compared with baseline top bucket."
    if bucket == "promoted_by_sec_pit":
        return "Rows whose rank improved after SEC PIT contribution."
    if bucket == "downgraded_by_sec_pit":
        return "Rows whose rank weakened after SEC PIT contribution."
    if incremental_return > 0:
        return "Unchanged bucket had positive incremental return."
    if incremental_return < 0:
        return "Unchanged bucket had negative incremental return."
    return "Rows whose rank did not change after SEC PIT contribution."


def _hit_rate(frame: pd.DataFrame) -> float:
    if frame.empty:
        return np.nan
    values = pd.to_numeric(frame["forward_return_20d"], errors="coerce").dropna()
    if values.empty:
        return np.nan
    return float((values > 0).mean())


def _alpha_row(frame: pd.DataFrame, bucket: str) -> dict[str, Any]:
    if frame.empty:
        return {}
    rows = frame.loc[frame["bucket"] == bucket]
    if rows.empty:
        return {}
    return rows.iloc[0].to_dict()


def _top_promoted_tickers(rank_shift: pd.DataFrame) -> list[dict[str, Any]]:
    if rank_shift.empty:
        return []
    selected = rank_shift.loc[rank_shift["rank_delta"] > 0].sort_values(
        ["rank_delta", "ticker"],
        ascending=[False, True],
    )
    return [
        {
            "ticker": row["ticker"],
            "rank_delta": int(row["rank_delta"]),
            "score_delta": _json_number(row["score_delta"]),
        }
        for row in selected.head(10).to_dict(orient="records")
    ]


def _top_downgraded_tickers(rank_shift: pd.DataFrame) -> list[dict[str, Any]]:
    if rank_shift.empty:
        return []
    selected = rank_shift.loc[rank_shift["rank_delta"] < 0].sort_values(
        ["rank_delta", "ticker"],
        ascending=[True, True],
    )
    return [
        {
            "ticker": row["ticker"],
            "rank_delta": int(row["rank_delta"]),
            "score_delta": _json_number(row["score_delta"]),
        }
        for row in selected.head(10).to_dict(orient="records")
    ]


def _counter_payload(counter: Counter[str], *, reverse: bool) -> list[dict[str, Any]]:
    items = sorted(counter.items(), key=lambda item: (item[1], item[0]), reverse=reverse)
    return [
        {"feature_id": feature, "total_contribution": _json_number(value)}
        for feature, value in items[:10]
    ]


def _pit_exclusion_limitations(counts: Counter[str]) -> list[str]:
    limitations: list[str] = []
    if counts["missing_available_time"]:
        limitations.append("SEC PIT attribution rows missing available_time were excluded.")
    if counts["future_available_time"]:
        limitations.append(
            "SEC PIT attribution rows with available_time after decision_date were excluded."
        )
    if counts["invalid_decision_date"]:
        limitations.append("SEC PIT attribution rows with invalid decision_date were excluded.")
    if counts["ticker_excluded"]:
        limitations.append("SEC PIT attribution rows outside requested tickers were excluded.")
    return limitations


def _ranking_interpretation(summary: dict[str, Any]) -> str:
    alpha = _float_or_zero(summary.get("incremental_alpha_20d"))
    if summary.get("comparison_status") != "OK":
        return "baseline comparison is limited by missing or insufficient input overlap."
    if alpha > 0:
        return f"SEC PIT top bucket exceeded baseline by {alpha:.4f} over 20d."
    if alpha < 0:
        return f"SEC PIT top bucket trailed baseline by {alpha:.4f} over 20d."
    return "No measurable 20d ranking improvement in available overlap."


def _drawdown_interpretation(summary: dict[str, Any]) -> str:
    improvement = _float_or_zero(summary.get("drawdown_improvement_20d"))
    if improvement > 0:
        return f"SEC PIT bucket had lower forward drawdown pressure by {improvement:.4f}."
    if improvement < 0:
        return f"SEC PIT bucket had worse forward drawdown pressure by {improvement:.4f}."
    return "No measurable drawdown improvement or per-ticker drawdown data is unavailable."


def _alpha_table_lines(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["- no incremental alpha rows available"]
    lines = [
        "| bucket | sample_count | avg_forward_return_20d | incremental_return_20d | "
        "drawdown_improvement_20d |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in frame.to_dict(orient="records"):
        lines.append(
            "| "
            f"`{row['bucket']}` | "
            f"{int(row['sample_count'])} | "
            f"{_format_float(row['avg_forward_return_20d'])} | "
            f"{_format_float(row['incremental_return_20d'])} | "
            f"{_format_float(row['drawdown_improvement_20d'])} |"
        )
    return lines


def _shift_table_lines(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["- none"]
    lines = [
        "| ticker | decision_date | baseline_rank | sec_pit_rank | rank_delta | score_delta |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for row in frame.head(10).to_dict(orient="records"):
        lines.append(
            "| "
            f"`{row['ticker']}` | "
            f"{row['decision_date']} | "
            f"{row['baseline_rank']} | "
            f"{row['sec_pit_rank']} | "
            f"{row['rank_delta']} | "
            f"{_format_float(row['score_delta'])} |"
        )
    return lines


def _action_change_lines(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["- none"]
    return [
        (
            f"- `{row['ticker']}` {row['decision_date']}: "
            f"{row['baseline_action']} -> {row['sec_pit_suggested_action']}"
        )
        for row in frame.head(10).to_dict(orient="records")
    ]


def _feature_driver_lines(value: object) -> list[str]:
    if not isinstance(value, list) or not value:
        return ["- none"]
    lines: list[str] = []
    for item in value[:10]:
        if not isinstance(item, dict):
            continue
        feature_id = item.get("feature_id", "")
        contribution = item.get("total_contribution", item.get("rank_ic_20d", ""))
        recommendation = item.get("recommendation", "")
        suffix = f" ({recommendation})" if recommendation else ""
        lines.append(f"- `{feature_id}`: {contribution}{suffix}")
    return lines or ["- none"]


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index)
    return pd.to_numeric(frame[column], errors="coerce")


def _first_existing_column(frame: pd.DataFrame, columns: tuple[str, ...]) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None


def _action_from_score(score: float) -> str:
    if pd.isna(score):
        return ""
    if score >= ACTION_POSITIVE_SCORE_MIN:
        return "REVIEW_POSITIVE"
    if score >= ACTION_WATCH_SCORE_MIN:
        return "WATCH"
    return "RESEARCH_ONLY"


def _read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON artifact must be an object: {path}")
    return payload


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


def _deterministic_generated_at(end: date) -> str:
    return f"{end.isoformat()}T00:00:00+00:00"


def _dominant_value(series: pd.Series) -> str:
    clean = [str(value) for value in series.dropna().tolist() if str(value)]
    if not clean:
        return ""
    return Counter(clean).most_common(1)[0][0]


def _mean_or_nan(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.mean()) if not values.empty else np.nan


def _float_or_zero(value: object) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if pd.isna(number) else number


def _json_number(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
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


def _format_float(value: object) -> str:
    number = _json_number(value)
    return "NA" if number is None else f"{number:.4f}"


def _file_sha256(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
